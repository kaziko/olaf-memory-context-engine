use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use ratatui::Terminal;
use unicode_width::UnicodeWidthStr;

use super::monitor::{
    cleanup_old_events, query_events, to_presentation, EventPresentation, SourceColor,
};

const BUFFER_CAP: usize = 2000;

// --- Guards ---

pub(crate) struct PidGuard {
    path: PathBuf,
}

impl PidGuard {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        std::fs::write(&path, "")?;
        Ok(Self { path })
    }
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn new() -> anyhow::Result<Self> {
        crossterm::terminal::enable_raw_mode()?;
        if let Err(e) = crossterm::execute!(io::stdout(), EnterAlternateScreen) {
            let _ = crossterm::terminal::disable_raw_mode();
            return Err(e.into());
        }
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(io::stdout(), LeaveAlternateScreen);
    }
}

// --- App state ---

pub(crate) struct App {
    events: VecDeque<EventPresentation>,
    scroll_offset: usize,
    follow: bool,
    unseen_count: usize,
    start_time: Instant,
    total_count: usize,
    dropped_count: usize,
    tool_filter: Option<String>,
    errors_only: bool,
    help_visible: bool,
    no_color: bool,
}

impl App {
    fn new(tool_filter: Option<String>, errors_only: bool) -> Self {
        Self {
            events: VecDeque::with_capacity(BUFFER_CAP),
            scroll_offset: 0,
            follow: true,
            unseen_count: 0,
            start_time: Instant::now(),
            total_count: 0,
            dropped_count: 0,
            tool_filter,
            errors_only,
            help_visible: false,
            no_color: std::env::var("NO_COLOR").is_ok(),
        }
    }

    pub(crate) fn push_event(&mut self, ev: EventPresentation) {
        self.total_count += 1;
        if !self.follow {
            self.unseen_count += 1;
        }
        self.events.push_back(ev);
        if self.events.len() > BUFFER_CAP {
            self.events.pop_front();
            self.dropped_count += 1;
        }
    }

    /// Returns false when the app should quit.
    pub(crate) fn handle_key(&mut self, key: KeyEvent) -> bool {
        // Help overlay: any key dismisses it
        if self.help_visible {
            self.help_visible = false;
            // q while help is open only dismisses help, does not quit
            return true;
        }

        match key.code {
            KeyCode::Char('q') => return false,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return false,
            KeyCode::Char('?') => {
                self.help_visible = true;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.follow = false;
                self.scroll_offset = self.scroll_offset.saturating_add(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.follow = true;
                self.scroll_offset = 0;
                self.unseen_count = 0;
            }
            _ => {}
        }
        true
    }

    fn uptime_str(&self) -> String {
        let secs = self.start_time.elapsed().as_secs();
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        let s = secs % 60;
        if h > 0 {
            format!("{h}h{m:02}m")
        } else if m > 0 {
            format!("{m}m{s:02}s")
        } else {
            format!("{s}s")
        }
    }
}

// --- Drawing ---

fn truncate_to_width(s: &str, max_width: usize) -> String {
    if UnicodeWidthStr::width(s) <= max_width {
        return s.to_string();
    }
    let mut width = 0;
    let mut result = String::new();
    for c in s.chars() {
        let cw = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if width + cw + 1 > max_width {
            // +1 for the ellipsis
            result.push('\u{2026}');
            break;
        }
        width += cw;
        result.push(c);
    }
    result
}

fn source_style(color: SourceColor, no_color: bool) -> Style {
    if no_color {
        return Style::default();
    }
    match color {
        SourceColor::Cyan => Style::default().fg(Color::Cyan),
        SourceColor::Yellow => Style::default().fg(Color::Yellow),
        SourceColor::Green => Style::default().fg(Color::Green),
        SourceColor::Default => Style::default(),
    }
}

fn event_to_line(ev: &EventPresentation, max_width: usize, no_color: bool) -> Line<'static> {
    let dim = if no_color {
        Style::default()
    } else {
        Style::default().add_modifier(Modifier::DIM)
    };

    let text = if ev.is_error {
        let err_style = if no_color {
            Style::default()
        } else {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
        };
        let error_text = ev.error_text.as_deref().unwrap_or(&ev.summary);
        let raw = format!(
            "{} {} {} ERROR: {} {}",
            ev.time_str, ev.source_tag, ev.session_str, error_text, ev.duration_str
        );
        let truncated = truncate_to_width(&raw, max_width);
        // Build spans for the truncated version
        vec![
            Span::styled(format!("{} ", ev.time_str), dim),
            Span::styled(format!("{}", ev.source_tag), source_style(ev.source_color, no_color)),
            Span::styled(format!("{} ", ev.session_str), dim),
            Span::styled("ERROR: ", err_style),
            Span::raw(if truncated.len() < raw.len() {
                truncate_to_width(error_text, max_width.saturating_sub(
                    ev.time_str.as_str().width() + ev.source_tag.as_str().width() + ev.session_str.as_str().width() + 9
                ))
            } else {
                error_text.to_string()
            }),
            Span::styled(format!(" {}", ev.duration_str.trim()), dim),
        ]
    } else {
        let raw = format!(
            "{} {}{} {}{}",
            ev.time_str, ev.source_tag, ev.session_str, ev.summary, ev.duration_str
        );
        let truncated = truncate_to_width(&raw, max_width);
        let summary_display = if truncated.len() < raw.len() {
            truncate_to_width(&ev.summary, max_width.saturating_sub(
                ev.time_str.as_str().width() + ev.source_tag.as_str().width() + ev.session_str.as_str().width() + ev.duration_str.as_str().width() + 3
            ))
        } else {
            ev.summary.clone()
        };
        vec![
            Span::styled(format!("{} ", ev.time_str), dim),
            Span::styled(ev.source_tag.clone(), source_style(ev.source_color, no_color)),
            Span::styled(format!("{} ", ev.session_str), dim),
            Span::raw(summary_display),
            Span::styled(ev.duration_str.clone(), dim),
        ]
    };

    Line::from(text)
}

fn draw(frame: &mut ratatui::Frame, app: &App) {
    let area = frame.area();

    // Outer block
    let title = " Olaf Monitor \u{2014} watching .olaf/index.db ";
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Layout: optional dropped line + event list + status bar
    let has_dropped = app.dropped_count > 0;
    let constraints = if has_dropped {
        vec![
            Constraint::Length(1),  // dropped notice
            Constraint::Min(1),     // events
            Constraint::Length(1),  // status bar
        ]
    } else {
        vec![
            Constraint::Min(1),     // events
            Constraint::Length(1),  // status bar
        ]
    };
    let chunks = Layout::vertical(constraints).split(inner);

    let (events_area, status_area) = if has_dropped {
        // Render dropped notice
        let dropped_style = if app.no_color {
            Style::default()
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let dropped_text = Paragraph::new(Line::from(Span::styled(
            format!("{} earlier events dropped", app.dropped_count),
            dropped_style,
        )));
        frame.render_widget(dropped_text, chunks[0]);
        (chunks[1], chunks[2])
    } else {
        (chunks[0], chunks[1])
    };

    // Event list
    let max_width = events_area.width as usize;
    let visible_height = events_area.height as usize;
    let total_events = app.events.len();

    let start = if app.follow {
        total_events.saturating_sub(visible_height)
    } else {
        total_events
            .saturating_sub(visible_height)
            .saturating_sub(app.scroll_offset)
    };
    let end = (start + visible_height).min(total_events);

    let items: Vec<ListItem> = app.events.iter()
        .skip(start)
        .take(end - start)
        .map(|ev| ListItem::new(event_to_line(ev, max_width, app.no_color)))
        .collect();

    let list = List::new(items);
    frame.render_widget(list, events_area);

    // Status bar
    let follow_indicator = if app.follow {
        "FOLLOW".to_string()
    } else if app.unseen_count > 0 {
        format!("PAUSED ({} new)", app.unseen_count)
    } else {
        "PAUSED".to_string()
    };

    let status_style = if app.no_color {
        Style::default()
    } else {
        Style::default().add_modifier(Modifier::REVERSED)
    };

    let mut spans = vec![
        Span::styled(format!(" {} events", app.total_count), status_style),
        Span::styled(format!(" | {}", app.uptime_str()), status_style),
    ];
    if let Some(ref tool) = app.tool_filter {
        spans.push(Span::styled(format!(" | tool:{tool}"), status_style));
    }
    if app.errors_only {
        let eo_style = if app.no_color {
            Style::default()
        } else {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD | Modifier::REVERSED)
        };
        spans.push(Span::styled(" | ERRORS ONLY", eo_style));
    }
    spans.push(Span::styled(format!(" | {follow_indicator} "), status_style));

    let status = Paragraph::new(Line::from(spans));
    frame.render_widget(status, status_area);

    // Help overlay
    if app.help_visible {
        draw_help_overlay(frame, area, app.no_color);
    }
}

fn draw_help_overlay(frame: &mut ratatui::Frame, area: Rect, no_color: bool) {
    let help_width = 40u16.min(area.width.saturating_sub(4));
    let help_height = 12u16.min(area.height.saturating_sub(4));
    let x = (area.width.saturating_sub(help_width)) / 2;
    let y = (area.height.saturating_sub(help_height)) / 2;
    let help_area = Rect::new(x, y, help_width, help_height);

    frame.render_widget(Clear, help_area);

    let border_style = if no_color {
        Style::default()
    } else {
        Style::default().fg(Color::Cyan)
    };

    let help_text = vec![
        Line::from(""),
        Line::from("  \u{2191}/k      Scroll up (pause)"),
        Line::from("  \u{2193}/j      Scroll down"),
        Line::from("  G/End    Resume follow"),
        Line::from("  q        Quit"),
        Line::from("  ?        Toggle this help"),
        Line::from(""),
        Line::from("  Press any key to dismiss"),
    ];

    let help = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Keys ")
                .border_style(border_style),
        )
        .wrap(Wrap { trim: false });

    frame.render_widget(help, help_area);
}

// --- Main TUI loop ---

pub(crate) fn run_tui(
    conn: rusqlite::Connection,
    tail: usize,
    tool: Option<String>,
    errors_only: bool,
) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;

    // PidGuard created first, drops last
    let pid = std::process::id();
    let pid_file = cwd.join(format!(".olaf/monitor.{pid}.pid"));
    let _pid_guard = PidGuard::new(pid_file)?;

    // TerminalGuard — atomic raw mode + alternate screen
    let _terminal_guard = TerminalGuard::new()?;

    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let _ = ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    });

    let mut app = App::new(tool.clone(), errors_only);

    // Cleanup old events before loading tail (matches plain-text path order)
    cleanup_old_events(&conn);

    // Load tail events
    let clamped_tail = tail.min(BUFFER_CAP);
    let tail_events = query_events(&conn, 0, Some(clamped_tail), tool.as_deref(), errors_only);
    let mut last_seen_id = 0i64;
    for ev in &tail_events {
        app.push_event(to_presentation(ev));
        last_seen_id = ev.id;
    }

    // Timing
    let mut next_db_poll = Instant::now();
    let mut next_cleanup = Instant::now() + Duration::from_secs(300);

    // Draw initial frame
    terminal.draw(|f| draw(f, &app))?;

    while running.load(Ordering::SeqCst) {
        // Poll for keyboard input
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if !app.handle_key(key) {
                    break;
                }
            }
        }

        // DB refresh
        if Instant::now() >= next_db_poll {
            let new_events = query_events(&conn, last_seen_id, None, tool.as_deref(), errors_only);
            for ev in &new_events {
                app.push_event(to_presentation(ev));
                last_seen_id = ev.id;
            }
            next_db_poll = Instant::now() + Duration::from_millis(500);
        }

        // Periodic cleanup
        if Instant::now() >= next_cleanup {
            cleanup_old_events(&conn);
            next_cleanup = Instant::now() + Duration::from_secs(300);
        }

        // Redraw
        terminal.draw(|f| draw(f, &app))?;
    }

    // Guards drop here in reverse order (TerminalGuard, then PidGuard)
    drop(_terminal_guard);
    drop(_pid_guard);

    eprintln!("Monitor stopped. {} events displayed.", app.total_count);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_event(summary: &str) -> EventPresentation {
        EventPresentation {
            time_str: "12:00:00".to_string(),
            source_tag: "[mcp]".to_string(),
            source_color: SourceColor::Cyan,
            session_str: String::new(),
            summary: summary.to_string(),
            duration_str: " (10ms)".to_string(),
            is_error: false,
            error_text: None,
        }
    }

    #[test]
    fn test_handle_key_follow_paused() {
        let mut app = App::new(None, false);
        assert!(app.follow);
        assert_eq!(app.scroll_offset, 0);

        // Up → paused
        app.handle_key(KeyEvent::from(KeyCode::Up));
        assert!(!app.follow);
        assert_eq!(app.scroll_offset, 1);

        // G → follow
        app.handle_key(KeyEvent::from(KeyCode::Char('G')));
        assert!(app.follow);
        assert_eq!(app.scroll_offset, 0);

        // k → paused
        app.handle_key(KeyEvent::from(KeyCode::Char('k')));
        assert!(!app.follow);
        assert_eq!(app.scroll_offset, 1);

        // End → follow
        app.handle_key(KeyEvent::from(KeyCode::End));
        assert!(app.follow);
        assert_eq!(app.scroll_offset, 0);
    }

    #[test]
    fn test_handle_key_help_dismiss() {
        let mut app = App::new(None, false);

        // ? → help visible
        let cont = app.handle_key(KeyEvent::from(KeyCode::Char('?')));
        assert!(cont);
        assert!(app.help_visible);

        // q while help → dismiss help, NOT quit
        let cont = app.handle_key(KeyEvent::from(KeyCode::Char('q')));
        assert!(cont); // did not quit
        assert!(!app.help_visible);

        // q again → quit
        let cont = app.handle_key(KeyEvent::from(KeyCode::Char('q')));
        assert!(!cont); // quit
    }

    #[test]
    fn test_push_event_buffer_cap() {
        let mut app = App::new(None, false);
        for i in 0..2001 {
            app.push_event(make_event(&format!("event {i}")));
        }
        assert_eq!(app.events.len(), BUFFER_CAP);
        assert_eq!(app.dropped_count, 1);
        assert_eq!(app.total_count, 2001);
    }

    #[test]
    fn test_push_event_unseen_paused() {
        let mut app = App::new(None, false);
        app.push_event(make_event("first"));
        assert_eq!(app.unseen_count, 0); // follow mode

        // Pause
        app.handle_key(KeyEvent::from(KeyCode::Up));
        assert!(!app.follow);

        app.push_event(make_event("second"));
        assert_eq!(app.unseen_count, 1);

        app.push_event(make_event("third"));
        assert_eq!(app.unseen_count, 2);

        // Resume follow clears unseen
        app.handle_key(KeyEvent::from(KeyCode::Char('G')));
        assert_eq!(app.unseen_count, 0);
    }

    #[test]
    fn test_pid_guard() {
        let dir = tempdir().unwrap();
        let pid_file = dir.path().join("test.pid");
        {
            let _guard = PidGuard::new(pid_file.clone()).unwrap();
            assert!(pid_file.exists());
        }
        // After drop
        assert!(!pid_file.exists());
    }

    #[test]
    fn test_truncate_to_width() {
        assert_eq!(truncate_to_width("hello", 10), "hello");
        assert_eq!(truncate_to_width("hello world", 6), "hello\u{2026}");
        assert_eq!(truncate_to_width("", 5), "");
    }
}
