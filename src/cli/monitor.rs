use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceColor {
    Cyan,
    Yellow,
    Green,
    Default,
}

pub(crate) struct EventPresentation {
    pub time_str: String,
    pub source_tag: String,
    pub source_color: SourceColor,
    pub session_str: String,
    pub summary: String,
    pub duration_str: String,
    pub is_error: bool,
    pub error_text: Option<String>,
}

pub(crate) fn to_presentation(ev: &EventRow) -> EventPresentation {
    let time_str = format_timestamp(ev.timestamp);
    let duration_str = ev.duration_ms.map(|ms| format!(" ({ms}ms)")).unwrap_or_default();
    let session_str = ev.session_id.as_deref().map(|s| format!(" <{s}>")).unwrap_or_default();
    let source_color = match ev.source.as_str() {
        "mcp" => SourceColor::Cyan,
        "hook" => SourceColor::Yellow,
        "cli" => SourceColor::Green,
        _ => SourceColor::Default,
    };
    EventPresentation {
        time_str,
        source_tag: format!("[{}]", ev.source),
        source_color,
        session_str,
        summary: ev.summary.clone(),
        duration_str,
        is_error: ev.is_error,
        error_text: ev.error_message.clone(),
    }
}

pub(crate) struct TuiCapabilities {
    pub stdout_is_tty: bool,
    pub stdin_is_tty: bool,
    pub json: bool,
    pub plain: bool,
    pub term: Option<String>,
}

impl TuiCapabilities {
    pub fn from_env(json: bool, plain: bool) -> Self {
        Self {
            stdout_is_tty: std::io::stdout().is_terminal(),
            stdin_is_tty: std::io::stdin().is_terminal(),
            json,
            plain,
            term: std::env::var("TERM").ok(),
        }
    }

    pub fn should_use_tui(&self) -> bool {
        self.stdout_is_tty
            && self.stdin_is_tty
            && !self.json
            && !self.plain
            && self.term.as_deref() != Some("dumb")
    }
}

pub(crate) fn run(
    json: bool,
    tail: usize,
    tool: Option<String>,
    errors_only: bool,
    plain: bool,
) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let db_path = cwd.join(".olaf/index.db");

    // Guard: verify Olaf is initialized
    if !db_path.exists() {
        anyhow::bail!("Olaf not initialized. Run olaf init first.");
    }

    let conn = olaf::db::open(&db_path)?;

    // Check TUI gate
    if TuiCapabilities::from_env(json, plain).should_use_tui() {
        return super::monitor_tui::run_tui(conn, tail, tool, errors_only);
    }

    // Cleanup old events (> 1 hour)
    cleanup_old_events(&conn);

    // Write own PID file
    let pid = std::process::id();
    let pid_file = cwd.join(format!(".olaf/monitor.{pid}.pid"));
    std::fs::write(&pid_file, "")?;

    // Set up Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    });

    let use_color = !json && std::io::stdout().is_terminal();

    println!("Olaf Monitor — watching .olaf/index.db (Ctrl+C to stop)");

    // Tail phase: show last N events
    let mut last_seen_id = 0i64;
    let mut event_count = 0usize;

    let tail_events = query_events(&conn, 0, Some(tail), tool.as_deref(), errors_only);
    if tail_events.is_empty() {
        println!("No Olaf activity seen yet. Waiting...");
    } else {
        for ev in &tail_events {
            print_event(ev, json, use_color);
            event_count += 1;
        }
        if let Some(last) = tail_events.last() {
            last_seen_id = last.id;
        }
    }

    // Follow loop
    let mut cleanup_timer = std::time::Instant::now();

    while running.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(500));

        if !running.load(Ordering::SeqCst) {
            break;
        }

        let new_events = query_events(&conn, last_seen_id, None, tool.as_deref(), errors_only);
        for ev in &new_events {
            print_event(ev, json, use_color);
            event_count += 1;
            last_seen_id = ev.id;
        }

        // Periodic cleanup every 5 minutes
        if cleanup_timer.elapsed() >= std::time::Duration::from_secs(300) {
            cleanup_old_events(&conn);
            cleanup_timer = std::time::Instant::now();
        }
    }

    // Exit cleanup
    let _ = std::fs::remove_file(&pid_file);
    println!("Monitor stopped. {event_count} events displayed.");

    Ok(())
}

pub(crate) struct EventRow {
    pub(crate) id: i64,
    pub(crate) timestamp: i64,
    pub(crate) source: String,
    pub(crate) session_id: Option<String>,
    pub(crate) event_type: String,
    pub(crate) tool_name: Option<String>,
    pub(crate) summary: String,
    pub(crate) duration_ms: Option<i64>,
    pub(crate) is_error: bool,
    pub(crate) error_message: Option<String>,
}

pub(crate) fn cleanup_old_events(conn: &rusqlite::Connection) {
    let cutoff = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
        - 3600;
    let _ = conn.execute("DELETE FROM activity_events WHERE timestamp < ?1", rusqlite::params![cutoff]);
}

pub(crate) fn query_events(
    conn: &rusqlite::Connection,
    after_id: i64,
    limit: Option<usize>,
    tool_filter: Option<&str>,
    errors_only: bool,
) -> Vec<EventRow> {
    // Build WHERE clause with filters applied BEFORE LIMIT to avoid
    // the bug where non-matching rows consume the LIMIT window.
    let cols = "id, timestamp, source, session_id, event_type, tool_name, summary, duration_ms, is_error, error_message";
    let mut conditions = vec!["id > ?1".to_string()];
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(after_id)];

    if errors_only {
        conditions.push("is_error = 1".to_string());
    }
    if let Some(tf) = tool_filter {
        params.push(Box::new(tf.to_string()));
        conditions.push(format!("tool_name = ?{}", params.len()));
    }

    let where_clause = conditions.join(" AND ");
    let (order, lim) = if let Some(n) = limit {
        params.push(Box::new(n as i64));
        ("DESC", format!(" LIMIT ?{}", params.len()))
    } else {
        ("ASC", " LIMIT 100".to_string())
    };

    let query = format!(
        "SELECT {cols} FROM activity_events WHERE {where_clause} ORDER BY id {order}{lim}"
    );

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let result = conn.prepare(&query).and_then(|mut stmt| {
        let rows = stmt.query_map(param_refs.as_slice(), map_row)?;
        let mut events: Vec<EventRow> = rows.filter_map(|r| r.ok()).collect();
        if limit.is_some() {
            events.reverse(); // DESC → chronological order
        }
        Ok(events)
    });

    match result {
        Ok(e) => e,
        Err(e) => {
            log::debug!("query_events failed: {e}");
            vec![]
        }
    }
}

pub(crate) fn map_row(row: &rusqlite::Row) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        id: row.get(0)?,
        timestamp: row.get(1)?,
        source: row.get(2)?,
        session_id: row.get(3)?,
        event_type: row.get(4)?,
        tool_name: row.get(5)?,
        summary: row.get(6)?,
        duration_ms: row.get(7)?,
        is_error: row.get::<_, i32>(8)? != 0,
        error_message: row.get(9)?,
    })
}

fn print_event(ev: &EventRow, json: bool, use_color: bool) {
    if json {
        let obj = serde_json::json!({
            "id": ev.id,
            "timestamp": ev.timestamp,
            "source": ev.source,
            "session_id": ev.session_id,
            "event_type": ev.event_type,
            "tool_name": ev.tool_name,
            "summary": ev.summary,
            "duration_ms": ev.duration_ms,
            "is_error": ev.is_error,
            "error_message": ev.error_message,
        });
        println!("{}", obj);
        return;
    }

    let p = to_presentation(ev);

    if use_color {
        let source_colored = match p.source_color {
            SourceColor::Cyan => format!("\x1b[36m{}\x1b[0m", p.source_tag),
            SourceColor::Yellow => format!("\x1b[33m{}\x1b[0m", p.source_tag),
            SourceColor::Green => format!("\x1b[32m{}\x1b[0m", p.source_tag),
            _ => p.source_tag.clone(),
        };
        let dim = "\x1b[2m";
        let reset = "\x1b[0m";

        if p.is_error {
            println!(
                "{} {}{dim}{}{reset} \x1b[31mERROR\x1b[0m: {} {dim}{}{reset}",
                p.time_str, source_colored, p.session_str,
                p.error_text.as_deref().unwrap_or(&p.summary),
                p.duration_str
            );
        } else {
            println!(
                "{} {}{dim}{}{reset} {}{dim}{}{reset}",
                p.time_str, source_colored, p.session_str, p.summary, p.duration_str
            );
        }
    } else if p.is_error {
        println!(
            "{} {}{} ERROR: {}{}",
            p.time_str, p.source_tag, p.session_str,
            p.error_text.as_deref().unwrap_or(&p.summary),
            p.duration_str
        );
    } else {
        println!("{} {}{} {}{}", p.time_str, p.source_tag, p.session_str, p.summary, p.duration_str);
    }
}

pub(crate) fn format_timestamp(ts: i64) -> String {
    let secs = ts % 86400;
    let h = (secs / 3600) % 24;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_db() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join(".olaf/index.db");
        let conn = olaf::db::open(&db_path).unwrap();
        (dir, conn)
    }

    fn insert_test_event(
        conn: &rusqlite::Connection,
        source: &str,
        event_type: &str,
        tool_name: Option<&str>,
        summary: &str,
        is_error: bool,
        timestamp_offset: i64,
    ) {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + timestamp_offset;
        conn.execute(
            "INSERT INTO activity_events (timestamp, source, event_type, tool_name, summary, is_error, error_message) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![ts, source, event_type, tool_name, summary, is_error as i32, if is_error { Some(summary) } else { None::<&str> }],
        ).unwrap();
    }

    #[test]
    fn test_format_tool_call_event() {
        let ev = EventRow {
            id: 1, timestamp: 73200 + 34 * 60 + 12, source: "mcp".into(),
            session_id: None, event_type: "tool_call".into(),
            tool_name: Some("get_brief".into()),
            summary: "get_brief(intent=fix auth bug) → 2847 chars".into(),
            duration_ms: Some(342), is_error: false, error_message: None,
        };
        // Just verify it doesn't panic
        print_event(&ev, false, false);
        print_event(&ev, true, false);
    }

    #[test]
    fn test_format_error_event() {
        let ev = EventRow {
            id: 1, timestamp: 73200, source: "mcp".into(),
            session_id: None, event_type: "tool_call".into(),
            tool_name: Some("get_brief".into()),
            summary: "get_brief".into(),
            duration_ms: Some(5), is_error: true,
            error_message: Some("internal error".into()),
        };
        print_event(&ev, false, false);
    }

    #[test]
    fn test_format_hook_error_event() {
        let ev = EventRow {
            id: 1, timestamp: 73200, source: "hook".into(),
            session_id: None, event_type: "hook_error".into(),
            tool_name: None,
            summary: "Restore cleanup failed".into(),
            duration_ms: Some(0), is_error: true,
            error_message: Some("permission denied".into()),
        };
        print_event(&ev, false, false);
    }

    #[test]
    fn test_filter_by_tool_name() {
        let (_dir, conn) = test_db();
        insert_test_event(&conn, "mcp", "tool_call", Some("get_brief"), "test1", false, 0);
        insert_test_event(&conn, "mcp", "tool_call", Some("save_observation"), "test2", false, 0);

        let events = query_events(&conn, 0, None, Some("get_brief"), false);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tool_name.as_deref(), Some("get_brief"));
    }

    #[test]
    fn test_filter_errors_only() {
        let (_dir, conn) = test_db();
        insert_test_event(&conn, "mcp", "tool_call", Some("get_brief"), "ok", false, 0);
        insert_test_event(&conn, "hook", "hook_error", None, "failed", true, 0);

        let events = query_events(&conn, 0, None, None, true);
        assert_eq!(events.len(), 1);
        assert!(events[0].is_error);
    }

    #[test]
    fn test_cleanup_old_events() {
        let (_dir, conn) = test_db();
        // Insert an old event (2 hours ago)
        let old_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 7200;
        conn.execute(
            "INSERT INTO activity_events (timestamp, source, event_type, summary, is_error) VALUES (?1, 'mcp', 'tool_call', 'old', 0)",
            rusqlite::params![old_ts],
        ).unwrap();
        // Insert a recent event
        insert_test_event(&conn, "mcp", "tool_call", None, "recent", false, 0);

        super::cleanup_old_events(&conn);

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM activity_events", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_cleanup_periodic() {
        let (_dir, conn) = test_db();
        let old_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            - 7200;
        conn.execute(
            "INSERT INTO activity_events (timestamp, source, event_type, summary, is_error) VALUES (?1, 'mcp', 'tool_call', 'old', 0)",
            rusqlite::params![old_ts],
        ).unwrap();
        super::cleanup_old_events(&conn);
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM activity_events", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_tail_returns_last_n() {
        let (_dir, conn) = test_db();
        for i in 0..20 {
            insert_test_event(&conn, "mcp", "tool_call", None, &format!("event {i}"), false, i);
        }
        let events = query_events(&conn, 0, Some(5), None, false);
        assert_eq!(events.len(), 5);
    }

    #[test]
    fn test_pid_file_create() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        let pid = std::process::id();
        let pid_file = olaf_dir.join(format!("monitor.{pid}.pid"));
        std::fs::write(&pid_file, "").unwrap();
        assert!(pid_file.exists());
    }

    #[test]
    fn test_pid_file_remove_own() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        let pid_file_1 = olaf_dir.join("monitor.111.pid");
        let pid_file_2 = olaf_dir.join("monitor.222.pid");
        std::fs::write(&pid_file_1, "").unwrap();
        std::fs::write(&pid_file_2, "").unwrap();
        std::fs::remove_file(&pid_file_1).unwrap();
        assert!(!pid_file_1.exists());
        assert!(pid_file_2.exists());
    }

    #[test]
    fn test_pid_file_cleanup_on_exit() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        let pid = std::process::id();
        let pid_file = olaf_dir.join(format!("monitor.{pid}.pid"));
        std::fs::write(&pid_file, "").unwrap();
        assert!(pid_file.exists());
        // Simulate exit cleanup
        let _ = std::fs::remove_file(&pid_file);
        assert!(!pid_file.exists());
    }

    #[test]
    fn test_should_use_tui_all_pass() {
        let cap = TuiCapabilities {
            stdout_is_tty: true,
            stdin_is_tty: true,
            json: false,
            plain: false,
            term: Some("xterm-256color".to_string()),
        };
        assert!(cap.should_use_tui());
    }

    #[test]
    fn test_should_use_tui_json_disables() {
        let cap = TuiCapabilities {
            stdout_is_tty: true,
            stdin_is_tty: true,
            json: true,
            plain: false,
            term: Some("xterm".to_string()),
        };
        assert!(!cap.should_use_tui());
    }

    #[test]
    fn test_should_use_tui_plain_disables() {
        let cap = TuiCapabilities {
            stdout_is_tty: true,
            stdin_is_tty: true,
            json: false,
            plain: true,
            term: Some("xterm".to_string()),
        };
        assert!(!cap.should_use_tui());
    }

    #[test]
    fn test_should_use_tui_dumb_term_disables() {
        let cap = TuiCapabilities {
            stdout_is_tty: true,
            stdin_is_tty: true,
            json: false,
            plain: false,
            term: Some("dumb".to_string()),
        };
        assert!(!cap.should_use_tui());
    }

    #[test]
    fn test_should_use_tui_no_tty_disables() {
        let cap = TuiCapabilities {
            stdout_is_tty: false,
            stdin_is_tty: true,
            json: false,
            plain: false,
            term: Some("xterm".to_string()),
        };
        assert!(!cap.should_use_tui());
    }

    #[test]
    fn test_should_use_tui_no_stdin_tty_disables() {
        let cap = TuiCapabilities {
            stdout_is_tty: true,
            stdin_is_tty: false,
            json: false,
            plain: false,
            term: Some("xterm".to_string()),
        };
        assert!(!cap.should_use_tui());
    }

    #[test]
    fn test_should_use_tui_none_term() {
        let cap = TuiCapabilities {
            stdout_is_tty: true,
            stdin_is_tty: true,
            json: false,
            plain: false,
            term: None,
        };
        assert!(cap.should_use_tui());
    }

    #[test]
    fn test_presentation_preserves_source_color_on_error() {
        // AC3: error events must keep original source coloring, not override to Red
        let mcp_error = EventRow {
            id: 1, timestamp: 73200, source: "mcp".into(),
            session_id: None, event_type: "tool_call".into(),
            tool_name: Some("get_brief".into()),
            summary: "get_brief".into(),
            duration_ms: Some(5), is_error: true,
            error_message: Some("internal error".into()),
        };
        let p = to_presentation(&mcp_error);
        assert_eq!(p.source_color, SourceColor::Cyan);
        assert!(p.is_error);

        let hook_error = EventRow {
            id: 2, timestamp: 73200, source: "hook".into(),
            session_id: None, event_type: "hook_error".into(),
            tool_name: None,
            summary: "failed".into(),
            duration_ms: None, is_error: true,
            error_message: Some("permission denied".into()),
        };
        let p = to_presentation(&hook_error);
        assert_eq!(p.source_color, SourceColor::Yellow);
        assert!(p.is_error);
    }

    #[test]
    fn test_plain_output_format_parity() {
        // Verify plain-mode output format matches pre-refactor expectations
        let ev = EventRow {
            id: 1, timestamp: 73200 + 34 * 60 + 12, source: "mcp".into(),
            session_id: Some("abc123".into()), event_type: "tool_call".into(),
            tool_name: Some("get_brief".into()),
            summary: "get_brief(intent=fix bug) → 100 chars".into(),
            duration_ms: Some(42), is_error: false, error_message: None,
        };
        let p = to_presentation(&ev);
        // Plain mode (no color) format: "{time} {source_tag}{session} {summary}{duration}"
        let plain_line = format!("{} {}{} {}{}", p.time_str, p.source_tag, p.session_str, p.summary, p.duration_str);
        assert_eq!(plain_line, "20:54:12 [mcp] <abc123> get_brief(intent=fix bug) → 100 chars (42ms)");
    }

    #[test]
    fn test_plain_error_output_format_parity() {
        let ev = EventRow {
            id: 1, timestamp: 73200, source: "hook".into(),
            session_id: None, event_type: "hook_error".into(),
            tool_name: None,
            summary: "cleanup".into(),
            duration_ms: Some(0), is_error: true,
            error_message: Some("permission denied".into()),
        };
        let p = to_presentation(&ev);
        let plain_line = format!(
            "{} {}{} ERROR: {}{}",
            p.time_str, p.source_tag, p.session_str,
            p.error_text.as_deref().unwrap_or(&p.summary),
            p.duration_str
        );
        assert_eq!(plain_line, "20:20:00 [hook] ERROR: permission denied (0ms)");
    }
}
