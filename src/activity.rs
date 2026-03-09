use std::path::{Path, PathBuf};

use crate::memory::store::is_sensitive_path;

/// A structured activity event for the live monitor.
#[derive(Default)]
pub struct ActivityEvent {
    pub source: &'static str,
    pub session_id: Option<String>,
    pub event_type: &'static str,
    pub tool_name: Option<String>,
    pub summary: String,
    pub duration_ms: Option<u64>,
    pub is_error: bool,
    pub error_message: Option<String>,
}

/// Check whether any live monitor process exists by globbing for
/// `.olaf/monitor.*.pid` files and verifying PID liveness.
pub fn is_monitor_active(project_root: &Path) -> bool {
    let pattern = project_root.join(".olaf").join("monitor.*.pid");
    let pattern_str = match pattern.to_str() {
        Some(s) => s,
        None => return false,
    };
    let paths: Vec<PathBuf> = match glob::glob(pattern_str) {
        Ok(entries) => entries.filter_map(|e| e.ok()).collect(),
        Err(_) => return false,
    };
    if paths.is_empty() {
        return false;
    }
    for path in &paths {
        if let Some(fname) = path.file_name().and_then(|n| n.to_str()) {
            // monitor.{pid}.pid -> extract middle part
            if let Some(pid_str) = fname.strip_prefix("monitor.").and_then(|s| s.strip_suffix(".pid"))
                && let Ok(pid) = pid_str.parse::<i32>()
                && is_pid_alive(pid)
            {
                return true;
            }
        }
    }
    false
}

fn is_pid_alive(pid: i32) -> bool {
    let ret = unsafe { libc::kill(pid, 0) };
    if ret == 0 {
        return true;
    }
    // ret == -1, check errno
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EPERM) => true,  // process exists, different user
        _ => false,                  // ESRCH or other → dead
    }
}

/// Insert an activity event into the database. Infallible — all errors swallowed.
pub fn emit(conn: &rusqlite::Connection, event: ActivityEvent) {
    let summary = sanitize_error(&event.summary, 120);
    let error_message = event.error_message.map(|m| sanitize_error(&m, 200));
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    if let Err(e) = conn.execute(
        "INSERT INTO activity_events (timestamp, source, session_id, event_type, tool_name, summary, duration_ms, is_error, error_message) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            timestamp,
            event.source,
            event.session_id,
            event.event_type,
            event.tool_name,
            summary,
            event.duration_ms.map(|d| d as i64),
            event.is_error as i32,
            error_message,
        ],
    ) {
        log::debug!("activity emit failed: {e}");
    }
}

/// Build a summary string for a tool call using allowlisted fields.
pub fn summarize_tool_call(tool_name: &str, args: Option<&serde_json::Value>, result_len: Option<usize>) -> String {
    let detail = match (tool_name, args) {
        ("get_brief" | "get_context", Some(v)) => {
            let intent = v.get("intent").and_then(|i| i.as_str()).unwrap_or("");
            let budget = v.get("token_budget").and_then(|b| b.as_u64());
            let mut s = format!("intent={}", truncate(intent, 80));
            if let Some(b) = budget {
                s.push_str(&format!(", budget={b}"));
            }
            s
        }
        ("get_impact", Some(v)) => {
            let fqn = v.get("symbol_fqn").and_then(|s| s.as_str()).unwrap_or("");
            format!("fqn={}", truncate(fqn, 80))
        }
        ("get_file_skeleton", Some(v)) => {
            let fp = v.get("file_path").and_then(|s| s.as_str()).unwrap_or("");
            if is_sensitive_path(fp) {
                "file=<redacted>".to_string()
            } else {
                format!("file={fp}")
            }
        }
        ("get_session_history", Some(v)) => {
            let fp = v.get("file_path").and_then(|s| s.as_str());
            let fqn = v.get("symbol_fqn").and_then(|s| s.as_str());
            match (fp, fqn) {
                (Some(f), _) if is_sensitive_path(f) => "file=<redacted>".to_string(),
                (Some(f), _) => format!("file={}", truncate(f, 80)),
                (_, Some(s)) => format!("fqn={}", truncate(s, 80)),
                _ => String::new(),
            }
        }
        ("save_observation", Some(v)) => {
            let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("?");
            let fp = v.get("file_path").and_then(|s| s.as_str());
            let file_part = match fp {
                Some(f) if is_sensitive_path(f) => ", file=<redacted>".to_string(),
                Some(f) => format!(", file={f}"),
                None => String::new(),
            };
            format!("kind={kind}{file_part}")
        }
        ("analyze_failure", _) => "<trace>".to_string(),
        ("trace_flow", Some(v)) => {
            let from = v.get("from_fqn").and_then(|s| s.as_str()).unwrap_or("");
            let to = v.get("to_fqn").and_then(|s| s.as_str()).unwrap_or("");
            format!("from={}, to={}", truncate(from, 60), truncate(to, 60))
        }
        ("index_status", _) => String::new(),
        ("submit_lsp_edges", Some(v)) => {
            let count = v.get("edges").and_then(|e| e.as_array()).map(|a| a.len()).unwrap_or(0);
            format!("{count} edges")
        }
        ("list_restore_points" | "undo_change", Some(v)) => {
            let fp = v.get("file_path").and_then(|s| s.as_str()).unwrap_or("");
            if is_sensitive_path(fp) {
                "file=<redacted>".to_string()
            } else {
                format!("file={fp}")
            }
        }
        _ => "<unknown tool>".to_string(),
    };

    let mut summary = if detail.is_empty() {
        tool_name.to_string()
    } else {
        format!("{tool_name}({detail})")
    };

    if let Some(len) = result_len {
        summary.push_str(&format!(" → {len} chars"));
    }

    summary
}

/// Truncate a string to `max` characters, appending "…" if truncated.
pub fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut result: String = s.chars().take(max).collect();
        result.push('…');
        result
    }
}

/// Scan for sensitive path tokens in an error message, redact them, then truncate.
pub fn sanitize_error(msg: &str, max: usize) -> String {
    let sanitized: Vec<&str> = msg
        .split_whitespace()
        .map(|token| {
            if is_sensitive_path(token) {
                "<redacted>"
            } else {
                token
            }
        })
        .collect();
    let joined = sanitized.join(" ");
    truncate(&joined, max)
}

/// Cached monitor liveness check + dedicated DB connection for the MCP server.
pub struct MonitorGuard {
    project_root: PathBuf,
    active: bool,
    last_check: std::time::Instant,
    activity_conn: Option<rusqlite::Connection>,
}

impl MonitorGuard {
    pub fn new(project_root: &Path) -> Self {
        let active = is_monitor_active(project_root);
        Self {
            project_root: project_root.to_path_buf(),
            active,
            last_check: std::time::Instant::now(),
            activity_conn: None,
        }
    }

    /// Returns cached liveness result, refreshing every 5 seconds.
    pub fn is_active(&mut self) -> bool {
        if self.last_check.elapsed() >= std::time::Duration::from_secs(5) {
            self.active = is_monitor_active(&self.project_root);
            self.last_check = std::time::Instant::now();
        }
        self.active
    }

    /// Emit an activity event. Infallible — errors swallowed.
    pub fn emit(&mut self, event: ActivityEvent) {
        if !self.is_active() {
            return;
        }
        // Lazily open dedicated connection
        if self.activity_conn.is_none() {
            match crate::db::open(&self.project_root.join(".olaf/index.db")) {
                Ok(conn) => self.activity_conn = Some(conn),
                Err(e) => {
                    log::debug!("activity conn open failed: {e}");
                    return;
                }
            }
        }
        if let Some(ref conn) = self.activity_conn {
            emit(conn, event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_db() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join(".olaf/index.db");
        let conn = crate::db::open(&db_path).unwrap();
        (dir, conn)
    }

    #[test]
    fn test_emit_inserts_event() {
        let (_dir, conn) = test_db();
        emit(&conn, ActivityEvent {
            source: "mcp",
            event_type: "tool_call",
            tool_name: Some("get_brief".to_string()),
            summary: "test summary".to_string(),
            ..Default::default()
        });
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM activity_events", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_emit_swallows_errors() {
        // Use a closed/invalid connection — should not panic
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("nonexistent/index.db");
        // Create a connection that won't have the table
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        emit(&conn, ActivityEvent {
            source: "mcp",
            event_type: "tool_call",
            summary: "should not panic".to_string(),
            ..Default::default()
        });
        // If we got here, the function didn't panic
        let _ = db_path;
    }

    #[test]
    fn test_emit_is_infallible() {
        // Compile-time check: emit() returns () not Result
        fn assert_unit_return(_f: fn(&rusqlite::Connection, ActivityEvent)) {}
        assert_unit_return(emit);
    }

    #[test]
    fn test_summarize_tool_call_truncates() {
        let long_intent = "a".repeat(200);
        let args = serde_json::json!({"intent": long_intent});
        let result = summarize_tool_call("get_brief", Some(&args), None);
        assert!(result.contains("…"));
        assert!(result.len() < 200);
    }

    #[test]
    fn test_summarize_tool_call_redacts_sensitive() {
        let args = serde_json::json!({"file_path": ".env"});
        let result = summarize_tool_call("get_file_skeleton", Some(&args), None);
        assert!(result.contains("<redacted>"));
        assert!(!result.contains(".env"));
    }

    #[test]
    fn test_summarize_unknown_tool() {
        let args = serde_json::json!({"secret": "data"});
        let result = summarize_tool_call("unknown_tool_xyz", Some(&args), None);
        assert!(result.contains("<unknown tool>"));
        assert!(!result.contains("data"));
    }

    #[test]
    fn test_summarize_analyze_failure_hides_trace() {
        let args = serde_json::json!({"trace": "very secret stack trace content"});
        let result = summarize_tool_call("analyze_failure", Some(&args), None);
        assert!(result.contains("<trace>"));
        assert!(!result.contains("secret"));
    }

    #[test]
    fn test_truncate_short_string() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long_string() {
        let result = truncate("hello world this is long", 10);
        assert!(result.ends_with('…'));
        assert!(result.len() <= 14); // 10 chars + "…" (3 bytes)
    }

    #[test]
    fn test_sanitize_error_redacts_sensitive_paths() {
        let msg = "failed to read .env because of permission denied";
        let result = sanitize_error(msg, 200);
        assert!(result.contains("<redacted>"));
        assert!(!result.contains(".env"));
    }

    #[test]
    fn test_sanitize_error_truncates() {
        let msg = "a ".repeat(200);
        let result = sanitize_error(&msg, 50);
        assert!(result.len() <= 53); // 50 + "…" (3 bytes)
    }

    #[test]
    fn test_is_monitor_active_false_when_no_pidfile() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".olaf")).unwrap();
        assert!(!is_monitor_active(dir.path()));
    }

    #[test]
    fn test_is_monitor_active_true_when_own_pid() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        let pid = std::process::id();
        std::fs::write(olaf_dir.join(format!("monitor.{pid}.pid")), "").unwrap();
        assert!(is_monitor_active(dir.path()));
    }

    #[test]
    fn test_is_monitor_active_false_when_dead_pid() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        std::fs::write(olaf_dir.join("monitor.999999999.pid"), "").unwrap();
        assert!(!is_monitor_active(dir.path()));
    }

    #[test]
    fn test_is_monitor_active_skips_malformed_filenames() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        // Malformed filename
        std::fs::write(olaf_dir.join("monitor.abc.pid"), "").unwrap();
        // Valid with own PID
        let pid = std::process::id();
        std::fs::write(olaf_dir.join(format!("monitor.{pid}.pid")), "").unwrap();
        assert!(is_monitor_active(dir.path()));
    }

    #[test]
    fn test_monitor_guard_caches_check() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".olaf")).unwrap();
        let mut guard = MonitorGuard::new(dir.path());
        let first = guard.is_active();
        // Should return same value without re-checking (cached)
        let second = guard.is_active();
        assert_eq!(first, second);
    }

    #[test]
    fn test_monitor_guard_refreshes_after_timeout() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".olaf")).unwrap();
        let mut guard = MonitorGuard::new(dir.path());
        assert!(!guard.is_active());
        // Manually expire the cache
        guard.last_check = std::time::Instant::now() - std::time::Duration::from_secs(10);
        // Create a PID file with own PID
        let pid = std::process::id();
        std::fs::write(dir.path().join(".olaf").join(format!("monitor.{pid}.pid")), "").unwrap();
        // Should refresh and find the PID
        assert!(guard.is_active());
    }

    #[test]
    fn test_monitor_guard_lazy_connection() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();
        let guard = MonitorGuard::new(dir.path());
        assert!(guard.activity_conn.is_none());
    }
}
