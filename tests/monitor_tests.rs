use std::path::Path;
use tempfile::tempdir;

fn setup_db(dir: &Path) -> rusqlite::Connection {
    let db_path = dir.join(".olaf/index.db");
    olaf::db::open(&db_path).unwrap()
}

fn create_pid_file(dir: &Path, pid: u32) {
    let olaf_dir = dir.join(".olaf");
    std::fs::create_dir_all(&olaf_dir).unwrap();
    std::fs::write(olaf_dir.join(format!("monitor.{pid}.pid")), "").unwrap();
}

fn count_events(conn: &rusqlite::Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM activity_events", [], |r| r.get(0)).unwrap()
}

#[test]
fn test_monitor_guard_emits_on_active() {
    let dir = tempdir().unwrap();
    let _conn = setup_db(dir.path());
    let pid = std::process::id();
    create_pid_file(dir.path(), pid);

    let mut guard = olaf::activity::MonitorGuard::new(dir.path());
    assert!(guard.is_active());

    guard.emit(olaf::activity::ActivityEvent {
        source: "mcp",
        event_type: "tool_call",
        tool_name: Some("get_brief".into()),
        summary: "test event".into(),
        ..Default::default()
    });

    // Verify event was written — need to open a separate connection to read
    let read_conn = setup_db(dir.path());
    let count = count_events(&read_conn);
    assert_eq!(count, 1, "MonitorGuard should emit when active");
}

#[test]
fn test_monitor_guard_skips_on_inactive() {
    let dir = tempdir().unwrap();
    let _conn = setup_db(dir.path());
    // No PID file — monitor is inactive

    let mut guard = olaf::activity::MonitorGuard::new(dir.path());
    assert!(!guard.is_active());

    guard.emit(olaf::activity::ActivityEvent {
        source: "mcp",
        event_type: "tool_call",
        summary: "should not be written".into(),
        ..Default::default()
    });

    let read_conn = setup_db(dir.path());
    let count = count_events(&read_conn);
    assert_eq!(count, 0, "MonitorGuard should not emit when inactive");
}

#[test]
fn test_dead_pid_means_inactive() {
    let dir = tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".olaf")).unwrap();
    // Use a PID that almost certainly doesn't exist
    create_pid_file(dir.path(), 999999999);

    assert!(!olaf::activity::is_monitor_active(dir.path()));
}

#[test]
fn test_concurrent_pid_files() {
    let dir = tempdir().unwrap();
    let olaf_dir = dir.path().join(".olaf");
    std::fs::create_dir_all(&olaf_dir).unwrap();

    let dir_path = dir.path().to_path_buf();
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let dp = dir_path.clone();
            std::thread::spawn(move || {
                // Use fake PIDs that won't be alive
                let fake_pid = 900000000 + i;
                let pid_file = dp.join(format!(".olaf/monitor.{fake_pid}.pid"));
                std::fs::write(&pid_file, "").unwrap();
                // Check should not panic or return incorrect results
                let _ = olaf::activity::is_monitor_active(&dp);
                std::fs::remove_file(&pid_file).unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // After all threads complete, no PID files should remain
    let remaining: Vec<_> = std::fs::read_dir(&olaf_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("monitor."))
        .collect();
    assert!(remaining.is_empty(), "all PID files should be cleaned up");
}

#[test]
fn test_hook_handler_emits_on_active() {
    // This test verifies that the emit() function works correctly
    // when called from a hook-like context with monitor_active=true
    let dir = tempdir().unwrap();
    let conn = setup_db(dir.path());
    let pid = std::process::id();
    create_pid_file(dir.path(), pid);

    assert!(olaf::activity::is_monitor_active(dir.path()));

    // Simulate what handle_post_tool_use does when monitor_active
    olaf::activity::emit(
        &conn,
        olaf::activity::ActivityEvent {
            source: "hook",
            event_type: "observation",
            tool_name: Some("Edit".into()),
            summary: "Edit src/main.rs → file_change".into(),
            duration_ms: Some(4),
            ..Default::default()
        },
    );

    let count = count_events(&conn);
    assert_eq!(count, 1);

    // Verify the event data
    let (source, event_type, summary): (String, String, String) = conn
        .query_row(
            "SELECT source, event_type, summary FROM activity_events WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(source, "hook");
    assert_eq!(event_type, "observation");
    assert!(summary.contains("Edit src/main.rs"));
}

#[test]
fn test_hook_error_emits_event() {
    let dir = tempdir().unwrap();
    let conn = setup_db(dir.path());
    let pid = std::process::id();
    create_pid_file(dir.path(), pid);

    // Simulate a hook error emission
    olaf::activity::emit(
        &conn,
        olaf::activity::ActivityEvent {
            source: "hook",
            event_type: "hook_error",
            summary: "handler failed".into(),
            is_error: true,
            error_message: Some("test error message".into()),
            ..Default::default()
        },
    );

    let count = count_events(&conn);
    assert_eq!(count, 1);

    let (is_error, error_msg): (i32, Option<String>) = conn
        .query_row(
            "SELECT is_error, error_message FROM activity_events WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(is_error, 1);
    assert_eq!(error_msg.as_deref(), Some("test error message"));
}

#[test]
fn test_session_end_emits_session_and_rule_events() {
    // Test that both session and rule events can be emitted
    let dir = tempdir().unwrap();
    let conn = setup_db(dir.path());
    let pid = std::process::id();
    create_pid_file(dir.path(), pid);

    // Emit session event
    olaf::activity::emit(
        &conn,
        olaf::activity::ActivityEvent {
            source: "hook",
            event_type: "session",
            session_id: Some("test-session".into()),
            summary: "Session ended: compressed".into(),
            duration_ms: Some(142),
            ..Default::default()
        },
    );

    // Emit rule event
    olaf::activity::emit(
        &conn,
        olaf::activity::ActivityEvent {
            source: "hook",
            event_type: "rule",
            session_id: Some("test-session".into()),
            summary: "Rule detection: 1 new rule(s)".into(),
            duration_ms: Some(14),
            ..Default::default()
        },
    );

    let count = count_events(&conn);
    assert_eq!(count, 2);

    // Verify both event types exist
    let session_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM activity_events WHERE event_type = 'session'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let rule_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM activity_events WHERE event_type = 'rule'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(session_count, 1);
    assert_eq!(rule_count, 1);
}
