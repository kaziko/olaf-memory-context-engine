use std::process::Command;

fn olaf() -> Command {
    Command::new(env!("CARGO_BIN_EXE_olaf"))
}

fn setup_db(dir: &std::path::Path) {
    // Create a DB with sessions and observations
    let db_path = dir.join(".olaf").join("index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    conn.execute(
        "INSERT INTO sessions (id, started_at, agent) VALUES ('test-session-001', ?1, 'test')",
        rusqlite::params![now - 3600],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn) \
         VALUES ('test-session-001', ?1, 'insight', 'test observation content', 'src/main.rs::main')",
        rusqlite::params![now - 3500],
    )
    .unwrap();
}

// Task 11.1: olaf sessions list outputs table with session data, exits 0
#[test]
fn test_sessions_list_outputs_table() {
    let dir = tempfile::tempdir().unwrap();
    setup_db(dir.path());

    let out = olaf()
        .args(["sessions", "list"])
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf sessions list");

    assert!(out.status.success(), "must exit 0; stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("SESSION_ID"), "must include table header; got: {stdout}");
    assert!(stdout.contains("test-session-001"), "must include session ID; got: {stdout}");
    assert!(stdout.contains("UTC"), "must include UTC timestamp; got: {stdout}");
}

// Task 11.2: olaf sessions show <valid_id> outputs observations, exits 0
#[test]
fn test_sessions_show_valid_id() {
    let dir = tempfile::tempdir().unwrap();
    setup_db(dir.path());

    let out = olaf()
        .args(["sessions", "show", "test-session-001"])
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf sessions show");

    assert!(out.status.success(), "must exit 0; stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Session: test-session-001"), "must show session header; got: {stdout}");
    assert!(stdout.contains("insight"), "must show observation kind; got: {stdout}");
    assert!(stdout.contains("test observation content"), "must show observation content; got: {stdout}");
    assert!(stdout.contains("UTC"), "must show UTC timestamp; got: {stdout}");
}

// Task 11.3: olaf sessions show <invalid_id> prints error to stderr, exits non-zero
#[test]
fn test_sessions_show_invalid_id() {
    let dir = tempfile::tempdir().unwrap();
    setup_db(dir.path());

    let out = olaf()
        .args(["sessions", "show", "nonexistent-session"])
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf sessions show");

    assert!(!out.status.success(), "must exit non-zero for invalid session");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Session not found") || stderr.contains("session not found"),
        "must print 'session not found' to stderr; got: {stderr}"
    );
}
