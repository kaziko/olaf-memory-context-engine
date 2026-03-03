pub(crate) mod protocol;
pub(crate) mod server;
pub(crate) mod tools;

/// Public entry-point called by `cli/serve.rs` via `olaf::mcp::run()`.
/// This is the only `pub` symbol the mcp crate exposes.
pub fn run() -> anyhow::Result<()> {
    let project_root = std::env::current_dir()?;
    let db_path = project_root.join(".olaf").join("index.db");
    let conn = crate::db::open(&db_path)?;

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    // Nanosecond precision for sub-second collision resistance across fast restarts
    let session_id = format!(
        "mcp-{}-{}{:09}",
        std::process::id(),
        ts.as_secs(),
        ts.subsec_nanos()
    );
    crate::memory::store::upsert_session(&conn, &session_id, "claude-code")?;
    server::run(conn, project_root, session_id)
}
