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

    // Detect workspace and construct Workspace state
    let (config, parse_warnings) = crate::workspace::parse_workspace_config(&project_root);
    let workspace = match config {
        Some(cfg) => crate::workspace::Workspace::load(conn, project_root, &cfg),
        None => crate::workspace::Workspace::single(conn, project_root, parse_warnings),
    };

    crate::memory::store::upsert_session(workspace.local_conn_ref(), &session_id, "claude-code")?;
    server::run(workspace, session_id)
}
