pub(crate) mod protocol;
pub(crate) mod server;
pub(crate) mod tools;

/// Public entry-point called by `cli/serve.rs` via `olaf::mcp::run()`.
/// This is the only `pub` symbol the mcp crate exposes.
pub fn run() -> anyhow::Result<()> {
    let project_root = std::env::current_dir()?;
    let db_path = project_root.join(".olaf").join("index.db");
    let conn = crate::db::open(&db_path)?;
    server::run(conn, project_root)
}
