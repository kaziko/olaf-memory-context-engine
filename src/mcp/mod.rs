pub(crate) mod protocol;
pub(crate) mod server;
pub(crate) mod tools;

/// Public entry-point called by `cli/serve.rs` via `olaf::mcp::run()`.
/// This is the only `pub` symbol the mcp crate exposes.
pub fn run() -> anyhow::Result<()> {
    server::run()
}
