// stdout-pure: no print!/println! ever in this module
pub(crate) fn run() -> anyhow::Result<()> {
    olaf::mcp::run()
}
