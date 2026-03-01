// stdout-pure: no print!/println! ever in this module
pub(crate) fn run(event: &str) -> anyhow::Result<()> {
    log::debug!("observe: event={}", event);
    Ok(())
}
