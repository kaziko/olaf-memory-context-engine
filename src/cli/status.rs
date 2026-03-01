use anyhow::Context;

pub(crate) fn run() -> anyhow::Result<()> {
    let db_path = std::env::current_dir()?.join(".olaf/index.db");
    let _conn = crate::db::open(&db_path).context("failed to open database")?;
    eprintln!("not yet implemented");
    Ok(())
}
