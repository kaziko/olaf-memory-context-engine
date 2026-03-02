use anyhow::Context;

pub(crate) fn run() -> anyhow::Result<()> {
    let db_path = std::env::current_dir()?.join(".olaf/index.db");

    if !db_path.exists() {
        println!("Index not initialized. Run `olaf index` to build the index.");
        return Ok(());
    }

    let conn = olaf::db::open(&db_path).context("failed to open database")?;
    let stats = olaf::graph::load_db_stats(&conn)?;

    let last_indexed = match stats.last_indexed_at {
        Some(ts) => chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| ts.to_string()),
        None => "never".to_string(),
    };

    println!("Files indexed:  {}", stats.files);
    println!("Symbols:        {}", stats.symbols);
    println!("Edges:          {}", stats.edges);
    println!("Observations:   {}", stats.observations);
    println!("Last indexed:   {}", last_indexed);

    Ok(())
}
