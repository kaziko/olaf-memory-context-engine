pub(crate) fn run() -> anyhow::Result<()> {
    let project_root = std::env::current_dir()?;
    let db_path = project_root.join(".olaf").join("index.db");
    let mut conn = olaf::db::open(&db_path)?;

    let start = std::time::Instant::now();
    let stats = olaf::index::run(&mut conn, &project_root)?;
    let elapsed = start.elapsed();

    eprintln!("indexed {} files, {} symbols, {} edges", stats.files, stats.symbols, stats.edges);

    if olaf::activity::is_monitor_active(&project_root) {
        olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
            source: "cli",
            event_type: "index",
            summary: format!("Full index: {} files, {} symbols", stats.files, stats.symbols),
            duration_ms: Some(elapsed.as_millis() as u64),
            ..Default::default()
        });
    }

    olaf::memory::run_compression(
        &mut conn,
        olaf::memory::DEFAULT_COMPRESSION_THRESHOLD_SECS,
    )?;

    // FR22: cleanup restore points older than 7 days (no session context during index)
    if let Err(e) = olaf::restore::cleanup_old_restore_points(&project_root, None) {
        log::debug!("index: restore cleanup failed: {e}");
    }

    Ok(())
}
