use std::time::{SystemTime, UNIX_EPOCH};

fn cli_normalize(project_root: &std::path::Path, file: &std::path::Path) -> anyhow::Result<String> {
    let input_str = file.to_string_lossy();
    olaf::restore::normalize_rel_path(project_root, &input_str)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn relative_age(millis: u128, now_ms: u128) -> String {
    let diff = now_ms.saturating_sub(millis);
    if diff < 60_000 {
        format!("{} seconds ago", diff / 1000)
    } else if diff < 3_600_000 {
        format!("{} minutes ago", diff / 60_000)
    } else if diff < 86_400_000 {
        format!("{} hours ago", diff / 3_600_000)
    } else {
        format!("{} days ago", diff / 86_400_000)
    }
}

pub(crate) fn run_list(file: &std::path::Path) -> anyhow::Result<()> {
    let project_root = std::env::current_dir()?;
    let rel = cli_normalize(&project_root, file)?;
    let points = olaf::restore::list_restore_points(&project_root, &rel)?;
    if points.is_empty() {
        println!("No restore points available for {}", rel);
        return Ok(());
    }
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    println!("Restore points for {}:", rel);
    for point in &points {
        println!("  {}  {} bytes  {}", point.id, point.size, relative_age(point.millis, now_ms));
    }
    Ok(())
}

pub(crate) fn run_restore(file: &std::path::Path, timestamp: i64) -> anyhow::Result<()> {
    anyhow::ensure!(timestamp >= 0, "timestamp must be a positive millisecond value");
    let millis = timestamp as u128;
    let project_root = std::env::current_dir()?;
    let rel = cli_normalize(&project_root, file)?;
    let snap_id = olaf::restore::find_snap_id_by_millis(&project_root, &rel, millis)?
        .ok_or_else(|| anyhow::anyhow!(
            "No snapshot found for {}ms in {}. Run 'olaf restore list {}' to see available snapshots.",
            millis, rel, rel
        ))?;
    olaf::restore::restore_to_snapshot(&project_root, &rel, &snap_id)?;
    println!("Restored {} to snapshot {} ({}ms).", rel, snap_id, millis);
    Ok(())
}
