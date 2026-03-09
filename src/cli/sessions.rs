use anyhow::Context;

pub(crate) fn run_list() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let db_path = cwd.join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).context("failed to open database")?;

    let sessions = olaf::memory::list_sessions(&conn, 20)?;

    if sessions.is_empty() {
        println!("No sessions found.");
        return Ok(());
    }

    println!("Sessions (most recent first):");
    println!();
    println!("  SESSION_ID                             STARTED                 OBS  STATUS");

    for s in &sessions {
        let started = chrono::DateTime::from_timestamp(s.started_at, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| s.started_at.to_string());

        let status = if s.compressed { "compressed" } else { "active" };

        println!(
            "  {:<38} {:<22} {:>4}  {}",
            s.session_id, started, s.observation_count, status
        );
    }

    println!();
    println!("Showing {} of {} sessions.", sessions.len(), sessions.len());

    Ok(())
}

pub(crate) fn run_show(id: &str) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let db_path = cwd.join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).context("failed to open database")?;

    let content_policy = olaf::policy::ContentPolicy::load(&cwd);
    let observations = match olaf::memory::get_session_observations(&conn, id, &content_policy)? {
        None => {
            eprintln!("Session not found: {id}");
            return Err(anyhow::anyhow!("session not found"));
        }
        Some(obs) => obs,
    };

    if observations.is_empty() {
        println!("No observations in session {id} (may be compressed)");
        return Ok(());
    }

    // Get session start time for header
    let session_start: Option<i64> = conn
        .query_row(
            "SELECT started_at FROM sessions WHERE id = ?1",
            rusqlite::params![id],
            |r| r.get(0),
        )
        .ok();

    let started_str = session_start
        .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!("Session: {id}");
    println!("Started: {started_str}");
    println!();

    for obs in &observations {
        let ts = chrono::DateTime::from_timestamp(obs.created_at, 0)
            .map(|dt| dt.format("%H:%M:%S UTC").to_string())
            .unwrap_or_else(|| obs.created_at.to_string());

        let location = obs
            .symbol_fqn
            .as_deref()
            .or(obs.file_path.as_deref())
            .unwrap_or("");

        let stale_marker = if obs.is_stale {
            let reason = obs.stale_reason.as_deref().unwrap_or("unknown");
            format!("  \u{26a0} STALE: {reason}")
        } else {
            String::new()
        };

        println!("  [{ts}] {:<10} {location}{stale_marker}", obs.kind);
        println!("           \"{}\"", obs.content);
        println!();
    }

    println!("{} observations shown.", observations.len());

    Ok(())
}
