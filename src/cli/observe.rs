// stdout-pure: no print!/println! ever in this module
use std::io::Read;
use std::path::PathBuf;

pub(crate) fn run(event: &str) -> anyhow::Result<()> {
    if let Err(e) = inner(event) {
        log::debug!("observe: {e}");
    }
    Ok(())
}

fn inner(event: &str) -> anyhow::Result<()> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;

    let payload = match serde_json::from_str::<olaf::memory::HookPayload>(&input) {
        Ok(p) => p,
        Err(e) => {
            log::debug!("observe: failed to parse payload: {e}");
            return Ok(());
        }
    };

    match event {
        "post-tool-use" => handle_post_tool_use(&payload),
        "session-end" => handle_session_end(&payload),
        _ => {
            log::debug!("observe: unhandled event: {event}");
            Ok(())
        }
    }
}

fn handle_session_end(payload: &olaf::memory::HookPayload) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let cwd = PathBuf::from(&payload.cwd);
    let mut conn = olaf::db::open(&cwd.join(".olaf/index.db"))
        .map_err(|e| { log::debug!("observe session-end: DB open failed: {e}"); e })?;
    olaf::memory::upsert_session(&conn, &payload.session_id, "claude-code")?;
    olaf::memory::mark_session_ended(&conn, &payload.session_id)?;
    // AC3/AC7: atomic detect+compress in a single BEGIN IMMEDIATE transaction.
    // IMMEDIATE acquires the write lock before the compressed check, preventing
    // concurrent processes from both passing the guard and writing duplicate anti_pattern obs.
    let ran = olaf::memory::run_session_end_pipeline(&mut conn, &payload.session_id)?;
    if !ran {
        log::debug!("observe session-end: session already compressed, skipping");
    }
    // FR22 stub: restore-point cleanup deferred to Story 4.4
    // TODO Story 4.4: olaf::restore::cleanup_old_restore_points(&cwd)?;
    log::debug!("observe session-end: completed in {:?}", start.elapsed()); // AC8: NFR5 — always logged
    Ok(())
}

fn handle_post_tool_use(payload: &olaf::memory::HookPayload) -> anyhow::Result<()> {
    let result = match olaf::memory::parse_post_tool_use(payload) {
        Some(r) => r,
        None => return Ok(()),
    };

    // AC6: Skip sensitive paths
    if result.file_path.as_deref().is_some_and(olaf::memory::is_sensitive_path) {
        log::debug!("observe: skipping sensitive path: {:?}", result.file_path);
        return Ok(());
    }

    // NFR5: timer starts here — measures internal handler time (DB open → observation write)
    let start = std::time::Instant::now();

    // AC7: Open DB using cwd from payload — if fails, log and return Ok(())
    let cwd = PathBuf::from(&payload.cwd);
    let conn = match olaf::db::open(&cwd.join(".olaf/index.db")) {
        Ok(c) => c,
        Err(e) => {
            log::debug!("observe: DB open failed: {e}");
            return Ok(());
        }
    };

    // Ensure session record exists
    olaf::memory::upsert_session(&conn, &result.session_id, "claude-code")?;

    // Write observation — symbol_fqn always None in Story 4.1 (AC3)
    olaf::memory::insert_auto_observation(
        &conn,
        &result.session_id,
        result.kind,
        &result.content,
        None,
        result.file_path.as_deref(),
    )?;

    // NFR5: debug-level timing for internal handler performance measurement
    log::debug!("observe: handler completed in {:?}", start.elapsed());
    Ok(())
}
