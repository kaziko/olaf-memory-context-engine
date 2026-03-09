// stdout-pure: no print!/println! ever in this module
use std::io::Read;
use std::path::PathBuf;

pub(crate) fn run(event: &str) -> anyhow::Result<()> {
    // Phase 1: read stdin and parse payload (if this fails, we have no cwd — just log)
    let payload = match read_and_parse_payload() {
        Ok(p) => p,
        Err(e) => {
            log::debug!("observe: {e}");
            return Ok(());
        }
    };
    let cwd = PathBuf::from(&payload.cwd);
    let monitor_active = olaf::activity::is_monitor_active(&cwd);

    // Phase 2: handle event (if this fails, we can emit an error event using cwd)
    if let Err(ref e) = handle_event(event, &payload, monitor_active) {
        log::debug!("observe: {e}");
        if monitor_active
            && let Ok(conn) = olaf::db::open(&cwd.join(".olaf/index.db"))
        {
            olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                source: "hook",
                event_type: "hook_error",
                summary: olaf::activity::truncate(&e.to_string(), 120),
                is_error: true,
                error_message: Some(olaf::activity::sanitize_error(&e.to_string(), 200)),
                ..Default::default()
            });
        }
    }
    Ok(())
}

fn read_and_parse_payload() -> anyhow::Result<olaf::memory::HookPayload> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;

    match serde_json::from_str::<olaf::memory::HookPayload>(&input) {
        Ok(p) => Ok(p),
        Err(e) => {
            log::debug!("observe: failed to parse payload: {e}");
            anyhow::bail!("failed to parse payload: {e}");
        }
    }
}

fn handle_event(event: &str, payload: &olaf::memory::HookPayload, monitor_active: bool) -> anyhow::Result<()> {
    match event {
        "pre-tool-use" => handle_pre_tool_use(payload, monitor_active),
        "post-tool-use" => handle_post_tool_use(payload, monitor_active),
        "session-end" => handle_session_end(payload, monitor_active),
        _ => {
            log::debug!("observe: unhandled event: {event}");
            Ok(())
        }
    }
}

fn handle_pre_tool_use(payload: &olaf::memory::HookPayload, monitor_active: bool) -> anyhow::Result<()> {
    // AC5: Only snapshot for Edit/Write tools
    match payload.tool_name.as_deref() {
        Some("Edit") | Some("Write") => {}
        _ => return Ok(()),
    }

    let tool_input = match &payload.tool_input {
        Some(t) => t,
        None => return Ok(()),
    };

    let abs_file_path = match tool_input.get("file_path").and_then(|v| v.as_str()) {
        Some(p) => p,
        None => return Ok(()),
    };

    // AC4: Skip sensitive paths (works on absolute paths via filename/extension check)
    if olaf::memory::is_sensitive_path(abs_file_path) {
        log::debug!("observe pre-tool-use: skipping sensitive path: {abs_file_path}");
        return Ok(());
    }

    let cwd = PathBuf::from(&payload.cwd);

    // AC8: Enforce project root boundary — reject paths outside cwd.
    let rel_file_path = match std::path::Path::new(abs_file_path).strip_prefix(&cwd) {
        Ok(rel) => {
            let mut normalized = std::path::PathBuf::new();
            for component in rel.components() {
                match component {
                    std::path::Component::ParentDir => {
                        log::debug!(
                            "observe pre-tool-use: rejecting path with .. component: {abs_file_path}"
                        );
                        return Ok(());
                    }
                    std::path::Component::CurDir => {}
                    c => normalized.push(c),
                }
            }
            normalized.to_string_lossy().into_owned()
        }
        Err(_) => {
            log::debug!(
                "observe pre-tool-use: skipping path outside project root: {abs_file_path}"
            );
            return Ok(());
        }
    };

    let start = std::time::Instant::now();

    // AC3: non-existent file handled inside snapshot() via NotFound match
    olaf::restore::snapshot(&cwd, &rel_file_path)?;

    let elapsed = start.elapsed();
    log::debug!("observe pre-tool-use: snapshot completed in {:?}", elapsed);

    if monitor_active
        && let Ok(conn) = olaf::db::open(&cwd.join(".olaf/index.db"))
    {
        let summary = if olaf::memory::is_sensitive_path(&rel_file_path) {
            "Snapshot: <redacted>".to_string()
        } else {
            format!("Snapshot: {rel_file_path}")
        };
        olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
            source: "hook",
            event_type: "snapshot",
            summary,
            duration_ms: Some(elapsed.as_millis() as u64),
            ..Default::default()
        });
    }

    Ok(())
}

fn handle_session_end(payload: &olaf::memory::HookPayload, monitor_active: bool) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let cwd = PathBuf::from(&payload.cwd);
    let mut conn = olaf::db::open(&cwd.join(".olaf/index.db"))
        .map_err(|e| { log::debug!("observe session-end: DB open failed: {e}"); e })?;
    olaf::memory::upsert_session(&conn, &payload.session_id, "claude-code")?;
    olaf::memory::mark_session_ended(&conn, &payload.session_id)?;
    let branch = olaf::config::detect_git_branch(&cwd);
    let ran = olaf::memory::run_session_end_pipeline(&mut conn, &payload.session_id, branch.as_deref())?;

    if !ran {
        log::debug!("observe session-end: session already compressed, skipping");
        if monitor_active {
            olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                source: "hook",
                event_type: "session",
                session_id: Some(payload.session_id.clone()),
                summary: "Session ended: already compressed, skipped".to_string(),
                duration_ms: Some(start.elapsed().as_millis() as u64),
                ..Default::default()
            });
        }
    } else {
        if monitor_active {
            olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                source: "hook",
                event_type: "session",
                session_id: Some(payload.session_id.clone()),
                summary: "Session ended: compressed".to_string(),
                duration_ms: Some(start.elapsed().as_millis() as u64),
                ..Default::default()
            });
        }

        let consolidation_start = std::time::Instant::now();
        let consolidated = olaf::memory::consolidate_observations(&mut conn, branch.as_deref())?;
        if consolidated > 0 {
            log::debug!("Consolidated {} duplicate observation(s)", consolidated);
        }
        if monitor_active && consolidated > 0 {
            olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                source: "hook",
                event_type: "consolidation",
                session_id: Some(payload.session_id.clone()),
                summary: format!("Consolidated {consolidated} duplicate(s)"),
                duration_ms: Some(consolidation_start.elapsed().as_millis() as u64),
                ..Default::default()
            });
        }

        let rule_start = std::time::Instant::now();
        let rule_count = olaf::memory::detect_and_write_rules(&mut conn, branch.as_deref())?;
        if rule_count > 0 {
            log::debug!("Generated {} new project rule(s)", rule_count);
        }
        if monitor_active {
            olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                source: "hook",
                event_type: "rule",
                session_id: Some(payload.session_id.clone()),
                summary: format!("Rule detection: {rule_count} new rule(s)"),
                duration_ms: Some(rule_start.elapsed().as_millis() as u64),
                ..Default::default()
            });
        }
    }

    // FR22: cleanup restore points older than 7 days
    let protect_ms = {
        conn.query_row(
            "SELECT started_at FROM sessions WHERE id = ?1",
            rusqlite::params![&payload.session_id],
            |r| r.get::<_, i64>(0),
        ).ok().map(|secs| (secs as u128) * 1000)
    };
    if let Err(e) = olaf::restore::cleanup_old_restore_points(&cwd, protect_ms) {
        log::debug!("observe session-end: restore cleanup failed: {e}");
        if monitor_active {
            olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                source: "hook",
                event_type: "hook_error",
                session_id: Some(payload.session_id.clone()),
                summary: "Restore cleanup failed".to_string(),
                is_error: true,
                error_message: Some(olaf::activity::sanitize_error(&e.to_string(), 200)),
                ..Default::default()
            });
        }
    }
    log::debug!("observe session-end: completed in {:?}", start.elapsed());
    Ok(())
}

fn handle_post_tool_use(payload: &olaf::memory::HookPayload, monitor_active: bool) -> anyhow::Result<()> {
    let tool_name = match payload.tool_name.as_deref() {
        Some(t) => t,
        None => return Ok(()),
    };

    let cwd = PathBuf::from(&payload.cwd);

    match tool_name {
        "Bash" => {
            let result = match olaf::memory::parse_post_tool_use(payload) {
                Some(r) => r,
                None => return Ok(()),
            };

            let start = std::time::Instant::now();
            let conn = match olaf::db::open(&cwd.join(".olaf/index.db")) {
                Ok(c) => c,
                Err(e) => {
                    log::debug!("observe: DB open failed: {e}");
                    return Ok(());
                }
            };
            let branch = olaf::config::detect_git_branch(&cwd);
            olaf::memory::upsert_session(&conn, &result.session_id, "claude-code")?;
            olaf::memory::insert_auto_observation(
                &conn,
                &result.session_id,
                result.kind,
                &result.content,
                None,
                result.file_path.as_deref(),
                branch.as_deref(),
            )?;
            let elapsed = start.elapsed();
            log::debug!("observe: handler completed in {:?}", elapsed);

            if monitor_active {
                olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                    source: "hook",
                    event_type: "observation",
                    session_id: Some(result.session_id.clone()),
                    tool_name: Some(tool_name.to_string()),
                    summary: format!("{tool_name} → {}", result.kind),
                    duration_ms: Some(elapsed.as_millis() as u64),
                    ..Default::default()
                });
            }
        }

        "Edit" | "Write" => {
            let tool_input = match &payload.tool_input {
                Some(t) => t,
                None => return Ok(()),
            };
            let abs_path = match tool_input.get("file_path").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return Ok(()),
            };

            let rel_path = match std::path::Path::new(abs_path).strip_prefix(&cwd) {
                Ok(rel) => {
                    let mut normalized = std::path::PathBuf::new();
                    for component in rel.components() {
                        match component {
                            std::path::Component::ParentDir => return Ok(()),
                            std::path::Component::CurDir => {}
                            c => normalized.push(c),
                        }
                    }
                    normalized.to_string_lossy().into_owned()
                }
                Err(_) => return Ok(()),
            };

            if olaf::memory::is_sensitive_path(&rel_path) {
                log::debug!("observe: skipping sensitive path: {rel_path}");
                return Ok(());
            }

            let start = std::time::Instant::now();
            let mut conn = match olaf::db::open(&cwd.join(".olaf/index.db")) {
                Ok(c) => c,
                Err(e) => {
                    log::debug!("observe: DB open failed: {e}");
                    return Ok(());
                }
            };

            let branch = olaf::config::detect_git_branch(&cwd);
            olaf::memory::upsert_session(&conn, &payload.session_id, "claude-code")?;

            // Track whether an observation was actually stored (not just a body-only diff)
            let mut obs_stored = false;
            let kind = match olaf::index::reindex_single_file(&mut conn, &cwd, &rel_path) {
                Ok(olaf::index::ReindexOutcome::Changed(diff)) => {
                    if let Some(content) = olaf::memory::format_structural_observation(&diff) {
                        olaf::memory::insert_auto_observation(
                            &conn,
                            &payload.session_id,
                            "file_change",
                            &content,
                            None,
                            Some(&rel_path),
                            branch.as_deref(),
                        )?;
                        obs_stored = true;
                    }
                    // body-only diff → no observation stored, obs_stored stays false
                    "file_change"
                }
                Ok(olaf::index::ReindexOutcome::Unchanged) => "unchanged",
                Ok(olaf::index::ReindexOutcome::SoftFailure(_)) => {
                    log::debug!("observe: structural diff soft failure, using basic obs");
                    let (content, kind) = make_fallback_obs(tool_name, tool_input, &rel_path);
                    olaf::memory::insert_auto_observation(
                        &conn,
                        &payload.session_id,
                        kind,
                        &content,
                        None,
                        Some(&rel_path),
                        branch.as_deref(),
                    )?;
                    obs_stored = true;
                    kind
                }
                Err(e) => {
                    log::debug!("observe: reindex_single_file hard error: {e}, using basic obs");
                    let (content, kind) = make_fallback_obs(tool_name, tool_input, &rel_path);
                    olaf::memory::insert_auto_observation(
                        &conn,
                        &payload.session_id,
                        kind,
                        &content,
                        None,
                        Some(&rel_path),
                        branch.as_deref(),
                    )?;
                    obs_stored = true;
                    kind
                }
            };

            let elapsed = start.elapsed();
            log::debug!("observe: handler completed in {:?}", elapsed);

            if monitor_active && obs_stored {
                olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                    source: "hook",
                    event_type: "observation",
                    session_id: Some(payload.session_id.clone()),
                    tool_name: Some(tool_name.to_string()),
                    summary: format!("{tool_name} {rel_path} → {kind}"),
                    duration_ms: Some(elapsed.as_millis() as u64),
                    ..Default::default()
                });
            }
        }

        n if n == "mcp__olaf__get_context"
            || n == "mcp__olaf__get_brief"
            || n == "mcp__olaf__get_impact" =>
        {
            let tool_input = match &payload.tool_input {
                Some(t) => t,
                None => return Ok(()),
            };

            let subject = if n == "mcp__olaf__get_impact" {
                match tool_input.get("symbol_fqn").and_then(|v| v.as_str()) {
                    Some(fqn) => format!("get_impact: {fqn}"),
                    None => return Ok(()),
                }
            } else {
                match tool_input.get("intent").and_then(|v| v.as_str()) {
                    Some(intent) => format!("get_context: {intent}"),
                    None => return Ok(()),
                }
            };

            let start = std::time::Instant::now();
            let conn = match olaf::db::open(&cwd.join(".olaf/index.db")) {
                Ok(c) => c,
                Err(e) => {
                    log::debug!("observe: DB open failed: {e}");
                    return Ok(());
                }
            };
            let branch = olaf::config::detect_git_branch(&cwd);
            olaf::memory::upsert_session(&conn, &payload.session_id, "claude-code")?;
            olaf::memory::insert_auto_observation(
                &conn,
                &payload.session_id,
                "context_retrieval",
                &subject,
                None,
                None,
                branch.as_deref(),
            )?;
            let elapsed = start.elapsed();
            log::debug!("observe: context_retrieval recorded in {:?}", elapsed);

            if monitor_active {
                olaf::activity::emit(&conn, olaf::activity::ActivityEvent {
                    source: "hook",
                    event_type: "observation",
                    session_id: Some(payload.session_id.clone()),
                    tool_name: Some(n.to_string()),
                    summary: format!("context_retrieval: {}", olaf::activity::truncate(&subject, 80)),
                    duration_ms: Some(elapsed.as_millis() as u64),
                    ..Default::default()
                });
            }
        }

        _ => {}
    }

    Ok(())
}

fn make_fallback_obs<'a>(
    tool_name: &str,
    tool_input: &serde_json::Value,
    rel_path: &str,
) -> (String, &'a str) {
    match tool_name {
        "Edit" => {
            let old_len = tool_input
                .get("old_string")
                .and_then(|v| v.as_str())
                .map(|s| s.len())
                .unwrap_or(0);
            (format!("Edited {rel_path}: replaced {old_len} chars"), "file_change")
        }
        "Write" => {
            let byte_count = tool_input
                .get("content")
                .and_then(|v| v.as_str())
                .map(|s| s.len())
                .unwrap_or(0);
            (format!("Wrote {rel_path}: {byte_count} bytes"), "file_change")
        }
        _ => unreachable!(),
    }
}
