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
        "pre-tool-use" => handle_pre_tool_use(&payload),
        "post-tool-use" => handle_post_tool_use(&payload),
        "session-end" => handle_session_end(&payload),
        _ => {
            log::debug!("observe: unhandled event: {event}");
            Ok(())
        }
    }
}

fn handle_pre_tool_use(payload: &olaf::memory::HookPayload) -> anyhow::Result<()> {
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
    // strip_prefix is lexical; walk components to:
    //   - reject ParentDir (..) to prevent /project/../outside escape
    //   - skip CurDir (.) to normalize src/./main.rs → src/main.rs (stable hash bucket)
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
                    std::path::Component::CurDir => {} // skip '.' — normalizes path for stable hash
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

    log::debug!("observe pre-tool-use: snapshot completed in {:?}", start.elapsed());
    Ok(())
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
    let branch = olaf::config::detect_git_branch(&cwd);
    let ran = olaf::memory::run_session_end_pipeline(&mut conn, &payload.session_id, branch.as_deref())?;
    if !ran {
        log::debug!("observe session-end: session already compressed, skipping");
    }
    // FR22: cleanup restore points older than 7 days, protecting current-session snapshots
    let protect_ms = {
        // Get session start time from DB to ensure current-session snapshots are never deleted
        conn.query_row(
            "SELECT started_at FROM sessions WHERE id = ?1",
            rusqlite::params![&payload.session_id],
            |r| r.get::<_, i64>(0),
        ).ok().map(|secs| (secs as u128) * 1000)
    };
    if let Err(e) = olaf::restore::cleanup_old_restore_points(&cwd, protect_ms) {
        log::debug!("observe session-end: restore cleanup failed: {e}");
    }
    log::debug!("observe session-end: completed in {:?}", start.elapsed()); // AC8: NFR5 — always logged
    Ok(())
}

fn handle_post_tool_use(payload: &olaf::memory::HookPayload) -> anyhow::Result<()> {
    let tool_name = match payload.tool_name.as_deref() {
        Some(t) => t,
        None => return Ok(()),
    };

    let cwd = PathBuf::from(&payload.cwd);

    match tool_name {
        "Bash" => {
            // Bash path: unchanged — use parse_post_tool_use, non-mut DB
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
            log::debug!("observe: handler completed in {:?}", start.elapsed());
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

            // Same path normalization as handle_pre_tool_use (lines 64–87)
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

            match olaf::index::reindex_single_file(&mut conn, &cwd, &rel_path) {
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
                    }
                    // body-only diff → no observation
                }
                Ok(olaf::index::ReindexOutcome::Unchanged) => {
                    // hash matched — no observation
                }
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
                }
            }

            log::debug!("observe: handler completed in {:?}", start.elapsed());
        }

        n if n == "mcp__olaf__get_context"
            || n == "mcp__olaf__get_brief"
            || n == "mcp__olaf__get_impact" =>
        {
            let tool_input = match &payload.tool_input {
                Some(t) => t,
                None => return Ok(()),
            };

            // Extract normalized subject string for dead-end detection
            let subject = if n == "mcp__olaf__get_impact" {
                match tool_input.get("symbol_fqn").and_then(|v| v.as_str()) {
                    Some(fqn) => format!("get_impact: {fqn}"),
                    None => return Ok(()), // malformed payload — skip
                }
            } else {
                match tool_input.get("intent").and_then(|v| v.as_str()) {
                    Some(intent) => format!("get_context: {intent}"),
                    None => return Ok(()), // malformed payload — skip
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
            log::debug!("observe: context_retrieval recorded in {:?}", start.elapsed());
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
