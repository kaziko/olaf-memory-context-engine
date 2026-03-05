use std::path::Path;
use rusqlite::OptionalExtension;
use serde_json::Value;

/// Error types for tool dispatch — maps to MCP error codes in server.rs.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ToolError {
    /// Tool name unknown → MCP -32601
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    /// Required parameter missing or wrong type → MCP -32602
    #[error("invalid params: {0}")]
    InvalidParams(String),
    /// Any other internal failure → MCP -32603
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Returns the list of available tool definitions.
pub(crate) fn list() -> Vec<Value> {
    vec![
        serde_json::json!({
            "name": "get_context",
            "description": "Get a token-budgeted context brief for a given intent. Triggers incremental re-index before building the response.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Natural language description of what you want to do, e.g. 'fix bug in auth module'"
                    },
                    "file_hints": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional file paths or partial paths to prioritize as pivot symbols"
                    },
                    "token_budget": {
                        "type": "integer",
                        "description": "Maximum tokens for the response (default: 4000)"
                    }
                },
                "required": ["intent"]
            }
        }),
        serde_json::json!({
            "name": "get_impact",
            "description": "Find symbols that call, extend, or implement a given symbol FQN. Note: import relationships are not tracked in the index.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Fully qualified name, e.g. 'src/auth.ts::AuthService::login'"
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Levels of dependents to traverse (default: 3)"
                    }
                },
                "required": ["symbol_fqn"]
            }
        }),
        serde_json::json!({
            "name": "get_file_skeleton",
            "description": "Get all symbol signatures, docstrings, and dependency edges for a file — no implementation bodies. Accepts exact or partial file paths.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "File path or partial path, e.g. 'src/auth.ts' or 'auth.ts'"
                    }
                },
                "required": ["file_path"]
            }
        }),
        serde_json::json!({
            "name": "index_status",
            "description": "Get index health: file count, symbol count, edge count, observation count, last indexed timestamp.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
        serde_json::json!({
            "name": "save_observation",
            "description": "Save an observation (insight, decision, error, etc.) linked to a symbol FQN or file path. Persists to session memory for retrieval in future sessions. At least one of symbol_fqn or file_path is required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Plain English description of the observation"
                    },
                    "kind": {
                        "type": "string",
                        "description": "One of: insight, decision, error, tool_call, file_change, anti_pattern"
                    },
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Optional: symbol FQN to link this observation to, e.g. 'src/auth.ts::AuthService::login'"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Optional: file path to link this observation to, e.g. 'src/auth.ts'"
                    }
                },
                "required": ["content", "kind"]
            }
        }),
        serde_json::json!({
            "name": "get_session_history",
            "description": "Get observations and changes from recent sessions, optionally filtered by file or symbol.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Optional: filter to observations linked to this file path"
                    },
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Optional: filter to observations linked to this symbol FQN (e.g. 'src/auth.ts::AuthService::login')"
                    },
                    "sessions_back": {
                        "type": "integer",
                        "description": "How many recent sessions to include (default: 5, max: 50)"
                    }
                }
            }
        }),
        serde_json::json!({
            "name": "list_restore_points",
            "description": "List available pre-edit snapshots for a file, sorted newest-first. Returns snapshot IDs to use with undo_change.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Project-relative or absolute path to the file"
                    }
                },
                "required": ["file_path"]
            }
        }),
        serde_json::json!({
            "name": "undo_change",
            "description": "Restore a file to a specific pre-edit snapshot using a snapshot ID from list_restore_points. Writes a decision observation recording the revert.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Project-relative or absolute path to the file to restore"
                    },
                    "snapshot_id": {
                        "type": "string",
                        "description": "Snapshot ID from list_restore_points output (e.g. '1740000000000-12345-7')"
                    }
                },
                "required": ["file_path", "snapshot_id"]
            }
        }),
        serde_json::json!({
            "name": "trace_flow",
            "description": "Find execution paths between two symbols in the call graph. Traverses calls/extends/implements edges. Returns shortest paths up to max_paths.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_fqn": { "type": "string", "description": "FQN of the starting symbol, e.g. 'src/auth.rs::login'" },
                    "target_fqn": { "type": "string", "description": "FQN of the destination symbol, e.g. 'src/db.rs::query'" },
                    "max_paths":  { "type": "integer", "description": "Maximum paths to return (default: 5, max: 20)" }
                },
                "required": ["source_fqn", "target_fqn"]
            }
        }),
        serde_json::json!({
            "name": "run_pipeline",
            "description": "Run context retrieval and impact analysis in one call. Returns a unified brief for a given intent. Faster than orchestrating get_context + get_impact separately.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Natural language description of the task, e.g. 'fix bug in auth module'"
                    },
                    "file_hints": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional file paths or partial paths to prioritize as pivot symbols"
                    },
                    "token_budget": {
                        "type": "integer",
                        "description": "Maximum tokens for the combined response (default: 4000)"
                    },
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Optional: FQN of primary symbol for impact analysis (e.g. 'src/auth.rs::authenticate'). Omit to skip impact graph."
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Impact traversal depth (default: 3, max: 10)"
                    }
                },
                "required": ["intent"]
            }
        }),
    ]
}

/// Dispatches a tools/call request to the appropriate handler.
/// Returns the tool's text response (server.rs wraps it in MCP content format).
pub(crate) fn dispatch(conn: &mut rusqlite::Connection, project_root: &Path, session_id: &str, params: Option<&Value>) -> Result<String, ToolError> {
    let tool_name = params
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: name".to_string()))?;

    let args = params.and_then(|p| p.get("arguments"));
    match tool_name {
        "get_context"      => handle_get_context(conn, project_root, args),
        "get_impact"       => handle_get_impact(conn, args),
        "get_file_skeleton" => handle_get_file_skeleton(conn, args),
        "index_status"     => handle_index_status(conn),
        "save_observation"     => handle_save_observation(conn, session_id, args),
        "get_session_history"  => handle_get_session_history(conn, args),
        "list_restore_points"  => handle_list_restore_points(project_root, args),
        "undo_change"          => handle_undo_change(conn, project_root, session_id, args),
        "run_pipeline"         => handle_run_pipeline(conn, project_root, args),
        "trace_flow"           => handle_trace_flow(conn, args),
        _ => Err(ToolError::UnknownTool(tool_name.to_string())),
    }
}

/// Normalize a file path for MCP handlers: converts to project-relative and rejects escapes.
fn mcp_normalize(project_root: &Path, file_path: &str) -> Result<String, ToolError> {
    crate::restore::normalize_rel_path(project_root, file_path)
        .map_err(|e| ToolError::InvalidParams(e.to_string()))
}

fn handle_get_context(conn: &mut rusqlite::Connection, project_root: &Path, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);
    let intent = args.get("intent").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: intent".to_string()))?;
    let file_hints: Vec<String> = args.get("file_hints").and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    let token_budget = args.get("token_budget").and_then(|v| v.as_u64()).map(|v| v as usize).unwrap_or(4000);

    crate::index::run_incremental(conn, project_root)?;

    crate::graph::query::get_context(conn, project_root, intent, &file_hints, token_budget)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

fn handle_get_impact(conn: &rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);
    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: symbol_fqn".to_string()))?;
    let depth = args.get("depth").and_then(|v| v.as_u64()).map(|v| v as usize).unwrap_or(3);

    crate::graph::query::get_impact(conn, symbol_fqn, depth)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

fn handle_get_file_skeleton(conn: &rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);
    let file_path = args.get("file_path").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: file_path".to_string()))?;
    let file_path = file_path.trim();
    if file_path.is_empty() {
        return Err(ToolError::InvalidParams("file_path must not be empty".to_string()));
    }
    crate::graph::query::get_file_skeleton(conn, file_path)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

fn handle_index_status(conn: &rusqlite::Connection) -> Result<String, ToolError> {
    crate::graph::query::index_status(conn)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

const VALID_KINDS: &[&str] = &["insight", "decision", "error", "tool_call", "file_change", "anti_pattern"];

fn handle_save_observation(conn: &mut rusqlite::Connection, session_id: &str, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let content = args.get("content").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: content".into()))?;
    let kind = args.get("kind").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: kind".into()))?;

    if !VALID_KINDS.contains(&kind) {
        return Err(ToolError::InvalidParams(
            format!("invalid kind '{kind}'; must be one of: insight, decision, error, tool_call, file_change, anti_pattern")
        ));
    }

    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());
    let file_path  = args.get("file_path").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());

    // NFR7: reject observations linked to sensitive files
    if let Some(fp) = file_path
        && crate::memory::store::is_sensitive_path(fp)
    {
        return Err(ToolError::InvalidParams(
            "file_path refers to a sensitive file — observation rejected per NFR7".into(),
        ));
    }
    if let Some(fqn) = symbol_fqn
        && let Some(prefix) = fqn.split("::").next()
        && crate::memory::store::is_sensitive_path(prefix)
    {
        return Err(ToolError::InvalidParams(
            "symbol_fqn refers to a sensitive file — observation rejected per NFR7".into(),
        ));
    }

    if symbol_fqn.is_none() && file_path.is_none() {
        return Err(ToolError::InvalidParams(
            "at least one of symbol_fqn or file_path is required".into()
        ));
    }

    crate::memory::store::upsert_session(conn, session_id, "claude-code")
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;
    let id = crate::memory::store::insert_observation(conn, session_id, kind, content, symbol_fqn, file_path)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    Ok(format!("Observation saved (id={id}, kind={kind})."))
}

fn handle_get_session_history(conn: &mut rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let sessions_back = args.get("sessions_back")
        .and_then(|v| v.as_i64())
        .map(|v| (v.clamp(1, 50)) as usize)
        .unwrap_or(5);

    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());
    let file_path = args.get("file_path").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());

    let session_ids = crate::memory::store::get_recent_session_ids(conn, sessions_back)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    if session_ids.is_empty() {
        return Ok("No sessions found.".into());
    }

    let observations = crate::memory::store::get_observations_filtered(conn, &session_ids, symbol_fqn, file_path)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    if observations.is_empty() {
        return Ok("No observations found matching the given filters.".into());
    }

    let total = observations.len();
    let capped = total.min(200);

    // Group observations by session_id, preserving order
    let mut session_order: Vec<String> = Vec::new();
    let mut by_session: std::collections::HashMap<String, Vec<&crate::memory::store::ObservationRow>> =
        std::collections::HashMap::new();

    for obs in observations.iter().take(capped) {
        if !by_session.contains_key(&obs.session_id) {
            session_order.push(obs.session_id.clone());
        }
        by_session.entry(obs.session_id.clone()).or_default().push(obs);
    }

    // Pre-fetch session start times for accurate headers
    let session_timestamps: std::collections::HashMap<String, i64> = session_ids.iter().filter_map(|sid| {
        conn.query_row(
            "SELECT started_at FROM sessions WHERE id = ?1",
            rusqlite::params![sid],
            |r| r.get(0),
        ).ok().map(|ts: i64| (sid.clone(), ts))
    }).collect();

    let mut output = format!("# Session History (last {} sessions)\n", session_ids.len());
    let mut stale_count = 0usize;

    for sid in &session_order {
        let ts = session_timestamps.get(sid).copied().unwrap_or(0);
        let dt = chrono::DateTime::from_timestamp(ts, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "unknown".into());

        output.push_str(&format!("\n## Session {} ({})\n\n", sid, dt));

        let obs_list = &by_session[sid];
        for obs in obs_list {
            if obs.is_stale {
                stale_count += 1;
                let reason = obs.stale_reason.as_deref().unwrap_or("unknown reason");
                output.push_str(&format!("- \u{26a0} [STALE \u{2014} {}] [{}] {}\n", reason, obs.kind, obs.content));
            } else {
                output.push_str(&format!("- [{}] {}\n", obs.kind, obs.content));
            }
            if let Some(fqn) = &obs.symbol_fqn {
                output.push_str(&format!("  Symbol: {}\n", fqn));
            }
            if let Some(fp) = &obs.file_path {
                output.push_str(&format!("  File: {}\n", fp));
            }
        }
    }

    let stale_suffix = if stale_count > 0 {
        format!(" ({} stale)", stale_count)
    } else {
        String::new()
    };
    output.push_str(&format!(
        "\n{} sessions, {} observations{}\n",
        session_order.len(),
        capped,
        stale_suffix
    ));

    if total > 200 {
        output.push_str(&format!(
            "\n(Showing 200 of {} observations. Use filters to narrow results.)\n",
            total
        ));
    }

    Ok(output)
}

fn handle_list_restore_points(project_root: &Path, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let file_path = args.get("file_path").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: file_path".into()))?;

    // NFR7: reject sensitive file paths
    if crate::memory::store::is_sensitive_path(file_path) {
        return Err(ToolError::InvalidParams("sensitive file path rejected per NFR7".into()));
    }

    let rel = mcp_normalize(project_root, file_path)?;

    let points = crate::restore::list_restore_points(project_root, &rel)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    if points.is_empty() {
        return Ok(format!("No restore points available for {rel}"));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let mut output = format!("Restore points for {} ({} available):\n", rel, points.len());
    for point in &points {
        let age = relative_age_ms(point.millis, now_ms);
        output.push_str(&format!("  {}  {} bytes  {}\n", point.id, point.size, age));
    }
    Ok(output)
}

fn handle_undo_change(conn: &mut rusqlite::Connection, project_root: &Path, session_id: &str, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let file_path = args.get("file_path").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: file_path".into()))?;
    let snapshot_id = args.get("snapshot_id").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: snapshot_id".into()))?;

    // NFR7: reject sensitive file paths
    if crate::memory::store::is_sensitive_path(file_path) {
        return Err(ToolError::InvalidParams("sensitive file path rejected per NFR7".into()));
    }

    let rel = mcp_normalize(project_root, file_path)?;

    crate::restore::restore_to_snapshot(project_root, &rel, snapshot_id)
        .map_err(|e| match e {
            crate::restore::RestoreError::SnapshotNotFound(id, available) =>
                ToolError::InvalidParams(format!("Snapshot '{id}' not found. Available: {available}")),
            crate::restore::RestoreError::PathOutsideRoot(p) =>
                ToolError::InvalidParams(format!("Invalid snapshot_id: {p}")),
            other => ToolError::Internal(anyhow::anyhow!("{other}")),
        })?;

    // Write a persistent decision observation (non-auto, survives compression)
    crate::memory::store::upsert_session(conn, session_id, "claude-code")
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;
    crate::memory::store::insert_observation(
        conn, session_id, "decision",
        &format!("Reverted {} — restore point {} applied", rel, snapshot_id),
        None, Some(&rel),
    ).map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    Ok(format!("Restored {} to snapshot {}.", rel, snapshot_id))
}

/// Maximum token budget accepted from callers. Prevents u64→usize cast overflow and
/// keeps `max_bytes = budget * 4` well within usize range on 32-bit targets (4 GB limit).
const MAX_TOKEN_BUDGET: u64 = 1_000_000;

fn handle_run_pipeline(
    conn: &mut rusqlite::Connection,
    project_root: &Path,
    args: Option<&Value>,
) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    // 2.1 Parse args
    let intent = args.get("intent").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: intent".to_string()))?;
    let file_hints: Vec<String> = args.get("file_hints").and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    let token_budget = args.get("token_budget").and_then(|v| v.as_u64())
        .map(|v| v.min(MAX_TOKEN_BUDGET) as usize)
        .unwrap_or(4000)
        .max(100);
    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str());
    let depth = args.get("depth").and_then(|v| v.as_u64())
        .map(|v| (v as usize).clamp(1, 10))
        .unwrap_or(3);

    // 2.2 Trigger incremental re-index
    crate::index::run_incremental(conn, project_root)?;

    // 2.3 Compute context budget and call get_context
    let ctx_budget = token_budget * 80 / 100;
    let context_output = crate::graph::query::get_context(conn, project_root, intent, &file_hints, ctx_budget)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    // 2.4 Build impact section
    let impact_output = if let Some(fqn) = symbol_fqn {
        crate::graph::query::get_impact(conn, fqn, depth)
            .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?
    } else {
        "No primary symbol specified — provide symbol_fqn for impact analysis.\n".to_string()
    };

    // 2.5 Assemble output (both sections already have #-level headings; use only --- as separator)
    let mut output = format!("{context_output}\n---\n{impact_output}");

    // 2.6 Hard-truncate to enforce token budget
    truncate_to_budget(&mut output, token_budget);

    // 2.7 Return
    Ok(output)
}

fn handle_trace_flow(conn: &rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let source_fqn = args.get("source_fqn").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: source_fqn".to_string()))?;
    let target_fqn = args.get("target_fqn").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: target_fqn".to_string()))?;
    let max_paths = args.get("max_paths").and_then(|v| v.as_u64())
        .map(|v| (v as usize).clamp(1, crate::graph::trace::MAX_PATHS_LIMIT))
        .unwrap_or(crate::graph::trace::MAX_PATHS_DEFAULT);

    let source_id: Option<i64> = conn.query_row(
        "SELECT id FROM symbols WHERE fqn = ?1",
        rusqlite::params![source_fqn],
        |r| r.get(0),
    ).optional().map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    let source_id = match source_id {
        Some(id) => id,
        None => return Ok(format!("Symbol not found: {source_fqn}\n\nRun 'olaf index' first.")),
    };

    let target_id: Option<i64> = conn.query_row(
        "SELECT id FROM symbols WHERE fqn = ?1",
        rusqlite::params![target_fqn],
        |r| r.get(0),
    ).optional().map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    let target_id = match target_id {
        Some(id) => id,
        None => return Ok(format!("Symbol not found: {target_fqn}\n\nRun 'olaf index' first.")),
    };

    let result = crate::graph::trace::trace_flow(conn, source_id, target_id, max_paths)
        .map_err(anyhow::Error::from)?;

    Ok(crate::graph::trace::format_trace_result(source_fqn, target_fqn, &result))
}

/// Truncates `s` so that `s.len().div_ceil(4) <= token_budget` after appending the note.
/// Finds the nearest valid UTF-8 char boundary to avoid panics on multibyte chars.
fn truncate_to_budget(s: &mut String, token_budget: usize) {
    const NOTE: &str = "\n(response truncated to fit token_budget)\n";
    let max_bytes = token_budget.saturating_mul(4);
    if s.len().div_ceil(4) <= token_budget {
        return;
    }
    // Reserve space for the note so the final output stays within budget.
    let cutoff = max_bytes.saturating_sub(NOTE.len());
    // Walk back to a valid UTF-8 char boundary (s.is_char_boundary(0) is always true).
    let boundary = (0..=cutoff).rev().find(|&i| s.is_char_boundary(i)).unwrap_or(0);
    s.truncate(boundary);
    s.push_str(NOTE);
    // Postcondition: s.len().div_ceil(4) <= token_budget (NOTE.len() reserved above).
}

/// Format a millisecond timestamp as a human-readable relative age string.
fn relative_age_ms(millis: u128, now_ms: u128) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_contains_trace_flow() {
        let tools = list();
        let matches: Vec<_> = tools.iter().filter(|t| t["name"] == "trace_flow").collect();
        assert_eq!(matches.len(), 1, "list() must contain exactly one trace_flow entry");
    }

    #[test]
    fn test_trace_flow_missing_source_fqn() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let args = serde_json::json!({ "target_fqn": "src/b.rs::bar" });
        let result = handle_trace_flow(&conn, Some(&args));
        assert!(matches!(result, Err(ToolError::InvalidParams(_))));
    }

    #[test]
    fn test_trace_flow_missing_target_fqn() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let args = serde_json::json!({ "source_fqn": "src/a.rs::foo" });
        let result = handle_trace_flow(&conn, Some(&args));
        assert!(matches!(result, Err(ToolError::InvalidParams(_))));
    }

    // 3.5 — list() contains exactly one entry with name "run_pipeline"
    #[test]
    fn test_list_contains_run_pipeline() {
        let tools = list();
        let matches: Vec<_> = tools.iter().filter(|t| t["name"] == "run_pipeline").collect();
        assert_eq!(matches.len(), 1, "list() must contain exactly one run_pipeline entry");
    }

    // 3.6 — truncation logic: 2000 ASCII chars (500 est-tokens), budget 100
    #[test]
    fn test_truncate_to_budget_basic() {
        let mut s = "a".repeat(2000);
        truncate_to_budget(&mut s, 100);
        assert!(s.len().div_ceil(4) <= 100, "truncated div_ceil(len/4)={} must be <= 100", s.len().div_ceil(4));
        assert!(
            s.ends_with("(response truncated to fit token_budget)\n"),
            "must end with truncation note; got: {:?}", &s[s.len().saturating_sub(50)..]
        );
    }

    // 3.7 — Unicode safety: string with multibyte chars, no panic
    #[test]
    fn test_truncate_to_budget_unicode_safety() {
        // "—" (U+2014 EM DASH) is 3 bytes in UTF-8
        let em_dash = "\u{2014}";
        // Build a string long enough to require truncation. Use budget=20 (max_bytes=80)
        // so the NOTE (42 bytes) fits. handle_run_pipeline always clamps to max(100) anyway.
        let mut s = em_dash.repeat(200); // 600 bytes = 150 est-tokens
        let budget = 20;
        truncate_to_budget(&mut s, budget);
        assert!(s.len().div_ceil(4) <= budget, "truncated div_ceil(len/4)={} must be <= {budget}", s.len().div_ceil(4));
    }
}
