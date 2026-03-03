use std::path::Path;
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
        _ => Err(ToolError::UnknownTool(tool_name.to_string())),
    }
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
