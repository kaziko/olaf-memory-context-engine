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
        "save_observation" => handle_save_observation(conn, session_id, args),
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
