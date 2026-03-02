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
    ]
}

/// Dispatches a tools/call request to the appropriate handler.
/// Returns the tool's text response (server.rs wraps it in MCP content format).
pub(crate) fn dispatch(conn: &mut rusqlite::Connection, project_root: &Path, params: Option<&Value>) -> Result<String, ToolError> {
    let tool_name = params
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: name".to_string()))?;

    let args = params.and_then(|p| p.get("arguments"));
    match tool_name {
        "get_context" => handle_get_context(conn, project_root, args),
        "get_impact"  => handle_get_impact(conn, args),
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
