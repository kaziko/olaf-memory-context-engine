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
/// Stories 2.2 and 2.3 push tool definitions into this vec.
pub(crate) fn list() -> Vec<Value> {
    // Tools added in Stories 2.2 (get_context, get_impact) and 2.3 (get_file_skeleton, index_status)
    vec![]
}

/// Dispatches a tools/call request to the appropriate handler.
/// Returns the tool's text response (server.rs wraps it in MCP content format).
pub(crate) fn dispatch(params: Option<&Value>) -> Result<String, ToolError> {
    let tool_name = params
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: name".to_string()))?;

    // Stories 2.2 and 2.3 will add match arms here; suppress single-binding lint
    #[allow(clippy::match_single_binding)]
    match tool_name {
        _ => Err(ToolError::UnknownTool(tool_name.to_string())),
    }
}
