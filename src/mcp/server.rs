// stdout-pure: no print!/println! ever in this module
use std::io::{BufRead, BufReader, Write};
use serde_json::Value;

use crate::mcp::{
    protocol::Response,
    tools,
};

pub(crate) fn run() -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    // Lock both for the duration — avoids repeated locking overhead in the hot loop
    let reader = BufReader::new(stdin.lock());
    let mut out = stdout.lock();

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                log::error!("stdin read error: {e}");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        if let Some(response) = handle_message(&line) {
            let json = match serde_json::to_string(&response) {
                Ok(j) => j,
                Err(e) => {
                    log::error!("response serialization failed: {e}");
                    continue;
                }
            };
            // writeln! + flush immediately — NEVER println!, NEVER defer flush
            writeln!(out, "{json}")?;
            out.flush()?;
        }
    }

    Ok(())
}

fn handle_message(line: &str) -> Option<Response> {
    // Stage 1: parse as raw JSON value.
    // True parse failures (malformed JSON) → -32700, id: null.
    let value: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("JSON parse error: {e}");
            return Some(Response::error(Value::Null, -32700, format!("Parse error: {e}")));
        }
    };

    // Stage 2: validate structure manually so structural errors → -32600 (not -32700).
    //
    // Order matters for correctness:
    //   1. Detect notifications (absent id) first — notifications never get responses,
    //      even if jsonrpc or method are invalid (P2b fix).
    //   2. Validate id type before using id in any error response (P2a fix).
    //   3. Validate jsonrpc version.
    //   4. Validate method — valid JSON with missing/non-string method → -32600 (P1 fix).

    // Step 1: id — absent → notification (no response); present → request.
    let Some(raw_id) = value.get("id").cloned() else {
        let method = value.get("method").and_then(|m| m.as_str()).unwrap_or("<unknown>");
        log::debug!("notification received: {method}");
        return None;
    };

    // Step 2: validate id type (JSON-RPC 2.0 §5: null, number, or string only).
    // Invalid types (bool, array, object) → normalize response id to null.
    if !matches!(raw_id, Value::Null | Value::Number(_) | Value::String(_)) {
        return Some(Response::error(
            Value::Null,
            -32600,
            "Invalid Request: id must be null, number, or string".to_string(),
        ));
    }
    let id = raw_id; // validated — safe to echo in all subsequent error responses

    // Step 3: validate jsonrpc version.
    let jsonrpc = value.get("jsonrpc").and_then(|v| v.as_str()).unwrap_or("");
    if jsonrpc != "2.0" {
        return Some(Response::error(
            id,
            -32600,
            format!("Invalid Request: unsupported jsonrpc version '{jsonrpc}'"),
        ));
    }

    // Step 4: validate method — must be a present string.
    let method = match value.get("method").and_then(|m| m.as_str()) {
        Some(m) => m,
        None => {
            return Some(Response::error(
                id,
                -32600,
                "Invalid Request: method must be a string".to_string(),
            ));
        }
    };

    Some(dispatch_request(id, method, value.get("params")))
}

fn dispatch_request(
    id: Value,
    method: &str,
    params: Option<&Value>,
) -> Response {
    match method {
        "initialize" => {
            let result = serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "olaf",
                    "version": env!("CARGO_PKG_VERSION")
                }
            });
            Response::ok(id, result)
        }

        "tools/list" => {
            Response::ok(id, serde_json::json!({ "tools": tools::list() }))
        }

        "tools/call" => match tools::dispatch(params) {
            Ok(text) => Response::ok(
                id,
                serde_json::json!({ "content": [{ "type": "text", "text": text }] }),
            ),
            Err(tools::ToolError::UnknownTool(name)) => {
                Response::error(id, -32601, format!("unknown tool: {name}"))
            }
            Err(tools::ToolError::InvalidParams(msg)) => {
                Response::error(id, -32602, msg)
            }
            Err(tools::ToolError::Internal(e)) => {
                Response::error(id, -32603, e.to_string())
            }
        },

        _ => Response::error(id, -32601, format!("method not found: {method}")),
    }
}
