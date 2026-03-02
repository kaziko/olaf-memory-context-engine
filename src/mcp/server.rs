// stdout-pure: no print!/println! ever in this module
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use serde_json::Value;

use crate::mcp::{
    protocol::Response,
    tools,
};

pub(crate) fn run(mut conn: rusqlite::Connection, project_root: PathBuf) -> anyhow::Result<()> {
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

        if let Some(response) = handle_message(&mut conn, &project_root, &line) {
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

fn handle_message(conn: &mut rusqlite::Connection, project_root: &std::path::Path, line: &str) -> Option<Response> {
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
    //   0. Require a JSON object — non-objects ([], 42, "str") → -32600, id: null.
    //   1. Detect notifications first: absent id AND valid string method → silent.
    //      Absent id WITHOUT a valid string method is an invalid request, not a notification.
    //   2. Validate id type before using id in any error response.
    //   3. Validate jsonrpc version.
    //   4. Validate method for requests with id.

    // Step 0: message must be a JSON object.
    let Some(obj) = value.as_object() else {
        return Some(Response::error(
            Value::Null,
            -32600,
            "Invalid Request: message must be a JSON object".to_string(),
        ));
    };

    // Step 1: notification detection.
    // A notification is defined by JSON-RPC 2.0 as a Request object with absent id AND
    // a valid string method. Messages with absent id but missing/invalid method are
    // invalid requests and must receive a -32600 response (not be silently dropped).
    let raw_id = obj.get("id").cloned();
    let method_str = obj.get("method").and_then(|m| m.as_str());

    if raw_id.is_none() {
        if let Some(method) = method_str {
            // Proper notification — no response, even if jsonrpc is wrong.
            log::debug!("notification received: {method}");
            return None;
        }
        // Absent id but no valid string method → invalid request.
        return Some(Response::error(
            Value::Null,
            -32600,
            "Invalid Request: method must be a string".to_string(),
        ));
    }
    let raw_id = raw_id.unwrap();

    // Step 2: validate id type (JSON-RPC 2.0 §5: null, number, or string only).
    if !matches!(raw_id, Value::Null | Value::Number(_) | Value::String(_)) {
        return Some(Response::error(
            Value::Null,
            -32600,
            "Invalid Request: id must be null, number, or string".to_string(),
        ));
    }
    let id = raw_id; // validated — safe to echo in all subsequent error responses

    // Step 3: validate jsonrpc version.
    let jsonrpc = obj.get("jsonrpc").and_then(|v| v.as_str()).unwrap_or("");
    if jsonrpc != "2.0" {
        return Some(Response::error(
            id,
            -32600,
            format!("Invalid Request: unsupported jsonrpc version '{jsonrpc}'"),
        ));
    }

    // Step 4: validate method for requests (id present).
    let method = match method_str {
        Some(m) => m,
        None => {
            return Some(Response::error(
                id,
                -32600,
                "Invalid Request: method must be a string".to_string(),
            ));
        }
    };

    Some(dispatch_request(conn, project_root, id, method, obj.get("params")))
}

fn dispatch_request(
    conn: &mut rusqlite::Connection,
    project_root: &std::path::Path,
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

        "tools/call" => match tools::dispatch(conn, project_root, params) {
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
