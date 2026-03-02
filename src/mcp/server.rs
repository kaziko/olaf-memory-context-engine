// stdout-pure: no print!/println! ever in this module
use std::io::{BufRead, BufReader, Write};
use serde_json::Value;

use crate::mcp::{
    protocol::{Request, Response},
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
    // Parse JSON — malformed input gets -32700 with id: null (JSON-RPC 2.0 §5)
    let req: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("JSON parse error: {e}");
            return Some(Response::error(Value::Null, -32700, format!("Parse error: {e}")));
        }
    };

    // Notifications have no id — process silently, no response
    let Some(id) = req.id.clone() else {
        log::debug!("notification received: {}", req.method);
        return None;
    };

    Some(dispatch_request(id, &req.method, req.params.as_ref()))
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
