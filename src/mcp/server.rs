// stdout-pure: no print!/println! ever in this module
use std::io::{BufRead, BufReader, Write};
use serde_json::Value;

use crate::mcp::{
    protocol::Response,
    tools,
};
use crate::activity::MonitorGuard;
use crate::workspace::Workspace;

/// MCP server instructions — injected into the initialize response so the LLM
/// knows when to prefer Olaf tools over native Read/Grep/Glob.
/// Keep under 800 tokens (~600 words) to avoid context bloat.
const SERVER_INSTRUCTIONS: &str = "\
Olaf is a codebase context engine that saves ~68% of exploration tokens by returning \
pre-indexed, token-budgeted results instead of raw file reads.

## When to use Olaf vs native tools

Decision tree — pick the FIRST matching rule:

1. **About to read or edit a file over 200 lines?** → `get_file_skeleton` first \
(returns signatures, docstrings, and dependency edges — 90%+ fewer tokens than reading the full file)
2. **Exploring unfamiliar code?** → `get_brief` (replaces 3-5 Grep+Read calls \
with a single token-budgeted context brief that auto-reindexes)
3. **Tracing how code connects across files?** → `trace_flow` (finds execution paths in the call graph \
instead of manual file-by-file reading)
4. **Analyzing who depends on a symbol?** → `get_impact` (traverses calls/extends/implements edges)
5. **Debugging a test failure or runtime error?** → `analyze_failure` (extracts file paths and symbols \
from stack traces, returns a focused context brief)
6. **Reading a small known file (<200 lines) for a targeted edit?** → native `Read` is fine
7. **Searching for a keyword in 1-2 files already in context?** → native `Grep` is fine
8. **Editing or writing files?** → always use native `Edit`/`Write` (Olaf is read-only)

## Key insight

One Olaf call replaces multiple native Read+Grep calls. For exploration and pre-edit understanding, \
Olaf tools MUST be preferred — they return only what matters within a token budget, while native reads \
return entire files including irrelevant sections.

## What Olaf does NOT do

Olaf is read-only. Always use Edit/Write for modifications, Bash for commands. \
Individual tool descriptions in tools/list provide parameter details — this overview covers \
tool selection strategy only.\
";

pub(crate) fn run(mut workspace: Workspace, session_id: String, mut monitor: MonitorGuard) -> anyhow::Result<()> {
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

        if let Some(response) = handle_message(&mut workspace, &session_id, &line, &mut monitor) {
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

fn handle_message(workspace: &mut Workspace, session_id: &str, line: &str, monitor: &mut MonitorGuard) -> Option<Response> {
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

    Some(dispatch_request(workspace, session_id, id, method, obj.get("params"), monitor))
}

fn dispatch_request(
    workspace: &mut Workspace,
    session_id: &str,
    id: Value,
    method: &str,
    params: Option<&Value>,
    monitor: &mut MonitorGuard,
) -> Response {
    match method {
        "initialize" => {
            let result = serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "olaf",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "instructions": SERVER_INSTRUCTIONS
            });
            Response::ok(id, result)
        }

        "tools/list" => {
            Response::ok(id, serde_json::json!({ "tools": tools::list() }))
        }

        "tools/call" => {
            // Extract tool name and args for activity monitoring
            let tool_name = params
                .and_then(|p| p.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let tool_args = params.and_then(|p| p.get("arguments")).cloned();

            let start = std::time::Instant::now();
            let result = tools::dispatch(workspace, session_id, params);
            let elapsed = start.elapsed().as_millis() as u64;

            // Emit activity event
            let (is_error, error_message, result_len) = match &result {
                Ok(text) => (false, None, Some(text.len())),
                Err(e) => (true, Some(crate::activity::sanitize_error(&e.to_string(), 200)), None),
            };
            monitor.emit(crate::activity::ActivityEvent {
                source: "mcp",
                session_id: Some(session_id.to_string()),
                event_type: "tool_call",
                tool_name: Some(tool_name.clone()),
                summary: crate::activity::summarize_tool_call(
                    &tool_name,
                    tool_args.as_ref(),
                    result_len,
                ),
                duration_ms: Some(elapsed),
                is_error,
                error_message,
            });

            match result {
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
            }
        }

        _ => Response::error(id, -32601, format!("method not found: {method}")),
    }
}
