use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};

fn spawn_server() -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null()) // suppress server diagnostics in test output
        .spawn()
        .expect("failed to spawn olaf serve")
}

/// Write requests (each as a newline-terminated JSON string), close stdin,
/// wait for process exit, collect and parse all stdout lines.
/// Asserts that the process exits successfully.
fn run_requests(requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut child = spawn_server();

    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        for req in requests {
            writeln!(w, "{}", serde_json::to_string(req).unwrap()).unwrap();
        }
        // stdin dropped here → EOF → server exits cleanly
    }

    let output = child.wait_with_output().expect("server process did not exit");
    assert!(output.status.success(), "server exited with non-zero status: {:?}", output.status);

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("non-JSON on stdout: {e}\nLine: {l:?}")))
        .collect()
}

#[test]
fn test_initialize_handshake() {
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "0.1.0" }
        }
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["jsonrpc"], "2.0");
    assert_eq!(r["id"], 1);
    assert!(r["result"].is_object(), "must have result");
    assert_eq!(r["result"]["protocolVersion"], "2024-11-05");
    assert!(r["result"]["capabilities"].is_object());
    assert!(r["result"]["serverInfo"]["name"].is_string());
    assert!(r["result"]["serverInfo"]["version"].is_string());
    // Error field must be absent
    assert!(r.get("error").is_none() || r["error"].is_null());
}

#[test]
fn test_notification_produces_no_output() {
    // notifications/initialized has no id — server must produce ZERO bytes on stdout
    let notification = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
        // no "id" field
    });

    let mut child = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        writeln!(w, "{}", serde_json::to_string(&notification).unwrap()).unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "server must exit successfully after notification");
    // Check raw bytes — even a stray newline byte must not appear
    assert_eq!(
        output.stdout.len(), 0,
        "notifications must produce zero bytes on stdout; got {} bytes: {:?}",
        output.stdout.len(), output.stdout
    );
}

#[test]
fn test_tools_list_empty() {
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["id"], 2);
    assert!(r["result"]["tools"].is_array(), "tools must be array");
    assert_eq!(r["result"]["tools"].as_array().unwrap().len(), 0, "tools must be empty in Story 2.1");
}

#[test]
fn test_unknown_method_returns_32601() {
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "nonexistent/method",
        "params": {}
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["id"], 3);
    assert_eq!(r["error"]["code"], -32601, "must be method-not-found error");
    assert!(r.get("result").is_none() || r["result"].is_null(), "must not have result");
}

#[test]
fn test_unknown_tool_returns_32601() {
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": { "name": "nonexistent_tool", "arguments": {} }
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["id"], 4);
    assert_eq!(r["error"]["code"], -32601);
    assert!(r["error"]["message"].as_str().unwrap_or("").contains("nonexistent_tool"));
}

#[test]
fn test_invalid_params_returns_32602() {
    // tools/call without the required "name" field → -32602 InvalidParams
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 5,
        "method": "tools/call",
        "params": { "arguments": {} }  // missing "name"
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["id"], 5);
    assert_eq!(r["error"]["code"], -32602, "missing name field must return -32602");
}

#[test]
fn test_parse_error_returns_32700_with_null_id() {
    // Malformed JSON → -32700 with id: null (JSON-RPC 2.0 §5 requirement)
    let mut child = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        writeln!(w, "{{not valid json: {{{{").unwrap();
    }

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "server must exit successfully after parse error");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "parse error must produce exactly one response line");

    let r: serde_json::Value = serde_json::from_str(lines[0]).expect("response must be valid JSON");
    assert_eq!(r["error"]["code"], -32700);
    // JSON-RPC 2.0 §5: id MUST be null when id could not be determined
    assert!(r["id"].is_null(), "parse error response id must be null, got: {}", r["id"]);
}

#[test]
fn test_server_survives_error() {
    // First request: bad method → error
    // Second request: valid tools/list → success
    // Server must process BOTH without crashing
    let requests = vec![
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "does_not_exist"
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/list",
            "params": {}
        }),
    ];

    let responses = run_requests(&requests);
    assert_eq!(responses.len(), 2, "server must respond to both requests");

    assert_eq!(responses[0]["id"], 10);
    assert_eq!(responses[0]["error"]["code"], -32601, "first must be method-not-found");

    assert_eq!(responses[1]["id"], 11);
    assert!(responses[1]["result"].is_object(), "second must succeed after error");
}

#[test]
fn test_stdout_purity() {
    // Every byte on stdout must be part of valid JSON-RPC objects with jsonrpc: "2.0"
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 99,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "purity-test", "version": "0.0.1" }
        }
    });

    let mut child = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        writeln!(w, "{}", serde_json::to_string(&req).unwrap()).unwrap();
    }

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    for line in stdout.lines() {
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("non-JSON on stdout: {e}\nOffending line: {line:?}"));
        assert_eq!(parsed["jsonrpc"], "2.0", "every response must have jsonrpc: 2.0");
    }
}

#[test]
fn test_null_id_is_request_not_notification() {
    // {"id": null} is an explicit null id — a Request, not a notification.
    // The server must respond (with id: null), not stay silent.
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": null,
        "method": "tools/list",
        "params": {}
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1, "explicit id:null must produce a response, not silence");

    let r = &responses[0];
    assert!(r["id"].is_null(), "response id must be null");
    assert!(r["result"]["tools"].is_array());
}

#[test]
fn test_invalid_jsonrpc_version_returns_32600() {
    // jsonrpc:"1.0" must be rejected with -32600 Invalid Request
    let req = serde_json::json!({
        "jsonrpc": "1.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["id"], 1, "id must be echoed back");
    assert_eq!(r["error"]["code"], -32600, "wrong jsonrpc version must return -32600");
}

#[test]
fn test_invalid_id_type_returns_32600() {
    // id: true (boolean) is not a valid JSON-RPC 2.0 id type — must return -32600
    let mut child = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        // Write raw JSON so we can send a boolean id
        writeln!(w, r#"{{"jsonrpc":"2.0","id":true,"method":"tools/list","params":{{}}}}"#).unwrap();
    }

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "server must exit successfully");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1);

    let r: serde_json::Value = serde_json::from_str(lines[0]).expect("must be valid JSON");
    assert_eq!(r["error"]["code"], -32600, "boolean id must return -32600");
    assert!(r["id"].is_null(), "response id must be null for invalid id type");
}

#[test]
fn test_valid_json_missing_method_returns_32600() {
    // Valid JSON but missing "method" — structural error must be -32600, not -32700 (P1)
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 7
        // "method" intentionally absent
    });

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert_eq!(r["id"], 7, "id must be echoed back");
    assert_eq!(
        r["error"]["code"], -32600,
        "missing method must return -32600 Invalid Request, not -32700 Parse Error"
    );
}

#[test]
fn test_notification_with_invalid_jsonrpc_produces_no_output() {
    // No id field → notification, so server must stay silent even though jsonrpc is wrong (P2b)
    let mut child = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        writeln!(w, r#"{{"jsonrpc":"1.0","method":"notifications/initialized"}}"#).unwrap();
    }

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "server must exit successfully");
    assert_eq!(
        output.stdout.len(), 0,
        "notification must produce zero output even with invalid jsonrpc"
    );
}
