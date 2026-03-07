use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};

fn spawn_server() -> (std::process::Child, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().expect("tempdir creation failed");
    let child = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .current_dir(tmpdir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null()) // suppress server diagnostics in test output
        .spawn()
        .expect("failed to spawn olaf serve");
    (child, tmpdir)
}

/// Write requests (each as a newline-terminated JSON string), close stdin,
/// wait for process exit, collect and parse all stdout lines.
/// Asserts that the process exits successfully.
fn run_requests(requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let (mut child, _tmpdir) = spawn_server();

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

    let (mut child, _tmpdir) = spawn_server();
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
    // This test was written for Story 2.1; after Story 2.2 the list is non-empty.
    // We keep the test but relax the assertion to just verify it's an array.
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
    let (mut child, _tmpdir) = spawn_server();
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

    let (mut child, _tmpdir) = spawn_server();
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
    let (mut child, _tmpdir) = spawn_server();
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
    let (mut child, _tmpdir) = spawn_server();
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

#[test]
fn test_object_without_method_returns_32600() {
    // {"jsonrpc":"2.0"} — valid JSON object, no id, no method — invalid request not notification
    let req = serde_json::json!({"jsonrpc": "2.0"});

    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);

    let r = &responses[0];
    assert!(r["id"].is_null(), "id must be null when request had no id");
    assert_eq!(r["error"]["code"], -32600, "absent id + missing method must be -32600, not silence");
}

#[test]
fn test_object_with_non_string_method_and_no_id_returns_32600() {
    // {"jsonrpc":"2.0","method":123} — non-string method, no id — invalid request not notification
    let (mut child, _tmpdir) = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        writeln!(w, r#"{{"jsonrpc":"2.0","method":123}}"#).unwrap();
    }

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "must produce exactly one error response");

    let r: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert!(r["id"].is_null());
    assert_eq!(r["error"]["code"], -32600);
}

#[test]
fn test_non_object_payload_returns_32600() {
    // [] and 42 are valid JSON but not JSON objects — must return -32600, not silence
    let responses = run_requests(&[serde_json::json!([]), serde_json::json!(42)]);
    assert_eq!(responses.len(), 2, "both non-object payloads must produce error responses");

    for r in &responses {
        assert!(r["id"].is_null());
        assert_eq!(r["error"]["code"], -32600);
    }
}

// ─── Indexed DB helpers ───────────────────────────────────────────────────────

/// Creates a TempDir with a small Rust project and runs `olaf index`.
/// Returns the TempDir — caller must keep it alive for the duration of tests using it.
fn prepare_indexed_tmpdir() -> tempfile::TempDir {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).expect("create src/");
    std::fs::write(
        src_dir.join("lib.rs"),
        "/// Adds two numbers.\npub fn add(a: i32, b: i32) -> i32 { a + b }\n\
         /// A counter.\npub struct Counter { count: u32 }\n",
    )
    .expect("write fixture");

    let status = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .output()
        .expect("olaf index failed to run");
    assert!(
        status.status.success(),
        "olaf index must succeed; stderr: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    tmpdir
}

/// Creates a TempDir with two `lib.rs` files at different paths, so a suffix query for `lib.rs`
/// returns multiple candidates.
fn prepare_indexed_tmpdir_multi_lib() -> tempfile::TempDir {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let src_dir = tmpdir.path().join("src");
    let util_dir = tmpdir.path().join("src").join("util");
    std::fs::create_dir_all(&src_dir).expect("create src/");
    std::fs::create_dir_all(&util_dir).expect("create src/util/");
    std::fs::write(src_dir.join("lib.rs"), "/// Root.\npub fn root() {}\n").expect("write src/lib.rs");
    std::fs::write(util_dir.join("lib.rs"), "/// Util.\npub fn util_fn() {}\n").expect("write src/util/lib.rs");

    let status = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .output()
        .expect("olaf index failed to run");
    assert!(
        status.status.success(),
        "olaf index must succeed; stderr: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    tmpdir
}

/// Creates a TempDir with a Rust file that has no parseable symbols (comment only).
fn prepare_indexed_tmpdir_no_symbols() -> tempfile::TempDir {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).expect("create src/");
    std::fs::write(src_dir.join("empty.rs"), "// No symbols here.\n").expect("write src/empty.rs");

    let status = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .output()
        .expect("olaf index failed to run");
    assert!(
        status.status.success(),
        "olaf index must succeed; stderr: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    tmpdir
}

/// Spawns the server with the given working directory, sends requests, collects responses.
fn run_requests_in(dir: &std::path::Path, requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn olaf serve");
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        for req in requests {
            writeln!(w, "{}", serde_json::to_string(req).unwrap()).unwrap();
        }
    }
    let output = child.wait_with_output().expect("server did not exit");
    assert!(output.status.success(), "server exited non-zero: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("non-JSON: {e}\nLine: {l:?}")))
        .collect()
}

// ─── Story 2.2 Tests ──────────────────────────────────────────────────────────

#[test]
fn test_tools_list_includes_context_and_impact() {
    let req = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);
    let tools = responses[0]["result"]["tools"].as_array().expect("tools must be array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    // Do NOT assert exact count — Story 2.3 adds more tools
    assert!(names.contains(&"get_context"), "must include get_context");
    assert!(names.contains(&"get_impact"), "must include get_impact");
}

#[test]
fn test_get_context_empty_db() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":2,"method":"tools/call",
        "params":{"name":"get_context","arguments":{"intent":"test intent"}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);
    let r = &responses[0];
    assert_eq!(r["id"], 2);
    // Must succeed (result with content), not return -32603 internal error
    assert!(r["result"].is_object(), "get_context must return result, not error; got: {}", r);
    assert!(r["result"]["content"].is_array(), "result must have content array");
}

#[test]
fn test_get_context_response_header_format() {
    // Verify the intent metadata header fields introduced in Story 6.1b are present and well-formed.
    // Intent signals come from the prompt text, not DB contents, so "fix the crash" produces
    // a high-confidence bugfix profile regardless of whether any symbols are indexed.
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":20,"method":"tools/call",
        "params":{"name":"get_context","arguments":{"intent":"fix the crash"}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result; got: {}", r);
    let content = r["result"]["content"].as_array().expect("content must be array");
    assert!(!content.is_empty(), "content must not be empty");
    let text = content[0]["text"].as_str().expect("first content item must have text");
    assert!(text.contains("intent_mode:"), "header must contain intent_mode");
    assert!(text.contains("intent_confidence:"), "header must contain intent_confidence");
    assert!(text.contains("intent_signals:"), "header must contain intent_signals");
}

#[test]
fn test_get_context_missing_intent_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":3,"method":"tools/call",
        "params":{"name":"get_context","arguments":{"file_hints":["src/main.rs"]}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing intent must return -32602");
}

#[test]
fn test_get_impact_empty_db() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":4,"method":"tools/call",
        "params":{"name":"get_impact","arguments":{"symbol_fqn":"src/main.rs::some_fn"}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);
    let r = &responses[0];
    assert_eq!(r["id"], 4);
    // Returns "not found" text response, NOT a JSON-RPC error
    assert!(r["result"].is_object(), "get_impact must return result, not error; got: {}", r);
}

#[test]
fn test_get_impact_missing_fqn_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":5,"method":"tools/call",
        "params":{"name":"get_impact","arguments":{}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing symbol_fqn must return -32602");
}

#[test]
fn test_get_impact_depth_parameter_accepted() {
    // Verify depth=1 and depth=3 are both accepted (not -32602) and return success responses.
    // Content difference is not testable with an empty DB — both return "Symbol not found" text.
    // This test confirms the depth parameter is parsed correctly without error.
    let depth1 = serde_json::json!({
        "jsonrpc":"2.0","id":6,"method":"tools/call",
        "params":{"name":"get_impact","arguments":{"symbol_fqn":"src/lib.rs::some_fn","depth":1}}
    });
    let depth3 = serde_json::json!({
        "jsonrpc":"2.0","id":7,"method":"tools/call",
        "params":{"name":"get_impact","arguments":{"symbol_fqn":"src/lib.rs::some_fn","depth":3}}
    });
    let responses = run_requests(&[depth1, depth3]);
    assert_eq!(responses.len(), 2, "both requests must produce responses");
    for (i, r) in responses.iter().enumerate() {
        assert!(r["result"].is_object(), "depth={} request must return result, not error; got: {}", i+1, r);
    }
}

// ─── Story 2.3 Tests ──────────────────────────────────────────────────────────

#[test]
fn test_tools_list_includes_file_skeleton_and_index_status() {
    let req = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let responses = run_requests(&[req]);
    let tools = responses[0]["result"]["tools"].as_array().expect("tools must be array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"get_file_skeleton"), "must include get_file_skeleton");
    assert!(names.contains(&"index_status"), "must include index_status");
}

#[test]
fn test_get_file_skeleton_missing_path_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":10,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing file_path must return -32602");
}

#[test]
fn test_get_file_skeleton_empty_path_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":11,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{"file_path":"   "}}
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "blank file_path must return -32602");
}

#[test]
fn test_get_file_skeleton_sensitive_file_blocked() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":12,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{"file_path":".env"}}
    });
    let responses = run_requests(&[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result, not error; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("not permitted"), "must indicate sensitive file blocked; got: {text}");
}

#[test]
fn test_get_file_skeleton_not_found_empty_db() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":13,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{"file_path":"src/main.rs"}}
    });
    let responses = run_requests(&[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result, not error; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(
        text.contains("No file found matching"),
        "must include informative not-found message; got: {text}"
    );
}

#[test]
fn test_get_file_skeleton_with_indexed_file() {
    let tmpdir = prepare_indexed_tmpdir();
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":14,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{"file_path":"src/lib.rs"}}
    });
    let responses = run_requests_in(tmpdir.path(), &[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("add"), "must include function name 'add'; got: {text}");
    assert!(!text.contains("a + b"), "must not include implementation body; got: {text}");
}

#[test]
fn test_index_status_empty_db() {
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":15,"method":"tools/call",
        "params":{"name":"index_status","arguments":{}}
    });
    let responses = run_requests(&[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("not initialized"), "empty DB must say not initialized; got: {text}");
}

#[test]
fn test_index_status_after_indexing() {
    let tmpdir = prepare_indexed_tmpdir();
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":16,"method":"tools/call",
        "params":{"name":"index_status","arguments":{}}
    });
    let responses = run_requests_in(tmpdir.path(), &[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("Files indexed:"), "must include file count; got: {text}");
    assert!(!text.contains("not initialized"), "indexed DB must not say not initialized; got: {text}");
}

#[test]
fn test_get_file_skeleton_disambiguation_returns_multiple_matches() {
    let tmpdir = prepare_indexed_tmpdir_multi_lib();
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":17,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{"file_path":"lib.rs"}}
    });
    let responses = run_requests_in(tmpdir.path(), &[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result, not error; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(
        text.contains("Multiple files match"),
        "must return disambiguation message; got: {text}"
    );
    assert!(
        text.contains("lib.rs"),
        "disambiguation list must include matching paths; got: {text}"
    );
}

#[test]
fn test_get_file_skeleton_zero_symbols_returns_informative_message() {
    let tmpdir = prepare_indexed_tmpdir_no_symbols();
    let req = serde_json::json!({
        "jsonrpc":"2.0","id":18,"method":"tools/call",
        "params":{"name":"get_file_skeleton","arguments":{"file_path":"src/empty.rs"}}
    });
    let responses = run_requests_in(tmpdir.path(), &[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result, not error; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    // Either the file was indexed with zero symbols, or it wasn't indexed at all.
    // Both are valid outcomes — what must NOT happen is a crash or -32603 error.
    assert!(
        text.contains("No symbols found in file") || text.contains("No file found matching"),
        "must return informative message; got: {text}"
    );
}

// ─── Story 6.3 Tests ──────────────────────────────────────────────────────────

// 3.1 — happy-path sections: context header, separator, no-symbol fallback, no error
#[test]
fn test_get_brief_sections_present() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "get_brief", "arguments": { "intent": "test" } }
    });
    let responses = run_requests(&[req]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("# Context Brief:"), "missing context header");
    assert!(text.contains("---"), "missing section separator");
    assert!(text.contains("No primary symbol specified"), "missing no-symbol message");
}

// 3.2 — AC2 empty index graceful: no error, separator present, no-symbol fallback present
#[test]
fn test_get_brief_empty_index_no_error() {
    // run_requests uses an empty tmpdir — no index, no sessions
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": { "name": "get_brief", "arguments": { "intent": "test" } }
        // no symbol_fqn
    });
    let responses = run_requests(&[req]);
    assert!(responses[0].get("error").is_none(), "must not have error on empty index; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    // Do NOT assert "## Impact Analysis" — that heading does not exist in the output format
    assert!(text.contains("---"), "separator must be present");
    assert!(text.contains("No primary symbol specified"), "fallback message must be present");
}

// 3.3 — AC3 budget compliance: token_budget=200, output must not exceed it
#[test]
fn test_get_brief_respects_token_budget() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": { "name": "get_brief", "arguments": { "intent": "test", "token_budget": 200 } }
    });
    let responses = run_requests(&[req]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.len() / 4 <= 200,
        "token budget exceeded: {} chars ({} est tokens)",
        text.len(), text.len() / 4
    );
}

// 3.4b — symbol_fqn branch: passes fqn+depth, get_impact path exercised, no error returned
#[test]
fn test_get_brief_with_symbol_fqn() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 10, "method": "tools/call",
        "params": {
            "name": "get_brief",
            "arguments": {
                "intent": "test",
                "symbol_fqn": "src/lib.rs::some_fn",
                "depth": 2
            }
        }
    });
    let responses = run_requests(&[req]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("# Context Brief:"), "missing context header");
    assert!(text.contains("---"), "missing section separator");
    // get_impact on empty DB returns "Symbol not found" — no "No primary symbol specified" fallback
    assert!(!text.contains("No primary symbol specified"), "symbol_fqn was provided, fallback must not appear");
}

// 3.4 — tools/list includes get_brief as first tool (AC: #2, #3)
#[test]
fn test_tools_list_includes_get_brief() {
    let req = serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}});
    let responses = run_requests(&[req]);
    let tools = responses[0]["result"]["tools"].as_array().expect("tools must be array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"get_brief"), "tools/list must include get_brief");
    assert_eq!(tools[0]["name"], "get_brief", "get_brief must be the first tool in tools/list response");
}

// 5.2 — regression: run_pipeline returns -32601 UnknownTool after rename (AC: #2)
#[test]
fn test_run_pipeline_returns_unknown_tool_error() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 99, "method": "tools/call",
        "params": { "name": "run_pipeline", "arguments": { "intent": "test" } }
    });
    let responses = run_requests(&[req]);
    let error = responses[0].get("error").expect("run_pipeline must return an error after rename");
    assert_eq!(
        error["code"].as_i64().unwrap_or(0),
        -32601,
        "error code must be -32601 (method not found / unknown tool); got: {}",
        error
    );
    let msg = error["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("run_pipeline"),
        "error message must mention the tool name 'run_pipeline'; got: {msg}"
    );
}

// ─── Story 7.2 Tests ──────────────────────────────────────────────────────────

#[test]
fn test_trace_flow_missing_source_fqn_returns_32602() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "target_fqn": "src/b.rs::bar"
        }}
    })]);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing source_fqn must return -32602");
}

#[test]
fn test_trace_flow_symbol_not_found_empty_db() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "src/a.rs::foo",
            "target_fqn": "src/b.rs::bar"
        }}
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Symbol not found"), "expected not-found message, got: {text}");
}

#[test]
fn test_tools_list_includes_trace_flow() {
    let req = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let responses = run_requests(&[req]);
    let tools = responses[0]["result"]["tools"].as_array().expect("tools must be array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"trace_flow"), "tools/list must include trace_flow");
}

#[test]
fn test_trace_flow_returns_path_for_connected_symbols() {
    // Create a TypeScript file where main() calls helper() — produces a calls edge.
    let tmpdir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        tmpdir.path().join("app.ts"),
        "export function helper() {}\nexport function main() { helper(); }",
    ).expect("write app.ts");

    let status = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .output()
        .expect("olaf index failed to run");
    assert!(status.status.success(), "olaf index must succeed; stderr: {}",
        String::from_utf8_lossy(&status.stderr));

    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::main",
            "target_fqn": "app.ts::helper"
        }}
    })]);

    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("path(s) found"), "must show found path(s); got: {text}");
    assert!(text.contains("app.ts::main"), "path must include source; got: {text}");
    assert!(text.contains("app.ts::helper"), "path must include target; got: {text}");
}

// ─── Story 7.3 Tests ──────────────────────────────────────────────────────────

/// Creates a TempDir with the given TypeScript source written to `app.ts`,
/// runs `olaf index` in that dir, and returns the TempDir.
/// Caller must keep the return value alive for the duration of the test.
fn prepare_ts_tmpdir(source: &str) -> tempfile::TempDir {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmpdir.path().join("app.ts"), source).expect("write app.ts");
    let out = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .output()
        .expect("olaf index");
    assert!(out.status.success(), "olaf index failed: {}", String::from_utf8_lossy(&out.stderr));
    tmpdir
}

// Task 2 — AC1: missing target_fqn returns -32602
#[test]
fn test_trace_flow_missing_target_fqn_returns_32602() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "src/a.rs::foo"
            // target_fqn intentionally absent
        }}
    })]);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing target_fqn must return -32602");
}

// Task 3 — AC2: source_fqn == target_fqn returns identity path with exactly one node
#[test]
fn test_trace_flow_source_equals_target() {
    let tmpdir = prepare_indexed_tmpdir();
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "src/lib.rs::add",
            "target_fqn": "src/lib.rs::add"
        }}
    })]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("1 path(s) found"), "must report 1 path; got: {text}");
    let path_sec = &text[text.find("### Path 1 (0 hops)").expect("must have 0-hop header")..];
    assert_eq!(
        path_sec.matches("\n- `").count(), 1,
        "identity path must list exactly one node; got path section: {path_sec}"
    );
}

// Task 4 — AC3: disconnected symbols return "No execution path found"
#[test]
fn test_trace_flow_no_path_between_symbols() {
    let tmpdir = prepare_ts_tmpdir("export function alpha() {}\nexport function beta() {}");
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::alpha",
            "target_fqn": "app.ts::beta"
        }}
    })]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("No execution path found"), "must report no path; got: {text}");
}

// Task 5 — AC4: two-hop path with ordered node assertions
#[test]
fn test_trace_flow_two_hop_path() {
    let tmpdir = prepare_ts_tmpdir(
        "export function c() {}\nexport function b() { c(); }\nexport function a() { b(); }"
    );
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::a",
            "target_fqn": "app.ts::c"
        }}
    })]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("1 path(s) found"), "must find 1 path; got: {text}");
    assert!(text.contains("2 hop"), "must show 2 hops; got: {text}");
    // Slice from "### Path 1" to avoid false matches in the header line
    let path_sec = &text[text.find("### Path 1").expect("path section")..];
    let pa = path_sec.find("app.ts::a").expect("a in path");
    let pb = path_sec.find("app.ts::b").expect("b in path");
    let pc = path_sec.find("app.ts::c").expect("c in path");
    assert!(pa < pb, "a must precede b in path listing");
    assert!(pb < pc, "b must precede c in path listing");
}

// Task 6 — AC5: max_paths limits returned paths
#[test]
fn test_trace_flow_max_paths_honored() {
    let tmpdir = prepare_ts_tmpdir(
        "export function tgt() {}\n\
         export function via1() { tgt(); }\n\
         export function via2() { tgt(); }\n\
         export function start() { via1(); via2(); }"
    );
    // Without max_paths: expect 2 paths
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::start",
            "target_fqn": "app.ts::tgt"
        }}
    })]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("2 path(s) found"), "without max_paths must find 2 paths; got: {text}");

    // With max_paths=1: expect exactly 1 path
    let responses2 = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::start",
            "target_fqn": "app.ts::tgt",
            "max_paths": 1
        }}
    })]);
    assert!(responses2[0].get("error").is_none(), "must not have error; got: {}", responses2[0]);
    let text2 = responses2[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text2.contains("1 path(s) found"), "max_paths=1 must return exactly 1 path; got: {text2}");
}

// Task 7 — AC6: extends edges are traversed
#[test]
fn test_trace_flow_extends_edge_traversed() {
    let tmpdir = prepare_ts_tmpdir(
        "export class Base {}\nexport class Child extends Base {}"
    );
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::Child",
            "target_fqn": "app.ts::Base"
        }}
    })]);
    assert!(responses[0].get("error").is_none(), "must not have error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("path(s) found"),
        "extends edge must produce a path; got: {text}"
    );
    assert!(
        !text.contains("No execution path found"),
        "must not say no path when extends edge exists; got: {text}"
    );
}

// Task 8 — AC5 extended: max_paths boundary conditions
#[test]
fn test_trace_flow_max_paths_boundary() {
    let tmpdir = prepare_ts_tmpdir(
        "export function tgt() {}\n\
         export function via1() { tgt(); }\n\
         export function via2() { tgt(); }\n\
         export function start() { via1(); via2(); }"
    );

    // max_paths=0 clamps to 1 internally; must return "1 path(s) found"
    let r0 = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::start",
            "target_fqn": "app.ts::tgt",
            "max_paths": 0
        }}
    })]);
    assert!(r0[0].get("error").is_none(), "max_paths=0 must not error; got: {}", r0[0]);
    let t0 = r0[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(t0.contains("1 path(s) found"), "max_paths=0 clamps to 1; got: {t0}");

    // max_paths=25 clamps to MAX_PATHS_LIMIT=20; fixture has 2 paths (well under cap)
    let r25 = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::start",
            "target_fqn": "app.ts::tgt",
            "max_paths": 25
        }}
    })]);
    assert!(r25[0].get("error").is_none(), "max_paths=25 must not error; got: {}", r25[0]);
    let t25 = r25[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(t25.contains("2 path(s) found"), "max_paths=25 clamps to 20, fixture has 2 paths; got: {t25}");

    // max_paths as invalid string "bad" → as_u64() returns None, falls back to default (5)
    let rbad = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "trace_flow", "arguments": {
            "source_fqn": "app.ts::start",
            "target_fqn": "app.ts::tgt",
            "max_paths": "bad"
        }}
    })]);
    assert!(rbad[0].get("error").is_none(), "max_paths='bad' must not error; got: {}", rbad[0]);
    let tbad = rbad[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(tbad.contains("2 path(s) found"), "max_paths='bad' falls back to default (5), fixture has 2 paths; got: {tbad}");
}

// ─── Story 9.5: analyze_failure integration tests ────────────────────────────

fn prepare_ts_indexed_tmpdir() -> tempfile::TempDir {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).expect("create src/");
    std::fs::write(
        src_dir.join("handler.ts"),
        "export function handleRequest(req: any) {\n  return processData(req);\n}\n\
         export function processData(data: any) {\n  throw new Error('fail');\n}\n",
    ).expect("write handler.ts");
    std::fs::write(
        src_dir.join("service.ts"),
        "export class UserService {\n  validateCredentials(user: string) {\n    return true;\n  }\n}\n",
    ).expect("write service.ts");

    let status = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .output()
        .expect("olaf index failed to run");
    assert!(status.status.success(), "olaf index must succeed; stderr: {}",
        String::from_utf8_lossy(&status.stderr));
    tmpdir
}

#[test]
fn test_analyze_failure_with_indexed_trace() {
    let tmpdir = prepare_ts_indexed_tmpdir();
    let trace = "Error: fail\n    at processData (src/handler.ts:5:9)\n    at handleRequest (src/handler.ts:2:10)\n";
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": { "trace": trace } }
    })]);
    assert!(responses[0].get("error").is_none(), "must not error; got: {}", responses[0]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Failure Analysis"), "must have Failure Analysis header; got: {text}");
    assert!(text.contains("Frame Resolution"), "must have frame table");
    // Path A proof: frame table must show resolved symbols with "line" or "file" tier
    assert!(text.contains("| line |") || text.contains("| file |"),
        "Path A must resolve at least one frame via tier 1 or 2; got: {text}");
    // Path A proof: context brief with pivot symbols must appear (not Path C fallback)
    assert!(text.contains("Pivot Symbols"), "Path A must produce pivot symbols section; got: {text}");
    assert!(!text.contains("Could not resolve input"), "Path A must not show Path C fallback");
}

#[test]
fn test_analyze_failure_no_match() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": {
            "trace": "Error at /xyz/a.ts:1:1\n  at run (b.ts:2:3)"
        } }
    })]);
    assert!(responses[0].get("error").is_none(), "must not error");
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Could not resolve input to indexed code"),
        "must indicate no resolution; got: {text}");
}

#[test]
fn test_analyze_failure_missing_trace_param() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": {} }
    })]);
    let err = &responses[0]["error"];
    assert!(err.is_object(), "must have error; got: {}", responses[0]);
    assert_eq!(err["code"], -32602, "must be InvalidParams error code");
}

#[test]
fn test_analyze_failure_symbol_name_only() {
    let tmpdir = prepare_ts_indexed_tmpdir();
    let trace = "Error: fail\n  0: handleRequest()\n";
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": { "trace": trace } }
    })]);
    assert!(responses[0].get("error").is_none(), "must not error");
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    // handleRequest has 13 chars >= 4, and if unambiguous should resolve via Tier 3
    assert!(text.contains("Failure Analysis"), "must have analysis; got: {text}");
    // Tier 3 proof: if name resolved, we get pivots (Path A); if ambiguous, we get Path B/C
    // Either way, should not get Path C if the symbol exists and is unambiguous
    assert!(text.contains("Pivot Symbols") || text.contains("Context Brief"),
        "Tier 3 with unambiguous name must produce context; got: {text}");
}

#[test]
fn test_analyze_failure_sanitized_trace_redaction() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": {
            "trace": "Error\nloading .env config\npassword = \"hunter2\"\nat /home/user/app/src/main.ts:10:5"
        } }
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(!text.contains("hunter2"), "password must be redacted");
    assert!(!text.contains("/home/user"), "absolute path must be stripped");
}

#[test]
fn test_analyze_failure_budget_truncation() {
    let mut big_trace = String::from("Error: big\n");
    for i in 0..10000 {
        big_trace.push_str(&format!("  line {i} of output\n"));
    }
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": {
            "trace": big_trace,
            "token_budget": 500
        } }
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.len().div_ceil(4) <= 500, "response must fit in 500 token budget; got {} tokens",
        text.len().div_ceil(4));
}

#[test]
fn test_analyze_failure_mixed_internal_external_frames() {
    let tmpdir = prepare_ts_indexed_tmpdir();
    let trace = "Error: fail\n    at processData (src/handler.ts:5:9)\n    at internalFunc (/usr/lib/node/internal.js:42:10)\n";
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": { "trace": trace } }
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(!text.contains("/usr/lib/"), "external paths must not leak; got: {text}");
    // Frame with system path should show em-dash in path column since file_path is None
    assert!(text.contains("\u{2014}"), "external frames should show em-dash in path column");
}

#[test]
fn test_analyze_failure_keyword_fallback() {
    let tmpdir = prepare_ts_indexed_tmpdir();
    // No stack frames, but domain terms matching indexed symbols
    let trace = "TypeError in UserService.validateCredentials: invalid input";
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": { "trace": trace } }
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    // Path B should find symbols matching "UserService" or "validateCredentials"
    assert!(text.contains("Failure Analysis"), "must have analysis header; got: {text}");
    // Path B proof: keyword fallback must produce context with matched symbols
    assert!(text.contains("Pivot Symbols") || text.contains("Context Brief"),
        "Path B keyword fallback must produce context from matched symbols; got: {text}");
    assert!(!text.contains("Could not resolve input"),
        "Path B must not fall through to Path C when keywords match; got: {text}");
}

#[test]
fn test_analyze_failure_path_c_no_keywords() {
    let responses = run_requests(&[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": {
            "trace": "it has an error and it failed"
        } }
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("Could not resolve input to indexed code"),
        "must indicate fallback; got: {text}");
}

#[test]
fn test_analyze_failure_frame_cap_in_output() {
    let tmpdir = prepare_ts_indexed_tmpdir();
    let mut trace = String::from("Error: deep stack\n");
    for i in 0..50 {
        trace.push_str(&format!("    at func_{i} (src/handler.ts:{i}:1)\n", i = i + 1));
    }
    let responses = run_requests_in(tmpdir.path(), &[serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "analyze_failure", "arguments": { "trace": trace } }
    })]);
    let text = responses[0]["result"]["content"][0]["text"].as_str().unwrap();
    // Extract Frame Resolution section and count data rows
    if let Some(start) = text.find("### Frame Resolution") {
        let section = &text[start..];
        let end = section.find("\n## ").or_else(|| section.find("\n### ").filter(|&p| p > 0))
            .unwrap_or(section.len());
        let frame_section = &section[..end];
        let data_rows = frame_section.lines()
            .filter(|l| l.starts_with("| ") && !l.starts_with("| #") && !l.starts_with("|-"))
            .count();
        assert!(data_rows <= 30, "frame table must have at most 30 data rows; got {data_rows}");
    }
}
