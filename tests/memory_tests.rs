use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};

fn spawn_server() -> (std::process::Child, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().expect("tempdir creation failed");
    let child = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .current_dir(tmpdir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn olaf serve");
    (child, tmpdir)
}

fn run_requests(requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let (mut child, _tmpdir) = spawn_server();
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        for req in requests {
            writeln!(w, "{}", serde_json::to_string(req).unwrap()).unwrap();
        }
    }
    let output = child.wait_with_output().expect("server process did not exit");
    assert!(output.status.success(), "server exited with non-zero status: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("non-JSON on stdout: {e}\nLine: {l:?}")))
        .collect()
}

// ─── Story 3.1 Tests ──────────────────────────────────────────────────────────

#[test]
fn test_tools_list_includes_save_observation() {
    let req = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let responses = run_requests(&[req]);
    let tools = responses[0]["result"]["tools"].as_array().expect("tools must be array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"save_observation"), "must include save_observation");
}

#[test]
fn test_save_observation_with_symbol_fqn_succeeds() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "Decided to skip caching — caused stale reads in query.rs",
                "kind": "decision",
                "symbol_fqn": "src/graph/query.rs::get_context"
            }
        }
    });
    let responses = run_requests(&[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result, not error; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("Observation saved"), "response must confirm save; got: {text}");
    assert!(r.get("error").is_none_or(|e| e.is_null()), "must not have error field");
}

#[test]
fn test_save_observation_with_file_path_only_succeeds() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "This file handles authentication logic",
                "kind": "insight",
                "file_path": "src/auth.rs"
            }
        }
    });
    let responses = run_requests(&[req]);
    let r = &responses[0];
    assert!(r["result"].is_object(), "must return result, not error; got: {}", r);
    let text = r["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("Observation saved"), "response must confirm save; got: {text}");
}

#[test]
fn test_save_observation_invalid_kind_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 4, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "Some content",
                "kind": "bogus_kind",
                "symbol_fqn": "src/lib.rs::some_fn"
            }
        }
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "invalid kind must return -32602; got: {}", responses[0]);
}

#[test]
fn test_save_observation_missing_content_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 5, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "kind": "insight",
                "symbol_fqn": "src/lib.rs::some_fn"
            }
        }
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing content must return -32602; got: {}", responses[0]);
}

#[test]
fn test_save_observation_no_anchor_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 6, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "Orphan observation with no anchor",
                "kind": "insight"
            }
        }
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "missing both symbol_fqn and file_path must return -32602; got: {}", responses[0]);
}

#[test]
fn test_save_observation_whitespace_only_anchors_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 8, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "Observation with blank anchors",
                "kind": "insight",
                "symbol_fqn": "   ",
                "file_path": "  "
            }
        }
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "whitespace-only anchors must return -32602; got: {}", responses[0]);
}

#[test]
fn test_save_observation_whitespace_only_content_returns_32602() {
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 9, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "   ",
                "kind": "insight",
                "symbol_fqn": "src/lib.rs::some_fn"
            }
        }
    });
    let responses = run_requests(&[req]);
    assert_eq!(responses[0]["error"]["code"], -32602, "whitespace-only content must return -32602; got: {}", responses[0]);
}

#[test]
fn test_save_observation_persists_to_db() {
    let (mut child, tmpdir) = spawn_server();
    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 7, "method": "tools/call",
        "params": {
            "name": "save_observation",
            "arguments": {
                "content": "Persistence test observation",
                "kind": "tool_call",
                "file_path": "src/main.rs"
            }
        }
    });
    {
        let stdin = child.stdin.take().unwrap();
        let mut w = BufWriter::new(stdin);
        writeln!(w, "{}", serde_json::to_string(&req).unwrap()).unwrap();
        // stdin dropped here → EOF → server exits cleanly
    }
    let output = child.wait_with_output().expect("server process did not exit");
    assert!(output.status.success(), "server must exit successfully");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let resp: serde_json::Value = serde_json::from_str(stdout.lines().next().expect("must have response"))
        .expect("must be valid JSON");
    assert!(resp["result"].is_object(), "save must succeed; got: {}", resp);
    let text = resp["result"]["content"][0]["text"].as_str().expect("text");
    assert!(text.contains("Observation saved"), "response must confirm save; got: {text}");

    // Query DB directly to verify persistence
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).expect("must open DB");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM observations", [], |r| r.get(0))
        .expect("must query observations");
    assert!(count >= 1, "observation must be persisted in DB; got count={count}");
}
