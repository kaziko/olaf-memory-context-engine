use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};

fn spawn_server() -> (Child, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().expect("tempdir creation failed");
    let child = spawn_server_in(tmpdir.path());
    (child, tmpdir)
}

fn spawn_server_in(dir: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn olaf serve")
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

// ─── Story 3.2 Tests ──────────────────────────────────────────────────────────

fn run_requests_in(dir: &Path, requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut child = spawn_server_in(dir);
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

fn save_obs_request(id: i64, kind: &str, content: &str, symbol_fqn: Option<&str>, file_path: Option<&str>) -> serde_json::Value {
    let mut args = serde_json::json!({ "content": content, "kind": kind });
    if let Some(fqn) = symbol_fqn {
        args["symbol_fqn"] = serde_json::json!(fqn);
    }
    if let Some(fp) = file_path {
        args["file_path"] = serde_json::json!(fp);
    }
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": { "name": "save_observation", "arguments": args }
    })
}

fn get_history_request(id: i64, symbol_fqn: Option<&str>, file_path: Option<&str>, sessions_back: Option<i64>) -> serde_json::Value {
    let mut args = serde_json::json!({});
    if let Some(fqn) = symbol_fqn {
        args["symbol_fqn"] = serde_json::json!(fqn);
    }
    if let Some(fp) = file_path {
        args["file_path"] = serde_json::json!(fp);
    }
    if let Some(sb) = sessions_back {
        args["sessions_back"] = serde_json::json!(sb);
    }
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": { "name": "get_session_history", "arguments": args }
    })
}

fn extract_text(resp: &serde_json::Value) -> &str {
    resp["result"]["content"][0]["text"].as_str().expect("must have text")
}

#[test]
fn test_tools_list_includes_get_session_history() {
    let req = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let responses = run_requests(&[req]);
    let tools = responses[0]["result"]["tools"].as_array().expect("tools must be array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"get_session_history"), "must include get_session_history");
}

#[test]
fn test_get_session_history_no_filters_returns_all() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "first observation", Some("f::foo"), None),
        save_obs_request(2, "decision", "second observation", None, Some("src/a.rs")),
        get_history_request(3, None, None, None),
    ]);
    let text = extract_text(&responses[2]);
    assert!(text.contains("first observation"), "must include first obs; got: {text}");
    assert!(text.contains("second observation"), "must include second obs; got: {text}");
    assert!(text.contains("[insight]"), "must show kind; got: {text}");
    assert!(text.contains("[decision]"), "must show kind; got: {text}");
}

#[test]
fn test_get_session_history_symbol_fqn_filter() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "about foo", Some("f::foo"), None),
        save_obs_request(2, "insight", "about bar", Some("f::bar"), None),
        get_history_request(3, Some("f::foo"), None, None),
    ]);
    let text = extract_text(&responses[2]);
    assert!(text.contains("about foo"), "must include foo obs; got: {text}");
    assert!(!text.contains("about bar"), "must NOT include bar obs; got: {text}");
}

#[test]
fn test_get_session_history_file_path_filter() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "about a", None, Some("src/a.rs")),
        save_obs_request(2, "insight", "about b", None, Some("src/b.rs")),
        get_history_request(3, None, Some("src/a.rs"), None),
    ]);
    let text = extract_text(&responses[2]);
    assert!(text.contains("about a"), "must include a obs; got: {text}");
    assert!(!text.contains("about b"), "must NOT include b obs; got: {text}");
}

#[test]
fn test_get_session_history_no_observations() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, None, None, None),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("No sessions found") || text.contains("No observations found"),
        "must report no data; got: {text}");
}

#[test]
fn test_get_session_history_stale_marker() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Run 1: save an observation
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "error", "off by one bug", Some("f::traverse"), None),
    ]);
    assert!(responses[0]["result"].is_object(), "save must succeed");

    // Mark it stale directly in DB
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "UPDATE observations SET is_stale = 1, stale_reason = 'Symbol source changed since observation was recorded'",
        [],
    ).unwrap();
    drop(conn);

    // Run 2: query and check staleness marker
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, None, None, Some(2)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("STALE"), "must show stale marker; got: {text}");
    assert!(text.contains("Symbol source changed"), "must show stale reason; got: {text}");
}

#[test]
fn test_get_session_history_cross_session() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Run 1: save obs in session 1
    run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "session one observation", Some("f::hello"), None),
    ]);
    // Run 2: save obs in session 2 + query both
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "decision", "session two observation", Some("f::hello"), None),
        get_history_request(2, None, None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("session one observation"), "must include session 1 obs; got: {text}");
    assert!(text.contains("session two observation"), "must include session 2 obs; got: {text}");
}

#[test]
fn test_get_session_history_excludes_sensitive_paths() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "safe observation", None, Some("src/main.rs")),
        get_history_request(2, None, None, None),
    ]);
    let safe_text = extract_text(&responses[1]);
    assert!(safe_text.contains("safe observation"), "must include safe obs; got: {safe_text}");

    // Now save to .env — should be rejected at save time per NFR7
    let responses2 = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "secret obs", None, Some(".env")),
    ]);
    assert!(responses2[0]["error"].is_object(), "saving to .env must be rejected; got: {}", responses2[0]);
}

#[test]
fn test_save_observation_sensitive_file_rejected() {
    let responses = run_requests(&[
        save_obs_request(1, "insight", "secret", None, Some(".env")),
    ]);
    assert_eq!(responses[0]["error"]["code"], -32602, "sensitive file_path must return -32602; got: {}", responses[0]);
    let msg = responses[0]["error"]["message"].as_str().unwrap_or("");
    assert!(msg.contains("sensitive"), "error must mention sensitive; got: {msg}");
}

#[test]
fn test_save_observation_sensitive_symbol_fqn_rejected() {
    let responses = run_requests(&[
        save_obs_request(1, "insight", "secret", Some(".env::DB_PASSWORD"), None),
    ]);
    assert_eq!(responses[0]["error"]["code"], -32602, "sensitive symbol_fqn must return -32602; got: {}", responses[0]);
}

#[test]
fn test_get_context_with_observations_includes_session_memory() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Create a minimal TypeScript file and index it
    std::fs::write(tmpdir.path().join("test.ts"), "function hello() { return 1; }\n").unwrap();
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    // Start MCP server, save observation linked to an indexed symbol, then get_context
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "hello returns hardcoded 1", Some("test.ts::hello"), None),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "get_context",
                "arguments": {
                    "intent": "understand hello function",
                    "file_hints": ["test.ts"],
                    "token_budget": 4000
                }
            }
        }),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("## Session Memory"), "must include Session Memory section; got: {text}");
    assert!(text.contains("hello returns hardcoded 1"), "must include observation content; got: {text}");
}

#[test]
fn test_get_context_without_observations_no_session_memory() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("test.ts"), "function greet() { return 'hi'; }\n").unwrap();
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    let responses = run_requests_in(tmpdir.path(), &[
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "get_context",
                "arguments": {
                    "intent": "understand greet function",
                    "file_hints": ["test.ts"],
                    "token_budget": 4000
                }
            }
        }),
    ]);
    let text = extract_text(&responses[0]);
    assert!(!text.contains("## Session Memory"), "must NOT include Session Memory section when no observations; got: {text}");
}

#[test]
fn test_get_session_history_negative_sessions_back_clamps_to_1() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Run 1: session with obs
    run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "session1 obs", Some("f::a"), None),
    ]);
    // Run 2: session with obs + query with negative sessions_back
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "session2 obs", Some("f::a"), None),
        get_history_request(2, None, None, Some(-1)),
    ]);
    let text = extract_text(&responses[1]);
    // sessions_back=-1 should clamp to 1, so only the most recent session
    assert!(text.contains("session2 obs"), "must include current session; got: {text}");
    assert!(!text.contains("session1 obs"), "must NOT include older session when clamped to 1; got: {text}");
}

#[test]
fn test_get_context_tiny_budget_no_empty_memory_header() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("test.ts"), "function hello() { return 1; }\n").unwrap();
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "a long observation about hello", Some("test.ts::hello"), None),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "get_context",
                "arguments": {
                    "intent": "understand hello function",
                    "file_hints": ["test.ts"],
                    "token_budget": 10
                }
            }
        }),
    ]);
    let text = extract_text(&responses[1]);
    // With budget=10, memory budget is 1 token — no observation can fit
    // The header "## Session Memory" must NOT appear without content
    if text.contains("## Session Memory") {
        // If the header is present, there must be actual observation content after it
        let after_header = text.split("## Session Memory").nth(1).unwrap_or("");
        assert!(after_header.contains("[insight]"), "Session Memory header must have content after it; got: {text}");
    }
}

#[test]
fn test_get_observations_for_context_limit_after_sensitive_filter() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Create a TS file with two symbols and index it
    std::fs::write(
        tmpdir.path().join("test.ts"),
        "function safeA() { return 1; }\nfunction safeB() { return 2; }\n",
    ).unwrap();
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    // Save a safe observation via MCP
    run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "safe context obs", Some("test.ts::safeA"), None),
    ]);

    // Insert sensitive observations directly into DB (MCP rejects them)
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let session_id: String = conn.query_row(
        "SELECT id FROM sessions LIMIT 1", [], |r| r.get(0)
    ).unwrap();
    for i in 0..5 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, file_path, is_stale, auto_generated) \
             VALUES (?1, ?2, 'insight', ?3, '.env', 0, 0)",
            rusqlite::params![session_id, 9000 + i, format!("sensitive obs {i}")],
        ).unwrap();
    }
    drop(conn);

    // Query via get_context — exercises get_observations_for_context code path
    let responses = run_requests_in(tmpdir.path(), &[
        serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "get_context",
                "arguments": {
                    "intent": "understand safeA function",
                    "file_hints": ["test.ts"],
                    "token_budget": 4000
                }
            }
        }),
    ]);
    let text = extract_text(&responses[0]);
    // Safe observation must surface despite sensitive rows with newer timestamps
    assert!(text.contains("safe context obs"), "must include safe obs past sensitive rows; got: {text}");
    assert!(!text.contains("sensitive obs"), "must NOT include sensitive obs; got: {text}");
}
