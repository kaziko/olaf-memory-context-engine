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

// ─── Story 3.3 Tests ──────────────────────────────────────────────────────────

/// Helper: create a TypeScript file, run `olaf index`, start server, save observation,
/// then return tmpdir for further manipulation.
fn setup_indexed_project_with_observation(
    ts_content: &str,
    obs_content: &str,
    obs_fqn: Option<&str>,
    obs_file_path: Option<&str>,
) -> tempfile::TempDir {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("test.ts"), ts_content).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("initial index must spawn");
    assert!(output.status.success(), "initial olaf index failed: {}", String::from_utf8_lossy(&output.stderr));

    // Save observation via MCP
    run_requests_in(
        tmpdir.path(),
        &[save_obs_request(1, "insight", obs_content, obs_fqn, obs_file_path)],
    );
    tmpdir
}

fn get_context_request(id: i64, file_hints: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": {
            "name": "get_context",
            "arguments": {
                "intent": "understand code",
                "file_hints": file_hints,
                "token_budget": 4000
            }
        }
    })
}

#[test]
fn test_staleness_incremental_source_changed() {
    // Story 7.1: signature change → observation marked stale
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "hello returns hardcoded 1",
        Some("test.ts::hello"),
        None,
    );

    // Change the signature (add parameter) → structural change → stale
    std::fs::write(tmpdir.path().join("test.ts"), "function hello(x: number) { return x; }\n").unwrap();

    // get_context triggers incremental re-index, then query history for stale marker
    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("STALE"), "observation must be marked STALE after signature change; got: {text}");
    assert!(
        text.contains("Signature of symbol"),
        "reason must mention 'Signature of symbol'; got: {text}"
    );
}

#[test]
fn test_staleness_incremental_body_only_not_stale() {
    // Story 7.1: body-only change (same signature) → observation NOT stale
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "hello returns hardcoded 1",
        Some("test.ts::hello"),
        None,
    );

    // Change only the body, keep signature unchanged
    std::fs::write(tmpdir.path().join("test.ts"), "function hello() { return 999; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(!text.contains("STALE"), "body-only change must NOT mark observation stale; got: {text}");
}

#[test]
fn test_staleness_incremental_unchanged_not_stale() {
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "hello returns 1",
        Some("test.ts::hello"),
        None,
    );

    // Re-index without changing the file (get_context triggers incremental)
    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(!text.contains("STALE"), "unchanged file must NOT mark observation stale; got: {text}");
}

#[test]
fn test_staleness_incremental_symbol_deleted() {
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\nfunction goodbye() { return 2; }\n",
        "hello is the entry point",
        Some("test.ts::hello"),
        None,
    );

    // Remove hello from file (only keep goodbye)
    std::fs::write(tmpdir.path().join("test.ts"), "function goodbye() { return 2; }\n").unwrap();

    // get_context triggers incremental re-index
    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("STALE"), "observation must be stale after symbol deletion; got: {text}");
    assert!(
        text.contains("no longer exists"),
        "reason must mention 'no longer exists'; got: {text}"
    );
}

#[test]
fn test_staleness_file_level_observation_not_stale() {
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "this file handles greetings",
        None,
        Some("test.ts"),
    );

    // Modify the file (source_hash changes)
    std::fs::write(tmpdir.path().join("test.ts"), "function hello() { return 999; }\n").unwrap();

    // get_context triggers incremental re-index
    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, None, Some("test.ts"), Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(
        !text.contains("STALE"),
        "file-level observation must NOT be stale when symbol changes; got: {text}"
    );
}

#[test]
fn test_staleness_file_deleted_stale_cleanup() {
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "hello is important",
        Some("test.ts::hello"),
        None,
    );

    // Delete the entire file
    std::fs::remove_file(tmpdir.path().join("test.ts")).unwrap();
    // Need at least one supported file for seen_paths to be non-empty,
    // otherwise the stale-file cleanup deletes everything via DELETE FROM files.
    std::fs::write(tmpdir.path().join("other.ts"), "function other() {}\n").unwrap();

    // get_context triggers incremental re-index (with stale-file cleanup)
    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["other.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(
        text.contains("STALE"),
        "observation must be stale after file deletion; got: {text}"
    );
    assert!(
        text.contains("no longer exists"),
        "reason must mention 'no longer exists'; got: {text}"
    );
}

#[test]
fn test_staleness_full_reindex_source_changed() {
    // Story 7.1: full re-index with signature change → observation marked stale
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "hello returns 1",
        Some("test.ts::hello"),
        None,
    );

    // Change signature (add parameter) — structural change
    std::fs::write(tmpdir.path().join("test.ts"), "function hello(n: number) { return n; }\n").unwrap();

    // Full re-index via CLI (covers full.rs path)
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("full re-index must spawn");
    assert!(output.status.success(), "full re-index failed: {}", String::from_utf8_lossy(&output.stderr));

    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("STALE"), "full re-index must mark observation stale after signature change; got: {text}");
    assert!(
        text.contains("Signature of symbol"),
        "reason must mention 'Signature of symbol'; got: {text}"
    );
}

#[test]
fn test_staleness_full_reindex_symbol_removed() {
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\nfunction goodbye() { return 2; }\n",
        "hello is primary",
        Some("test.ts::hello"),
        None,
    );

    // Remove hello from file
    std::fs::write(tmpdir.path().join("test.ts"), "function goodbye() { return 2; }\n").unwrap();

    // Full re-index via CLI
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("full re-index must spawn");
    assert!(output.status.success(), "full re-index failed: {}", String::from_utf8_lossy(&output.stderr));

    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("STALE"), "full re-index must mark removed symbol stale; got: {text}");
    assert!(
        text.contains("no longer exists"),
        "reason must mention 'no longer exists'; got: {text}"
    );
}

// ─── Story 3.4 Tests ──────────────────────────────────────────────────────────

#[test]
fn test_compression_removes_ephemeral_retains_insights() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let mut conn = olaf::db::open(&db_path).unwrap();

    // Create an ended session (ended 2 hours ago)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let ended = now - 7200;
    conn.execute(
        "INSERT INTO sessions (id, started_at, ended_at, agent) VALUES ('comp-sess', ?1, ?2, 'test')",
        rusqlite::params![ended - 3600, ended],
    ).unwrap();

    // Insert mixed observations
    for (kind, content) in &[
        ("tool_call", "read file"),
        ("file_change", "modified main.rs"),
        ("insight", "important finding"),
        ("decision", "chose approach A"),
    ] {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content) VALUES ('comp-sess', ?1, ?2, ?3)",
            rusqlite::params![now, kind, content],
        ).unwrap();
    }

    // Run compression
    olaf::memory::run_compression(&mut conn, 3600).unwrap();

    // Verify ephemeral deleted, insights retained
    let obs_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM observations WHERE session_id = 'comp-sess'", [], |r| r.get(0)
    ).unwrap();
    assert_eq!(obs_count, 2, "only insight and decision should remain");

    let compressed: i64 = conn.query_row(
        "SELECT compressed FROM sessions WHERE id = 'comp-sess'", [], |r| r.get(0)
    ).unwrap();
    assert_eq!(compressed, 1);

    // Verify via MCP get_session_history that retained observations are still visible
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, None, None, Some(5)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("important finding"), "retained insight must appear in history; got: {text}");
    assert!(!text.contains("read file"), "ephemeral tool_call must NOT appear after compression; got: {text}");
}

#[test]
fn test_compressed_session_visible_in_get_session_history() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let mut conn = olaf::db::open(&db_path).unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let ended = now - 7200;
    conn.execute(
        "INSERT INTO sessions (id, started_at, ended_at, agent) VALUES ('vis-sess', ?1, ?2, 'test')",
        rusqlite::params![ended - 3600, ended],
    ).unwrap();

    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn) VALUES ('vis-sess', ?1, 'insight', 'retained insight', 'f::hello')",
        rusqlite::params![now],
    ).unwrap();
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content) VALUES ('vis-sess', ?1, 'tool_call', 'ephemeral call')",
        rusqlite::params![now],
    ).unwrap();

    olaf::memory::run_compression(&mut conn, 3600).unwrap();

    // Verify via MCP
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, None, None, Some(5)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("retained insight"), "compressed session insight must be visible; got: {text}");
}

// ─── Story 4.1 Tests ──────────────────────────────────────────────────────────

fn make_edit_payload(session_id: &str, cwd: &str, file_path: &str) -> serde_json::Value {
    serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "Edit",
        "tool_input": {
            "file_path": format!("{cwd}/{file_path}"),
            "old_string": "hello world",
            "new_string": "goodbye world"
        },
        "tool_response": {},
        "tool_use_id": "toolu_01test"
    })
}

fn make_write_payload(session_id: &str, cwd: &str, file_path: &str, content: &str) -> serde_json::Value {
    serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "Write",
        "tool_input": {
            "file_path": format!("{cwd}/{file_path}"),
            "content": content
        },
        "tool_use_id": "toolu_02test"
    })
}

fn make_bash_payload(session_id: &str, cwd: &str, command: &str) -> serde_json::Value {
    serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "Bash",
        "tool_input": { "command": command },
        "tool_use_id": "toolu_03test"
    })
}

fn run_observe(tmpdir: &tempfile::TempDir, payload: &serde_json::Value) -> std::process::Output {
    let json = serde_json::to_string(payload).unwrap();
    std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "post-tool-use"])
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(json.as_bytes()).unwrap();
            child.wait_with_output()
        })
        .expect("olaf observe must spawn")
}

// Task 9.1: Full PostToolUse flow for Edit
#[test]
fn test_observe_edit_payload_creates_observation() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Initialize DB
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let payload = make_edit_payload("hook-sess-1", cwd, "src/main.rs");
    let output = run_observe(&tmpdir, &payload);

    assert!(output.status.success(), "olaf observe must exit 0; stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert!(output.stdout.is_empty(), "stdout must be empty; got: {:?}", String::from_utf8_lossy(&output.stdout));

    // Verify observation in DB
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let row: (String, String, Option<String>, i64) = conn.query_row(
        "SELECT kind, content, file_path, auto_generated FROM observations WHERE session_id = 'hook-sess-1'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ).unwrap();
    assert_eq!(row.0, "file_change");
    assert!(row.1.contains("Edited src/main.rs"), "content must mention file; got: {}", row.1);
    assert_eq!(row.2.as_deref(), Some("src/main.rs"));
    assert_eq!(row.3, 1, "auto_generated must be 1");
}

// Task 9.2: Write payload full flow
#[test]
fn test_observe_write_payload_creates_file_change_observation() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let payload = make_write_payload("hook-sess-2", cwd, "src/lib.rs", "fn main() {}");
    let output = run_observe(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let kind: String = conn.query_row(
        "SELECT kind FROM observations WHERE session_id = 'hook-sess-2'",
        [],
        |r| r.get(0),
    ).unwrap();
    assert_eq!(kind, "file_change");
}

// Task 9.3: Bash payload full flow
#[test]
fn test_observe_bash_payload_creates_tool_call_observation() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let payload = make_bash_payload("hook-sess-3", cwd, "cargo build");
    let output = run_observe(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let (kind, file_path): (String, Option<String>) = conn.query_row(
        "SELECT kind, file_path FROM observations WHERE session_id = 'hook-sess-3'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ).unwrap();
    assert_eq!(kind, "tool_call");
    assert!(file_path.is_none(), "Bash observations must have no file_path");
}

// Task 10.1: Valid payload exits 0 with empty stdout
#[test]
fn test_observe_valid_payload_exits_0_empty_stdout() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Initialize DB so the observe handler can write
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let payload = make_edit_payload("hook-sess-10a", cwd, "src/a.rs");
    let output = run_observe(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0; stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert!(output.stdout.is_empty(), "stdout must be empty");
}

// Task 10.2: Malformed JSON exits 0, empty stdout
#[test]
fn test_observe_malformed_json_exits_0() {
    let tmpdir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "post-tool-use"])
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(b"not valid json {{{").unwrap();
            child.wait_with_output()
        })
        .expect("must spawn");

    assert!(output.status.success(), "malformed JSON must exit 0; got: {:?}", output.status);
    assert!(output.stdout.is_empty(), "stdout must be empty on malformed input");
}

// Task 10.3: Sensitive file path → exits 0, no observation
#[test]
fn test_observe_sensitive_path_no_observation() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    // .env is a sensitive path
    let payload = serde_json::json!({
        "session_id": "hook-sensitive",
        "cwd": cwd,
        "tool_name": "Edit",
        "tool_input": {
            "file_path": format!("{cwd}/.env"),
            "old_string": "SECRET=old",
            "new_string": "SECRET=new"
        }
    });
    let output = run_observe(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM observations WHERE session_id = 'hook-sensitive'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "no observation must be created for sensitive paths");
}

// Task 10.4: DB open failure → exits 0
// Use a path inside a file (not a directory) so SQLite cannot create the .olaf subdir,
// making DB open fail deterministically even in privileged/containerized environments.
#[test]
fn test_observe_invalid_cwd_exits_0() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Create a regular file at the path we'll use as "cwd" — SQLite cannot mkdir inside a file
    let fake_cwd = tmpdir.path().join("not_a_dir");
    std::fs::write(&fake_cwd, b"not a directory").unwrap();
    let fake_cwd_str = fake_cwd.to_str().unwrap();

    let payload = serde_json::json!({
        "session_id": "hook-bad-cwd",
        "cwd": fake_cwd_str,
        "tool_name": "Edit",
        "tool_input": {
            "file_path": format!("{fake_cwd_str}/src/x.rs"),
            "old_string": "a",
            "new_string": "b"
        }
    });
    let output = run_observe(&tmpdir, &payload);
    assert!(output.status.success(), "DB open failure must exit 0; got: {:?}", output.status);
}

// Task 10.5: file_path outside cwd → exits 0, no observation
#[test]
fn test_observe_file_outside_cwd_no_observation() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let payload = serde_json::json!({
        "session_id": "hook-outside",
        "cwd": cwd,
        "tool_name": "Edit",
        "tool_input": {
            "file_path": "/etc/passwd",
            "old_string": "root",
            "new_string": "user"
        }
    });
    let output = run_observe(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM observations WHERE session_id = 'hook-outside'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "no observation for files outside project root");
}

// ─── Story 7.1: Structural Observation Integration Tests ─────────────────────

// Signature change on an indexed TS file → structural observation with diff message
#[test]
fn test_observe_structural_signature_changed() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let ts_path = src_dir.join("auth.ts");
    std::fs::write(&ts_path, "function authenticate() { return true; }\n").unwrap();

    // Index the file so DB has its signature
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    // Write signature-changing edit to disk
    std::fs::write(&ts_path, "function authenticate(user: string) { return user.length > 0; }\n").unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let abs_path = ts_path.to_str().unwrap();
    let payload = serde_json::json!({
        "session_id": "struct-sig-test",
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "Edit",
        "tool_input": {
            "file_path": abs_path,
            "old_string": "function authenticate() { return true; }",
            "new_string": "function authenticate(user: string) { return user.length > 0; }"
        },
        "tool_use_id": "toolu_sig_test"
    });
    let output = run_observe(&tmpdir, &payload);
    assert!(output.status.success(), "observe must exit 0; stderr: {}", String::from_utf8_lossy(&output.stderr));

    let db_path = tmpdir.path().join(".olaf/index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let (content, file_path): (String, Option<String>) = conn
        .query_row(
            "SELECT content, file_path FROM observations WHERE session_id = 'struct-sig-test'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("structural observation must be written");
    assert!(
        content.contains("signature of `authenticate` changed"),
        "expected structural obs; got: {content}"
    );
    assert_eq!(file_path.as_deref(), Some("src/auth.ts"));
}

// Body-only change on indexed TS file → no observation written (AC3)
#[test]
fn test_observe_structural_body_only_no_obs() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    let ts_path = src_dir.join("helper.ts");
    std::fs::write(&ts_path, "function compute() { return 1; }\n").unwrap();

    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    // Body-only change — same signature
    std::fs::write(&ts_path, "function compute() { return 999; }\n").unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let abs_path = ts_path.to_str().unwrap();
    let payload = serde_json::json!({
        "session_id": "struct-body-test",
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "Edit",
        "tool_input": {
            "file_path": abs_path,
            "old_string": "return 1;",
            "new_string": "return 999;"
        },
        "tool_use_id": "toolu_body_test"
    });
    let output = run_observe(&tmpdir, &payload);
    assert!(output.status.success());

    let db_path = tmpdir.path().join(".olaf/index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = 'struct-body-test'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "body-only change must NOT create an observation");
}

// Unsupported file type (e.g. .md) → SoftFailure path → basic fallback observation written
#[test]
fn test_observe_structural_unsupported_file_fallback() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf/index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let md_path = tmpdir.path().join("README.md");
    std::fs::write(&md_path, "# Hello\n").unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let abs_path = md_path.to_str().unwrap();
    let payload = serde_json::json!({
        "session_id": "struct-fallback-test",
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "Edit",
        "tool_input": {
            "file_path": abs_path,
            "old_string": "Hello",
            "new_string": "Goodbye"
        },
        "tool_use_id": "toolu_fallback_test"
    });
    let output = run_observe(&tmpdir, &payload);
    assert!(output.status.success());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let content: String = conn
        .query_row(
            "SELECT content FROM observations WHERE session_id = 'struct-fallback-test'",
            [],
            |r| r.get(0),
        )
        .expect("fallback observation must be written for unsupported file type");
    assert!(content.contains("Edited README.md"), "expected fallback obs; got: {content}");
}

// ─── Story 4.2: SessionEnd Hook Integration Tests ────────────────────────────

fn run_observe_event(
    event: &str,
    tmpdir: &tempfile::TempDir,
    payload: &serde_json::Value,
) -> std::process::Output {
    let json = serde_json::to_string(payload).unwrap();
    std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", event])
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(json.as_bytes()).unwrap();
            child.wait_with_output()
        })
        .expect("olaf observe must spawn")
}

fn make_session_end_payload(session_id: &str, cwd: &str) -> serde_json::Value {
    serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "SessionEnd"
    })
}

fn insert_file_change_at(conn: &rusqlite::Connection, session_id: &str, file_path: &str, created_at: i64) {
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, file_path, auto_generated) \
         VALUES (?1, ?2, 'file_change', 'edit', ?3, 1)",
        rusqlite::params![session_id, created_at, file_path],
    )
    .expect("insert file_change");
}

// Task 5.3: SessionEnd with 4 file_change obs in same bucket → exits 0, anti_pattern written
#[test]
fn test_session_end_thrashing_detection_writes_anti_pattern() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "se-sess-1";
    olaf::memory::upsert_session(&conn, session_id, "test").unwrap();
    // 4 file_change obs in same 300s bucket
    for t in [0i64, 60, 120, 180] {
        insert_file_change_at(&conn, session_id, "src/main.rs", t);
    }
    drop(conn);

    let payload = make_session_end_payload(session_id, cwd);
    let output = run_observe_event("session-end", &tmpdir, &payload);

    assert!(output.status.success(), "must exit 0; stderr: {}", String::from_utf8_lossy(&output.stderr));
    assert!(output.stdout.is_empty(), "stdout must be empty");

    let conn2 = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = 'anti_pattern'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "anti_pattern observation must be written");

    let content: String = conn2
        .query_row(
            "SELECT content FROM observations WHERE session_id = ?1 AND kind = 'anti_pattern'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(content.contains("File thrashing detected: src/main.rs"), "content: {content}");
}

// Task 5.4: SessionEnd with no thrashing → exits 0, no anti_pattern obs
#[test]
fn test_session_end_no_thrashing_no_anti_pattern() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "se-sess-2";
    olaf::memory::upsert_session(&conn, session_id, "test").unwrap();
    // Only 2 file_change obs — below threshold
    for t in [0i64, 60] {
        insert_file_change_at(&conn, session_id, "src/lib.rs", t);
    }
    drop(conn);

    let payload = make_session_end_payload(session_id, cwd);
    let output = run_observe_event("session-end", &tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");

    let conn2 = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = 'anti_pattern'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "no anti_pattern obs when no thrashing");
}

// Task 5.5: SessionEnd → file_change obs deleted after compression, insight retained
#[test]
fn test_session_end_compression_deletes_ephemeral_retains_insight() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "se-sess-3";
    olaf::memory::upsert_session(&conn, session_id, "test").unwrap();
    // Insert an insight (should be retained) and a file_change (should be deleted)
    olaf::memory::insert_auto_observation(&conn, session_id, "insight", "important finding", None, None).unwrap();
    insert_file_change_at(&conn, session_id, "src/a.rs", 100);
    drop(conn);

    let payload = make_session_end_payload(session_id, cwd);
    let output = run_observe_event("session-end", &tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");

    let conn2 = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = 'file_change'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "file_change obs must be deleted after compression");

    let insight_count: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = 'insight'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(insight_count, 1, "insight obs must be retained after compression");
}

// Task 5.6: Second SessionEnd call → exits 0, no duplicate anti_pattern obs (idempotency)
#[test]
fn test_session_end_idempotent_no_duplicate_anti_patterns() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "se-sess-4";
    olaf::memory::upsert_session(&conn, session_id, "test").unwrap();
    for t in [0i64, 60, 120, 180] {
        insert_file_change_at(&conn, session_id, "src/main.rs", t);
    }
    drop(conn);

    let payload = make_session_end_payload(session_id, cwd);
    // First call
    let out1 = run_observe_event("session-end", &tmpdir, &payload);
    assert!(out1.status.success(), "first call must exit 0");

    // Second call — should be no-op
    let out2 = run_observe_event("session-end", &tmpdir, &payload);
    assert!(out2.status.success(), "second call must exit 0");

    let conn2 = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = 'anti_pattern'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "only 1 anti_pattern obs — no duplicates from second call");
}

// Task 5.7: SessionEnd with malformed payload → exits 0, stdout empty
#[test]
fn test_session_end_malformed_payload_exits_0() {
    let tmpdir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "session-end"])
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(b"not json at all {{{").unwrap();
            child.wait_with_output()
        })
        .expect("must spawn");

    assert!(output.status.success(), "malformed JSON must exit 0; status: {:?}", output.status);
    assert!(output.stdout.is_empty(), "stdout must be empty on malformed input");
}

// Task 5.8: SessionEnd with invalid cwd → exits 0
#[test]
fn test_session_end_invalid_cwd_exits_0() {
    let tmpdir = tempfile::tempdir().unwrap();
    let payload = make_session_end_payload("se-bad-cwd", "/nonexistent/path/");
    let output = run_observe_event("session-end", &tmpdir, &payload);
    assert!(output.status.success(), "invalid cwd must exit 0; got: {:?}", output.status);
    assert!(output.stdout.is_empty(), "stdout must be empty");
}

// Concurrency: two parallel session-end processes for the same session → exactly 1 anti_pattern
// Verifies BEGIN IMMEDIATE serialization prevents TOCTOU duplicate writes.
#[test]
fn test_session_end_concurrent_produces_exactly_one_anti_pattern() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let cwd = tmpdir.path().to_str().unwrap().to_string();
    let session_id = "se-concurrent";
    olaf::memory::upsert_session(&conn, session_id, "test").unwrap();
    for t in [0i64, 60, 120, 180] {
        insert_file_change_at(&conn, session_id, "src/hot.rs", t);
    }
    drop(conn);

    let payload_str =
        serde_json::to_string(&make_session_end_payload(session_id, &cwd)).unwrap();

    // Spawn both processes before writing to either stdin so they race on DB acquisition
    let mut child1 = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "session-end"])
        .current_dir(&cwd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn child1");

    let mut child2 = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "session-end"])
        .current_dir(&cwd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn child2");

    // Feed payload to both (order doesn't matter; both are already running)
    use std::io::Write;
    child1.stdin.take().unwrap().write_all(payload_str.as_bytes()).unwrap();
    child2.stdin.take().unwrap().write_all(payload_str.as_bytes()).unwrap();

    let out1 = child1.wait_with_output().expect("wait child1");
    let out2 = child2.wait_with_output().expect("wait child2");

    assert!(out1.status.success(), "child1 must exit 0; stderr: {}", String::from_utf8_lossy(&out1.stderr));
    assert!(out2.status.success(), "child2 must exit 0; stderr: {}", String::from_utf8_lossy(&out2.stderr));
    assert!(out1.stdout.is_empty(), "child1 stdout must be empty");
    assert!(out2.stdout.is_empty(), "child2 stdout must be empty");

    let conn2 = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn2
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = 'anti_pattern'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "BEGIN IMMEDIATE must serialize: exactly 1 anti_pattern regardless of race outcome");
}

// ─── Story 8.1 Integration Tests ──────────────────────────────────────────────

fn get_history_request_with_sort(
    id: i64,
    symbol_fqn: Option<&str>,
    file_path: Option<&str>,
    sessions_back: Option<i64>,
    sort_mode: Option<&str>,
) -> serde_json::Value {
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
    if let Some(sm) = sort_mode {
        args["sort_mode"] = serde_json::json!(sm);
    }
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": { "name": "get_session_history", "arguments": args }
    })
}

#[test]
fn test_session_mode_shows_scores_and_session_headers() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "first finding", Some("f::foo"), None),
        save_obs_request(2, "decision", "second finding", None, Some("src/a.rs")),
        get_history_request_with_sort(3, None, None, None, Some("session")),
    ]);
    let text = extract_text(&responses[2]);
    assert!(text.contains("## Session"), "must include session headers; got: {text}");
    assert!(text.contains("[score:"), "must include score annotations; got: {text}");
    assert!(text.contains("[insight]"), "must show kind; got: {text}");
    assert!(text.contains("first finding"), "must include first obs; got: {text}");
    assert!(text.contains("Relevance:"), "must include relevance footer; got: {text}");
}

#[test]
fn test_session_mode_default_when_omitted() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "test obs", Some("f::x"), None),
        get_history_request_with_sort(2, None, None, None, None),
    ]);
    let text = extract_text(&responses[1]);
    // Default should behave like session mode: session headers present
    assert!(text.contains("## Session"), "default must use session mode; got: {text}");
    assert!(text.contains("[score:"), "default must include scores; got: {text}");
}

#[test]
fn test_relevance_mode_flat_ranked_no_session_headers() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "alpha obs", Some("f::a"), None),
        save_obs_request(2, "decision", "beta obs", None, Some("src/b.rs")),
        get_history_request_with_sort(3, None, None, None, Some("relevance")),
    ]);
    let text = extract_text(&responses[2]);
    assert!(!text.contains("## Session"), "relevance mode must NOT have session headers; got: {text}");
    assert!(text.contains("Relevance Ranked"), "must have relevance header; got: {text}");
    assert!(text.contains("1. [score:"), "must have numbered ranked entries; got: {text}");
    assert!(text.contains("Relevance:"), "must have relevance footer; got: {text}");

    // Scores should be in descending order
    let scores: Vec<f64> = text.lines()
        .filter_map(|l| {
            let start = l.find("[score: ")?;
            let rest = &l[start + 8..];
            // Score ends at first space (before " · signal]") or "]"
            let end = rest.find([' ', ']']).unwrap_or(rest.len());
            rest[..end].parse::<f64>().ok()
        })
        .collect();
    for pair in scores.windows(2) {
        assert!(pair[0] >= pair[1], "scores must be descending: {:.2} >= {:.2}", pair[0], pair[1]);
    }
}

#[test]
fn test_invalid_sort_mode_returns_error() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request_with_sort(1, None, None, None, Some("invalid")),
    ]);
    let error = &responses[0]["error"];
    assert!(error.is_object(), "must return error for invalid sort_mode; got: {:?}", responses[0]);
    assert_eq!(error["code"], -32602, "must return -32602 InvalidParams; got: {:?}", error);
}

#[test]
fn test_relevance_mode_stale_after_nonstale_at_similar_scores() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Save two observations
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "fresh finding", Some("f::fresh"), None),
        save_obs_request(2, "insight", "stale finding", Some("f::stale"), None),
    ]);
    assert!(responses[0]["result"].is_object());
    assert!(responses[1]["result"].is_object());

    // Mark second observation stale
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "UPDATE observations SET is_stale = 1, stale_reason = 'source changed' WHERE content = 'stale finding'",
        [],
    ).unwrap();
    drop(conn);

    // Query in relevance mode
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request_with_sort(1, None, None, Some(2), Some("relevance")),
    ]);
    let text = extract_text(&responses[0]);

    // Fresh should appear before stale
    let fresh_pos = text.find("fresh finding").expect("must contain fresh finding");
    let stale_pos = text.find("stale finding").expect("must contain stale finding");
    assert!(fresh_pos < stale_pos, "non-stale must appear before stale; fresh@{fresh_pos} stale@{stale_pos}");
    assert!(text.contains("STALE"), "must show stale marker; got: {text}");
}

#[test]
fn test_relevance_mode_sorts_before_truncation() {
    // Regression test: relevance mode must sort ALL fetched observations by score,
    // then truncate to 200. Previously it truncated first, losing high-relevance items.
    let tmpdir = tempfile::tempdir().unwrap();

    // Save 1 observation via MCP to bootstrap the DB
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "bootstrap", Some("f::x"), None),
    ]);
    assert!(responses[0]["result"].is_object());

    // Directly insert 210 observations: 205 old (30 days) + 5 very recent (1 min ago)
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;

    // Get the session_id from the bootstrap observation
    let session_id: String = conn.query_row(
        "SELECT session_id FROM observations LIMIT 1", [], |r| r.get(0),
    ).unwrap();

    // Insert 205 old observations (30 days ago) — these will have low scores
    for i in 0..205 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated, is_stale, stale_reason) \
             VALUES (?1, ?2, 'insight', ?3, NULL, NULL, 0, 0, NULL)",
            rusqlite::params![session_id, now - 30 * 86400, format!("old-obs-{i}")],
        ).unwrap();
    }

    // Insert 5 very recent observations (1 minute ago) — these will have high scores
    for i in 0..5 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated, is_stale, stale_reason) \
             VALUES (?1, ?2, 'insight', ?3, NULL, NULL, 0, 0, NULL)",
            rusqlite::params![session_id, now - 60, format!("RECENT-obs-{i}")],
        ).unwrap();
    }
    drop(conn);

    // Query in relevance mode — should surface the 5 recent ones in top results
    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request_with_sort(1, None, None, Some(5), Some("relevance")),
    ]);
    let text = extract_text(&responses[0]);

    // All 5 recent observations must appear (they have highest scores)
    for i in 0..5 {
        assert!(
            text.contains(&format!("RECENT-obs-{i}")),
            "recent obs {i} must appear in top 200; got: {text}"
        );
    }

    // The first 5 ranked items should all be RECENT (highest scores)
    let lines: Vec<&str> = text.lines()
        .filter(|l| l.starts_with(|c: char| c.is_ascii_digit()))
        .take(6) // bootstrap + 5 recent
        .collect();
    let recent_in_top = lines.iter().filter(|l| l.contains("RECENT-obs-")).count();
    assert!(recent_in_top >= 5, "all 5 recent observations must be in top ranks; found {recent_in_top} in top lines");
}

#[test]
fn test_relevance_mode_header_reflects_actual_sessions() {
    // The relevance header must show the count of sessions actually present in
    // the displayed results, not the requested sessions_back value.
    let tmpdir = tempfile::tempdir().unwrap();

    // Save observations — all go into the same session (single server invocation)
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "only session obs", Some("f::x"), None),
        // Request 10 sessions back but only 1 session exists
        get_history_request_with_sort(2, None, None, Some(10), Some("relevance")),
    ]);
    let text = extract_text(&responses[1]);

    // Header should say "from 1 sessions", not "from 10 sessions"
    assert!(
        text.contains("from 1 sessions"),
        "header must reflect actual session count (1), not requested (10); got: {text}"
    );
}

#[test]
fn test_relevance_mode_tiebreak_newest_first() {
    // At equal score and equal staleness, newer observations should rank first.
    let tmpdir = tempfile::tempdir().unwrap();

    // Bootstrap DB
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "bootstrap", Some("f::x"), None),
    ]);
    assert!(responses[0]["result"].is_object());

    // Insert two observations with identical age characteristics but different created_at
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;

    let session_id: String = conn.query_row(
        "SELECT session_id FROM observations LIMIT 1", [], |r| r.get(0),
    ).unwrap();

    // Both 3 days old, but OLDER is 1 hour older than NEWER
    // They'll have nearly identical scores, so tiebreak by created_at should apply
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated, is_stale, stale_reason) \
         VALUES (?1, ?2, 'insight', 'OLDER-tiebreak', NULL, NULL, 0, 0, NULL)",
        rusqlite::params![session_id, now - 3 * 86400 - 3600],
    ).unwrap();
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated, is_stale, stale_reason) \
         VALUES (?1, ?2, 'insight', 'NEWER-tiebreak', NULL, NULL, 0, 0, NULL)",
        rusqlite::params![session_id, now - 3 * 86400],
    ).unwrap();
    drop(conn);

    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request_with_sort(1, None, None, Some(5), Some("relevance")),
    ]);
    let text = extract_text(&responses[0]);

    let newer_pos = text.find("NEWER-tiebreak").expect("must contain NEWER-tiebreak");
    let older_pos = text.find("OLDER-tiebreak").expect("must contain OLDER-tiebreak");
    assert!(
        newer_pos < older_pos,
        "at equal score, newer must rank before older; newer@{newer_pos} older@{older_pos}"
    );
}

// ─── Story 8.2 Tests ──────────────────────────────────────────────────────────

#[test]
fn test_82_signature_change_stale_reason_includes_signatures() {
    // 6.1: Enhanced stale_reason with old→new signatures
    let tmpdir = setup_indexed_project_with_observation(
        "function greet() { return 'hi'; }\n",
        "greet returns greeting",
        Some("test.ts::greet"),
        None,
    );

    // Change signature
    std::fs::write(tmpdir.path().join("test.ts"), "function greet(name: string) { return name; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::greet"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("STALE"), "must be stale; got: {text}");
    assert!(text.contains("function greet()"), "reason must include old signature; got: {text}");
    assert!(text.contains("function greet(name: string)"), "reason must include new signature; got: {text}");
}

#[test]
fn test_82_rename_detection_end_to_end() {
    // 6.2: Rename detection requires identical (signature, kind) between removed+added FQNs.
    // In TS, function signatures include the name (e.g., "function foo() "), so renaming a
    // function always changes the signature. The rename path fires when FQNs change but
    // signatures stay identical — a narrow case that the unit tests cover thoroughly
    // (rename_detected_unique_sig_kind in diff.rs).
    //
    // This integration test verifies the observable end-to-end behavior: when a function
    // is renamed, the old FQN disappears and the observation is correctly marked stale.
    let tmpdir = setup_indexed_project_with_observation(
        "function oldName() { return 1; }\n",
        "oldName is important",
        Some("test.ts::oldName"),
        None,
    );

    std::fs::write(tmpdir.path().join("test.ts"), "function newName() { return 1; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::oldName"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("STALE"), "must be stale after rename; got: {text}");
    // TS function rename changes signature, so falls back to "no longer exists"
    assert!(
        text.contains("no longer exists"),
        "TS function rename (signature changes) must use 'no longer exists'; got: {text}"
    );
}

#[test]
fn test_82_ambiguous_rename_falls_back() {
    // 6.3: 2 removed + 2 added with same sig → no rename, just remove+add
    let tmpdir = setup_indexed_project_with_observation(
        "function alpha() { return 1; }\nfunction beta() { return 2; }\n",
        "alpha is entry point",
        Some("test.ts::alpha"),
        None,
    );

    // Remove both, add two new with same signature pattern
    std::fs::write(tmpdir.path().join("test.ts"), "function gamma() { return 3; }\nfunction delta() { return 4; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::alpha"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("STALE"), "must be stale; got: {text}");
    assert!(
        text.contains("no longer exists"),
        "ambiguous rename must fall back to 'no longer exists'; got: {text}"
    );
}

#[test]
fn test_82_file_path_obs_backtick_reference_stale() {
    // 6.4: File-path observation with backtick-quoted symbol reference → stale
    let tmpdir = setup_indexed_project_with_observation(
        "function render() { return '<div>'; }\n",
        "The `render` function creates HTML",
        None,
        Some("test.ts"),
    );

    // Change render's signature
    std::fs::write(tmpdir.path().join("test.ts"), "function render(tag: string) { return tag; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, None, Some("test.ts"), Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(
        text.contains("STALE"),
        "file-path obs with backtick reference must be stale; got: {text}"
    );
    assert!(
        text.contains("Referenced symbol"),
        "reason must mention 'Referenced symbol'; got: {text}"
    );
}

#[test]
fn test_82_file_path_obs_no_backtick_not_stale() {
    // 6.5: File-path observation WITHOUT backtick reference → NOT stale
    let tmpdir = setup_indexed_project_with_observation(
        "function render() { return '<div>'; }\n",
        "The render function creates HTML output for the page",
        None,
        Some("test.ts"),
    );

    // Change render's signature
    std::fs::write(tmpdir.path().join("test.ts"), "function render(tag: string) { return tag; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, None, Some("test.ts"), Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(
        !text.contains("STALE"),
        "file-path obs without backtick reference must NOT be stale; got: {text}"
    );
}

#[test]
fn test_82_backward_compat_symbol_removal() {
    // 6.6: Existing behavior preserved for symbol removal
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\nfunction world() { return 2; }\n",
        "hello is the main function",
        Some("test.ts::hello"),
        None,
    );

    // Remove hello entirely
    std::fs::write(tmpdir.path().join("test.ts"), "function world() { return 2; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("STALE"), "must be stale; got: {text}");
    assert!(
        text.contains("no longer exists"),
        "reason must say 'no longer exists'; got: {text}"
    );
}

#[test]
fn test_82_body_only_no_staleness_any_type() {
    // 6.7: Body-only changes don't trigger staleness for any observation type
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("test.ts"), "function hello() { return 1; }\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .unwrap();
    assert!(output.status.success());

    // Save both a symbol-linked and file-path observation
    run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "hello is fast", Some("test.ts::hello"), None),
        save_obs_request(2, "insight", "The `hello` function is fast", None, Some("test.ts")),
    ]);

    // Body-only change (same signature)
    std::fs::write(tmpdir.path().join("test.ts"), "function hello() { return 999; }\n").unwrap();

    let responses = run_requests_in(tmpdir.path(), &[
        get_context_request(1, &["test.ts"]),
        get_history_request(2, Some("test.ts::hello"), None, Some(2)),
        get_history_request(3, None, Some("test.ts"), Some(2)),
    ]);
    let text1 = extract_text(&responses[1]);
    let text2 = extract_text(&responses[2]);
    assert!(!text1.contains("STALE"), "symbol-linked obs must not be stale on body-only change; got: {text1}");
    assert!(!text2.contains("STALE"), "file-path obs must not be stale on body-only change; got: {text2}");
}

#[test]
fn test_82_first_reason_wins() {
    // 6.8: Already-stale observation doesn't get overwritten
    let tmpdir = setup_indexed_project_with_observation(
        "function hello() { return 1; }\n",
        "hello returns 1",
        Some("test.ts::hello"),
        None,
    );

    // First change: signature change → stale
    std::fs::write(tmpdir.path().join("test.ts"), "function hello(x: number) { return x; }\n").unwrap();
    run_requests_in(tmpdir.path(), &[get_context_request(1, &["test.ts"])]);

    // Second change: remove symbol entirely
    std::fs::write(tmpdir.path().join("test.ts"), "function other() { return 2; }\n").unwrap();
    run_requests_in(tmpdir.path(), &[get_context_request(1, &["test.ts"])]);

    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, Some("test.ts::hello"), None, Some(2)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("STALE"), "must be stale; got: {text}");
    // First reason was signature change, not "no longer exists"
    assert!(
        text.contains("Signature of symbol"),
        "first reason must be preserved (signature change, not removal); got: {text}"
    );
}

#[test]
fn test_82_full_reindex_rename_detection() {
    // 6.9: Full reindex also triggers rename detection
    let tmpdir = setup_indexed_project_with_observation(
        "function oldFunc() { return 1; }\n",
        "oldFunc is important",
        Some("test.ts::oldFunc"),
        None,
    );

    // Rename: remove old, add new with same signature
    std::fs::write(tmpdir.path().join("test.ts"), "function newFunc() { return 1; }\n").unwrap();

    // Full re-index via CLI (not incremental)
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .unwrap();
    assert!(output.status.success());

    let responses = run_requests_in(tmpdir.path(), &[
        get_history_request(1, Some("test.ts::oldFunc"), None, Some(2)),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("STALE"), "full reindex must mark stale after rename; got: {text}");
    // TS function rename changes signature, so falls back to "no longer exists"
    assert!(
        text.contains("no longer exists"),
        "TS function rename (signature changes) must use 'no longer exists'; got: {text}"
    );
}

// ─── Story 10.1: Dead-end detection end-to-end via hook harness ──────────────

#[test]
fn test_dead_end_detection_via_hook_harness() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "dead-end-e2e";

    // Post two get_context events with the same intent
    for _ in 0..2 {
        let payload = serde_json::json!({
            "session_id": session_id,
            "cwd": cwd,
            "hook_event_name": "PostToolUse",
            "tool_name": "mcp__olaf__get_context",
            "tool_input": {
                "intent": "refactoring auth module"
            }
        });
        let output = run_observe_event("post-tool-use", &tmpdir, &payload);
        assert!(output.status.success(), "post-tool-use must exit 0; stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    // Trigger session-end to run dead-end detection
    let end_payload = make_session_end_payload(session_id, cwd);
    let output = run_observe_event("session-end", &tmpdir, &end_payload);
    assert!(output.status.success(), "session-end must exit 0; stderr: {}", String::from_utf8_lossy(&output.stderr));

    // Verify anti_pattern observation with "Dead-end" was written
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let content: String = conn
        .query_row(
            "SELECT content FROM observations WHERE session_id = ?1 AND kind = 'anti_pattern'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .expect("anti_pattern observation must exist");
    assert!(content.contains("Dead-end"), "content must mention Dead-end; got: {content}");
    assert!(content.contains("refactoring auth module"), "content must mention the repeated intent; got: {content}");
}

// get_brief records context_retrieval with "get_context:" prefix (same as get_context)
#[test]
fn test_context_retrieval_get_brief() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "brief-e2e";
    let payload = serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "mcp__olaf__get_brief",
        "tool_input": { "intent": "adding caching layer" }
    });
    let output = run_observe_event("post-tool-use", &tmpdir, &payload);
    assert!(output.status.success());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let content: String = conn
        .query_row(
            "SELECT content FROM observations WHERE session_id = ?1 AND kind = 'context_retrieval'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .expect("context_retrieval observation must exist for get_brief");
    assert_eq!(content, "get_context: adding caching layer");
}

// get_impact records context_retrieval with "get_impact:" prefix using symbol_fqn
#[test]
fn test_context_retrieval_get_impact() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "impact-e2e";
    let payload = serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "mcp__olaf__get_impact",
        "tool_input": { "symbol_fqn": "src/db.rs::connect" }
    });
    let output = run_observe_event("post-tool-use", &tmpdir, &payload);
    assert!(output.status.success());

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let content: String = conn
        .query_row(
            "SELECT content FROM observations WHERE session_id = ?1 AND kind = 'context_retrieval'",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .expect("context_retrieval observation must exist for get_impact");
    assert_eq!(content, "get_impact: src/db.rs::connect");
}

// Malformed payload (missing required field) → no observation written, exit 0
#[test]
fn test_context_retrieval_malformed_payload_skipped() {
    let tmpdir = tempfile::tempdir().unwrap();
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let _conn = olaf::db::open(&db_path).unwrap();
    drop(_conn);

    let cwd = tmpdir.path().to_str().unwrap();
    let session_id = "malformed-e2e";

    // get_context without "intent" field
    let payload = serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "mcp__olaf__get_context",
        "tool_input": { "wrong_field": "value" }
    });
    let output = run_observe_event("post-tool-use", &tmpdir, &payload);
    assert!(output.status.success(), "malformed payload must not crash");

    // get_impact without "symbol_fqn" field
    let payload2 = serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PostToolUse",
        "tool_name": "mcp__olaf__get_impact",
        "tool_input": { "fqn": "wrong_name" }
    });
    let output2 = run_observe_event("post-tool-use", &tmpdir, &payload2);
    assert!(output2.status.success(), "malformed get_impact payload must not crash");

    // No observations should have been written
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1",
            rusqlite::params![session_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "malformed payloads must not produce observations");
}

// ─── Story 10.2 Integration Tests ────────────────────────────────────────────

#[test]
fn test_get_context_bm25_ranking_with_intent() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Single file with one symbol — both observations link to same symbol/file
    // so they both appear in Session Memory regardless of pivot selection
    std::fs::write(tmpdir.path().join("app.ts"), "function handler() { return true; }\n").unwrap();
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["index"])
        .current_dir(tmpdir.path())
        .output()
        .expect("index must succeed");

    // Save two observations on the same symbol: one matches the intent, one does not
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "cache invalidation causes stale reads in production", Some("app.ts::handler"), None),
        save_obs_request(2, "insight", "authentication uses JWT tokens with refresh rotation", Some("app.ts::handler"), None),
        serde_json::json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "get_context",
                "arguments": {
                    "intent": "fix authentication JWT token refresh bug",
                    "file_hints": ["app.ts"],
                    "token_budget": 8000
                }
            }
        }),
    ]);
    let text = extract_text(&responses[2]);
    // Session Memory must exist and contain both observations
    assert!(text.contains("## Session Memory"), "must include Session Memory section; got: {text}");
    assert!(text.contains("authentication uses JWT"), "must include auth observation; got: {text}");
    assert!(text.contains("cache invalidation"), "must include cache observation; got: {text}");

    // BM25 must rank auth observation above cache observation for auth-related intent
    let auth_pos = text.find("authentication uses JWT").unwrap();
    let cache_pos = text.find("cache invalidation").unwrap();
    assert!(auth_pos < cache_pos,
        "BM25 must rank auth observation above cache observation for auth-related intent; auth@{auth_pos} cache@{cache_pos}");
}

#[test]
fn test_get_session_history_includes_signal_label() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "signal format test", Some("f::foo"), None),
        get_history_request_with_sort(2, None, None, None, Some("session")),
    ]);
    let text = extract_text(&responses[1]);
    // Output must contain the "· signal" format: [score: X.XX · recency] or similar
    assert!(text.contains("\u{00b7}"), "session mode must include · signal separator; got: {text}");
    // Must contain one of the valid signal types
    let has_signal = text.contains("recency") || text.contains("confidence") || text.contains("fts") || text.contains("stale");
    assert!(has_signal, "session mode must include a primary signal label; got: {text}");
}

#[test]
fn test_relevance_mode_includes_signal_label() {
    let tmpdir = tempfile::tempdir().unwrap();
    let responses = run_requests_in(tmpdir.path(), &[
        save_obs_request(1, "insight", "relevance signal test", Some("f::bar"), None),
        get_history_request_with_sort(2, None, None, None, Some("relevance")),
    ]);
    let text = extract_text(&responses[1]);
    assert!(text.contains("\u{00b7}"), "relevance mode must include · signal separator; got: {text}");
    let has_signal = text.contains("recency") || text.contains("confidence") || text.contains("fts") || text.contains("stale");
    assert!(has_signal, "relevance mode must include a primary signal label; got: {text}");
}
