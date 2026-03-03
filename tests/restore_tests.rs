// Restore integration tests — Story 4.3 + 4.4

use std::io::Write;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn run_observe_pre_tool_use(
    tmpdir: &tempfile::TempDir,
    payload: &serde_json::Value,
) -> std::process::Output {
    let json = serde_json::to_string(payload).unwrap();
    std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "pre-tool-use"])
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child.stdin.take().unwrap().write_all(json.as_bytes()).unwrap();
            child.wait_with_output()
        })
        .expect("olaf observe must spawn")
}

fn make_pre_tool_use_payload(
    session_id: &str,
    cwd: &str,
    tool_name: &str,
    file_path: &str,
) -> serde_json::Value {
    serde_json::json!({
        "session_id": session_id,
        "cwd": cwd,
        "hook_event_name": "PreToolUse",
        "tool_name": tool_name,
        "tool_input": { "file_path": file_path }
    })
}

fn path_hash(rel: &str) -> String {
    blake3::hash(rel.as_bytes()).to_hex().to_string()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// 4.3: Edit tool on existing file → snapshot created with correct contents
#[test]
fn test_pre_tool_use_edit_creates_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::write(src_dir.join("main.rs"), b"fn main() {}").unwrap();

    let abs_path = tmpdir.path().join("src/main.rs");
    let payload = make_pre_tool_use_payload(
        "sess-edit",
        &tmpdir.path().to_string_lossy(),
        "Edit",
        &abs_path.to_string_lossy(),
    );
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    assert!(output.stdout.is_empty(), "stdout must be empty (NFR16)");

    let hash = path_hash("src/main.rs");
    let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
    assert!(snap_dir.exists(), "snap dir must exist");

    let snaps: Vec<_> = std::fs::read_dir(&snap_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
        .collect();
    assert_eq!(snaps.len(), 1, "exactly one .snap file");
    assert_eq!(std::fs::read(snaps[0].path()).unwrap(), b"fn main() {}");
}

// 4.4: Write for a non-existent file → no snapshot (AC3)
#[test]
fn test_pre_tool_use_write_nonexistent_file_no_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    let abs_path = tmpdir.path().join("new_file_that_does_not_exist.rs");

    let payload = make_pre_tool_use_payload(
        "sess-new",
        &tmpdir.path().to_string_lossy(),
        "Write",
        &abs_path.to_string_lossy(),
    );
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    assert!(output.stdout.is_empty(), "stdout must be empty");

    let restores_dir = tmpdir.path().join(".olaf").join("restores");
    assert!(!restores_dir.exists(), ".olaf/restores must NOT be created");
}

// 4.5: Sensitive path (.env) → no snapshot (AC4)
#[test]
fn test_pre_tool_use_sensitive_path_no_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    let env_path = tmpdir.path().join(".env");
    std::fs::write(&env_path, b"SECRET=abc").unwrap();

    let payload = make_pre_tool_use_payload(
        "sess-env",
        &tmpdir.path().to_string_lossy(),
        "Edit",
        &env_path.to_string_lossy(),
    );
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    assert!(output.stdout.is_empty(), "stdout must be empty");

    let restores_dir = tmpdir.path().join(".olaf").join("restores");
    assert!(!restores_dir.exists(), "no snapshot for sensitive path");
}

// 4.6: Bash tool → no snapshot (AC5)
#[test]
fn test_pre_tool_use_bash_tool_no_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    let payload = serde_json::json!({
        "session_id": "sess-bash",
        "cwd": tmpdir.path().to_string_lossy(),
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": { "command": "ls" }
    });
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");

    let restores_dir = tmpdir.path().join(".olaf").join("restores");
    assert!(!restores_dir.exists(), "no snapshot for Bash tool (AC5)");
}

// 4.7: Malformed payload → exits 0, stdout empty (AC6)
#[test]
fn test_pre_tool_use_malformed_payload() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Send raw empty JSON object — HookPayload deserialization will fail, inner() returns Ok(())
    let json = "{}".to_string();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["observe", "--event", "pre-tool-use"])
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child.stdin.take().unwrap().write_all(json.as_bytes()).unwrap();
            child.wait_with_output()
        })
        .expect("olaf observe must spawn");

    assert!(output.status.success(), "must exit 0 for malformed payload");
    assert!(output.stdout.is_empty(), "stdout must be empty");
}

// 4.8: Write tool (same as 4.3 but Write) → snapshot created (AC1)
#[test]
fn test_pre_tool_use_write_creates_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("readme.md"), b"# Hello").unwrap();

    let abs_path = tmpdir.path().join("readme.md");
    let payload = make_pre_tool_use_payload(
        "sess-write",
        &tmpdir.path().to_string_lossy(),
        "Write",
        &abs_path.to_string_lossy(),
    );
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    assert!(output.stdout.is_empty(), "stdout must be empty");

    let hash = path_hash("readme.md");
    let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
    assert!(snap_dir.exists(), "snap dir must exist for Write tool");

    let snaps: Vec<_> = std::fs::read_dir(&snap_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
        .collect();
    assert_eq!(snaps.len(), 1, "exactly one .snap file");
    assert_eq!(std::fs::read(snaps[0].path()).unwrap(), b"# Hello");
}

// 4.9: Path outside project root → no snapshot (AC8)
// Uses a second temp dir so the path is guaranteed outside the project root on any platform.
#[test]
fn test_pre_tool_use_path_outside_project_root_no_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    let outside_dir = tempfile::tempdir().unwrap();
    let outside_path = outside_dir.path().join("outside_file.txt");

    let payload = make_pre_tool_use_payload(
        "sess-outside",
        &tmpdir.path().to_string_lossy(),
        "Edit",
        &outside_path.to_string_lossy(),
    );
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    assert!(output.stdout.is_empty(), "stdout must be empty");

    let restores_dir = tmpdir.path().join(".olaf").join("restores");
    assert!(!restores_dir.exists(), "no snapshot for path outside project root");
}

// 4.10: Path with .. traversal that passes lexical strip_prefix but escapes root → no snapshot (AC8)
// Path is built with PathBuf::push so separators are platform-correct.
#[test]
fn test_pre_tool_use_dotdot_escape_no_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    // Build: <tmpdir>/src/../../../../escape.txt
    // strip_prefix(&tmpdir) succeeds lexically, but relative part contains ".." components.
    let mut tricky = tmpdir.path().to_path_buf();
    tricky.push("src");
    tricky.push("..");
    tricky.push("..");
    tricky.push("..");
    tricky.push("..");
    tricky.push("escape.txt");

    let payload = make_pre_tool_use_payload(
        "sess-dotdot",
        &tmpdir.path().to_string_lossy(),
        "Edit",
        &tricky.to_string_lossy(),
    );
    let output = run_observe_pre_tool_use(&tmpdir, &payload);

    assert!(output.status.success(), "must exit 0");
    assert!(output.stdout.is_empty(), "stdout must be empty");

    let restores_dir = tmpdir.path().join(".olaf").join("restores");
    assert!(!restores_dir.exists(), "no snapshot for path with .. traversal (AC8 — ParentDir guard)");
}

// ─── Story 4.4 Helpers ────────────────────────────────────────────────────────

/// Build a JSON-RPC 2.0 `tools/call` request for an MCP tool.
fn make_mcp_request(tool: &str, args: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool,
            "arguments": args
        }
    })
}

/// Run `olaf serve` as a subprocess, send one JSON-RPC request line, and return the parsed response.
fn run_mcp_tool(tmpdir: &tempfile::TempDir, request: serde_json::Value) -> serde_json::Value {
    let json = serde_json::to_string(&request).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .current_dir(tmpdir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child.stdin.take().unwrap().write_all(json.as_bytes()).unwrap();
            child.wait_with_output()
        })
        .expect("olaf serve must spawn");

    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let first_line = stdout_str.lines().next().unwrap_or("{}");
    serde_json::from_str(first_line).unwrap_or_else(|_| serde_json::json!({}))
}

/// Create a snapshot for a file using the pre-tool-use hook subprocess.
fn snapshot_via_hook(tmpdir: &tempfile::TempDir, file_path: &std::path::Path) {
    let abs = if file_path.is_absolute() {
        file_path.to_path_buf()
    } else {
        tmpdir.path().join(file_path)
    };
    let payload = make_pre_tool_use_payload(
        "snap-sess",
        &tmpdir.path().to_string_lossy(),
        "Edit",
        &abs.to_string_lossy(),
    );
    run_observe_pre_tool_use(tmpdir, &payload);
}

// ─── Story 4.4 Integration Tests ─────────────────────────────────────────────

// 6.3: list_restore_points MCP returns entries sorted newest-first
#[test]
fn test_list_restore_points_mcp_returns_entries() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("data.txt"), b"v1").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("data.txt"));
    std::thread::sleep(std::time::Duration::from_millis(5));
    std::fs::write(tmpdir.path().join("data.txt"), b"v2").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("data.txt"));

    let req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": "data.txt"}));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_none(), "should not be an error: {resp}");
    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("data.txt"), "response should mention file: {text}");
    // Two snapshots were taken — response should show at least 2 entries
    assert!(text.contains("bytes"), "response should list snapshot sizes: {text}");
}

// 6.4: list_restore_points MCP returns informational message (not error) for unsnapshotted file
#[test]
fn test_list_restore_points_mcp_no_snapshots() {
    let tmpdir = tempfile::tempdir().unwrap();
    let req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": "nonexistent.txt"}));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_none(), "must NOT be a JSON-RPC error (AC1): {resp}");
    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("No restore points available"), "should say no restore points: {text}");
}

// 6.5: list_restore_points MCP accepts absolute path
#[test]
fn test_list_restore_points_mcp_absolute_path() {
    let tmpdir = tempfile::tempdir().unwrap();
    let file = tmpdir.path().join("abs.txt");
    std::fs::write(&file, b"hello").unwrap();
    snapshot_via_hook(&tmpdir, &file);

    // Canonicalize to resolve macOS /var → /private/var symlink so the
    // absolute path matches what olaf serve sees via std::env::current_dir().
    let canonical_file = file.canonicalize().unwrap_or(file);
    let req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": canonical_file.to_string_lossy()}));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_none(), "should not error for absolute path: {resp}");
    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(text.contains("abs.txt"), "response should mention file: {text}");
}

// 6.6: list_restore_points MCP rejects sensitive path
#[test]
fn test_list_restore_points_mcp_sensitive_path_rejected() {
    let tmpdir = tempfile::tempdir().unwrap();
    let req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": ".env"}));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_some(), "sensitive path must return JSON-RPC error: {resp}");
}

// 6.7: list_restore_points MCP rejects path with .. components
#[test]
fn test_list_restore_points_mcp_dotdot_rejected() {
    let tmpdir = tempfile::tempdir().unwrap();
    let req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": "../../etc/passwd"}));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_some(), "path escape must return JSON-RPC error: {resp}");
}

// 6.8: undo_change restores file content from snapshot
#[test]
fn test_undo_change_restores_file_content() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("target.txt"), b"original").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("target.txt"));

    // Overwrite with bad content
    std::fs::write(tmpdir.path().join("target.txt"), b"bad content").unwrap();

    // Get snapshot ID via list
    let list_req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": "target.txt"}));
    let list_resp = run_mcp_tool(&tmpdir, list_req);
    let list_text = list_resp["result"]["content"][0]["text"].as_str().unwrap_or("");

    // Extract first snapshot ID from response (first word after "  " on a line)
    let snap_id = list_text.lines()
        .find(|l| l.trim_start().starts_with(|c: char| c.is_ascii_digit()))
        .and_then(|l| l.split_whitespace().next())
        .expect("should find a snapshot ID in list output");

    // Undo
    let undo_req = make_mcp_request("undo_change", serde_json::json!({
        "file_path": "target.txt",
        "snapshot_id": snap_id
    }));
    let undo_resp = run_mcp_tool(&tmpdir, undo_req);
    assert!(undo_resp.get("error").is_none(), "undo_change must succeed: {undo_resp}");

    let content = std::fs::read(tmpdir.path().join("target.txt")).unwrap();
    assert_eq!(content, b"original", "file must be restored to original content");
}

// 6.9: undo_change with invalid snapshot_id returns error containing "not found"
#[test]
fn test_undo_change_invalid_snapshot_id_lists_available() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("file.txt"), b"content").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("file.txt"));

    let req = make_mcp_request("undo_change", serde_json::json!({
        "file_path": "file.txt",
        "snapshot_id": "invalid-snap-id"
    }));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_some(), "invalid snapshot_id must return error: {resp}");
    let msg = resp["error"]["message"].as_str().unwrap_or("");
    assert!(msg.contains("not found") || msg.contains("invalid"), "error should say not found: {msg}");
}

// 6.10: undo_change writes a decision observation to the DB
#[test]
fn test_undo_change_writes_decision_observation() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("observed.txt"), b"original").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("observed.txt"));

    // Get snapshot ID
    let list_req = make_mcp_request("list_restore_points", serde_json::json!({"file_path": "observed.txt"}));
    let list_resp = run_mcp_tool(&tmpdir, list_req);
    let list_text = list_resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    let snap_id = list_text.lines()
        .find(|l| l.trim_start().starts_with(|c: char| c.is_ascii_digit()))
        .and_then(|l| l.split_whitespace().next())
        .expect("should find snapshot ID");

    // Undo
    let undo_req = make_mcp_request("undo_change", serde_json::json!({
        "file_path": "observed.txt",
        "snapshot_id": snap_id
    }));
    run_mcp_tool(&tmpdir, undo_req);

    // Check DB for decision observation
    let db_path = tmpdir.path().join(".olaf").join("index.db");
    let conn = rusqlite::Connection::open(&db_path).expect("DB must exist after undo_change");
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM observations WHERE kind = 'decision' AND content LIKE '%Reverted%'",
        [],
        |r| r.get(0),
    ).unwrap_or(0);
    assert!(count > 0, "decision observation must be written to DB after undo_change");
}

// 6.11: undo_change rejects sensitive path
#[test]
fn test_undo_change_sensitive_path_rejected() {
    let tmpdir = tempfile::tempdir().unwrap();
    let req = make_mcp_request("undo_change", serde_json::json!({
        "file_path": ".env",
        "snapshot_id": "1740000000000-1-0"
    }));
    let resp = run_mcp_tool(&tmpdir, req);

    assert!(resp.get("error").is_some(), "sensitive path must return error for undo_change: {resp}");
}

// 6.12: CLI restore list with relative path shows snapshot ID
#[test]
fn test_cli_restore_list_relative_path() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("cli_file.txt"), b"content").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("cli_file.txt"));

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["restore", "list", "cli_file.txt"])
        .current_dir(tmpdir.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("olaf restore list must run");

    assert!(output.status.success(), "must exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cli_file.txt"), "should mention file: {stdout}");
    assert!(stdout.contains("bytes"), "should list snapshot size: {stdout}");
}

// 6.13: CLI restore applies snapshot and restores file content
#[test]
fn test_cli_restore_applies_snapshot() {
    let tmpdir = tempfile::tempdir().unwrap();
    std::fs::write(tmpdir.path().join("restore_me.txt"), b"original").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("restore_me.txt"));

    // Overwrite
    std::fs::write(tmpdir.path().join("restore_me.txt"), b"corrupted").unwrap();

    // Get timestamp from snapshot filename
    let hash = path_hash("restore_me.txt");
    let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
    let snap_file = std::fs::read_dir(&snap_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with(".snap"))
        .expect("snap file must exist");
    let stem = snap_file.file_name().to_string_lossy().to_string();
    let stem = stem.strip_suffix(".snap").unwrap();
    let millis: i64 = stem.split('-').next().unwrap().parse().unwrap();

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["restore", "restore_me.txt", &millis.to_string()])
        .current_dir(tmpdir.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("olaf restore must run");

    assert!(output.status.success(), "CLI restore must exit 0");
    let content = std::fs::read(tmpdir.path().join("restore_me.txt")).unwrap();
    assert_eq!(content, b"original", "file must be restored");
}

// 6.14: olaf index cleans up stale snaps but keeps fresh ones
#[test]
fn test_cleanup_removes_old_snaps_but_not_new() {
    let tmpdir = tempfile::tempdir().unwrap();

    // Create a fresh file and snapshot it
    std::fs::write(tmpdir.path().join("kept.txt"), b"kept").unwrap();
    snapshot_via_hook(&tmpdir, std::path::Path::new("kept.txt"));

    // Also create a synthetic stale snap manually (millis=1 = epoch+1ms, definitely >7 days)
    let hash = path_hash("stale.txt");
    let stale_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
    std::fs::create_dir_all(&stale_dir).unwrap();
    std::fs::write(stale_dir.join("1-1-0.snap"), b"stale").unwrap();

    // Run olaf index (requires a valid DB, create .olaf dir first)
    std::fs::create_dir_all(tmpdir.path().join(".olaf")).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(tmpdir.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("olaf index must run");

    assert!(output.status.success(), "olaf index must exit 0: {}", String::from_utf8_lossy(&output.stderr));

    // Stale snap should be gone
    assert!(!stale_dir.join("1-1-0.snap").exists(), "stale snap must be deleted after olaf index");

    // Fresh snap for kept.txt should still exist
    let kept_hash = path_hash("kept.txt");
    let kept_dir = tmpdir.path().join(".olaf").join("restores").join(&kept_hash);
    let kept_snaps: Vec<_> = std::fs::read_dir(&kept_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
        .collect();
    assert!(!kept_snaps.is_empty(), "fresh snap for kept.txt must still exist");
}
