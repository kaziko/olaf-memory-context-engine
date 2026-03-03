// Restore integration tests — Story 4.3

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
