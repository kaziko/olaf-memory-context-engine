use std::fs;
use std::process::Command;

fn olaf() -> Command {
    Command::new(env!("CARGO_BIN_EXE_olaf"))
}

fn run_init(dir: &std::path::Path) -> std::process::Output {
    olaf()
        .arg("init")
        .current_dir(dir)
        .output()
        .expect("failed to run olaf init")
}

#[test]
fn test_init_creates_olaf_dir() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);
    assert!(dir.path().join(".olaf").is_dir(), ".olaf/ must be created");
}

#[test]
fn test_init_adds_gitignore_entry() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);
    let content = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
    assert!(
        content.lines().any(|l| l.trim() == ".olaf/"),
        ".gitignore must contain .olaf/ line"
    );
}

#[test]
fn test_init_idempotent_gitignore() {
    let dir = tempfile::tempdir().unwrap();
    run_init(dir.path());
    run_init(dir.path());
    let content = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
    let count = content.lines().filter(|l| l.trim() == ".olaf/").count();
    assert_eq!(count, 1, ".olaf/ must appear exactly once in .gitignore, got {count}");
}

#[test]
fn test_init_creates_mcp_json() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);
    let content = fs::read_to_string(dir.path().join(".mcp.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();
    let cmd = json["mcpServers"]["olaf"]["command"].as_str();
    assert!(cmd.is_some(), ".mcp.json must have mcpServers.olaf.command");
    assert!(!cmd.unwrap().is_empty(), "command must not be empty");
}

#[test]
fn test_init_idempotent_mcp_same_binary() {
    let dir = tempfile::tempdir().unwrap();
    // First run: creates .mcp.json
    run_init(dir.path());
    let mtime_before = fs::metadata(dir.path().join(".mcp.json"))
        .unwrap()
        .modified()
        .unwrap();

    // Second run: should be AlreadyCurrent — no backup created
    run_init(dir.path());

    let backups: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".mcp.json.bak.")
        })
        .collect();
    assert_eq!(backups.len(), 0, "no backup should be created on idempotent run");

    let mtime_after = fs::metadata(dir.path().join(".mcp.json"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(mtime_before, mtime_after, ".mcp.json must not be rewritten on idempotent run");
}

#[test]
fn test_init_reconcile_mcp_stale_binary() {
    let dir = tempfile::tempdir().unwrap();
    // Pre-seed with a different binary path
    let stale_mcp = serde_json::json!({
        "mcpServers": {
            "olaf": {
                "command": "/tmp/fake-olaf-stale",
                "args": ["serve"],
                "type": "stdio"
            }
        }
    });
    fs::write(
        dir.path().join(".mcp.json"),
        serde_json::to_string_pretty(&stale_mcp).unwrap() + "\n",
    )
    .unwrap();

    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);

    // Verify command was updated to current binary
    let content = fs::read_to_string(dir.path().join(".mcp.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();
    let cmd = json["mcpServers"]["olaf"]["command"].as_str().unwrap();
    assert_ne!(cmd, "/tmp/fake-olaf-stale", "stale command must be replaced");

    // Verify a backup was created
    let backups: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".mcp.json.bak.")
        })
        .collect();
    assert!(!backups.is_empty(), "a backup must exist after reconciling stale binary");
}

#[test]
fn test_init_creates_backup_when_mcp_exists() {
    let dir = tempfile::tempdir().unwrap();
    // Pre-seed with an existing .mcp.json containing a different olaf path
    let existing = serde_json::json!({"mcpServers": {"olaf": {"command": "/old/path/olaf"}}});
    fs::write(
        dir.path().join(".mcp.json"),
        serde_json::to_string_pretty(&existing).unwrap() + "\n",
    )
    .unwrap();

    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);

    let backups: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".mcp.json.bak.")
        })
        .collect();
    assert!(!backups.is_empty(), ".mcp.json.bak.<ts> must exist after modifying existing file");
}

#[test]
fn test_init_installs_hooks() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);

    let settings_path = dir.path().join(".claude/settings.local.json");
    assert!(settings_path.exists(), "settings.local.json must be created");
    let content = fs::read_to_string(&settings_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    let empty: Vec<serde_json::Value> = vec![];
    for event in ["PostToolUse", "PreToolUse", "SessionEnd"] {
        let entries = json["hooks"][event].as_array().unwrap_or(&empty);
        let has_hook = entries.iter().any(|entry| {
            entry["hooks"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .any(|h| {
                    h["command"]
                        .as_str()
                        .map(|c| c.contains("olaf observe"))
                        .unwrap_or(false)
                })
        });
        assert!(has_hook, "hook for {event} must be installed");
    }
}

#[test]
fn test_init_idempotent_hooks() {
    let dir = tempfile::tempdir().unwrap();
    run_init(dir.path());
    run_init(dir.path());

    let content = fs::read_to_string(dir.path().join(".claude/settings.local.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    let empty: Vec<serde_json::Value> = vec![];
    for event in ["PostToolUse", "PreToolUse", "SessionEnd"] {
        let entries = json["hooks"][event].as_array().unwrap_or(&empty);
        let olaf_count: usize = entries
            .iter()
            .flat_map(|entry| entry["hooks"].as_array().into_iter().flatten())
            .filter(|h| {
                h["command"]
                    .as_str()
                    .map(|c| c.contains("olaf observe"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(olaf_count, 1, "exactly one olaf hook for {event}, got {olaf_count}");
    }
}

#[test]
fn test_init_corrupt_mcp_json() {
    let dir = tempfile::tempdir().unwrap();
    // Pre-seed with a non-object JSON root (array)
    fs::write(dir.path().join(".mcp.json"), "[1,2,3]\n").unwrap();

    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init must not panic on corrupt .mcp.json: {:?}", out);

    // .mcp.json must now be a valid object with the olaf entry
    let content = fs::read_to_string(dir.path().join(".mcp.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(json.is_object(), ".mcp.json must be a JSON object after repair");
    assert!(
        json["mcpServers"]["olaf"]["command"].as_str().is_some(),
        "mcpServers.olaf.command must exist after repair"
    );
}

#[test]
fn test_status_shows_mcp_path_and_hooks() {
    let dir = tempfile::tempdir().unwrap();
    // Initialize first
    let init_out = run_init(dir.path());
    assert!(init_out.status.success(), "olaf init failed: {:?}", init_out);

    let status_out = olaf()
        .arg("status")
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf status");
    assert!(status_out.status.success(), "olaf status failed: {:?}", status_out);

    let stdout = String::from_utf8_lossy(&status_out.stdout);
    // Must show absolute path to .mcp.json
    assert!(
        stdout.contains(".mcp.json"),
        "status must contain .mcp.json path; got:\n{stdout}"
    );
    assert!(
        stdout.contains("MCP status:"),
        "status must contain MCP status line; got:\n{stdout}"
    );
    assert!(
        stdout.contains("Hook PostToolUse:"),
        "status must contain Hook PostToolUse line; got:\n{stdout}"
    );
    assert!(
        stdout.contains("Hook PreToolUse:"),
        "status must contain Hook PreToolUse line; got:\n{stdout}"
    );
    assert!(
        stdout.contains("Hook SessionEnd:"),
        "status must contain Hook SessionEnd line; got:\n{stdout}"
    );
}

/// Sibling non-Olaf commands within the same hook entry must be preserved when a stale
/// Olaf hook is reconciled (Finding #1 from code review).
#[test]
fn test_init_reconcile_stale_hook_preserves_sibling_commands() {
    let dir = tempfile::tempdir().unwrap();

    // Pre-seed settings.local.json: PostToolUse has a stale olaf hook AND a sibling command.
    let stale = serde_json::json!({
        "hooks": {
            "PostToolUse": [
                {
                    "matcher": "",
                    "hooks": [
                        {"type": "command", "command": "/old/path/olaf observe --event post-tool-use"},
                        {"type": "command", "command": "echo KEEP_ME"}
                    ]
                }
            ]
        }
    });
    fs::create_dir_all(dir.path().join(".claude")).unwrap();
    fs::write(
        dir.path().join(".claude/settings.local.json"),
        serde_json::to_string_pretty(&stale).unwrap() + "\n",
    )
    .unwrap();

    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);

    let content = fs::read_to_string(dir.path().join(".claude/settings.local.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    // Sibling echo command must still be present somewhere in PostToolUse
    let post_entries = json["hooks"]["PostToolUse"].as_array().unwrap();
    let has_sibling = post_entries.iter().flat_map(|e| {
        e["hooks"].as_array().into_iter().flatten()
    }).any(|h| h["command"].as_str() == Some("echo KEEP_ME"));
    assert!(has_sibling, "sibling 'echo KEEP_ME' must be preserved after stale hook reconciliation");

    // Stale olaf path must be gone
    let has_stale = post_entries.iter().flat_map(|e| {
        e["hooks"].as_array().into_iter().flatten()
    }).any(|h| {
        h["command"].as_str()
            .map(|c| c.contains("/old/path/olaf"))
            .unwrap_or(false)
    });
    assert!(!has_stale, "stale olaf hook must be removed");
}

/// When a current hook and a stale hook both exist in the same section, the stale one must
/// be removed without adding a duplicate current entry (Finding #2 from code review).
#[test]
fn test_init_reconcile_removes_stale_when_current_also_present() {
    let dir = tempfile::tempdir().unwrap();

    // First init to get the current binary path written into the hooks file
    run_init(dir.path());
    let current_content =
        fs::read_to_string(dir.path().join(".claude/settings.local.json")).unwrap();
    let current_json: serde_json::Value = serde_json::from_str(&current_content).unwrap();

    // Extract the current command string that was written
    let current_cmd = current_json["hooks"]["PostToolUse"][0]["hooks"][0]["command"]
        .as_str()
        .expect("current command must be present after first init")
        .to_owned();

    // Now inject a stale entry alongside the current one
    let mixed = serde_json::json!({
        "hooks": {
            "PostToolUse": [
                {
                    "matcher": "",
                    "hooks": [{"type": "command", "command": current_cmd}]
                },
                {
                    "matcher": "",
                    "hooks": [{"type": "command", "command": "/stale/olaf observe --event post-tool-use"}]
                }
            ],
            "PreToolUse": current_json["hooks"]["PreToolUse"].clone(),
            "SessionEnd": current_json["hooks"]["SessionEnd"].clone()
        }
    });
    fs::write(
        dir.path().join(".claude/settings.local.json"),
        serde_json::to_string_pretty(&mixed).unwrap() + "\n",
    )
    .unwrap();

    let out = run_init(dir.path());
    assert!(out.status.success(), "olaf init failed: {:?}", out);

    let content = fs::read_to_string(dir.path().join(".claude/settings.local.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&content).unwrap();

    let post_entries = json["hooks"]["PostToolUse"].as_array().unwrap();
    let olaf_cmds: Vec<&str> = post_entries.iter()
        .flat_map(|e| e["hooks"].as_array().into_iter().flatten())
        .filter_map(|h| h["command"].as_str())
        .filter(|c| c.contains("olaf observe"))
        .collect();

    assert_eq!(olaf_cmds.len(), 1, "exactly one olaf hook must remain after stale cleanup, got: {olaf_cmds:?}");
    assert_eq!(olaf_cmds[0], current_cmd, "remaining hook must be the current-binary command");
}

/// `olaf status` must not create backup files even when config JSON is corrupt (Finding #3).
#[test]
fn test_status_does_not_create_backup_on_corrupt_json() {
    let dir = tempfile::tempdir().unwrap();
    // Pre-seed corrupt .mcp.json
    fs::write(dir.path().join(".mcp.json"), "[1,2,3]\n").unwrap();

    let out = olaf()
        .arg("status")
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf status");
    assert!(out.status.success(), "olaf status failed: {:?}", out);

    let backups: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with(".mcp.json.bak."))
        .collect();
    assert!(backups.is_empty(), "status must not create backup files; found: {backups:?}");
}

/// Bare `olaf` invocation (no subcommand) must produce clap's standard usage error,
/// not an anyhow generic error (Finding #4).
#[test]
fn test_bare_invocation_shows_usage_error() {
    let out = olaf()
        .output()
        .expect("failed to invoke olaf");
    assert!(!out.status.success(), "bare olaf must exit non-zero");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Usage") || stderr.contains("usage") || stderr.contains("USAGE"),
        "bare olaf must print usage guidance; got stderr:\n{stderr}"
    );
}

#[test]
fn test_completions_bash() {
    let out = olaf()
        .args(["completions", "bash"])
        .output()
        .expect("failed to run olaf completions bash");
    assert!(out.status.success(), "olaf completions bash must exit 0: {:?}", out);
    assert!(!out.stdout.is_empty(), "stdout must be non-empty for bash completions");
}
