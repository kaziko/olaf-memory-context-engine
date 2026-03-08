use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};
use tempfile::tempdir;

/// Helper to initialize a repo in a temp dir and index it.
fn init_repo(dir: &std::path::Path) {
    let olaf_dir = dir.join(".olaf");
    std::fs::create_dir_all(&olaf_dir).unwrap();
    let db_path = olaf_dir.join("index.db");
    let mut conn = olaf::db::open(&db_path).unwrap();
    olaf::index::run(&mut conn, dir).unwrap();
}

/// Helper to create a TypeScript file in a directory.
fn create_ts_file(dir: &std::path::Path, name: &str, content: &str) {
    std::fs::write(dir.join(name), content).unwrap();
}

/// Spawn olaf serve in a directory and send requests, returning parsed responses.
fn run_mcp_requests(dir: &std::path::Path, requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
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

    let output = child.wait_with_output().expect("failed to wait");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn workspace_cross_repo_pivot_discovery() {
    let root = tempdir().unwrap();

    // Create local repo with a UNIQUE symbol name not in remote
    let local = root.path().join("backend");
    std::fs::create_dir_all(&local).unwrap();
    create_ts_file(
        &local,
        "payments.ts",
        "export class PaymentProcessor {\n  charge() { return true; }\n}\n",
    );
    init_repo(&local);

    // Create remote repo with a UNIQUE symbol name not in local
    let remote = root.path().join("frontend");
    std::fs::create_dir_all(&remote).unwrap();
    create_ts_file(
        &remote,
        "dashboard.ts",
        "export class DashboardRenderer {\n  render() { return false; }\n}\n",
    );
    init_repo(&remote);

    // Create workspace.toml
    let ws_toml = format!(
        "[workspace]\nmembers = [\n  {{ path = \".\", label = \"backend\" }},\n  {{ path = \"{}\", label = \"frontend\" }},\n]\n",
        remote.canonicalize().unwrap().display()
    );
    std::fs::write(local.join(".olaf").join("workspace.toml"), &ws_toml).unwrap();

    // Query via MCP — intent mentions both unique names so both should appear
    let responses = run_mcp_requests(&local, &[
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "get_context",
            "arguments": {
                "intent": "payment processor and dashboard renderer",
                "token_budget": 8000
            }
        }}),
    ]);

    assert!(responses.len() >= 2, "Expected at least 2 responses");
    let text = responses[1]["result"]["content"][0]["text"].as_str().unwrap_or("");

    // Must find symbols from BOTH repos — not just one
    assert!(
        text.contains("PaymentProcessor"),
        "Should find PaymentProcessor from local repo. Got:\n{}",
        &text[..text.len().min(800)]
    );
    assert!(
        text.contains("DashboardRenderer"),
        "Should find DashboardRenderer from remote repo. Got:\n{}",
        &text[..text.len().min(800)]
    );
    // Remote symbols should be labelled with repo name in heading
    assert!(
        text.contains("[frontend]"),
        "Remote symbols should be tagged with repo label in heading. Got:\n{}",
        &text[..text.len().min(800)]
    );
}

#[test]
fn workspace_missing_member_produces_warning() {
    let root = tempdir().unwrap();
    let local = root.path().join("local");
    std::fs::create_dir_all(&local).unwrap();
    create_ts_file(&local, "main.ts", "export function main() {}\n");
    init_repo(&local);

    let ws_toml = "[workspace]\nmembers = [\n  { path = \".\", label = \"local\" },\n  { path = \"../nonexistent\", label = \"ghost\" },\n]\n";
    std::fs::write(local.join(".olaf").join("workspace.toml"), ws_toml).unwrap();

    let (config, warnings) = olaf::workspace::parse_workspace_config(&local);
    assert!(warnings.iter().any(|w| w.message.contains("does not exist")));
    let config = config.unwrap();

    let conn = olaf::db::open(&local.join(".olaf").join("index.db")).unwrap();
    let ws = olaf::workspace::Workspace::load(conn, local, &config);
    assert!(!ws.has_remotes()); // Ghost member skipped
    let fmt = ws.format_warnings();
    assert!(fmt.contains("does not exist"));
}

#[test]
fn no_workspace_toml_identical_behavior() {
    let dir = tempdir().unwrap();
    create_ts_file(dir.path(), "main.ts", "export function hello() {}\n");
    init_repo(dir.path());

    let (config, warnings) = olaf::workspace::parse_workspace_config(dir.path());
    assert!(config.is_none());
    assert!(warnings.is_empty());

    // MCP get_brief should work without workspace
    let responses = run_mcp_requests(dir.path(), &[
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "get_brief",
            "arguments": { "intent": "hello function", "token_budget": 4000 }
        }}),
    ]);

    assert!(responses.len() >= 2);
    let brief = responses[1]["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(brief.contains("hello"), "Should find hello symbol without workspace");
}

#[test]
fn malformed_toml_produces_warning_falls_back() {
    let dir = tempdir().unwrap();
    create_ts_file(dir.path(), "main.ts", "export function test_fn() {}\n");
    init_repo(dir.path());

    std::fs::write(
        dir.path().join(".olaf").join("workspace.toml"),
        "this is {{ not valid toml",
    )
    .unwrap();

    let (config, warnings) = olaf::workspace::parse_workspace_config(dir.path());
    assert!(config.is_none());
    assert!(!warnings.is_empty());
    assert!(warnings[0].message.contains("Malformed"));

    // MCP should still work (falls back to single mode) and show warning
    let responses = run_mcp_requests(dir.path(), &[
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "get_brief",
            "arguments": { "intent": "test function", "token_budget": 4000 }
        }}),
    ]);

    assert!(responses.len() >= 2);
    let brief = responses[1]["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(brief.contains("Workspace Warnings"), "Should show workspace warnings for malformed toml");
    assert!(brief.contains("Malformed"), "Should contain malformed warning");
}

#[test]
fn workspace_cli_init_creates_toml() {
    let dir = tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".olaf")).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["workspace", "init"])
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf workspace init");

    assert!(output.status.success(), "workspace init failed: {}", String::from_utf8_lossy(&output.stderr));
    assert!(dir.path().join(".olaf").join("workspace.toml").exists());

    let content = std::fs::read_to_string(dir.path().join(".olaf").join("workspace.toml")).unwrap();
    assert!(content.contains("[workspace]"));
    assert!(content.contains("members"));
}

#[test]
fn workspace_cli_list_no_workspace() {
    let dir = tempdir().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["workspace", "list"])
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf workspace list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No workspace configured"));
}

#[test]
fn workspace_cli_doctor_no_workspace() {
    let dir = tempdir().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["workspace", "doctor"])
        .current_dir(dir.path())
        .output()
        .expect("failed to run olaf workspace doctor");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No workspace configured"));
}

#[test]
fn workspace_mcp_get_brief_with_workspace() {
    let root = tempdir().unwrap();

    // Create local repo
    let local = root.path().join("api");
    std::fs::create_dir_all(&local).unwrap();
    create_ts_file(
        &local,
        "server.ts",
        "export class ApiServer {\n  start() { return 'ok'; }\n}\n",
    );
    init_repo(&local);

    // Create remote repo
    let remote = root.path().join("sdk");
    std::fs::create_dir_all(&remote).unwrap();
    create_ts_file(
        &remote,
        "client.ts",
        "export class SdkClient {\n  connect() { return true; }\n}\n",
    );
    init_repo(&remote);

    // Create workspace.toml
    let ws_toml = format!(
        "[workspace]\nmembers = [\n  {{ path = \".\", label = \"api\" }},\n  {{ path = \"{}\", label = \"sdk\" }},\n]\n",
        remote.canonicalize().unwrap().display()
    );
    std::fs::write(local.join(".olaf").join("workspace.toml"), &ws_toml).unwrap();

    let responses = run_mcp_requests(&local, &[
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "get_brief",
            "arguments": { "intent": "server client api sdk", "token_budget": 4000 }
        }}),
    ]);

    assert!(responses.len() >= 2, "Expected at least 2 responses");

    let brief = responses[1]["result"]["content"][0]["text"].as_str().unwrap_or("");

    // Must contain symbols from BOTH repos
    assert!(
        brief.contains("ApiServer"),
        "get_brief should include local ApiServer. Got: {}",
        &brief[..brief.len().min(800)]
    );
    assert!(
        brief.contains("SdkClient"),
        "get_brief should include remote SdkClient. Got: {}",
        &brief[..brief.len().min(800)]
    );
    // Workspace scope note
    assert!(
        brief.contains("Impact analysis: local repo only") || brief.contains("local repo only"),
        "get_brief should note local-only scope. Got: {}",
        &brief[..brief.len().min(800)]
    );
}

#[test]
fn workspace_duplicate_via_symlinks() {
    let dir = tempdir().unwrap();
    let olaf_dir = dir.path().join(".olaf");
    std::fs::create_dir_all(&olaf_dir).unwrap();

    let real_dir = dir.path().join("real-repo");
    std::fs::create_dir_all(&real_dir).unwrap();

    #[cfg(unix)]
    {
        let link_path = dir.path().join("link-repo");
        std::os::unix::fs::symlink(&real_dir, &link_path).unwrap();

        let ws_toml = "[workspace]\nmembers = [\n  { path = \"real-repo\", label = \"original\" },\n  { path = \"link-repo\", label = \"duplicate\" },\n]\n";
        std::fs::write(olaf_dir.join("workspace.toml"), ws_toml).unwrap();

        let (config, warnings) = olaf::workspace::parse_workspace_config(dir.path());
        let config = config.expect("should parse");
        assert_eq!(config.members.len(), 1, "Duplicate should be deduped");
        assert!(warnings.iter().any(|w| w.message.contains("Duplicate")));
    }
}

#[test]
fn workspace_cli_add_and_list() {
    let root = tempdir().unwrap();

    // Init workspace
    let local = root.path().join("main");
    std::fs::create_dir_all(local.join(".olaf")).unwrap();

    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["workspace", "init"])
        .current_dir(&local)
        .output()
        .expect("workspace init failed");

    // Create another repo
    let other = root.path().join("other");
    std::fs::create_dir_all(&other).unwrap();

    // Add it
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["workspace", "add", other.to_str().unwrap()])
        .current_dir(&local)
        .output()
        .expect("workspace add failed");

    assert!(output.status.success(), "add failed: {}", String::from_utf8_lossy(&output.stderr));

    // List should show both
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .args(["workspace", "list"])
        .current_dir(&local)
        .output()
        .expect("workspace list failed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("other"), "Should list the added member");
}

#[test]
fn workspace_retrieval_notes_include_repo_labels() {
    let root = tempdir().unwrap();

    let local = root.path().join("backend");
    std::fs::create_dir_all(&local).unwrap();
    create_ts_file(
        &local,
        "payments.ts",
        "export class PaymentProcessor {\n  charge() { return true; }\n}\n",
    );
    init_repo(&local);

    let remote = root.path().join("frontend");
    std::fs::create_dir_all(&remote).unwrap();
    create_ts_file(
        &remote,
        "dashboard.ts",
        "export class DashboardRenderer {\n  render() { return false; }\n}\n",
    );
    init_repo(&remote);

    let ws_toml = format!(
        "[workspace]\nmembers = [\n  {{ path = \".\", label = \"backend\" }},\n  {{ path = \"{}\", label = \"frontend\" }},\n]\n",
        remote.canonicalize().unwrap().display()
    );
    std::fs::write(local.join(".olaf").join("workspace.toml"), &ws_toml).unwrap();

    let responses = run_mcp_requests(&local, &[
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
        serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
            "name": "get_context",
            "arguments": {
                "intent": "payment processor and dashboard renderer",
                "token_budget": 8000
            }
        }}),
    ]);

    assert!(responses.len() >= 2);
    let text = responses[1]["result"]["content"][0]["text"].as_str().unwrap_or("");

    assert!(text.contains("## Retrieval Notes"),
        "workspace get_context must include retrieval notes; got:\n{}", &text[..text.len().min(800)]);

    // Remote pivots must show the workspace label in retrieval notes
    if text.contains("DashboardRenderer") {
        assert!(text.contains("[frontend]"),
            "retrieval notes must include remote repo label [frontend]; got:\n{}", &text[..text.len().min(1200)]);
    }

    // Local pivots must show local-priority strategy, remote pivots remote-round-robin
    assert!(
        text.contains("local-priority") || text.contains("remote-round-robin"),
        "retrieval notes must include selection strategy annotation; got:\n{}", &text[..text.len().min(1200)]
    );
}
