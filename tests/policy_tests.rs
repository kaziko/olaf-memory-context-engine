// Integration tests for policy-aware context filtering (Story 6.2–6.4).
// These tests spawn `olaf serve` as a subprocess, send JSON-RPC requests,
// and verify that deny/redact rules are enforced end-to-end.

use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::{Command, Stdio};

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn spawn_server_in(dir: &Path) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("serve")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn olaf serve")
}

fn run_requests_in(dir: &Path, requests: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut child = spawn_server_in(dir);
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
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("bad JSON: {e}\nLine: {l:?}")))
        .collect()
}

fn index_dir(dir: &Path) {
    let status = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(dir)
        .output()
        .expect("failed to run olaf index");
    assert!(status.status.success(), "olaf index failed: {}", String::from_utf8_lossy(&status.stderr));
}

fn tool_call(id: i64, tool: &str, args: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0", "id": id, "method": "tools/call",
        "params": { "name": tool, "arguments": args }
    })
}

fn extract_text(resp: &serde_json::Value) -> &str {
    resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text in response: {resp}"))
}

fn write_policy(dir: &Path, toml_content: &str) {
    let policy_dir = dir.join(".olaf");
    std::fs::create_dir_all(&policy_dir).expect("create .olaf dir");
    std::fs::write(policy_dir.join("policy.toml"), toml_content).expect("write policy.toml");
}

// ─── 6.2 — Graph layer integration tests (via MCP) ──────────────────────────

#[test]
fn test_get_context_excludes_denied_symbols() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    // Two files: one will be denied, one allowed
    std::fs::write(
        src_dir.join("billing.rs"),
        "/// Billing service.\npub struct BillingService { amount: u64 }\n\
         /// Charge a customer.\npub fn charge(amount: u64) -> bool { true }\n",
    ).unwrap();
    std::fs::write(
        src_dir.join("public_api.rs"),
        "/// Public API handler.\npub fn handle_request() -> String { String::new() }\n",
    ).unwrap();

    index_dir(tmpdir.path());

    // Deny everything under src/billing.rs
    write_policy(tmpdir.path(), r#"
        [[deny]]
        path = "src/billing.rs"
    "#);

    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_context", serde_json::json!({
            "intent": "understand billing and public api",
            "file_hints": ["billing.rs", "public_api.rs"],
            "token_budget": 4000
        })),
    ]);

    let text = extract_text(&responses[0]);
    assert!(!text.contains("BillingService"), "denied symbols must NOT appear; got: {text}");
    assert!(!text.contains("charge"), "denied file's symbols must NOT appear; got: {text}");
    assert!(text.contains("handle_request"), "allowed symbols must appear; got: {text}");
}

#[test]
fn test_get_context_redacts_matching_symbols() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    std::fs::write(
        src_dir.join("secrets.rs"),
        "/// Secret key manager.\npub fn get_secret_key() -> String { \"super_secret\".into() }\n",
    ).unwrap();

    index_dir(tmpdir.path());

    write_policy(tmpdir.path(), r#"
        [[redact]]
        path = "src/secrets.rs"
    "#);

    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_context", serde_json::json!({
            "intent": "understand secret key management",
            "file_hints": ["secrets.rs"],
            "token_budget": 4000
        })),
    ]);

    let text = extract_text(&responses[0]);
    assert!(text.contains("[redacted by policy]"), "redacted symbols must show redaction marker; got: {text}");
    // The implementation body should not appear
    assert!(!text.contains("super_secret"), "implementation must not leak through redaction; got: {text}");
}

#[test]
fn test_get_file_skeleton_denied_returns_not_found() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    std::fs::write(
        src_dir.join("billing.rs"),
        "/// Billing.\npub fn charge() {}\n",
    ).unwrap();

    index_dir(tmpdir.path());

    write_policy(tmpdir.path(), r#"
        [[deny]]
        path = "src/billing.rs"
    "#);

    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_file_skeleton", serde_json::json!({
            "file_path": "src/billing.rs"
        })),
    ]);

    let text = extract_text(&responses[0]);
    // Must say "No file found" — must NOT say "denied by policy" (information leak)
    assert!(text.contains("No file found"), "denied file must report 'No file found'; got: {text}");
    assert!(!text.contains("denied"), "must NOT leak that denial happened; got: {text}");
    assert!(!text.contains("policy"), "must NOT leak policy info; got: {text}");
}

#[test]
fn test_get_impact_denied_direct_query() {
    let tmpdir = tempfile::tempdir().unwrap();

    std::fs::write(
        tmpdir.path().join("app.ts"),
        "export function secretFn() {}\nexport function publicFn() { secretFn(); }\n",
    ).unwrap();

    index_dir(tmpdir.path());

    write_policy(tmpdir.path(), r#"
        [[deny]]
        fqn_prefix = "app.ts::secretFn"
    "#);

    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_impact", serde_json::json!({
            "symbol_fqn": "app.ts::secretFn"
        })),
    ]);

    let text = extract_text(&responses[0]);
    assert!(text.contains("Symbol not found"), "denied symbol must report 'Symbol not found'; got: {text}");
}

#[test]
fn test_policy_plus_hardcoded_both_apply() {
    // Even without a policy file, hardcoded rules (.env etc.) must still apply.
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    std::fs::write(
        src_dir.join("lib.rs"),
        "/// A function.\npub fn hello() {}\n",
    ).unwrap();

    index_dir(tmpdir.path());

    // No policy file — hardcoded .env rule should still work
    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_file_skeleton", serde_json::json!({
            "file_path": ".env"
        })),
    ]);

    let text = extract_text(&responses[0]);
    assert!(text.contains("not permitted"), "hardcoded .env rule must still block; got: {text}");
}

// ─── 6.3 — Observation retrieval (via MCP) ──────────────────────────────────

#[test]
fn test_observations_denied_by_file_path() {
    let tmpdir = tempfile::tempdir().unwrap();

    // Create and index a file so DB exists
    std::fs::write(
        tmpdir.path().join("app.ts"),
        "export function handler() {}\n",
    ).unwrap();
    index_dir(tmpdir.path());

    // Save observation anchored to a file that will be denied
    run_requests_in(tmpdir.path(), &[
        tool_call(1, "save_observation", serde_json::json!({
            "kind": "insight",
            "content": "billing module has a bug",
            "file_path": "src/billing.rs"
        })),
    ]);

    // Now add deny rule for that path
    write_policy(tmpdir.path(), r#"
        [[deny]]
        path = "src/billing.rs"
    "#);

    // Query session history — denied observation must not appear
    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_session_history", serde_json::json!({})),
    ]);

    let text = extract_text(&responses[0]);
    assert!(!text.contains("billing module has a bug"), "observation with denied file_path must be excluded; got: {text}");
}

// ─── 6.4 — Reload and error ────────────────────────────────────────────────

#[test]
fn test_policy_reload_picks_up_changes() {
    // Policy is loaded per-call, so creating a policy file between calls
    // should make new rules apply immediately.
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    std::fs::write(
        src_dir.join("billing.rs"),
        "/// Billing.\npub fn charge() {}\n",
    ).unwrap();

    index_dir(tmpdir.path());

    // First call: no policy — should find the file
    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_file_skeleton", serde_json::json!({
            "file_path": "src/billing.rs"
        })),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("charge"), "without policy, file should be visible; got: {text}");

    // Now create policy that denies billing.rs
    write_policy(tmpdir.path(), r#"
        [[deny]]
        path = "src/billing.rs"
    "#);

    // Second call: policy active — should not find the file
    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_file_skeleton", serde_json::json!({
            "file_path": "src/billing.rs"
        })),
    ]);
    let text = extract_text(&responses[0]);
    assert!(text.contains("No file found"), "with deny policy, file must not be found; got: {text}");
}

#[test]
fn test_malformed_policy_falls_back_gracefully() {
    let tmpdir = tempfile::tempdir().unwrap();
    let src_dir = tmpdir.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();

    std::fs::write(
        src_dir.join("lib.rs"),
        "/// Hello.\npub fn hello() {}\n",
    ).unwrap();

    index_dir(tmpdir.path());

    // Write invalid TOML
    write_policy(tmpdir.path(), "this is {{{{ not valid TOML at all !!!}}}}");

    // Server should still work — malformed policy falls back to default (no rules)
    let responses = run_requests_in(tmpdir.path(), &[
        tool_call(1, "get_file_skeleton", serde_json::json!({
            "file_path": "src/lib.rs"
        })),
    ]);

    let text = extract_text(&responses[0]);
    assert!(text.contains("hello"), "malformed policy must fall back to default (allow all); got: {text}");
}
