use anyhow::Context;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Tool-preference rules template (Task 1)
// ---------------------------------------------------------------------------

/// Instruction template for `.claude/rules/olaf-tools.md`.
/// The `{HASH}` placeholder is replaced at render time with a blake3 hash of
/// the template body (with placeholder blanked) so that staleness detection
/// works purely on content, independent of version numbers.
const OLAF_RULES_TEMPLATE: &str = r#"<!-- olaf-tools {HASH} -->
## Olaf Context Engine

This project uses Olaf for intelligent codebase context.

### REQUIRED: Load Olaf tools before first use

Olaf MCP tools are deferred. Before first use each session, call:
ToolSearch("select:mcp__olaf__get_brief,mcp__olaf__get_context,mcp__olaf__get_file_skeleton,mcp__olaf__get_impact,mcp__olaf__trace_flow,mcp__olaf__analyze_failure,mcp__olaf__save_observation,mcp__olaf__index_status")

### Tool routing — prefer Olaf over native tools

| Task | Olaf tool | Instead of | Notes |
|-|-|-|-|
| Explore codebase (start here) | `get_brief` | Multiple Grep + Read | Auto-reindexes; broad entry point |
| Fine-grained context control | `get_context` | Multiple Grep + Read | Auto-reindexes |
| Quick file structure overview | `get_file_skeleton` | Reading entire file | Fast, does NOT reindex |
| Analyze dependencies/impact | `get_impact` | Grep for usages | Does NOT reindex |
| Trace execution paths | `trace_flow` | Manual file-by-file tracing | Does NOT reindex |
| Diagnose errors/failures | `analyze_failure` | Ad-hoc investigation | Auto-reindexes local repo |
| Persist important findings | `save_observation` | (no equivalent) | |

### Freshness after edits

`get_brief`, `get_context`, and `analyze_failure` auto-reindex the local repo before returning results.
In workspace mode, remote members are NOT reindexed on demand — freshness warnings are advisory.
Other tools (`get_file_skeleton`, `get_impact`, `trace_flow`) do NOT reindex.
If results seem stale after edits: call `get_brief` or `get_context` to trigger a local reindex, or run `olaf index` from CLI.
`index_status` is diagnostic only — it reports freshness but does not reindex.

### When to use native tools instead

- **Editing files**: always use Edit/Write (Olaf is read-only)
- **Running commands**: always use Bash
- **Reading a specific known file path**: Read is fine for targeted reads
- **Simple keyword search in 1-2 files**: Grep is fine for narrow searches
"#;

// ---------------------------------------------------------------------------
// Hash / render helpers (Task 2)
// ---------------------------------------------------------------------------

/// Compute blake3 hash of the rendered template body (with `{HASH}` blanked).
fn compute_rules_hash() -> String {
    let body = OLAF_RULES_TEMPLATE.replace("{HASH}", "");
    blake3::hash(body.as_bytes()).to_hex().to_string()
}

/// Render the template with the actual hash substituted in.
fn render_rules() -> String {
    let hash = compute_rules_hash();
    OLAF_RULES_TEMPLATE.replace("{HASH}", &hash)
}

// ---------------------------------------------------------------------------
// Rules file reconcile + check (Task 3 & Task 5)
// ---------------------------------------------------------------------------

const RULES_REL_PATH: &str = ".claude/rules/olaf-tools.md";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RulesFileStatus {
    Current,
    Outdated { detected_hash: String },
    Missing,
    Malformed { reason: String },
}

/// Reconcile `.claude/rules/olaf-tools.md` — create, update, or leave as-is.
pub(crate) fn reconcile_tool_rules(cwd: &Path) -> anyhow::Result<ReconcileAction> {
    let rules_path = cwd.join(RULES_REL_PATH);

    // Ensure directory exists
    if let Some(parent) = rules_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let rendered = render_rules();

    if !rules_path.exists() {
        atomic_write(&rules_path, &rendered)?;
        return Ok(ReconcileAction::Created);
    }

    let existing = std::fs::read_to_string(&rules_path)?;

    // Full-content hash comparison — catches header edits, body edits, everything.
    let existing_hash = blake3::hash(existing.as_bytes()).to_hex().to_string();
    let rendered_hash = blake3::hash(rendered.as_bytes()).to_hex().to_string();

    if existing_hash == rendered_hash {
        return Ok(ReconcileAction::AlreadyCurrent);
    }

    atomic_write(&rules_path, &rendered)?;
    Ok(ReconcileAction::Updated)
}

/// Check state of `.claude/rules/olaf-tools.md` without modifying anything.
pub(crate) fn check_tool_rules(cwd: &Path) -> anyhow::Result<RulesFileStatus> {
    let rules_path = cwd.join(RULES_REL_PATH);

    if !rules_path.exists() {
        return Ok(RulesFileStatus::Missing);
    }

    let existing = std::fs::read_to_string(&rules_path)?;

    // Check for hash marker presence
    if !existing.starts_with("<!-- olaf-tools ") {
        return Ok(RulesFileStatus::Malformed {
            reason: "missing olaf-tools marker on first line".to_string(),
        });
    }

    // Extract detected hash from marker
    let first_line = existing.lines().next().unwrap_or("");
    let detected_hash = first_line
        .strip_prefix("<!-- olaf-tools ")
        .and_then(|s| s.strip_suffix(" -->"))
        .unwrap_or("");

    if detected_hash.is_empty()
        || detected_hash.contains(' ')
        || !detected_hash.chars().all(|c| c.is_ascii_hexdigit())
    {
        return Ok(RulesFileStatus::Malformed {
            reason: "hash marker is malformed".to_string(),
        });
    }

    // Full-content comparison
    let rendered = render_rules();
    let existing_full_hash = blake3::hash(existing.as_bytes()).to_hex().to_string();
    let rendered_full_hash = blake3::hash(rendered.as_bytes()).to_hex().to_string();

    if existing_full_hash == rendered_full_hash {
        Ok(RulesFileStatus::Current)
    } else {
        Ok(RulesFileStatus::Outdated {
            detected_hash: detected_hash.to_string(),
        })
    }
}

/// Atomic write via tmp + rename.
fn atomic_write(path: &Path, content: &str) -> anyhow::Result<()> {
    let tmp_path = path.with_extension("md.tmp");
    std::fs::write(&tmp_path, content)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReconcileAction {
    Created,
    Updated,
    AlreadyCurrent,
}

pub(crate) fn print_branding() {
    println!("Olaf ver. {} — codebase context engine for Claude Code", env!("CARGO_PKG_VERSION"));
    println!("─────────────────────────────────────────────────────");
}

/// Creates `.olaf/` if absent. Returns `true` if created.
pub(crate) fn ensure_olaf_dir(cwd: &Path) -> anyhow::Result<bool> {
    let dir = cwd.join(".olaf");
    if dir.exists() {
        return Ok(false);
    }
    std::fs::create_dir_all(&dir)?;
    Ok(true)
}

/// Appends `.olaf/` to `.gitignore` only if the line is absent. Returns `true` if appended.
pub(crate) fn ensure_gitignore_entry(cwd: &Path) -> anyhow::Result<bool> {
    let path = cwd.join(".gitignore");
    let content = if path.exists() {
        std::fs::read_to_string(&path)?
    } else {
        String::new()
    };
    if content.lines().any(|l| l.trim() == ".olaf/") {
        return Ok(false);
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    use std::io::Write;
    if !content.is_empty() && !content.ends_with('\n') {
        writeln!(file)?;
    }
    writeln!(file, ".olaf/")?;
    Ok(true)
}

/// Copies `path` to `<path>.bak.<unix_timestamp>`.
fn backup_file(path: &Path) -> anyhow::Result<()> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();
    let backup = PathBuf::from(format!("{}.bak.{}", path.display(), ts));
    std::fs::copy(path, &backup)
        .with_context(|| format!("failed to create backup at {}", backup.display()))?;
    Ok(())
}

/// Read-write JSON object loader: guarantees a `Value::Object` root. If the file is missing,
/// returns an empty object. If the root is not an object, backs up the file (when non-empty)
/// and returns a fresh empty object. Use only for write operations.
fn load_json_object(path: &Path) -> anyhow::Result<(Value, bool)> {
    if !path.exists() {
        return Ok((json!({}), false));
    }
    let s = std::fs::read_to_string(path)?;
    match serde_json::from_str::<Value>(&s) {
        Ok(Value::Object(_)) => Ok((serde_json::from_str(&s)?, false)),
        Ok(_) | Err(_) => {
            if !s.trim().is_empty() {
                backup_file(path)?;
            }
            Ok((json!({}), true))
        }
    }
}

/// Read-only JSON object loader: returns an empty object for missing or corrupt files
/// without creating any backup. Use for diagnostic/status queries only.
fn peek_json_object(path: &Path) -> anyhow::Result<Value> {
    if !path.exists() {
        return Ok(json!({}));
    }
    let s = std::fs::read_to_string(path)?;
    match serde_json::from_str::<Value>(&s) {
        Ok(Value::Object(_)) => Ok(serde_json::from_str(&s)?),
        Ok(_) | Err(_) => Ok(json!({})),
    }
}

/// Reconciles the `"olaf"` entry in `.mcp.json`:
/// - Command matches current binary → `AlreadyCurrent` (no write)
/// - Stale or missing entry → update and write (backup existing file first)
pub(crate) fn reconcile_mcp_entry(
    cwd: &Path,
    binary: &Path,
) -> anyhow::Result<ReconcileAction> {
    let mcp_path = cwd.join(".mcp.json");
    let (mut json, was_corrupt) = load_json_object(&mcp_path)?;

    let current_command = binary.to_string_lossy().into_owned();

    // Safe nested read — no [] on potentially non-object nested values
    let existing = json
        .get("mcpServers")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("olaf"))
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("command"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);

    match existing {
        Some(ref cmd) if cmd == &current_command => {
            return Ok(ReconcileAction::AlreadyCurrent);
        }
        _ => {
            if mcp_path.exists() && !was_corrupt {
                backup_file(&mcp_path)?;
            }
        }
    }

    // Normalize mcpServers to object before mutating
    if !json.get("mcpServers").map(|v| v.is_object()).unwrap_or(false) {
        json["mcpServers"] = json!({});
    }
    json["mcpServers"]["olaf"] = json!({
        "command": current_command,
        "args": ["serve"],
        "type": "stdio"
    });
    std::fs::write(&mcp_path, serde_json::to_string_pretty(&json)? + "\n")?;
    Ok(if existing.is_some() {
        ReconcileAction::Updated
    } else {
        ReconcileAction::Created
    })
}

fn hook_command_for(binary: &Path, event: &str) -> String {
    format!("{} observe --event {}", binary.display(), event)
}

/// Reconciles hooks in `.claude/settings.local.json` for PostToolUse, PreToolUse, SessionEnd.
///
/// For each event section the algorithm is:
/// 1. Walk every outer entry; inside each entry's `hooks` array, remove any inner command that
///    contains `"olaf observe"` but is NOT the current target command. This preserves non-Olaf
///    sibling commands within the same entry.
/// 2. Drop outer entries whose `hooks` array became empty after step 1.
/// 3. If the current target command was already found in step 1 → no new entry needed.
///    If the current target command was absent → append a fresh outer entry.
/// 4. Return `AlreadyCurrent` if nothing changed, `Updated` if stale entries were cleaned,
///    `Created` if the hook was newly added.
///
/// The file is written only when at least one section actually changed.
pub(crate) fn reconcile_hooks(
    cwd: &Path,
    binary: &Path,
) -> anyhow::Result<Vec<(&'static str, ReconcileAction)>> {
    let settings_path = cwd.join(".claude/settings.local.json");
    if let Some(parent) = settings_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let (mut json, was_corrupt) = load_json_object(&settings_path)?;

    const EVENTS: [(&str, &str); 3] = [
        ("PostToolUse", "post-tool-use"),
        ("PreToolUse", "pre-tool-use"),
        ("SessionEnd", "session-end"),
    ];

    // Normalize json["hooks"] to object
    let hooks_normalized = !json.get("hooks").map(|v| v.is_object()).unwrap_or(false);
    if hooks_normalized {
        json["hooks"] = json!({});
    }

    let mut results = Vec::new();
    let mut file_changed = was_corrupt || hooks_normalized;

    for (section, event_flag) in &EVENTS {
        let target_cmd = hook_command_for(binary, event_flag);

        // Normalize section to array
        let section_normalized = !json["hooks"]
            .get(*section)
            .map(|v| v.is_array())
            .unwrap_or(false);
        if section_normalized {
            json["hooks"][*section] = json!([]);
            file_changed = true;
        }

        let entries = json["hooks"][*section].as_array_mut().unwrap();

        let mut current_found = false;
        let mut stale_removed = false;

        // Step 1: Surgically remove stale olaf hooks from within each entry,
        //         preserving non-olaf sibling commands and noting if current exists.
        for entry in entries.iter_mut() {
            if let Some(inner) = entry.get_mut("hooks").and_then(|v| v.as_array_mut()) {
                let before_len = inner.len();
                inner.retain(|h| {
                    let cmd = h.get("command").and_then(|v| v.as_str()).unwrap_or("");
                    if cmd == target_cmd {
                        current_found = true;
                        true // keep — this is the correct current hook
                    } else if cmd.contains("olaf observe") {
                        false // remove — stale olaf hook with a different path
                    } else {
                        true // keep — unrelated command, not ours to touch
                    }
                });
                if inner.len() < before_len {
                    stale_removed = true;
                    file_changed = true;
                }
            }
        }

        // Step 2: Remove outer entries that are now empty after stale hook removal.
        let before_count = entries.len();
        entries.retain(|entry| {
            !entry
                .get("hooks")
                .and_then(|v| v.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(false)
        });
        if entries.len() < before_count {
            file_changed = true;
        }

        // Step 3: Add current hook if not already present.
        let action = if !current_found {
            entries.push(json!({
                "matcher": "",
                "hooks": [{"type": "command", "command": target_cmd}]
            }));
            file_changed = true;
            if stale_removed {
                ReconcileAction::Updated
            } else {
                ReconcileAction::Created
            }
        } else if stale_removed {
            ReconcileAction::Updated
        } else {
            ReconcileAction::AlreadyCurrent
        };

        results.push((*section, action));
    }

    if file_changed {
        std::fs::write(
            &settings_path,
            serde_json::to_string_pretty(&json)? + "\n",
        )?;
    }
    Ok(results)
}

/// Returns `(is_registered, absolute_path_to_mcp_json)`. Read-only — no side effects.
pub(crate) fn check_mcp_registered(cwd: &Path) -> anyhow::Result<(bool, PathBuf)> {
    let mcp_path = cwd.join(".mcp.json");
    let abs_path = mcp_path
        .canonicalize()
        .unwrap_or_else(|_| cwd.join(".mcp.json"));

    if !mcp_path.exists() {
        return Ok((false, abs_path));
    }

    let json = peek_json_object(&mcp_path)?;
    let registered = json
        .get("mcpServers")
        .and_then(|v| v.as_object())
        .map(|o| o.contains_key("olaf"))
        .unwrap_or(false);

    Ok((registered, abs_path))
}

/// Returns `[PostToolUse_installed, PreToolUse_installed, SessionEnd_installed]`.
/// Read-only — no side effects.
pub(crate) fn check_hooks_installed(cwd: &Path) -> anyhow::Result<[bool; 3]> {
    let settings_path = cwd.join(".claude/settings.local.json");
    if !settings_path.exists() {
        return Ok([false, false, false]);
    }

    let json = peek_json_object(&settings_path)?;

    let check = |section: &str| -> bool {
        json.get("hooks")
            .and_then(|v| v.as_object())
            .and_then(|o| o.get(section))
            .and_then(|v| v.as_array())
            .map(|entries| {
                entries
                    .iter()
                    .flat_map(|entry| {
                        entry
                            .get("hooks")
                            .and_then(|v| v.as_array())
                            .into_iter()
                            .flatten()
                    })
                    .filter_map(|h| h.get("command").and_then(|v| v.as_str()))
                    .any(|cmd| cmd.contains("olaf observe"))
            })
            .unwrap_or(false)
    };

    Ok([
        check("PostToolUse"),
        check("PreToolUse"),
        check("SessionEnd"),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // 6.1 — Hash is deterministic across calls
    #[test]
    fn test_rules_hash_stable() {
        let h1 = compute_rules_hash();
        let h2 = compute_rules_hash();
        assert_eq!(h1, h2);
        assert!(!h1.is_empty());
    }

    // 6.2 — Rendered output contains hash, no {HASH} literal
    #[test]
    fn test_render_no_placeholder() {
        let rendered = render_rules();
        assert!(!rendered.contains("{HASH}"), "rendered output must not contain {{HASH}} placeholder");
        let hash = compute_rules_hash();
        assert!(rendered.contains(&hash), "rendered output must contain the computed hash");
    }

    // 6.3 — Creates fresh file in empty tempdir
    #[test]
    fn test_reconcile_creates_fresh() {
        let tmp = TempDir::new().unwrap();
        let action = reconcile_tool_rules(tmp.path()).unwrap();
        assert_eq!(action, ReconcileAction::Created);
        let path = tmp.path().join(RULES_REL_PATH);
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("<!-- olaf-tools "));
        assert!(!content.contains("{HASH}"));
    }

    // 6.4 — Second call returns AlreadyCurrent
    #[test]
    fn test_reconcile_idempotent() {
        let tmp = TempDir::new().unwrap();
        reconcile_tool_rules(tmp.path()).unwrap();
        let action = reconcile_tool_rules(tmp.path()).unwrap();
        assert_eq!(action, ReconcileAction::AlreadyCurrent);
    }

    // 6.5 — Stale hash triggers update
    #[test]
    fn test_reconcile_updates_stale_hash() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "<!-- olaf-tools deadbeef -->\nold content\n").unwrap();
        let action = reconcile_tool_rules(tmp.path()).unwrap();
        assert_eq!(action, ReconcileAction::Updated);
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains(&compute_rules_hash()));
    }

    // 6.6 — Malformed header triggers update
    #[test]
    fn test_reconcile_malformed_header() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "no marker at all\njust random content\n").unwrap();
        let action = reconcile_tool_rules(tmp.path()).unwrap();
        assert_eq!(action, ReconcileAction::Updated);
    }

    // 6.7 — check_tool_rules returns Missing on empty dir
    #[test]
    fn test_check_missing() {
        let tmp = TempDir::new().unwrap();
        let status = check_tool_rules(tmp.path()).unwrap();
        assert_eq!(status, RulesFileStatus::Missing);
    }

    // 6.8 — check_tool_rules returns Outdated with detected hash
    #[test]
    fn test_check_outdated() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "<!-- olaf-tools abc123def456 -->\nold body\n").unwrap();
        let status = check_tool_rules(tmp.path()).unwrap();
        match status {
            RulesFileStatus::Outdated { detected_hash } => {
                assert_eq!(detected_hash, "abc123def456");
            }
            other => panic!("expected Outdated, got {:?}", other),
        }
    }

    // 6.9 — check_tool_rules returns Malformed with reason
    #[test]
    fn test_check_malformed() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "totally not a rules file\n").unwrap();
        let status = check_tool_rules(tmp.path()).unwrap();
        match status {
            RulesFileStatus::Malformed { reason } => {
                assert!(reason.contains("marker"), "reason should mention marker: {}", reason);
            }
            other => panic!("expected Malformed, got {:?}", other),
        }
    }

    // 6.10 — check_tool_rules returns Current for valid file
    #[test]
    fn test_check_current() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, render_rules()).unwrap();
        let status = check_tool_rules(tmp.path()).unwrap();
        assert_eq!(status, RulesFileStatus::Current);
    }

    // 6.9b — Non-hex marker reported as Malformed, not Outdated
    #[test]
    fn test_check_non_hex_marker_is_malformed() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "<!-- olaf-tools 中中中 -->\nbody\n").unwrap();
        let status = check_tool_rules(tmp.path()).unwrap();
        match status {
            RulesFileStatus::Malformed { reason } => {
                assert!(reason.contains("malformed"), "reason: {}", reason);
            }
            other => panic!("expected Malformed for non-hex marker, got {:?}", other),
        }
    }

    // 6.11 — Body edit detected via full-content hash comparison
    #[test]
    fn test_reconcile_detects_body_edit() {
        let tmp = TempDir::new().unwrap();
        // First create the correct file
        reconcile_tool_rules(tmp.path()).unwrap();
        let path = tmp.path().join(RULES_REL_PATH);
        // Now modify the body but keep the header hash intact
        let mut content = std::fs::read_to_string(&path).unwrap();
        content.push_str("\n<!-- user added this line -->\n");
        std::fs::write(&path, &content).unwrap();
        // Reconcile should detect the change
        let action = reconcile_tool_rules(tmp.path()).unwrap();
        assert_eq!(action, ReconcileAction::Updated);
    }
}
