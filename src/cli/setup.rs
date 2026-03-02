use anyhow::Context;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReconcileAction {
    Created,
    Updated,
    AlreadyCurrent,
}

pub(crate) fn print_branding() {
    println!("olaf  codebase context engine for Claude Code");
    println!("─────────────────────────────────────────────");
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
