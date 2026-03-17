use crate::cli::setup::{
    ensure_gitignore_entry, ensure_olaf_dir, print_branding, reconcile_hooks, reconcile_mcp_entry,
    reconcile_tool_rules, ReconcileAction,
};

pub(crate) fn run() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let binary = stable_binary_path(std::env::current_exe()?.canonicalize()?);

    // --- Setup steps ---
    let olaf_dir_created = ensure_olaf_dir(&cwd)?;
    let gitignore_added = ensure_gitignore_entry(&cwd)?;
    let mcp_action = reconcile_mcp_entry(&cwd, &binary)?;
    let hook_actions = reconcile_hooks(&cwd, &binary)?;
    let rules_action = reconcile_tool_rules(&cwd)?;

    // --- Full index ---
    let db_path = cwd.join(".olaf/index.db");
    let mut conn = olaf::db::open(&db_path)?;
    let stats = olaf::index::run(&mut conn, &cwd)?;

    // --- Output ---
    print_branding();

    let all_current = !olaf_dir_created
        && !gitignore_added
        && mcp_action == ReconcileAction::AlreadyCurrent
        && hook_actions.iter().all(|(_, a)| *a == ReconcileAction::AlreadyCurrent)
        && rules_action == ReconcileAction::AlreadyCurrent;

    if all_current {
        println!("Everything is up-to-date.");
    } else {
        println!(
            "  .olaf/              {}",
            if olaf_dir_created { "created" } else { "already exists" }
        );
        println!(
            "  .gitignore          {}",
            if gitignore_added { "updated" } else { "already up-to-date" }
        );
        println!("  .mcp.json           {}", action_label(&mcp_action));
        for (event, action) in &hook_actions {
            println!("  hook {:<15} {}", event, action_label(action));
        }
        println!("  tool preferences    {}", action_label(&rules_action));
    }

    println!(
        "\nIndex: {} files, {} symbols, {} edges, {} centrality",
        stats.files, stats.symbols, stats.edges, stats.centrality_computed
    );

    // --- Auto-registration in parent workspace ---
    match auto_register_in_parent_workspace(&cwd) {
        Ok(Some(ws_path)) => {
            println!(
                "  workspace           registered in {}",
                ws_path.display()
            );
        }
        Ok(None) => {} // No parent workspace found — silent
        Err(e) => {
            eprintln!("  workspace           warning: {e}");
        }
    }

    Ok(())
}

/// Scan parent directories (up to 5 levels) for `.olaf/workspace.toml`.
/// If found and current repo is not listed, append it atomically.
fn auto_register_in_parent_workspace(
    cwd: &std::path::Path,
) -> anyhow::Result<Option<std::path::PathBuf>> {
    let canonical_cwd = cwd.canonicalize()?;

    let mut search = cwd.to_path_buf();
    for _ in 0..5 {
        if !search.pop() {
            break;
        }
        let ws_toml = search.join(".olaf").join("workspace.toml");
        if !ws_toml.exists() {
            continue;
        }

        // Found a parent workspace.toml — check if we're already listed
        let content = match std::fs::read_to_string(&ws_toml) {
            Ok(c) => c,
            Err(e) => {
                return Err(anyhow::anyhow!("cannot read {}: {e}", ws_toml.display()));
            }
        };

        let mut parsed: toml::Value = match toml::from_str(&content) {
            Ok(v) => v,
            Err(e) => {
                return Err(anyhow::anyhow!("malformed {}: {e}", ws_toml.display()));
            }
        };

        // Check existing members
        let ws_dir = ws_toml.parent().unwrap().parent().unwrap_or(&search);
        if let Some(members) = parsed
            .get("workspace")
            .and_then(|w| w.get("members"))
            .and_then(|m| m.as_array())
        {
            for member in members {
                if let Some(path_str) = member.get("path").and_then(|p| p.as_str()) {
                    let member_abs = ws_dir.join(path_str);
                    if let Ok(member_canonical) = member_abs.canonicalize()
                        && member_canonical == canonical_cwd
                    {
                        return Ok(None); // Already registered
                    }
                }
            }
        }

        // Compute relative path from workspace dir to current repo
        let rel_path = olaf::workspace::pathdiff_public(&canonical_cwd, &ws_dir.canonicalize()?);

        // Compute a label from the directory name
        let label = canonical_cwd
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unnamed".to_string());

        // Append new member
        let members = parsed
            .as_table_mut()
            .unwrap()
            .entry("workspace")
            .or_insert(toml::Value::Table(toml::value::Table::new()))
            .as_table_mut()
            .unwrap()
            .entry("members")
            .or_insert(toml::Value::Array(Vec::new()))
            .as_array_mut()
            .unwrap();

        let mut new_member = toml::value::Table::new();
        new_member.insert("path".to_string(), toml::Value::String(rel_path));
        new_member.insert("label".to_string(), toml::Value::String(label));
        members.push(toml::Value::Table(new_member));

        // Atomic write: tmp + rename
        let tmp_path = ws_toml.with_extension("toml.tmp");
        std::fs::write(&tmp_path, toml::to_string_pretty(&parsed)?)?;
        std::fs::rename(&tmp_path, &ws_toml)?;

        return Ok(Some(ws_toml));
    }

    Ok(None) // No parent workspace found
}

/// If the binary lives inside a versioned Homebrew Cellar path
/// (e.g. `/opt/homebrew/Cellar/olaf/0.2.0/bin/olaf`), return the
/// stable prefix symlink (`/opt/homebrew/bin/olaf`) instead so that
/// hooks and MCP config survive future `brew upgrade` calls.
fn stable_binary_path(binary: std::path::PathBuf) -> std::path::PathBuf {
    let s = binary.to_string_lossy();
    if let Some(cellar_pos) = s.find("/Cellar/")
        && let Some(bin_name) = binary.file_name()
    {
        let prefix = &s[..cellar_pos];
        return std::path::PathBuf::from(format!(
            "{}/bin/{}",
            prefix,
            bin_name.to_string_lossy()
        ));
    }
    binary
}

fn action_label(action: &ReconcileAction) -> &'static str {
    match action {
        ReconcileAction::Created => "created",
        ReconcileAction::Updated => "updated",
        ReconcileAction::AlreadyCurrent => "already up-to-date",
    }
}

