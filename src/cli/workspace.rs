use std::path::Path;

pub(crate) fn run_init() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let olaf_dir = cwd.join(".olaf");
    std::fs::create_dir_all(&olaf_dir)?;

    let ws_toml = olaf_dir.join("workspace.toml");
    if ws_toml.exists() {
        println!("Workspace already exists at {}", ws_toml.display());
        return Ok(());
    }

    let canonical = cwd.canonicalize()?;
    let label = canonical
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "local".to_string());

    let config = olaf::workspace::WorkspaceConfig {
        members: vec![olaf::workspace::WorkspaceMember {
            path: canonical,
            label,
            role: None,
        }],
        warnings: vec![],
    };

    let toml_content = olaf::workspace::serialize_workspace_config(&config, &cwd);
    std::fs::write(&ws_toml, toml_content)?;

    println!("Created workspace at {}", ws_toml.display());
    Ok(())
}

pub(crate) fn run_add(path: &Path) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let ws_toml = cwd.join(".olaf").join("workspace.toml");

    if !ws_toml.exists() {
        anyhow::bail!("No workspace found. Run `olaf workspace init` first.");
    }

    let canonical = std::fs::canonicalize(path)
        .unwrap_or_else(|_| olaf::workspace::resolve_path_public(path));

    // Check for duplicates
    let content = std::fs::read_to_string(&ws_toml)?;
    let mut parsed: toml::Value = toml::from_str(&content)?;

    let ws_dir = ws_toml.parent().unwrap().parent().unwrap_or(&cwd);

    if let Some(members) = parsed
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
    {
        for member in members {
            if let Some(path_str) = member.get("path").and_then(|p| p.as_str()) {
                let member_abs = ws_dir.join(path_str);
                if let Ok(member_canonical) = member_abs.canonicalize()
                    && member_canonical == canonical
                {
                    println!("Already registered: {}", canonical.display());
                    return Ok(());
                }
            }
        }
    }

    let rel_path = olaf::workspace::pathdiff_public(&canonical, &ws_dir.canonicalize()?);
    let label = canonical
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unnamed".to_string());

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
    new_member.insert("label".to_string(), toml::Value::String(label.clone()));
    members.push(toml::Value::Table(new_member));

    let tmp_path = ws_toml.with_extension("toml.tmp");
    std::fs::write(&tmp_path, toml::to_string_pretty(&parsed)?)?;
    std::fs::rename(&tmp_path, &ws_toml)?;

    println!("Added '{}' ({})", label, canonical.display());
    Ok(())
}

pub(crate) fn run_list() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let (config, warnings) = olaf::workspace::parse_workspace_config(&cwd);

    for w in &warnings {
        eprintln!("warning: {}", w);
    }

    let Some(config) = config else {
        println!("No workspace configured. Run `olaf workspace init` to create one.");
        return Ok(());
    };

    if config.members.is_empty() {
        println!("Workspace has no members.");
        return Ok(());
    }

    println!("{:<20} {:<12} PATH", "LABEL", "STATUS");
    for m in &config.members {
        let status = if !m.path.exists() {
            "missing"
        } else if m.path.join(".olaf").join("index.db").exists() {
            "indexed"
        } else {
            "not-indexed"
        };
        println!("{:<20} {:<12} {}", m.label, status, m.path.display());
    }

    Ok(())
}

pub(crate) fn run_doctor() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let (config, warnings) = olaf::workspace::parse_workspace_config(&cwd);

    for w in &warnings {
        eprintln!("warning: {}", w);
    }

    let Some(config) = config else {
        println!("No workspace configured.");
        return Ok(());
    };

    let mut problems = 0;

    for m in &config.members {
        println!("--- {} ({})", m.label, m.path.display());

        if !m.path.exists() {
            println!("  [ERROR] Path does not exist");
            println!("  Fix: update path in workspace.toml or remove member");
            problems += 1;
            continue;
        }

        let db_path = m.path.join(".olaf").join("index.db");
        if !db_path.exists() {
            println!("  [WARN] No .olaf/index.db found");
            println!("  Fix: run `olaf init` in {}", m.path.display());
            problems += 1;
            continue;
        }

        match olaf::db::open_readonly(&db_path) {
            Ok(conn) => {
                println!("  [OK] Database opens read-only");

                // Check schema version compatibility
                let remote_version: Option<i64> = conn
                    .query_row(
                        "SELECT MAX(id) FROM _rusqlite_migrations_state WHERE status = 'complete'",
                        [],
                        |r| r.get(0),
                    )
                    .ok()
                    .flatten();

                match remote_version {
                    Some(ver) => {
                        let local_version = olaf::db::MIGRATION_COUNT;
                        if ver < local_version {
                            println!("  [WARN] Schema version {} (local has {}) — run `olaf init` in that repo to upgrade", ver, local_version);
                            problems += 1;
                        } else {
                            println!("  [OK] Schema version {} (compatible)", ver);
                        }
                    }
                    None => {
                        println!("  [WARN] No migration state — run `olaf init` in that repo to upgrade");
                        problems += 1;
                    }
                }

                // Freshness
                let last_indexed: Option<i64> = conn
                    .query_row("SELECT MAX(last_indexed_at) FROM files", [], |r| r.get(0))
                    .ok()
                    .flatten();

                match last_indexed {
                    Some(ts) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        let age_hours = (now - ts) / 3600;
                        if age_hours > 24 {
                            println!("  [WARN] Last indexed {} hours ago", age_hours);
                            println!("  Fix: run `olaf index` in {}", m.path.display());
                            problems += 1;
                        } else {
                            println!("  [OK] Last indexed {} hour(s) ago", age_hours.max(0));
                        }
                    }
                    None => {
                        println!("  [WARN] No files indexed");
                        println!("  Fix: run `olaf init` in {}", m.path.display());
                        problems += 1;
                    }
                }
            }
            Err(e) => {
                println!("  [ERROR] Cannot open database: {e}");
                println!("  Fix: run `olaf init` in {} to rebuild", m.path.display());
                problems += 1;
            }
        }
    }

    println!("\n{} member(s) checked, {} problem(s) found.", config.members.len(), problems);
    Ok(())
}
