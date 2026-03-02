use crate::cli::setup::{
    ensure_gitignore_entry, ensure_olaf_dir, print_branding, reconcile_hooks, reconcile_mcp_entry,
    ReconcileAction,
};

pub(crate) fn run() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let binary = std::env::current_exe()?.canonicalize()?;

    // --- Setup steps ---
    let olaf_dir_created = ensure_olaf_dir(&cwd)?;
    let gitignore_added = ensure_gitignore_entry(&cwd)?;
    let mcp_action = reconcile_mcp_entry(&cwd, &binary)?;
    let hook_actions = reconcile_hooks(&cwd, &binary)?;

    // --- Full index ---
    let db_path = cwd.join(".olaf/index.db");
    let mut conn = olaf::db::open(&db_path)?;
    let stats = olaf::index::run(&mut conn, &cwd)?;

    // --- Output ---
    print_branding();

    let all_current = !olaf_dir_created
        && !gitignore_added
        && mcp_action == ReconcileAction::AlreadyCurrent
        && hook_actions.iter().all(|(_, a)| *a == ReconcileAction::AlreadyCurrent);

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
    }

    println!(
        "\nIndex: {} files, {} symbols, {} edges",
        stats.files, stats.symbols, stats.edges
    );

    Ok(())
}

fn action_label(action: &ReconcileAction) -> &'static str {
    match action {
        ReconcileAction::Created => "created",
        ReconcileAction::Updated => "updated",
        ReconcileAction::AlreadyCurrent => "already up-to-date",
    }
}

