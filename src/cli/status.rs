use anyhow::Context;
use crate::cli::setup::{check_hooks_installed, check_mcp_registered, check_tool_rules, print_branding, RulesFileStatus};

pub(crate) fn run() -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let db_path = cwd.join(".olaf/index.db");

    print_branding();

    if !db_path.exists() {
        println!("Index not initialized. Run `olaf index` to build the index.");
        // Still show MCP/hook diagnostics even when uninitialized
        print_diagnostics(&cwd)?;
        return Ok(());
    }

    let conn = olaf::db::open(&db_path).context("failed to open database")?;
    let stats = olaf::graph::load_db_stats(&conn)?;

    let last_indexed = match stats.last_indexed_at {
        Some(ts) => chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| ts.to_string()),
        None => "never".to_string(),
    };

    println!("Files indexed:  {}", stats.files);
    println!("Symbols:        {}", stats.symbols);
    println!("Edges:          {}", stats.edges);
    println!("Observations:   {}", stats.observations);
    println!("Last indexed:   {}", last_indexed);

    // Memory health one-liner
    {
        use olaf::memory::{ResolvedBranchScope, memory_health_report, format_memory_health_summary};
        let scope = match olaf::config::detect_git_branch(&cwd) {
            Some(b) => ResolvedBranchScope::Branch(b),
            None => ResolvedBranchScope::All,
        };
        match memory_health_report(&conn, &scope) {
            Ok(report) => println!("{}", format_memory_health_summary(&report)),
            Err(e) => eprintln!("Memory health: error ({})", e),
        }
    }

    print_diagnostics(&cwd)?;

    Ok(())
}

fn print_diagnostics(cwd: &std::path::Path) -> anyhow::Result<()> {
    println!();

    let (registered, mcp_path) = check_mcp_registered(cwd)?;
    println!("MCP config:         {}", mcp_path.display());
    println!(
        "MCP status:         {}",
        if registered { "registered" } else { "not registered" }
    );

    let [post, pre, session] = check_hooks_installed(cwd)?;
    println!(
        "Hook PostToolUse:   {}",
        if post { "installed" } else { "missing" }
    );
    println!(
        "Hook PreToolUse:    {}",
        if pre { "installed" } else { "missing" }
    );
    println!(
        "Hook SessionEnd:    {}",
        if session { "installed" } else { "missing" }
    );

    let rules_status = check_tool_rules(cwd)?;
    println!(
        "Tool preferences:   {}",
        match &rules_status {
            RulesFileStatus::Current => "current".to_string(),
            RulesFileStatus::Outdated { detected_hash } => {
                let short: String = detected_hash.chars().take(8).collect();
                format!("outdated (hash: {}…)", short)
            }
            RulesFileStatus::Missing => "missing".to_string(),
            RulesFileStatus::Malformed { reason } => format!("malformed ({})", reason),
        }
    );

    Ok(())
}
