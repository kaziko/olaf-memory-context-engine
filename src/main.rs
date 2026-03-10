mod cli;

use clap::Parser;

#[derive(clap::Parser)]
#[command(about = "Codebase context engine for Claude Code", disable_version_flag = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Index the current project
    Index,
    /// Start the MCP stdio server
    Serve,
    /// Show index statistics
    Status,
    /// Initialize Olaf in the current project
    Init,
    /// Handle Claude Code hook events
    Observe {
        #[arg(long)]
        event: String,
    },
    /// Browse session observations
    Sessions {
        #[command(subcommand)]
        action: SessionsCommands,
    },
    /// Manage file restore points
    Restore {
        #[command(subcommand)]
        action: Option<RestoreSubcommands>,
        file: Option<std::path::PathBuf>,
        timestamp: Option<i64>,
    },
    /// Manage multi-repo workspace
    Workspace {
        #[command(subcommand)]
        action: WorkspaceCommands,
    },
    /// Live activity monitor — run in a separate terminal
    Monitor {
        /// Output events as JSON lines instead of formatted text
        #[arg(long)]
        json: bool,
        /// Show last N events on startup (default: 10)
        #[arg(long, default_value = "10")]
        tail: usize,
        /// Filter to specific tool name
        #[arg(long)]
        tool: Option<String>,
        /// Show only errors
        #[arg(long)]
        errors_only: bool,
        /// Force plain-text output (no TUI)
        #[arg(long)]
        plain: bool,
    },
    /// Generate shell completion scripts
    Completions {
        /// Shell to generate completions for
        shell: clap_complete::Shell,
    },
}

#[derive(clap::Subcommand)]
enum WorkspaceCommands {
    /// Initialize a workspace with the current repo
    Init,
    /// Add a repository to the workspace
    Add {
        /// Path to the repository to add
        path: std::path::PathBuf,
    },
    /// List workspace members and their status
    List,
    /// Validate workspace members health
    Doctor,
}

#[derive(clap::Subcommand)]
enum SessionsCommands {
    /// List recent sessions
    List,
    /// Show observations from a session
    Show { id: String },
}

#[derive(clap::Subcommand)]
enum RestoreSubcommands {
    /// List available snapshots for a file
    List { file: std::path::PathBuf },
}

/// Returns true only when `--version` or `-V` appears before any positional argument in argv.
/// Any subcommand (including `help` and future additions) is a positional argument and does
/// not start with `-`, so this check is future-proof without maintaining a name list.
fn has_toplevel_version_flag() -> bool {
    for arg in std::env::args().skip(1) {
        if !arg.starts_with('-') {
            return false; // positional arg (subcommand) found before --version/-V
        }
        if arg == "--version" || arg == "-V" {
            return true;
        }
    }
    false
}

fn main() -> anyhow::Result<()> {
    // Intercept top-level --version/-V before clap so the branding block appears first.
    // With disable_version_flag = true, clap does not handle these flags itself.
    if has_toplevel_version_flag() {
        cli::setup::print_branding();
        println!("olaf {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Index => {
            cli::index::run()?;
        }
        Commands::Serve => {
            cli::serve::run()?;
        }
        Commands::Status => {
            cli::status::run()?;
        }
        Commands::Init => {
            cli::init::run()?;
        }
        Commands::Observe { event } => {
            cli::observe::run(&event)?;
        }
        Commands::Sessions { action } => match action {
            SessionsCommands::List => {
                cli::sessions::run_list()?;
            }
            SessionsCommands::Show { id } => {
                cli::sessions::run_show(&id)?;
            }
        },
        Commands::Restore {
            action,
            file,
            timestamp,
        } => match action {
            Some(RestoreSubcommands::List { file }) => {
                cli::restore::run_list(&file)?;
            }
            None => {
                if let (Some(file), Some(ts)) = (file, timestamp) {
                    cli::restore::run_restore(&file, ts)?;
                } else {
                    anyhow::bail!(
                        "usage: olaf restore <file> <timestamp>  OR  olaf restore list <file>"
                    );
                }
            }
        },
        Commands::Workspace { action } => match action {
            WorkspaceCommands::Init => {
                cli::workspace::run_init()?;
            }
            WorkspaceCommands::Add { path } => {
                cli::workspace::run_add(&path)?;
            }
            WorkspaceCommands::List => {
                cli::workspace::run_list()?;
            }
            WorkspaceCommands::Doctor => {
                cli::workspace::run_doctor()?;
            }
        },
        Commands::Monitor { json, tail, tool, errors_only, plain } => {
            cli::monitor::run(json, tail, tool, errors_only, plain)?;
        }
        Commands::Completions { shell } => {
            use clap::CommandFactory;
            clap_complete::generate(shell, &mut Cli::command(), "olaf", &mut std::io::stdout());
        }
    }

    Ok(())
}
