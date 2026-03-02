mod cli;

use clap::Parser;

#[derive(clap::Parser)]
#[command(version, about = "Codebase context engine for Claude Code")]
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

fn main() -> anyhow::Result<()> {
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
    }

    Ok(())
}
