# CLI Commands

All commands run from your project root.

## Core

| Command | Description |
|-|-|
| `olaf init` | Initialize Olaf: creates `.olaf/`, registers MCP server, installs hooks, writes tool preference rules, runs initial index. Safe to re-run — idempotent. |
| `olaf index` | Re-index the project manually. Only changed files are re-parsed (incremental). |
| `olaf status` | Show index health: file count, symbol count, edges, observations, MCP registration, hook status, tool rules status. |

## Sessions

| Command | Description |
|-|-|
| `olaf sessions list` | List recent sessions with observation counts and timestamps. |
| `olaf sessions show <id>` | Show all observations from a specific session. |

## Restore

| Command | Description |
|-|-|
| `olaf restore list <file>` | List available pre-edit snapshots for a file, newest first. |
| `olaf restore <file> <timestamp>` | Restore a file to a specific snapshot by timestamp. |

## Workspace

| Command | Description |
|-|-|
| `olaf workspace init` | Create a workspace manifest (`.olaf/workspace.toml`) with the current repo as first member. |
| `olaf workspace add <path>` | Add a repository to the workspace. Deduplicates by canonical path. |
| `olaf workspace list` | List workspace members with status: `indexed`, `not-indexed`, or `missing`. |
| `olaf workspace doctor` | Validate all members: path exists, DB opens, schema compatible, freshness report. |

## Utilities

| Command | Description |
|-|-|
| `olaf completions <shell>` | Print shell completion script for `bash`, `zsh`, `fish`, or `powershell`. |
| `olaf monitor` | Watch live activity events in a bordered TUI (see below). Flags: `--json` (JSON lines), `--tail <N>` (last N events, default 10), `--tool <name>` (filter by tool), `--errors-only`, `--plain` (force plain-text output). |

## Monitor TUI

When run on an interactive terminal, `olaf monitor` launches a bordered TUI with scrollable event log, color-coded sources, and a status bar.

### TUI activation

TUI activates when **all** of: stdout is a TTY, stdin is a TTY, `--json` is not passed, `--plain` is not passed, and `TERM` is not `dumb`. When any condition fails, output falls back to plain text (identical to previous behavior).

`NO_COLOR` does **not** disable TUI — it disables colors within the TUI (borders and layout still render, just monochrome).

### Keyboard shortcuts

| Key | Action |
|-|-|
| `↑` / `k` | Scroll up (pauses auto-follow) |
| `↓` / `j` | Scroll down |
| `G` / `End` | Resume auto-follow |
| `q` | Quit (dismisses help overlay first if open) |
| `?` | Toggle help overlay |

### Viewport modes

- **FOLLOW** (default): auto-scrolls to show newest events.
- **PAUSED**: activated by scrolling up. Status bar shows `PAUSED (N new)`. Press `G` or `End` to resume.

### Event buffer

TUI mode caps the in-memory buffer at 2000 events. When old events are dropped, a notice appears at the top. This cap does not apply to `--plain` or `--json` modes.
