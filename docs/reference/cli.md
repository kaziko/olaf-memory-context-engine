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
| `olaf monitor` | Watch live activity events (tool calls, hooks, sessions, indexing). Flags: `--json` (JSON lines output), `--tail <N>` (last N events, default 10), `--tool <name>` (filter by tool), `--errors-only`. |
