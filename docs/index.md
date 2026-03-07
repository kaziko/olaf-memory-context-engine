---
layout: default
title: Getting Started
---

![Olaf]({{ "/hero.png" | relative_url }})

# Getting Started with Olaf

Olaf is a codebase context engine and session memory for Claude Code. It indexes your project's symbols and dependencies, then exposes them through an MCP server so Claude can instantly retrieve focused, token-budgeted context for any task. It also records observations — decisions, errors, insights — linked to specific symbols and files, so Claude remembers what happened in previous sessions. Install it once per project and Claude automatically gets both the context and the memory it needs.

---

- [Why Olaf](#why-olaf)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Verify It's Working](#verify-its-working)
- [Troubleshooting](#troubleshooting)
- [Upgrade](#upgrade)
- [How Claude Uses Olaf](#how-claude-uses-olaf)
- [Available MCP Tools](#available-mcp-tools)
- [CLI Reference](#cli-reference)

---

## Why Olaf

Claude Code reads files. A lot of them. On every request it may scan dozens of source files to understand your codebase — burning tokens, slowing responses, and often still missing the right context. Olaf replaces that with a pre-built symbol and dependency graph. Claude asks Olaf, Olaf returns exactly what's relevant, and the rest of your codebase stays out of the way.

**Free with no limits.** because why not. Olaf is open source and runs entirely on your machine.

**Built specifically for Claude Code.** `olaf init` wires everything up in one command — MCP server registration, hooks, initial index. No manual config files, no agent-specific workarounds.

**Session memory across conversations.** Olaf automatically records decisions, errors, and file changes as observations linked to symbols and files. When Claude retrieves context, relevant past observations surface automatically — so it knows what was tried before, what failed, and why.

**Undo any AI edit instantly.** Before every file change, Olaf creates a shadow snapshot. If Claude makes a mess, `undo_change` restores the file to exactly how it was — no git required, no lost work.

**Your code never leaves your machine.** The index lives in `.olaf/index.db` in your project. No cloud sync, no telemetry.

## Installation

Choose any of the three methods:

### Homebrew (macOS — recommended)

```sh
brew tap kaziko/olaf
brew install olaf
```

### cargo

```sh
cargo install olaf
```

Requires Rust toolchain. Install from [rustup.rs](https://rustup.rs) if needed.

### Pre-built binary

Download the binary for your platform from the [GitHub Releases page](https://github.com/kaziko/olaf-memory-context-engine/releases):

| Platform | Binary |
|-|-|
| macOS (Apple Silicon) | `olaf-aarch64-apple-darwin` |
| macOS (Intel) | `olaf-x86_64-apple-darwin` |
| Linux (x86_64) | `olaf-x86_64-unknown-linux-musl` |
| Linux (ARM64) | `olaf-aarch64-unknown-linux-musl` |

Rename to `olaf`, make executable (`chmod +x olaf`), and move to a directory in your PATH.

## Quick Start

### 1. Navigate to your project

```sh
cd /path/to/your/project
```

### 2. Initialize Olaf

```sh
olaf init
```

`olaf init` does four things automatically:
- Creates `.olaf/` — local database directory
- Registers the MCP server in `.mcp.json` — Claude Code reads this to connect
- Installs hooks in `.claude/settings.local.json` — enables passive observation capture and shadow snapshots
- Runs the initial index — scans your project files and builds the symbol graph

### 3. Open Claude Code

Claude Code reads `.mcp.json` on startup and connects to the Olaf MCP server automatically. No manual configuration needed.

### 4. Ask Claude for context

In Claude Code, try:

```
Use get_brief to understand the authentication module
```

Claude calls the `get_brief` MCP tool, which retrieves a token-budgeted context brief with optional impact analysis — covering relevant symbols, their dependencies, and any saved observations for that area of the codebase.

You can also use individual tools for targeted queries: `get_context` for context only, `get_impact` for impact analysis only.

## Verify It's Working

Run `olaf status` to see a health report:

```
Files indexed:  142
Symbols:        1,847
Edges:          3,204
Observations:   12
Last indexed:   2026-03-04 10:00:00 UTC

MCP config:         /path/to/project/.mcp.json
MCP status:         registered
Hook PostToolUse:   installed
Hook PreToolUse:    installed
Hook SessionEnd:    installed
```

A healthy installation shows:
- `MCP status: registered`
- All three hooks: `installed`
- Non-zero file and symbol counts

In Claude Code, ask Claude to call `get_context` for a specific task. If it returns a context brief with symbols and file paths, Olaf is working correctly.

## Troubleshooting

### MCP server not connecting

Run `olaf status`. If `MCP status: not registered`, the `.mcp.json` file is missing or misconfigured. Fix: re-run `olaf init` from your project root (it is idempotent — safe to run multiple times).

### Hooks missing

Run `olaf status`. If any hook shows `missing` (e.g., `Hook PostToolUse: missing`), the hook configuration in `.claude/settings.local.json` was not written or was removed. Fix: re-run `olaf init`.

### Index is empty (Files indexed: 0)

The index was not built or was cleared. Fix: run `olaf index` manually from your project root, then `olaf status` to confirm the counts.

### Index not initialized

If `olaf status` prints `Index not initialized. Run olaf index to build the index.`, run `olaf init` first to set up the full environment, then `olaf index` if the index still doesn't build automatically.

## Upgrade

### Homebrew

```sh
brew update && brew upgrade olaf
```

### cargo

```sh
cargo install olaf --force
```

### Pre-built binary

Download the latest release from the [GitHub Releases page](https://github.com/kaziko/olaf-memory-context-engine/releases), replace the existing `olaf` binary in your PATH with the new one.

---

## How Claude Uses Olaf

Once Olaf is connected, Claude can see all 11 tools and decides on its own whether to call them — you don't need to mention them in every prompt.

**The short version:** task-oriented prompts trigger Olaf automatically; vague prompts may not.

### Prompts that work well (Claude will reach for Olaf)

```
Help me fix the bug in the authentication flow
Refactor the session compression module
What does the payment service depend on?
```

Claude recognizes these as codebase tasks and will call `get_brief` to gather context before answering.

### How Olaf reads your intent

Olaf automatically classifies the intent behind your task description and adjusts how it retrieves context:

| Mode | Triggered by | What changes |
|-|-|-|
| **bug-fix** | "fix", "debug", "crash", "error" | Deeper inbound traversal — traces callers and error paths |
| **refactor** | "refactor", "rename", "restructure" | Wide outbound traversal — surfaces everything that would break |
| **implementation** | "add", "implement", "create", "extend" | Focuses on integration points and adjacent surfaces |
| **balanced** | Vague or mixed signals | Even traversal, wider pivot pool |

The detected mode, confidence score, and matched signals are included in every context brief so Claude understands how the context was shaped. If the intent is ambiguous (e.g. "refactor and fix"), confidence falls below the threshold and Olaf falls back to balanced mode automatically.

### Prompts that may not (too vague)

```
Help me with my code
What's in this project?
```

Vague prompts don't give Claude enough signal. It may fall back to reading files directly instead.

### When you want to be certain

Add an explicit instruction:

```
Use get_brief to understand the authentication module, then help me fix the login bug
```

This guarantees Olaf is used and Claude starts with a full picture of the relevant code.

### Undoing AI edits

Before every file change, Olaf automatically saves a snapshot. If Claude makes a mess, you can restore any file to exactly how it was.

**To undo the last edit to a file:**

```
Use undo_change to restore src/auth.rs to its previous state
```

**To see all available snapshots for a file first:**

```
Use list_restore_points for src/auth.rs
```

Claude will list the snapshots with timestamps, then you can pick one:

```
Restore src/auth.rs to snapshot 1741234567890-12345-3
```

Snapshots are created automatically — no git required, no manual setup.

### Session memory

Olaf records observations — decisions, errors, insights, file changes — linked to specific symbols and files. These persist across sessions and surface automatically when Claude retrieves context for the same area of code.

**Why this matters:** without memory, Claude starts every session from zero. If a previous session tried an approach that failed, Claude has no way to know. With Olaf, past observations appear in the context brief alongside the code, so Claude avoids repeating mistakes and builds on previous work.

**To save an observation manually:**

```
Use save_observation to record that the retry logic in connection_pool was removed because it masked timeout errors
```

**To review past observations:**

```
Use get_session_history filtered to src/db/connection_pool.rs
```

Most observations are captured automatically by the PostToolUse hook — you only need `save_observation` for high-level decisions or insights that Claude wouldn't otherwise record.

### What runs automatically (no prompting needed)

Three hooks run silently in the background during every Claude Code session:

- **PostToolUse** — records every file edit and shell command as an observation
- **PreToolUse** — creates a snapshot before every AI edit (enables `undo_change`)
- **SessionEnd** — compresses session history to retain key insights

You never need to ask for these — they fire on their own.

---

## Available MCP Tools

Once connected, Claude can use these tools:

**Context retrieval**

| Tool | Description |
|-|-|
| `get_brief` | Start here. Context brief for any task; includes impact analysis when `symbol_fqn` is provided. Use `get_context` or `get_impact` for fine-grained control |
| `get_context` | Token-budgeted context brief for a task; triggers incremental re-index |
| `get_impact` | Find symbols that call, extend, implement, or use a given symbol FQN as a type |
| `get_file_skeleton` | Signatures, docstrings, and edges for a file (no implementation bodies) |
| `analyze_failure` | Parse a stack trace or error output and return a context brief focused on the failure path |

**Session memory**

| Tool | Description |
|-|-|
| `save_observation` | Record a decision, insight, or error linked to a symbol FQN or file path |
| `get_session_history` | Observations from recent sessions, filterable by file or symbol; supports relevance-ranked retrieval |

**Code navigation & status**

| Tool | Description |
|-|-|
| `trace_flow` | Trace execution paths between two symbols through the dependency graph |
| `index_status` | File count, symbol count, edge count, observation count, last indexed timestamp |

**Safety**

| Tool | Description |
|-|-|
| `list_restore_points` | Pre-edit snapshots for a file, sorted newest-first |
| `undo_change` | Restore a file to a specific snapshot; records a decision observation |

---

## CLI Reference

All commands run from your project root.

| Command | Description |
|-|-|
| `olaf init` | Initialize Olaf: creates `.olaf/`, registers MCP server, installs hooks, runs initial index. Safe to re-run — idempotent. |
| `olaf index` | Re-index the project manually. Only changed files are re-parsed (incremental). |
| `olaf status` | Show index health: file count, symbol count, edges, observations, MCP registration status, hook status. |
| `olaf sessions list` | List recent sessions with observation counts and timestamps. |
| `olaf sessions show <id>` | Show all observations from a specific session. |
| `olaf restore list <file>` | List available pre-edit snapshots for a file, newest first. |
| `olaf restore <file> <timestamp>` | Restore a file to a specific snapshot by timestamp. |
| `olaf completions <shell>` | Print shell completion script for `bash`, `zsh`, `fish`, or `powershell`. |
