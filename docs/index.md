---
layout: default
title: Getting Started
---

![Olaf]({{ "/hero.png" | relative_url }})

# Getting Started with Olaf

Olaf is a codebase context engine for Claude Code. It indexes your project's symbols and dependencies, then exposes them through an MCP server so Claude can instantly retrieve focused, token-budgeted context for any task — no manual file pasting required. Install it once per project and Claude automatically gets the context it needs.

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
Use get_context to understand the authentication module
```

Claude calls the `get_context` MCP tool, which retrieves a token-budgeted context brief covering the relevant symbols, their dependencies, and any saved observations for that area of the codebase.

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
brew upgrade olaf
```

### cargo

```sh
cargo install olaf --force
```

### Pre-built binary

Download the latest release from the [GitHub Releases page](https://github.com/kaziko/olaf-memory-context-engine/releases), replace the existing `olaf` binary in your PATH with the new one.

---

## How Claude Uses Olaf

Once Olaf is connected, Claude can see all 8 tools and decides on its own whether to call them — you don't need to mention them in every prompt.

**The short version:** task-oriented prompts trigger Olaf automatically; vague prompts may not.

### Prompts that work well (Claude will reach for Olaf)

```
Help me fix the bug in the authentication flow
Refactor the session compression module
What does the payment service depend on?
```

Claude recognizes these as codebase tasks and will call `get_context` or `get_impact` to gather context before answering.

### Prompts that may not (too vague)

```
Help me with my code
What's in this project?
```

Vague prompts don't give Claude enough signal. It may fall back to reading files directly instead.

### When you want to be certain

Add an explicit instruction:

```
Use get_context to understand the authentication module, then help me fix the login bug
```

This guarantees Olaf is used and Claude starts with a full picture of the relevant code.

### What runs automatically (no prompting needed)

Three hooks run silently in the background during every Claude Code session:

- **PostToolUse** — records every file edit and shell command as an observation
- **PreToolUse** — creates a snapshot before every AI edit (enables `undo_change`)
- **SessionEnd** — compresses session history to retain key insights

You never need to ask for these — they fire on their own.

---

## Available MCP Tools

Once connected, Claude can use these tools:

| Tool | Description |
|-|-|
| `get_context` | Token-budgeted context brief for a task; triggers incremental re-index |
| `get_impact` | Find symbols that call, extend, or implement a given symbol FQN |
| `get_file_skeleton` | Signatures, docstrings, and edges for a file (no implementation bodies) |
| `index_status` | File count, symbol count, edge count, observation count, last indexed timestamp |
| `save_observation` | Store an insight or decision linked to a symbol FQN or file path |
| `get_session_history` | Observations and changes from recent sessions, filterable by file or symbol |
| `list_restore_points` | Pre-edit snapshots for a file, sorted newest-first |
| `undo_change` | Restore a file to a specific snapshot; records a decision observation |
