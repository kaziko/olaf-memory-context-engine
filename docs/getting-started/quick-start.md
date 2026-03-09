# Quick Start

## 1. Navigate to your project

```sh
cd /path/to/your/project
```

## 2. Initialize Olaf

```sh
olaf init
```

`olaf init` does five things automatically:

- Creates `.olaf/` — local database directory
- Registers the MCP server in `.mcp.json` — Claude Code reads this to connect
- Installs hooks in `.claude/settings.local.json` — enables passive observation capture and shadow snapshots
- Writes tool preference rules to `.claude/rules/olaf-tools.md` — guides Claude to prefer Olaf MCP tools over native file reads
- Runs the initial index — scans your project files and builds the symbol graph

## 3. Open Claude Code

Claude Code reads `.mcp.json` on startup and connects to the Olaf MCP server automatically. No manual configuration needed.

## 4. Ask Claude for context

In Claude Code, try:

```
Use get_brief to understand the authentication module
```

Claude calls the `get_brief` MCP tool, which retrieves a token-budgeted context brief with optional impact analysis — covering relevant symbols, their dependencies, and any saved observations for that area of the codebase.

You can also use individual tools for targeted queries: `get_context` for context only, `get_impact` for impact analysis only.

## What runs automatically

Three hooks run silently in the background during every Claude Code session:

- **PostToolUse** — records every file edit and shell command as an observation
- **PreToolUse** — creates a snapshot before every AI edit (enables `undo_change`)
- **SessionEnd** — compresses session history, detects recurring patterns, and promotes them to project rules

You never need to ask for these — they fire on their own.
