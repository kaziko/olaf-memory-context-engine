---
hide:
  - navigation
---

# Olaf

<div style="text-align: center" markdown>
![Olaf](assets/hero.png){ width="600" }
</div>

**Codebase context engine and session memory for Claude Code.**

Olaf is a codebase indexing and context retrieval engine that integrates with Claude Code via the Model Context Protocol (MCP). It parses your project's source files (TypeScript, JavaScript, Python, Rust, PHP, Go), stores symbol-level summaries in a local SQLite database, and serves them to Claude Code on demand — so the AI always has accurate, up-to-date context about your codebase without reading every file on each request.

Olaf also acts as **session memory** — it automatically records decisions, errors, and file changes as observations linked to specific symbols and files. These observations persist across sessions, so Claude remembers what was tried before, what failed, and why certain decisions were made. Combined with pre-edit snapshots, this gives Claude both recall and undo.

---

## Why Olaf?

Claude Code reads files. A lot of them. On every request it may scan dozens of source files to understand your codebase — burning tokens, slowing responses, and often still missing the right context. Olaf replaces that with a pre-built symbol and dependency graph. Claude asks Olaf, Olaf returns exactly what's relevant, and the rest of your codebase stays out of the way.

<div class="grid cards" markdown>

- **Free**

---

    Because why not. Olaf is open source and runs entirely on your machine.

- **Built specifically for Claude Code**

    ---

    `olaf init` wires everything up in one command — MCP server, hooks, index. No manual config.

- **Session memory across conversations**

    ---

    Decisions, errors, and file changes persist. Claude knows what was tried before and why.

- **Undo any AI edit instantly**

    ---

    Before every file change, Olaf creates a shadow snapshot. Restore with one command — no git required.

- **Your code never leaves your machine**

    ---

    The index lives in `.olaf/index.db` in your project. No cloud sync, no telemetry.

</div>

## Features

- **Multi-language indexing** — TypeScript, JavaScript, Python, Rust, PHP, Go
- **Intent-aware context** — classifies your task (bug-fix, refactor, implementation) and adjusts retrieval depth
- **Score-explainable retrieval** — every context brief shows why each symbol was selected
- **Token-budgeted briefs** — context fits within your budget, not a dump of every file
- **Impact analysis** — traces callers, implementors, and type-usage edges
- **Execution path tracing** — find how symbol A reaches symbol B through the call graph
- **Session memory** — decisions, errors, and insights persist across conversations
- **Branch-aware memory** — observations scoped to the branch you're working on
- **Dead-end detection** — failed approaches flagged in future context briefs
- **Observation consolidation** — near-duplicate observations merged automatically
- **Auto-generated project rules** — recurring insights promoted to standing rules
- **Live activity monitor** — `olaf monitor` for real-time MCP and hook events
- **Content policy filtering** — deny/redact rules for sensitive modules
- **Tool preference rules** — automatic Claude Code tool routing via `.claude/rules/`
- **Pre-edit snapshots** — undo any AI edit instantly
- **LSP edge injection** — enrich the graph with type-resolved edges
- **Failure analysis** — parse stack traces and get targeted context
- **Multi-repo workspaces** — federated context across linked repositories

[Get started](getting-started/installation.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/kaziko/olaf-memory-context-engine){ .md-button }
