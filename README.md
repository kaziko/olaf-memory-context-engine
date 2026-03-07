# olaf

![Olaf](docs/hero.png)

**Codebase context engine for Claude Code & Codex (soon).**

Olaf is a codebase indexing and context retrieval engine that integrates with Claude Code via the Model Context Protocol (MCP). It parses your project's source files (TypeScript, JavaScript, Python, Rust, PHP, Go), stores symbol-level summaries in a local SQLite database, and serves them to Claude Code on demand — so the AI always has accurate, up-to-date context about your codebase without reading every file on each request.

Olaf also acts as **session memory** — it automatically records decisions, errors, and file changes as observations linked to specific symbols and files. These observations persist across sessions, so Claude remembers what was tried before, what failed, and why certain decisions were made. Combined with pre-edit snapshots, this gives Claude both recall and undo.

### Features

- **Multi-language indexing** — TypeScript, JavaScript, Python, Rust, PHP, Go
- **Intent-aware context** — classifies your task (bug-fix, refactor, implementation) and adjusts retrieval depth and direction
- **Token-budgeted briefs** — context fits within your budget, not a dump of every file
- **Impact analysis** — traces callers, implementors, and type-usage edges for any symbol
- **Execution path tracing** — find how symbol A reaches symbol B through the call graph
- **Session memory** — decisions, errors, and insights persist across conversations
- **Pre-edit snapshots** — undo any AI edit instantly, no git required
- **LSP edge injection** — enrich the graph with type-resolved edges from language servers
- **Failure analysis** — parse stack traces and get context briefs focused on the error path
- **Multi-repo workspaces** — federated context retrieval across linked repositories via `.olaf/workspace.toml` (pivot search and context assembly span repos; impact analysis, memory, and graph traversal remain local per-repo)

## Install

### Via cargo

```sh
cargo install olaf
```

### Homebrew (macOS)

```sh
brew tap kaziko/olaf
brew install olaf
```

### Pre-built binaries

Download a pre-built binary for your platform from the [GitHub Releases page](https://github.com/kaziko/olaf-memory-context-engine/releases):

| Platform | Binary |
|-|-|
| macOS (Apple Silicon) | `olaf-aarch64-apple-darwin` |
| macOS (Intel) | `olaf-x86_64-apple-darwin` |
| Linux x86_64 (static) | `olaf-x86_64-unknown-linux-musl` |
| Linux arm64 (static) | `olaf-aarch64-unknown-linux-musl` |

Linux binaries are fully static — no glibc dependency.

## Quick Start

**1. Index your project:**

```sh
cd /path/to/your/project
olaf init
```

**2. Configure Claude Code MCP** — `olaf init` writes this automatically to `.mcp.json`:

```json
{
  "mcpServers": {
    "olaf": {
      "command": "/path/to/olaf",
      "args": ["serve"],
      "type": "stdio"
    }
  }
}
```

**3. Use in Claude Code** — Olaf exposes these MCP tools to Claude:

**Context retrieval:**
- `get_brief` — start here. Context brief for any task with optional impact analysis
- `get_context` — token-budgeted context retrieval (fine-grained control)
- `get_impact` — find symbols that call, extend, or depend on a given symbol
- `get_file_skeleton` — structure of a file (signatures, edges, no bodies)
- `analyze_failure` — parse a stack trace or error and get a context brief focused on the failure path

**Session memory:**
- `save_observation` — record a decision, insight, or error linked to a symbol or file
- `get_session_history` — retrieve past observations across sessions, ranked by relevance

**Code navigation:**
- `trace_flow` — trace execution paths between two symbols through the call graph
- `index_status` — check indexing coverage and freshness

**LSP integration:**
- `submit_lsp_edges` — inject type-resolved edges from a language server (interface implementations, dynamic dispatch, generics) into the graph for richer `get_impact` and `trace_flow` results

**Multi-repo workspace:**
- `get_brief` and `get_context` automatically fan out across linked repos when `.olaf/workspace.toml` exists
- Impact analysis, memory, and graph traversal remain local per-repo

**Safety:**
- `list_restore_points` — view pre-edit snapshots available for undo
- `undo_change` — restore a file to a specific snapshot

## Documentation

Full documentation: [https://kaziko.github.io/olaf-memory-context-engine](https://kaziko.github.io/olaf-memory-context-engine)

## License

MIT

## Keywords

mcp, model-context-protocol, claude-code, codebase-indexing, context-engine, code-intelligence, session-memory, claude-memory, automatic memory, symbol-indexing, ai-tools, llm-context, undo-snapshots
