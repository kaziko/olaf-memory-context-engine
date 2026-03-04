# olaf

![Olaf](docs/hero.png)

**Codebase context engine for Claude Code & Codex (soon).**

Olaf is a codebase indexing and context retrieval engine that integrates with Claude Code via the Model Context Protocol (MCP). It parses your project's source files (TypeScript, JavaScript, Python, Rust, PHP), stores symbol-level summaries in a local SQLite database, and serves them to Claude Code on demand — so the AI always has accurate, up-to-date context about your codebase without reading every file on each request. Olaf also captures session observations and can restore files to pre-edit snapshots via built-in undo support.

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

- `get_context` — retrieve indexed summaries for files relevant to your task
- `get_impact` — find files that reference a given symbol or path
- `get_file_skeleton` — get the structure (functions, classes, exports) of a file
- `index_status` — check indexing coverage and freshness
- `save_observation` — store a session note for future recall
- `get_session_history` — retrieve observations and changes from recent sessions
- `list_restore_points` — view file snapshots available for undo
- `undo_change` — restore a file to a specific pre-edit snapshot

## Documentation

Full documentation: [https://kaziko.github.io/olaf-memory-context-engine](https://kaziko.github.io/olaf-memory-context-engine)

## License

MIT
