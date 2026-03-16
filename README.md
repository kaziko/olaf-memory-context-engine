# Olaf

![Olaf](docs/hero.png)

**Codebase context engine and session memory for Claude Code.**

Olaf is a codebase indexing and context retrieval engine that integrates with Claude Code via the Model Context Protocol (MCP). It parses your project's source files (TypeScript, JavaScript, Python, Rust, PHP, Go), stores symbol-level summaries in a local SQLite database, and serves them to Claude Code on demand — so the AI always has accurate, up-to-date context about your codebase without reading every file on each request.

Olaf also acts as **session memory** — it automatically records decisions, errors, and file changes as observations linked to specific symbols and files. These observations persist across sessions, so Claude remembers what was tried before, what failed, and why certain decisions were made. Combined with pre-edit snapshots, this gives Claude both recall and undo.

### Features

- **Multi-language indexing** — TypeScript, JavaScript, Python, Rust, PHP, Go
- **Intent-aware context** — classifies your task (bug-fix, refactor, implementation) and adjusts retrieval depth and direction
- **Score-explainable retrieval** — every context brief includes a `## Retrieval Notes` section showing why each pivot symbol was selected (keyword scores, file hints, fallback) and a recency label on each memory observation
- **Token-budgeted briefs** — context fits within your budget, not a dump of every file
- **Impact analysis** — traces callers, implementors, and type-usage edges for any symbol
- **Execution path tracing** — find how symbol A reaches symbol B through the call graph
- **Session memory** — decisions, errors, and insights persist across conversations
- **Importance tiers** — observations are automatically assigned importance (low → critical) based on kind, scope, and context, so retrieval prioritizes what matters most
- **Project-scoped observations** — save observations with `scope: "project"` for cross-file insights that aren't tied to a single symbol or file
- **Semantic recall** — observation retrieval uses vector embeddings to rank results by semantic similarity to your current task, not just recency
- **Smart nudging** — detects two patterns: repeated repo-wide search commands (`rg`, `grep -r`) that `get_brief` would handle more efficiently, and file-thrash (repeated edits without recording insights). Appends a one-time suggestion to the next eligible tool response
- **Memory health diagnostics** — `memory_health` tool reports observation counts, staleness, consolidation stats, and actionable recommendations
- **Branch-aware memory** — observations are automatically scoped to the branch you're working on, so feature-branch context stays isolated from main
- **Dead-end detection** — when Claude records a failed approach, Olaf flags it in future context briefs so the same mistake isn't repeated
- **Observation consolidation** — near-duplicate observations are automatically merged, keeping memory concise without losing information
- **Auto-generated project rules** — when the same code-level insight or decision recurs across 3+ sessions for the same file or symbol, Olaf promotes it to a standing rule that appears in every relevant context brief. Not for workflow preferences (use CLAUDE.md for those) — for lessons Claude learns about your code through repeated work
- **Live activity monitor** — run `olaf monitor` in a separate terminal to see MCP tool calls, hook events, session lifecycle, and index operations in real time. Supports `--json`, `--tool`, `--errors-only` filters
- **Content policy filtering** — define deny and redact rules in `.olaf/policy.toml` to silently exclude sensitive modules from all MCP output, or redact implementation bodies while preserving signatures for navigation
- **Tool preference rules** — `olaf init` writes `.claude/rules/olaf-tools.md` so Claude automatically prefers Olaf MCP tools over native file reads for codebase exploration, with hash-based drift detection to keep rules current across upgrades
- **Pre-edit snapshots** — undo any AI edit instantly, no git required
- **LSP edge injection** — enrich the graph with type-resolved edges from language servers
- **Failure analysis** — parse stack traces and get context briefs focused on the error path
- **Multi-repo workspaces** — federated context retrieval across linked repositories via `.olaf/workspace.toml` (pivot search and context assembly span repos; impact analysis, memory, and graph traversal remain local per-repo)

### How much does it save?

**On a large codebase** — benchmarked on **kubernetes/kubernetes** (16,789 files, 302k symbols):

| | Regular tools (Grep + Read) | Olaf | Savings |
|-|-|-|-|
| Mean across 10 queries | ~11k tokens / 3-5 calls | ~1.2k tokens / 1 call | **78%** |
| Best case (file hint) | ~37k tokens | ~67 tokens | **99.8%** |

**On a small codebase** — benchmarked on Olaf's own source (15 modules, ~5,500 lines):

| | Regular tools (Grep + Read) | Olaf | Savings |
|-|-|-|-|
| Ranking algorithm | ~6,000 tokens / 5-7 calls | ~1,950 tokens / 1 call | **68%** |
| Cross-module trace | ~5,550 tokens / 7 calls | ~1,800 tokens / 1 call | **68%** |

One tool call instead of many. The gap widens on larger codebases where regular tools need even more rounds of searching. See the [full benchmark methodology](https://kaziko.github.io/olaf-memory-context-engine/reference/benchmarks/) for details.

**Note**: The external benchmark was run before Go edge extraction was added. Recall numbers reflect keyword-only retrieval — graph-assisted retrieval with 181k edges is expected to perform better. A re-run is planned.

## Install

### Homebrew (macOS) (recommended)

```sh
brew tap kaziko/olaf
brew install olaf
```

### Via cargo

```sh
cargo install olaf
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

**1. Initialize Olaf:**

```sh
cd /path/to/your/project
olaf init
```

This creates the `.olaf/` database, registers the MCP server in `.mcp.json`, installs Claude Code hooks, writes tool preference rules, and runs the initial index — all in one command.

**2. Open Claude Code** — it reads `.mcp.json` on startup and connects to Olaf automatically.

**3. Use it** — Olaf exposes these MCP tools to Claude:

**Context retrieval:**
- `get_brief` — start here. Context brief for any task with optional impact analysis
- `get_context` — token-budgeted context retrieval (fine-grained control)
- `get_impact` — find symbols that call, extend, or depend on a given symbol
- `get_file_skeleton` — structure of a file (signatures, edges, no bodies)
- `analyze_failure` — parse a stack trace or error and get a context brief focused on the failure path

**Session memory:**
- `save_observation` — record a decision, insight, or error linked to a symbol or file (supports `scope: "project"` for cross-file observations)
- `get_session_history` — retrieve past observations across sessions, ranked by relevance and semantic similarity
- `memory_health` — observation counts, staleness breakdown, consolidation stats, and actionable recommendations

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
