# Changelog

## 0.5.0 — 2026-03-09

### Added

- **Content policy filtering** — define deny and redact rules in `.olaf/policy.toml` to exclude sensitive modules from all MCP output or redact implementation bodies while preserving signatures for navigation. Deny rules make symbols indistinguishable from non-existent; redact rules preserve signatures but strip bodies. Policy is loaded fresh on every tool call — no server restart needed.
- **Tool preference rules** — `olaf init` writes `.claude/rules/olaf-tools.md` with a tool routing table that guides Claude to prefer Olaf MCP tools over native file reads for codebase exploration. Includes hash-based drift detection: `olaf init` regenerates the file when the template changes (e.g., after upgrade), and `olaf status` reports the rules file state.
- **Subagent worktree resolution** — when Claude Code spawns a subagent with `isolation: "worktree"`, Olaf now resolves the worktree path back to the main repository root before database access, ensuring observations and snapshots are written to the correct `.olaf/index.db`. Distinguishes worktrees from submodules.

### Fixed

- Resolve clippy lints for Rust 1.94.

### Changed

- `olaf init` now performs five setup steps (added tool preference rules).
- Version is shown in the `olaf` branding header.

## 0.4.0 — 2026-03-04

### Added

- **Go parser** — TypeScript, JavaScript, Python, Rust, PHP, and now Go.
- **`get_brief`** — renamed from `run_pipeline`; unified entry point for context + impact.
- **`analyze_failure`** — parse stack traces and get context briefs focused on the failure path.
- **`submit_lsp_edges`** — inject type-resolved edges from language servers into the graph.
- **Multi-repo workspaces** — `get_brief` and `get_context` fan out across linked repos via `.olaf/workspace.toml`.
- **Score-explainable retrieval** — every context brief includes `## Retrieval Notes` showing why each pivot was selected.
- **Dead-end detection** — failed approaches are flagged in future context briefs.
- **Scored observation retrieval** — BM25 + recency decay + confidence scoring.
- **Branch-aware memory** — observations scoped to current git branch.
- **Auto-generated project rules** — recurring insights promoted to standing rules.
- **Live activity monitor** — `olaf monitor` with `--json`, `--tool`, `--errors-only` filters.
- **Observation consolidation** — near-duplicate observations merged via Jaccard similarity.
- **`uses_type` edges** in `get_impact` traversal.
