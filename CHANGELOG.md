# Changelog

## 0.7.2 — 2026-03-28

### Added

- **Auto-skeleton in `get_brief`** — implementation and bugfix intents now auto-include a file skeleton of the primary edit target, giving agents immediate structural context without a second tool call.
- **Coverage and omission signals on `get_file_skeleton`** — skeleton responses include a `coverage:` footer indicating parser completeness (strong/unknown) and `omitted:` hints listing known parser blind spots per language.

## 0.7.1 — 2026-03-26

### Added

- **Skeleton detail levels** — `get_file_skeleton` accepts a `detail` parameter (`minimal`, `standard`, `detailed`) to control rendering verbosity. Minimal shows names and line ranges; detailed includes all children and dependency edges.
- **Declaration-aware skeleton rendering for Python, Go, and PHP** — skeletons now render nested children (class fields, enum variants, interface members) under their parent declarations, matching the existing Rust/TS/JS behavior.
- **Python decorator signatures** — decorated methods now include `@decorator` lines in skeleton output.
- **PHP constants and trait usage** — class constants and `use TraitName` statements are extracted as child symbols and rendered in skeletons.
- **Freshness metadata on all read tools** — `get_file_skeleton`, `get_impact`, `trace_flow`, `get_brief`, and `get_context` now append a `freshness:` footer indicating whether indexed data is current or stale. Uses blake3 hash comparison between disk and database. Stale files trigger on-demand reindex with `refresh_if_stale`. Workspace mode scopes freshness to `local_freshness` since remote members are not demand-reindexed.

### Fixed

- **TS/JS skeleton rendering** — fixed enum kind display, type alias signatures, JS class field extraction, and edge resolution for TypeScript and JavaScript parsers.
- **DB corruption recovery** — `open`/`migrate` now detects corrupt databases and automatically recreates them instead of crashing.

### Changed

- bump `ratatui` 0.29 → 0.30 (fixes `lru` CVE)
- bump `rustls-webpki` 0.103.9 → 0.103.10

## 0.7.0 — 2026-03-21

### Added

- **Sub-declaration indexing** — all parsers (Rust, TypeScript, JavaScript, Python, Go, PHP) now extract child symbols: struct fields, enum variants, trait items, interface members, class fields. Child symbols are stored with `parent_id` links in the symbol graph.
- **Declaration-aware skeleton rendering** — `get_file_skeleton` produces IDE-like outlines: enum variants, struct fields, and trait items render nested under their parent declarations; impl methods are visually grouped under their type by FQN pattern matching. Output uses `####` sub-headers for children.
- **Trait const extraction** — Rust parser now extracts `const` items from trait bodies (previously silently dropped).
- **Child symbol filtering in retrieval** — child symbols are excluded from FTS pivot selection (`rank_symbols_by_keywords` and `find_pivot_symbols`), restoring keyword recall@3 from 0.833 to 1.000.
- **Content policy on child symbols** — redact/deny rules are checked per-child individually; a redacted child under a visible parent shows `[redacted by policy]` without leaking dependency names.
- **Token-protected nesting** — `format_parent_with_children` caps combined children + methods at 50 entries with `... and N more` indicator.

### Changed

- **Sparse PageRank** — replaced petgraph's O(N·V²·E) dense PageRank with a sparse O(N·(V+E)) implementation. On kubernetes (304k nodes, 185k edges), indexing went from 40+ minutes to 59 seconds.
- **petgraph dependency removed** — the sparse PageRank implementation uses only standard library collections.
- **Child symbols excluded from PageRank** — fields, variants, and trait items have no call edges and get centrality=0.0 by design.
- Benchmark numbers updated: kubernetes now indexes 398k symbols (up from 302k due to child extraction).

### Breaking Changes

- **`EmbedError::DimensionMismatch` removed** — this variant was never constructed; external crates with exhaustive `match` on `EmbedError` must remove the arm.

### Internal

- `IntentProfile.bugfix_score` and `refactor_score` are now `#[cfg(test)]`-only; `impl_score` removed entirely.
- `ProjectRule` drops unused fields (`scope_fingerprint`, `is_active`, `stale_reason`, `created_at`, `updated_at`, `branch`).
- Removed spurious `#[allow(dead_code)]` from `ObservationRow.id` and `created_at`.

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
