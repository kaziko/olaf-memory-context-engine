# Session Memory

Olaf records observations — decisions, errors, insights, file changes — linked to specific symbols and files. These persist across sessions and surface automatically when Claude retrieves context for the same area of code.

Session memory is for **code-related** knowledge: what was tried, what failed, why a design decision was made, how a particular module works. It is not the place for workflow preferences, output formatting ("use lists instead of tables"), or coding style rules — those belong in `CLAUDE.md`, which Claude reads at the start of every session.

## Why this matters

Without memory, Claude starts every session from zero. If a previous session tried an approach that failed, Claude has no way to know. With Olaf, past observations appear in the context brief alongside the code, so Claude avoids repeating mistakes and builds on previous work.

## Saving observations manually

```
Use save_observation to record that the retry logic in connection_pool
was removed because it masked timeout errors
```

Most observations are captured automatically by the PostToolUse hook — you only need `save_observation` for high-level decisions or insights that Claude wouldn't otherwise record.

## Reviewing past observations

```
Use get_session_history filtered to src/db/connection_pool.rs
```

## Importance tiers

Every observation is assigned an importance level — **low**, **medium**, **high**, or **critical** — based on its kind and scope:

- `file_change` and `tool_call` (auto-captured) → low
- `insight` and `error` → medium
- `decision` → high
- Project-scoped decisions → high (elevated automatically)

When retrieval results exceed the token budget, lower-importance observations are dropped first. You can override the default by passing `importance` to `save_observation`.

## Project-scoped observations

Some insights apply to the project as a whole, not a specific file or symbol — for example, "this codebase uses the repository pattern for all DB access." Save these with `scope: "project"`:

```
Use save_observation with scope "project" to record that all database
access goes through the repository pattern
```

Project-scoped observations appear in context briefs regardless of which file is being explored.

## Semantic recall

When you retrieve observations via `get_session_history` or through context briefs, Olaf ranks results by **semantic similarity** to your current task — not just recency. This means a two-week-old insight about "connection pool timeout handling" will surface when you're debugging a timeout, even if dozens of unrelated observations were recorded since.

Semantic ranking uses vector embeddings computed in the background. New observations are embedded automatically; no configuration needed.

## Smart nudging

Olaf detects two patterns that suggest Claude could work more effectively and appends a one-time suggestion to the next eligible tool response:

**Repo-wide search detection** — when Claude uses `rg` or `grep -r` three or more times in the recent 10 Bash commands, Olaf suggests switching to `get_brief` for token-budgeted exploration:

> [Olaf] You've used repo-wide search 3 times recently. For exploration, try: get_brief({"intent": "find where auth tokens are validated"})

**File-thrash detection** — when the same file is edited 3+ times within 5 minutes without recording any insight, decision, or error:

> [Olaf] Multiple edits to `src/auth.rs` in the last 5 minutes without saving an insight. Consider: save_observation(...)

Both nudges fire at most once per session (whichever triggers first) and are automatically suppressed if you save a valuable observation at any point. When both signals are present, the bash-search nudge takes priority. Nudges only appear on read-oriented tools (`get_brief`, `get_context`, `get_session_history`, `memory_health`, `get_file_skeleton`, `get_impact`, `trace_flow`, `analyze_failure`) — never on mutation tools like `save_observation` or JSON-returning tools like `submit_lsp_edges`.

## Memory health

Run `memory_health` to get a diagnostic report of your observation store:

- Observation counts by kind and importance
- Staleness breakdown (how many observations reference code that has since changed)
- Consolidation statistics
- Actionable recommendations (e.g., "42% of observations are stale — consider reviewing")

## Branch-scoped memory

Observations are automatically tagged with the current git branch at capture time. When you retrieve context or session history, only observations from the current branch (plus branch-less legacy observations) are returned by default.

To see everything across all branches, pass `branch: "all"` to `get_session_history`, `get_context`, or `get_brief`.

This keeps feature-branch experiments from polluting `main` memory and vice versa.
