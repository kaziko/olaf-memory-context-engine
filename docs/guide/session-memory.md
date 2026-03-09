# Session Memory

Olaf records observations — decisions, errors, insights, file changes — linked to specific symbols and files. These persist across sessions and surface automatically when Claude retrieves context for the same area of code.

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

## Branch-scoped memory

Observations are automatically tagged with the current git branch at capture time. When you retrieve context or session history, only observations from the current branch (plus branch-less legacy observations) are returned by default.

To see everything across all branches, pass `branch: "all"` to `get_session_history`, `get_context`, or `get_brief`.

This keeps feature-branch experiments from polluting `main` memory and vice versa.
