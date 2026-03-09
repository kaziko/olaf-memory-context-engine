# Project Rules (Auto-Generated)

Individual observations decay over time — they age, get compressed at session end, and are eventually purged after 90 days. A critical insight from session 3 may not survive to session 30 because it lost the budget race to newer observations. Project rules solve this by detecting patterns that keep recurring and promoting them to durable, always-present context.

## How it works

1. At the end of each session, Olaf scans all `insight` and `decision` observations from the last 90 days
2. Observations are grouped by the file or symbol they're linked to
3. Within each group, Olaf looks for content overlap using keyword similarity — observations that say similar things about the same code area form a cluster
4. A cluster becomes a **rule candidate** when it contains 3+ observations from 3+ distinct sessions
5. New candidates start as **pending** — they are not injected into context briefs yet
6. When the pattern is reinforced by a 4th distinct session, the rule **auto-promotes to active**
7. Active rules appear in a `## Project Rules` section in every context brief where the rule's linked symbols or files are relevant

## What project rules are good for

Rules capture code-level lessons that emerge from repeated work in the same area:

- "Always check middleware chain before modifying auth routes" — an insight Claude recorded multiple times while working on `src/auth/`
- "The connection pool retry logic masks timeout errors" — a decision linked to `ConnectionPool` that kept coming up across sessions
- "Database column names must use snake_case" — a pattern linked to model files that Claude learned through repeated corrections

These are things Claude discovers about **your code** through hands-on work, not things you tell it upfront.

## What project rules are NOT for

Rules only form from `insight` and `decision` observations that are linked to specific files or symbols in your codebase. They will **not** capture:

- **Workflow preferences** — "always format summaries as lists, not tables" has no file or symbol scope
- **General coding style** — "use 2-space indentation" is not tied to a specific code area
- **Tool usage preferences** — "always run tests before committing" is a process rule, not a code insight
- **One-off instructions** — something you said once in one session won't cluster

For workflow preferences and project-wide conventions, use your project's `CLAUDE.md` file instead — Claude reads it as system instructions on every session.

### Project rules vs CLAUDE.md

| | CLAUDE.md | Project Rules |
|-|-|-|
| Created by | You, manually | Olaf, automatically from recurring observations |
| Scope | Entire project | Specific files and symbols |
| Content | Conventions, workflow preferences | Code-level insights and decisions |
| When applied | Every conversation | Only when querying related code |
| Lifespan | Until you change it | Until linked code changes structurally |

## Rule lifecycle

- **Pending** — detected pattern, not yet shown in briefs. Prevents false positives from coincidental clusters
- **Active** — confirmed by 4+ sessions. Injected into context briefs within a dedicated token budget
- **Inactive** — automatically invalidated when a linked symbol is renamed, removed, or has its signature changed. Inactive rules are not reactivated automatically — the pattern must re-emerge naturally from new observations

!!! note
    Rules are branch-scoped: observations from different branches never cluster together, so feature-branch experiments don't produce rules that leak into `main`.
