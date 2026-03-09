# How Claude Uses Olaf

Once Olaf is connected, Claude can see all 11 tools and decides on its own whether to call them — you don't need to mention them in every prompt.

**The short version:** task-oriented prompts trigger Olaf automatically; vague prompts may not.

## Prompts that work well

Claude will reach for Olaf with these:

```
Help me fix the bug in the authentication flow
Refactor the session compression module
What does the payment service depend on?
```

Claude recognizes these as codebase tasks and will call `get_brief` to gather context before answering.

## How Olaf reads your intent

Olaf automatically classifies the intent behind your task description and adjusts how it retrieves context:

| Mode | Triggered by | What changes |
|-|-|-|
| **bug-fix** | "fix", "debug", "crash", "error" | Deeper inbound traversal — traces callers and error paths |
| **refactor** | "refactor", "rename", "restructure" | Wide outbound traversal — surfaces everything that would break |
| **implementation** | "add", "implement", "create", "extend" | Focuses on integration points and adjacent surfaces |
| **balanced** | Vague or mixed signals | Even traversal, wider pivot pool |

The detected mode, confidence score, and matched signals are included in every context brief so Claude understands how the context was shaped. If the intent is ambiguous (e.g. "refactor and fix"), confidence falls below the threshold and Olaf falls back to balanced mode automatically.

## What's in every context brief

Every response from `get_context` and `get_brief` ends with a `## Retrieval Notes` section explaining the retrieval decisions:

```
## Retrieval Notes
- authenticate (auth::authenticate): kw=2 deg=5 [bug-fix]
- SessionStore (session::SessionStore): file-hint "session.rs" [bug-fix]
- legacy_handler (handler::legacy_handler): fallback [bug-fix]
```

Each line shows the pivot symbol, its fully-qualified name, why it was selected, and the active intent mode:

| Reason | Meaning |
|-|-|
| `kw=N deg=M` | Matched N intent keywords; M inbound edges (higher = more central) |
| `file-hint "…"` | Selected because it lives in a file you named in `file_hints` |
| `caller-supplied` | FQN was passed directly (e.g. from a stack trace in `analyze_failure`) |
| `fallback` | No keyword or file-hint match — first symbols in the index |

If any selected pivots were dropped (sensitive path or token budget exhausted), an omission count is appended: `(N pivots omitted: budget/sensitive-path)`.

If active project rules match the query's symbols or files, they appear in a `## Project Rules` section with evidence metadata (observation count, session count, recency).

Session memory observations also carry a recency label — `(recent)`, `(aged)`, or `(stale)` — based on age and staleness, so Claude knows how much to trust each past observation. Observations are sorted by relevance score before budget fitting, so the most current and non-stale entries survive on tight budgets.

In multi-repo workspace mode, each retrieval note also shows the source repo label and selection strategy:

```
## Retrieval Notes
- PaymentProcessor [backend] (local-priority): kw=1 deg=3 [implementation]
- DashboardRenderer [frontend] (remote-round-robin): kw=1 deg=1 [implementation]
```

## Prompts that may not work (too vague)

```
Help me with my code
What's in this project?
```

Vague prompts don't give Claude enough signal. It may fall back to reading files directly instead.

## When you want to be certain

Add an explicit instruction:

```
Use get_brief to understand the authentication module, then help me fix the login bug
```

This guarantees Olaf is used and Claude starts with a full picture of the relevant code.
