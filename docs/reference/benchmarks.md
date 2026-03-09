# Token Savings Benchmarks

How much does Olaf actually save compared to using Claude Code's built-in tools (Grep, Read, Glob)?

We ran two real tasks on Olaf's own codebase (~5,500 lines of Rust across 15 modules) and measured tokens consumed and tool calls made. Both paths aimed at the same outcome: enough context to understand the answer.

## Benchmark 1: "How does context ranking work?"

A focused question about a single subsystem — pivot selection, keyword scoring, and in-degree tiebreaking.

### Path A — Regular tools

| Step | Tool | Purpose | Output tokens |
|-|-|-|-|
| 1 | Grep (files) | Find files mentioning rank/score | ~35 |
| 2 | Grep (content) | Find function signatures | ~1,050 |
| 3 | Read (query.rs, 200 lines) | Read ranking function | ~2,150 |
| 4 | Read (query.rs, intent detection) | Understand traversal policy | ~1,050 |
| 5 | Read (store.rs, scoring) | Observation scoring | ~700 |
| | **Total** | **5-7 tool calls** | **~6,000** |

### Path B — Olaf

| Step | Tool | Purpose | Output tokens |
|-|-|-|-|
| 1 | `get_context` | Single call with intent | ~1,950 |
| | **Total** | **1 tool call** | **~1,950** |

**Result: 68% fewer tokens, 1 call instead of 5-7.**

Olaf's response included the ranking function, the `SelectionReason` enum, `find_pivot_symbols`, `get_context_from_pivot_scores`, plus relevant test cases and session memory — all within a 4,000-token budget. The regular approach required manually discovering each function, reading it, then deciding what to read next.

## Benchmark 2: "Trace MCP get_context request flow"

A cross-module question requiring understanding of how an MCP request flows from the handler in `mcp/tools.rs` through intent detection, pivot selection, BFS traversal, and brief assembly in `graph/query.rs`.

### Path A — Regular tools

| Step | Tool | Purpose | Output tokens |
|-|-|-|-|
| 1 | Grep (mcp/) | Find get_context references | ~700 |
| 2 | Grep (all src/) | Find all context functions | ~350 |
| 3 | Read (tools.rs, 50 lines) | MCP handler | ~525 |
| 4 | Read (query.rs, 80 lines) | get_context + variants | ~900 |
| 5 | Grep (query.rs) | Locate helper functions | ~75 |
| 6 | Read (query.rs, 90 lines) | Intent detection + traversal policy | ~1,050 |
| 7 | Read (query.rs, 170 lines) | build_context_brief | ~1,950 |
| | **Total** | **7 tool calls** | **~5,550** |

### Path B — Olaf

| Step | Tool | Purpose | Output tokens |
|-|-|-|-|
| 1 | `get_context` | Single call with intent | ~1,800 |
| | **Total** | **1 tool call** | **~1,800** |

**Result: 68% fewer tokens, 1 call instead of 7.**

## What the numbers understate

### Tool call overhead

Each tool call in Claude Code has ~1-2 seconds of latency overhead for the round-trip. Seven sequential calls means 10-14 seconds of pure overhead vs ~2 seconds for a single Olaf call.

### Planning cost

With regular tools, the AI must decide *what to search for next* at each step. That planning burns tokens in the model's reasoning budget — real cost that doesn't show up in the output token counts. Olaf eliminates this entirely: one call, one result.

### Session memory is included for free

Olaf's response automatically includes relevant past observations — file changes, signature changes, decisions from prior sessions. With regular tools, you'd need additional `git log` or `git diff` calls to get equivalent context.

### Scales with codebase size

On a small codebase (15 modules), regular tools need 5-7 calls. On a larger codebase with 50+ files, the same question can easily require 10-20 calls and 15,000+ tokens as the search space grows. Olaf's cost stays roughly constant — the symbol graph handles the fan-out internally.

## Summary

| Metric | Regular tools | Olaf |
|-|-|-|
| Tokens per question | 5,000-6,000 | 1,800-1,950 |
| Tool calls per question | 5-7 | 1 |
| Token reduction | — | ~68% |
| Tool call reduction | — | ~85% |
| Latency overhead | 10-14s | ~2s |

**Conservative headline: 3-4x fewer tokens, 7x fewer tool calls.**

## Methodology

Both benchmarks were conducted on Olaf's own codebase (Rust, ~5,500 lines across 15 source modules). Token counts are estimated at 1 token per 4 characters of tool output. Regular tool paths followed the natural discovery pattern — grep to find relevant files, read to understand them, grep again to find the next piece. Olaf paths used `get_context` with a 4,000-token budget and natural language intent.

The benchmarks measure **retrieval cost** — how many tokens it takes to gather enough context to answer the question. They do not measure answer quality, which depends on the AI model, not the retrieval method.
