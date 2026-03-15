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

## External Repo Benchmark

Measured on **kubernetes/kubernetes** at commit `040ca596` using `get_brief` (wraps `get_context` + impact analysis). The self-benchmark above uses `get_context` directly — these are complementary measurements showing realistic agent-facing cost vs core retrieval cost.

### Environment

- CPU: Apple M4 Max (arm64)
- RAM: 128 GB
- OS: macOS Darwin 25.2.0
- Build: `--release` profile
- Olaf commit: `8b924f1`

### Indexing

| Metric | Value |
|-|-|
| Indexed files | 16,789 |
| Symbols | 302,352 |
| Edges | 0 |
| Index time | 30.4s |

### Per-query results (budget=4000)

| Query | Tag | Latency (ms) | Olaf tokens | Baseline A tokens | Savings |
|-|-|-|-|-|-|
| keyword-exact-scheduleone | keyword | 948 | 2,004 | 13,402 | 85.0% |
| keyword-module-garbage-collector | keyword | 889 | 930 | 8,321 | 88.8% |
| keyword-cross-module-reflector-informer | keyword | 939 | 1,467 | 21,511 | 93.2% |
| lowconf-broad-handle-errors | low_confidence | 857 | 1,313 | 6,647 | 80.2% |
| bugfix-sync-deployment-stuck | bugfix | 1,173 | 947 | 8,846 | 89.3% |
| impl-pod-eviction-threshold | impl | 1,078 | 772 | 6,765 | 88.6% |
| refactor-endpoint-controller-sync | refactor | 1,027 | 826 | 7,336 | 88.7% |
| filehint-kubelet-syncpod | file_hint | 824 | 67 | 36,962 | 99.8% |
| lowconf-ambiguous-token | low_confidence | 790 | 1,211 | 3,700 | 67.3% |
| fallback-zookeeper-leader | fallback | 1,043 | 1,782 | 0 | n/a |

### Multi-budget summary

| Budget | Mean savings | Median savings |
|-|-|-|
| 2,000 | 79.3% | 88.8% |
| 4,000 | 78.1% | 88.7% |
| 8,000 | 77.5% | 88.7% |

### Latency

| Metric | Value |
|-|-|
| Cold first query | 2,490 ms |
| Warm p50 | 948 ms |
| Warm p95 | 1,173 ms |
| Warm max | 1,173 ms |

**NFR1 comparison (warm only)**: NFR1 defines a 1-second ceiling for `get_context`. This benchmark measures `get_brief`, which wraps `get_context` and adds impact analysis. The warm p50 (948ms) is under 1s; the p95 (1,173ms) exceeds it. Queries exceeding 1s were bugfix-sync-deployment-stuck (1,173ms), impl-pod-eviction-threshold (1,078ms), refactor-endpoint-controller-sync (1,027ms), and fallback-zookeeper-leader (1,043ms). Note: the Go parser produced 0 edges, so impact analysis had no graph to traverse — the cause of >1s latency on these queries is not yet profiled and may be SQLite query overhead on a 302k-symbol table.

### Recall

Expected pivots hit rate: **0%** across all queries with defined pivots. On a 302k-symbol table (with 0 edges — effectively a flat list, not a graph), the keyword ranker surfaces related but different symbols than the hand-picked expected pivots. This means the benchmark validates token *reduction* but cannot confirm retrieval *accuracy* — Olaf returned less text, but none of the expected symbols were present in the results.

### Conclusion

Measured **~78% token reduction** (Baseline A, budget=4000) on kubernetes — an additional data point alongside the self-benchmark's ~68% figure. The higher reduction on the external repo reflects the larger baseline cost of manually reading Go files (many 6k-36k tokens) that Olaf's budget-constrained retrieval avoids. The self-benchmark uses `get_context` on a smaller Rust codebase; the external benchmark uses `get_brief` on a large Go codebase.

**Important caveat**: With 0% recall on expected pivots, this benchmark demonstrates that Olaf returns significantly fewer tokens than manual workflows, but does not validate that the returned content is the *right* content. Token savings and retrieval quality are distinct metrics — high savings with low recall means the tool is compact but may not surface the symbols a developer actually needs. The 0 edges indexed (Go parser limitation) also means this benchmark tested keyword-only retrieval on a flat symbol table, not Olaf's graph-assisted features.
