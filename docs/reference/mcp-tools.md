# MCP Tools

Once connected, Claude can use these tools:

## Context retrieval

| Tool | Description |
|-|-|
| `get_brief` | Start here. Context brief for any task; includes impact analysis when `symbol_fqn` is provided. Use `get_context` or `get_impact` for fine-grained control. Accepts optional `branch` param (`"all"` for cross-branch). |
| `get_context` | Token-budgeted context brief for a task; triggers incremental re-index. Accepts optional `branch` param (`"all"` for cross-branch). |
| `get_impact` | Find symbols that call, extend, implement, or use a given symbol FQN as a type |
| `get_file_skeleton` | Signatures, docstrings, and edges for a file (no implementation bodies) |
| `analyze_failure` | Parse a stack trace or error output and return a context brief focused on the failure path |

## Session memory

| Tool | Description |
|-|-|
| `save_observation` | Record a decision, insight, or error linked to a symbol FQN or file path |
| `get_session_history` | Observations from recent sessions, filterable by file or symbol; supports relevance-ranked retrieval. Pass `branch: "all"` to include observations from all branches. |

## Code navigation & status

| Tool | Description |
|-|-|
| `trace_flow` | Trace execution paths between two symbols through the dependency graph |
| `index_status` | File count, symbol count, edge count, observation count, last indexed timestamp |

## Safety

| Tool | Description |
|-|-|
| `list_restore_points` | Pre-edit snapshots for a file, sorted newest-first |
| `undo_change` | Restore a file to a specific snapshot; records a decision observation |

## LSP integration

| Tool | Description |
|-|-|
| `submit_lsp_edges` | Inject type-resolved edges from a language server (interface implementations, dynamic dispatch, generics) into the graph for richer `get_impact` and `trace_flow` results |

## Multi-repo workspace

- `get_brief` and `get_context` automatically fan out across linked repos when `.olaf/workspace.toml` exists
- Impact analysis, memory, and graph traversal remain local per-repo
