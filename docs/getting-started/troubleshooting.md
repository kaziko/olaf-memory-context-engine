# Verify & Troubleshoot

## Verify It's Working

Run `olaf status` to see a health report:

```
Files indexed:  142
Symbols:        1,847
Edges:          3,204
Observations:   12
Last indexed:   2026-03-04 10:00:00 UTC

MCP config:         /path/to/project/.mcp.json
MCP status:         registered
Hook PostToolUse:   installed
Hook PreToolUse:    installed
Hook SessionEnd:    installed
```

A healthy installation shows:

- `MCP status: registered`
- All three hooks: `installed`
- Non-zero file and symbol counts

In Claude Code, ask Claude to call `get_context` for a specific task. If it returns a context brief with symbols and file paths, Olaf is working correctly.

## Troubleshooting

### MCP server not connecting

Run `olaf status`. If `MCP status: not registered`, the `.mcp.json` file is missing or misconfigured. Fix: re-run `olaf init` from your project root (it is idempotent — safe to run multiple times).

### Hooks missing

Run `olaf status`. If any hook shows `missing` (e.g., `Hook PostToolUse: missing`), the hook configuration in `.claude/settings.local.json` was not written or was removed. Fix: re-run `olaf init`.

### Index is empty (Files indexed: 0)

The index was not built or was cleared. Fix: run `olaf index` manually from your project root, then `olaf status` to confirm the counts.

### Index not initialized

If `olaf status` prints `Index not initialized. Run olaf index to build the index.`, run `olaf init` first to set up the full environment, then `olaf index` if the index still doesn't build automatically.
