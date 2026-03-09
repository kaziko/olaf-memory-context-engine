# Known Limitations

## Subagent Hook Behavior

**Investigated:** Story 10-7b (2026-03-09)

### What Works

- **Hook delivery**: Claude Code fires PreToolUse/PostToolUse hooks for tool calls made inside subagents (Agent tool). Olaf captures these observations normally.
- **Session attribution**: Subagents share the parent session's `session_id`. All observations (parent + subagent) group under the same session.
- **Non-worktree subagents**: Regular subagents (no `isolation: "worktree"`) share the parent's `cwd`. Observations and snapshots work identically to main-conversation edits.

### Worktree Isolation (Mitigated)

When a subagent runs with `isolation: "worktree"`, Claude Code sets the subagent's working directory to the worktree path. Since Olaf resolves `.olaf/index.db` from `payload.cwd`, this would cause observations to target a non-existent or orphaned database.

**Mitigation (v0.4.x+):** Olaf's hook handler (`observe.rs`) resolves worktree `cwd` to the main repository root via `resolve_worktree_root()` for DB access and snapshot storage. File paths are relativized against the raw worktree `cwd`, producing correct relative paths (e.g., `src/lib.rs`). Branch detection uses the worktree's own HEAD, preserving correct branch attribution. The `snapshot()` function reads file content from the worktree (where the file actually lives) but stores it in the main repo's `.olaf/restores/`.

The resolver distinguishes worktrees from submodules by checking for a `worktrees` path component in the gitdir — submodule `.git` files (which point to `.git/modules/`) are left untouched.

### Snapshot/Undo for Worktree Subagent Edits

For worktree-isolated subagent edits, pre-edit snapshots are read from the worktree and stored in the main repo's `.olaf/restores/`. The snapshot captures the file state before modification. However, after the worktree is cleaned up by Claude Code, the relative path in the restore point references a file that may have been merged back or discarded. `olaf undo` will attempt to restore to the main repo path, which is correct if the worktree changes were merged.

### SessionEnd Behavior

Subagents fire `SubagentStop` events, not `SessionEnd`. Olaf's `SessionEnd` hook handles session finalization (compression, etc.) and only fires when the main conversation session ends. This is correct behavior — subagent work is part of the parent session.
