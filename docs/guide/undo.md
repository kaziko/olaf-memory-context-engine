# Undoing AI Edits

Before every file change, Olaf automatically saves a snapshot. If Claude makes a mess, you can restore any file to exactly how it was.

## Undo the last edit to a file

```
Use undo_change to restore src/auth.rs to its previous state
```

## See all available snapshots first

```
Use list_restore_points for src/auth.rs
```

Claude will list the snapshots with timestamps, then you can pick one:

```
Restore src/auth.rs to snapshot 1741234567890-12345-3
```

Snapshots are created automatically — no git required, no manual setup.
