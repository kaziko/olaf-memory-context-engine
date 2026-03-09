# Tool Preference Rules

`olaf init` writes a rules file at `.claude/rules/olaf-tools.md` that teaches Claude when to use Olaf MCP tools instead of native file reads. The rules include:

- **Tool routing table** — maps common tasks (explore codebase, analyze dependencies, diagnose errors) to the appropriate Olaf tool
- **Freshness guidance** — which tools auto-reindex after edits and which require a manual reindex
- **When to use native tools** — editing, running commands, reading a single known file, or narrow keyword searches are still best done with Edit/Write, Bash, Read, and Grep

## Drift detection

The rules file includes a content hash in a marker comment. On each `olaf init`, Olaf compares the hash to detect drift — if the template has changed (e.g., after an Olaf upgrade), the file is regenerated.

`olaf status` reports the rules file state:

| Status | Meaning |
|-|-|
| `current` | File matches the expected template |
| `outdated` | Hash mismatch — template has changed since the file was written |
| `missing` | File does not exist |
| `malformed` | File exists but has no valid hash marker |

## Customization

You can edit the file freely. If you want to restore the default content, delete it and re-run `olaf init`.
