# Multi-Repo Workspaces

If you work across multiple repositories (e.g. backend + frontend + shared-types), Olaf can search for context across all of them at once.

## Set up a workspace

```sh
cd /path/to/main-repo
olaf workspace init
olaf workspace add ../frontend
olaf workspace add ../shared-types
```

This creates `.olaf/workspace.toml` listing the linked repos. When Claude calls `get_brief` or `get_context`, Olaf fans out pivot search across all workspace members and assembles a unified context brief.

## What spans repos

Pivot symbol search and context assembly — if the intent mentions "auth", Olaf finds `AuthService` in the backend and `AuthClient` in the frontend.

## What stays local

Impact analysis (`get_impact`), execution path tracing (`trace_flow`), session memory, observations, and graph traversal all operate within the local repo only. Cross-repo dependency resolution (e.g. tracing a call from frontend to backend API) requires edge stitching across databases, which is not yet supported.

## Auto-registration

When you run `olaf init` inside a repo that is a child of a workspace, it automatically registers itself in the parent workspace manifest.

## Diagnostics

Run `olaf workspace doctor` to check all members for path issues, missing databases, schema version mismatches, and index freshness.
