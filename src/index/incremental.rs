use std::collections::HashSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use ignore::WalkBuilder;
use rusqlite::Connection;

use crate::graph::store;
use crate::index::{is_sensitive, IndexStats};
use crate::memory::staleness;
use crate::parser::{self, EdgeKind};

// KNOWN LIMITATION: edges for relationships that no longer exist in changed files
// remain in the DB until a forced full re-index (`olaf index`). Only cross-file
// inbound edges to REMOVED symbols are cascade-deleted. This is acceptable:
// edges are traversal hints; use `olaf index` to restore full edge consistency.

/// Walk `project_root` and re-index only files whose blake3 hash has changed
/// since the last run (or that are new/deleted).
///
/// Returns `IndexStats` where `files` = number of files actually re-parsed
/// (unchanged files and deleted files are NOT counted).
///
/// This is a library function for the MCP query path (Story 2.2). It is NOT
/// called by the `olaf index` CLI command — that still calls `index::full::run()`.
pub fn run(conn: &mut Connection, project_root: &Path) -> anyhow::Result<IndexStats> {
    let stored = store::load_file_hash_map(conn)?;

    let walker = WalkBuilder::new(project_root)
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            !(entry.file_type().is_some_and(|t| t.is_dir())
                && (name == ".olaf" || name == "target"))
        })
        .build();

    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let mut seen_paths: HashSet<String> = HashSet::new();
    let mut pending_edges: Vec<(String, String, String)> = Vec::new();
    let mut files_reindexed: usize = 0;
    let mut symbols_changed: usize = 0;

    for entry_result in walker {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                log::warn!("walk error: {}", e);
                continue;
            }
        };

        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }

        let abs_path = entry.path();

        if is_sensitive(abs_path) {
            log::debug!("sensitive file excluded: {}", abs_path.display());
            continue;
        }

        // Match full.rs: strip_prefix with warn+skip on failure (NOT unwrap_or)
        let relative_path = match abs_path.strip_prefix(project_root) {
            Ok(rel) => rel.to_string_lossy().into_owned(),
            Err(_) => {
                log::warn!("path outside project root — skipping: {}", abs_path.display());
                continue;
            }
        };

        let lang_str = match parser::detect_language(&relative_path) {
            Some(parser::Language::TypeScript | parser::Language::Tsx) => "typescript",
            Some(parser::Language::JavaScript | parser::Language::Jsx) => "javascript",
            Some(parser::Language::Python) => "python",
            Some(parser::Language::Rust) => "rust",
            Some(parser::Language::Php) => "php",
            None => {
                log::debug!("unsupported extension, skipping: {}", relative_path);
                continue;
            }
        };

        // Insert into seen_paths AFTER confirming supported+non-sensitive, BEFORE reading bytes.
        // This protects against transient IO errors leaving stale rows (same placement as full.rs).
        seen_paths.insert(relative_path.clone());

        let bytes = match std::fs::read(abs_path) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("IO error reading {}: {} — skipping", relative_path, e);
                continue;
            }
        };

        let hash_hex = blake3::hash(&bytes).to_hex().to_string();

        // Skip unchanged files
        if let Some((_file_id, stored_hash)) = stored.get(&relative_path)
            && *stored_hash == hash_hex
        {
            log::debug!("unchanged: {}", relative_path);
            continue;
        }

        // Changed or new file — re-parse
        let (symbols, edges) = match parser::parse_file(&relative_path, &bytes) {
            Ok(r) => r,
            Err(e) => {
                log::warn!("parse error in {}: {} — skipping", relative_path, e);
                continue;
            }
        };

        let file_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.kind != EdgeKind::Imports)
            .map(|e| (e.source_fqn.clone(), e.target_fqn.clone(), e.kind.as_str().to_string()))
            .collect();
        pending_edges.extend(file_edges);

        let sym_count = symbols.len();
        let tx = conn.transaction()?;
        let file_id = store::upsert_file(&tx, &relative_path, &hash_hex, lang_str, now_ts)?;
        let diff = store::update_file_symbols(&tx, file_id, &symbols)?;
        staleness::mark_stale_for_changed_symbols(&tx, &diff.changed)?;
        tx.commit()?;

        files_reindexed += 1;
        symbols_changed += sym_count;
    }

    // Two-pass edge resolution — copied exactly from full.rs to avoid regressions.
    //
    // Pass 1: FQN map lookup (full FQN e.g. "src/file.ts::callee")
    // Pass 2: short-name fallback with kind-specific valid-symbol-kinds filtering.
    //
    // INSERT OR IGNORE handles idempotency for edges that already exist from
    // unchanged files — they are preserved, not duplicated.
    let fqn_map = store::load_fqn_id_map(conn)?;
    let name_map = store::load_name_id_map(conn)?;
    let mut resolved: Vec<(i64, i64, String)> = Vec::with_capacity(pending_edges.len());
    for (src_fqn, tgt_fqn, kind) in &pending_edges {
        let Some(&src_id) = fqn_map.get(src_fqn) else {
            continue;
        };
        let tgt_id = fqn_map.get(tgt_fqn).copied().or_else(|| {
            let valid_kinds: &[&str] = match kind.as_str() {
                "calls" => &["function", "method"],
                "uses_type" => &["class", "interface", "type_alias"],
                "extends" => &["class", "interface"],
                "implements" => &["interface"],
                "hooks_into" | "fires_hook" => &["function", "method"],
                "uses_trait" => &["class"],
                _ => return None,
            };
            let candidates = name_map.get(tgt_fqn)?;
            let filtered: Vec<i64> = candidates
                .iter()
                .filter(|(_, sym_kind)| valid_kinds.contains(&sym_kind.as_str()))
                .map(|(id, _)| *id)
                .collect();
            if filtered.len() == 1 { Some(filtered[0]) } else { None }
        });
        let Some(tgt_id) = tgt_id else {
            continue;
        };
        resolved.push((src_id, tgt_id, kind.clone()));
    }

    let edge_refs: Vec<(i64, i64, &str)> =
        resolved.iter().map(|(s, t, k)| (*s, *t, k.as_str())).collect();
    let tx = conn.transaction()?;
    let edges_inserted = store::insert_edges_bulk(&tx, &edge_refs)?;
    tx.commit()?;

    // Stale file cleanup — must match full.rs semantics exactly.
    // Guard on project_root.is_dir(), not on file count.
    if project_root.is_dir() {
        let tx = conn.transaction()?;
        let deleted = if seen_paths.is_empty() {
            tx.execute("DELETE FROM files", [])?
        } else {
            store::delete_unseen_files(&tx, &seen_paths)?
        };
        tx.commit()?;
        if deleted > 0 {
            log::info!("removed {} stale file entries", deleted);
        }
    } else {
        log::warn!(
            "project root {:?} is not accessible — skipping stale-file cleanup",
            project_root
        );
    }

    Ok(IndexStats { files: files_reindexed, symbols: symbols_changed, edges: edges_inserted })
}
