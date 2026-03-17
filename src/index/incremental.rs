use std::collections::HashSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use ignore::WalkBuilder;
use rusqlite::{Connection, OptionalExtension};

use crate::graph::store;
use crate::index::{diff, is_sensitive, IndexStats};
use crate::memory::staleness;
use crate::parser::{self, EdgeKind};

/// Reason a single-file reindex could not produce a structural diff.
pub enum SoftFailureReason {
    IoError,
    UnsupportedLanguage,
    ParseError,
}

/// Outcome of reindexing a single file.
pub enum ReindexOutcome {
    /// File hash matched stored hash — no changes.
    Unchanged,
    /// File was re-parsed; structural diff is available.
    Changed(diff::StructuralDiff),
    /// Could not produce a structural diff; hook should fall back to basic observation.
    SoftFailure(SoftFailureReason),
}

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
/// Runs an incremental index pass, re-parsing only files whose content hash has changed
/// since the last run. Does NOT rebuild the entire index from scratch; the `olaf index`
/// CLI command still calls `index::full::run()` for a full rebuild.
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
            Some(parser::Language::Go) => "go",
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
        let old_syms = diff::load_file_symbols(&tx, file_id)?;
        store::update_file_symbols(&tx, file_id, &symbols)?;
        let structural_diff = diff::compute(&relative_path, &old_syms, &symbols);
        staleness::mark_stale_for_structural_diff(&tx, &structural_diff)?;
        tx.commit()?;

        files_reindexed += 1;
        symbols_changed += sym_count;
    }

    // INSERT OR IGNORE handles idempotency for edges that already exist from
    // unchanged files — they are preserved, not duplicated.
    let edges_inserted = resolve_and_insert_file_edges(conn, &pending_edges)?;

    // Stale file cleanup — must match full.rs semantics exactly.
    // Guard on project_root.is_dir(), not on file count.
    if project_root.is_dir() {
        let tx = conn.transaction()?;
        let doomed_fqns = store::collect_fqns_for_unseen_files(&tx, &seen_paths)?;
        staleness::mark_stale_for_removed_symbols(&tx, &doomed_fqns)?;
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

    Ok(IndexStats { files: files_reindexed, symbols: symbols_changed, edges: edges_inserted, centrality_computed: 0 })
}

/// Re-index a single file and return a structural diff outcome.
///
/// Used by the PostToolUse hook to produce structural observations immediately after a file edit.
/// Resolves and inserts edges for this file using INSERT OR IGNORE (idempotent). Pre-existing
/// edges from the old version that no longer exist are NOT removed — consistent with the KNOWN
/// LIMITATION comment above. Use `olaf index` to restore full edge consistency.
pub fn reindex_single_file(
    conn: &mut Connection,
    project_root: &Path,
    rel_path: &str,
) -> anyhow::Result<ReindexOutcome> {
    let abs_path = project_root.join(rel_path);
    let bytes = match std::fs::read(&abs_path) {
        Ok(b) => b,
        Err(_) => return Ok(ReindexOutcome::SoftFailure(SoftFailureReason::IoError)),
    };

    let hash_hex = blake3::hash(&bytes).to_hex().to_string();

    // Check against stored hash — if unchanged, skip expensive parse
    let stored_hash: Option<String> = conn
        .query_row(
            "SELECT blake3_hash FROM files WHERE path = ?1",
            rusqlite::params![rel_path],
            |r| r.get(0),
        )
        .optional()?;
    if stored_hash.as_deref() == Some(hash_hex.as_str()) {
        return Ok(ReindexOutcome::Unchanged);
    }

    let lang_str = match parser::detect_language(rel_path) {
        Some(parser::Language::TypeScript | parser::Language::Tsx) => "typescript",
        Some(parser::Language::JavaScript | parser::Language::Jsx) => "javascript",
        Some(parser::Language::Python) => "python",
        Some(parser::Language::Rust) => "rust",
        Some(parser::Language::Php) => "php",
        Some(parser::Language::Go) => "go",
        None => return Ok(ReindexOutcome::SoftFailure(SoftFailureReason::UnsupportedLanguage)),
    };

    let (symbols, edges) = match parser::parse_file(rel_path, &bytes) {
        Ok(r) => r,
        Err(_) => return Ok(ReindexOutcome::SoftFailure(SoftFailureReason::ParseError)),
    };

    let now_ts =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let tx = conn.transaction()?;
    let file_id = store::upsert_file(&tx, rel_path, &hash_hex, lang_str, now_ts)?;
    let old_syms = diff::load_file_symbols(&tx, file_id)?;
    store::update_file_symbols(&tx, file_id, &symbols)?;
    let structural_diff = diff::compute(rel_path, &old_syms, &symbols);
    staleness::mark_stale_for_structural_diff(&tx, &structural_diff)?;
    tx.commit()?;

    // Resolve and insert edges for this file — must happen after the symbol transaction
    // so the FQN map includes this file's symbols.
    let file_edges: Vec<(String, String, String)> = edges
        .iter()
        .filter(|e| e.kind != EdgeKind::Imports)
        .map(|e| (e.source_fqn.clone(), e.target_fqn.clone(), e.kind.as_str().to_string()))
        .collect();
    resolve_and_insert_file_edges(conn, &file_edges)?;

    Ok(ReindexOutcome::Changed(structural_diff))
}

/// Two-pass edge resolution and insertion shared between `run()` and `reindex_single_file()`.
///
/// Pass 1: exact FQN map lookup.
/// Pass 2: short-name fallback with kind-specific symbol-kind filtering (unambiguous only).
/// INSERT OR IGNORE is used by `insert_edges_bulk` — idempotent for pre-existing edges.
///
/// Returns the number of edges inserted.
fn resolve_and_insert_file_edges(
    conn: &mut Connection,
    raw_edges: &[(String, String, String)],
) -> anyhow::Result<usize> {
    if raw_edges.is_empty() {
        return Ok(0);
    }
    let fqn_map = store::load_fqn_id_map(conn)?;
    let name_map = store::load_name_id_map(conn)?;
    let mut resolved: Vec<(i64, i64, String)> = Vec::with_capacity(raw_edges.len());
    for (src_fqn, tgt_fqn, kind) in raw_edges {
        let Some(&src_id) = fqn_map.get(src_fqn.as_str()) else { continue };
        let tgt_id = fqn_map.get(tgt_fqn.as_str()).copied().or_else(|| {
            let valid_kinds: &[&str] = match kind.as_str() {
                "calls" => &["function", "method"],
                "uses_type" => &["class", "interface", "type_alias"],
                "extends" => &["class", "interface"],
                "implements" => &["interface"],
                "hooks_into" | "fires_hook" => &["function", "method"],
                "uses_trait" => &["class"],
                _ => return None,
            };
            let candidates = name_map.get(tgt_fqn.as_str())?;
            let filtered: Vec<i64> = candidates
                .iter()
                .filter(|(_, sym_kind)| valid_kinds.contains(&sym_kind.as_str()))
                .map(|(id, _)| *id)
                .collect();
            if filtered.len() == 1 { Some(filtered[0]) } else { None }
        });
        let Some(tgt_id) = tgt_id else { continue };
        resolved.push((src_id, tgt_id, kind.clone()));
    }
    if resolved.is_empty() {
        return Ok(0);
    }
    let edge_refs: Vec<(i64, i64, &str)> =
        resolved.iter().map(|(s, t, k)| (*s, *t, k.as_str())).collect();
    let tx = conn.transaction()?;
    let count = store::insert_edges_bulk(&tx, &edge_refs)?;
    tx.commit()?;
    Ok(count)
}
