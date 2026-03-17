use std::collections::HashSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use ignore::WalkBuilder;
use rusqlite::Connection;

use crate::graph::store;
use crate::index::{diff, is_sensitive};
use crate::memory::staleness;
use crate::parser::{self, EdgeKind};

/// Summary statistics returned from a full index run.
pub struct IndexStats {
    pub files: usize,
    pub symbols: usize,
    pub edges: usize,
    pub centrality_computed: usize,
}

/// Walk `project_root`, parse all supported source files, and persist their
/// symbols and dependency edges to the SQLite database.
///
/// ## Exclusions (in priority order)
/// 1. `.gitignore` rules — enforced by the `ignore` crate
/// 2. Hard-coded directory skips: `.olaf/`, `target/`
/// 3. Sensitive file patterns — see `index::is_sensitive()` (Layer 1 security)
/// 4. Unsupported extensions — `detect_language()` returns `None`; file is
///    NOT entered in the `files` table, NOT read from disk
///
/// ## Error handling
/// - File read errors: `log::warn!` and continue (non-fatal)
/// - Parse errors: `log::warn!` and continue (non-fatal)
/// - DB errors: propagate via `anyhow::Result`
pub fn run(conn: &mut Connection, project_root: &Path) -> anyhow::Result<IndexStats> {
    let walker = WalkBuilder::new(project_root)
        .hidden(false) // include dot-files (we filter sensitive ourselves)
        .git_ignore(true) // respect .gitignore
        .git_global(true) // respect global gitignore
        .filter_entry(|entry| {
            // Prune .olaf/ and target/ entire subtrees
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
    // Raw (source_fqn, target_fqn, kind) collected during file walk
    let mut pending_edges: Vec<(String, String, String)> = Vec::new();
    let mut files_indexed: usize = 0;
    let mut symbols_indexed: usize = 0;

    for entry_result in walker {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                log::warn!("walk error: {}", e);
                continue;
            }
        };

        // Only process regular files
        let file_type = entry.file_type();
        if !file_type.is_some_and(|t| t.is_file()) {
            continue;
        }

        let abs_path = entry.path();

        // 1. Sensitive exclusion (Layer 1 security)
        if is_sensitive(abs_path) {
            log::debug!("sensitive file excluded: {}", abs_path.display());
            continue;
        }

        // 2. Build relative path string for DB storage and parser
        let relative_path = match abs_path.strip_prefix(project_root) {
            Ok(rel) => rel.to_string_lossy().into_owned(),
            Err(_) => {
                log::warn!("path outside project root — skipping: {}", abs_path.display());
                continue;
            }
        };

        // 3. Language detection — skip unsupported extensions without reading bytes
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

        // Mark as seen immediately after confirming the file is supported and non-sensitive.
        // Inserting BEFORE read/parse means a transient IO or parse error preserves the
        // existing DB row instead of letting stale-file cleanup delete it.
        seen_paths.insert(relative_path.clone());

        // 4. Read file bytes
        let bytes = match std::fs::read(abs_path) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("IO error reading {}: {} — skipping", relative_path, e);
                continue;
            }
        };

        // 5. Compute blake3 hash
        let hash = blake3::hash(&bytes).to_hex().to_string();

        // 6. Parse
        let (symbols, edges) = match parser::parse_file(&relative_path, &bytes) {
            Ok(result) => result,
            Err(e) => {
                log::warn!("parse error in {}: {} — skipping", relative_path, e);
                continue;
            }
        };

        // 7. Collect non-Imports edges (Imports edges cannot be stored with current schema — AC9)
        let file_edges: Vec<_> = edges
            .iter()
            .filter(|e| e.kind != EdgeKind::Imports)
            .map(|e| (e.source_fqn.clone(), e.target_fqn.clone(), e.kind.as_str().to_string()))
            .collect();
        pending_edges.extend(file_edges);

        // 8. Per-file transaction: upsert file + replace symbols + staleness detection
        let sym_count = symbols.len();
        let tx = conn.transaction()?;
        let file_id = store::upsert_file(&tx, &relative_path, &hash, lang_str, now_ts)?;
        // Load old symbol snapshots (with signatures) before replace so we can compute
        // structural diff with the same semantics as the incremental path.
        let old_syms = diff::load_file_symbols(&tx, file_id)?;
        store::replace_file_symbols(&tx, file_id, &symbols)?;
        let structural_diff = diff::compute(&relative_path, &old_syms, &symbols);
        staleness::mark_stale_for_structural_diff(&tx, &structural_diff)?;
        tx.commit()?;

        files_indexed += 1;
        symbols_indexed += sym_count;
    }

    // 9. Bulk edge insert: load FQN→ID and name→(ID,kind) maps, resolve all edges
    //
    // Two-pass resolution handles both parser output styles:
    //   - Full FQN (e.g., "src/file.ts::callee") → direct fqn_map lookup
    //   - Short name (e.g., "callee") → fallback name_map lookup with kind filter
    //
    // Fallback filtering: a `calls` edge must not resolve to a class symbol even if
    // a class named `foo` is the only `foo` in the project — that would be a false
    // positive (the reviewer's reproduced regression).  Each edge kind constrains
    // which symbol kinds are semantically valid targets.
    //
    // Unresolvable targets (external deps, stdlib, ambiguous names after filtering)
    // are skipped silently.
    let fqn_map = store::load_fqn_id_map(conn)?;
    let name_map = store::load_name_id_map(conn)?;
    let mut resolved: Vec<(i64, i64, String)> = Vec::with_capacity(pending_edges.len());
    for (src_fqn, tgt_fqn, kind) in &pending_edges {
        let Some(&src_id) = fqn_map.get(src_fqn) else {
            continue; // source not in project (edge from deleted/external symbol)
        };
        let tgt_id = fqn_map.get(tgt_fqn).copied().or_else(|| {
            // Fallback: short-name resolution for bare identifiers like "callee".
            // Filter candidates to semantically valid symbol kinds first, then
            // require exactly one match to avoid false edges.
            let valid_kinds: &[&str] = match kind.as_str() {
                "calls" => &["function", "method"],
                "uses_type" => &["class", "interface", "type_alias"],
                "extends" => &["class", "interface"],
                "implements" => &["interface"],
                "hooks_into" | "fires_hook" => &["function", "method"],
                "uses_trait" => &["class"],
                // "references" and unknown edge kinds: no fallback — too ambiguous
                _ => return None,
            };
            let candidates = name_map.get(tgt_fqn)?;
            let filtered: Vec<i64> = candidates
                .iter()
                .filter(|(_, sym_kind)| valid_kinds.contains(&sym_kind.as_str()))
                .map(|(id, _)| *id)
                .collect();
            // Resolve only when unambiguous after kind filtering
            if filtered.len() == 1 { Some(filtered[0]) } else { None }
        });
        let Some(tgt_id) = tgt_id else {
            continue; // unresolvable: external dep, stdlib, or ambiguous name
        };
        resolved.push((src_id, tgt_id, kind.clone()));
    }

    let edge_refs: Vec<(i64, i64, &str)> =
        resolved.iter().map(|(s, t, k)| (*s, *t, k.as_str())).collect();
    let tx = conn.transaction()?;
    let edges_inserted = store::insert_edges_bulk(&tx, &edge_refs)?;
    tx.commit()?;

    // 10. Stale file cleanup — guard on root accessibility, not file count.
    //
    // Using `project_root.is_dir()` instead of `candidates_seen > 0` handles two
    // edge cases that the counter-based guard missed:
    //   a. Truly empty project (no files at all) → candidates_seen = 0 but cleanup
    //      should still remove previously-indexed rows.
    //   b. Project with only unsupported/sensitive files → seen_paths is empty but
    //      cleanup should still wipe all stale rows.
    //
    // If the project root itself doesn't exist or isn't a directory, something is
    // wrong with the invocation — skip cleanup to preserve the prior index state.
    let mut centrality_count = 0;
    if project_root.is_dir() {
        let tx = conn.transaction()?;
        let doomed_fqns = store::collect_fqns_for_unseen_files(&tx, &seen_paths)?;
        staleness::mark_stale_for_removed_symbols(&tx, &doomed_fqns)?;
        let deleted = if seen_paths.is_empty() {
            // No supported files remain (project empty or only unsupported files).
            // Delete ALL previously indexed rows.
            tx.execute("DELETE FROM files", [])?
        } else {
            store::delete_unseen_files(&tx, &seen_paths)?
        };
        tx.commit()?;
        if deleted > 0 {
            log::info!(
                "removed {} stale file entries (files deleted/moved since last index)",
                deleted
            );
        }

        let centrality_start = std::time::Instant::now();
        centrality_count = store::compute_and_store_centrality(conn)?;
        let centrality_ms = centrality_start.elapsed().as_millis();
        log::info!(
            "computed PageRank centrality for {} symbols in {}ms",
            centrality_count,
            centrality_ms
        );
    } else {
        log::warn!(
            "project root {:?} is not accessible — skipping stale-file cleanup and centrality to preserve index",
            project_root
        );
    }

    Ok(IndexStats {
        files: files_indexed,
        symbols: symbols_indexed,
        edges: edges_inserted,
        centrality_computed: centrality_count,
    })
}
