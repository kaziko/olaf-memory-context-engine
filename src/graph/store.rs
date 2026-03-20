use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, Transaction, params};

use crate::graph::query::expand_name_tokens;
use crate::parser::Symbol;

/// Diff returned by `update_file_symbols()` describing which symbols changed.
///
/// `changed` contains `(fqn, old_source_hash, new_source_hash)` for each
/// symbol that was UPDATE'd in-place. Staleness detection uses the FQN to
/// mark linked observations stale where `old_source_hash != new_source_hash`.
///
/// `removed` contains FQNs of symbols no longer present after re-parse.
pub(crate) struct SymbolDiff {
    pub changed: Vec<(String, String, String)>,
    pub added: usize,
    pub removed: Vec<String>,
}

/// Index statistics as stored in the DB — shared by `olaf status` (CLI)
/// and the `index_status` MCP tool.
pub struct DbStats {
    pub files: i64,
    pub symbols: i64,
    pub edges: i64,
    pub observations: i64,
    /// Unix timestamp of most recently indexed file; `None` if no files indexed.
    pub last_indexed_at: Option<i64>,
}

pub fn load_db_stats(conn: &Connection) -> Result<DbStats, StoreError> {
    let (files, symbols, edges, observations, last_indexed_at) = conn.query_row(
        "SELECT \
            (SELECT COUNT(*) FROM files), \
            (SELECT COUNT(*) FROM symbols), \
            (SELECT COUNT(*) FROM edges), \
            (SELECT COUNT(*) FROM observations), \
            (SELECT MAX(last_indexed_at) FROM files)",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )?;
    Ok(DbStats { files, symbols, edges, observations, last_indexed_at })
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// Insert or update a file row, preserving `id` on conflict.
///
/// Uses `ON CONFLICT DO UPDATE` — never `INSERT OR REPLACE` — to ensure
/// `file_id` stability (REPLACE = DELETE + INSERT churns the PK and cascades
/// deletion of all symbols/edges for that file).
///
/// Returns the stable `file_id` via a follow-up SELECT (because
/// `last_insert_rowid()` returns 0 on the UPDATE path).
pub(crate) fn upsert_file(
    tx: &Transaction,
    relative_path: &str,
    hash: &str,
    language: &str,
    now_ts: i64,
) -> Result<i64, StoreError> {
    tx.execute(
        "INSERT INTO files (path, blake3_hash, language, last_indexed_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(path) DO UPDATE SET
           blake3_hash = excluded.blake3_hash,
           language = excluded.language,
           last_indexed_at = excluded.last_indexed_at",
        params![relative_path, hash, language, now_ts],
    )?;
    let file_id: i64 = tx.query_row(
        "SELECT id FROM files WHERE path = ?1",
        params![relative_path],
        |r| r.get(0),
    )?;
    Ok(file_id)
}

/// Replace all symbols for a file: DELETE existing rows, then bulk INSERT new ones.
///
/// Caller owns the transaction — this function never calls `conn.transaction()`.
pub(crate) fn replace_file_symbols(
    tx: &Transaction,
    file_id: i64,
    symbols: &[Symbol],
) -> Result<(), StoreError> {
    tx.execute("DELETE FROM symbols WHERE file_id = ?1", params![file_id])?;

    // Deduplicate by FQN — keep first occurrence. This handles cases where
    // the parser emits duplicate FQNs from the same file (e.g., Rust files
    // with multiple impl blocks for the same type producing identical
    // path::Type::method FQNs for different trait implementations).
    let mut seen_fqns = HashSet::new();
    for sym in symbols {
        if !seen_fqns.insert(&sym.fqn) {
            log::debug!(
                "skipping duplicate FQN within file_id {file_id}: {}",
                sym.fqn,
            );
            continue;
        }
        let name_tokens = expand_name_tokens(&sym.name);
        tx.execute(
            "INSERT INTO symbols
             (file_id, fqn, name, name_tokens, kind, start_line, end_line, signature, docstring, source_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                file_id,
                sym.fqn,
                sym.name,
                name_tokens,
                sym.kind.as_str(),
                sym.start_line,
                sym.end_line,
                sym.signature,
                sym.docstring,
                sym.source_hash,
            ],
        )?;
    }

    // Second pass: resolve parent_fqn → parent_id for child symbols
    resolve_parent_ids(tx, file_id, symbols)?;

    Ok(())
}

/// Resolve parent_fqn to parent_id for child symbols within a file.
/// Symbols with parent_fqn set get their parent_id updated by looking up
/// the parent FQN among symbols in the same file.
fn resolve_parent_ids(
    tx: &Transaction,
    file_id: i64,
    symbols: &[Symbol],
) -> Result<(), StoreError> {
    let children: Vec<&Symbol> = symbols.iter().filter(|s| s.parent_fqn.is_some()).collect();
    if children.is_empty() {
        return Ok(());
    }

    let mut update_stmt =
        tx.prepare("UPDATE symbols SET parent_id = ?1 WHERE file_id = ?2 AND fqn = ?3")?;
    let mut lookup_stmt =
        tx.prepare("SELECT id FROM symbols WHERE file_id = ?1 AND fqn = ?2")?;

    for child in children {
        let parent_fqn = child.parent_fqn.as_ref().unwrap();
        let parent_id: Option<i64> = match lookup_stmt
            .query_row(params![file_id, parent_fqn], |r| r.get(0))
        {
            Ok(id) => Some(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(StoreError::Sqlite(e)),
        };
        if let Some(pid) = parent_id {
            update_stmt.execute(params![pid, file_id, child.fqn])?;
        } else {
            log::debug!(
                "parent_fqn '{}' not found for child '{}' in file_id {}",
                parent_fqn,
                child.fqn,
                file_id,
            );
        }
    }
    Ok(())
}

/// Update symbols for a file using stable-identity semantics.
///
/// - Symbols whose FQN persists across re-parse are UPDATE'd in-place, preserving
///   their SQLite `id` and all inbound FK edges (edges from other files pointing
///   to these symbols are NOT cascade-deleted).
/// - Symbols with a new FQN are INSERT'd.
/// - Symbols whose FQN is no longer present are DELETE'd (cascade-cleans their edges).
///
/// Returns a `SymbolDiff` with `changed` = `(fqn, old_hash, new_hash)` tuples
/// for every symbol that was UPDATE'd, and `removed` = FQNs of deleted symbols.
///
/// Caller owns the transaction — this function never calls `conn.transaction()`.
pub(crate) fn update_file_symbols(
    tx: &Transaction,
    file_id: i64,
    new_symbols: &[Symbol],
) -> Result<SymbolDiff, StoreError> {
    // Load current symbols for this file: fqn → (id, source_hash)
    let mut current: HashMap<String, (i64, String)> = {
        let mut stmt =
            tx.prepare("SELECT id, fqn, source_hash FROM symbols WHERE file_id = ?1")?;
        stmt.query_map([file_id], |row| {
            Ok((
                row.get::<_, String>(1)?,
                (row.get::<_, i64>(0)?, row.get::<_, String>(2)?),
            ))
        })?
        .collect::<Result<_, _>>()?
    };

    let mut diff = SymbolDiff { changed: Vec::new(), added: 0, removed: Vec::new() };

    // Deduplicate by FQN — keep first occurrence (same pattern as replace_file_symbols)
    let mut seen_fqns = HashSet::new();
    for sym in new_symbols {
        if !seen_fqns.insert(&sym.fqn) {
            log::debug!(
                "skipping duplicate FQN in update_file_symbols for file_id {file_id}: {}",
                sym.fqn,
            );
            continue;
        }
        let name_tokens = expand_name_tokens(&sym.name);
        if let Some((existing_id, old_hash)) = current.remove(&sym.fqn) {
            // UPDATE in-place — preserves id, preserves FK edges pointing to this symbol
            tx.execute(
                "UPDATE symbols SET name=?1, kind=?2, start_line=?3, end_line=?4,
                 signature=?5, docstring=?6, source_hash=?7, name_tokens=?8 WHERE id=?9",
                params![
                    sym.name,
                    sym.kind.as_str(),
                    sym.start_line,
                    sym.end_line,
                    sym.signature,
                    sym.docstring,
                    sym.source_hash,
                    name_tokens,
                    existing_id
                ],
            )?;
            diff.changed.push((sym.fqn.clone(), old_hash, sym.source_hash.clone()));
        } else {
            // INSERT new symbol
            tx.execute(
                "INSERT INTO symbols
                 (file_id, fqn, name, name_tokens, kind, start_line, end_line, signature, docstring, source_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    file_id,
                    sym.fqn,
                    sym.name,
                    name_tokens,
                    sym.kind.as_str(),
                    sym.start_line,
                    sym.end_line,
                    sym.signature,
                    sym.docstring,
                    sym.source_hash
                ],
            )?;
            diff.added += 1;
        }
    }

    // DELETE symbols whose FQN is no longer present (cascade-deletes their outbound edges)
    for (fqn, (old_id, _)) in &current {
        tx.execute("DELETE FROM symbols WHERE id = ?1", [old_id])?;
        diff.removed.push(fqn.clone());
    }

    // Second pass: resolve parent_fqn → parent_id for child symbols
    resolve_parent_ids(tx, file_id, new_symbols)?;

    Ok(diff)
}

/// Load `path → (file_id, blake3_hash)` map for all indexed files.
///
/// Used at the start of an incremental index run to detect which files have
/// changed (hash mismatch) or are new (path not in map) since the last run.
pub(crate) fn load_file_hash_map(
    conn: &Connection,
) -> Result<HashMap<String, (i64, String)>, StoreError> {
    let mut stmt = conn.prepare("SELECT id, path, blake3_hash FROM files")?;
    let map = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                (row.get::<_, i64>(0)?, row.get::<_, String>(2)?),
            ))
        })?
        .collect::<Result<HashMap<_, _>, _>>()?;
    Ok(map)
}

/// Bulk insert (source_id, target_id, kind) edge tuples using INSERT OR IGNORE.
///
/// `INSERT OR IGNORE` respects the UNIQUE(source_id, target_id, kind) constraint
/// without changing the `id` (unlike INSERT OR REPLACE). Returns the number of
/// newly inserted edges.
///
/// Caller owns the transaction — this function never calls `conn.transaction()`.
pub(crate) fn insert_edges_bulk(
    tx: &Transaction,
    resolved: &[(i64, i64, &str)],
) -> Result<usize, StoreError> {
    let mut inserted = 0usize;
    let mut stmt = tx.prepare(
        "INSERT OR IGNORE INTO edges (source_id, target_id, kind) VALUES (?1, ?2, ?3)",
    )?;
    for (src_id, tgt_id, kind) in resolved {
        inserted += stmt.execute(params![src_id, tgt_id, kind])?;
    }
    Ok(inserted)
}

/// Result of an `insert_lsp_edges` batch operation.
pub(crate) struct InsertResult {
    pub inserted: usize,
    pub already_existed: usize,
}

/// Bulk insert LSP-originated edges with `source_origin = 'lsp'`.
///
/// Uses `INSERT OR IGNORE` against the `UNIQUE(source_id, target_id, kind)` constraint.
/// Caller owns the transaction.
pub(crate) fn insert_lsp_edges(
    tx: &Transaction,
    edges: &[(i64, i64, &str)],
) -> Result<InsertResult, StoreError> {
    let mut inserted = 0usize;
    let mut stmt = tx.prepare(
        "INSERT OR IGNORE INTO edges (source_id, target_id, kind, source_origin) VALUES (?1, ?2, ?3, 'lsp')",
    )?;
    for (src_id, tgt_id, kind) in edges {
        inserted += stmt.execute(params![src_id, tgt_id, kind])?;
    }
    let already_existed = edges.len() - inserted;
    Ok(InsertResult { inserted, already_existed })
}

/// Batch-resolve FQN strings to symbol IDs using chunked `WHERE fqn IN (...)` queries.
///
/// Returns a map of FQN → symbol ID for all FQNs that matched. Missing FQNs are
/// absent from the map. Uses chunk size 100 to stay under SQLite's variable limit.
pub(crate) fn resolve_fqns_to_ids(
    conn: &Connection,
    fqns: &[&str],
) -> Result<HashMap<String, i64>, StoreError> {
    let mut result = HashMap::new();
    for chunk in fqns.chunks(100) {
        let placeholders: String = chunk
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT fqn, id FROM symbols WHERE fqn IN ({})", placeholders);
        let params: Vec<&dyn rusqlite::ToSql> =
            chunk.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params.as_slice(), |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (fqn, id) = row?;
            result.insert(fqn, id);
        }
    }
    Ok(result)
}

/// Load the full FQN → symbol ID map in a single SELECT.
///
/// Used for bulk edge resolution after the file walk completes, replacing
/// per-edge N+1 queries with a single map lookup.
pub(crate) fn load_fqn_id_map(conn: &Connection) -> Result<HashMap<String, i64>, StoreError> {
    let mut stmt = conn.prepare("SELECT fqn, id FROM symbols")?;
    let map = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<Result<HashMap<_, _>, _>>()?;
    Ok(map)
}

/// Load the short-name → `(symbol_id, symbol_kind)` map in a single SELECT.
///
/// Used as a fallback in edge resolution when `target_fqn` is a bare identifier
/// (e.g., `"callee"`) rather than a full FQN (e.g., `"src/file.ts::callee"`).
/// Parsers emit short names for Calls, UsesType, and other edges.
///
/// The `kind` column is included so callers can filter by semantically valid symbol
/// kinds before resolving — e.g., `calls` edges must not resolve to a class symbol
/// even if a class named `foo` happens to be the only `foo` in the project.
///
/// A name resolves unambiguously only when exactly one kind-filtered candidate
/// remains. Multiple matches are left unresolved to avoid false edges.
pub(crate) fn load_name_id_map(
    conn: &Connection,
) -> Result<HashMap<String, Vec<(i64, String)>>, StoreError> {
    let mut stmt = conn.prepare("SELECT name, id, kind FROM symbols")?;
    let mut map: HashMap<String, Vec<(i64, String)>> = HashMap::new();
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?))
    })?;
    for row in rows {
        let (name, id, kind) = row?;
        map.entry(name).or_default().push((id, kind));
    }
    Ok(map)
}

/// Collect FQNs for all symbols belonging to files NOT in `seen_paths`.
///
/// Used before `delete_unseen_files` to mark observations stale for symbols
/// that are about to be cascade-deleted. When `seen_paths` is empty, returns
/// ALL symbol FQNs (all files are about to be deleted).
pub(crate) fn collect_fqns_for_unseen_files(
    tx: &Transaction,
    seen_paths: &HashSet<String>,
) -> Result<Vec<String>, StoreError> {
    if seen_paths.is_empty() {
        let mut stmt = tx.prepare("SELECT fqn FROM symbols")?;
        let fqns = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(fqns);
    }
    let placeholders = seen_paths
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT s.fqn FROM symbols s JOIN files f ON s.file_id = f.id \
         WHERE f.path NOT IN ({})",
        placeholders
    );
    let params: Vec<&dyn rusqlite::ToSql> =
        seen_paths.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let mut stmt = tx.prepare(&sql)?;
    let fqns = stmt
        .query_map(params.as_slice(), |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(fqns)
}

/// Look up the tightest-enclosing symbol FQN for a given file path and line number.
///
/// Returns the FQN of the narrowest symbol whose `start_line <= line <= end_line`.
/// When multiple symbols span the same line (e.g., a method inside a class), the
/// narrowest one (smallest line span) is returned via `ORDER BY (end_line - start_line) ASC`.
///
/// Returns `Ok(None)` when:
/// - The file is not in the index
/// - No symbol spans the given line
///
/// Resolves a file-path + line-number to the FQN of the symbol whose definition
/// spans that line. Returns None if the file is not indexed or no symbol spans the line.
pub fn lookup_symbol_at_line(
    conn: &Connection,
    rel_path: &str,
    line: u32,
) -> Result<Option<String>, StoreError> {
    // story-15-2: remove parent_id filter to optionally return children
    let mut stmt = conn.prepare(
        "SELECT s.fqn FROM symbols s \
         JOIN files f ON s.file_id = f.id \
         WHERE f.path = ?1 AND s.start_line <= ?2 AND s.end_line >= ?2 \
         AND s.parent_id IS NULL \
         ORDER BY (s.end_line - s.start_line) ASC \
         LIMIT 1",
    )?;
    match stmt.query_row(params![rel_path, line], |r| r.get::<_, String>(0)) {
        Ok(fqn) => Ok(Some(fqn)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(StoreError::Sqlite(e)),
    }
}

/// Delete all `files` rows whose `path` is NOT in `seen_paths`.
///
/// The `ON DELETE CASCADE` on `symbols(file_id)` and `edges(source_id/target_id)`
/// cleans dependent rows automatically — no manual cleanup needed.
///
/// Empty `seen_paths` returns `Ok(0)` (delete nothing). The call site is
/// responsible for the "delete all" decision when `seen_paths` is empty —
/// guarded by `project_root.is_dir()` in both `full.rs` and `incremental.rs`.
///
/// Caller owns the transaction — this function never calls `conn.transaction()`.
pub(crate) fn delete_unseen_files(
    tx: &Transaction,
    seen_paths: &HashSet<String>,
) -> Result<usize, StoreError> {
    if seen_paths.is_empty() {
        return Ok(0);
    }
    let placeholders = seen_paths
        .iter()
        .enumerate()
        .map(|(i, _)| format!("?{}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM files WHERE path NOT IN ({})", placeholders);
    let params: Vec<&dyn rusqlite::ToSql> =
        seen_paths.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    Ok(tx.execute(&sql, params.as_slice())?)
}

const PAGERANK_DAMPING: f64 = 0.85;
const PAGERANK_MAX_ITER: usize = 100;

/// Compute PageRank centrality on the static dependency graph and store scores
/// in the `centrality` column of the `symbols` table.
///
/// Returns the number of symbols whose centrality was updated.
/// If there are no edges, resets all centrality to 0.0 and returns 0.
pub fn compute_and_store_centrality(conn: &Connection) -> anyhow::Result<usize> {
    use petgraph::algo::page_rank;
    use petgraph::graph::{DiGraph, NodeIndex};

    // 1. Load all symbol IDs
    let mut stmt = conn.prepare("SELECT id FROM symbols")?;
    let symbol_ids: Vec<i64> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    drop(stmt);

    if symbol_ids.is_empty() {
        return Ok(0);
    }

    // 2. Load static-only edges
    let mut stmt = conn.prepare(
        "SELECT source_id, target_id FROM edges WHERE source_origin = 'static'",
    )?;
    let edges: Vec<(i64, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;
    drop(stmt);

    // 3. No edges → reset all centrality to 0.0
    if edges.is_empty() {
        conn.execute("UPDATE symbols SET centrality = 0.0 WHERE centrality != 0.0", [])?;
        return Ok(0);
    }

    // 4. Build petgraph DiGraph
    let mut graph = DiGraph::<i64, ()>::new();
    let mut node_map: HashMap<i64, NodeIndex> = HashMap::with_capacity(symbol_ids.len());
    for &id in &symbol_ids {
        let idx = graph.add_node(id);
        node_map.insert(id, idx);
    }
    for (src, tgt) in &edges {
        if let (Some(&s), Some(&t)) = (node_map.get(src), node_map.get(tgt)) {
            graph.add_edge(s, t, ());
        }
    }

    // 5. Compute PageRank
    let scores = page_rank(&graph, PAGERANK_DAMPING, PAGERANK_MAX_ITER);

    // 6. Batch UPDATE in a transaction (RAII Transaction auto-rolls-back on error)
    {
        let tx = conn.unchecked_transaction()?;
        let mut update = tx.prepare("UPDATE symbols SET centrality = ?1 WHERE id = ?2")?;
        for (&id, &idx) in &node_map {
            let score = scores[idx.index()];
            update.execute(params![score, id])?;
        }
        drop(update);
        tx.commit()?;
    }

    Ok(symbol_ids.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use tempfile::tempdir;

    fn open_test_db() -> rusqlite::Connection {
        let dir = tempdir().unwrap();
        db::open(&dir.path().join("index.db")).unwrap()
    }

    // ─── Story 4.1 Unit Tests ────────────────────────────────────────────────────

    fn insert_test_symbol(conn: &mut rusqlite::Connection, file_path: &str, fqn: &str, name: &str, start: u32, end: u32) {
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, file_path, "hash", "rust", 1000).unwrap();
        tx.execute(
            "INSERT OR IGNORE INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) \
             VALUES (?1, ?2, ?3, 'function', ?4, ?5, 'h')",
            params![file_id, fqn, name, start, end],
        ).unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn test_lookup_symbol_at_line_within_range() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/lib.rs", "src/lib.rs::my_fn", "my_fn", 5, 15);

        let fqn = lookup_symbol_at_line(&conn, "src/lib.rs", 10).unwrap();
        assert_eq!(fqn.as_deref(), Some("src/lib.rs::my_fn"));
    }

    #[test]
    fn test_lookup_symbol_at_line_outside_range_returns_none() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/lib.rs", "src/lib.rs::my_fn", "my_fn", 5, 15);

        let fqn = lookup_symbol_at_line(&conn, "src/lib.rs", 20).unwrap();
        assert!(fqn.is_none(), "line outside symbol range must return None");
    }

    #[test]
    fn test_lookup_symbol_at_line_nested_returns_narrowest() {
        let mut conn = open_test_db();
        // class spans 1-50, method spans 10-20 → method is narrower
        {
            let tx = conn.transaction().unwrap();
            let file_id = upsert_file(&tx, "src/a.rs", "h", "rust", 1000).unwrap();
            tx.execute(
                "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) \
                 VALUES (?1, 'src/a.rs::MyClass', 'MyClass', 'class', 1, 50, 'h')",
                params![file_id],
            ).unwrap();
            tx.execute(
                "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) \
                 VALUES (?1, 'src/a.rs::MyClass::method', 'method', 'function', 10, 20, 'h')",
                params![file_id],
            ).unwrap();
            tx.commit().unwrap();
        }

        let fqn = lookup_symbol_at_line(&conn, "src/a.rs", 15).unwrap();
        assert_eq!(fqn.as_deref(), Some("src/a.rs::MyClass::method"), "narrowest symbol must be returned");
    }

    #[test]
    fn test_lookup_symbol_at_line_file_not_indexed_returns_none() {
        let conn = open_test_db();
        let fqn = lookup_symbol_at_line(&conn, "nonexistent.rs", 1).unwrap();
        assert!(fqn.is_none(), "unindexed file must return None");
    }

    #[test]
    fn test_upsert_file_returns_id() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let id = upsert_file(&tx, "src/main.rs", "abc123", "rust", 1000).unwrap();
        tx.commit().unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_upsert_file_stable_id_on_conflict() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let id1 = upsert_file(&tx, "src/main.rs", "abc123", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        let id2 = upsert_file(&tx, "src/main.rs", "def456", "rust", 2000).unwrap();
        tx.commit().unwrap();

        assert_eq!(id1, id2, "upsert must preserve file_id on conflict");
    }

    #[test]
    fn test_replace_file_symbols_no_duplication() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/lib.rs", "aaa", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let make_test_sym = || crate::parser::symbols::Symbol {
            fqn: "src/lib.rs::foo".to_string(),
            name: "foo".to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 5,
            signature: None,
            docstring: None,
            source_hash: "deadbeef".to_string(),
            parent_fqn: None,
        };

        // First replace
        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &[make_test_sym()]).unwrap();
        tx.commit().unwrap();

        // Second replace (simulate re-index)
        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &[make_test_sym()]).unwrap();
        tx.commit().unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE file_id = ?1",
                params![file_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "replace_file_symbols must not duplicate symbols");
    }

    #[test]
    fn test_replace_file_symbols_dedup_fqn() {
        // Regression test: Rust files with multiple impl blocks for the same type
        // produce duplicate FQNs (e.g., `impl Foo { fn method() }` and
        // `impl Trait for Foo { fn method() }` both yield `path::Foo::method`).
        // replace_file_symbols must handle this without crashing.
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/lib.rs", "aaa", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let sym_a = crate::parser::symbols::Symbol {
            fqn: "src/lib.rs::Foo::method".to_string(),
            name: "method".to_string(),
            kind: crate::parser::SymbolKind::Method,
            start_line: 10,
            end_line: 15,
            signature: Some("fn method(&self)".to_string()),
            docstring: None,
            source_hash: "aaa".to_string(),
            parent_fqn: None,
        };
        let sym_b = crate::parser::symbols::Symbol {
            fqn: "src/lib.rs::Foo::method".to_string(), // same FQN, different impl block
            name: "method".to_string(),
            kind: crate::parser::SymbolKind::Method,
            start_line: 20,
            end_line: 25,
            signature: Some("fn method(&self) -> bool".to_string()),
            docstring: None,
            source_hash: "bbb".to_string(),
            parent_fqn: None,
        };

        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &[sym_a, sym_b]).unwrap();
        tx.commit().unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM symbols WHERE fqn = 'src/lib.rs::Foo::method'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "duplicate FQN must be deduplicated to one row");

        // Verify the first occurrence was kept (start_line=10, not 20)
        let start_line: u32 = conn
            .query_row(
                "SELECT start_line FROM symbols WHERE fqn = 'src/lib.rs::Foo::method'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(start_line, 10, "first occurrence must be kept");
    }

    #[test]
    fn test_insert_edges_bulk_deduplication() {
        let mut conn = open_test_db();
        // Set up two symbols
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/a.rs", "aaa", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let make_sym = |fqn: &str, name: &str| crate::parser::symbols::Symbol {
            fqn: fqn.to_string(),
            name: name.to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 2,
            signature: None,
            docstring: None,
            source_hash: "hash".to_string(),
            parent_fqn: None,
        };

        let tx = conn.transaction().unwrap();
        replace_file_symbols(
            &tx,
            file_id,
            &[make_sym("src/a.rs::foo", "foo"), make_sym("src/a.rs::bar", "bar")],
        )
        .unwrap();
        tx.commit().unwrap();

        let fqn_map = load_fqn_id_map(&conn).unwrap();
        let src = fqn_map["src/a.rs::foo"];
        let tgt = fqn_map["src/a.rs::bar"];

        // Insert same edge twice
        let tx = conn.transaction().unwrap();
        insert_edges_bulk(&tx, &[(src, tgt, "calls"), (src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "duplicate edge must be ignored");
    }

    #[test]
    fn test_delete_unseen_files_cascades() {
        let mut conn = open_test_db();
        // Insert a file and a symbol
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/gone.rs", "aaa", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let sym = crate::parser::symbols::Symbol {
            fqn: "src/gone.rs::fn1".to_string(),
            name: "fn1".to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 3,
            signature: None,
            docstring: None,
            source_hash: "hash".to_string(),
            parent_fqn: None,
        };
        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &[sym]).unwrap();
        tx.commit().unwrap();

        // Delete with empty seen_paths — should delete nothing (guard behaviour)
        let tx = conn.transaction().unwrap();
        let deleted = delete_unseen_files(&tx, &HashSet::new()).unwrap();
        tx.commit().unwrap();
        assert_eq!(deleted, 0, "empty set must delete nothing");

        // Delete with seen_paths that does NOT include "src/gone.rs"
        let seen: HashSet<String> = HashSet::from(["src/other.rs".to_string()]);
        let tx = conn.transaction().unwrap();
        let deleted = delete_unseen_files(&tx, &seen).unwrap();
        tx.commit().unwrap();
        assert_eq!(deleted, 1, "gone.rs must be deleted");

        let sym_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM symbols", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sym_count, 0, "cascade must delete symbols when file is deleted");
    }

    #[test]
    fn test_update_file_symbols_preserves_id() {
        let mut conn = open_test_db();

        let make_sym = |fqn: &str, hash: &str| crate::parser::symbols::Symbol {
            fqn: fqn.to_string(),
            name: "foo".to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 5,
            signature: None,
            docstring: None,
            source_hash: hash.to_string(),
            parent_fqn: None,
        };

        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/lib.rs", "aaa", "rust", 1000).unwrap();
        tx.commit().unwrap();

        // First: use replace_file_symbols to establish initial symbol
        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &[make_sym("src/lib.rs::foo", "hash1")]).unwrap();
        tx.commit().unwrap();

        let id_before: i64 = conn
            .query_row("SELECT id FROM symbols WHERE fqn = 'src/lib.rs::foo'", [], |r| r.get(0))
            .unwrap();

        // Now update with same FQN — should UPDATE in-place, preserving id
        let tx = conn.transaction().unwrap();
        let diff =
            update_file_symbols(&tx, file_id, &[make_sym("src/lib.rs::foo", "hash2")]).unwrap();
        tx.commit().unwrap();

        let id_after: i64 = conn
            .query_row("SELECT id FROM symbols WHERE fqn = 'src/lib.rs::foo'", [], |r| r.get(0))
            .unwrap();

        assert_eq!(id_before, id_after, "update_file_symbols must preserve symbol id for same FQN");
        assert_eq!(diff.changed.len(), 1, "one symbol should be in changed list");
        assert_eq!(diff.changed[0].0, "src/lib.rs::foo", "changed entry must carry correct FQN");
        assert_eq!(diff.changed[0].1, "hash1", "old_hash must be recorded");
        assert_eq!(diff.changed[0].2, "hash2", "new_hash must be recorded");
        assert_eq!(diff.added, 0);
        assert_eq!(diff.removed.len(), 0);
    }

    #[test]
    fn test_update_file_symbols_preserves_inbound_edges() {
        let mut conn = open_test_db();

        let make_sym = |_file_id: i64, fqn: &str, name: &str| crate::parser::symbols::Symbol {
            fqn: fqn.to_string(),
            name: name.to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 3,
            signature: None,
            docstring: None,
            source_hash: "h1".to_string(),
            parent_fqn: None,
        };

        // File A (caller), File B (target with helper())
        let tx = conn.transaction().unwrap();
        let file_a = upsert_file(&tx, "src/a.rs", "aaa", "rust", 1000).unwrap();
        let file_b = upsert_file(&tx, "src/b.rs", "bbb", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_a, &[make_sym(file_a, "src/a.rs::caller", "caller")])
            .unwrap();
        replace_file_symbols(&tx, file_b, &[make_sym(file_b, "src/b.rs::helper", "helper")])
            .unwrap();
        tx.commit().unwrap();

        // Create edge: A's caller → B's helper
        let fqn_map = load_fqn_id_map(&conn).unwrap();
        let src_id = fqn_map["src/a.rs::caller"];
        let tgt_id = fqn_map["src/b.rs::helper"];
        let tx = conn.transaction().unwrap();
        insert_edges_bulk(&tx, &[(src_id, tgt_id, "calls")]).unwrap();
        tx.commit().unwrap();

        let edge_count_before: i64 =
            conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
        assert_eq!(edge_count_before, 1, "should have one A→B edge before update");

        // Update B's symbol (same FQN, body changed) — inbound edge from A must survive
        let updated_sym = crate::parser::symbols::Symbol {
            fqn: "src/b.rs::helper".to_string(),
            name: "helper".to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 4, // body changed
            signature: None,
            docstring: None,
            source_hash: "h2".to_string(),
            parent_fqn: None,
        };
        let tx = conn.transaction().unwrap();
        let diff = update_file_symbols(&tx, file_b, &[updated_sym]).unwrap();
        tx.commit().unwrap();

        assert_eq!(diff.changed.len(), 1, "helper must be in changed (UPDATE'd in-place)");
        assert_eq!("src/b.rs::helper", diff.changed[0].0, "FQN must be recorded");

        let edge_count_after: i64 =
            conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
        assert_eq!(
            edge_count_after, 1,
            "inbound edge from A to B's helper must survive update_file_symbols (stable-identity)"
        );
    }

    #[test]
    fn test_load_file_hash_map() {
        let mut conn = open_test_db();

        let tx = conn.transaction().unwrap();
        upsert_file(&tx, "src/main.rs", "hash_abc", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let map = load_file_hash_map(&conn).unwrap();
        assert!(map.contains_key("src/main.rs"), "map must contain the inserted file path");
        let (id, hash) = &map["src/main.rs"];
        assert!(*id > 0, "file_id must be positive");
        assert_eq!(hash, "hash_abc", "stored hash must match");
    }

    #[test]
    fn test_load_fqn_id_map_returns_all_symbols() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/x.rs", "hash", "rust", 100).unwrap();
        tx.commit().unwrap();

        let syms: Vec<_> = ["fn_a", "fn_b", "fn_c"]
            .iter()
            .map(|n| crate::parser::symbols::Symbol {
                fqn: format!("src/x.rs::{}", n),
                name: n.to_string(),
                kind: crate::parser::SymbolKind::Function,
                start_line: 1,
                end_line: 2,
                signature: None,
                docstring: None,
                source_hash: "h".to_string(),
                parent_fqn: None,
            })
            .collect();

        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &syms).unwrap();
        tx.commit().unwrap();

        let map = load_fqn_id_map(&conn).unwrap();
        assert_eq!(map.len(), 3);
        assert!(map.contains_key("src/x.rs::fn_a"));
        assert!(map.contains_key("src/x.rs::fn_b"));
        assert!(map.contains_key("src/x.rs::fn_c"));
    }

    #[test]
    fn test_load_db_stats_empty_db() {
        let conn = open_test_db();
        let stats = load_db_stats(&conn).unwrap();
        assert_eq!(stats.files, 0);
        assert_eq!(stats.symbols, 0);
        assert_eq!(stats.edges, 0);
        assert_eq!(stats.observations, 0);
        assert!(stats.last_indexed_at.is_none());
    }

    #[test]
    fn test_load_db_stats_after_index() {
        let dir = tempdir().unwrap();
        // Write a Rust source file so the indexer has something to parse
        std::fs::write(dir.path().join("lib.rs"), "pub fn hello() {}").unwrap();

        let mut conn = db::open(&dir.path().join("index.db")).unwrap();
        crate::index::run(&mut conn, dir.path()).unwrap();

        let stats = load_db_stats(&conn).unwrap();
        assert!(stats.files > 0, "at least one file must be indexed");
        assert!(stats.symbols > 0, "at least one symbol must be indexed");
        assert!(stats.last_indexed_at.is_some(), "last_indexed_at must be set after indexing");
    }

    // ─── Story 9.6 Unit Tests ────────────────────────────────────────────────────

    fn setup_two_symbols(conn: &mut rusqlite::Connection) -> (i64, i64) {
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/a.rs", "aaa", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let make_sym = |fqn: &str, name: &str| crate::parser::symbols::Symbol {
            fqn: fqn.to_string(),
            name: name.to_string(),
            kind: crate::parser::SymbolKind::Function,
            start_line: 1,
            end_line: 2,
            signature: None,
            docstring: None,
            source_hash: "hash".to_string(),
            parent_fqn: None,
        };

        let tx = conn.transaction().unwrap();
        replace_file_symbols(
            &tx,
            file_id,
            &[make_sym("src/a.rs::foo", "foo"), make_sym("src/a.rs::bar", "bar")],
        ).unwrap();
        tx.commit().unwrap();

        let fqn_map = load_fqn_id_map(conn).unwrap();
        (fqn_map["src/a.rs::foo"], fqn_map["src/a.rs::bar"])
    }

    #[test]
    fn test_insert_lsp_edges_basic() {
        let mut conn = open_test_db();
        let (src, tgt) = setup_two_symbols(&mut conn);

        let tx = conn.transaction().unwrap();
        let result = insert_lsp_edges(&tx, &[(src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        assert_eq!(result.inserted, 1);
        assert_eq!(result.already_existed, 0);

        let origin: String = conn.query_row(
            "SELECT source_origin FROM edges WHERE source_id = ?1 AND target_id = ?2",
            params![src, tgt],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(origin, "lsp");
    }

    #[test]
    fn test_insert_lsp_edges_dedup_with_static() {
        let mut conn = open_test_db();
        let (src, tgt) = setup_two_symbols(&mut conn);

        // Insert static edge first
        let tx = conn.transaction().unwrap();
        insert_edges_bulk(&tx, &[(src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        // Insert same triple as LSP edge — should be ignored
        let tx = conn.transaction().unwrap();
        let result = insert_lsp_edges(&tx, &[(src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        assert_eq!(result.inserted, 0);
        assert_eq!(result.already_existed, 1);

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1, "only one edge row should exist");

        let origin: String = conn.query_row(
            "SELECT source_origin FROM edges WHERE source_id = ?1 AND target_id = ?2",
            params![src, tgt],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(origin, "static", "first writer (static) must win");
    }

    #[test]
    fn test_insert_lsp_edges_dedup_lsp_first() {
        let mut conn = open_test_db();
        let (src, tgt) = setup_two_symbols(&mut conn);

        // Insert LSP edge first
        let tx = conn.transaction().unwrap();
        insert_lsp_edges(&tx, &[(src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        // Insert same triple as static edge — should be ignored
        let tx = conn.transaction().unwrap();
        insert_edges_bulk(&tx, &[(src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 1, "only one edge row should exist");

        let origin: String = conn.query_row(
            "SELECT source_origin FROM edges WHERE source_id = ?1 AND target_id = ?2",
            params![src, tgt],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(origin, "lsp", "first writer (lsp) must win");
    }

    #[test]
    fn test_lsp_edges_cascade_on_symbol_delete() {
        let mut conn = open_test_db();
        let (src, tgt) = setup_two_symbols(&mut conn);

        // Insert LSP edge
        let tx = conn.transaction().unwrap();
        insert_lsp_edges(&tx, &[(src, tgt, "calls")]).unwrap();
        tx.commit().unwrap();

        let edge_count: i64 = conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
        assert_eq!(edge_count, 1);

        // Get the file_id for our symbols
        let file_id: i64 = conn.query_row(
            "SELECT file_id FROM symbols WHERE id = ?1", params![src], |r| r.get(0),
        ).unwrap();

        // replace_file_symbols with empty vec — deletes all symbols, cascade deletes edges
        let tx = conn.transaction().unwrap();
        replace_file_symbols(&tx, file_id, &[]).unwrap();
        tx.commit().unwrap();

        let edge_count: i64 = conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
        assert_eq!(edge_count, 0, "LSP edges must be cascade-deleted when symbols are removed");
    }

    #[test]
    fn test_resolve_fqns_to_ids() {
        let mut conn = open_test_db();
        setup_two_symbols(&mut conn);

        let result = resolve_fqns_to_ids(&conn, &["src/a.rs::foo", "src/a.rs::bar", "nonexistent::sym"]).unwrap();
        assert_eq!(result.len(), 2, "only existing FQNs should resolve");
        assert!(result.contains_key("src/a.rs::foo"));
        assert!(result.contains_key("src/a.rs::bar"));
        assert!(!result.contains_key("nonexistent::sym"));
    }

    #[test]
    fn test_update_file_symbols_dedup_fqn() {
        use crate::parser::{SymbolKind, Symbol};

        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let file_id = upsert_file(&tx, "src/dup.rs", "hash1", "rust", 1000).unwrap();

        let symbols = vec![
            Symbol {
                fqn: "src/dup.rs::Foo::fmt".into(),
                name: "fmt".into(),
                kind: SymbolKind::Method,
                start_line: 1,
                end_line: 5,
                signature: None,
                docstring: None,
                source_hash: "h1".into(),
                parent_fqn: None,
            },
            Symbol {
                fqn: "src/dup.rs::Foo::fmt".into(),
                name: "fmt".into(),
                kind: SymbolKind::Method,
                start_line: 10,
                end_line: 15,
                signature: None,
                docstring: None,
                source_hash: "h2".into(),
                parent_fqn: None,
            },
        ];

        let diff = update_file_symbols(&tx, file_id, &symbols).unwrap();
        assert_eq!(diff.added, 1, "only first occurrence should be inserted");

        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM symbols WHERE file_id = ?1",
            [file_id],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 1, "exactly one symbol should exist after dedup");

        // Verify first-wins: persisted symbol should have first symbol's data
        let (start, hash): (u32, String) = tx.query_row(
            "SELECT start_line, source_hash FROM symbols WHERE file_id = ?1",
            [file_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).unwrap();
        assert_eq!(start, 1, "first-wins: should keep first symbol's start_line");
        assert_eq!(hash, "h1", "first-wins: should keep first symbol's source_hash");
    }

    // ─── PageRank centrality tests ───────────────────────────────────────────────

    /// Helper: insert a symbol and return its DB id.
    fn insert_symbol_returning_id(conn: &Connection, file_id: i64, fqn: &str, name: &str) -> i64 {
        conn.execute(
            "INSERT OR IGNORE INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) \
             VALUES (?1, ?2, ?3, 'function', 1, 10, 'h')",
            params![file_id, fqn, name],
        ).unwrap();
        conn.query_row("SELECT id FROM symbols WHERE fqn = ?1", [fqn], |r| r.get(0)).unwrap()
    }

    /// Helper: insert a static edge between two symbol IDs.
    fn insert_static_edge(conn: &Connection, src: i64, tgt: i64, kind: &str) {
        conn.execute(
            "INSERT OR IGNORE INTO edges (source_id, target_id, kind, source_origin) VALUES (?1, ?2, ?3, 'static')",
            params![src, tgt, kind],
        ).unwrap();
    }

    #[test]
    fn test_pagerank_known_graph() {
        // Graph: A→B, A→C, B→C, C→D
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let fid = upsert_file(&tx, "src/pr.rs", "h", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let a = insert_symbol_returning_id(&conn, fid, "src/pr.rs::A", "A");
        let b = insert_symbol_returning_id(&conn, fid, "src/pr.rs::B", "B");
        let c = insert_symbol_returning_id(&conn, fid, "src/pr.rs::C", "C");
        let d = insert_symbol_returning_id(&conn, fid, "src/pr.rs::D", "D");

        insert_static_edge(&conn, a, b, "calls");
        insert_static_edge(&conn, a, c, "calls");
        insert_static_edge(&conn, b, c, "calls");
        insert_static_edge(&conn, c, d, "calls");

        let count = compute_and_store_centrality(&conn).unwrap();
        assert_eq!(count, 4);

        let get_centrality = |fqn: &str| -> f64 {
            conn.query_row("SELECT centrality FROM symbols WHERE fqn = ?1", [fqn], |r| r.get(0)).unwrap()
        };

        let ca = get_centrality("src/pr.rs::A");
        let cb = get_centrality("src/pr.rs::B");
        let cc = get_centrality("src/pr.rs::C");
        let cd = get_centrality("src/pr.rs::D");

        // Frozen expected values from petgraph 0.7.1 page_rank(d=0.85, 100 iters)
        // on graph A→B, A→C, B→C, C→D
        let eps = 0.001;
        assert!((ca - 0.1175).abs() < eps, "A expected ~0.1175, got {ca}");
        assert!((cb - 0.1674).abs() < eps, "B expected ~0.1674, got {cb}");
        assert!((cc - 0.3163).abs() < eps, "C expected ~0.3163, got {cc}");
        assert!((cd - 0.3989).abs() < eps, "D expected ~0.3989, got {cd}");

        // Structural invariants
        assert!(cc > ca, "C (two incoming) must have higher centrality than A (zero incoming)");
        assert!(ca > 0.0, "all nodes must have non-zero centrality in a connected graph");
        assert!(cb > 0.0);
        assert!(cc > 0.0);
        assert!(cd > 0.0);
    }

    #[test]
    fn test_pagerank_empty_graph_no_edges() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let fid = upsert_file(&tx, "src/empty.rs", "h", "rust", 1000).unwrap();
        tx.commit().unwrap();

        insert_symbol_returning_id(&conn, fid, "src/empty.rs::A", "A");
        insert_symbol_returning_id(&conn, fid, "src/empty.rs::B", "B");

        let count = compute_and_store_centrality(&conn).unwrap();
        assert_eq!(count, 0, "no edges → return 0");

        let ca: f64 = conn.query_row(
            "SELECT centrality FROM symbols WHERE fqn = 'src/empty.rs::A'", [], |r| r.get(0),
        ).unwrap();
        assert!((ca - 0.0).abs() < f64::EPSILON, "no edges → centrality must be 0.0");
    }

    #[test]
    fn test_pagerank_single_symbol_no_edges() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let fid = upsert_file(&tx, "src/single.rs", "h", "rust", 1000).unwrap();
        tx.commit().unwrap();

        insert_symbol_returning_id(&conn, fid, "src/single.rs::Alone", "Alone");

        let count = compute_and_store_centrality(&conn).unwrap();
        assert_eq!(count, 0);

        let c: f64 = conn.query_row(
            "SELECT centrality FROM symbols WHERE fqn = 'src/single.rs::Alone'", [], |r| r.get(0),
        ).unwrap();
        assert!((c - 0.0).abs() < f64::EPSILON, "single symbol with no edges → centrality 0.0");
    }

    #[test]
    fn test_pagerank_stale_scores_reset_when_edges_removed() {
        let mut conn = open_test_db();
        let tx = conn.transaction().unwrap();
        let fid = upsert_file(&tx, "src/stale.rs", "h", "rust", 1000).unwrap();
        tx.commit().unwrap();

        let a = insert_symbol_returning_id(&conn, fid, "src/stale.rs::A", "A");
        let b = insert_symbol_returning_id(&conn, fid, "src/stale.rs::B", "B");

        // First: compute with edges → non-zero centrality
        insert_static_edge(&conn, a, b, "calls");
        let count = compute_and_store_centrality(&conn).unwrap();
        assert_eq!(count, 2);

        let ca: f64 = conn.query_row(
            "SELECT centrality FROM symbols WHERE fqn = 'src/stale.rs::A'", [], |r| r.get(0),
        ).unwrap();
        assert!(ca > 0.0, "with edges, centrality must be non-zero");

        // Now remove edges and recompute → centrality must reset to 0.0
        conn.execute("DELETE FROM edges", []).unwrap();
        let count2 = compute_and_store_centrality(&conn).unwrap();
        assert_eq!(count2, 0, "no edges → return 0");

        let ca2: f64 = conn.query_row(
            "SELECT centrality FROM symbols WHERE fqn = 'src/stale.rs::A'", [], |r| r.get(0),
        ).unwrap();
        assert!((ca2 - 0.0).abs() < f64::EPSILON, "stale centrality must be reset to 0.0");
    }
}
