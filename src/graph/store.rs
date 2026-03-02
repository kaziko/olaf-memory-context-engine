use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, Transaction, params};

use crate::parser::Symbol;

/// Diff returned by `update_file_symbols()` describing which symbols changed.
///
/// `changed` contains `(symbol_id, old_source_hash, new_source_hash)` for each
/// symbol that was UPDATE'd in-place. Story 3.3 uses this to mark observations
/// stale only where `old_source_hash != new_source_hash`.
pub(crate) struct SymbolDiff {
    pub changed: Vec<(i64, String, String)>,
    pub added: usize,
    pub removed: usize,
}

/// Index statistics as stored in the DB — shared by `olaf status` (CLI)
/// and the `index_status` MCP tool (Story 2.3).
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
    for sym in symbols {
        tx.execute(
            "INSERT INTO symbols
             (file_id, fqn, name, kind, start_line, end_line, signature, docstring, source_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                file_id,
                sym.fqn,
                sym.name,
                sym.kind.as_str(),
                sym.start_line,
                sym.end_line,
                sym.signature,
                sym.docstring,
                sym.source_hash,
            ],
        )?;
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
/// Returns a `SymbolDiff` with `changed` = `(symbol_id, old_hash, new_hash)` tuples
/// for every symbol that was UPDATE'd (for Story 3.3 staleness detection).
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

    let mut diff = SymbolDiff { changed: Vec::new(), added: 0, removed: 0 };

    for sym in new_symbols {
        if let Some((existing_id, old_hash)) = current.remove(&sym.fqn) {
            // UPDATE in-place — preserves id, preserves FK edges pointing to this symbol
            tx.execute(
                "UPDATE symbols SET name=?1, kind=?2, start_line=?3, end_line=?4,
                 signature=?5, docstring=?6, source_hash=?7 WHERE id=?8",
                params![
                    sym.name,
                    sym.kind.as_str(),
                    sym.start_line,
                    sym.end_line,
                    sym.signature,
                    sym.docstring,
                    sym.source_hash,
                    existing_id
                ],
            )?;
            diff.changed.push((existing_id, old_hash, sym.source_hash.clone()));
        } else {
            // INSERT new symbol
            tx.execute(
                "INSERT INTO symbols
                 (file_id, fqn, name, kind, start_line, end_line, signature, docstring, source_hash)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    file_id,
                    sym.fqn,
                    sym.name,
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
    for (old_id, _) in current.values() {
        tx.execute("DELETE FROM symbols WHERE id = ?1", [old_id])?;
        diff.removed += 1;
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use tempfile::tempdir;

    fn open_test_db() -> rusqlite::Connection {
        let dir = tempdir().unwrap();
        db::open(&dir.path().join("index.db")).unwrap()
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
        assert_eq!(diff.changed[0].0, id_before, "changed entry must carry correct symbol_id");
        assert_eq!(diff.changed[0].1, "hash1", "old_hash must be recorded");
        assert_eq!(diff.changed[0].2, "hash2", "new_hash must be recorded");
        assert_eq!(diff.added, 0);
        assert_eq!(diff.removed, 0);
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
        };
        let tx = conn.transaction().unwrap();
        let diff = update_file_symbols(&tx, file_b, &[updated_sym]).unwrap();
        tx.commit().unwrap();

        assert_eq!(diff.changed.len(), 1, "helper must be in changed (UPDATE'd in-place)");
        assert_eq!(tgt_id, diff.changed[0].0, "symbol id must be preserved");

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
}
