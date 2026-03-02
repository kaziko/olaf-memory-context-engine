use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, Transaction, params};

use crate::parser::Symbol;

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
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
/// Empty `seen_paths` returns `Ok(0)` (delete nothing). The guard at the call
/// site (`candidates_seen > 0`) is where the "delete all" decision belongs.
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
}
