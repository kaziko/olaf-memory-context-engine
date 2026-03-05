use rusqlite::Transaction;

use crate::index::diff::StructuralDiff;

const CHUNK_SIZE: usize = 500;

/// Mark observations stale for symbols whose source_hash changed.
///
/// Filters to entries where `old_hash != new_hash`, then delegates to
/// `mark_stale_for_changed_fqns` for the actual batch UPDATE.
#[allow(dead_code)] // replaced by mark_stale_for_structural_diff in incremental.rs; kept for potential callers
pub(crate) fn mark_stale_for_changed_symbols(
    tx: &Transaction,
    changed_symbols: &[(String, String, String)],
) -> Result<(), rusqlite::Error> {
    let fqns: Vec<String> = changed_symbols
        .iter()
        .filter(|(_, old, new)| old != new)
        .map(|(fqn, _, _)| fqn.clone())
        .collect();
    mark_stale_for_changed_fqns(tx, &fqns)
}

/// Mark observations stale for the given FQNs (already filtered as actually changed).
///
/// Used by both the incremental path (via `mark_stale_for_changed_symbols`) and
/// the full re-index path (which computes its own FQN diff).
pub(crate) fn mark_stale_for_changed_fqns(
    tx: &Transaction,
    fqns: &[String],
) -> Result<(), rusqlite::Error> {
    if fqns.is_empty() {
        return Ok(());
    }
    let reason = "Symbol source changed since observation was recorded";
    batch_mark_stale(tx, fqns, reason)
}

/// Mark observations stale for symbols whose FQN no longer exists in the index.
pub(crate) fn mark_stale_for_removed_symbols(
    tx: &Transaction,
    removed_fqns: &[String],
) -> Result<(), rusqlite::Error> {
    if removed_fqns.is_empty() {
        return Ok(());
    }
    let reason = "Symbol no longer exists in index";
    batch_mark_stale(tx, removed_fqns, reason)
}

/// Mark observations stale based on a structural diff result.
///
/// - `signature_changed`: uses specific reason naming the changed symbol.
/// - `removed`: delegates to `mark_stale_for_removed_symbols` ("Symbol no longer exists in index").
/// - `body_only` and `added`: no staleness change.
pub(crate) fn mark_stale_for_structural_diff(
    tx: &Transaction,
    diff: &StructuralDiff,
) -> Result<(), rusqlite::Error> {
    for (fqn, _, _) in &diff.signature_changed {
        let reason = format!("Signature of symbol '{}' changed", short_name(fqn));
        batch_mark_stale(tx, std::slice::from_ref(fqn), &reason)?;
    }
    mark_stale_for_removed_symbols(tx, &diff.removed)?;
    Ok(())
}

fn short_name(fqn: &str) -> &str {
    fqn.rsplit("::").next().unwrap_or(fqn)
}

/// Shared batch UPDATE logic: mark observations stale in chunks of `CHUNK_SIZE`.
fn batch_mark_stale(
    tx: &Transaction,
    fqns: &[String],
    reason: &str,
) -> Result<(), rusqlite::Error> {
    for chunk in fqns.chunks(CHUNK_SIZE) {
        let placeholders: String = (0..chunk.len())
            .map(|i| format!("?{}", i + 2)) // ?1 is the reason
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "UPDATE observations SET is_stale = 1, stale_reason = ?1 \
             WHERE symbol_fqn IN ({}) AND is_stale = 0",
            placeholders
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() + 1);
        params.push(&reason);
        for fqn in chunk {
            params.push(fqn);
        }
        tx.execute(&sql, params.as_slice())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use rusqlite::params;
    use tempfile::tempdir;

    fn open_test_db() -> rusqlite::Connection {
        let dir = tempdir().unwrap();
        db::open(&dir.path().join("test.db")).unwrap()
    }

    fn insert_session(conn: &rusqlite::Connection, id: &str) {
        conn.execute(
            "INSERT INTO sessions (id, started_at) VALUES (?1, ?2)",
            params![id, 1000],
        )
        .unwrap();
    }

    fn insert_observation(
        conn: &rusqlite::Connection,
        session_id: &str,
        symbol_fqn: Option<&str>,
        file_path: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, file_path) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![session_id, 1000, "note", "test content", symbol_fqn, file_path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn get_stale_info(conn: &rusqlite::Connection, obs_id: i64) -> (bool, Option<String>) {
        conn.query_row(
            "SELECT is_stale, stale_reason FROM observations WHERE id = ?1",
            [obs_id],
            |r| Ok((r.get::<_, bool>(0)?, r.get::<_, Option<String>>(1)?)),
        )
        .unwrap()
    }

    #[test]
    fn changed_symbol_different_hashes_marks_stale() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("src/lib.rs::foo"), None);

        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_symbols(
            &tx,
            &[("src/lib.rs::foo".into(), "hash1".into(), "hash2".into())],
        )
        .unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(reason.unwrap(), "Symbol source changed since observation was recorded");
    }

    #[test]
    fn changed_symbol_same_hash_not_marked_stale() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("src/lib.rs::foo"), None);

        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_symbols(
            &tx,
            &[("src/lib.rs::foo".into(), "hash1".into(), "hash1".into())],
        )
        .unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(!stale);
        assert!(reason.is_none());
    }

    #[test]
    fn removed_symbol_marks_stale() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("src/lib.rs::bar"), None);

        let tx = conn.transaction().unwrap();
        mark_stale_for_removed_symbols(&tx, &["src/lib.rs::bar".into()]).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(reason.unwrap(), "Symbol no longer exists in index");
    }

    #[test]
    fn file_level_observation_not_marked_stale() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        // File-level observation: symbol_fqn = NULL
        let obs_id = insert_observation(&conn, "s1", None, Some("src/lib.rs"));

        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_symbols(
            &tx,
            &[("src/lib.rs::foo".into(), "h1".into(), "h2".into())],
        )
        .unwrap();
        mark_stale_for_removed_symbols(&tx, &["src/lib.rs::foo".into()]).unwrap();
        tx.commit().unwrap();

        let (stale, _) = get_stale_info(&conn, obs_id);
        assert!(!stale, "file-level observation must not be marked stale");
    }

    #[test]
    fn already_stale_observation_not_double_updated() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("src/lib.rs::foo"), None);

        // First: mark stale via removed
        let tx = conn.transaction().unwrap();
        mark_stale_for_removed_symbols(&tx, &["src/lib.rs::foo".into()]).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(reason.as_deref(), Some("Symbol no longer exists in index"));

        // Second: attempt to mark stale via changed — should not overwrite reason
        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_fqns(&tx, &["src/lib.rs::foo".into()]).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(
            reason.as_deref(),
            Some("Symbol no longer exists in index"),
            "original reason must be preserved"
        );
    }

    #[test]
    fn unrelated_fqn_not_affected() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("src/lib.rs::bar"), None);

        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_fqns(&tx, &["src/lib.rs::foo".into()]).unwrap();
        mark_stale_for_removed_symbols(&tx, &["src/lib.rs::baz".into()]).unwrap();
        tx.commit().unwrap();

        let (stale, _) = get_stale_info(&conn, obs_id);
        assert!(!stale, "observation linked to different FQN must not be affected");
    }

    #[test]
    fn batch_multiple_fqns() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs1 = insert_observation(&conn, "s1", Some("m::a"), None);
        let obs2 = insert_observation(&conn, "s1", Some("m::b"), None);
        let obs3 = insert_observation(&conn, "s1", Some("m::c"), None);

        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_fqns(
            &tx,
            &["m::a".into(), "m::b".into(), "m::c".into()],
        )
        .unwrap();
        tx.commit().unwrap();

        assert!(get_stale_info(&conn, obs1).0);
        assert!(get_stale_info(&conn, obs2).0);
        assert!(get_stale_info(&conn, obs3).0);
    }

    #[test]
    fn batch_exceeding_chunk_size() {
        let mut conn = open_test_db();
        insert_session(&conn, "s1");

        // Create 501 observations with unique FQNs
        let fqns: Vec<String> = (0..501).map(|i| format!("m::fn_{}", i)).collect();
        let obs_ids: Vec<i64> = fqns
            .iter()
            .map(|fqn| insert_observation(&conn, "s1", Some(fqn), None))
            .collect();

        let tx = conn.transaction().unwrap();
        mark_stale_for_changed_fqns(&tx, &fqns).unwrap();
        tx.commit().unwrap();

        // Verify all 501 observations are stale
        for obs_id in &obs_ids {
            let (stale, _) = get_stale_info(&conn, *obs_id);
            assert!(stale, "observation {} must be stale across chunk boundary", obs_id);
        }
    }

    #[test]
    fn structural_diff_signature_changed_uses_specific_reason() {
        use crate::index::diff::StructuralDiff;
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("f.rs::foo"), None);

        let diff = StructuralDiff {
            file_path: "f.rs".into(),
            added: vec![],
            removed: vec![],
            signature_changed: vec![("f.rs::foo".into(), "fn foo()".into(), "fn foo(x: i32)".into())],
            body_only: vec![],
        };

        let tx = conn.transaction().unwrap();
        mark_stale_for_structural_diff(&tx, &diff).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(reason.unwrap(), "Signature of symbol 'foo' changed");
    }

    #[test]
    fn structural_diff_body_only_does_not_mark_stale() {
        use crate::index::diff::StructuralDiff;
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("f.rs::foo"), None);

        let diff = StructuralDiff {
            file_path: "f.rs".into(),
            added: vec![],
            removed: vec![],
            signature_changed: vec![],
            body_only: vec!["f.rs::foo".into()],
        };

        let tx = conn.transaction().unwrap();
        mark_stale_for_structural_diff(&tx, &diff).unwrap();
        tx.commit().unwrap();

        let (stale, _) = get_stale_info(&conn, obs_id);
        assert!(!stale);
    }

    #[test]
    fn structural_diff_removed_uses_generic_reason() {
        use crate::index::diff::StructuralDiff;
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        let obs_id = insert_observation(&conn, "s1", Some("f.rs::bar"), None);

        let diff = StructuralDiff {
            file_path: "f.rs".into(),
            added: vec![],
            removed: vec!["f.rs::bar".into()],
            signature_changed: vec![],
            body_only: vec![],
        };

        let tx = conn.transaction().unwrap();
        mark_stale_for_structural_diff(&tx, &diff).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(reason.unwrap(), "Symbol no longer exists in index");
    }
}
