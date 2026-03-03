use rusqlite::{Connection, params};

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub(crate) fn upsert_session(
    conn: &Connection,
    session_id: &str,
    agent: &str,
) -> Result<(), StoreError> {
    conn.execute(
        "INSERT OR IGNORE INTO sessions (id, started_at, agent) VALUES (?1, ?2, ?3)",
        params![session_id, now_secs(), agent],
    )?;
    Ok(())
}

pub(crate) fn insert_observation(
    conn: &Connection,
    session_id: &str,
    kind: &str,
    content: &str,
    symbol_fqn: Option<&str>,
    file_path: Option<&str>,
) -> Result<i64, StoreError> {
    conn.execute(
        "INSERT INTO observations \
         (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0)",
        params![session_id, now_secs(), kind, content, symbol_fqn, file_path],
    )?;
    Ok(conn.last_insert_rowid())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_test_db() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open(&db_path).expect("open DB");
        (conn, dir)
    }

    #[test]
    fn test_upsert_session_creates_row() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "sess-1", "test-agent").unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions WHERE id = 'sess-1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_upsert_session_idempotent() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "sess-2", "test-agent").unwrap();
        upsert_session(&conn, "sess-2", "test-agent").unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions WHERE id = 'sess-2'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "INSERT OR IGNORE must not create duplicate rows");
    }

    #[test]
    fn test_insert_observation_with_symbol_fqn() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "sess-3", "test-agent").unwrap();
        let id = insert_observation(
            &conn,
            "sess-3",
            "insight",
            "Cache busting causes stale reads in query.rs",
            Some("src/query.rs::get_context"),
            None,
        )
        .unwrap();
        assert!(id > 0, "must return a positive row id");

        let (kind, symbol_fqn, file_path): (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT kind, symbol_fqn, file_path FROM observations WHERE id = ?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(kind, "insight");
        assert_eq!(symbol_fqn.as_deref(), Some("src/query.rs::get_context"));
        assert!(file_path.is_none());
    }

    #[test]
    fn test_insert_observation_file_path_only_has_null_symbol_fqn() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "sess-4", "test-agent").unwrap();
        let id = insert_observation(
            &conn,
            "sess-4",
            "decision",
            "Decided to skip caching for this file",
            None,
            Some("src/auth.rs"),
        )
        .unwrap();
        assert!(id > 0);

        let (symbol_fqn, file_path): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT symbol_fqn, file_path FROM observations WHERE id = ?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(symbol_fqn.is_none(), "symbol_fqn must be NULL when not provided");
        assert_eq!(file_path.as_deref(), Some("src/auth.rs"));
    }
}
