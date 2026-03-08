use rusqlite::Connection;

pub const DEFAULT_COMPRESSION_THRESHOLD_SECS: i64 = 3600;
pub const DEFAULT_PURGE_THRESHOLD_SECS: i64 = 90 * 86400; // 7_776_000

pub fn run_compression(conn: &mut Connection, threshold_secs: i64) -> anyhow::Result<()> {
    let purged = super::store::purge_old_sessions(conn, DEFAULT_PURGE_THRESHOLD_SECS)?;
    if purged > 0 {
        log::info!("purged {} session(s) older than 90 days", purged);
    }
    let compressed = super::store::compress_stale_sessions(conn, threshold_secs)?;
    if !compressed.is_empty() {
        log::info!(
            "compressed {} stale session(s): {}",
            compressed.len(),
            compressed.join(", ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn open_test_db() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open(&db_path).expect("open DB");
        (conn, dir)
    }

    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    fn create_ended_session(conn: &Connection, id: &str, ended_at: i64) {
        conn.execute(
            "INSERT INTO sessions (id, started_at, ended_at, agent) VALUES (?1, ?2, ?3, 'test')",
            params![id, ended_at - 7200, ended_at],
        )
        .unwrap();
    }

    fn insert_obs(conn: &Connection, session_id: &str, kind: &str, content: &str) {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content) VALUES (?1, ?2, ?3, ?4)",
            params![session_id, now_secs(), kind, content],
        )
        .unwrap();
    }

    fn count_obs(conn: &Connection, session_id: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1",
            params![session_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn count_obs_by_kind(conn: &Connection, session_id: &str, kind: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1 AND kind = ?2",
            params![session_id, kind],
            |r| r.get(0),
        )
        .unwrap()
    }

    // Task 8.1: mixed kinds → only tool_call and file_change deleted
    #[test]
    fn test_compress_session_retains_valuable_deletes_ephemeral() {
        let (mut conn, _dir) = open_test_db();
        let ended = now_secs() - 7200; // 2 hours ago
        create_ended_session(&conn, "s1", ended);
        insert_obs(&conn, "s1", "tool_call", "read file");
        insert_obs(&conn, "s1", "file_change", "modified foo.rs");
        insert_obs(&conn, "s1", "insight", "important finding");
        insert_obs(&conn, "s1", "decision", "chose approach A");
        insert_obs(&conn, "s1", "error", "parse failed");
        insert_obs(&conn, "s1", "anti_pattern", "bad pattern found");

        assert_eq!(count_obs(&conn, "s1"), 6);

        let tx = conn.transaction().unwrap();
        let deleted = super::super::store::compress_session(&tx, "s1").unwrap();
        tx.commit().unwrap();

        assert_eq!(deleted, 2); // tool_call + file_change
        assert_eq!(count_obs(&conn, "s1"), 4); // insight + decision + error + anti_pattern
        assert_eq!(count_obs_by_kind(&conn, "s1", "tool_call"), 0);
        assert_eq!(count_obs_by_kind(&conn, "s1", "file_change"), 0);
        assert_eq!(count_obs_by_kind(&conn, "s1", "insight"), 1);
        assert_eq!(count_obs_by_kind(&conn, "s1", "decision"), 1);
        assert_eq!(count_obs_by_kind(&conn, "s1", "error"), 1);
        assert_eq!(count_obs_by_kind(&conn, "s1", "anti_pattern"), 1);

        // Session marked compressed
        let compressed: i64 = conn
            .query_row("SELECT compressed FROM sessions WHERE id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(compressed, 1);
    }

    // Task 8.2: already-compressed session → no-op
    #[test]
    fn test_compress_already_compressed_is_noop() {
        let (mut conn, _dir) = open_test_db();
        let ended = now_secs() - 7200;
        create_ended_session(&conn, "s1", ended);
        insert_obs(&conn, "s1", "insight", "kept");

        // First compression
        let tx = conn.transaction().unwrap();
        super::super::store::compress_session(&tx, "s1").unwrap();
        tx.commit().unwrap();

        // Second compression — should be no-op
        let tx = conn.transaction().unwrap();
        let deleted = super::super::store::compress_session(&tx, "s1").unwrap();
        tx.commit().unwrap();

        assert_eq!(deleted, 0);
        assert_eq!(count_obs(&conn, "s1"), 1); // insight still there
    }

    // Task 8.3: active session (ended_at IS NULL) → NOT compressed
    #[test]
    fn test_active_session_not_compressed() {
        let (mut conn, _dir) = open_test_db();
        // Active session: ended_at is NULL
        conn.execute(
            "INSERT INTO sessions (id, started_at, agent) VALUES ('active', ?1, 'test')",
            params![now_secs() - 7200],
        )
        .unwrap();
        insert_obs(&conn, "active", "tool_call", "should remain");

        let compressed = super::super::store::compress_stale_sessions(&mut conn, 3600).unwrap();
        assert!(compressed.is_empty());
        assert_eq!(count_obs(&conn, "active"), 1); // not deleted
    }

    // Task 8.4: session below threshold → not compressed
    #[test]
    fn test_session_below_threshold_not_compressed() {
        let (mut conn, _dir) = open_test_db();
        // Ended 30 minutes ago — below 1-hour threshold
        let ended = now_secs() - 1800;
        create_ended_session(&conn, "recent", ended);
        insert_obs(&conn, "recent", "tool_call", "should remain");

        let compressed = super::super::store::compress_stale_sessions(&mut conn, 3600).unwrap();
        assert!(compressed.is_empty());
        assert_eq!(count_obs(&conn, "recent"), 1);
    }

    // ─── Story 10.1: Purge Tests ──────────────────────────────────────────────

    // Session ended 91 days ago → purged (session + obs gone)
    #[test]
    fn test_purge_old_session_deleted() {
        let (mut conn, _dir) = open_test_db();
        let ended = now_secs() - 91 * 86400;
        create_ended_session(&conn, "old", ended);
        insert_obs(&conn, "old", "insight", "old insight");
        insert_obs(&conn, "old", "decision", "old decision");

        let purged = super::super::store::purge_old_sessions(&mut conn, 90 * 86400).unwrap();
        assert_eq!(purged, 1);

        // Session row gone
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM sessions WHERE id = 'old'", [], |r| r.get(0)).unwrap();
        assert_eq!(count, 0);
        // Observations gone
        assert_eq!(count_obs(&conn, "old"), 0);
    }

    // Active session (ended_at IS NULL) → not purged
    #[test]
    fn test_purge_active_session_not_deleted() {
        let (mut conn, _dir) = open_test_db();
        conn.execute(
            "INSERT INTO sessions (id, started_at, agent) VALUES ('active', ?1, 'test')",
            params![now_secs() - 100 * 86400],
        ).unwrap();
        insert_obs(&conn, "active", "insight", "active insight");

        let purged = super::super::store::purge_old_sessions(&mut conn, 90 * 86400).unwrap();
        assert_eq!(purged, 0);
        assert_eq!(count_obs(&conn, "active"), 1);
    }

    // Session ended 89 days ago → not purged
    #[test]
    fn test_purge_recent_session_not_deleted() {
        let (mut conn, _dir) = open_test_db();
        let ended = now_secs() - 89 * 86400;
        create_ended_session(&conn, "recent", ended);
        insert_obs(&conn, "recent", "insight", "recent insight");

        let purged = super::super::store::purge_old_sessions(&mut conn, 90 * 86400).unwrap();
        assert_eq!(purged, 0);
        assert_eq!(count_obs(&conn, "recent"), 1);
    }

    // 2 old sessions → returns 2
    #[test]
    fn test_purge_returns_count() {
        let (mut conn, _dir) = open_test_db();
        let ended = now_secs() - 100 * 86400;
        create_ended_session(&conn, "old1", ended);
        create_ended_session(&conn, "old2", ended - 86400);

        let purged = super::super::store::purge_old_sessions(&mut conn, 90 * 86400).unwrap();
        assert_eq!(purged, 2);
    }

    // Verify observations deleted atomically with session
    #[test]
    fn test_purge_observations_cascade_in_transaction() {
        let (mut conn, _dir) = open_test_db();
        let ended = now_secs() - 100 * 86400;
        create_ended_session(&conn, "old", ended);
        insert_obs(&conn, "old", "insight", "insight1");
        insert_obs(&conn, "old", "decision", "decision1");
        insert_obs(&conn, "old", "error", "error1");

        // Keep a recent session to verify it's untouched
        create_ended_session(&conn, "keep", now_secs() - 86400);
        insert_obs(&conn, "keep", "insight", "keep this");

        super::super::store::purge_old_sessions(&mut conn, 90 * 86400).unwrap();

        assert_eq!(count_obs(&conn, "old"), 0);
        assert_eq!(count_obs(&conn, "keep"), 1);
    }

    // Task 8.5: multiple sessions, only ended+stale ones compressed
    #[test]
    fn test_multiple_sessions_only_stale_compressed() {
        let (mut conn, _dir) = open_test_db();
        let now = now_secs();

        // Active session (no ended_at)
        conn.execute(
            "INSERT INTO sessions (id, started_at, agent) VALUES ('active', ?1, 'test')",
            params![now - 7200],
        )
        .unwrap();
        insert_obs(&conn, "active", "tool_call", "active obs");

        // Recent ended session (below threshold)
        create_ended_session(&conn, "recent", now - 1800);
        insert_obs(&conn, "recent", "tool_call", "recent obs");

        // Stale ended session (above threshold)
        create_ended_session(&conn, "stale", now - 7200);
        insert_obs(&conn, "stale", "tool_call", "stale obs");
        insert_obs(&conn, "stale", "insight", "stale insight");

        let compressed = super::super::store::compress_stale_sessions(&mut conn, 3600).unwrap();
        assert_eq!(compressed, vec!["stale"]);

        // Active and recent still have tool_call
        assert_eq!(count_obs_by_kind(&conn, "active", "tool_call"), 1);
        assert_eq!(count_obs_by_kind(&conn, "recent", "tool_call"), 1);
        // Stale had tool_call removed, insight retained
        assert_eq!(count_obs_by_kind(&conn, "stale", "tool_call"), 0);
        assert_eq!(count_obs_by_kind(&conn, "stale", "insight"), 1);
    }
}
