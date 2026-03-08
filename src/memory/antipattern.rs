use std::collections::HashMap;

use rusqlite::{Connection, params};

use super::store::{StoreError, insert_auto_observation};

/// Detect dead-end exploration: same context retrieved ≥2 times with no productive activity.
/// Returns Vec of "{subject} (×{count})" strings for repeated subjects, or empty if
/// the session was productive (has insight/decision/file_change observations).
pub fn detect_dead_end(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<String>, StoreError> {
    // Step A: find repeated subjects (same content, count ≥ 2)
    let mut stmt = conn.prepare(
        "SELECT content, CAST(COUNT(*) AS INTEGER) AS cnt \
         FROM observations \
         WHERE session_id = ?1 AND kind = 'context_retrieval' AND auto_generated = 1 \
         GROUP BY content \
         HAVING cnt >= 2 \
         ORDER BY content",
    )?;
    let repeated: Vec<(String, usize)> = stmt
        .query_map(params![session_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as usize))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    if repeated.is_empty() {
        return Ok(vec![]);
    }

    // Step B: check for productive activity (suppression guard)
    let productive_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM observations \
         WHERE session_id = ?1 AND kind IN ('insight', 'decision', 'file_change')",
        params![session_id],
        |r| r.get(0),
    )?;
    if productive_count > 0 {
        return Ok(vec![]);
    }

    // Step C: return repeated subjects formatted
    Ok(repeated
        .into_iter()
        .map(|(subject, count)| format!("{subject} (×{count})"))
        .collect())
}

/// Detect file thrashing using a tumbling 5-minute window (epoch bucket: created_at / 300).
/// Only counts `auto_generated = 1` file_change observations — manual saves are excluded.
/// Returns Vec<(file_path, max_count_in_any_single_bucket)> sorted by file_path.
/// O(n) — entirely in SQLite, no in-memory per-observation loops.
pub fn detect_file_thrashing(
    conn: &Connection,
    session_id: &str,
) -> Result<Vec<(String, usize)>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT file_path, (created_at / 300) AS bucket, CAST(COUNT(*) AS INTEGER) AS cnt \
         FROM observations \
         WHERE session_id = ?1 \
           AND kind = 'file_change' \
           AND auto_generated = 1 \
           AND file_path IS NOT NULL \
         GROUP BY file_path, bucket \
         HAVING cnt >= 4 \
         ORDER BY file_path, bucket",
    )?;

    // Collect rows and take max cnt per file_path across all buckets
    let rows = stmt.query_map(params![session_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(2)? as usize))
    })?;

    let mut max_per_file: HashMap<String, usize> = HashMap::new();
    for row in rows {
        let (file_path, cnt) = row?;
        let entry = max_per_file.entry(file_path).or_insert(0);
        if cnt > *entry {
            *entry = cnt;
        }
    }

    let mut result: Vec<(String, usize)> = max_per_file.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

/// Run all anti-pattern detections and write findings as observations.
/// Returns Ok(()) when complete; errors remain in library domain (StoreError).
pub fn detect_and_write_anti_patterns(
    conn: &Connection,
    session_id: &str,
) -> Result<(), StoreError> {
    // Dead-end detection
    let dead_ends = detect_dead_end(conn, session_id)?;
    if !dead_ends.is_empty() {
        let content = format!(
            "Dead-end exploration: same context retrieved multiple times without producing \
             insight, decision, or file changes. Repeated: {}",
            dead_ends.join(", ")
        );
        insert_auto_observation(conn, session_id, "anti_pattern", &content, None, None)?;
    }

    // File thrashing detection
    let thrashing = detect_file_thrashing(conn, session_id)?;
    for (path, cnt) in thrashing {
        insert_auto_observation(
            conn,
            session_id,
            "anti_pattern",
            &format!("File thrashing detected: {} modified {} times in 5 minutes", path, cnt),
            None,
            Some(path.as_str()),
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::store::upsert_session;

    fn open_test_db() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open(&db_path).expect("open DB");
        (conn, dir)
    }

    fn insert_file_change(conn: &Connection, session_id: &str, file_path: &str, created_at: i64) {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, file_path, auto_generated) \
             VALUES (?1, ?2, 'file_change', 'edit', ?3, 1)",
            params![session_id, created_at, file_path],
        )
        .expect("insert file_change");
    }

    // 1.4: empty session → both detections return empty
    #[test]
    fn test_empty_session_returns_empty() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();

        let dead_ends = detect_dead_end(&conn, "s1").unwrap();
        assert!(dead_ends.is_empty());

        let thrashing = detect_file_thrashing(&conn, "s1").unwrap();
        assert!(thrashing.is_empty());
    }

    // 1.5: 4 file_change obs in same 300s bucket → returns 1 result with count=4
    #[test]
    fn test_detect_file_thrashing_four_in_same_bucket() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for t in [0i64, 60, 120, 180] {
            insert_file_change(&conn, "s1", "src/main.rs", t);
        }
        let result = detect_file_thrashing(&conn, "s1").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "src/main.rs");
        assert_eq!(result[0].1, 4);
    }

    // 1.6: 3 obs (under threshold) → empty
    #[test]
    fn test_detect_file_thrashing_under_threshold() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for t in [0i64, 60, 120] {
            insert_file_change(&conn, "s1", "src/main.rs", t);
        }
        let result = detect_file_thrashing(&conn, "s1").unwrap();
        assert!(result.is_empty());
    }

    // 1.7: 4 obs for file A and 2 obs for file B → only file A
    #[test]
    fn test_detect_file_thrashing_only_threshold_file_returned() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for t in [0i64, 60, 120, 180] {
            insert_file_change(&conn, "s1", "src/a.rs", t);
        }
        for t in [0i64, 60] {
            insert_file_change(&conn, "s1", "src/b.rs", t);
        }
        let result = detect_file_thrashing(&conn, "s1").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "src/a.rs");
    }

    // 1.8: 6 obs spanning two buckets (3+3) → neither bucket hits threshold → empty
    #[test]
    fn test_detect_file_thrashing_bucket_boundary_split() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // bucket 0: created_at = 0, 60, 120
        for t in [0i64, 60, 120] {
            insert_file_change(&conn, "s1", "src/main.rs", t);
        }
        // bucket 1: created_at = 300, 360, 420
        for t in [300i64, 360, 420] {
            insert_file_change(&conn, "s1", "src/main.rs", t);
        }
        let result = detect_file_thrashing(&conn, "s1").unwrap();
        assert!(result.is_empty(), "tumbling window: split across boundary should not be counted");
    }

    // 1.9: 4 file_change obs with auto_generated=0 → not counted
    #[test]
    fn test_detect_file_thrashing_excludes_manual_saves() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // Insert with auto_generated=0 (manual saves)
        for t in [0i64, 60, 120, 180] {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, file_path, auto_generated) \
                 VALUES ('s1', ?1, 'file_change', 'manual edit', 'src/main.rs', 0)",
                params![t],
            )
            .unwrap();
        }
        let result = detect_file_thrashing(&conn, "s1").unwrap();
        assert!(result.is_empty(), "manual saves (auto_generated=0) must not trigger thrashing");
    }

    // 1.10: thrashing detected → anti_pattern obs written with correct content, file_path, auto_generated=1
    #[test]
    fn test_detect_and_write_anti_patterns_writes_observation() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for t in [0i64, 60, 120, 180] {
            insert_file_change(&conn, "s1", "src/main.rs", t);
        }
        detect_and_write_anti_patterns(&conn, "s1").unwrap();

        let (kind, content, file_path, auto_gen): (String, String, Option<String>, i64) = conn
            .query_row(
                "SELECT kind, content, file_path, auto_generated FROM observations \
                 WHERE kind = 'anti_pattern' AND session_id = 's1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(kind, "anti_pattern");
        assert!(content.contains("File thrashing detected: src/main.rs"));
        assert!(content.contains("4 times in 5 minutes"));
        assert_eq!(file_path.as_deref(), Some("src/main.rs"));
        assert_eq!(auto_gen, 1);
    }

    // Dead-end: 2 same intents, no productive obs → returns 1 subject
    #[test]
    fn test_dead_end_two_same_intents_no_productive_obs() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
                 VALUES ('s1', 100, 'context_retrieval', 'get_context: refactoring', 1)",
                [],
            ).unwrap();
        }
        let result = detect_dead_end(&conn, "s1").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("get_context: refactoring"));
        assert!(result[0].contains("×2"));
    }

    // Dead-end suppressed by file_change
    #[test]
    fn test_no_dead_end_with_file_change() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
                 VALUES ('s1', 100, 'context_retrieval', 'get_context: refactoring', 1)",
                [],
            ).unwrap();
        }
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
             VALUES ('s1', 200, 'file_change', 'edited src/main.rs', 1)",
            [],
        ).unwrap();
        let result = detect_dead_end(&conn, "s1").unwrap();
        assert!(result.is_empty(), "file_change should suppress dead-end");
    }

    // Dead-end suppressed by decision
    #[test]
    fn test_no_dead_end_with_decision() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
                 VALUES ('s1', 100, 'context_retrieval', 'get_context: refactoring', 1)",
                [],
            ).unwrap();
        }
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
             VALUES ('s1', 200, 'decision', 'chose approach A', 0)",
            [],
        ).unwrap();
        let result = detect_dead_end(&conn, "s1").unwrap();
        assert!(result.is_empty(), "decision should suppress dead-end");
    }

    // Single retrieval → below threshold
    #[test]
    fn test_no_dead_end_single_retrieval() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
             VALUES ('s1', 100, 'context_retrieval', 'get_context: refactoring', 1)",
            [],
        ).unwrap();
        let result = detect_dead_end(&conn, "s1").unwrap();
        assert!(result.is_empty(), "single retrieval should not trigger dead-end");
    }

    // Two different subjects: A appears 2×, B appears 1× → only A returned
    #[test]
    fn test_dead_end_two_different_subjects() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
                 VALUES ('s1', 100, 'context_retrieval', 'get_context: refactoring', 1)",
                [],
            ).unwrap();
        }
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
             VALUES ('s1', 100, 'context_retrieval', 'get_impact: foo::bar', 1)",
            [],
        ).unwrap();
        let result = detect_dead_end(&conn, "s1").unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("get_context: refactoring"));
    }

    // Full detect_and_write_anti_patterns → verify anti_pattern obs content
    #[test]
    fn test_dead_end_observation_written() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..3 {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) \
                 VALUES ('s1', 100, 'context_retrieval', 'get_context: debug auth', 1)",
                [],
            ).unwrap();
        }
        detect_and_write_anti_patterns(&conn, "s1").unwrap();

        let content: String = conn
            .query_row(
                "SELECT content FROM observations WHERE kind = 'anti_pattern' AND session_id = 's1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(content.contains("Dead-end exploration"));
        assert!(content.contains("get_context: debug auth"));
        assert!(content.contains("×3"));
    }
}
