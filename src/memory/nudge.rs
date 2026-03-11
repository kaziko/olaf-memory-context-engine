use rusqlite::{params, Connection, OptionalExtension};

use super::store::StoreError;

/// Observation kinds that indicate the agent is persisting valuable knowledge.
const VALUABLE_KINDS: &[&str] = &["insight", "decision", "error"];

/// Tools eligible for nudge append — plain-text exploratory tools only.
/// JSON-returning tools (submit_lsp_edges) and mutation tools (save_observation, undo_change) are excluded.
pub(crate) const NUDGE_ELIGIBLE_TOOLS: &[&str] = &[
    "get_brief", "get_context", "get_session_history", "memory_health",
];

/// Detect struggle pattern: same file edited ≥ 3 times in any 5-minute tumbling window.
/// Returns the top file_path if found, None otherwise.
pub(crate) fn detect_struggle(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<String>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT file_path \
         FROM observations \
         WHERE session_id = ?1 \
           AND kind = 'file_change' \
           AND auto_generated = 1 \
           AND file_path IS NOT NULL \
         GROUP BY file_path, (created_at / 300) \
         HAVING COUNT(*) >= 3 \
         ORDER BY (created_at / 300) DESC, COUNT(*) DESC \
         LIMIT 1",
    )?;
    let result = stmt
        .query_row(params![session_id], |r| r.get::<_, String>(0))
        .optional()?;
    Ok(result)
}

/// Check whether the session already has a valuable observation (insight/decision/error).
pub(crate) fn has_valuable_observation(
    conn: &Connection,
    session_id: &str,
) -> Result<bool, StoreError> {
    // Build IN clause from VALUABLE_KINDS to keep single source of truth
    let placeholders: Vec<String> = VALUABLE_KINDS.iter().enumerate()
        .map(|(i, _)| format!("?{}", i + 2))
        .collect();
    let sql = format!(
        "SELECT COUNT(*) FROM observations \
         WHERE session_id = ?1 AND kind IN ({})",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut param_values: Vec<&dyn rusqlite::ToSql> = vec![&session_id];
    for kind in VALUABLE_KINDS {
        param_values.push(kind);
    }
    let count: i64 = stmt.query_row(rusqlite::params_from_iter(param_values), |r| r.get(0))?;
    Ok(count > 0)
}

/// Check whether the nudge has already been sent for this session.
pub(crate) fn is_nudge_sent(conn: &Connection, session_id: &str) -> Result<bool, StoreError> {
    let result = conn
        .query_row(
            "SELECT COALESCE(nudge_sent, 0) FROM sessions WHERE id = ?1",
            params![session_id],
            |r| r.get::<_, i64>(0),
        )
        .optional()?;
    Ok(result.unwrap_or(0) != 0)
}

/// Mark the nudge as sent for this session.
pub(crate) fn mark_nudge_sent(conn: &Connection, session_id: &str) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE sessions SET nudge_sent = 1 WHERE id = ?1",
        params![session_id],
    )?;
    Ok(())
}

/// Orchestrate nudge decision. Returns formatted nudge text if all conditions are met:
/// - session has not been nudged yet
/// - session has no valuable observations (insight/decision/error)
/// - struggle pattern detected (file thrashing)
pub(crate) fn should_nudge(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<String>, StoreError> {
    if is_nudge_sent(conn, session_id)? {
        return Ok(None);
    }
    if has_valuable_observation(conn, session_id)? {
        return Ok(None);
    }
    match detect_struggle(conn, session_id)? {
        Some(file) => Ok(Some(format!(
            "\n\n[Olaf] Multiple edits to `{file}` in the last 5 minutes without saving an insight. \
             Consider: save_observation({{\"kind\": \"insight\", \"scope\": \"project\", \"content\": \"...\"}})"
        ))),
        None => Ok(None),
    }
}

/// Returns true if this observation kind should suppress future nudges.
pub(crate) fn is_nudge_suppressing_kind(kind: &str) -> bool {
    VALUABLE_KINDS.contains(&kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::store::{insert_auto_observation, insert_observation, upsert_session, Importance};

    fn open_test_db() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open(&db_path).expect("open DB");
        (conn, dir)
    }

    // --- detect_struggle ---

    #[test]
    fn test_detect_struggle_3_edits_same_file_same_bucket() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // Insert 3 file_change observations for same file, same timestamp (same bucket)
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        let result = detect_struggle(&conn, "s1").unwrap();
        assert_eq!(result, Some("src/auth.rs".to_string()));
    }

    #[test]
    fn test_detect_struggle_2_edits_not_enough() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..2 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        let result = detect_struggle(&conn, "s1").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_detect_struggle_different_files_no_trigger() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/a.rs"), None).unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/b.rs"), None).unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/c.rs"), None).unwrap();
        let result = detect_struggle(&conn, "s1").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_detect_struggle_different_buckets_no_trigger() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // Insert with manually spaced created_at across different 5-min buckets
        // We can't control created_at via insert_auto_observation, so we insert directly
        let base = 300 * 1000; // bucket boundary
        for i in 0..3 {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, file_path, auto_generated, is_stale, importance) \
                 VALUES (?1, ?2, 'file_change', 'edit', 'src/auth.rs', 1, 0, 'low')",
                params!["s1", base + i * 300], // each in a different bucket
            ).unwrap();
        }
        let result = detect_struggle(&conn, "s1").unwrap();
        assert_eq!(result, None);
    }

    // --- has_valuable_observation ---

    #[test]
    fn test_has_valuable_insight() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "insight", "learned something", None, Some("f.rs"), None, Importance::Medium).unwrap();
        assert!(has_valuable_observation(&conn, "s1").unwrap());
    }

    #[test]
    fn test_has_valuable_decision() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "decision", "chose X", None, Some("f.rs"), None, Importance::Medium).unwrap();
        assert!(has_valuable_observation(&conn, "s1").unwrap());
    }

    #[test]
    fn test_has_valuable_error() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "error", "failed because X", None, Some("f.rs"), None, Importance::Medium).unwrap();
        assert!(has_valuable_observation(&conn, "s1").unwrap());
    }

    #[test]
    fn test_no_valuable_only_file_change() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("f.rs"), None).unwrap();
        insert_auto_observation(&conn, "s1", "tool_call", "ran tool", None, Some("f.rs"), None).unwrap();
        assert!(!has_valuable_observation(&conn, "s1").unwrap());
    }

    // --- is_nudge_sent / mark_nudge_sent ---

    #[test]
    fn test_nudge_sent_roundtrip() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        assert!(!is_nudge_sent(&conn, "s1").unwrap());
        mark_nudge_sent(&conn, "s1").unwrap();
        assert!(is_nudge_sent(&conn, "s1").unwrap());
    }

    // --- should_nudge ---

    #[test]
    fn test_should_nudge_struggling_no_valuable() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        let result = should_nudge(&conn, "s1").unwrap();
        assert!(result.is_some());
        let text = result.unwrap();
        assert!(text.contains("src/auth.rs"));
        assert!(text.contains("save_observation"));
        assert!(text.contains("scope"));
    }

    #[test]
    fn test_should_nudge_none_when_nudge_already_sent() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        mark_nudge_sent(&conn, "s1").unwrap();
        assert!(should_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_should_nudge_none_when_insight_exists() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        insert_observation(&conn, "s1", "insight", "learned X", None, Some("f.rs"), None, Importance::Medium).unwrap();
        assert!(should_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_should_nudge_none_healthy_session() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/a.rs"), None).unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/b.rs"), None).unwrap();
        assert!(should_nudge(&conn, "s1").unwrap().is_none());
    }

    // --- is_nudge_suppressing_kind ---

    #[test]
    fn test_suppressing_kinds() {
        assert!(is_nudge_suppressing_kind("insight"));
        assert!(is_nudge_suppressing_kind("decision"));
        assert!(is_nudge_suppressing_kind("error"));
        assert!(!is_nudge_suppressing_kind("file_change"));
        assert!(!is_nudge_suppressing_kind("tool_call"));
        assert!(!is_nudge_suppressing_kind("anti_pattern"));
    }

    // --- NUDGE_ELIGIBLE_TOOLS (integration path coverage for tasks 2.4-2.7) ---

    /// Task 2.4: Eligible tools include the expected exploratory tools
    #[test]
    fn test_eligible_tools_include_exploratory() {
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_brief"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_context"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_session_history"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"memory_health"));
    }

    /// Task 2.5: JSON-returning and mutation tools are NOT eligible for nudge
    #[test]
    fn test_ineligible_tools_excluded() {
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"submit_lsp_edges"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"save_observation"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"undo_change"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"get_file_skeleton"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"get_impact"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"trace_flow"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"index_status"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"analyze_failure"));
    }

    /// Task 2.6: Saving an insight observation suppresses nudge (simulates tools.rs path)
    #[test]
    fn test_save_insight_suppresses_nudge() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // Set up struggle pattern
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        // Verify nudge would fire
        assert!(should_nudge(&conn, "s1").unwrap().is_some());

        // Simulate what handle_save_observation does: check kind + mark_nudge_sent
        let kind = "insight";
        assert!(is_nudge_suppressing_kind(kind));
        mark_nudge_sent(&conn, "s1").unwrap();

        // Verify nudge is now suppressed
        assert!(should_nudge(&conn, "s1").unwrap().is_none());
        assert!(is_nudge_sent(&conn, "s1").unwrap());
    }

    /// Task 2.7: Healthy session with < 3 edits never triggers nudge regardless of tool
    #[test]
    fn test_healthy_session_no_nudge_regardless_of_tool() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // Only 2 edits — below threshold
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        // should_nudge returns None — so even if tool is eligible, no nudge would be appended
        assert!(should_nudge(&conn, "s1").unwrap().is_none());
        // Also verify no false positive from detect_struggle
        assert!(detect_struggle(&conn, "s1").unwrap().is_none());
    }
}
