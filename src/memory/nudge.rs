use rusqlite::{params, Connection, OptionalExtension};

use super::store::StoreError;

/// Observation kinds that indicate the agent is persisting valuable knowledge.
const VALUABLE_KINDS: &[&str] = &["insight", "decision", "error"];

/// Tools eligible for nudge append — plain-text exploratory tools only.
/// JSON-returning tools (submit_lsp_edges) and mutation tools (save_observation, undo_change) are excluded.
pub(crate) const NUDGE_ELIGIBLE_TOOLS: &[&str] = &[
    "get_brief", "get_context", "get_session_history", "memory_health",
    "get_file_skeleton", "get_impact", "trace_flow", "analyze_failure",
];

/// Signal that repo-wide Bash searches were detected in the session.
pub(crate) struct BashNudgeSignal {
    pub count: u32,
}

/// Minimum command length to attempt classification. Shorter commands are likely
/// truncated beyond usefulness.
const MIN_CLASSIFIABLE_LEN: usize = 15;

/// Classify a command string (after "Ran command: " prefix) as a repo-wide search.
/// Returns true only for high-confidence matches. Biases toward false negatives.
fn is_repo_wide_search(cmd: &str) -> bool {
    let cmd = cmd.trim();
    if cmd.len() < MIN_CLASSIFIABLE_LEN {
        return false;
    }

    // Reject piped commands where grep/rg is not the leading command
    if let Some(pipe_pos) = cmd.find('|') {
        let before_pipe = cmd[..pipe_pos].trim();
        if !before_pipe.starts_with("grep") && !before_pipe.starts_with("rg") {
            return false;
        }
    }

    if cmd.starts_with("grep ") || cmd.starts_with("grep\t") {
        return is_recursive_grep(cmd);
    }
    if cmd.starts_with("rg ") || cmd.starts_with("rg\t") {
        return is_broad_rg(cmd);
    }
    false
}

/// Check if a grep command has recursive flags (-r, -R, --recursive).
fn is_recursive_grep(cmd: &str) -> bool {
    for token in cmd.split_whitespace().skip(1) {
        if token == "--recursive" {
            return true;
        }
        if token.starts_with('-') && !token.starts_with("--") {
            let flags = &token[1..];
            if flags.contains('r') || flags.contains('R') {
                return true;
            }
        }
    }
    false
}

/// Check if an rg command is a broad repo-wide search (no narrowing path operand).
/// Scans all non-flag tokens after `rg` for path-like operands that would narrow scope.
fn is_broad_rg(cmd: &str) -> bool {
    let tokens: Vec<&str> = cmd.split_whitespace().collect();
    if tokens.contains(&"--files") {
        return false;
    }
    // Check all non-flag tokens (skip "rg" itself) for path-like operands.
    // The first non-flag token is the pattern; any subsequent non-flag token is a path.
    let mut saw_pattern = false;
    for token in tokens.iter().skip(1) {
        if token.starts_with('-') {
            continue;
        }
        if !saw_pattern {
            saw_pattern = true;
            continue; // this is the search pattern
        }
        // Any non-flag token after the pattern is a path operand → narrowed search
        if looks_like_path(token) {
            return false;
        }
    }
    true
}

/// Heuristic: does this token look like a file/directory path?
/// Contains a slash or ends with a file extension (dot followed by 1-4 alphanum chars).
fn looks_like_path(token: &str) -> bool {
    // Strip surrounding quotes
    let t = token.trim_matches(|c| c == '"' || c == '\'');
    if t.contains('/') {
        return true;
    }
    // Check for file extension at the end: .rs, .ts, .py, .html etc.
    if let Some(dot_pos) = t.rfind('.') {
        let ext = &t[dot_pos + 1..];
        if !ext.is_empty() && ext.len() <= 4 && ext.chars().all(|c| c.is_alphanumeric()) {
            // But not if the dot is part of a regex pattern — check if there's
            // content before the dot that also looks like a path
            // Simple: if the whole token has no spaces and ends with .ext, it's a path
            // Patterns like "foo.bar" could be regex, but we bias toward false negatives
            // Only treat as path if there's a clear path-like prefix (alphanumeric/underscore/hyphen before dot)
            let prefix = &t[..dot_pos];
            if !prefix.is_empty() && prefix.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.') {
                return true;
            }
        }
    }
    false
}

/// Detect repeated repo-wide Bash search commands in the current session.
/// Queries the most recent 10 Bash-command observations and classifies them.
/// Returns a signal if ≥ 3 match repo-wide search patterns.
pub(crate) fn detect_bash_search_nudge(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<BashNudgeSignal>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT content FROM observations \
         WHERE session_id = ?1 \
           AND kind = 'tool_call' \
           AND content LIKE 'Ran command:%' \
         ORDER BY created_at DESC \
         LIMIT 10",
    )?;
    let rows: Vec<String> = stmt
        .query_map(params![session_id], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    let prefix = "Ran command: ";
    let mut count = 0u32;
    for content in &rows {
        if let Some(cmd) = content.strip_prefix(prefix)
            && is_repo_wide_search(cmd)
        {
            count += 1;
        }
    }

    if count >= 3 {
        Ok(Some(BashNudgeSignal { count }))
    } else {
        Ok(None)
    }
}

/// Format the nudge message for a Bash search signal.
fn format_bash_nudge(signal: &BashNudgeSignal) -> String {
    format!(
        "\n\n[Olaf] You've used repo-wide search {} times recently. \
         For exploration, try: get_brief({{\"intent\": \"find where auth tokens are validated\"}})",
        signal.count
    )
}

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
/// - bash search signal detected (priority) OR struggle pattern detected (file thrashing)
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
    // Bash search nudge takes priority over file-thrash
    if let Some(signal) = detect_bash_search_nudge(conn, session_id)? {
        return Ok(Some(format_bash_nudge(&signal)));
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

    /// Eligible tools include all exploratory tools (expanded in story 11-6)
    #[test]
    fn test_eligible_tools_include_exploratory() {
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_brief"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_context"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_session_history"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"memory_health"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_file_skeleton"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"get_impact"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"trace_flow"));
        assert!(NUDGE_ELIGIBLE_TOOLS.contains(&"analyze_failure"));
    }

    /// JSON-returning and mutation tools are NOT eligible for nudge
    #[test]
    fn test_ineligible_tools_excluded() {
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"submit_lsp_edges"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"save_observation"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"undo_change"));
        assert!(!NUDGE_ELIGIBLE_TOOLS.contains(&"index_status"));
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

    // --- is_repo_wide_search classification ---

    #[test]
    fn test_classify_rg_broad_match() {
        assert!(is_repo_wide_search(r#"rg "TODO" --type rs"#));
        assert!(is_repo_wide_search(r#"rg -t rs "struct""#));
        assert!(is_repo_wide_search(r#"rg --type-add 'web:*.html' pattern"#));
    }

    #[test]
    fn test_classify_grep_recursive_match() {
        assert!(is_repo_wide_search(r#"grep -r "pattern" ."#));
        assert!(is_repo_wide_search("grep -rn TODO src/"));
        assert!(is_repo_wide_search(r#"grep -Rl "import""#));
        assert!(is_repo_wide_search("grep --recursive foo"));
    }

    #[test]
    fn test_classify_single_file_grep_no_match() {
        assert!(!is_repo_wide_search("grep foo src/main.rs"));
        assert!(!is_repo_wide_search(r#"grep -n "foo" src/main.rs"#));
    }

    #[test]
    fn test_classify_git_grep_no_match() {
        assert!(!is_repo_wide_search("git grep pattern"));
        assert!(!is_repo_wide_search(r#"git grep -n "TODO""#));
    }

    #[test]
    fn test_classify_rg_files_no_match() {
        assert!(!is_repo_wide_search("rg --files src/"));
        assert!(!is_repo_wide_search("rg --files --type rs"));
    }

    #[test]
    fn test_classify_rg_narrowed_to_file_no_match() {
        assert!(!is_repo_wide_search(r#"rg "pattern" src/auth.rs"#));
        assert!(!is_repo_wide_search(r#"rg "foo" src/"#));
        // Path operand before trailing flags (P1 review finding)
        assert!(!is_repo_wide_search(r#"rg "pattern" src/ --type rs"#));
        assert!(!is_repo_wide_search(r#"rg foo src/ -g '*.rs'"#));
    }

    #[test]
    fn test_classify_piped_command_no_match() {
        assert!(!is_repo_wide_search("cat foo | grep bar baz"));
        assert!(!is_repo_wide_search("find . -name '*.rs' | rg pattern foo"));
    }

    #[test]
    fn test_classify_non_search_commands_no_match() {
        assert!(!is_repo_wide_search("git status --porcelain"));
        assert!(!is_repo_wide_search("cargo test --lib memory"));
        assert!(!is_repo_wide_search("ls -la src/"));
    }

    #[test]
    fn test_classify_short_command_no_match() {
        assert!(!is_repo_wide_search("rg foo"));       // 6 chars < 15
        assert!(!is_repo_wide_search("grep -r x"));    // 10 chars < 15
    }

    #[test]
    fn test_classify_rg_pattern_with_dot_not_path() {
        // "foo.bar" is a regex pattern, not a file path
        assert!(is_repo_wide_search(r#"rg "foo\.bar" --type rs"#));
    }

    // --- detect_bash_search_nudge ---

    fn insert_bash_obs(conn: &Connection, session_id: &str, cmd: &str) {
        insert_auto_observation(
            conn, session_id, "tool_call",
            &format!("Ran command: {cmd}"),
            None, None, None,
        ).unwrap();
    }

    #[test]
    fn test_detect_bash_3_rg_nudge_fires() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_bash_obs(&conn, "s1", r#"rg "TODO" --type rs"#);
        insert_bash_obs(&conn, "s1", r#"rg "FIXME" --type rs"#);
        insert_bash_obs(&conn, "s1", r#"rg "HACK" --type rs"#);
        let signal = detect_bash_search_nudge(&conn, "s1").unwrap().unwrap();
        assert_eq!(signal.count, 3);
    }

    #[test]
    fn test_detect_bash_2_rg_no_nudge() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_bash_obs(&conn, "s1", r#"rg "TODO" --type rs"#);
        insert_bash_obs(&conn, "s1", r#"rg "FIXME" --type rs"#);
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_3_grep_r_nudge_fires() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_bash_obs(&conn, "s1", r#"grep -r "pattern" ."#);
        insert_bash_obs(&conn, "s1", "grep -rn TODO src/");
        insert_bash_obs(&conn, "s1", r#"grep -Rl "import" ."#);
        let signal = detect_bash_search_nudge(&conn, "s1").unwrap().unwrap();
        assert_eq!(signal.count, 3);
    }

    #[test]
    fn test_detect_bash_mixed_observations_only_searches_count() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // 3 searches + non-search + non-bash observations
        insert_bash_obs(&conn, "s1", r#"rg "TODO" --type rs"#);
        insert_bash_obs(&conn, "s1", "git status --porcelain");
        insert_bash_obs(&conn, "s1", r#"rg "FIXME" --type rs"#);
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("f.rs"), None).unwrap();
        insert_bash_obs(&conn, "s1", r#"rg "HACK" --type rs"#);
        let signal = detect_bash_search_nudge(&conn, "s1").unwrap().unwrap();
        assert_eq!(signal.count, 3);
    }

    #[test]
    fn test_detect_bash_single_file_grep_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", "grep foo src/main.rs file");
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_git_grep_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", "git grep pattern in files");
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_rg_files_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", "rg --files --type rs src/");
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_non_search_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", "cargo test --lib memory::nudge");
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_truncated_ambiguous_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", "rg foo"); // too short
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_rg_narrowed_to_file_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", r#"rg "pattern" src/auth.rs"#);
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_detect_bash_piped_grep_no_match() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..5 {
            insert_bash_obs(&conn, "s1", "cat foo.txt | grep bar baz");
        }
        assert!(detect_bash_search_nudge(&conn, "s1").unwrap().is_none());
    }

    #[test]
    fn test_bash_nudge_message_format() {
        let signal = BashNudgeSignal { count: 3 };
        let msg = format_bash_nudge(&signal);
        assert!(msg.contains("[Olaf]"));
        assert!(msg.contains("3 times"));
        assert!(msg.contains("get_brief"));
    }

    // --- should_nudge with bash search priority (Task 3) ---

    #[test]
    fn test_should_nudge_bash_search_wins_over_file_thrash() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // Set up file-thrash pattern
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        // Set up bash search pattern
        insert_bash_obs(&conn, "s1", r#"rg "TODO" --type rs"#);
        insert_bash_obs(&conn, "s1", r#"rg "FIXME" --type rs"#);
        insert_bash_obs(&conn, "s1", r#"rg "HACK" --type rs"#);
        let result = should_nudge(&conn, "s1").unwrap();
        assert!(result.is_some());
        let text = result.unwrap();
        // Bash nudge won — contains get_brief, not save_observation
        assert!(text.contains("get_brief"));
        assert!(text.contains("repo-wide search"));
    }

    #[test]
    fn test_should_nudge_file_thrash_when_no_bash_search() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        for _ in 0..3 {
            insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/auth.rs"), None).unwrap();
        }
        let result = should_nudge(&conn, "s1").unwrap();
        assert!(result.is_some());
        let text = result.unwrap();
        assert!(text.contains("save_observation"));
    }

    #[test]
    fn test_should_nudge_none_healthy_no_signals() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "edit", None, Some("src/a.rs"), None).unwrap();
        insert_bash_obs(&conn, "s1", "cargo test --lib memory");
        assert!(should_nudge(&conn, "s1").unwrap().is_none());
    }
}
