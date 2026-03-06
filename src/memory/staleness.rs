use std::collections::HashSet;

use rusqlite::Transaction;

use crate::index::diff::StructuralDiff;
use crate::parser::symbols::fqn_short_name;

const CHUNK_SIZE: usize = 500;

/// Mark observations stale for the given FQNs (already filtered as actually changed).
#[cfg(test)]
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
    for (fqn, old_sig, new_sig) in &diff.signature_changed {
        let reason = format!(
            "Signature of symbol '{}' changed: `{}` → `{}`",
            fqn_short_name(fqn),
            old_sig,
            new_sig
        );
        batch_mark_stale(tx, std::slice::from_ref(fqn), &reason)?;
    }
    mark_stale_for_removed_symbols(tx, &diff.removed)?;

    // Renamed symbols: mark observations on old FQN as stale
    for (old_fqn, new_fqn) in &diff.renamed {
        let reason = format!("Symbol renamed to '{}'", new_fqn);
        batch_mark_stale(tx, std::slice::from_ref(old_fqn), &reason)?;
    }

    // File-path observation text scanning (AC #2):
    // Build changed FQN list from signature_changed + removed + renamed old FQNs
    let mut changed_fqns: Vec<String> = Vec::new();
    for (fqn, _, _) in &diff.signature_changed {
        changed_fqns.push(fqn.clone());
    }
    for fqn in &diff.removed {
        changed_fqns.push(fqn.clone());
    }
    for (old_fqn, _) in &diff.renamed {
        changed_fqns.push(old_fqn.clone());
    }

    if !changed_fqns.is_empty() {
        // Query file-path-only observations for this file
        let mut stmt = tx.prepare(
            "SELECT id, content FROM observations \
             WHERE symbol_fqn IS NULL AND file_path = ?1 AND is_stale = 0",
        )?;
        let rows: Vec<(i64, String)> = stmt
            .query_map([&diff.file_path], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        for (id, content) in rows {
            let matches = extract_backtick_symbol_refs(&content, &changed_fqns);
            if !matches.is_empty() {
                let reason = if matches.len() == 1 {
                    format!("Referenced symbol '{}' structurally changed", matches[0])
                } else {
                    format!(
                        "Referenced symbol '{}' structurally changed (and {} others)",
                        matches[0],
                        matches.len() - 1
                    )
                };
                mark_observation_stale_by_id(tx, id, &reason)?;
            }
        }
    }

    Ok(())
}

/// Mark a single observation stale by ID, preserving first-reason-wins.
fn mark_observation_stale_by_id(
    tx: &Transaction,
    id: i64,
    reason: &str,
) -> Result<(), rusqlite::Error> {
    tx.execute(
        "UPDATE observations SET is_stale = 1, stale_reason = ?1 WHERE id = ?2 AND is_stale = 0",
        rusqlite::params![reason, id],
    )?;
    Ok(())
}

/// Strip trailing `()` or `(...)` from a backtick-quoted token.
///
/// If the last char is `)`, scan backward counting nesting depth to find
/// the matching `(`, then remove from that `(` to end.
fn strip_trailing_parens(token: &str) -> &str {
    if !token.ends_with(')') {
        return token;
    }
    let bytes = token.as_bytes();
    let mut depth: i32 = 0;
    for i in (0..bytes.len()).rev() {
        if bytes[i] == b')' {
            depth += 1;
        } else if bytes[i] == b'(' {
            depth -= 1;
            if depth == 0 {
                return &token[..i];
            }
        }
    }
    // Unbalanced — no matching `(`, return as-is
    token
}

/// Extract backtick-quoted symbol references from observation content.
///
/// Returns a deduplicated, provenance-ordered list of matched FQNs:
/// Phase 1 (full FQN) matches first sorted alphabetically,
/// then Phase 2 (short name) matches sorted alphabetically,
/// with duplicates removed.
pub(crate) fn extract_backtick_symbol_refs(content: &str, changed_fqns: &[String]) -> Vec<String> {
    // Extract single-backtick tokens from content
    let tokens = extract_backtick_tokens(content);

    // Collect which FQNs were matched by Phase 1 (full FQN) vs Phase 2 (short name).
    // A FQN that matches both phases is classified as Phase 1 (higher provenance).
    let mut phase1_set: HashSet<String> = HashSet::new();
    let mut phase2_set: HashSet<String> = HashSet::new();

    for token in &tokens {
        let trimmed = token.trim_end_matches('\r');
        let normalized = strip_trailing_parens(trimmed);

        if normalized.is_empty() {
            continue;
        }

        // Phase 1: full FQN match
        let phase1_hit = changed_fqns.iter().any(|fqn| normalized == fqn.as_str());
        if phase1_hit {
            for fqn in changed_fqns {
                if normalized == fqn.as_str() {
                    phase1_set.insert(fqn.clone());
                }
            }
        } else {
            // Phase 2: short name match
            for fqn in changed_fqns {
                if normalized == fqn_short_name(fqn) {
                    phase2_set.insert(fqn.clone());
                }
            }
        }
    }

    // Phase 1 wins: remove any FQN from Phase 2 that also appeared in Phase 1
    phase2_set.retain(|fqn| !phase1_set.contains(fqn));

    let mut phase1: Vec<String> = phase1_set.into_iter().collect();
    let mut phase2: Vec<String> = phase2_set.into_iter().collect();
    phase1.sort();
    phase2.sort();

    phase1.extend(phase2);
    phase1
}

/// Extract single-backtick-quoted tokens from text.
///
/// Skips multi-backtick sequences (`` `` `` or `` ``` ``).
/// Spans do NOT cross line boundaries. Empty spans are skipped.
fn extract_backtick_tokens(content: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let chars: Vec<(usize, char)> = content.char_indices().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i].1 == '`' {
            // Count consecutive backticks
            let start = i;
            while i < len && chars[i].1 == '`' {
                i += 1;
            }
            let backtick_count = i - start;

            if backtick_count > 1 {
                // Multi-backtick: skip to matching closing sequence or end-of-line
                while i < len && chars[i].1 != '\n' {
                    if chars[i].1 == '`' {
                        let cs = i;
                        while i < len && chars[i].1 == '`' {
                            i += 1;
                        }
                        if i - cs == backtick_count {
                            break;
                        }
                        continue;
                    }
                    i += 1;
                }
                continue;
            }

            // Single backtick — scan for closing single backtick
            let token_start = i;
            let mut found_close = false;
            while i < len {
                if chars[i].1 == '\n' {
                    break; // Unclosed — discard
                }
                if chars[i].1 == '`' {
                    // Found closing backtick
                    let token_end = chars[i].0;
                    let token_start_byte = chars[token_start].0;
                    let token_str = &content[token_start_byte..token_end];
                    if !token_str.is_empty() {
                        tokens.push(token_str.to_string());
                    }
                    i += 1;
                    found_close = true;
                    break;
                }
                i += 1;
            }
            if !found_close {
                // Unclosed span — already skipped past newline or end
            }
        } else {
            i += 1;
        }
    }

    tokens
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
        use crate::index::diff::StructuralDiff;
        let mut conn = open_test_db();
        insert_session(&conn, "s1");
        // File-level observation: symbol_fqn = NULL — must not be affected by symbol staleness
        let obs_id = insert_observation(&conn, "s1", None, Some("src/lib.rs"));

        let diff = StructuralDiff {
            file_path: "src/lib.rs".into(),
            added: vec![],
            removed: vec!["src/lib.rs::foo".into()],
            signature_changed: vec![("src/lib.rs::foo".into(), "fn foo()".into(), "fn foo(x: i32)".into())],
            body_only: vec![],
            renamed: vec![],
        };
        let tx = conn.transaction().unwrap();
        mark_stale_for_structural_diff(&tx, &diff).unwrap();
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
            renamed: vec![],
        };

        let tx = conn.transaction().unwrap();
        mark_stale_for_structural_diff(&tx, &diff).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(
            reason.unwrap(),
            "Signature of symbol 'foo' changed: `fn foo()` → `fn foo(x: i32)`"
        );
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
            renamed: vec![],
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
            renamed: vec![],
        };

        let tx = conn.transaction().unwrap();
        mark_stale_for_structural_diff(&tx, &diff).unwrap();
        tx.commit().unwrap();

        let (stale, reason) = get_stale_info(&conn, obs_id);
        assert!(stale);
        assert_eq!(reason.unwrap(), "Symbol no longer exists in index");
    }

    // --- Task 5 unit tests ---

    // 5.1: extract_backtick_symbol_refs basic cases
    #[test]
    fn backtick_short_name_match() {
        let fqns = vec!["src/auth.rs::authenticate".into()];
        let result = extract_backtick_symbol_refs("The `authenticate` function", &fqns);
        assert_eq!(result, vec!["src/auth.rs::authenticate"]);
    }

    #[test]
    fn backtick_no_backtick_quotes_empty() {
        let fqns = vec!["src/auth.rs::authenticate".into()];
        let result = extract_backtick_symbol_refs("The authenticate function", &fqns);
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_partial_name_no_match() {
        let fqns = vec!["src/auth.rs::authenticate".into()];
        let result = extract_backtick_symbol_refs("The `auth` function", &fqns);
        assert!(result.is_empty());
    }

    // 5.2: full FQN in backticks
    #[test]
    fn backtick_full_fqn_match() {
        let fqns = vec!["src/auth.rs::login".into()];
        let result = extract_backtick_symbol_refs("See `src/auth.rs::login` for details", &fqns);
        assert_eq!(result, vec!["src/auth.rs::login"]);
    }

    // 5.3: token normalization with () stripping
    #[test]
    fn backtick_strip_trailing_parens() {
        let fqns = vec!["src/auth.rs::foo".into(), "src/auth.rs::login".into()];
        // `foo()` → foo, matches short name
        let result = extract_backtick_symbol_refs("Call `foo()` now", &fqns);
        assert_eq!(result, vec!["src/auth.rs::foo"]);
        // `login(user)` → login
        let result = extract_backtick_symbol_refs("Call `login(user)` now", &fqns);
        assert_eq!(result, vec!["src/auth.rs::login"]);
        // Full FQN with ()
        let result = extract_backtick_symbol_refs("See `src/auth.rs::login()` here", &fqns);
        assert_eq!(result, vec!["src/auth.rs::login"]);
    }

    // 5.3b: no dot/generic stripping
    #[test]
    fn backtick_no_dot_stripping() {
        let fqns = vec!["src/ui.rs::render".into()];
        let result = extract_backtick_symbol_refs("Use `obj.render` here", &fqns);
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_no_generic_stripping() {
        let fqns = vec!["src/util.rs::foo".into(), "src/auth.rs::login".into()];
        let result = extract_backtick_symbol_refs("Use `foo<T>` here", &fqns);
        assert!(result.is_empty());
        let result = extract_backtick_symbol_refs("See `src/auth.rs::login<T>`", &fqns);
        assert!(result.is_empty());
    }

    // 5.3c: backtick scanner edge cases
    #[test]
    fn backtick_unmatched_no_close_before_newline() {
        let fqns = vec!["f.rs::foo".into()];
        let result = extract_backtick_symbol_refs("Some `foo\nbar`", &fqns);
        // Unclosed on first line, second line has `bar` but no backtick open
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_empty_span_skipped() {
        let fqns = vec!["f.rs::foo".into()];
        let result = extract_backtick_symbol_refs("Empty `` here", &fqns);
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_no_backticks_at_all() {
        let fqns = vec!["f.rs::foo".into()];
        let result = extract_backtick_symbol_refs("no backticks here", &fqns);
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_crlf_trimmed() {
        let fqns = vec!["f.rs::foo".into()];
        let result = extract_backtick_symbol_refs("See `foo\r`\r\n", &fqns);
        assert_eq!(result, vec!["f.rs::foo"]);
    }

    #[test]
    fn backtick_unbalanced_paren_not_stripped() {
        let fqns = vec!["f.rs::)weird".into()];
        let result = extract_backtick_symbol_refs("See `)weird` here", &fqns);
        assert_eq!(result, vec!["f.rs::)weird"]);
    }

    // 5.3d: multi-backtick skip
    #[test]
    fn backtick_double_backtick_skipped() {
        let fqns = vec!["f.rs::code".into()];
        let result = extract_backtick_symbol_refs("See ``some code`` here", &fqns);
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_triple_backtick_skipped() {
        let fqns = vec!["f.rs::fenced".into()];
        let result = extract_backtick_symbol_refs("See ```fenced``` here", &fqns);
        assert!(result.is_empty());
    }

    #[test]
    fn backtick_double_then_single() {
        let fqns = vec!["f.rs::real".into()];
        let result = extract_backtick_symbol_refs("``code`` then `real`", &fqns);
        assert_eq!(result, vec!["f.rs::real"]);
    }

    // 5.3e: Phase 1 vs Phase 2 provenance
    #[test]
    fn backtick_phase1_wins_over_phase2() {
        let fqns = vec![
            "a.rs::foo".into(),
            "z.rs::foo".into(),
        ];
        // `z.rs::foo` matches Phase 1 (full FQN), `foo` matches Phase 2 for both
        // but z.rs::foo already matched Phase 1, so Phase 2 only adds a.rs::foo
        let result = extract_backtick_symbol_refs("`z.rs::foo` and `foo`", &fqns);
        // Phase 1 first (z.rs::foo), then Phase 2 (a.rs::foo)
        assert_eq!(result, vec!["z.rs::foo", "a.rs::foo"]);
    }

    // 5.3f: deduplication
    #[test]
    fn backtick_dedup_full_fqn_and_short_name() {
        let fqns = vec!["src/auth.rs::login".into()];
        let result = extract_backtick_symbol_refs("`src/auth.rs::login` and `login`", &fqns);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "src/auth.rs::login");
    }

    #[test]
    fn backtick_dedup_same_token_twice() {
        let fqns = vec!["f.rs::foo".into()];
        let result = extract_backtick_symbol_refs("`foo` and `foo`", &fqns);
        assert_eq!(result.len(), 1);
    }

    // 5.8: enhanced stale_reason format (already tested above in
    //       structural_diff_signature_changed_uses_specific_reason)

    // 5.9: has_structural_changes with only renamed
    #[test]
    fn has_structural_changes_renamed_only() {
        use crate::index::diff::StructuralDiff;
        let diff = StructuralDiff {
            file_path: "f.rs".into(),
            added: vec![],
            removed: vec![],
            signature_changed: vec![],
            body_only: vec![],
            renamed: vec![("f.rs::old".into(), "f.rs::new".into())],
        };
        assert!(diff.has_structural_changes());
    }

    // 5.10: duplicate short names
    #[test]
    fn backtick_duplicate_short_names_both_match() {
        let fqns = vec![
            "a.rs::render".into(),
            "b.rs::render".into(),
        ];
        let result = extract_backtick_symbol_refs("The `render` function", &fqns);
        assert_eq!(result.len(), 2);
        // Both appear, sorted alphabetically
        assert_eq!(result[0], "a.rs::render");
        assert_eq!(result[1], "b.rs::render");
    }
}
