use rusqlite::{Connection, Transaction, params, types::ToSql};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Debug)]
pub struct ObservationRow {
    #[allow(dead_code)]
    pub id: i64,
    pub session_id: String,
    #[allow(dead_code)]
    pub created_at: i64,
    pub kind: String,
    pub content: String,
    pub symbol_fqn: Option<String>,
    pub file_path: Option<String>,
    pub is_stale: bool,
    pub stale_reason: Option<String>,
}

pub fn upsert_session(
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

/// Insert an automatically-captured observation with `auto_generated = 1`.
///
/// Passively-captured observations (from hooks) are marked `auto_generated = 1`
/// to distinguish them from manually-saved ones (`auto_generated = 0`).
/// This flag is used by session compression to identify ephemeral observations.
pub fn insert_auto_observation(
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
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
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

/// Layer 3 sensitive-file exclusion for observations (defense-in-depth).
/// KEEP IN SYNC with `graph/query.rs::is_output_sensitive` and `index::is_sensitive`.
pub fn is_sensitive_path(path: &str) -> bool {
    let p = std::path::Path::new(path);
    let file_name = match p.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    if matches!(file_name, ".env" | "id_rsa") {
        return true;
    }
    if file_name.starts_with(".env.") || file_name.starts_with("id_rsa.") {
        return true;
    }
    if let Some(ext) = p.extension().and_then(|e| e.to_str())
        && matches!(ext, "pem" | "key" | "p12")
    {
        return true;
    }
    false
}

#[derive(Debug)]
pub(crate) struct ScoredObservation {
    pub(crate) obs: ObservationRow,
    pub(crate) relevance_score: f64,
}

/// Score observations by recency (90% weight) and staleness (10% weight).
/// Recency uses absolute exponential decay with 7-day half-life: `0.5^(age_days / 7.0)`.
/// Score is request-scoped — not stored in DB.
pub(crate) fn score_observations(observations: Vec<ObservationRow>) -> Vec<ScoredObservation> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as f64;

    observations
        .into_iter()
        .map(|obs| {
            let age_secs = (now_secs - obs.created_at as f64).max(0.0);
            let age_days = age_secs / 86400.0;
            let recency = 0.5_f64.powf(age_days / 7.0);
            let staleness = if obs.is_stale { 0.0 } else { 1.0 };
            let score = (0.90 * recency + 0.10 * staleness).clamp(0.0, 1.0);
            ScoredObservation {
                obs,
                relevance_score: score,
            }
        })
        .collect()
}

pub(crate) fn get_recent_session_ids(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<String>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT s.id FROM sessions s \
         JOIN observations o ON o.session_id = s.id \
         ORDER BY s.started_at DESC, s.rowid DESC \
         LIMIT ?1",
    )?;
    let ids = stmt
        .query_map(params![limit as i64], |r| r.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    Ok(ids)
}

pub(crate) fn get_observations_filtered(
    conn: &Connection,
    session_ids: &[String],
    symbol_fqn: Option<&str>,
    file_path: Option<&str>,
) -> Result<Vec<ObservationRow>, StoreError> {
    if session_ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders: Vec<String> = (1..=session_ids.len()).map(|i| format!("?{i}")).collect();
    let mut sql = format!(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, is_stale, stale_reason \
         FROM observations WHERE session_id IN ({}) ",
        placeholders.join(", ")
    );

    let mut param_idx = session_ids.len() + 1;
    if symbol_fqn.is_some() {
        sql.push_str(&format!("AND symbol_fqn = ?{param_idx} "));
        param_idx += 1;
    }
    if file_path.is_some() {
        sql.push_str(&format!("AND file_path = ?{param_idx} "));
        param_idx += 1;
    }
    // Cap SQL fetch at 800 rows (4x the 200 display cap) to bound DB scan
    // while leaving headroom for sensitive-path filtering in Rust.
    sql.push_str(&format!("ORDER BY created_at DESC, id DESC LIMIT ?{param_idx}"));

    let mut dynamic_params: Vec<Box<dyn ToSql>> = session_ids
        .iter()
        .map(|s| Box::new(s.clone()) as Box<dyn ToSql>)
        .collect();
    if let Some(fqn) = symbol_fqn {
        dynamic_params.push(Box::new(fqn.to_string()));
    }
    if let Some(fp) = file_path {
        dynamic_params.push(Box::new(fp.to_string()));
    }
    dynamic_params.push(Box::new(800i64));

    let param_refs: Vec<&dyn ToSql> = dynamic_params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(param_refs.as_slice(), |r| {
            Ok(ObservationRow {
                id: r.get(0)?,
                session_id: r.get(1)?,
                created_at: r.get(2)?,
                kind: r.get(3)?,
                content: r.get(4)?,
                symbol_fqn: r.get(5)?,
                file_path: r.get(6)?,
                is_stale: r.get::<_, i64>(7)? != 0,
                stale_reason: r.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows.into_iter().filter(|r| {
        r.file_path.as_deref().is_none_or(|p| !is_sensitive_path(p))
    }).collect())
}

pub(crate) fn get_observations_for_context(
    conn: &Connection,
    symbol_fqns: &[&str],
    file_paths: &[&str],
    limit: usize,
) -> Result<Vec<ObservationRow>, StoreError> {
    if symbol_fqns.is_empty() && file_paths.is_empty() {
        return Ok(Vec::new());
    }

    let mut conditions = Vec::new();
    let mut dynamic_params: Vec<Box<dyn ToSql>> = Vec::new();
    let mut idx = 1usize;

    if !symbol_fqns.is_empty() {
        let phs: Vec<String> = symbol_fqns.iter().map(|_| { let p = format!("?{idx}"); idx += 1; p }).collect();
        conditions.push(format!("symbol_fqn IN ({})", phs.join(", ")));
        for fqn in symbol_fqns {
            dynamic_params.push(Box::new(fqn.to_string()));
        }
    }

    if !file_paths.is_empty() {
        let phs: Vec<String> = file_paths.iter().map(|_| { let p = format!("?{idx}"); idx += 1; p }).collect();
        conditions.push(format!("file_path IN ({})", phs.join(", ")));
        for fp in file_paths {
            dynamic_params.push(Box::new(fp.to_string()));
        }
    }

    // Over-fetch 4x limit from SQL to allow headroom for sensitive-path filtering
    // in Rust, while still bounding the DB scan for large observation tables.
    let sql_limit = limit.saturating_mul(4).max(limit);
    let limit_ph = format!("?{idx}");
    dynamic_params.push(Box::new(sql_limit as i64));

    let sql = format!(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, is_stale, stale_reason \
         FROM observations WHERE ({}) ORDER BY created_at DESC, id DESC LIMIT {}",
        conditions.join(" OR "),
        limit_ph,
    );

    let param_refs: Vec<&dyn ToSql> = dynamic_params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(param_refs.as_slice(), |r| {
            Ok(ObservationRow {
                id: r.get(0)?,
                session_id: r.get(1)?,
                created_at: r.get(2)?,
                kind: r.get(3)?,
                content: r.get(4)?,
                symbol_fqn: r.get(5)?,
                file_path: r.get(6)?,
                is_stale: r.get::<_, i64>(7)? != 0,
                stale_reason: r.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Filter sensitive paths in Rust after fetch, then apply final limit.
    Ok(rows.into_iter()
        .filter(|r| r.file_path.as_deref().is_none_or(|p| !is_sensitive_path(p)))
        .take(limit)
        .collect())
}

pub(crate) fn get_scored_observations_for_context(
    conn: &Connection,
    symbol_fqns: &[&str],
    file_paths: &[&str],
    limit: usize,
) -> Result<Vec<ScoredObservation>, StoreError> {
    let observations = get_observations_for_context(conn, symbol_fqns, file_paths, limit)?;
    let mut scored = score_observations(observations);
    scored.sort_by(|a, b| b.relevance_score.total_cmp(&a.relevance_score));
    Ok(scored)
}

#[derive(Debug)]
pub struct SessionSummary {
    pub session_id: String,
    pub started_at: i64,
    pub observation_count: u32,
    pub compressed: bool,
}

/// Delete ephemeral observations (tool_call, file_change) from a session within
/// the caller-provided transaction, then mark session compressed.
/// Returns count of deleted observations. Idempotent: returns Ok(0) if already compressed.
pub(crate) fn compress_session(tx: &Transaction, session_id: &str) -> Result<u64, StoreError> {
    // Guard: if already compressed, no-op
    let compressed: i64 = tx.query_row(
        "SELECT compressed FROM sessions WHERE id = ?1",
        params![session_id],
        |r| r.get(0),
    )?;
    if compressed != 0 {
        return Ok(0);
    }

    let deleted = tx.execute(
        "DELETE FROM observations WHERE session_id = ?1 AND kind IN ('tool_call', 'file_change')",
        params![session_id],
    )?;
    tx.execute(
        "UPDATE sessions SET compressed = 1 WHERE id = ?1",
        params![session_id],
    )?;
    Ok(deleted as u64)
}

/// Mark a session as ended by setting ended_at to now. Idempotent: WHERE clause prevents
/// overwrite if ended_at is already set.
pub fn mark_session_ended(conn: &Connection, session_id: &str) -> Result<(), StoreError> {
    conn.execute(
        "UPDATE sessions SET ended_at = ?1 WHERE id = ?2 AND ended_at IS NULL",
        params![now_secs(), session_id],
    )?;
    Ok(())
}

/// Check whether a session has already been compressed.
/// Returns Ok(false) if session row not found (safe default for upsert-before-check flow).
#[cfg(test)]
pub(crate) fn is_session_compressed(conn: &Connection, session_id: &str) -> Result<bool, StoreError> {
    match conn.query_row(
        "SELECT compressed FROM sessions WHERE id = ?1",
        params![session_id],
        |r| r.get::<_, i64>(0),
    ) {
        Ok(v) => Ok(v != 0),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
        Err(e) => Err(StoreError::Sqlite(e)),
    }
}

/// Compress a specific session unconditionally (not age-based). Creates a transaction,
/// calls compress_session, then commits. Returns count of deleted observations.
#[cfg(test)]
pub(crate) fn compress_specific_session(
    conn: &mut Connection,
    session_id: &str,
) -> Result<u64, StoreError> {
    let tx = conn.transaction()?;
    let deleted = compress_session(&tx, session_id)?;
    tx.commit()?;
    Ok(deleted)
}

/// Find sessions that have ended and are stale beyond the threshold, then compress each.
/// Returns list of compressed session IDs. Active sessions (ended_at IS NULL) are never compressed.
pub(crate) fn compress_stale_sessions(
    conn: &mut Connection,
    threshold_secs: i64,
) -> Result<Vec<String>, StoreError> {
    let cutoff = now_secs() - threshold_secs;
    let session_ids: Vec<String> = {
        let mut stmt = conn.prepare(
            "SELECT id FROM sessions WHERE ended_at IS NOT NULL AND ended_at < ?1 AND compressed = 0",
        )?;
        stmt.query_map(params![cutoff], |r| r.get(0))?
            .collect::<Result<Vec<String>, _>>()?
    };

    let mut compressed_ids = Vec::new();
    for sid in &session_ids {
        let tx = conn.transaction()?;
        compress_session(&tx, sid)?;
        tx.commit()?;
        compressed_ids.push(sid.clone());
    }
    Ok(compressed_ids)
}

/// List recent sessions with observation counts.
/// LEFT JOIN ensures zero-observation sessions appear with obs_count = 0.
pub fn list_sessions(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<SessionSummary>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT s.id, s.started_at, s.compressed, COUNT(o.id) AS obs_count \
         FROM sessions s LEFT JOIN observations o ON o.session_id = s.id \
         GROUP BY s.id ORDER BY s.started_at DESC, s.rowid DESC LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(params![limit as i64], |r| {
            Ok(SessionSummary {
                session_id: r.get(0)?,
                started_at: r.get(1)?,
                compressed: r.get::<_, i64>(2)? != 0,
                observation_count: r.get::<_, i64>(3)? as u32,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Get all observations for a session, filtering sensitive paths.
/// Returns None if session does not exist, Some(vec) if it does (may be empty for compressed sessions).
pub fn get_session_observations(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<Vec<ObservationRow>>, StoreError> {
    // Check session exists — distinguish "no rows" from real DB errors
    let exists = match conn.query_row(
        "SELECT 1 FROM sessions WHERE id = ?1",
        params![session_id],
        |_| Ok(true),
    ) {
        Ok(_) => true,
        Err(rusqlite::Error::QueryReturnedNoRows) => false,
        Err(e) => return Err(StoreError::Sqlite(e)),
    };
    if !exists {
        return Ok(None);
    }

    let mut stmt = conn.prepare(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, is_stale, stale_reason \
         FROM observations WHERE session_id = ?1 ORDER BY created_at ASC, id ASC",
    )?;
    let rows = stmt
        .query_map(params![session_id], |r| {
            Ok(ObservationRow {
                id: r.get(0)?,
                session_id: r.get(1)?,
                created_at: r.get(2)?,
                kind: r.get(3)?,
                content: r.get(4)?,
                symbol_fqn: r.get(5)?,
                file_path: r.get(6)?,
                is_stale: r.get::<_, i64>(7)? != 0,
                stale_reason: r.get(8)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(
        rows.into_iter()
            .filter(|r| {
                r.file_path.as_deref().is_none_or(|p| !is_sensitive_path(p))
                    && r.symbol_fqn.as_deref().is_none_or(|fqn| {
                        // Extract file component from FQN (e.g., ".env::DB_PASSWORD" → ".env")
                        fqn.split("::").next().is_none_or(|f| !is_sensitive_path(f))
                    })
            })
            .collect(),
    ))
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

    // ─── Story 4.1 Unit Tests ────────────────────────────────────────────────────

    #[test]
    fn test_insert_auto_observation_has_auto_generated_1() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "auto-sess", "test-agent").unwrap();
        let id = insert_auto_observation(
            &conn,
            "auto-sess",
            "file_change",
            "Edited src/main.rs: replaced 10 chars",
            None,
            Some("src/main.rs"),
        )
        .unwrap();
        assert!(id > 0);
        let auto_generated: i64 = conn
            .query_row(
                "SELECT auto_generated FROM observations WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(auto_generated, 1, "auto_generated must be 1 for passively-captured observations");
    }

    #[test]
    fn test_insert_observation_has_auto_generated_0() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "manual-sess", "test-agent").unwrap();
        let id = insert_observation(
            &conn,
            "manual-sess",
            "insight",
            "Manual observation",
            None,
            Some("src/a.rs"),
        )
        .unwrap();
        let auto_generated: i64 = conn
            .query_row(
                "SELECT auto_generated FROM observations WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(auto_generated, 0, "existing insert_observation must still produce auto_generated=0");
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

    // ─── Story 3.2 Unit Tests ────────────────────────────────────────────────────

    #[test]
    fn test_get_recent_session_ids_ordered_and_limited() {
        let (conn, _dir) = open_test_db();
        // Create 3 sessions with distinct started_at
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('s1', 100, 'a')", []).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('s2', 200, 'a')", []).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('s3', 300, 'a')", []).unwrap();
        // Add observations to each
        for sid in &["s1", "s2", "s3"] {
            insert_observation(&conn, sid, "insight", "test", Some("f::x"), None).unwrap();
        }
        let ids = get_recent_session_ids(&conn, 2).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], "s3"); // most recent first
        assert_eq!(ids[1], "s2");
    }

    #[test]
    fn test_get_recent_session_ids_skips_empty_sessions() {
        let (conn, _dir) = open_test_db();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('empty', 500, 'a')", []).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('has-obs', 400, 'a')", []).unwrap();
        insert_observation(&conn, "has-obs", "insight", "test", Some("f::x"), None).unwrap();

        let ids = get_recent_session_ids(&conn, 10).unwrap();
        assert_eq!(ids, vec!["has-obs"], "empty session must be excluded");
    }

    #[test]
    fn test_get_recent_session_ids_deterministic_same_timestamp() {
        let (conn, _dir) = open_test_db();
        // Two sessions with identical started_at — rowid tiebreaker
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('first', 100, 'a')", []).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('second', 100, 'a')", []).unwrap();
        insert_observation(&conn, "first", "insight", "a", Some("f::x"), None).unwrap();
        insert_observation(&conn, "second", "insight", "b", Some("f::x"), None).unwrap();

        // Run twice to confirm determinism
        let ids1 = get_recent_session_ids(&conn, 10).unwrap();
        let ids2 = get_recent_session_ids(&conn, 10).unwrap();
        assert_eq!(ids1, ids2, "ordering must be deterministic");
        assert_eq!(ids1[0], "second", "higher rowid must come first");
    }

    #[test]
    fn test_get_observations_filtered_by_symbol_fqn() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "about foo", Some("f::foo"), None).unwrap();
        insert_observation(&conn, "s1", "decision", "about bar", Some("f::bar"), None).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], Some("f::foo"), None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol_fqn.as_deref(), Some("f::foo"));
    }

    #[test]
    fn test_get_observations_filtered_by_file_path() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "about src", None, Some("src/a.rs")).unwrap();
        insert_observation(&conn, "s1", "insight", "about lib", None, Some("src/b.rs")).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, Some("src/a.rs")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_path.as_deref(), Some("src/a.rs"));
    }

    #[test]
    fn test_get_observations_filtered_no_filter_returns_all() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "one", Some("f::a"), None).unwrap();
        insert_observation(&conn, "s1", "decision", "two", None, Some("src/b.rs")).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_get_observations_for_context_matches_any() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "by fqn", Some("f::foo"), None).unwrap();
        insert_observation(&conn, "s1", "insight", "by path", None, Some("src/a.rs")).unwrap();
        insert_observation(&conn, "s1", "insight", "unrelated", Some("f::bar"), Some("src/z.rs")).unwrap();

        let rows = get_observations_for_context(&conn, &["f::foo"], &["src/a.rs"], 50).unwrap();
        assert_eq!(rows.len(), 2);
        let contents: Vec<&str> = rows.iter().map(|r| r.content.as_str()).collect();
        assert!(contents.contains(&"by fqn"));
        assert!(contents.contains(&"by path"));
    }

    #[test]
    fn test_get_observations_filtered_excludes_sensitive_paths() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "safe", None, Some("src/a.rs")).unwrap();
        insert_observation(&conn, "s1", "insight", "secret", None, Some(".env")).unwrap();
        insert_observation(&conn, "s1", "insight", "key", None, Some("certs/server.pem")).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "safe");
    }

    #[test]
    fn test_is_sensitive_path() {
        assert!(is_sensitive_path(".env"));
        assert!(is_sensitive_path(".env.local"));
        assert!(is_sensitive_path("id_rsa"));
        assert!(is_sensitive_path("id_rsa.pub"));
        assert!(is_sensitive_path("certs/server.pem"));
        assert!(is_sensitive_path("keys/my.key"));
        assert!(is_sensitive_path("store.p12"));
        assert!(!is_sensitive_path("src/main.rs"));
        assert!(!is_sensitive_path("config.toml"));
    }

    // ─── Story 3.4 Unit Tests ────────────────────────────────────────────────────

    #[test]
    fn test_list_sessions_returns_correct_counts_and_compressed() {
        let (conn, _dir) = open_test_db();
        conn.execute(
            "INSERT INTO sessions (id, started_at, agent, compressed) VALUES ('s1', 100, 'a', 0)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, started_at, agent, compressed) VALUES ('s2', 200, 'a', 1)",
            [],
        ).unwrap();
        insert_observation(&conn, "s1", "insight", "obs1", Some("f::x"), None).unwrap();
        insert_observation(&conn, "s1", "insight", "obs2", Some("f::y"), None).unwrap();
        insert_observation(&conn, "s2", "decision", "obs3", None, Some("src/a.rs")).unwrap();

        let sessions = list_sessions(&conn, 10).unwrap();
        assert_eq!(sessions.len(), 2);
        // Most recent first (s2 started_at=200)
        assert_eq!(sessions[0].session_id, "s2");
        assert_eq!(sessions[0].observation_count, 1);
        assert!(sessions[0].compressed);
        assert_eq!(sessions[1].session_id, "s1");
        assert_eq!(sessions[1].observation_count, 2);
        assert!(!sessions[1].compressed);
    }

    #[test]
    fn test_list_sessions_zero_observation_sessions() {
        let (conn, _dir) = open_test_db();
        conn.execute(
            "INSERT INTO sessions (id, started_at, agent) VALUES ('empty', 100, 'a')",
            [],
        ).unwrap();

        let sessions = list_sessions(&conn, 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_id, "empty");
        assert_eq!(sessions[0].observation_count, 0);
    }

    #[test]
    fn test_get_session_observations_valid_session() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "finding", Some("f::x"), None).unwrap();
        insert_observation(&conn, "s1", "decision", "chose A", None, Some("src/a.rs")).unwrap();

        let obs = get_session_observations(&conn, "s1").unwrap();
        assert!(obs.is_some());
        let obs = obs.unwrap();
        assert_eq!(obs.len(), 2);
    }

    #[test]
    fn test_get_session_observations_invalid_session() {
        let (conn, _dir) = open_test_db();
        let obs = get_session_observations(&conn, "nonexistent").unwrap();
        assert!(obs.is_none());
    }

    #[test]
    fn test_get_session_observations_filters_sensitive_paths() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "safe obs", None, Some("src/main.rs")).unwrap();
        insert_observation(&conn, "s1", "insight", "secret obs", None, Some(".env")).unwrap();
        insert_observation(&conn, "s1", "insight", "key obs", None, Some("certs/server.pem")).unwrap();

        let obs = get_session_observations(&conn, "s1").unwrap().unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].content, "safe obs");
    }

    // ─── Story 4.2 Unit Tests ────────────────────────────────────────────────────

    // 2.4: mark_session_ended — calling twice does not error; ended_at set once
    #[test]
    fn test_mark_session_ended_idempotent() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();

        // ended_at should be NULL initially
        let ended_at_before: Option<i64> = conn
            .query_row("SELECT ended_at FROM sessions WHERE id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert!(ended_at_before.is_none());

        mark_session_ended(&conn, "s1").unwrap();

        let ended_at_first: Option<i64> = conn
            .query_row("SELECT ended_at FROM sessions WHERE id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert!(ended_at_first.is_some(), "ended_at must be set after first call");

        // Second call must not error and must not change ended_at
        mark_session_ended(&conn, "s1").unwrap();
        let ended_at_second: Option<i64> = conn
            .query_row("SELECT ended_at FROM sessions WHERE id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            ended_at_first, ended_at_second,
            "ended_at must not change on second call"
        );
    }

    // 2.5: is_session_compressed returns false before compress, true after
    #[test]
    fn test_is_session_compressed_before_and_after() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.db");
        let mut conn = crate::db::open(&db_path).unwrap();
        upsert_session(&conn, "s1", "a").unwrap();

        let before = is_session_compressed(&conn, "s1").unwrap();
        assert!(!before, "new session must not be compressed");

        compress_specific_session(&mut conn, "s1").unwrap();

        let after = is_session_compressed(&conn, "s1").unwrap();
        assert!(after, "session must be marked compressed after compress_specific_session");
    }

    // 2.6: compress_specific_session — ephemeral obs deleted, insight retained, compressed=1
    #[test]
    fn test_compress_specific_session_deletes_ephemeral_retains_insight() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "keep me", None, None).unwrap();
        insert_auto_observation(&conn, "s1", "tool_call", "delete me", None, None).unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "delete me too", None, Some("src/a.rs")).unwrap();

        let deleted = compress_specific_session(&mut conn, "s1").unwrap();
        assert_eq!(deleted, 2, "two ephemeral obs must be deleted");

        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM observations WHERE session_id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 1, "only insight must remain");

        let kind: String = conn
            .query_row("SELECT kind FROM observations WHERE session_id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(kind, "insight");

        let compressed: i64 = conn
            .query_row("SELECT compressed FROM sessions WHERE id = 's1'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(compressed, 1, "session must be marked compressed");
    }

    // ─── Story 8.1 Unit Tests ────────────────────────────────────────────────────

    fn make_obs(created_at: i64, is_stale: bool, stale_reason: Option<&str>) -> ObservationRow {
        ObservationRow {
            id: 1,
            session_id: "s1".into(),
            created_at,
            kind: "insight".into(),
            content: "test".into(),
            symbol_fqn: None,
            file_path: None,
            is_stale,
            stale_reason: stale_reason.map(|s| s.to_string()),
        }
    }

    fn now_epoch() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn test_score_recency_ordering() {
        let now = now_epoch();
        let recent = make_obs(now - 3600, false, None);       // 1 hour old
        let old = make_obs(now - 14 * 86400, false, None);    // 14 days old

        let scored = score_observations(vec![recent, old]);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "1-hour-old ({:.4}) must score higher than 14-day-old ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_score_staleness_penalty() {
        let now = now_epoch();
        let fresh = make_obs(now - 86400, false, None);
        let stale = make_obs(now - 86400, true, Some("source changed"));

        let scored = score_observations(vec![fresh, stale]);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "non-stale ({:.4}) must score higher than same-age stale ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_score_absolute_decay_stability() {
        let now = now_epoch();
        let obs1 = make_obs(now - 3 * 86400, false, None);
        let obs2 = make_obs(now - 3 * 86400, false, None);

        // Score in two different result sets
        let scored_alone = score_observations(vec![obs1]);
        let scored_with_others = score_observations(vec![
            make_obs(now - 100, false, None),
            obs2,
            make_obs(now - 30 * 86400, false, None),
        ]);

        let diff = (scored_alone[0].relevance_score - scored_with_others[1].relevance_score).abs();
        assert!(diff < 0.01, "same observation in different sets must produce similar score (diff={:.4})", diff);
    }

    #[test]
    fn test_score_clamping() {
        let now = now_epoch();
        let zero_age = make_obs(now, false, None);
        let very_old = make_obs(0, false, None); // epoch

        let scored = score_observations(vec![zero_age, very_old]);
        for so in &scored {
            assert!(so.relevance_score >= 0.0, "score must be >= 0.0");
            assert!(so.relevance_score <= 1.0, "score must be <= 1.0");
        }
    }

    #[test]
    fn test_score_7day_half_life() {
        let now = now_epoch();
        let seven_days = make_obs(now - 7 * 86400, false, None);

        let scored = score_observations(vec![seven_days]);
        // Recency at 7 days = 0.5, staleness = 1.0 (non-stale)
        // Expected: 0.90 * 0.5 + 0.10 * 1.0 = 0.55
        let expected = 0.55;
        let diff = (scored[0].relevance_score - expected).abs();
        assert!(
            diff < 0.02,
            "7-day-old observation should score ~{:.2} but got {:.4} (diff={:.4})",
            expected, scored[0].relevance_score, diff
        );
    }
}
