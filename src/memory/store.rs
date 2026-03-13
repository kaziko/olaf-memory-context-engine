use rusqlite::{Connection, Transaction, params, types::ToSql};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// Retention-policy tier for observations. Controls recency decay half-life,
/// compression protection, and purge protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Importance {
    Critical,
    High,
    Medium,
    Low,
}

impl Importance {
    /// Default importance based on observation kind.
    /// Used by both manual (`save_observation`) and auto (`insert_auto_observation`) paths.
    pub fn default_for_kind(kind: &str) -> Self {
        match kind {
            "decision" => Importance::High,
            "anti_pattern" => Importance::Medium,
            "file_change" | "tool_call" | "context_retrieval" => Importance::Low,
            _ => Importance::Medium,
        }
    }

    /// Half-life in days for recency decay scoring.
    pub fn half_life_days(&self) -> f64 {
        match self {
            Importance::Critical => 365.0,
            Importance::High => 14.0,
            Importance::Medium => 7.0,
            Importance::Low => 3.5,
        }
    }
}

impl std::fmt::Display for Importance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Importance::Critical => write!(f, "critical"),
            Importance::High => write!(f, "high"),
            Importance::Medium => write!(f, "medium"),
            Importance::Low => write!(f, "low"),
        }
    }
}

impl std::str::FromStr for Importance {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "critical" => Ok(Importance::Critical),
            "high" => Ok(Importance::High),
            "medium" => Ok(Importance::Medium),
            "low" => Ok(Importance::Low),
            _ => Err(format!("invalid importance '{s}'; must be one of: critical, high, medium, low")),
        }
    }
}

impl rusqlite::types::FromSql for Importance {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let s = value.as_str()?;
        s.parse::<Importance>().map_err(|e| rusqlite::types::FromSqlError::Other(Box::new(
            std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        )))
    }
}

impl rusqlite::types::ToSql for Importance {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(rusqlite::types::ToSqlOutput::from(self.to_string()))
    }
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
    pub confidence: Option<f64>,
    pub branch: Option<String>,
    pub importance: Importance,
}

/// Branch scope for memory health queries. Callers resolve the branch name
/// before calling store code.
#[derive(Debug, Clone)]
pub enum ResolvedBranchScope {
    Branch(String),
    All,
}

/// Observation-level metrics for the memory health report.
#[derive(Debug, Default)]
pub struct ObservationMetrics {
    pub total: u64,
    pub stale: u64,
    pub by_kind: Vec<(String, u64)>,
    pub by_importance: [u64; 4], // [critical, high, medium, low]
    pub scope_symbol_only: u64,
    pub scope_file_only: u64,
    pub scope_both: u64,
    pub scope_project: u64,
    pub retrieval_traffic: u64,
    pub noise_count: u64,
    pub has_recent_activity: bool,
    pub oldest_days: Option<u64>,
    pub oldest_non_stale_days: Option<u64>,
}

/// Rule-level metrics for the memory health report.
#[derive(Debug, Default)]
pub struct RuleMetrics {
    pub active: u64,
    pub pending: u64,
    pub stale: u64,
}

/// Session-level metrics (always global — sessions have no branch column).
#[derive(Debug, Default)]
pub struct SessionMetrics {
    pub total: u64,
    pub compressed: u64,
}

/// Full memory health diagnostic report.
#[derive(Debug)]
pub struct MemoryHealthReport {
    pub branch_label: String,
    pub observations: ObservationMetrics,
    pub rules: RuleMetrics,
    pub sessions: SessionMetrics,
    pub recommendations: Vec<String>,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_observation(
    conn: &Connection,
    session_id: &str,
    kind: &str,
    content: &str,
    symbol_fqn: Option<&str>,
    file_path: Option<&str>,
    branch: Option<&str>,
    importance: Importance,
) -> Result<i64, StoreError> {
    conn.execute(
        "INSERT INTO observations \
         (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated, branch, importance) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8)",
        params![session_id, now_secs(), kind, content, symbol_fqn, file_path, branch, importance],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Insert an automatically-captured observation with `auto_generated = 1`.
///
/// Passively-captured observations (from hooks) are marked `auto_generated = 1`
/// to distinguish them from manually-saved ones (`auto_generated = 0`).
/// This flag is used by session compression to identify ephemeral observations.
/// Confidence is computed after insertion by correlating with nearby tool_call
/// and structural file_change observations within a 120-second window.
pub fn insert_auto_observation(
    conn: &Connection,
    session_id: &str,
    kind: &str,
    content: &str,
    symbol_fqn: Option<&str>,
    file_path: Option<&str>,
    branch: Option<&str>,
) -> Result<i64, StoreError> {
    let ts = now_secs();
    // Unknown auto-captured kinds default to Low (safe for future kinds),
    // unlike manual kinds which default to Medium via default_for_kind catch-all.
    let importance = match Importance::default_for_kind(kind) {
        Importance::Medium if !matches!(kind, "anti_pattern" | "decision" | "insight" | "error") => Importance::Low,
        other => other,
    };
    conn.execute(
        "INSERT INTO observations \
         (session_id, created_at, kind, content, symbol_fqn, file_path, auto_generated, branch, importance) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8)",
        params![session_id, ts, kind, content, symbol_fqn, file_path, branch, importance],
    )?;
    let id = conn.last_insert_rowid();
    let confidence = compute_confidence(conn, session_id, ts, id);
    conn.execute(
        "UPDATE observations SET confidence = ?1 WHERE id = ?2",
        params![confidence, id],
    )?;
    Ok(id)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Compute a confidence score [0.0, 1.0] for an auto-generated observation by correlating
/// it with nearby tool_call and structural file_change observations within 120s.
/// - Base: 0.5
/// - +0.2 if any `tool_call` within 120s (excluding `exclude_id`)
/// - +0.1 if any structural `file_change` within 120s (added/removed/signature/renamed/moved)
pub(crate) fn compute_confidence(
    conn: &Connection,
    session_id: &str,
    created_at: i64,
    exclude_id: i64,
) -> f64 {
    let window_start = created_at - 120;
    let window_end = created_at + 120;

    let has_tool_call: bool = conn.query_row(
        "SELECT COUNT(*) FROM observations \
         WHERE session_id = ?1 AND kind = 'tool_call' AND auto_generated = 1 \
         AND created_at BETWEEN ?2 AND ?3 AND id != ?4",
        rusqlite::params![session_id, window_start, window_end, exclude_id],
        |r| r.get::<_, i64>(0),
    ).unwrap_or(0) > 0;

    let has_structural_change: bool = conn.query_row(
        "SELECT COUNT(*) FROM observations \
         WHERE session_id = ?1 AND kind = 'file_change' AND auto_generated = 1 \
         AND created_at BETWEEN ?2 AND ?3 AND id != ?4 \
         AND (content LIKE '%added %' OR content LIKE '%removed %' OR content LIKE '%signature of %' \
              OR content LIKE '%renamed %' OR content LIKE '%moved %')",
        rusqlite::params![session_id, window_start, window_end, exclude_id],
        |r| r.get::<_, i64>(0),
    ).unwrap_or(0) > 0;

    let mut score = 0.5_f64;
    if has_tool_call { score += 0.2; }
    if has_structural_change { score += 0.1; }
    score.clamp(0.0, 1.0)
}

pub use crate::sensitive::is_sensitive as is_sensitive_path;

#[derive(Debug)]
pub(crate) struct ScoredObservation {
    pub(crate) obs: ObservationRow,
    pub(crate) relevance_score: f64,
    pub(crate) primary_signal: String,
    /// Weighted contributions per signal: bm25, semantic, recency, confidence, staleness.
    /// Values are weight × raw component value; they sum to the final score.
    pub(crate) score_breakdown: ScoreBreakdown,
}

/// Weighted signal contributions. Each field = weight × raw component value.
#[derive(Debug, Clone, Default)]
pub(crate) struct ScoreBreakdown {
    pub(crate) bm25: f64,
    pub(crate) semantic: f64,
    pub(crate) recency: f64,
    pub(crate) confidence: f64,
    pub(crate) staleness: f64,
}

impl ScoreBreakdown {
    /// Format as compact annotation for context-memory output.
    pub(crate) fn format_compact(&self) -> String {
        let mut parts = Vec::new();
        if self.bm25 > 0.001 { parts.push(format!("bm25={:.2}", self.bm25)); }
        if self.semantic > 0.001 { parts.push(format!("sem={:.2}", self.semantic)); }
        if self.recency > 0.001 { parts.push(format!("rec={:.2}", self.recency)); }
        if self.confidence > 0.001 { parts.push(format!("conf={:.2}", self.confidence)); }
        if self.staleness < -0.001 { parts.push(format!("stale={:.2}", self.staleness)); }
        if parts.is_empty() { return String::new(); }
        format!("[{}]", parts.join(" "))
    }
}

// ── Weight constants: tuning candidates ──

// Weights WITHOUT semantic signal (current behavior)
const W_BM25: f64 = 0.35;
const W_RECENCY: f64 = 0.40;
const W_CONFIDENCE: f64 = 0.15;

// Weights WITH semantic signal
const W_BM25_SEM: f64 = 0.20;
const W_SEMANTIC: f64 = 0.25;
const W_RECENCY_SEM: f64 = 0.35;
const W_CONFIDENCE_SEM: f64 = 0.10;

const STALENESS_PENALTY: f64 = -0.30;

/// Score observations using composite relevance. When `semantic_scores` is Some,
/// uses rebalanced weights (BM25 20%, semantic 25%, recency 35%, confidence 10%).
/// When None, uses original weights (BM25 35%, recency 40%, confidence 15%).
/// Staleness penalty (-0.30) unchanged in both modes.
pub(crate) fn score_observations(
    conn: &Connection,
    observations: Vec<ObservationRow>,
    query: Option<&str>,
    semantic_scores: Option<&std::collections::HashMap<i64, f64>>,
) -> Vec<ScoredObservation> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as f64;

    let has_semantic = semantic_scores.is_some();
    let (w_bm25, w_recency, w_confidence) = if has_semantic {
        (W_BM25_SEM, W_RECENCY_SEM, W_CONFIDENCE_SEM)
    } else {
        (W_BM25, W_RECENCY, W_CONFIDENCE)
    };

    // Build BM25 scores if query is provided and observations are non-empty
    let bm25_scores: std::collections::HashMap<i64, f64> = match query {
        Some(q) if !q.trim().is_empty() && !observations.is_empty() => {
            compute_bm25_scores(conn, &observations, q)
        }
        _ => std::collections::HashMap::new(),
    };

    // Normalize BM25: raw values are negative (more negative = better match).
    // min_raw is the most-negative (best) score; divide each raw by min_raw → [0,1].
    let min_bm25 = bm25_scores.values().cloned().fold(f64::INFINITY, f64::min);

    observations
        .into_iter()
        .map(|obs| {
            let age_secs = (now_secs - obs.created_at as f64).max(0.0);
            let age_days = age_secs / 86400.0;
            let half_life = obs.importance.half_life_days();
            let recency = 0.5_f64.powf(age_days / half_life);
            let confidence = obs.confidence.unwrap_or(0.5);
            let staleness_val = if obs.is_stale { STALENESS_PENALTY } else { 0.0 };

            let bm25_norm = if min_bm25 < 0.0 {
                bm25_scores.get(&obs.id).map(|raw| raw / min_bm25).unwrap_or(0.0)
            } else {
                0.0
            };

            let semantic_raw = semantic_scores
                .and_then(|m| m.get(&obs.id).copied())
                .unwrap_or(0.0);
            let semantic_contrib = if has_semantic { W_SEMANTIC * semantic_raw } else { 0.0 };

            let bm25_contrib = w_bm25 * bm25_norm;
            let recency_contrib = w_recency * recency;
            let confidence_contrib = w_confidence * confidence;

            let score = (bm25_contrib + semantic_contrib + recency_contrib + confidence_contrib + staleness_val)
                .clamp(0.0, 1.0);

            let primary_signal = if obs.is_stale {
                "stale".to_string()
            } else {
                let contribs = [
                    (bm25_contrib, "fts"),
                    (semantic_contrib, "semantic"),
                    (recency_contrib, "recency"),
                    (confidence_contrib, "confidence"),
                ];
                contribs.iter()
                    .max_by(|a, b| a.0.total_cmp(&b.0))
                    .map(|(_, name)| name.to_string())
                    .unwrap_or_else(|| "recency".to_string())
            };

            let score_breakdown = ScoreBreakdown {
                bm25: bm25_contrib,
                semantic: semantic_contrib,
                recency: recency_contrib,
                confidence: confidence_contrib,
                staleness: staleness_val,
            };

            ScoredObservation {
                obs,
                relevance_score: score,
                primary_signal,
                score_breakdown,
            }
        })
        .collect()
}

/// Run FTS5 BM25 query for the given observations and query string.
/// Returns a map of observation id → raw BM25 score (negative: more negative = better match).
fn compute_bm25_scores(
    conn: &Connection,
    observations: &[ObservationRow],
    query: &str,
) -> std::collections::HashMap<i64, f64> {
    // Build OR-joined FTS5 MATCH query from whitespace-split tokens.
    // Strip FTS5-special characters to prevent syntax errors that would silently disable BM25.
    let tokens: Vec<String> = query
        .split_whitespace()
        .map(|t| {
            t.chars()
                .filter(|c| !matches!(c, '"' | '(' | ')' | ':' | '*' | '^' | '{' | '}' | '[' | ']' | '~' | '!' | '+' | '-'))
                .collect::<String>()
        })
        .filter(|t| !t.is_empty())
        .collect();
    if tokens.is_empty() {
        return std::collections::HashMap::new();
    }
    let fts_query = tokens.join(" OR ");

    // Build JSON array of candidate IDs for constrained FTS search (parameterized)
    let id_json: String = {
        let ids: Vec<String> = observations.iter().map(|o| o.id.to_string()).collect();
        format!("[{}]", ids.join(","))
    };

    let sql = "SELECT rowid, bm25(observations_fts) \
         FROM observations_fts \
         WHERE observations_fts MATCH ?1 AND rowid IN (SELECT value FROM json_each(?2))";

    let mut scores = std::collections::HashMap::new();
    if let Ok(mut stmt) = conn.prepare(sql)
        && let Ok(rows) = stmt.query_map(rusqlite::params![fts_query, id_json], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?))
        })
    {
        for row in rows.flatten() {
            scores.insert(row.0, row.1);
        }
    }
    scores
}

pub(crate) fn get_recent_session_ids(
    conn: &Connection,
    limit: usize,
    branch: Option<&str>,
) -> Result<Vec<String>, StoreError> {
    let ids = match branch {
        Some(b) => {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT s.id FROM sessions s \
                 JOIN observations o ON o.session_id = s.id \
                   AND o.kind != 'context_retrieval' \
                   AND o.consolidated_into IS NULL \
                   AND (o.branch = ?2 OR o.branch IS NULL) \
                 ORDER BY s.started_at DESC, s.rowid DESC \
                 LIMIT ?1",
            )?;
            stmt.query_map(params![limit as i64, b], |r| r.get(0))?
                .collect::<Result<Vec<String>, _>>()?
        }
        None => {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT s.id FROM sessions s \
                 JOIN observations o ON o.session_id = s.id \
                   AND o.kind != 'context_retrieval' \
                   AND o.consolidated_into IS NULL \
                 ORDER BY s.started_at DESC, s.rowid DESC \
                 LIMIT ?1",
            )?;
            stmt.query_map(params![limit as i64], |r| r.get(0))?
                .collect::<Result<Vec<String>, _>>()?
        }
    };
    Ok(ids)
}

pub(crate) fn get_observations_filtered(
    conn: &Connection,
    session_ids: &[String],
    symbol_fqn: Option<&str>,
    file_path: Option<&str>,
    branch: Option<&str>,
    content_policy: &crate::policy::ContentPolicy,
) -> Result<Vec<ObservationRow>, StoreError> {
    if session_ids.is_empty() {
        return Ok(Vec::new());
    }

    let placeholders: Vec<String> = (1..=session_ids.len()).map(|i| format!("?{i}")).collect();
    let mut sql = format!(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, is_stale, stale_reason, confidence, branch, importance \
         FROM observations WHERE session_id IN ({}) AND kind != 'context_retrieval' AND consolidated_into IS NULL ",
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
    if branch.is_some() {
        sql.push_str(&format!("AND (branch = ?{param_idx} OR branch IS NULL) "));
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
    if let Some(b) = branch {
        dynamic_params.push(Box::new(b.to_string()));
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
                confidence: r.get(9)?,
                branch: r.get(10)?,
                importance: r.get(11)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rows.into_iter().filter(|r| {
        r.file_path.as_deref().is_none_or(|p| !is_sensitive_path(p))
            && r.file_path.as_deref().is_none_or(|p| !content_policy.is_denied(p, None))
            && r.symbol_fqn.as_deref().is_none_or(|fqn| {
                !content_policy.is_denied(fqn.split("::").next().unwrap_or(""), Some(fqn))
            })
    }).collect())
}

pub(crate) fn get_observations_for_context(
    conn: &Connection,
    symbol_fqns: &[&str],
    file_paths: &[&str],
    limit: usize,
    branch: Option<&str>,
    content_policy: &crate::policy::ContentPolicy,
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
    idx += 1;
    dynamic_params.push(Box::new(sql_limit as i64));

    let branch_clause = if branch.is_some() {
        format!("AND (branch = ?{idx} OR branch IS NULL) ")
    } else {
        String::new()
    };
    if let Some(b) = branch {
        dynamic_params.push(Box::new(b.to_string()));
    }

    let sql = format!(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, is_stale, stale_reason, confidence, branch, importance \
         FROM observations WHERE ({}) AND kind != 'context_retrieval' AND consolidated_into IS NULL {}ORDER BY created_at DESC, id DESC LIMIT {}",
        conditions.join(" OR "),
        branch_clause,
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
                confidence: r.get(9)?,
                branch: r.get(10)?,
                importance: r.get(11)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Filter sensitive paths in Rust after fetch, then apply final limit.
    Ok(rows.into_iter()
        .filter(|r| {
            r.file_path.as_deref().is_none_or(|p| !is_sensitive_path(p))
                && r.file_path.as_deref().is_none_or(|p| !content_policy.is_denied(p, None))
                && r.symbol_fqn.as_deref().is_none_or(|fqn| {
                    !content_policy.is_denied(fqn.split("::").next().unwrap_or(""), Some(fqn))
                })
        })
        .take(limit)
        .collect())
}

/// Fetch project-scoped observations (no symbol_fqn, no file_path, not consolidated).
pub(crate) fn get_project_scoped_observations(
    conn: &Connection,
    branch: Option<&str>,
    limit: usize,
) -> Result<Vec<ObservationRow>, StoreError> {
    let (branch_clause, branch_param) = if let Some(b) = branch {
        ("AND (branch = ?1 OR branch IS NULL) ".to_string(), Some(b.to_string()))
    } else {
        (String::new(), None)
    };

    let sql = format!(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, \
         is_stale, stale_reason, confidence, branch, importance \
         FROM observations \
         WHERE symbol_fqn IS NULL AND file_path IS NULL \
           AND consolidated_into IS NULL \
           AND kind != 'context_retrieval' \
           {} \
         ORDER BY created_at DESC, id DESC \
         LIMIT {}",
        branch_clause,
        if branch_param.is_some() { "?2" } else { "?1" },
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = if let Some(ref b) = branch_param {
        stmt.query_map(rusqlite::params![b, limit as i64], |r| {
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
                confidence: r.get(9)?,
                branch: r.get(10)?,
                importance: r.get(11)?,
            })
        })?.collect::<Result<Vec<_>, _>>()?
    } else {
        stmt.query_map(rusqlite::params![limit as i64], |r| {
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
                confidence: r.get(9)?,
                branch: r.get(10)?,
                importance: r.get(11)?,
            })
        })?.collect::<Result<Vec<_>, _>>()?
    };

    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn get_scored_observations_for_context(
    conn: &Connection,
    symbol_fqns: &[&str],
    file_paths: &[&str],
    limit: usize,
    intent: Option<&str>,
    branch: Option<&str>,
    content_policy: &crate::policy::ContentPolicy,
    embedder: Option<&dyn super::embedder::EmbedText>,
) -> Result<Vec<ScoredObservation>, StoreError> {
    let observations = get_observations_for_context(conn, symbol_fqns, file_paths, limit, branch, content_policy)?;

    let semantic_scores = compute_semantic_scores(conn, &observations, intent, embedder);

    let mut scored = score_observations(conn, observations, intent, semantic_scores.as_ref());
    scored.sort_by(|a, b| b.relevance_score.total_cmp(&a.relevance_score));
    Ok(scored)
}

/// Compute semantic similarity scores for observations against the intent query.
/// Returns None when embedder is None, intent is missing, or no stored embeddings exist.
fn compute_semantic_scores(
    conn: &Connection,
    observations: &[ObservationRow],
    intent: Option<&str>,
    embedder: Option<&dyn super::embedder::EmbedText>,
) -> Option<std::collections::HashMap<i64, f64>> {
    let embedder = embedder?;
    let intent_query = intent?;
    if intent_query.trim().is_empty() || observations.is_empty() {
        return None;
    }

    let query_vec = embedder.embed_query(intent_query).ok()?;

    let obs_ids: Vec<i64> = observations.iter().map(|o| o.id).collect();
    let stored = super::embedder::load_embeddings(
        conn, &obs_ids, embedder.model_id(), embedder.model_rev(),
    ).ok()?;

    if stored.is_empty() {
        return None;
    }

    let mut scores = std::collections::HashMap::new();
    for (obs_id, embedding) in &stored {
        let sim = super::embedder::cosine_similarity(&query_vec, embedding);
        // Normalize cosine from [-1,1] to [0,1]
        scores.insert(*obs_id, ((sim as f64) + 1.0) / 2.0);
    }
    Some(scores)
}

#[derive(Debug)]
pub struct SessionSummary {
    pub session_id: String,
    pub started_at: i64,
    pub observation_count: u32,
    pub compressed: bool,
}

/// Delete ephemeral observations (tool_call, file_change, context_retrieval) from a session
/// within the caller-provided transaction, then mark session compressed.
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
        "DELETE FROM observations WHERE session_id = ?1 AND kind IN ('tool_call', 'file_change', 'context_retrieval') AND importance NOT IN ('critical', 'high')",
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

/// Purge sessions (and their observations) older than the given threshold.
/// Only affects sessions with ended_at set and older than cutoff.
/// Deletes observations first (FK child), then sessions (FK parent), in a single transaction.
pub(crate) fn purge_old_sessions(conn: &mut Connection, older_than_secs: i64) -> Result<usize, StoreError> {
    let cutoff: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
        - older_than_secs;
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM observations WHERE session_id IN (\
         SELECT id FROM sessions WHERE ended_at IS NOT NULL AND ended_at < ?1 \
         AND id NOT IN (SELECT DISTINCT session_id FROM observations WHERE importance = 'critical'))",
        params![cutoff],
    )?;
    let purged = tx.execute(
        "DELETE FROM sessions WHERE ended_at IS NOT NULL AND ended_at < ?1 \
         AND id NOT IN (SELECT DISTINCT session_id FROM observations WHERE importance = 'critical')",
        params![cutoff],
    )?;
    tx.commit()?;
    Ok(purged)
}

/// List recent sessions with observation counts.
/// LEFT JOIN ensures zero-observation sessions appear with obs_count = 0.
pub fn list_sessions(
    conn: &Connection,
    limit: usize,
) -> Result<Vec<SessionSummary>, StoreError> {
    let mut stmt = conn.prepare(
        "SELECT s.id, s.started_at, s.compressed, COUNT(o.id) AS obs_count \
         FROM sessions s LEFT JOIN observations o ON o.session_id = s.id AND o.kind != 'context_retrieval' AND o.consolidated_into IS NULL \
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
    content_policy: &crate::policy::ContentPolicy,
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
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path, is_stale, stale_reason, confidence, branch, importance \
         FROM observations WHERE session_id = ?1 AND kind != 'context_retrieval' AND consolidated_into IS NULL ORDER BY created_at ASC, id ASC",
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
                confidence: r.get(9)?,
                branch: r.get(10)?,
                importance: r.get(11)?,
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
                    && r.file_path.as_deref().is_none_or(|p| !content_policy.is_denied(p, None))
                    && r.symbol_fqn.as_deref().is_none_or(|fqn| {
                        !content_policy.is_denied(fqn.split("::").next().unwrap_or(""), Some(fqn))
                    })
            })
            .collect(),
    ))
}

/// Build a memory health diagnostic report.
pub fn memory_health_report(
    conn: &Connection,
    scope: &ResolvedBranchScope,
) -> Result<MemoryHealthReport, StoreError> {
    let now = now_secs();
    let seven_days_ago = now - 7 * 86400;

    // Branch clause shared across observation and rule queries
    let (obs_branch_clause, rule_branch_clause, branch_label, branch_param) = match scope {
        ResolvedBranchScope::Branch(b) => (
            "AND (branch = ?1 OR branch IS NULL)".to_string(),
            "AND (branch = ?1 OR branch IS NULL)".to_string(),
            format!("branch: {b}"),
            Some(b.clone()),
        ),
        ResolvedBranchScope::All => (
            String::new(),
            String::new(),
            "all branches".to_string(),
            None,
        ),
    };

    // ── Observation aggregate query ──
    let obs_agg_sql = format!(
        "SELECT \
            SUM(CASE WHEN kind != 'context_retrieval' THEN 1 ELSE 0 END) AS total, \
            SUM(CASE WHEN kind != 'context_retrieval' AND is_stale = 1 THEN 1 ELSE 0 END) AS stale_count, \
            SUM(CASE WHEN kind = 'context_retrieval' THEN 1 ELSE 0 END) AS retrieval_count, \
            SUM(CASE WHEN kind IN ('tool_call','file_change','context_retrieval') THEN 1 ELSE 0 END) AS noise_count, \
            SUM(CASE WHEN importance = 'critical' AND kind != 'context_retrieval' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN importance = 'high' AND kind != 'context_retrieval' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN importance = 'medium' AND kind != 'context_retrieval' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN importance = 'low' AND kind != 'context_retrieval' THEN 1 ELSE 0 END), \
            SUM(CASE WHEN symbol_fqn IS NOT NULL AND file_path IS NULL AND kind != 'context_retrieval' THEN 1 ELSE 0 END) AS symbol_only, \
            SUM(CASE WHEN symbol_fqn IS NULL AND file_path IS NOT NULL AND kind != 'context_retrieval' THEN 1 ELSE 0 END) AS file_only, \
            SUM(CASE WHEN symbol_fqn IS NOT NULL AND file_path IS NOT NULL AND kind != 'context_retrieval' THEN 1 ELSE 0 END) AS both_scope, \
            SUM(CASE WHEN symbol_fqn IS NULL AND file_path IS NULL AND kind != 'context_retrieval' THEN 1 ELSE 0 END) AS project_scope, \
            MIN(CASE WHEN kind != 'context_retrieval' THEN created_at ELSE NULL END) AS oldest_ts, \
            MIN(CASE WHEN is_stale = 0 AND kind != 'context_retrieval' THEN created_at ELSE NULL END) AS oldest_non_stale_ts, \
            MAX(CASE WHEN kind != 'context_retrieval' AND created_at >= ?{p} THEN 1 ELSE 0 END) AS has_recent \
         FROM observations \
         WHERE consolidated_into IS NULL {obs_clause}",
        obs_clause = obs_branch_clause,
        p = if branch_param.is_some() { "2" } else { "1" },
    );

    let obs_metrics = if let Some(ref b) = branch_param {
        conn.query_row(&obs_agg_sql, params![b, seven_days_ago], |r| {
            Ok(ObservationMetrics {
                total: r.get::<_, Option<i64>>(0)?.unwrap_or(0) as u64,
                stale: r.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                retrieval_traffic: r.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
                noise_count: r.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                by_importance: [
                    r.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                    r.get::<_, Option<i64>>(5)?.unwrap_or(0) as u64,
                    r.get::<_, Option<i64>>(6)?.unwrap_or(0) as u64,
                    r.get::<_, Option<i64>>(7)?.unwrap_or(0) as u64,
                ],
                scope_symbol_only: r.get::<_, Option<i64>>(8)?.unwrap_or(0) as u64,
                scope_file_only: r.get::<_, Option<i64>>(9)?.unwrap_or(0) as u64,
                scope_both: r.get::<_, Option<i64>>(10)?.unwrap_or(0) as u64,
                scope_project: r.get::<_, Option<i64>>(11)?.unwrap_or(0) as u64,
                oldest_days: r.get::<_, Option<i64>>(12)?.map(|ts| ((now - ts) / 86400) as u64),
                oldest_non_stale_days: r.get::<_, Option<i64>>(13)?.map(|ts| ((now - ts) / 86400) as u64),
                has_recent_activity: r.get::<_, Option<i64>>(14)?.unwrap_or(0) != 0,
                by_kind: Vec::new(), // filled below
            })
        })?
    } else {
        conn.query_row(&obs_agg_sql, params![seven_days_ago], |r| {
            Ok(ObservationMetrics {
                total: r.get::<_, Option<i64>>(0)?.unwrap_or(0) as u64,
                stale: r.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                retrieval_traffic: r.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
                noise_count: r.get::<_, Option<i64>>(3)?.unwrap_or(0) as u64,
                by_importance: [
                    r.get::<_, Option<i64>>(4)?.unwrap_or(0) as u64,
                    r.get::<_, Option<i64>>(5)?.unwrap_or(0) as u64,
                    r.get::<_, Option<i64>>(6)?.unwrap_or(0) as u64,
                    r.get::<_, Option<i64>>(7)?.unwrap_or(0) as u64,
                ],
                scope_symbol_only: r.get::<_, Option<i64>>(8)?.unwrap_or(0) as u64,
                scope_file_only: r.get::<_, Option<i64>>(9)?.unwrap_or(0) as u64,
                scope_both: r.get::<_, Option<i64>>(10)?.unwrap_or(0) as u64,
                scope_project: r.get::<_, Option<i64>>(11)?.unwrap_or(0) as u64,
                oldest_days: r.get::<_, Option<i64>>(12)?.map(|ts| ((now - ts) / 86400) as u64),
                oldest_non_stale_days: r.get::<_, Option<i64>>(13)?.map(|ts| ((now - ts) / 86400) as u64),
                has_recent_activity: r.get::<_, Option<i64>>(14)?.unwrap_or(0) != 0,
                by_kind: Vec::new(),
            })
        })?
    };

    // ── Observation by-kind query ──
    let kind_sql = format!(
        "SELECT kind, COUNT(*) FROM observations \
         WHERE consolidated_into IS NULL AND kind != 'context_retrieval' {obs_clause} \
         GROUP BY kind ORDER BY kind",
        obs_clause = obs_branch_clause,
    );
    let mut obs_metrics = obs_metrics;
    {
        let mut stmt = conn.prepare(&kind_sql)?;
        let rows = if let Some(ref b) = branch_param {
            stmt.query_map(params![b], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?
        };
        obs_metrics.by_kind = rows.into_iter().map(|(k, c)| (k, c as u64)).collect();
    }

    // ── Rules aggregate query ──
    let rules_sql = format!(
        "SELECT \
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END), \
            SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), \
            SUM(CASE WHEN is_active = -1 THEN 1 ELSE 0 END) \
         FROM project_rules WHERE 1=1 {rule_clause}",
        rule_clause = rule_branch_clause,
    );
    let rules = if let Some(ref b) = branch_param {
        conn.query_row(&rules_sql, params![b], |r| {
            Ok(RuleMetrics {
                active: r.get::<_, Option<i64>>(0)?.unwrap_or(0) as u64,
                pending: r.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                stale: r.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
            })
        })?
    } else {
        conn.query_row(&rules_sql, [], |r| {
            Ok(RuleMetrics {
                active: r.get::<_, Option<i64>>(0)?.unwrap_or(0) as u64,
                pending: r.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                stale: r.get::<_, Option<i64>>(2)?.unwrap_or(0) as u64,
            })
        })?
    };

    // ── Sessions aggregate query (always global) ──
    let sessions: SessionMetrics = conn.query_row(
        "SELECT COUNT(*), SUM(CASE WHEN compressed = 1 THEN 1 ELSE 0 END) FROM sessions",
        [],
        |r| Ok(SessionMetrics {
            total: r.get::<_, Option<i64>>(0)?.unwrap_or(0) as u64,
            compressed: r.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
        }),
    )?;

    // ── Recent insight/decision check (scope-aware) ──
    let recent_insight_sql = format!(
        "SELECT COUNT(*) FROM observations \
         WHERE consolidated_into IS NULL \
           AND kind IN ('insight', 'decision') \
           AND created_at >= ?{p} \
           {obs_clause}",
        obs_clause = obs_branch_clause,
        p = if branch_param.is_some() { "2" } else { "1" },
    );
    let recent_insight_count: i64 = if let Some(ref b) = branch_param {
        conn.query_row(&recent_insight_sql, params![b, seven_days_ago], |r| r.get(0))?
    } else {
        conn.query_row(&recent_insight_sql, params![seven_days_ago], |r| r.get(0))?
    };

    // ── Compute recommendations ──
    let recommendations = compute_recommendations(
        &obs_metrics,
        &rules,
        recent_insight_count > 0,
        &branch_label,
    );

    Ok(MemoryHealthReport {
        branch_label,
        observations: obs_metrics,
        rules,
        sessions,
        recommendations,
    })
}

/// Pure function: compute recommendations from health metrics.
pub fn compute_recommendations(
    obs: &ObservationMetrics,
    rules: &RuleMetrics,
    has_recent_insights: bool,
    branch_label: &str,
) -> Vec<String> {
    let mut recs = Vec::new();

    if obs.total > 0 {
        let stale_pct = (obs.stale as f64 / obs.total as f64) * 100.0;
        if stale_pct > 50.0 {
            recs.push(format!("{:.0}% of observations are stale — consider reviewing outdated entries", stale_pct));
        }

        let low_ratio = obs.by_importance[3] as f64 / obs.total as f64;
        if low_ratio > 0.70 {
            recs.push("Over 70% of observations are low-importance — retention may need tuning".to_string());
        }

        // Noise ratio: compression-eligible / total health-visible (AC3 total excludes context_retrieval)
        if obs.total > 0 {
            let noise_ratio = obs.noise_count as f64 / obs.total as f64;
            if noise_ratio > 0.60 {
                recs.push("Over 60% of observations are compression-eligible (tool_call, file_change, context_retrieval) — compression may be overdue".to_string());
            }
        }

        if !has_recent_insights && obs.has_recent_activity {
            let msg = if branch_label.starts_with("branch:") {
                format!("No insights or decisions recorded in 7 days despite recent activity on {}", branch_label.trim_start_matches("branch: "))
            } else {
                "No insights or decisions recorded in 7 days despite recent activity".to_string()
            };
            recs.push(msg);
        }
    }

    if rules.stale > 0 {
        recs.push(format!("{} stale rule(s) detected — linked symbols may have changed", rules.stale));
    }

    recs
}

/// Format memory health report as markdown for MCP response.
pub fn format_memory_health_markdown(report: &MemoryHealthReport) -> String {
    let mut out = format!("## Memory Health ({})\n\n", report.branch_label);
    let obs = &report.observations;

    if obs.total == 0 {
        out.push_str("**Observations**: No observations stored.\n");
        if obs.retrieval_traffic > 0 {
            out.push_str(&format!("  - Retrieval traffic (context_retrieval): {}\n", obs.retrieval_traffic));
        }
        out.push('\n');
    } else {
        let stale_pct = if obs.total > 0 { (obs.stale as f64 / obs.total as f64) * 100.0 } else { 0.0 };
        out.push_str(&format!("**Observations**: {} total ({} stale — {:.0}%)\n", obs.total, obs.stale, stale_pct));

        // by kind
        let kinds: Vec<String> = obs.by_kind.iter().map(|(k, c)| format!("{c} {k}")).collect();
        if !kinds.is_empty() {
            out.push_str(&format!("  - By kind: {}\n", kinds.join(", ")));
        }

        // by importance
        out.push_str(&format!(
            "  - By importance: {} critical, {} high, {} medium, {} low\n",
            obs.by_importance[0], obs.by_importance[1], obs.by_importance[2], obs.by_importance[3]
        ));

        // by scope
        out.push_str(&format!(
            "  - By scope: {} symbol, {} file, {} both, {} project\n",
            obs.scope_symbol_only, obs.scope_file_only, obs.scope_both, obs.scope_project
        ));

        out.push_str(&format!("  - Retrieval traffic (context_retrieval): {}\n", obs.retrieval_traffic));
        out.push_str(&format!("  - Noise (compression-eligible): {}\n\n", obs.noise_count));
    }

    let r = &report.rules;
    out.push_str(&format!("**Rules**: {} active, {} pending, {} stale\n\n", r.active, r.pending, r.stale));

    let s = &report.sessions;
    out.push_str(&format!("**Sessions** (all branches): {} total ({} compressed)\n\n", s.total, s.compressed));

    if obs.oldest_days.is_some() || obs.oldest_non_stale_days.is_some() {
        out.push_str(&format!(
            "**Age**: oldest observation {} days, oldest non-stale {} days\n\n",
            obs.oldest_days.map(|d| d.to_string()).unwrap_or_else(|| "n/a".to_string()),
            obs.oldest_non_stale_days.map(|d| d.to_string()).unwrap_or_else(|| "n/a".to_string()),
        ));
    }

    if !report.recommendations.is_empty() {
        out.push_str("### Recommendations\n");
        for rec in &report.recommendations {
            out.push_str(&format!("- {rec}\n"));
        }
    }

    out
}

/// One-liner memory health summary for CLI status output.
pub fn format_memory_health_summary(report: &MemoryHealthReport) -> String {
    let obs = &report.observations;
    let stale_pct = if obs.total > 0 { (obs.stale as f64 / obs.total as f64) * 100.0 } else { 0.0 };
    format!(
        "Memory: {} obs ({:.0}% stale), {} active rules, {} sessions",
        obs.total, stale_pct, report.rules.active, report.sessions.total,
    )
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
            None,
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
            None,
            Importance::Medium,
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
            None,
            Importance::Medium,
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
            None,
            Importance::Medium,
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
            insert_observation(&conn, sid, "insight", "test", Some("f::x"), None, None, Importance::Medium).unwrap();
        }
        let ids = get_recent_session_ids(&conn, 2, None).unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], "s3"); // most recent first
        assert_eq!(ids[1], "s2");
    }

    #[test]
    fn test_get_recent_session_ids_skips_context_retrieval_only_sessions() {
        let (conn, _dir) = open_test_db();
        // Session with only context_retrieval obs should not consume a slot
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('retrieval-only', 500, 'a')", []).unwrap();
        insert_auto_observation(&conn, "retrieval-only", "context_retrieval", "get_context: debug", None, None, None).unwrap();
        // Session with real observations
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('real', 400, 'a')", []).unwrap();
        insert_observation(&conn, "real", "insight", "useful finding", Some("f::x"), None, None, Importance::Medium).unwrap();

        let ids = get_recent_session_ids(&conn, 10, None).unwrap();
        assert_eq!(ids, vec!["real"], "context_retrieval-only session must not appear");
    }

    #[test]
    fn test_get_recent_session_ids_skips_empty_sessions() {
        let (conn, _dir) = open_test_db();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('empty', 500, 'a')", []).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('has-obs', 400, 'a')", []).unwrap();
        insert_observation(&conn, "has-obs", "insight", "test", Some("f::x"), None, None, Importance::Medium).unwrap();

        let ids = get_recent_session_ids(&conn, 10, None).unwrap();
        assert_eq!(ids, vec!["has-obs"], "empty session must be excluded");
    }

    #[test]
    fn test_get_recent_session_ids_deterministic_same_timestamp() {
        let (conn, _dir) = open_test_db();
        // Two sessions with identical started_at — rowid tiebreaker
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('first', 100, 'a')", []).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('second', 100, 'a')", []).unwrap();
        insert_observation(&conn, "first", "insight", "a", Some("f::x"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "second", "insight", "b", Some("f::x"), None, None, Importance::Medium).unwrap();

        // Run twice to confirm determinism
        let ids1 = get_recent_session_ids(&conn, 10, None).unwrap();
        let ids2 = get_recent_session_ids(&conn, 10, None).unwrap();
        assert_eq!(ids1, ids2, "ordering must be deterministic");
        assert_eq!(ids1[0], "second", "higher rowid must come first");
    }

    #[test]
    fn test_get_observations_filtered_by_symbol_fqn() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "about foo", Some("f::foo"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "decision", "about bar", Some("f::bar"), None, None, Importance::Medium).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], Some("f::foo"), None, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol_fqn.as_deref(), Some("f::foo"));
    }

    #[test]
    fn test_get_observations_filtered_by_file_path() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "about src", None, Some("src/a.rs"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "about lib", None, Some("src/b.rs"), None, Importance::Medium).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, Some("src/a.rs"), None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].file_path.as_deref(), Some("src/a.rs"));
    }

    #[test]
    fn test_get_observations_filtered_no_filter_returns_all() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "one", Some("f::a"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "decision", "two", None, Some("src/b.rs"), None, Importance::Medium).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, None, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_get_observations_for_context_matches_any() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "by fqn", Some("f::foo"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "by path", None, Some("src/a.rs"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "unrelated", Some("f::bar"), Some("src/z.rs"), None, Importance::Medium).unwrap();

        let rows = get_observations_for_context(&conn, &["f::foo"], &["src/a.rs"], 50, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 2);
        let contents: Vec<&str> = rows.iter().map(|r| r.content.as_str()).collect();
        assert!(contents.contains(&"by fqn"));
        assert!(contents.contains(&"by path"));
    }

    #[test]
    fn test_get_observations_filtered_excludes_sensitive_paths() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "safe", None, Some("src/a.rs"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "secret", None, Some(".env"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "key", None, Some("certs/server.pem"), None, Importance::Medium).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, None, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "safe");
    }

    #[test]
    fn test_is_sensitive_path_re_export_works() {
        // Smoke test: verifies the public re-export alias resolves to the shared implementation.
        assert!(is_sensitive_path(".env"));
        assert!(!is_sensitive_path("src/main.rs"));
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
        insert_observation(&conn, "s1", "insight", "obs1", Some("f::x"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "obs2", Some("f::y"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s2", "decision", "obs3", None, Some("src/a.rs"), None, Importance::Medium).unwrap();

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
        insert_observation(&conn, "s1", "insight", "finding", Some("f::x"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "decision", "chose A", None, Some("src/a.rs"), None, Importance::Medium).unwrap();

        let obs = get_session_observations(&conn, "s1", &crate::policy::ContentPolicy::default()).unwrap();
        assert!(obs.is_some());
        let obs = obs.unwrap();
        assert_eq!(obs.len(), 2);
    }

    #[test]
    fn test_get_session_observations_invalid_session() {
        let (conn, _dir) = open_test_db();
        let obs = get_session_observations(&conn, "nonexistent", &crate::policy::ContentPolicy::default()).unwrap();
        assert!(obs.is_none());
    }

    #[test]
    fn test_get_session_observations_filters_sensitive_paths() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "safe obs", None, Some("src/main.rs"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "secret obs", None, Some(".env"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "key obs", None, Some("certs/server.pem"), None, Importance::Medium).unwrap();

        let obs = get_session_observations(&conn, "s1", &crate::policy::ContentPolicy::default()).unwrap().unwrap();
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
        insert_observation(&conn, "s1", "insight", "keep me", None, None, None, Importance::Medium).unwrap();
        insert_auto_observation(&conn, "s1", "tool_call", "delete me", None, None, None).unwrap();
        insert_auto_observation(&conn, "s1", "file_change", "delete me too", None, Some("src/a.rs"), None).unwrap();

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
            confidence: None,
            branch: None,
            importance: Importance::Medium,
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
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let recent = make_obs(now - 3600, false, None);       // 1 hour old
        let old = make_obs(now - 14 * 86400, false, None);    // 14 days old

        let scored = score_observations(&conn, vec![recent, old], None, None);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "1-hour-old ({:.4}) must score higher than 14-day-old ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_score_staleness_penalty() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let fresh = make_obs(now - 86400, false, None);
        let stale = make_obs(now - 86400, true, Some("source changed"));

        let scored = score_observations(&conn, vec![fresh, stale], None, None);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "non-stale ({:.4}) must score higher than same-age stale ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_score_absolute_decay_stability() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let obs1 = make_obs(now - 3 * 86400, false, None);
        let obs2 = make_obs(now - 3 * 86400, false, None);

        // Score in two different result sets
        let scored_alone = score_observations(&conn, vec![obs1], None, None);
        let scored_with_others = score_observations(&conn, vec![
            make_obs(now - 100, false, None),
            obs2,
            make_obs(now - 30 * 86400, false, None),
        ], None, None);

        let diff = (scored_alone[0].relevance_score - scored_with_others[1].relevance_score).abs();
        assert!(diff < 0.01, "same observation in different sets must produce similar score (diff={:.4})", diff);
    }

    #[test]
    fn test_score_clamping() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let zero_age = make_obs(now, false, None);
        let very_old = make_obs(0, false, None); // epoch

        let scored = score_observations(&conn, vec![zero_age, very_old], None, None);
        for so in &scored {
            assert!(so.relevance_score >= 0.0, "score must be >= 0.0");
            assert!(so.relevance_score <= 1.0, "score must be <= 1.0");
        }
    }

    #[test]
    fn test_score_7day_half_life() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let seven_days = make_obs(now - 7 * 86400, false, None);

        let scored = score_observations(&conn, vec![seven_days], None, None);
        // Recency at 7 days = 0.5^(7/7) = 0.5, confidence = NULL → 0.5 baseline, no BM25, no staleness penalty
        // Expected: 0.35 * 0 + 0.40 * 0.5 + 0.15 * 0.5 = 0.275
        let expected = 0.275;
        let diff = (scored[0].relevance_score - expected).abs();
        assert!(
            diff < 0.02,
            "7-day-old observation should score ~{:.3} but got {:.4} (diff={:.4})",
            expected, scored[0].relevance_score, diff
        );
    }

    // ─── Story 10.2 Unit Tests ───────────────────────────────────────────────────

    #[test]
    fn test_confidence_column_exists_after_migration() {
        let (conn, _dir) = open_test_db();
        // Verify the confidence column exists by inserting and reading it
        upsert_session(&conn, "conf-sess", "test").unwrap();
        insert_observation(&conn, "conf-sess", "insight", "test obs", None, None, None, Importance::Medium).unwrap();
        let id: i64 = conn.query_row("SELECT id FROM observations LIMIT 1", [], |r| r.get(0)).unwrap();
        // NULL by default for manual observations
        let conf: Option<f64> = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!(conf.is_none(), "manual observation confidence must be NULL");
    }

    #[test]
    fn test_auto_observation_gets_confidence_score() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "auto-conf-sess", "test").unwrap();
        let id = insert_auto_observation(&conn, "auto-conf-sess", "file_change", "edited src/a.rs", None, Some("src/a.rs"), None).unwrap();
        let conf: Option<f64> = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!(conf.is_some(), "auto observation must have a confidence score");
        let c = conf.unwrap();
        assert!((0.0..=1.0).contains(&c), "confidence must be in [0.0, 1.0], got {c}");
    }

    #[test]
    fn test_auto_observation_base_confidence_is_0_5() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "base-conf-sess", "test").unwrap();
        // Insert isolated auto observation with no nearby tool_call or file_change
        let id = insert_auto_observation(&conn, "base-conf-sess", "insight", "isolated observation", None, None, None).unwrap();
        let conf: f64 = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!((conf - 0.5).abs() < 0.01, "isolated auto observation should have 0.5 base confidence, got {conf}");
    }

    #[test]
    fn test_confidence_boosted_by_tool_call() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "boost-sess", "test").unwrap();
        // Insert an auto-generated tool_call (only auto_generated=1 observations count)
        insert_auto_observation(&conn, "boost-sess", "tool_call", "Read file", None, None, None).unwrap();
        // Now insert auto observation — should detect the nearby tool_call
        let id = insert_auto_observation(&conn, "boost-sess", "file_change", "changed src/a.rs", None, Some("src/a.rs"), None).unwrap();
        let conf: f64 = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!(conf >= 0.7, "tool_call nearby should boost confidence to >= 0.7, got {conf}");
    }

    #[test]
    fn test_confidence_boosted_by_structural_change() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "struct-sess", "test").unwrap();
        // Insert a structural file_change in production format: "Modified `file`: added `fn_name`"
        insert_auto_observation(&conn, "struct-sess", "file_change", "Modified `src/lib.rs`: added `new_function`", None, Some("src/lib.rs"), None).unwrap();
        // Auto observation should detect the structural change
        let id = insert_auto_observation(&conn, "struct-sess", "insight", "noted structural change", None, None, None).unwrap();
        let conf: f64 = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!(conf >= 0.6, "structural file_change nearby should boost confidence to >= 0.6, got {conf}");
    }

    #[test]
    fn test_confidence_max_with_both_signals() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "max-sess", "test").unwrap();
        // Insert auto-generated tool_call and structural file_change (production format)
        insert_auto_observation(&conn, "max-sess", "tool_call", "Edit file", None, None, None).unwrap();
        insert_auto_observation(&conn, "max-sess", "file_change", "Modified `src/lib.rs`: removed `old_fn`", None, Some("src/lib.rs"), None).unwrap();
        let id = insert_auto_observation(&conn, "max-sess", "insight", "both signals", None, None, None).unwrap();
        let conf: f64 = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!((conf - 0.8).abs() < 0.01, "tool_call + structural change → confidence = 0.8, got {conf}");
    }

    #[test]
    fn test_confidence_not_self_correlated() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "self-sess", "test").unwrap();
        // No other observations — auto observation should only get base 0.5
        let id = insert_auto_observation(&conn, "self-sess", "tool_call", "Read file", None, None, None).unwrap();
        let conf: f64 = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        // Base 0.5 only — the tool_call is excluded from its own correlation check
        assert!((conf - 0.5).abs() < 0.01, "observation must not correlate with itself, got {conf}");
    }

    #[test]
    fn test_manual_observation_confidence_stays_null() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "manual-null-sess", "test").unwrap();
        let id = insert_observation(&conn, "manual-null-sess", "insight", "manual obs", None, None, None, Importance::Medium).unwrap();
        let conf: Option<f64> = conn.query_row(
            "SELECT confidence FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0)
        ).unwrap();
        assert!(conf.is_none(), "insert_observation must leave confidence NULL");
    }

    #[test]
    fn test_score_observations_with_confidence_boosts_score() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let high_conf = ObservationRow {
            id: 1, session_id: "s".into(), created_at: now - 86400,
            kind: "insight".into(), content: "high confidence obs".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.8), branch: None, importance: Importance::Medium,
        };
        let low_conf = ObservationRow {
            id: 2, session_id: "s".into(), created_at: now - 86400,
            kind: "insight".into(), content: "low confidence obs".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.2), branch: None, importance: Importance::Medium,
        };
        let scored = score_observations(&conn, vec![high_conf, low_conf], None, None);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "high confidence ({:.4}) must score higher than low confidence ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_score_observations_stale_primary_signal() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let stale_obs = make_obs(now - 3600, true, Some("outdated"));
        let scored = score_observations(&conn, vec![stale_obs], None, None);
        assert_eq!(scored[0].primary_signal, "stale", "stale observation must have primary_signal='stale'");
    }

    #[test]
    fn test_score_observations_recency_primary_signal_when_no_query() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let obs = make_obs(now - 3600, false, None);
        // Without query, BM25 is 0, recency (0.40) > confidence (0.15*0.5=0.075)
        let scored = score_observations(&conn, vec![obs], None, None);
        assert_eq!(scored[0].primary_signal, "recency", "no-query fresh obs must have primary_signal='recency'");
    }

    #[test]
    fn test_fts5_table_exists_after_migration() {
        let (conn, _dir) = open_test_db();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='observations_fts'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 1, "observations_fts virtual table must exist after migration");
    }

    #[test]
    fn test_fts5_trigger_populates_on_insert() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "fts-sess", "test").unwrap();
        insert_observation(&conn, "fts-sess", "insight", "unique fts trigger test phrase", None, None, None, Importance::Medium).unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM observations_fts WHERE observations_fts MATCH 'trigger'",
            [], |r| r.get(0),
        ).unwrap();
        assert!(n >= 1, "FTS5 trigger must index content on INSERT");
    }

    #[test]
    fn test_fts5_bm25_ranking_with_query() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "bm25-sess", "test").unwrap();
        let now = now_epoch();
        // obs1: highly relevant to "rust memory management"
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) VALUES ('bm25-sess', ?1, 'insight', 'rust memory management allocation patterns', 1)",
            rusqlite::params![now - 100],
        ).unwrap();
        let id1: i64 = conn.last_insert_rowid();
        // obs2: somewhat relevant
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, auto_generated) VALUES ('bm25-sess', ?1, 'insight', 'general programming note', 1)",
            rusqlite::params![now - 100],
        ).unwrap();
        let id2: i64 = conn.last_insert_rowid();

        let obs1 = ObservationRow { id: id1, session_id: "bm25-sess".into(), created_at: now - 100, kind: "insight".into(), content: "rust memory management allocation patterns".into(), symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None, confidence: None, branch: None, importance: Importance::Medium };
        let obs2 = ObservationRow { id: id2, session_id: "bm25-sess".into(), created_at: now - 100, kind: "insight".into(), content: "general programming note".into(), symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None, confidence: None, branch: None, importance: Importance::Medium };

        let scored = score_observations(&conn, vec![obs1, obs2], Some("rust memory management"), None);
        // obs1 should score higher due to BM25 match
        assert!(
            scored[0].relevance_score >= scored[1].relevance_score,
            "BM25 matched obs ({:.4}) must score >= unmatched obs ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
        assert_eq!(scored[0].obs.id, id1, "obs matching 'rust memory management' must rank first");
    }

    #[test]
    fn test_fts5_trigger_deletes_on_delete() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "del-sess", "test").unwrap();
        insert_observation(&conn, "del-sess", "insight", "xyzzy_unique_term", None, None, None, Importance::Medium).unwrap();
        let id: i64 = conn.query_row("SELECT id FROM observations WHERE content LIKE 'xyzzy%'", [], |r| r.get(0)).unwrap();
        conn.execute("DELETE FROM observations WHERE id = ?1", rusqlite::params![id]).unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM observations_fts WHERE observations_fts MATCH 'xyzzy_unique_term'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 0, "FTS5 delete trigger must remove content from index");
    }

    // ─── Story 10.3 Unit Tests ───────────────────────────────────────────────────

    #[test]
    fn test_insert_observation_with_branch() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id = insert_observation(&conn, "s1", "insight", "found pattern", None, None, Some("main"), Importance::Medium).unwrap();
        let branch: Option<String> = conn
            .query_row("SELECT branch FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0))
            .unwrap();
        assert_eq!(branch.as_deref(), Some("main"));
    }

    #[test]
    fn test_insert_observation_no_branch() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id = insert_observation(&conn, "s1", "insight", "legacy obs", None, None, None, Importance::Medium).unwrap();
        let branch: Option<String> = conn
            .query_row("SELECT branch FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0))
            .unwrap();
        assert!(branch.is_none(), "NULL branch stored when None passed");
    }

    #[test]
    fn test_insert_auto_observation_with_branch() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id = insert_auto_observation(&conn, "s1", "file_change", "modified main.rs", None, None, Some("feature/x")).unwrap();
        let branch: Option<String> = conn
            .query_row("SELECT branch FROM observations WHERE id = ?1", rusqlite::params![id], |r| r.get(0))
            .unwrap();
        assert_eq!(branch.as_deref(), Some("feature/x"));
    }

    #[test]
    fn test_observations_filtered_by_branch() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "on main", None, None, Some("main"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "on feature", None, None, Some("feature/y"), Importance::Medium).unwrap();

        let rows = get_observations_filtered(&conn, &["s1".into()], None, None, Some("main"), &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].content, "on main");
    }

    #[test]
    fn test_observations_null_branch_globally_visible() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "legacy", None, None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "on main", None, None, Some("main"), Importance::Medium).unwrap();

        // NULL-branch obs is visible when filtering by "main"
        let rows = get_observations_filtered(&conn, &["s1".into()], None, None, Some("main"), &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 2, "NULL-branch obs must appear in any branch-filtered query");
    }

    #[test]
    fn test_observations_no_branch_filter() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "on main", None, None, Some("main"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "on feature", None, None, Some("feature/z"), Importance::Medium).unwrap();

        // No branch filter → all observations returned
        let rows = get_observations_filtered(&conn, &["s1".into()], None, None, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_observations_branch_switch_within_session() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        insert_observation(&conn, "s1", "insight", "work on main", None, None, Some("main"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "work on dev", None, None, Some("dev"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "generic note", None, None, None, Importance::Medium).unwrap();

        let main_rows = get_observations_filtered(&conn, &["s1".into()], None, None, Some("main"), &crate::policy::ContentPolicy::default()).unwrap();
        // main + NULL = 2
        assert_eq!(main_rows.len(), 2);

        let dev_rows = get_observations_filtered(&conn, &["s1".into()], None, None, Some("dev"), &crate::policy::ContentPolicy::default()).unwrap();
        // dev + NULL = 2
        assert_eq!(dev_rows.len(), 2);
    }

    // ─── Consolidation exclusion tests ──────────────────────────────────────

    fn mark_consolidated(conn: &Connection, obs_id: i64, survivor_id: i64) {
        conn.execute(
            "UPDATE observations SET consolidated_into = ?1 WHERE id = ?2",
            params![survivor_id, obs_id],
        ).unwrap();
    }

    #[test]
    fn test_get_observations_filtered_excludes_consolidated() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id1 = insert_observation(&conn, "s1", "insight", "survivor obs", Some("f::x"), None, None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "s1", "insight", "consolidated obs", Some("f::x"), None, None, Importance::Medium).unwrap();
        mark_consolidated(&conn, id2, id1);

        let rows = get_observations_filtered(&conn, &["s1".into()], None, None, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, id1);
    }

    #[test]
    fn test_get_observations_for_context_excludes_consolidated() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id1 = insert_observation(&conn, "s1", "insight", "survivor context", None, Some("src/a.rs"), None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "s1", "insight", "merged context", None, Some("src/a.rs"), None, Importance::Medium).unwrap();
        mark_consolidated(&conn, id2, id1);

        let rows = get_observations_for_context(&conn, &[], &["src/a.rs"], 10, None, &crate::policy::ContentPolicy::default()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, id1);
    }

    #[test]
    fn test_get_recent_session_ids_excludes_consolidated_only_sessions() {
        let (conn, _dir) = open_test_db();
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('only-consolidated', 100, 'a')", []).unwrap();
        let id1 = insert_observation(&conn, "only-consolidated", "insight", "obs", Some("f::x"), None, None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "only-consolidated", "insight", "obs2", Some("f::x"), None, None, Importance::Medium).unwrap();
        mark_consolidated(&conn, id1, id2);
        mark_consolidated(&conn, id2, id1); // both consolidated — session has no visible obs

        // Session with all obs consolidated should still appear (it has a consolidated_into IS NULL check on obs join)
        // Actually both are consolidated, so no obs match the JOIN, and session won't appear
        let ids = get_recent_session_ids(&conn, 10, None).unwrap();
        assert!(ids.is_empty(), "session with only consolidated obs should not appear");
    }

    #[test]
    fn test_list_sessions_count_excludes_consolidated() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id1 = insert_observation(&conn, "s1", "insight", "visible", Some("f::x"), None, None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "s1", "insight", "merged", Some("f::x"), None, None, Importance::Medium).unwrap();
        mark_consolidated(&conn, id2, id1);

        let sessions = list_sessions(&conn, 10).unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].observation_count, 1, "consolidated obs should not count");
    }

    #[test]
    fn test_get_session_observations_excludes_consolidated() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id1 = insert_observation(&conn, "s1", "insight", "visible", Some("f::x"), None, None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "s1", "insight", "merged", Some("f::x"), None, None, Importance::Medium).unwrap();
        mark_consolidated(&conn, id2, id1);

        let obs = get_session_observations(&conn, "s1", &crate::policy::ContentPolicy::default()).unwrap().unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].id, id1);
    }

    // ─── Story 11.1 Unit Tests ───────────────────────────────────────────────────

    #[test]
    fn test_importance_from_str_valid() {
        assert_eq!("critical".parse::<Importance>().unwrap(), Importance::Critical);
        assert_eq!("HIGH".parse::<Importance>().unwrap(), Importance::High);
        assert_eq!("Medium".parse::<Importance>().unwrap(), Importance::Medium);
        assert_eq!("low".parse::<Importance>().unwrap(), Importance::Low);
    }

    #[test]
    fn test_importance_from_str_invalid() {
        assert!("urgent".parse::<Importance>().is_err());
        assert!("".parse::<Importance>().is_err());
    }

    #[test]
    fn test_importance_display() {
        assert_eq!(Importance::Critical.to_string(), "critical");
        assert_eq!(Importance::High.to_string(), "high");
        assert_eq!(Importance::Medium.to_string(), "medium");
        assert_eq!(Importance::Low.to_string(), "low");
    }

    #[test]
    fn test_importance_from_sql_to_sql_roundtrip() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id = insert_observation(&conn, "s1", "decision", "arch choice", None, Some("src/a.rs"), None, Importance::Critical).unwrap();
        let stored: String = conn
            .query_row("SELECT importance FROM observations WHERE id = ?1", params![id], |r| r.get(0))
            .unwrap();
        assert_eq!(stored, "critical");

        // Read back through ObservationRow
        let obs = get_session_observations(&conn, "s1", &crate::policy::ContentPolicy::default()).unwrap().unwrap();
        assert_eq!(obs[0].importance, Importance::Critical);
    }

    #[test]
    fn test_importance_default_for_kind() {
        assert_eq!(Importance::default_for_kind("decision"), Importance::High);
        assert_eq!(Importance::default_for_kind("anti_pattern"), Importance::Medium);
        assert_eq!(Importance::default_for_kind("file_change"), Importance::Low);
        assert_eq!(Importance::default_for_kind("tool_call"), Importance::Low);
        assert_eq!(Importance::default_for_kind("context_retrieval"), Importance::Low);
        assert_eq!(Importance::default_for_kind("insight"), Importance::Medium);
        assert_eq!(Importance::default_for_kind("unknown_future_kind"), Importance::Medium);
    }

    #[test]
    fn test_auto_observation_gets_importance_by_kind() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let tool_id = insert_auto_observation(&conn, "s1", "tool_call", "Read file", None, None, None).unwrap();
        let ap_id = insert_auto_observation(&conn, "s1", "anti_pattern", "dead-end detected", None, None, None).unwrap();

        let tool_imp: String = conn.query_row("SELECT importance FROM observations WHERE id = ?1", params![tool_id], |r| r.get(0)).unwrap();
        assert_eq!(tool_imp, "low");

        let ap_imp: String = conn.query_row("SELECT importance FROM observations WHERE id = ?1", params![ap_id], |r| r.get(0)).unwrap();
        assert_eq!(ap_imp, "medium");
    }

    #[test]
    fn test_auto_observation_unknown_kind_defaults_to_low() {
        // AC3: unknown future auto-captured kinds must default to Low, not Medium
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "a").unwrap();
        let id = insert_auto_observation(&conn, "s1", "future_new_kind", "something new", None, None, None).unwrap();
        let imp: String = conn.query_row("SELECT importance FROM observations WHERE id = ?1", params![id], |r| r.get(0)).unwrap();
        assert_eq!(imp, "low", "unknown auto-captured kinds must default to low per AC3");
    }

    #[test]
    fn test_scoring_critical_beats_medium_after_30_days() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let critical_old = ObservationRow {
            id: 1, session_id: "s".into(), created_at: now - 30 * 86400,
            kind: "decision".into(), content: "critical arch".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.5), branch: None, importance: Importance::Critical,
        };
        let medium_week = ObservationRow {
            id: 2, session_id: "s".into(), created_at: now - 7 * 86400,
            kind: "insight".into(), content: "medium finding".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.5), branch: None, importance: Importance::Medium,
        };
        let scored = score_observations(&conn, vec![critical_old, medium_week], None, None);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "critical from 30 days ({:.4}) must score higher than medium from 7 days ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_scoring_low_decays_faster_than_medium() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let medium_obs = ObservationRow {
            id: 1, session_id: "s".into(), created_at: now - 3 * 86400,
            kind: "insight".into(), content: "medium".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.5), branch: None, importance: Importance::Medium,
        };
        let low_obs = ObservationRow {
            id: 2, session_id: "s".into(), created_at: now - 3 * 86400,
            kind: "tool_call".into(), content: "low".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.5), branch: None, importance: Importance::Low,
        };
        let scored = score_observations(&conn, vec![medium_obs, low_obs], None, None);
        assert!(
            scored[0].relevance_score > scored[1].relevance_score,
            "medium from 3 days ({:.4}) must score higher than low from 3 days ({:.4})",
            scored[0].relevance_score, scored[1].relevance_score
        );
    }

    #[test]
    fn test_scoring_critical_365_day_old_still_meaningful() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let old_critical = ObservationRow {
            id: 1, session_id: "s".into(), created_at: now - 365 * 86400,
            kind: "decision".into(), content: "ancient decision".into(),
            symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None,
            confidence: Some(0.5), branch: None, importance: Importance::Critical,
        };
        let scored = score_observations(&conn, vec![old_critical], None, None);
        // At 365 days with 365-day half-life, recency = 0.5
        // Score = 0.35*0 + 0.40*0.5 + 0.15*0.5 = 0.275
        assert!(scored[0].relevance_score > 0.2, "365-day critical must still have meaningful score, got {:.4}", scored[0].relevance_score);
        assert!(scored[0].relevance_score < 0.5, "365-day critical should not dominate, got {:.4}", scored[0].relevance_score);
    }

    #[test]
    fn test_score_with_none_semantic_matches_current_behavior() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        let obs = make_obs(now - 3600, false, None);
        // Without semantic scores, weights are BM25=0.35, recency=0.40, confidence=0.15
        let scored = score_observations(&conn, vec![obs], None, None);
        assert_eq!(scored[0].primary_signal, "recency");
        assert!(scored[0].score_breakdown.semantic.abs() < 0.001, "no semantic signal without semantic_scores");
        assert!(scored[0].score_breakdown.recency > 0.1, "recency should contribute");
    }

    #[test]
    fn test_score_with_semantic_scores_semantic_wins_primary_signal() {
        let (conn, _dir) = open_test_db();
        let now = now_epoch();
        // Very old observation so recency is near-zero
        let obs = make_obs(now - 365 * 86400, false, None);
        // Give it a very high cosine similarity
        let mut sem_scores = std::collections::HashMap::new();
        sem_scores.insert(obs.id, 1.0);
        let scored = score_observations(&conn, vec![obs], None, Some(&sem_scores));
        // Semantic contrib = 0.25 * 1.0 = 0.25, recency contrib ≈ 0, confidence contrib = 0.10 * 0.5 = 0.05
        assert_eq!(scored[0].primary_signal, "semantic", "high cosine must make semantic the primary signal");
        assert!(scored[0].score_breakdown.semantic > 0.2, "semantic contribution should be ~0.25");
    }

    #[test]
    fn test_score_breakdown_format_compact() {
        let bd = ScoreBreakdown { bm25: 0.04, semantic: 0.21, recency: 0.28, confidence: 0.05, staleness: 0.0 };
        let formatted = bd.format_compact();
        assert!(formatted.contains("bm25=0.04"), "should contain bm25");
        assert!(formatted.contains("sem=0.21"), "should contain semantic");
        assert!(formatted.contains("rec=0.28"), "should contain recency");
        assert!(formatted.contains("conf=0.05"), "should contain confidence");
        assert!(!formatted.contains("stale"), "no staleness when 0");

        let stale_bd = ScoreBreakdown { bm25: 0.0, semantic: 0.0, recency: 0.28, confidence: 0.05, staleness: -0.30 };
        let formatted = stale_bd.format_compact();
        assert!(formatted.contains("stale=-0.30"), "should show staleness when applied");
    }

    #[test]
    fn test_purge_protects_critical_session() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        // Old session with a critical observation
        conn.execute("INSERT INTO sessions (id, started_at, ended_at, agent) VALUES ('critical-sess', 100, 200, 'a')", []).unwrap();
        insert_observation(&conn, "critical-sess", "decision", "critical arch decision", None, Some("src/a.rs"), None, Importance::Critical).unwrap();
        insert_observation(&conn, "critical-sess", "tool_call", "Read file", None, None, None, Importance::Low).unwrap();

        // Old session without critical observations
        conn.execute("INSERT INTO sessions (id, started_at, ended_at, agent) VALUES ('normal-sess', 100, 200, 'a')", []).unwrap();
        insert_observation(&conn, "normal-sess", "insight", "routine note", None, None, None, Importance::Medium).unwrap();

        let purged = purge_old_sessions(&mut conn, 1).unwrap();
        assert_eq!(purged, 1, "only non-critical session should be purged");

        // critical-sess and its observations must survive
        let critical_exists: bool = conn.query_row("SELECT COUNT(*) FROM sessions WHERE id = 'critical-sess'", [], |r| r.get::<_, i64>(0)).unwrap() > 0;
        assert!(critical_exists, "critical session must survive purge");

        // normal-sess should be gone
        let normal_exists: bool = conn.query_row("SELECT COUNT(*) FROM sessions WHERE id = 'normal-sess'", [], |r| r.get::<_, i64>(0)).unwrap() > 0;
        assert!(!normal_exists, "non-critical session must be purged");
    }

    #[test]
    fn test_purge_does_not_protect_high_only_session() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        conn.execute("INSERT INTO sessions (id, started_at, ended_at, agent) VALUES ('high-sess', 100, 200, 'a')", []).unwrap();
        insert_observation(&conn, "high-sess", "decision", "high importance", None, None, None, Importance::High).unwrap();

        let purged = purge_old_sessions(&mut conn, 1).unwrap();
        assert_eq!(purged, 1, "high-only session should be purged (only critical protects)");
    }

    #[test]
    fn test_compress_respects_importance() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.db");
        let mut conn = crate::db::open(&db_path).unwrap();

        upsert_session(&conn, "s1", "a").unwrap();
        // A critical tool_call should survive compression
        insert_observation(&conn, "s1", "tool_call", "critical tool call", None, None, None, Importance::Critical).unwrap();
        // A low tool_call should be deleted
        insert_auto_observation(&conn, "s1", "tool_call", "routine tool call", None, None, None).unwrap();
        // A regular insight should survive (not ephemeral kind)
        insert_observation(&conn, "s1", "insight", "keep me", None, None, None, Importance::Medium).unwrap();

        let deleted = compress_specific_session(&mut conn, "s1").unwrap();
        assert_eq!(deleted, 1, "only the low-importance ephemeral obs should be deleted");

        let remaining: i64 = conn.query_row("SELECT COUNT(*) FROM observations WHERE session_id = 's1'", [], |r| r.get(0)).unwrap();
        assert_eq!(remaining, 2, "critical tool_call and insight must survive");
    }

    #[test]
    fn test_migration_010_importance_column() {
        let (conn, _dir) = open_test_db();
        // Verify the importance column exists with DEFAULT and CHECK constraint
        upsert_session(&conn, "mig-sess", "test").unwrap();
        // Insert without specifying importance (should default to 'medium')
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content) VALUES ('mig-sess', 1000, 'insight', 'test')",
            [],
        ).unwrap();
        let imp: String = conn.query_row(
            "SELECT importance FROM observations WHERE session_id = 'mig-sess'", [], |r| r.get(0)
        ).unwrap();
        assert_eq!(imp, "medium", "importance must default to 'medium'");

        // CHECK constraint: invalid value must fail
        let result = conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, importance) VALUES ('mig-sess', 1001, 'insight', 'test', 'urgent')",
            [],
        );
        assert!(result.is_err(), "CHECK constraint must reject invalid importance values");
    }

    #[test]
    fn test_importance_index_exists() {
        let (conn, _dir) = open_test_db();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_observations_importance_session'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(n, 1, "composite index on (importance, session_id) must exist");
    }

    // ─── Story 11.2 — project-scoped observation retrieval ───────────────────

    #[test]
    fn test_get_project_scoped_returns_unanchored_only() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        // project-scoped (no anchors)
        insert_observation(&conn, "s1", "insight", "project-wide note", None, None, None, Importance::Medium).unwrap();
        // anchored to file
        insert_observation(&conn, "s1", "insight", "file note", None, Some("src/main.rs"), None, Importance::Medium).unwrap();
        // anchored to symbol
        insert_observation(&conn, "s1", "insight", "symbol note", Some("src/main.rs::main"), None, None, Importance::Medium).unwrap();

        let results = get_project_scoped_observations(&conn, None, 100).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, "project-wide note");
    }

    #[test]
    fn test_get_project_scoped_excludes_consolidated() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        let id1 = insert_observation(&conn, "s1", "insight", "original note", None, None, None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "s1", "insight", "duplicate note", None, None, None, Importance::Medium).unwrap();
        // Mark id2 as consolidated into id1
        conn.execute("UPDATE observations SET consolidated_into = ?1 WHERE id = ?2", rusqlite::params![id1, id2]).unwrap();

        let results = get_project_scoped_observations(&conn, None, 100).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id1);
    }

    #[test]
    fn test_get_project_scoped_branch_filtering() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "insight", "main branch note", None, None, Some("main"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "feature branch note", None, None, Some("feature"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "global note", None, None, None, Importance::Medium).unwrap();

        // Filter for "main" branch: should get main + global (NULL branch)
        let results = get_project_scoped_observations(&conn, Some("main"), 100).unwrap();
        assert_eq!(results.len(), 2);
        let contents: Vec<&str> = results.iter().map(|r| r.content.as_str()).collect();
        assert!(contents.contains(&"main branch note"));
        assert!(contents.contains(&"global note"));
    }

    // ─── Story 11.3 — Memory Health Diagnostic Tool ──────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn insert_obs_at(conn: &Connection, session_id: &str, kind: &str, content: &str,
                     symbol_fqn: Option<&str>, file_path: Option<&str>,
                     branch: Option<&str>, importance: Importance,
                     created_at: i64, is_stale: bool) {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, file_path, branch, importance, is_stale) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![session_id, created_at, kind, content, symbol_fqn, file_path, branch, importance, is_stale as i64],
        ).unwrap();
    }

    #[test]
    fn test_health_empty_db() {
        let (conn, _dir) = open_test_db();
        let report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert_eq!(report.observations.total, 0);
        assert_eq!(report.observations.stale, 0);
        assert_eq!(report.observations.retrieval_traffic, 0);
        assert!(report.observations.by_kind.is_empty());
        assert!(!report.observations.has_recent_activity);
        assert!(report.observations.oldest_days.is_none());
        assert_eq!(report.rules.active, 0);
        assert_eq!(report.sessions.total, 0);
        assert!(report.recommendations.is_empty());
    }

    #[test]
    fn test_health_basic_counts() {
        let (conn, _dir) = open_test_db();
        let now = now_secs();
        upsert_session(&conn, "s1", "test").unwrap();
        // Various kinds
        insert_obs_at(&conn, "s1", "insight", "i1", Some("mod::f"), None, Some("main"), Importance::High, now - 100, false);
        insert_obs_at(&conn, "s1", "decision", "d1", None, Some("a.rs"), Some("main"), Importance::High, now - 200, false);
        insert_obs_at(&conn, "s1", "error", "e1", Some("mod::g"), Some("b.rs"), Some("main"), Importance::Medium, now - 300, true);
        insert_obs_at(&conn, "s1", "tool_call", "tc1", None, None, Some("main"), Importance::Low, now - 50, false);
        insert_obs_at(&conn, "s1", "file_change", "fc1", None, Some("c.rs"), Some("main"), Importance::Low, now - 60, false);
        insert_obs_at(&conn, "s1", "context_retrieval", "cr1", None, None, Some("main"), Importance::Low, now - 10, false);

        let report = memory_health_report(&conn, &ResolvedBranchScope::Branch("main".into())).unwrap();
        let obs = &report.observations;

        // context_retrieval excluded from total
        assert_eq!(obs.total, 5);
        assert_eq!(obs.stale, 1);
        assert_eq!(obs.retrieval_traffic, 1);
        // noise = tool_call + file_change + context_retrieval = 3
        assert_eq!(obs.noise_count, 3);

        // by importance: critical=0, high=2, medium=1, low=2
        assert_eq!(obs.by_importance, [0, 2, 1, 2]);

        // scope categories
        assert_eq!(obs.scope_symbol_only, 1); // insight with symbol only
        assert_eq!(obs.scope_file_only, 2);   // decision (file only) + file_change (file only)
        assert_eq!(obs.scope_both, 1);         // error (symbol + file)
        assert_eq!(obs.scope_project, 1);      // tool_call (no anchors)

        assert!(obs.has_recent_activity);
    }

    #[test]
    fn test_health_consolidated_exclusion() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        let id1 = insert_observation(&conn, "s1", "insight", "survivor", None, None, None, Importance::Medium).unwrap();
        let id2 = insert_observation(&conn, "s1", "insight", "absorbed", None, None, None, Importance::Medium).unwrap();
        // Mark id2 as consolidated
        conn.execute("UPDATE observations SET consolidated_into = ?1 WHERE id = ?2", params![id1, id2]).unwrap();

        let report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert_eq!(report.observations.total, 1);
    }

    #[test]
    fn test_health_retrieval_traffic_separation() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "context_retrieval", "cr1", None, None, None, Importance::Low).unwrap();
        insert_observation(&conn, "s1", "context_retrieval", "cr2", None, None, None, Importance::Low).unwrap();
        insert_observation(&conn, "s1", "insight", "i1", None, None, None, Importance::Medium).unwrap();

        let report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert_eq!(report.observations.total, 1); // only insight
        assert_eq!(report.observations.retrieval_traffic, 2);
    }

    #[test]
    fn test_health_noise_count() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "tool_call", "tc", None, None, None, Importance::Low).unwrap();
        insert_observation(&conn, "s1", "file_change", "fc", None, None, None, Importance::Low).unwrap();
        insert_observation(&conn, "s1", "context_retrieval", "cr", None, None, None, Importance::Low).unwrap();
        insert_observation(&conn, "s1", "insight", "i", None, None, None, Importance::Medium).unwrap();

        let report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert_eq!(report.observations.noise_count, 3);
    }

    #[test]
    fn test_health_branch_filtering() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "insight", "main-obs", None, None, Some("main"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "feat-obs", None, None, Some("feature"), Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "global-obs", None, None, None, Importance::Medium).unwrap();

        let main_report = memory_health_report(&conn, &ResolvedBranchScope::Branch("main".into())).unwrap();
        assert_eq!(main_report.observations.total, 2); // main + NULL

        let all_report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert_eq!(all_report.observations.total, 3);
    }

    #[test]
    fn test_health_scope_categories_sum_to_total() {
        let (conn, _dir) = open_test_db();
        upsert_session(&conn, "s1", "test").unwrap();
        insert_observation(&conn, "s1", "insight", "sym", Some("mod::f"), None, None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "file", None, Some("a.rs"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "both", Some("mod::g"), Some("b.rs"), None, Importance::Medium).unwrap();
        insert_observation(&conn, "s1", "insight", "proj", None, None, None, Importance::Medium).unwrap();

        let report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        let obs = &report.observations;
        assert_eq!(obs.scope_symbol_only + obs.scope_file_only + obs.scope_both + obs.scope_project, obs.total);
    }

    #[test]
    fn test_health_has_recent_activity() {
        let (conn, _dir) = open_test_db();
        let now = now_secs();
        upsert_session(&conn, "s1", "test").unwrap();
        // Only old observations
        insert_obs_at(&conn, "s1", "insight", "old", None, None, None, Importance::Medium, now - 30 * 86400, false);

        let report = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert!(!report.observations.has_recent_activity);

        // Add recent observation
        insert_obs_at(&conn, "s1", "insight", "new", None, None, None, Importance::Medium, now - 100, false);
        let report2 = memory_health_report(&conn, &ResolvedBranchScope::All).unwrap();
        assert!(report2.observations.has_recent_activity);
    }

    #[test]
    fn test_health_recommendation_stale_threshold() {
        let obs = ObservationMetrics { total: 100, stale: 51, ..Default::default() };
        let rules = RuleMetrics::default();
        let recs = compute_recommendations(&obs, &rules, true, "all branches");
        assert!(recs.iter().any(|r| r.contains("stale")));

        let obs_below = ObservationMetrics { total: 100, stale: 50, ..Default::default() };
        let recs_below = compute_recommendations(&obs_below, &rules, true, "all branches");
        assert!(!recs_below.iter().any(|r| r.contains("stale")));
    }

    #[test]
    fn test_health_recommendation_low_importance() {
        let obs = ObservationMetrics { total: 100, by_importance: [0, 0, 29, 71], ..Default::default() };
        let rules = RuleMetrics::default();
        let recs = compute_recommendations(&obs, &rules, true, "all branches");
        assert!(recs.iter().any(|r| r.contains("low-importance")));
    }

    #[test]
    fn test_health_recommendation_noise_ratio() {
        // noise_count/total > 60%: 25/40 = 62.5%
        let obs = ObservationMetrics { total: 40, noise_count: 25, ..Default::default() };
        let rules = RuleMetrics::default();
        let recs = compute_recommendations(&obs, &rules, true, "all branches");
        assert!(recs.iter().any(|r| r.contains("compression-eligible")));
    }

    #[test]
    fn test_health_recommendation_activity_gated_insight() {
        let obs = ObservationMetrics { total: 10, has_recent_activity: true, ..Default::default() };
        let rules = RuleMetrics::default();
        // No recent insights + has_recent_activity = recommend
        let recs = compute_recommendations(&obs, &rules, false, "branch: main");
        assert!(recs.iter().any(|r| r.contains("No insights")));
        assert!(recs.iter().any(|r| r.contains("main")));

        // No recent insights but no recent activity = don't recommend
        let obs_quiet = ObservationMetrics { total: 10, has_recent_activity: false, ..Default::default() };
        let recs_quiet = compute_recommendations(&obs_quiet, &rules, false, "branch: main");
        assert!(!recs_quiet.iter().any(|r| r.contains("No insights")));
    }

    #[test]
    fn test_health_recommendation_stale_rules() {
        let obs = ObservationMetrics::default();
        let rules = RuleMetrics { active: 3, pending: 1, stale: 2 };
        let recs = compute_recommendations(&obs, &rules, true, "all branches");
        assert!(recs.iter().any(|r| r.contains("stale rule")));
    }

    #[test]
    fn test_health_formatter_empty_state() {
        let report = MemoryHealthReport {
            branch_label: "all branches".to_string(),
            observations: ObservationMetrics::default(),
            rules: RuleMetrics { active: 2, pending: 0, stale: 0 },
            sessions: SessionMetrics { total: 5, compressed: 3 },
            recommendations: Vec::new(),
        };
        let md = format_memory_health_markdown(&report);
        assert!(md.contains("No observations stored."));
        assert!(md.contains("2 active"));
        assert!(md.contains("5 total"));
    }

    #[test]
    fn test_health_formatter_retrieval_only_shows_empty_state() {
        let report = MemoryHealthReport {
            branch_label: "branch: main".to_string(),
            observations: ObservationMetrics { retrieval_traffic: 10, ..Default::default() },
            rules: RuleMetrics::default(),
            sessions: SessionMetrics::default(),
            recommendations: Vec::new(),
        };
        let md = format_memory_health_markdown(&report);
        assert!(md.contains("No observations stored."), "retrieval-only DB should show empty state");
        assert!(md.contains("Retrieval traffic"), "should still report retrieval traffic count");
    }

    #[test]
    fn test_health_summary_format() {
        let report = MemoryHealthReport {
            branch_label: "branch: main".to_string(),
            observations: ObservationMetrics { total: 42, stale: 5, ..Default::default() },
            rules: RuleMetrics { active: 3, pending: 1, stale: 0 },
            sessions: SessionMetrics { total: 34, compressed: 28 },
            recommendations: Vec::new(),
        };
        let summary = format_memory_health_summary(&report);
        assert!(summary.contains("42 obs"));
        assert!(summary.contains("12%"));
        assert!(summary.contains("3 active rules"));
        assert!(summary.contains("34 sessions"));
    }
}
