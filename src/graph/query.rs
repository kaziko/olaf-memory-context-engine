use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use rusqlite::{Connection, OptionalExtension, params};

use crate::graph::skeleton::skeletonize;
use crate::policy::ContentPolicy;
use crate::sensitive::is_sensitive;
use crate::workspace::Workspace;

#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O error reading source: {0}")]
    Io(#[from] std::io::Error),
}

impl From<crate::graph::store::StoreError> for QueryError {
    fn from(e: crate::graph::store::StoreError) -> Self {
        match e {
            crate::graph::store::StoreError::Sqlite(e) => QueryError::Sqlite(e),
        }
    }
}

// --- Intent classification tuning ---

/// Confidence (= dominance = top/total) below this triggers balanced fallback + wider pivot pool.
/// At 0.60, mixed intents like "refactor and fix" (dominance=0.50) correctly fall back;
/// single-category intents like "fix" or "fix crash" (dominance=1.00) use their mode.
const LOW_CONFIDENCE_THRESHOLD: f32 = 0.60;

/// Include inbound edges when inbound-oriented signal weight reaches this.
/// 0.35 means: if 35%+ of matched signals point toward bug-fix or refactor,
/// callers/dependents are relevant enough to include.
const INBOUND_BLEND_THRESHOLD: f32 = 0.35;

/// Use BFS depth 3 only when refactor weight is dominant (>= 50%).
/// Below this, depth 2 avoids pulling too-distant symbols for focused tasks.
const REFACTOR_DEPTH_THRESHOLD: f32 = 0.50;

/// Per-keyword candidate cap during the gather phase in find_pivot_symbols.
///
/// The gather query uses `ORDER BY id ASC` for test determinism (see dev notes). This means
/// on very large indexes with >50 matches per keyword, symbols with lower IDs are preferred
/// during candidate selection regardless of their in-degree — the final sort only ranks
/// whatever was gathered. Raising this limit trades more queries/memory for broader coverage.
const CANDIDATE_GATHER_LIMIT: usize = 50;

/// Default pivot candidate pool size (keyword-match branch in find_pivot_symbols).
const DEFAULT_PIVOT_POOL: usize = 5;

/// Wider pivot pool under low confidence to reduce pivot miss risk.
/// Applies only to the keyword-match branch; file-hints branch uses LIMIT 10 regardless.
const LOW_CONFIDENCE_PIVOT_POOL: usize = 8;

/// Emit intent_mix only when 2nd-highest mode weight exceeds this AND confidence >= threshold.
/// Avoids cluttering output for unambiguous single-mode queries or low-confidence noise.
const NON_TRIVIAL_MIX_THRESHOLD: f32 = 0.20;

pub(crate) enum IntentMode {
    BugFix,
    Refactor,
    Implementation,
    Balanced,
}

/// Scored result of intent classification.
pub(crate) struct IntentProfile {
    #[allow(dead_code)] pub bugfix_score: usize,   // read only in tests; production uses normalized weights
    #[allow(dead_code)] pub refactor_score: usize,
    #[allow(dead_code)] pub impl_score: usize,
    #[allow(dead_code)] pub total: usize,
    pub confidence: f32,              // dominance = top/total (0.0 if total==0)
    pub dominant_mode: IntentMode,    // winning mode by score, BEFORE confidence fallback
    pub execution_mode: IntentMode,   // actual mode used for traversal (Balanced when low-confidence)
    pub w_bugfix: f32,                // normalized weight
    pub w_refactor: f32,
    pub w_impl: f32,
    pub matched_signals: Vec<String>, // canonical keywords, category order then alpha: bugfix first, then refactor, then impl
}

/// Derived traversal parameters from IntentProfile.
pub(crate) struct TraversalPolicy {
    pub depth: usize,
    pub include_inbound: bool,
    pub inbound_first: bool,   // true = BugFix-style (inbound before outbound)
    pub pivot_pool_size: usize,
}

fn contains_word(text: &str, word: &str) -> bool {
    text.split(|c: char| !c.is_alphanumeric())
        .any(|w| w.eq_ignore_ascii_case(word))
}

pub(crate) fn detect_intent_profile(intent: &str) -> IntentProfile {
    let lower = intent.to_lowercase();

    let bugfix_keywords   = ["bug", "crash", "debug", "error", "fix"];   // alphabetical within category
    let refactor_keywords = ["extract", "refactor", "rename", "restructure"];
    let impl_keywords     = ["add", "build", "create", "implement"];

    // Collect per-category: extend matched_signals in category order (bugfix, refactor, impl)
    let mut matched_signals: Vec<String> = Vec::new();
    let b_matches: Vec<&str> = bugfix_keywords.iter().copied().filter(|&w| contains_word(&lower, w)).collect();
    let r_matches: Vec<&str> = refactor_keywords.iter().copied().filter(|&w| contains_word(&lower, w)).collect();
    let i_matches: Vec<&str> = impl_keywords.iter().copied().filter(|&w| contains_word(&lower, w)).collect();
    matched_signals.extend(b_matches.iter().map(|&w| w.to_string()));
    matched_signals.extend(r_matches.iter().map(|&w| w.to_string()));
    matched_signals.extend(i_matches.iter().map(|&w| w.to_string()));

    let b = b_matches.len();
    let r = r_matches.len();
    let i = i_matches.len();
    let total = b + r + i;
    let top = b.max(r).max(i);

    let (w_bugfix, w_refactor, w_impl, confidence) = if total == 0 {
        (0.0f32, 0.0, 0.0, 0.0)
    } else {
        let t = total as f32;
        (b as f32 / t, r as f32 / t, i as f32 / t, top as f32 / t)
    };

    // dominant_mode: winning mode by score only (before confidence threshold)
    // Tie-breaking: BugFix > Refactor > Implementation > Balanced
    let dominant_mode = if total == 0 {
        IntentMode::Balanced
    } else if b >= r && b >= i {
        IntentMode::BugFix
    } else if r >= i {
        IntentMode::Refactor
    } else {
        IntentMode::Implementation
    };

    // execution_mode: falls back to Balanced when confidence is too low
    let execution_mode = if confidence < LOW_CONFIDENCE_THRESHOLD {
        IntentMode::Balanced
    } else {
        match &dominant_mode {
            IntentMode::BugFix => IntentMode::BugFix,
            IntentMode::Refactor => IntentMode::Refactor,
            IntentMode::Implementation => IntentMode::Implementation,
            IntentMode::Balanced => IntentMode::Balanced,
        }
    };

    IntentProfile { bugfix_score: b, refactor_score: r, impl_score: i, total,
                    confidence, dominant_mode, execution_mode, w_bugfix, w_refactor, w_impl, matched_signals }
}

pub(crate) fn derive_traversal_policy(profile: &IntentProfile) -> TraversalPolicy {
    // Low confidence or no signals → balanced fallback; short-circuits AC#3 and AC#4.
    if profile.confidence < LOW_CONFIDENCE_THRESHOLD || profile.total == 0 {
        return TraversalPolicy {
            depth: 2,
            include_inbound: false,
            inbound_first: false,
            pivot_pool_size: LOW_CONFIDENCE_PIVOT_POOL,
        };
    }

    // AC#3: depth 3 only when refactor is dominant
    let depth = if profile.w_refactor >= REFACTOR_DEPTH_THRESHOLD { 3 } else { 2 };

    // AC#4: include inbound when inbound-oriented weight is material
    let include_inbound = (profile.w_bugfix + profile.w_refactor) >= INBOUND_BLEND_THRESHOLD;
    let inbound_first = include_inbound && profile.w_bugfix >= profile.w_refactor;

    TraversalPolicy {
        depth,
        include_inbound,
        inbound_first,
        pivot_pool_size: DEFAULT_PIVOT_POOL,
    }
}

struct SymbolRow {
    id: i64,
    fqn: String,
    name: String,
    file_path: String,
    start_line: i64,
    end_line: i64,
    signature: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum SelectionReason {
    Keyword { kw_score: usize, in_degree: i64 },
    FileHint { hint: String },
    CallerSupplied,
    Fallback,
}

#[derive(Debug, Clone)]
pub(crate) struct PivotScore {
    pub id: i64,
    pub fqn: String,
    pub reason: SelectionReason,
}

#[derive(Debug, Clone)]
pub(crate) struct TaggedPivotScore {
    pub pivot: PivotScore,
    pub member_index: usize,
}

fn estimate_tokens(s: &str) -> usize {
    s.len().div_ceil(4)
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Select pivot symbols from the graph for the given intent.
///
/// Scoring: `rank = (kw_score DESC, in_degree DESC, id ASC)`
/// where `kw_score` is the count of distinct, case-normalized intent keywords (> 3 chars)
/// that match the symbol's `name` or `fqn`, and `in_degree` is the number of edges
/// pointing to the symbol.
///
/// File hints take priority: when `file_hints` is non-empty and at least one hint matches
/// a symbol, those symbols are returned and keyword ranking is skipped. If hints match
/// nothing, the function falls through to keyword ranking.
pub(crate) fn rank_symbols_by_keywords(
    conn: &Connection,
    keywords: &[String],
    limit: usize,
) -> Result<Vec<PivotScore>, QueryError> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let unique_words: HashSet<String> = keywords.iter()
        .filter(|w| w.len() > 3)
        .map(|w| w.to_lowercase())
        .collect();

    if unique_words.is_empty() {
        return Ok(Vec::new());
    }

    let mut candidates: HashMap<i64, usize> = HashMap::new();
    let mut kw_stmt = conn.prepare(
        "SELECT id FROM symbols WHERE lower(name) LIKE ?1 OR lower(fqn) LIKE ?1
         ORDER BY id ASC LIMIT ?2"
    )?;
    for word in &unique_words {
        let pattern = format!("%{word}%");
        let rows: Vec<i64> = kw_stmt.query_map(params![pattern, CANDIDATE_GATHER_LIMIT as i64], |r| r.get(0))?
            .collect::<Result<_,_>>()?;
        for id in rows { *candidates.entry(id).or_insert(0) += 1; }
    }

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    const IN_DEG_CHUNK: usize = 500;
    let cand_ids: Vec<i64> = candidates.keys().copied().collect();
    let mut in_degrees: HashMap<i64, i64> = HashMap::new();
    let mut full_chunk_stmt: Option<rusqlite::Statement<'_>> = None;
    let mut tail_chunk_stmt: Option<rusqlite::Statement<'_>> = None;
    for chunk in cand_ids.chunks(IN_DEG_CHUNK) {
        let opt = if chunk.len() == IN_DEG_CHUNK { &mut full_chunk_stmt } else { &mut tail_chunk_stmt };
        if opt.is_none() {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT target_id, COUNT(*) FROM edges WHERE target_id IN ({placeholders}) GROUP BY target_id"
            );
            *opt = Some(conn.prepare(&sql)?);
        }
        let stmt = opt.as_mut().expect("statement was just prepared above");
        let rows: Vec<(i64, i64)> = stmt
            .query_map(rusqlite::params_from_iter(chunk.iter()), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })?
            .collect::<Result<_,_>>()?;
        for (id, count) in rows { in_degrees.insert(id, count); }
    }

    let mut scored: Vec<(i64, usize, i64)> = candidates.into_iter()
        .map(|(id, kw)| {
            let in_deg = in_degrees.get(&id).copied().unwrap_or(0);
            (id, kw, in_deg)
        })
        .collect();
    scored.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then(b.2.cmp(&a.2))
            .then(a.0.cmp(&b.0))
    });

    let top: Vec<(i64, usize, i64)> = scored.into_iter().take(limit).collect();
    if top.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<i64> = top.iter().map(|(id, _, _)| *id).collect();
    let fqn_placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let fqn_sql = format!("SELECT id, fqn FROM symbols WHERE id IN ({fqn_placeholders})");
    let mut fqn_stmt = conn.prepare(&fqn_sql)?;
    let fqn_map: HashMap<i64, String> = fqn_stmt
        .query_map(rusqlite::params_from_iter(ids.iter()), |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<Result<_,_>>()?;
    let mut result: Vec<PivotScore> = Vec::with_capacity(top.len());
    for (id, kw, in_deg) in top {
        match fqn_map.get(&id) {
            Some(fqn) => result.push(PivotScore {
                id,
                fqn: fqn.clone(),
                reason: SelectionReason::Keyword { kw_score: kw, in_degree: in_deg },
            }),
            None => log::warn!("rank_symbols_by_keywords: symbol id={id} scored but missing from fqn batch query — row may have been deleted"),
        }
    }
    Ok(result)
}

fn find_pivot_symbols(conn: &Connection, intent: &str, file_hints: &[String], pool_size: usize) -> Result<Vec<PivotScore>, QueryError> {
    let mut seen: HashSet<i64> = HashSet::new();

    // --- File hints branch ---
    if !file_hints.is_empty() {
        let mut result: Vec<PivotScore> = Vec::new();
        let mut hint_stmt = conn.prepare(
            "SELECT s.id, s.fqn FROM symbols s JOIN files f ON f.id=s.file_id
             WHERE f.path LIKE ?1 ORDER BY (s.end_line-s.start_line) DESC LIMIT 10"
        )?;
        for hint in file_hints {
            let pattern = format!("%{hint}%");
            let rows: Vec<(i64, String)> = hint_stmt.query_map(params![pattern], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?.collect::<Result<_,_>>()?;
            for (id, fqn) in rows {
                if seen.insert(id) {
                    result.push(PivotScore {
                        id,
                        fqn,
                        reason: SelectionReason::FileHint { hint: hint.clone() },
                    });
                }
            }
        }
        if !result.is_empty() { return Ok(result); }
    }

    // --- Keyword branch: delegate to shared ranking function ---
    let keywords: Vec<String> = intent.split_whitespace().map(|s| s.to_string()).collect();
    let ranked = rank_symbols_by_keywords(conn, &keywords, pool_size)?;

    if !ranked.is_empty() {
        return Ok(ranked);
    }

    // Fallback: no keyword matches — return first symbols in DB
    let mut stmt = conn.prepare("SELECT id, fqn FROM symbols ORDER BY id ASC LIMIT ?1")?;
    let rows: Vec<(i64, String)> = stmt.query_map(params![pool_size as i64], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    })?.collect::<Result<_,_>>()?;
    Ok(rows.into_iter().map(|(id, fqn)| PivotScore { id, fqn, reason: SelectionReason::Fallback }).collect())
}

fn process_inbound_edges(
    stmt: &mut rusqlite::Statement<'_>,
    current_id: i64,
    current_depth: usize,
    pivot_set: &HashSet<i64>,
    visited: &mut HashSet<i64>,
    queue: &mut VecDeque<(i64, usize)>,
    supporting: &mut Vec<(i64, String)>,
) -> Result<(), QueryError> {
    let inbound: Vec<i64> = stmt.query_map(params![current_id], |r| r.get(0))?
        .collect::<Result<_,_>>()?;
    for id in inbound {
        if visited.insert(id) {
            queue.push_back((id, current_depth + 1));
            if !pivot_set.contains(&id) {
                let reason = if current_depth == 0 {
                    "inbound caller of pivot".to_string()
                } else {
                    format!("inbound caller (depth {})", current_depth + 1)
                };
                supporting.push((id, reason));
            }
        }
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
fn traverse_bfs(
    conn: &Connection,
    pivot_ids: &[i64],
    policy: &TraversalPolicy,
) -> Result<(Vec<i64>, Vec<(i64, String)>), QueryError> {
    let pivot_set: HashSet<i64> = pivot_ids.iter().copied().collect();
    let mut visited: HashSet<i64> = pivot_set.clone();
    let mut queue: VecDeque<(i64, usize)> = pivot_ids.iter().map(|&id| (id, 0)).collect();
    let mut supporting: Vec<(i64, String)> = Vec::new();

    let mut inbound_stmt = conn.prepare(
        "SELECT DISTINCT source_id FROM edges WHERE target_id=?1 ORDER BY source_id LIMIT 20"
    )?;
    let mut outbound_stmt = conn.prepare(
        "SELECT DISTINCT target_id FROM edges WHERE source_id=?1 ORDER BY target_id LIMIT 20"
    )?;

    while let Some((current_id, current_depth)) = queue.pop_front() {
        if current_depth >= policy.depth { continue; }

        if policy.inbound_first {
            process_inbound_edges(&mut inbound_stmt, current_id, current_depth, &pivot_set, &mut visited, &mut queue, &mut supporting)?;
        }

        // Outbound edges
        let outbound: Vec<i64> = outbound_stmt.query_map(params![current_id], |r| r.get(0))?
            .collect::<Result<_,_>>()?;
        for id in outbound {
            if visited.insert(id) {
                queue.push_back((id, current_depth + 1));
                if !pivot_set.contains(&id) {
                    let reason = if current_depth == 0 {
                        "outbound dependency of pivot".to_string()
                    } else {
                        format!("outbound dependency (depth {})", current_depth + 1)
                    };
                    supporting.push((id, reason));
                }
            }
        }

        if policy.include_inbound && !policy.inbound_first {
            process_inbound_edges(&mut inbound_stmt, current_id, current_depth, &pivot_set, &mut visited, &mut queue, &mut supporting)?;
        }
    }

    Ok((pivot_ids.to_vec(), supporting))
}

fn load_symbol_row(conn: &Connection, symbol_id: i64) -> Result<Option<SymbolRow>, QueryError> {
    let row = conn.query_row(
        "SELECT s.id, s.fqn, s.name, f.path, s.start_line, s.end_line, s.signature
         FROM symbols s JOIN files f ON f.id=s.file_id WHERE s.id=?1",
        params![symbol_id],
        |r| Ok(SymbolRow {
            id: r.get(0)?,
            fqn: r.get(1)?,
            name: r.get(2)?,
            file_path: r.get(3)?,
            start_line: r.get(4)?,
            end_line: r.get(5)?,
            signature: r.get(6)?,
        }),
    ).optional()?;
    Ok(row)
}

fn read_symbol_source(project_root: &Path, file_path: &str, start_line: i64, end_line: i64) -> Result<String, std::io::Error> {
    let full_path = project_root.join(file_path);
    let content = std::fs::read_to_string(&full_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let start = (start_line - 1).max(0) as usize;
    let end = (end_line as usize).min(lines.len());
    let (start, end) = (start.min(end), end);
    Ok(lines[start..end].join("\n"))
}

/// Create an embedder for the given project root. Returns None without `embeddings` feature
/// or on any initialization failure (including missing ONNX runtime).
fn create_embedder(project_root: &Path) -> Option<Box<dyn crate::memory::embedder::EmbedText>> {
    #[cfg(feature = "embeddings")]
    {
        let cache_dir = project_root.join(".olaf").join("models");
        // catch_unwind: ort panics (instead of Err) when libonnxruntime is missing
        std::panic::catch_unwind(|| {
            crate::memory::embedder::FastEmbedder::new(&cache_dir)
                .ok()
                .map(|e| Box::new(e) as Box<dyn crate::memory::embedder::EmbedText>)
        })
        .ok()
        .flatten()
    }
    #[cfg(not(feature = "embeddings"))]
    {
        let _ = project_root;
        None
    }
}

fn format_scored_observation_entry(scored: &crate::memory::store::ScoredObservation) -> String {
    let obs = &scored.obs;
    let confidence_val = obs.confidence.unwrap_or(0.5);
    let signal_label = if scored.primary_signal == "stale" || obs.is_stale {
        "(stale)"
    } else {
        let conf_tag = if confidence_val >= 0.7 { "high confidence" } else if confidence_val < 0.4 { "low confidence" } else { "" };
        let age_tag = if scored.relevance_score >= 0.5 { "recent" } else { "aged" };
        if conf_tag.is_empty() {
            if age_tag == "recent" { "(recent)" } else { "(aged)" }
        } else if age_tag == "recent" {
            if conf_tag == "high confidence" { "(recent · high confidence)" } else { "(recent · low confidence)" }
        } else if conf_tag == "low confidence" {
            "(aged · low confidence)"
        } else {
            "(aged)"
        }
    };

    let importance_tag = match obs.importance {
        crate::memory::store::Importance::Medium => String::new(),
        ref imp => format!("[{}] ", imp),
    };

    let breakdown = scored.score_breakdown.format_compact();

    let mut entry = String::new();
    if obs.is_stale {
        let reason = obs.stale_reason.as_deref().unwrap_or("unknown reason");
        if breakdown.is_empty() {
            entry.push_str(&format!("- \u{26a0} [STALE \u{2014} {}] {}[{}] {} {}\n", reason, importance_tag, obs.kind, obs.content, signal_label));
        } else {
            entry.push_str(&format!("- \u{26a0} [STALE \u{2014} {}] {}[{}] {} {} {}\n", reason, importance_tag, obs.kind, obs.content, signal_label, breakdown));
        }
    } else if breakdown.is_empty() {
        entry.push_str(&format!("- {}[{}] {} {}\n", importance_tag, obs.kind, obs.content, signal_label));
    } else {
        entry.push_str(&format!("- {}[{}] {} {} {}\n", importance_tag, obs.kind, obs.content, signal_label, breakdown));
    }
    if let Some(fqn) = &obs.symbol_fqn {
        entry.push_str(&format!("  Symbol: {}\n", fqn));
    }
    if let Some(fp) = &obs.file_path {
        entry.push_str(&format!("  File: {}\n", fp));
    }
    entry
}

/// Compute semantic scores for project-scoped observations against intent.
/// Returns None when embedder is None, intent is missing, or no stored embeddings exist.
fn compute_project_semantic_scores(
    conn: &Connection,
    observations: &[&crate::memory::store::ObservationRow],
    intent: Option<&str>,
    embedder: Option<&dyn crate::memory::embedder::EmbedText>,
) -> Option<std::collections::HashMap<i64, f64>> {
    let embedder = embedder?;
    let intent_query = intent?;
    if intent_query.trim().is_empty() || observations.is_empty() {
        return None;
    }

    let query_vec = embedder.embed_query(intent_query).ok()?;

    let obs_ids: Vec<i64> = observations.iter().map(|o| o.id).collect();
    let stored = crate::memory::embedder::load_embeddings(
        conn, &obs_ids, embedder.model_id(), embedder.model_rev(),
    ).ok()?;

    if stored.is_empty() { return None; }

    let mut scores = std::collections::HashMap::new();
    for (obs_id, embedding) in &stored {
        let sim = crate::memory::embedder::cosine_similarity(
            &query_vec, embedding,
        );
        scores.insert(*obs_id, ((sim as f64) + 1.0) / 2.0);
    }
    Some(scores)
}

fn extract_intent_label(intent_header: &str) -> String {
    for line in intent_header.lines() {
        if let Some(rest) = line.strip_prefix("intent_mode: ") {
            return rest.trim().to_string();
        }
    }
    "balanced".to_string()
}

fn format_retrieval_notes(rendered_pivots: &[&PivotScore], intent_label: &str, omitted: usize) -> String {
    if rendered_pivots.is_empty() && omitted == 0 {
        return String::new();
    }
    let mut out = String::from("\n## Retrieval Notes\n");
    for ps in rendered_pivots {
        let short_name = ps.fqn.rsplit("::").next().unwrap_or(&ps.fqn);
        let score_str = match &ps.reason {
            SelectionReason::Keyword { kw_score, in_degree } => format!("kw={kw_score} deg={in_degree}"),
            SelectionReason::FileHint { hint } => format!("file-hint \"{hint}\""),
            SelectionReason::CallerSupplied => "caller-supplied".to_string(),
            SelectionReason::Fallback => "fallback".to_string(),
        };
        out.push_str(&format!("- {short_name} ({}): {score_str} [{intent_label}]\n", ps.fqn));
    }
    if omitted > 0 {
        out.push_str(&format!("({omitted} pivots omitted: budget/sensitive-path)\n"));
    }
    out
}

fn format_retrieval_notes_multi(
    rendered_pivots: &[(&TaggedPivotScore, bool)],
    intent_label: &str,
    omitted: usize,
    labels: &[(usize, &str)],
) -> String {
    if rendered_pivots.is_empty() && omitted == 0 {
        return String::new();
    }
    let mut out = String::from("\n## Retrieval Notes\n");
    for (tps, is_local) in rendered_pivots {
        let ps = &tps.pivot;
        let short_name = ps.fqn.rsplit("::").next().unwrap_or(&ps.fqn);
        let label = labels.iter()
            .find(|(idx, _)| *idx == tps.member_index)
            .map(|(_, l)| *l)
            .unwrap_or("unknown");
        let strategy = if *is_local { "local-priority" } else { "remote-round-robin" };
        let score_str = match &ps.reason {
            SelectionReason::Keyword { kw_score, in_degree } => format!("kw={kw_score} deg={in_degree}"),
            SelectionReason::FileHint { hint } => format!("file-hint \"{hint}\""),
            SelectionReason::CallerSupplied => "caller-supplied".to_string(),
            SelectionReason::Fallback => "fallback".to_string(),
        };
        out.push_str(&format!("- {short_name} [{label}] ({strategy}): {score_str} [{intent_label}]\n", ));
    }
    if omitted > 0 {
        out.push_str(&format!("({omitted} pivots omitted: budget/sensitive-path)\n"));
    }
    out
}

fn format_pivot_entry(row: &SymbolRow, source: &str) -> String {
    format!(
        "## {} (`{}`)\nFile: `{}` lines {}-{}\n\n```\n{}\n```\n\n",
        row.name, row.fqn, row.file_path, row.start_line, row.end_line, source
    )
}

const NO_SYMBOLS_IN_INDEX: &str = "\nNo symbols found. Run `olaf index` first.\n";
const NO_SYMBOLS_FOR_FQNS: &str = "\nNo symbols found matching provided FQNs.\n";

fn format_intent_header(profile: &IntentProfile, intent: &str) -> String {
    let intent_label = match &profile.execution_mode {
        IntentMode::BugFix => "bug-fix",
        IntentMode::Refactor => "refactor",
        IntentMode::Implementation => "implementation",
        IntentMode::Balanced => "balanced",
    };
    let signals_str = if profile.matched_signals.is_empty() {
        "none".to_string()
    } else {
        profile.matched_signals.join(", ")
    };
    let mut output = format!(
        "# Context Brief: {intent}\nintent_mode: {intent_label}\nintent_confidence: {:.2}\nintent_signals: {signals_str}\n",
        profile.confidence
    );
    let mut weights = [
        ("bug-fix", profile.w_bugfix),
        ("refactor", profile.w_refactor),
        ("implementation", profile.w_impl),
    ];
    weights.sort_by(|a, b| b.1.total_cmp(&a.1));
    if profile.confidence >= LOW_CONFIDENCE_THRESHOLD && weights[1].1 > NON_TRIVIAL_MIX_THRESHOLD {
        output.push_str(&format!(
            "intent_mix: bug-fix={:.2},refactor={:.2},implementation={:.2}\n",
            profile.w_bugfix, profile.w_refactor, profile.w_impl
        ));
    }
    if matches!(profile.execution_mode, IntentMode::Balanced)
        && !matches!(profile.dominant_mode, IntentMode::Balanced)
    {
        output.push_str("intent_fallback_reason: low confidence\n");
    }
    output
}

#[allow(clippy::too_many_arguments)]
fn build_context_brief(
    conn: &Connection,
    project_root: &Path,
    intent_header: &str,
    pivot_scores: &[PivotScore],
    policy: &TraversalPolicy,
    token_budget: usize,
    intent_query: Option<&str>,
    branch: Option<&str>,
    content_policy: &ContentPolicy,
) -> Result<(String, String), QueryError> {
    let mut output = intent_header.to_string();

    if pivot_scores.is_empty() {
        output.push_str(NO_SYMBOLS_IN_INDEX);
        return Ok((output, String::new()));
    }

    let pivot_ids: Vec<i64> = pivot_scores.iter().map(|ps| ps.id).collect();
    let (pivots, supporting_with_reasons) = traverse_bfs(conn, &pivot_ids, policy)?;

    let pivot_budget = token_budget * 70 / 100;
    let skeleton_budget = token_budget * 20 / 100;
    // Rules budget probe — allocated dynamically after traversal

    output.push_str("\n## Pivot Symbols\n\n");

    let mut pivot_tokens = 0usize;
    let mut all_fqns: HashSet<String> = HashSet::new();
    let mut all_file_paths: HashSet<String> = HashSet::new();
    let mut rendered_pivot_ids: HashSet<i64> = HashSet::new();

    for id in &pivots {
        let Some(row) = load_symbol_row(conn, *id)? else { continue };
        if is_sensitive(&row.file_path) { continue; }
        if content_policy.is_denied(&row.file_path, Some(&row.fqn)) { continue; }

        all_fqns.insert(row.fqn.clone());
        all_file_paths.insert(row.file_path.clone());

        let source = if content_policy.is_redacted(&row.file_path, Some(&row.fqn)) {
            format!("{}\n  [redacted by policy]", row.signature.as_deref().unwrap_or("(signature unavailable)"))
        } else {
            match read_symbol_source(project_root, &row.file_path, row.start_line, row.end_line) {
                Ok(s) if !s.is_empty() => s,
                _ => {
                    row.signature.as_deref().unwrap_or("(source unavailable)").to_string()
                }
            }
        };

        let entry = format_pivot_entry(&row, &source);
        let entry_tokens = estimate_tokens(&entry);
        if pivot_tokens + entry_tokens > pivot_budget { break; }
        output.push_str(&entry);
        pivot_tokens += entry_tokens;
        rendered_pivot_ids.insert(*id);
    }

    if !supporting_with_reasons.is_empty() {
        output.push_str("## Supporting Symbols\n\n");
        let mut skeleton_tokens = 0usize;
        for (id, reason) in &supporting_with_reasons {
            let Some(row) = load_symbol_row(conn, *id)? else { continue };
            if is_sensitive(&row.file_path) { continue; }
            if content_policy.is_denied(&row.file_path, Some(&row.fqn)) { continue; }

            all_fqns.insert(row.fqn.clone());
            all_file_paths.insert(row.file_path.clone());

            let skeleton = skeletonize(conn, row.id)?;
            let reason_line = format!("Why: {reason}\n");
            let skeleton_tokens_needed = estimate_tokens(&skeleton);
            let reason_tokens_needed = estimate_tokens(&reason_line);

            if skeleton_tokens + skeleton_tokens_needed > skeleton_budget { break; }

            output.push_str(&skeleton);
            skeleton_tokens += skeleton_tokens_needed;

            if skeleton_tokens + reason_tokens_needed <= skeleton_budget {
                output.push_str(&reason_line);
                skeleton_tokens += reason_tokens_needed;
            }
        }
    }

    // Memory + Rules injection — 10% budget total
    let fqns_vec: Vec<String> = all_fqns.iter().cloned().collect();
    let file_paths_vec: Vec<String> = all_file_paths.iter().cloned().collect();
    let fqns: Vec<&str> = all_fqns.iter().map(|s| s.as_str()).collect();
    let file_paths: Vec<&str> = all_file_paths.iter().map(|s| s.as_str()).collect();

    // Probe for active rules to determine budget split
    let rules = crate::memory::rules::get_active_rules(conn, &fqns_vec, &file_paths_vec, branch, 5)
        .unwrap_or_default();
    let (rules_budget, memory_budget) = if rules.is_empty() {
        (0, token_budget * 10 / 100)
    } else {
        (std::cmp::min(token_budget * 5 / 100, 300), token_budget * 5 / 100)
    };

    // Carve project sub-budget from memory budget (AC3: 20% of memory_budget, capped at 200)
    let project_sub_budget = std::cmp::min(memory_budget * 20 / 100, 200);
    let anchored_memory_budget = memory_budget - project_sub_budget;

    // Create embedder once for both anchored and project-scoped scoring
    let embedder = create_embedder(project_root);
    let embedder_ref = embedder.as_ref().map(|e| e.as_ref());
    let mut mem_section = String::new();

    if !fqns.is_empty() || !file_paths.is_empty() {
        let scored = crate::memory::store::get_scored_observations_for_context(
            conn, &fqns, &file_paths, 50, intent_query, branch, content_policy,
            embedder_ref,
        )
        .unwrap_or_default();

        if !scored.is_empty() {
            let mut mem_tokens = 0usize;
            for scored_obs in &scored {
                let entry = format_scored_observation_entry(scored_obs);
                let entry_tokens = estimate_tokens(&entry);
                if mem_tokens + entry_tokens > anchored_memory_budget {
                    break;
                }
                mem_section.push_str(&entry);
                mem_tokens += entry_tokens;
            }
        }
    }

    // Project-scoped observations with relevance gating (AC4)
    if project_sub_budget > 0
        && let Ok(project_obs) = crate::memory::store::get_project_scoped_observations(conn, branch, 20)
        && !project_obs.is_empty()
    {
        let intent_tokens: std::collections::HashSet<String> = intent_query
            .map(|q| crate::memory::rules::extract_tokens(q).into_iter().collect())
            .unwrap_or_default();
        let min_matches = if intent_tokens.len() < 2 { 1 } else { 2 };

        // Token overlap gate: cheap pre-filter
        let mut scored_project: Vec<(usize, &crate::memory::store::ObservationRow)> = project_obs.iter()
            .filter_map(|obs| {
                let obs_tokens: std::collections::HashSet<String> =
                    crate::memory::rules::extract_tokens(&obs.content).into_iter().collect();
                let matching = obs_tokens.intersection(&intent_tokens).count();
                if matching >= min_matches { Some((matching, obs)) } else { None }
            })
            .collect();

        // When embeddings available, use semantic similarity for sort; else fall back to token count
        let semantic_sort = compute_project_semantic_scores(
            conn, &scored_project.iter().map(|(_, o)| *o).collect::<Vec<_>>(),
            intent_query, embedder_ref,
        );
        if let Some(ref sem) = semantic_sort {
            scored_project.sort_by(|a, b| {
                let sa = sem.get(&a.1.id).copied().unwrap_or(0.0);
                let sb = sem.get(&b.1.id).copied().unwrap_or(0.0);
                sb.total_cmp(&sa).then_with(|| b.1.created_at.cmp(&a.1.created_at))
            });
        } else {
            scored_project.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.created_at.cmp(&a.1.created_at)));
        }

        let mut project_tokens = 0usize;
        for (_score, obs) in &scored_project {
            let entry = format!("- [project] [{}] {} ({})\n",
                obs.kind,
                obs.content,
                if obs.is_stale { "stale" } else { "active" },
            );
            let entry_tokens = estimate_tokens(&entry);
            if project_tokens + entry_tokens > project_sub_budget {
                break;
            }
            mem_section.push_str(&entry);
            project_tokens += entry_tokens;
        }
    }

    if !mem_section.is_empty() {
        output.push_str("## Session Memory\n\n");
        output.push_str(&mem_section);
    }

    // Rules injection
    if !rules.is_empty() && rules_budget > 0 {
        let mut rules_section = String::new();
        let mut rules_tokens = 0usize;
        for rule in &rules {
            let age_secs = now_secs() - rule.last_seen_at;
            let age_label = if age_secs < 3600 {
                format!("{}m ago", age_secs / 60)
            } else if age_secs < 86400 {
                format!("{}h ago", age_secs / 3600)
            } else {
                format!("{}d ago", age_secs / 86400)
            };
            let entry = format!(
                "- {} [{} obs · {} sessions · last seen {}]\n",
                rule.content, rule.support_count, rule.session_count, age_label
            );
            let entry_tokens = estimate_tokens(&entry);
            if rules_tokens + entry_tokens > rules_budget {
                break;
            }
            rules_section.push_str(&entry);
            rules_tokens += entry_tokens;
        }
        if !rules_section.is_empty() {
            output.push_str("## Project Rules\n\n");
            output.push_str(&rules_section);
        }
    }

    // Build retrieval notes
    let intent_label = extract_intent_label(intent_header);
    let rendered: Vec<&PivotScore> = pivot_scores.iter()
        .filter(|ps| rendered_pivot_ids.contains(&ps.id))
        .collect();
    let omitted = pivot_scores.len() - rendered.len();
    let retrieval_notes = format_retrieval_notes(&rendered, &intent_label, omitted);

    Ok((output, retrieval_notes))
}

pub(crate) fn get_context(
    conn: &Connection,
    project_root: &Path,
    intent: &str,
    file_hints: &[String],
    token_budget: usize,
    branch: Option<&str>,
    content_policy: &ContentPolicy,
) -> Result<(String, String), QueryError> {
    let profile = detect_intent_profile(intent);
    let policy = derive_traversal_policy(&profile);
    let intent_header = format_intent_header(&profile, intent);
    let mut pivot_scores = find_pivot_symbols(conn, intent, file_hints, policy.pivot_pool_size)?;
    pivot_scores.retain(|ps| !content_policy.is_denied_by_fqn(&ps.fqn));
    build_context_brief(conn, project_root, &intent_header, &pivot_scores, &policy, token_budget, Some(intent), branch, content_policy)
}

/// Build a context brief directly from caller-provided `PivotScore` values, preserving their
/// `SelectionReason` (e.g. `Keyword` with `kw_score`/`in_degree`). Used by `analyze_failure`
/// Path B so that keyword scores are surfaced in retrieval notes rather than reclassified.
#[allow(clippy::too_many_arguments)]
pub(crate) fn get_context_from_pivot_scores(
    conn: &Connection,
    project_root: &Path,
    intent: &str,
    pivot_scores: Vec<PivotScore>,
    token_budget: usize,
    branch: Option<&str>,
    content_policy: &ContentPolicy,
) -> Result<(String, String), QueryError> {
    let profile = detect_intent_profile(intent);
    let policy = derive_traversal_policy(&profile);
    let intent_header = format_intent_header(&profile, intent);
    let filtered: Vec<PivotScore> = pivot_scores.into_iter()
        .filter(|ps| !content_policy.is_denied_by_fqn(&ps.fqn))
        .collect();
    build_context_brief(conn, project_root, &intent_header, &filtered, &policy, token_budget, Some(intent), branch, content_policy)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn get_context_with_pivots(
    conn: &Connection,
    project_root: &Path,
    intent: &str,
    pivot_fqns: &[String],
    token_budget: usize,
    branch: Option<&str>,
    content_policy: &ContentPolicy,
) -> Result<(String, String), QueryError> {
    let profile = detect_intent_profile(intent);
    let policy = derive_traversal_policy(&profile);
    let intent_header = format_intent_header(&profile, intent);

    let mut pivot_scores: Vec<PivotScore> = Vec::new();
    for fqn in pivot_fqns {
        if content_policy.is_denied_by_fqn(fqn) { continue; }
        if let Some(id) = conn.query_row(
            "SELECT id FROM symbols WHERE fqn = ?1",
            params![fqn],
            |r| r.get::<_, i64>(0),
        ).optional()? {
            pivot_scores.push(PivotScore {
                id,
                fqn: fqn.clone(),
                reason: SelectionReason::CallerSupplied,
            });
        }
    }

    if pivot_scores.is_empty() {
        let mut output = intent_header;
        output.push_str(NO_SYMBOLS_FOR_FQNS);
        return Ok((output, String::new()));
    }

    build_context_brief(conn, project_root, &intent_header, &pivot_scores, &policy, token_budget, Some(intent), branch, content_policy)
}

// --- Workspace-aware federated queries ---

/// Find pivot symbols across all workspace members.
/// Tags results with `(member_index, pivot_score)`.
/// Interleaves local-first: up to 60% from local, rest round-robin from remotes.
pub(crate) fn find_pivot_symbols_multi(
    workspace: &Workspace,
    intent: &str,
    file_hints: &[String],
    pool_size: usize,
) -> Result<Vec<TaggedPivotScore>, QueryError> {
    let members = workspace.all_read_conns();
    let mut per_member: Vec<(usize, Vec<PivotScore>)> = Vec::new();

    for m in &members {
        let pivots = find_pivot_symbols(m.conn, intent, file_hints, pool_size)?;
        if !pivots.is_empty() {
            per_member.push((m.index, pivots));
        }
    }

    if per_member.is_empty() {
        return Ok(vec![]);
    }

    // Separate local (index 0) from remotes (keep per-member grouping for round-robin)
    let local_pivots: Vec<&PivotScore> = per_member
        .iter()
        .filter(|(idx, _)| *idx == 0)
        .flat_map(|(_, ps)| ps.iter())
        .collect();
    let remote_groups: Vec<&(usize, Vec<PivotScore>)> = per_member
        .iter()
        .filter(|(idx, _)| *idx != 0)
        .collect();
    let has_remotes = !remote_groups.is_empty();

    let local_limit = if !has_remotes {
        pool_size
    } else {
        // At least 1 remote slot when pool_size >= 2
        let max_local = pool_size * 60 / 100;
        max_local.min(pool_size.saturating_sub(1))
    };

    let mut result: Vec<TaggedPivotScore> = Vec::new();
    let mut seen: HashSet<(usize, i64)> = HashSet::new();

    // Take local pivots first
    for ps in &local_pivots {
        if result.len() >= local_limit {
            break;
        }
        if seen.insert((0, ps.id)) {
            result.push(TaggedPivotScore { pivot: (*ps).clone(), member_index: 0 });
        }
    }

    // Fill remaining from remotes using true round-robin across members
    if has_remotes {
        let mut cursors: Vec<usize> = vec![0; remote_groups.len()];
        let mut exhausted = 0;
        while result.len() < pool_size && exhausted < remote_groups.len() {
            exhausted = 0;
            for (gi, (idx, pivots)) in remote_groups.iter().enumerate() {
                let idx = *idx;
                if result.len() >= pool_size { break; }
                // Advance cursor for this group until we find an unseen pivot or exhaust
                while cursors[gi] < pivots.len() {
                    let ps = &pivots[cursors[gi]];
                    cursors[gi] += 1;
                    if seen.insert((idx, ps.id)) {
                        result.push(TaggedPivotScore { pivot: ps.clone(), member_index: idx });
                        break;
                    }
                }
                if cursors[gi] >= pivots.len() {
                    exhausted += 1;
                }
            }
        }
    }

    Ok(result)
}

/// Build context brief from tagged pivots across workspace members.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_context_brief_multi(
    workspace: &Workspace,
    intent_header: &str,
    tagged_pivots: &[TaggedPivotScore],
    policy: &TraversalPolicy,
    token_budget: usize,
    intent_query: Option<&str>,
    branch: Option<&str>,
    content_policy: &ContentPolicy,
) -> Result<(String, String), QueryError> {
    let members = workspace.all_read_conns();
    let mut output = intent_header.to_string();

    if tagged_pivots.is_empty() {
        output.push_str(NO_SYMBOLS_IN_INDEX);
        return Ok((output, String::new()));
    }

    let pivot_budget = token_budget * 70 / 100;
    let skeleton_budget = token_budget * 20 / 100;

    output.push_str("\n## Pivot Symbols\n\n");

    let mut pivot_tokens = 0usize;
    // Only local fqns/file_paths for memory injection — remote identifiers must NOT
    // pollute local observation queries (story: "memory local-only").
    let mut local_fqns: HashSet<String> = HashSet::new();
    let mut local_file_paths: HashSet<String> = HashSet::new();
    let mut rendered_pivot_ids: HashSet<(usize, i64)> = HashSet::new();

    for tp in tagged_pivots {
        let m = match members.iter().find(|m| m.index == tp.member_index) {
            Some(m) => m,
            None => continue,
        };

        let Some(row) = load_symbol_row(m.conn, tp.pivot.id)? else { continue };
        if is_sensitive(&row.file_path) { continue; }
        if content_policy.is_denied(&row.file_path, Some(&row.fqn)) { continue; }

        // Only collect identifiers from local repo for memory matching
        if tp.member_index == 0 {
            local_fqns.insert(row.fqn.clone());
            local_file_paths.insert(row.file_path.clone());
        }

        let source = if content_policy.is_redacted(&row.file_path, Some(&row.fqn)) {
            format!("{}\n  [redacted by policy]", row.signature.as_deref().unwrap_or("(signature unavailable)"))
        } else {
            match read_symbol_source(m.project_root, &row.file_path, row.start_line, row.end_line) {
                Ok(s) if !s.is_empty() => s,
                _ => row.signature.as_deref().unwrap_or("(source unavailable)").to_string(),
            }
        };

        let entry = if tp.member_index != 0 {
            format!(
                "## {} (`{}`) [{}]\nFile: `{}` lines {}-{}\n\n```\n{}\n```\n\n",
                row.name, row.fqn, m.label, row.file_path, row.start_line, row.end_line, source
            )
        } else {
            format_pivot_entry(&row, &source)
        };
        let entry_tokens = estimate_tokens(&entry);
        if pivot_tokens + entry_tokens > pivot_budget { break; }
        output.push_str(&entry);
        pivot_tokens += entry_tokens;
        rendered_pivot_ids.insert((tp.member_index, tp.pivot.id));
    }

    // Supporting symbols — BFS per member (local only for traversal, per story scope)
    // Only do BFS for local pivots since cross-repo traversal is out of scope
    let local_pivots: Vec<i64> = tagged_pivots
        .iter()
        .filter(|tp| tp.member_index == 0)
        .map(|tp| tp.pivot.id)
        .collect();

    if !local_pivots.is_empty() {
        let local = &members[0];
        let (_, supporting_with_reasons) = traverse_bfs(local.conn, &local_pivots, policy)?;
        if !supporting_with_reasons.is_empty() {
            output.push_str("## Supporting Symbols\n\n");
            let mut skeleton_tokens = 0usize;
            for (id, reason) in &supporting_with_reasons {
                let Some(row) = load_symbol_row(local.conn, *id)? else { continue };
                if is_sensitive(&row.file_path) { continue; }
                if content_policy.is_denied(&row.file_path, Some(&row.fqn)) { continue; }

                local_fqns.insert(row.fqn.clone());
                local_file_paths.insert(row.file_path.clone());

                let skeleton = skeletonize(local.conn, row.id)?;
                let reason_line = format!("Why: {reason}\n");
                let skeleton_tokens_needed = estimate_tokens(&skeleton);
                let reason_tokens_needed = estimate_tokens(&reason_line);

                if skeleton_tokens + skeleton_tokens_needed > skeleton_budget { break; }
                output.push_str(&skeleton);
                skeleton_tokens += skeleton_tokens_needed;

                if skeleton_tokens + reason_tokens_needed <= skeleton_budget {
                    output.push_str(&reason_line);
                    skeleton_tokens += reason_tokens_needed;
                }
            }
        }
    }

    // Memory + Rules injection — local only (10% budget total)
    let local_conn = &members[0].conn;
    let fqns_vec: Vec<String> = local_fqns.iter().cloned().collect();
    let file_paths_vec: Vec<String> = local_file_paths.iter().cloned().collect();
    let fqns: Vec<&str> = local_fqns.iter().map(|s| s.as_str()).collect();
    let file_paths: Vec<&str> = local_file_paths.iter().map(|s| s.as_str()).collect();

    // Rules are local-only — do NOT query remote workspace DBs
    let rules = crate::memory::rules::get_active_rules(local_conn, &fqns_vec, &file_paths_vec, branch, 5)
        .unwrap_or_default();
    let (rules_budget, memory_budget) = if rules.is_empty() {
        (0, token_budget * 10 / 100)
    } else {
        (std::cmp::min(token_budget * 5 / 100, 300), token_budget * 5 / 100)
    };

    // Carve project sub-budget from memory budget (AC3/AC10: local-only)
    let project_sub_budget = std::cmp::min(memory_budget * 20 / 100, 200);
    let anchored_memory_budget = memory_budget - project_sub_budget;

    // Create embedder once for both anchored and project-scoped scoring
    let embedder = create_embedder(workspace.local_root());
    let embedder_ref = embedder.as_ref().map(|e| e.as_ref());
    let mut mem_section = String::new();

    if !fqns.is_empty() || !file_paths.is_empty() {
        let scored = crate::memory::store::get_scored_observations_for_context(
            local_conn, &fqns, &file_paths, 50, intent_query, branch, content_policy,
            embedder_ref,
        )
        .unwrap_or_default();

        if !scored.is_empty() {
            let mut mem_tokens = 0usize;
            for scored_obs in &scored {
                let entry = format_scored_observation_entry(scored_obs);
                let entry_tokens = estimate_tokens(&entry);
                if mem_tokens + entry_tokens > anchored_memory_budget { break; }
                mem_section.push_str(&entry);
                mem_tokens += entry_tokens;
            }
        }
    }

    // Project-scoped observations — local_conn only (AC10)
    if project_sub_budget > 0
        && let Ok(project_obs) = crate::memory::store::get_project_scoped_observations(local_conn, branch, 20)
        && !project_obs.is_empty()
    {
        let intent_tokens: std::collections::HashSet<String> = intent_query
            .map(|q| crate::memory::rules::extract_tokens(q).into_iter().collect())
            .unwrap_or_default();
        let min_matches = if intent_tokens.len() < 2 { 1 } else { 2 };

        // Token overlap gate: cheap pre-filter
        let mut scored_project: Vec<(usize, &crate::memory::store::ObservationRow)> = project_obs.iter()
            .filter_map(|obs| {
                let obs_tokens: std::collections::HashSet<String> =
                    crate::memory::rules::extract_tokens(&obs.content).into_iter().collect();
                let matching = obs_tokens.intersection(&intent_tokens).count();
                if matching >= min_matches { Some((matching, obs)) } else { None }
            })
            .collect();

        // When embeddings available, use semantic similarity for sort; else fall back to token count
        let semantic_sort = compute_project_semantic_scores(
            local_conn, &scored_project.iter().map(|(_, o)| *o).collect::<Vec<_>>(),
            intent_query, embedder_ref,
        );
        if let Some(ref sem) = semantic_sort {
            scored_project.sort_by(|a, b| {
                let sa = sem.get(&a.1.id).copied().unwrap_or(0.0);
                let sb = sem.get(&b.1.id).copied().unwrap_or(0.0);
                sb.total_cmp(&sa).then_with(|| b.1.created_at.cmp(&a.1.created_at))
            });
        } else {
            scored_project.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.created_at.cmp(&a.1.created_at)));
        }

        let mut project_tokens = 0usize;
        for (_score, obs) in &scored_project {
            let entry = format!("- [project] [{}] {} ({})\n",
                obs.kind,
                obs.content,
                if obs.is_stale { "stale" } else { "active" },
            );
            let entry_tokens = estimate_tokens(&entry);
            if project_tokens + entry_tokens > project_sub_budget {
                break;
            }
            mem_section.push_str(&entry);
            project_tokens += entry_tokens;
        }
    }

    if !mem_section.is_empty() {
        output.push_str("## Session Memory\n\n");
        output.push_str(&mem_section);
    }

    // Rules injection — local only
    if !rules.is_empty() && rules_budget > 0 {
        let mut rules_section = String::new();
        let mut rules_tokens = 0usize;
        for rule in &rules {
            let age_secs = now_secs() - rule.last_seen_at;
            let age_label = if age_secs < 3600 {
                format!("{}m ago", age_secs / 60)
            } else if age_secs < 86400 {
                format!("{}h ago", age_secs / 3600)
            } else {
                format!("{}d ago", age_secs / 86400)
            };
            let entry = format!(
                "- {} [{} obs · {} sessions · last seen {}]\n",
                rule.content, rule.support_count, rule.session_count, age_label
            );
            let entry_tokens = estimate_tokens(&entry);
            if rules_tokens + entry_tokens > rules_budget { break; }
            rules_section.push_str(&entry);
            rules_tokens += entry_tokens;
        }
        if !rules_section.is_empty() {
            output.push_str("## Project Rules\n\n");
            output.push_str(&rules_section);
        }
    }

    // Build retrieval notes with workspace labels
    let intent_label = extract_intent_label(intent_header);
    let labels: Vec<(usize, &str)> = members.iter().map(|m| (m.index, m.label)).collect();
    let rendered: Vec<(&TaggedPivotScore, bool)> = tagged_pivots.iter()
        .filter(|tp| rendered_pivot_ids.contains(&(tp.member_index, tp.pivot.id)))
        .map(|tp| (tp, tp.member_index == 0))
        .collect();
    let omitted = tagged_pivots.len() - rendered.len();
    let retrieval_notes = format_retrieval_notes_multi(&rendered, &intent_label, omitted, &labels);

    Ok((output, retrieval_notes))
}

/// Top-level workspace-aware context retrieval.
pub(crate) fn get_context_workspace(
    workspace: &Workspace,
    intent: &str,
    file_hints: &[String],
    token_budget: usize,
    branch: Option<&str>,
    content_policy: &ContentPolicy,
) -> Result<(String, String), QueryError> {
    let profile = detect_intent_profile(intent);
    let policy = derive_traversal_policy(&profile);
    let intent_header = format_intent_header(&profile, intent);
    let mut tagged_pivots = find_pivot_symbols_multi(workspace, intent, file_hints, policy.pivot_pool_size)?;
    tagged_pivots.retain(|tp| !content_policy.is_denied_by_fqn(&tp.pivot.fqn));
    build_context_brief_multi(workspace, &intent_header, &tagged_pivots, &policy, token_budget, Some(intent), branch, content_policy)
}

/// Private helper: query candidate file paths from the files table.
fn query_file_candidates(
    conn: &Connection,
    where_clause: &str,
    param: &str,
) -> Result<Vec<String>, QueryError> {
    let sql = format!("SELECT path FROM files {where_clause} ORDER BY path");
    let mut stmt = conn.prepare(&sql)?;
    let paths = stmt.query_map(params![param], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(paths)
}

pub(crate) fn get_file_skeleton(conn: &Connection, file_path: &str, content_policy: &ContentPolicy) -> Result<String, QueryError> {
    // Input-level sensitive check — returns "not permitted" to the caller
    if is_sensitive(file_path) {
        return Ok(format!("Access to sensitive file '{file_path}' is not permitted.\n"));
    }

    // Content policy deny — return "not found" so denied files are invisible
    if content_policy.is_denied(file_path, None) {
        return Ok(format!("No file found matching: {file_path}\n\nEnsure the file is indexed with `olaf index`.\n"));
    }

    // Stage 1: exact file match
    let mut candidates = query_file_candidates(conn, "WHERE path = ?1", file_path)?;

    // Stage 2: suffix match only if no exact match
    if candidates.is_empty() {
        let suffix = format!("%{file_path}");
        candidates = query_file_candidates(conn, "WHERE path LIKE ?1", &suffix)?;
    }

    // Sensitive filter on candidates: silently remove — don't reveal that sensitive paths exist
    candidates.retain(|p| !is_sensitive(p));

    // Content policy deny filter on candidates
    candidates.retain(|p| !content_policy.is_denied(p, None));

    if candidates.is_empty() {
        return Ok(format!(
            "No file found matching: {file_path}\n\nEnsure the file is indexed with `olaf index`.\n"
        ));
    }
    if candidates.len() > 1 {
        return Ok(format!(
            "Multiple files match '{file_path}':\n{}\nProvide a more specific path.\n",
            candidates.iter().map(|p| format!("  {p}")).collect::<Vec<_>>().join("\n")
        ));
    }
    let resolved_path = &candidates[0];

    // Fetch symbols for the single resolved file
    let mut stmt = conn.prepare(
        "SELECT s.id FROM symbols s JOIN files f ON f.id=s.file_id
         WHERE f.path = ?1 ORDER BY s.start_line",
    )?;
    let symbol_ids: Vec<i64> = stmt
        .query_map(params![resolved_path], |r| r.get(0))?
        .collect::<Result<_, _>>()?;

    if symbol_ids.is_empty() {
        return Ok(format!(
            "No symbols found in file: {resolved_path}. The file may not contain indexable symbols.\n"
        ));
    }

    let mut output = format!("# File Skeleton: {resolved_path}\n\n");
    for id in symbol_ids {
        // Check per-symbol redaction by loading the FQN
        if let Some(row) = load_symbol_row(conn, id)? {
            if content_policy.is_denied(resolved_path, Some(&row.fqn)) {
                continue;
            }
            if content_policy.is_redacted(resolved_path, Some(&row.fqn)) {
                output.push_str(&format!(
                    "{}\n  [redacted by policy]\n\n",
                    row.signature.as_deref().unwrap_or(&row.fqn)
                ));
                continue;
            }
        }
        output.push_str(&crate::graph::skeleton::skeletonize(conn, id)?);
    }
    Ok(output)
}

pub(crate) fn index_status(conn: &Connection) -> Result<String, QueryError> {
    let stats = crate::graph::store::load_db_stats(conn)?;

    let last_indexed = match stats.last_indexed_at {
        None => return Ok("Index not initialized. Run `olaf index` first.\n".to_string()),
        Some(ts) => chrono::DateTime::from_timestamp(ts, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| ts.to_string()),
    };

    Ok(format!(
        "Files indexed:  {}\nSymbols:        {}\nEdges:          {}\nObservations:   {}\nLast indexed:   {}\n",
        stats.files, stats.symbols, stats.edges, stats.observations, last_indexed
    ))
}

const MAX_IMPACT_PER_HOP: usize = 100;
const MAX_IMPACT_DEPTH: usize = 10;

pub(crate) fn get_impact(
    conn: &Connection,
    symbol_fqn: &str,
    depth: usize,
    content_policy: &ContentPolicy,
) -> Result<String, QueryError> {
    // Direct-query guard: denied FQN (by fqn_prefix or path rules) returns "not found"
    if content_policy.is_denied_by_fqn(symbol_fqn) {
        return Ok(format!("Symbol not found: {symbol_fqn}\n\nRun `olaf index` first."));
    }

    let depth = depth.min(MAX_IMPACT_DEPTH);
    let symbol_id: Option<i64> = conn.query_row(
        "SELECT id FROM symbols WHERE fqn = ?1",
        params![symbol_fqn],
        |r| r.get(0),
    ).optional()?;

    let Some(symbol_id) = symbol_id else {
        return Ok(format!(
            "Symbol not found: {symbol_fqn}\n\nRun `olaf index` first."
        ));
    };

    let mut visited: HashSet<i64> = HashSet::from([symbol_id]);
    let mut queue: VecDeque<(i64, usize)> = VecDeque::from([(symbol_id, 0)]);
    let mut results: Vec<(String, String, String, String, usize)> = Vec::new(); // fqn, name, path, kind, depth
    let mut truncated = false;

    let mut impact_stmt = conn.prepare(
        "SELECT DISTINCT s.id, s.fqn, s.name, f.path, e.kind
         FROM edges e JOIN symbols s ON s.id=e.source_id JOIN files f ON f.id=s.file_id
         WHERE e.target_id=?1
           AND e.kind IN ('calls', 'extends', 'implements', 'uses_type')
         LIMIT ?2"
    )?;
    while let Some((current_id, current_depth)) = queue.pop_front() {
        if current_depth >= depth { continue; }
        let rows: Vec<(i64, String, String, String, String)> = impact_stmt.query_map(
            params![current_id, MAX_IMPACT_PER_HOP as i64],
            |r| Ok((r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
                    r.get::<_,String>(3)?, r.get::<_,String>(4)?))
        )?.collect::<Result<_,_>>()?;
        if rows.len() == MAX_IMPACT_PER_HOP { truncated = true; }
        for (id, fqn, name, path, kind) in rows {
            if visited.insert(id) {
                queue.push_back((id, current_depth + 1));
                if !is_sensitive(&path) && !content_policy.is_denied(&path, Some(&fqn)) {
                    results.push((fqn, name, path, kind, current_depth + 1));
                }
            }
        }
    }

    let mut output = format!("# Impact Analysis: {symbol_fqn}\n\n");
    output.push_str(&format!(
        "{} direct and transitive dependent(s) found (depth={depth})\n",
        results.len()
    ));
    output.push_str("Note: traverses calls, extends, implements, and uses_type edges. Import relationships are not yet tracked at symbol level.\n\n");

    if results.is_empty() {
        output.push_str("No dependents found.\n");
    } else {
        // Group by depth
        let mut by_depth: std::collections::BTreeMap<usize, Vec<(&str, &str, &str, &str)>> = Default::default();
        for (fqn, name, path, kind, d) in &results {
            by_depth.entry(*d).or_default().push((fqn, name, path, kind));
        }
        for (d, items) in &by_depth {
            output.push_str(&format!("### Depth {d}\n\n"));
            for (fqn, name, path, kind) in items {
                output.push_str(&format!("- {name} ({kind}) in {path}\n  FQN: {fqn}\n"));
            }
            output.push('\n');
        }
    }

    if truncated {
        output.push_str(&format!(
            "⚠ Results truncated: ≥{MAX_IMPACT_PER_HOP} dependents per hop — use a narrower symbol or reduce depth\n"
        ));
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn detect_intent(intent: &str) -> IntentMode {
        detect_intent_profile(intent).dominant_mode
    }

    /// Minimal in-memory DB for legacy BFS/pivot tests (no kind column on edges).
    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT NOT NULL);
             CREATE TABLE symbols (
                 id INTEGER PRIMARY KEY, fqn TEXT NOT NULL, name TEXT NOT NULL,
                 file_id INTEGER NOT NULL, start_line INTEGER NOT NULL,
                 end_line INTEGER NOT NULL, signature TEXT
             );
             CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL);",
        ).unwrap();
        conn
    }

    /// Full-schema in-memory DB matching the real migration schema.
    fn build_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT NOT NULL, hash TEXT);
             CREATE TABLE symbols (
                 id INTEGER PRIMARY KEY, file_id INTEGER NOT NULL, fqn TEXT NOT NULL,
                 name TEXT NOT NULL, kind TEXT, start_line INTEGER NOT NULL,
                 end_line INTEGER NOT NULL, signature TEXT, docstring TEXT, source_hash TEXT
             );
             CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT);",
        ).unwrap();
        conn
    }

    #[test]
    fn detect_intent_word_boundary_no_false_positive() {
        assert!(matches!(detect_intent("prefix the code"), IntentMode::Balanced));
        assert!(matches!(detect_intent("address the issue"), IntentMode::Balanced));
    }

    #[test]
    fn detect_intent_bugfix_signals() {
        assert!(matches!(detect_intent("fix the bug"), IntentMode::BugFix));
        assert!(matches!(detect_intent("debug the crash"), IntentMode::BugFix));
        assert!(matches!(detect_intent("there is an error"), IntentMode::BugFix));
        assert!(matches!(detect_intent("app crash on startup"), IntentMode::BugFix));
        assert!(matches!(detect_intent("bug in parser"), IntentMode::BugFix));
    }

    #[test]
    fn detect_intent_refactor_signals() {
        assert!(matches!(detect_intent("refactor the module"), IntentMode::Refactor));
        assert!(matches!(detect_intent("rename this function"), IntentMode::Refactor));
        assert!(matches!(detect_intent("restructure the auth"), IntentMode::Refactor));
        assert!(matches!(detect_intent("extract helper"), IntentMode::Refactor));
    }

    #[test]
    fn detect_intent_implementation_signals() {
        assert!(matches!(detect_intent("add new feature"), IntentMode::Implementation));
        assert!(matches!(detect_intent("implement the handler"), IntentMode::Implementation));
        assert!(matches!(detect_intent("create a new endpoint"), IntentMode::Implementation));
        assert!(matches!(detect_intent("build the api"), IntentMode::Implementation));
    }

    #[test]
    fn detect_intent_unknown_returns_balanced() {
        assert!(matches!(detect_intent("optimize performance"), IntentMode::Balanced));
        assert!(matches!(detect_intent("understand the codebase"), IntentMode::Balanced));
        assert!(matches!(detect_intent(""), IntentMode::Balanced));
    }

    #[test]
    fn detect_intent_tie_resolved_by_priority() {
        // "refactor" (score 1) + "fix"+"bug" (score 2) → BugFix wins
        assert!(matches!(detect_intent("refactor and fix the bug"), IntentMode::BugFix));
        // "refactor" (score 1) + "add" (score 1) → Refactor wins (BugFix=0)
        assert!(matches!(detect_intent("refactor and add feature"), IntentMode::Refactor));
    }

    fn insert_symbol(conn: &Connection, id: i64, fqn: &str, name: &str, file_id: i64) {
        conn.execute(
            "INSERT INTO symbols (id, fqn, name, file_id, start_line, end_line) VALUES (?1, ?2, ?3, ?4, 1, 10)",
            params![id, fqn, name, file_id],
        ).unwrap();
    }

    #[test]
    fn traverse_bfs_bugfix_ranks_callers_before_outbound() {
        // AC #1: inbound callers must appear before outbound deps in BugFix mode
        let conn = setup_db();
        conn.execute("INSERT INTO files (id, path) VALUES (1, 'src/lib.rs')", []).unwrap();
        insert_symbol(&conn, 1, "lib::pivot", "pivot", 1);
        insert_symbol(&conn, 2, "lib::caller", "caller", 1);
        insert_symbol(&conn, 3, "lib::dep", "dep", 1);
        conn.execute("INSERT INTO edges (source_id, target_id) VALUES (2, 1)", []).unwrap(); // caller→pivot (inbound)
        conn.execute("INSERT INTO edges (source_id, target_id) VALUES (1, 3)", []).unwrap(); // pivot→dep (outbound)

        let policy = TraversalPolicy { depth: 2, include_inbound: true, inbound_first: true, pivot_pool_size: 5 };
        let (_, supporting) = traverse_bfs(&conn, &[1], &policy).unwrap();
        assert!(supporting.iter().any(|(id, _)| *id == 2), "BugFix must include inbound caller");
        assert!(supporting.iter().any(|(id, _)| *id == 3), "BugFix must include outbound dep");
        let caller_pos = supporting.iter().position(|(id, _)| *id == 2).unwrap();
        let dep_pos = supporting.iter().position(|(id, _)| *id == 3).unwrap();
        assert!(caller_pos < dep_pos, "inbound caller must rank before outbound dependency in BugFix mode");
    }

    #[test]
    fn traverse_bfs_refactor_includes_inbound_and_outbound() {
        // AC #2: Refactor includes both inbound callers and outbound deps
        let conn = setup_db();
        conn.execute("INSERT INTO files (id, path) VALUES (1, 'src/lib.rs')", []).unwrap();
        insert_symbol(&conn, 1, "lib::pivot", "pivot", 1);
        insert_symbol(&conn, 2, "lib::caller", "caller", 1);
        insert_symbol(&conn, 3, "lib::dep", "dep", 1);
        conn.execute("INSERT INTO edges (source_id, target_id) VALUES (2, 1)", []).unwrap(); // inbound
        conn.execute("INSERT INTO edges (source_id, target_id) VALUES (1, 3)", []).unwrap(); // outbound

        let policy = TraversalPolicy { depth: 3, include_inbound: true, inbound_first: false, pivot_pool_size: 5 };
        let (_, supporting) = traverse_bfs(&conn, &[1], &policy).unwrap();
        assert!(supporting.iter().any(|(id, _)| *id == 2), "Refactor must include inbound callers");
        assert!(supporting.iter().any(|(id, _)| *id == 3), "Refactor must include outbound deps");
    }

    #[test]
    fn traverse_bfs_implementation_excludes_inbound() {
        let conn = setup_db();
        conn.execute("INSERT INTO files (id, path) VALUES (1, 'src/lib.rs')", []).unwrap();
        insert_symbol(&conn, 1, "lib::pivot", "pivot", 1);
        insert_symbol(&conn, 2, "lib::caller", "caller", 1);
        conn.execute("INSERT INTO edges (source_id, target_id) VALUES (2, 1)", []).unwrap(); // inbound

        let policy = TraversalPolicy { depth: 2, include_inbound: false, inbound_first: false, pivot_pool_size: 5 };
        let (_, supporting) = traverse_bfs(&conn, &[1], &policy).unwrap();
        assert!(!supporting.iter().any(|(id, _)| *id == 2), "Implementation mode should exclude inbound callers");
    }

    #[test]
    fn get_context_response_contains_intent_mode_line() {
        // AC #5: real get_context output must contain the intent_mode header line
        let conn = setup_db();
        conn.execute("INSERT INTO files (id, path) VALUES (1, 'src/lib.rs')", []).unwrap();
        conn.execute(
            "INSERT INTO symbols (id, fqn, name, file_id, start_line, end_line, signature) VALUES (1, 'lib::pivot', 'pivot', 1, 1, 10, 'fn pivot()')",
            [],
        ).unwrap();

        let root = std::path::Path::new("/nonexistent");
        let (result, _notes) = get_context(&conn, root, "fix the crash", &[], 4000, None, &ContentPolicy::default()).unwrap();
        assert!(result.contains("intent_mode: bug-fix\n"), "get_context output must include intent_mode header line");
        assert!(result.contains("intent_confidence:"), "get_context output must include intent_confidence");
        assert!(result.contains("intent_signals:"), "get_context output must include intent_signals");
    }

    // --- IntentProfile scoring tests ---

    #[test]
    fn profile_single_category_high_confidence() {
        let p = detect_intent_profile("fix crash");
        assert_eq!(p.bugfix_score, 2); // "fix" + "crash"
        assert_eq!(p.total, 2);
        assert!((p.confidence - 1.00).abs() < 0.01, "confidence={}", p.confidence);
        assert!(matches!(p.dominant_mode, IntentMode::BugFix));
        assert!(matches!(p.execution_mode, IntentMode::BugFix));
        assert!(p.matched_signals.contains(&"fix".to_string()));
        assert!(p.matched_signals.contains(&"crash".to_string()));
    }

    #[test]
    fn profile_single_signal_no_regression() {
        // Regression guard: a single clear signal must NOT fall back to Balanced.
        let p = detect_intent_profile("fix the null pointer");
        assert_eq!(p.bugfix_score, 1);
        assert_eq!(p.total, 1);
        assert!((p.confidence - 1.00).abs() < 0.01, "confidence={}", p.confidence);
        assert!(matches!(p.execution_mode, IntentMode::BugFix));
    }

    #[test]
    fn profile_mixed_two_categories_low_confidence() {
        let p = detect_intent_profile("refactor and fix");
        assert_eq!(p.bugfix_score, 1);
        assert_eq!(p.refactor_score, 1);
        assert_eq!(p.total, 2);
        assert!((p.confidence - 0.50).abs() < 0.01, "confidence={}", p.confidence);
        // dominant_mode: BugFix (tie-break); execution_mode: Balanced (confidence=0.50 < 0.60)
        assert!(matches!(p.dominant_mode, IntentMode::BugFix));
        assert!(matches!(p.execution_mode, IntentMode::Balanced));
    }

    #[test]
    fn fallback_reason_emitted_when_mode_detected_but_low_confidence() {
        let profile = detect_intent_profile("refactor and fix"); // dominant=BugFix, execution=Balanced
        let intent_label = match &profile.execution_mode {
            IntentMode::BugFix => "bug-fix",
            IntentMode::Refactor => "refactor",
            IntentMode::Implementation => "implementation",
            IntentMode::Balanced => "balanced",
        };
        let mut header = format!("# Context Brief: test\nintent_mode: {intent_label}\n");
        if matches!(profile.execution_mode, IntentMode::Balanced)
            && !matches!(profile.dominant_mode, IntentMode::Balanced)
        {
            header.push_str("intent_fallback_reason: low confidence\n");
        }
        assert!(header.contains("intent_fallback_reason: low confidence"),
            "must emit fallback reason when dominant_mode != Balanced but execution_mode == Balanced");
    }

    #[test]
    fn fallback_reason_absent_when_genuinely_balanced() {
        let profile = detect_intent_profile("show me the auth module"); // total=0, both modes Balanced
        let mut header = "# Context Brief: test\n".to_string();
        if matches!(profile.execution_mode, IntentMode::Balanced)
            && !matches!(profile.dominant_mode, IntentMode::Balanced)
        {
            header.push_str("intent_fallback_reason: low confidence\n");
        }
        assert!(!header.contains("intent_fallback_reason"),
            "must NOT emit fallback reason when no signals were detected");
    }

    #[test]
    fn profile_three_categories_even_split() {
        let p = detect_intent_profile("refactor fix add");
        assert_eq!(p.total, 3);
        assert!((p.confidence - 0.33).abs() < 0.01);
        assert!(matches!(p.execution_mode, IntentMode::Balanced));
    }

    #[test]
    fn profile_no_signals_zero_confidence() {
        let p = detect_intent_profile("show me the auth module");
        assert_eq!(p.total, 0);
        assert_eq!(p.confidence, 0.0);
        assert!(matches!(p.dominant_mode, IntentMode::Balanced));
        assert!(matches!(p.execution_mode, IntentMode::Balanced));
        assert_eq!(p.matched_signals, Vec::<String>::new());
    }

    #[test]
    fn profile_signals_are_canonical_keywords_not_raw_input() {
        // Input token "debugging" does NOT match keyword "debug" (exact match only)
        let p = detect_intent_profile("debugging the session");
        assert_eq!(p.total, 0, "partial stem 'debugging' must not match 'debug'");
    }

    // --- TraversalPolicy derivation tests ---

    #[test]
    fn policy_high_bugfix_confidence_inbound_first() {
        let p = detect_intent_profile("fix the crash");
        let policy = derive_traversal_policy(&p);
        assert_eq!(policy.depth, 2);
        assert!(policy.include_inbound);
        assert!(policy.inbound_first);
        assert_eq!(policy.pivot_pool_size, DEFAULT_PIVOT_POOL);
    }

    #[test]
    fn policy_dominant_refactor_depth_three() {
        let p = detect_intent_profile("refactor restructure");
        let policy = derive_traversal_policy(&p);
        assert_eq!(policy.depth, 3);
        assert!(policy.include_inbound);
        assert!(!policy.inbound_first); // refactor >= bugfix → outbound first
    }

    #[test]
    fn policy_implementation_outbound_only() {
        let p = detect_intent_profile("implement the cache layer");
        let policy = derive_traversal_policy(&p);
        assert_eq!(policy.depth, 2);
        assert!(!policy.include_inbound);
    }

    #[test]
    fn policy_low_confidence_fallback_widens_pool() {
        let p = detect_intent_profile("refactor and fix"); // confidence ≈ 0.50 < 0.60
        let policy = derive_traversal_policy(&p);
        assert!(!policy.include_inbound, "low confidence must use balanced (outbound only)");
        assert_eq!(policy.pivot_pool_size, LOW_CONFIDENCE_PIVOT_POOL);
    }

    // --- BFS direction tests (use build_test_db for full-schema inserts) ---

    #[test]
    fn traverse_policy_bugfix_inbound_before_outbound() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pivot','pivot','fn',1,5,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'caller','caller','fn',6,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (3,1,'dep','dep','fn',11,15,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,2,1,'calls')", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (2,1,3,'calls')", []).unwrap();

        let policy = TraversalPolicy { depth: 2, include_inbound: true, inbound_first: true, pivot_pool_size: 5 };
        let (_pivots, supporting) = traverse_bfs(&conn, &[1], &policy).unwrap();
        assert_eq!(supporting[0].0, 2, "caller must rank before dependency for BugFix");
        assert_eq!(supporting[0].1, "inbound caller of pivot");
        let dep_entry = supporting.iter().find(|(id, _)| *id == 3).unwrap();
        assert_eq!(dep_entry.1, "outbound dependency of pivot");
    }

    #[test]
    fn traverse_policy_implementation_no_inbound() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pivot','pivot','fn',1,5,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'caller','caller','fn',6,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (3,1,'dep','dep','fn',11,15,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,2,1,'calls')", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (2,1,3,'calls')", []).unwrap();

        let policy = TraversalPolicy { depth: 2, include_inbound: false, inbound_first: false, pivot_pool_size: 5 };
        let (_pivots, supporting) = traverse_bfs(&conn, &[1], &policy).unwrap();
        assert!(supporting.iter().all(|(id, _)| *id != 2), "caller must be absent in Implementation mode");
        assert!(supporting.iter().any(|(id, _)| *id == 3));
    }

    // --- Budget rule: reasons dropped before symbols ---

    #[test]
    fn reason_dropped_before_symbol_when_budget_tight() {
        // Simulate the budget loop directly.
        // Budget is just enough for the skeleton but not the reason.
        let skeleton = "fn foo() {}\n";
        let reason = "outbound dependency of pivot";
        let reason_line = format!("Why: {reason}\n");
        let skeleton_tokens = estimate_tokens(skeleton);
        let reason_tokens = estimate_tokens(&reason_line);
        // Budget = exactly skeleton_tokens; reason must NOT fit.
        let budget = skeleton_tokens;
        assert!(skeleton_tokens <= budget, "skeleton must fit");
        assert!(
            skeleton_tokens + reason_tokens > budget,
            "reason must not fit given this budget"
        );
        // Verify per-entry logic: symbol included, reason excluded
        let mut output = String::new();
        let mut used = 0usize;
        if used + skeleton_tokens <= budget {
            output.push_str(skeleton);
            used += skeleton_tokens;
        }
        if used + reason_tokens <= budget {
            output.push_str(&reason_line);
        }
        assert!(output.contains("fn foo()"), "symbol must be present");
        assert!(!output.contains("Why:"), "reason must be absent when budget is tight");
    }

    // --- Story 6.2 AC tests: skeletonized adjacent nodes ---

    #[test]
    fn ac1_pivot_symbol_rendered_with_fenced_code_block() {
        // Use a NamedTempFile so the path is unique per test run (no races in parallel tests).
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp, b"fn my_pivot() {\n    // body\n    let x = 1;\n}\n").unwrap();
        let tmp_dir = tmp.path().parent().unwrap().to_path_buf();
        let filename = tmp.path().file_name().unwrap().to_str().unwrap().to_string();

        let conn = build_test_db();
        conn.execute(&format!("INSERT INTO files VALUES (1,'{filename}','h')"), []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'crate::my_pivot','my_pivot','fn',1,4,NULL,NULL,NULL)", []).unwrap();

        let (result, _notes) = get_context(&conn, &tmp_dir, "implement my_pivot", &[], 4000, None, &ContentPolicy::default()).unwrap();

        assert!(result.contains("```"), "AC1: pivot output must contain fenced code block");
        assert!(result.contains("fn my_pivot"), "AC1: pivot source body must appear in output");
        // tmp is dropped here, deleting the file automatically
    }

    #[test]
    fn ac2_supporting_symbol_renders_signature_and_docstring_no_code_block() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'crate::pivot','pivot','fn',1,5,'fn pivot()',NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'crate::helper','helper','fn',6,40,'fn helper(x: i32)','Does the helping.',NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,1,2,'calls')", []).unwrap();

        let root = std::path::Path::new("/nonexistent");
        let (result, _notes) = get_context(&conn, root, "implement pivot", &[], 4000, None, &ContentPolicy::default()).unwrap();

        let supporting_section = result.split("## Supporting Symbols").nth(1).unwrap_or("");
        assert!(supporting_section.contains("Signature:"), "AC2: supporting section must contain Signature line");
        assert!(supporting_section.contains("Does the helping."), "AC2: supporting section must contain docstring text");
        assert!(!supporting_section.contains("```"), "AC2: supporting section must NOT contain fenced code block");
    }

    #[test]
    fn ac3_supporting_no_docstring_emits_no_placeholder() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'crate::pivot','pivot','fn',1,5,'fn pivot()',NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'crate::helper','helper','fn',6,40,'fn helper()',NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,1,2,'calls')", []).unwrap();

        let root = std::path::Path::new("/nonexistent");
        let (result, _notes) = get_context(&conn, root, "implement pivot", &[], 4000, None, &ContentPolicy::default()).unwrap();

        let supporting_section = result.split("## Supporting Symbols").nth(1).unwrap_or("");
        assert!(supporting_section.contains("Signature:"), "AC3: Signature line must be present when sig is set");
        assert!(!supporting_section.contains("No docstring"), "AC3: must not emit 'No docstring' placeholder");
        assert!(!supporting_section.contains("unavailable"), "AC3: must not emit 'unavailable' placeholder");
    }

    #[test]
    fn ac4_pivot_exhausts_budget_zero_supporting_symbols_in_output() {
        // token_budget=50 → pivot_budget=35, skeleton_budget=10.
        // Pivot fallback (no real file) uses sig "fn pivot()" → format_pivot_entry output ≈ 70 chars
        // → ~18 tokens ≤ 35 → pivot IS emitted.
        // Skeleton for dep (sig "fn dep()", no doc) ≈ 70 chars → ~18 tokens > 10 → dep NOT emitted.
        // This proves the ordering: pivot fits and appears, supporting doesn't fit and is absent.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'crate::pivot','pivot','fn',1,5,'fn pivot()',NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'crate::dep','dep','fn',6,40,'fn dep()',NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,1,2,'calls')", []).unwrap();

        let root = std::path::Path::new("/nonexistent");
        let (result, _notes) = get_context(&conn, root, "implement pivot", &[], 50, None, &ContentPolicy::default()).unwrap();

        // Pivot must be present — proves pivot-first ordering was honoured
        assert!(result.contains("## pivot"), "AC4: pivot symbol must appear in output");
        // Supporting dep must be absent — it lost the budget race to the pivot
        assert!(!result.contains("### dep"), "AC4: supporting symbol must not appear when pivot exhausts skeleton budget");
    }

    #[test]
    fn ac5_supporting_section_at_least_30_percent_fewer_chars_than_full_body() {
        // 1 small pivot + 3 supporting symbols with 30-line bodies (end_line - start_line = 29)
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'crate::pivot','pivot','fn',1,5,'fn pivot()',NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'crate::alpha','alpha','fn',1,30,'fn alpha()','Alpha docstring.',NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (3,1,'crate::beta','beta','fn',31,60,'fn beta()','Beta docstring.',NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (4,1,'crate::gamma','gamma','fn',61,90,'fn gamma()','Gamma docstring.',NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,1,2,'calls')", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (2,1,3,'calls')", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (3,1,4,'calls')", []).unwrap();

        let root = std::path::Path::new("/nonexistent");
        let (result, _notes) = get_context(&conn, root, "implement pivot", &[], 50000, None, &ContentPolicy::default()).unwrap();

        let supporting_section = result.split("## Supporting Symbols").nth(1).unwrap_or("");

        // Guard: all 3 supporting symbols must be present (prevents false-pass on empty output)
        assert!(supporting_section.contains("### alpha"), "AC5: alpha must appear in supporting section");
        assert!(supporting_section.contains("### beta"), "AC5: beta must appear in supporting section");
        assert!(supporting_section.contains("### gamma"), "AC5: gamma must appear in supporting section");

        // Hypothetical full-body: 3 symbols × 30 lines × 60 chars/line
        let hypothetical_full_chars = 3 * 30 * 60_usize;
        assert!(
            supporting_section.len() <= hypothetical_full_chars * 70 / 100,
            "AC5: supporting section ({} chars) must be ≥30% fewer than hypothetical full bodies ({} chars, threshold={})",
            supporting_section.len(),
            hypothetical_full_chars,
            hypothetical_full_chars * 70 / 100
        );
    }

    // --- Story 6.4: Hybrid pivot ranking tests ---

    #[test]
    fn pivot_ranking_kw_score_dominates_in_degree() {
        // X matches 2 keywords ("context", "build") but has in_degree=0.
        // Y matches 1 keyword ("pipeline") but has in_degree=5.
        // X must rank first because kw_score=2 > kw_score=1.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        // Symbol X: name matches "context" and "build" via LIKE "%context%" and "%build%"
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::context_builder','context_builder','fn',1,10,NULL,NULL,NULL)", []).unwrap();
        // Symbol Y: name matches "pipeline" via LIKE "%pipeline%"; add 5 callers → in_degree=5
        conn.execute("INSERT INTO symbols VALUES (2,1,'pkg::pipeline_stage','pipeline_stage','fn',11,20,NULL,NULL,NULL)", []).unwrap();
        for i in 10..15_i64 {
            conn.execute(
                &format!("INSERT INTO symbols VALUES ({i},1,'pkg::caller{i}','caller{i}','fn',{},{}+5,NULL,NULL,NULL)", i*100, i*100),
                [],
            ).unwrap();
            conn.execute(&format!("INSERT INTO edges VALUES ({i},{i},2,'calls')"), []).unwrap();
        }

        let result = find_pivot_symbols(&conn, "build context pipeline", &[], 5).unwrap();
        assert!(!result.is_empty(), "must return results");
        assert_eq!(result[0].id, 1, "X (kw_score=2) must rank before Y (kw_score=1) despite Y having higher in_degree");
    }

    #[test]
    fn pivot_ranking_in_degree_tiebreak() {
        // P and Q both match "processor" → kw_score=1 each.
        // Q has in_degree=5, P has in_degree=0. Q must rank first.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::processor','processor','fn',1,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,1,'pkg::processor_v2','processor_v2','fn',11,20,NULL,NULL,NULL)", []).unwrap();
        // Add 5 callers for processor_v2 (id=2) → in_degree=5
        for i in 10..15_i64 {
            conn.execute(
                &format!("INSERT INTO symbols VALUES ({i},1,'pkg::caller{i}','caller{i}','fn',{},{}+5,NULL,NULL,NULL)", i*100, i*100),
                [],
            ).unwrap();
            conn.execute(&format!("INSERT INTO edges VALUES ({i},{i},2,'calls')"), []).unwrap();
        }

        // Intent: "processor data" — "processor" (len=9) and "data" (len=4) both qualify.
        // Only "processor" yields matches; "data" yields none.
        let result = find_pivot_symbols(&conn, "processor data", &[], 5).unwrap();
        assert_eq!(result.len(), 2, "both processor symbols must be returned");
        assert_eq!(result[0].id, 2, "processor_v2 (in_degree=5) must rank before processor (in_degree=0)");
    }

    #[test]
    fn pivot_ranking_fqn_matching() {
        // Symbol with name="handler" but fqn containing "authenticate" must be returned
        // when intent includes "authenticate". Current name-only query would miss this.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'auth::authenticate_handler','handler','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        let result = find_pivot_symbols(&conn, "authenticate users", &[], 5).unwrap();
        let ids: Vec<i64> = result.iter().map(|ps| ps.id).collect();
        assert!(ids.contains(&1), "symbol must be found via FQN LIKE match on 'authenticate'");
    }

    #[test]
    fn pivot_ranking_file_hints_take_priority() {
        // File hint must bypass keyword ranking entirely.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'specific_file.rs','h')", []).unwrap();
        conn.execute("INSERT INTO files VALUES (2,'other_file.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::hint_sym','hint_sym','fn',1,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,2,'pkg::other_sym','other_sym','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        let result = find_pivot_symbols(&conn, "other build context", &["specific_file.rs".to_string()], 5).unwrap();
        let ids: Vec<i64> = result.iter().map(|ps| ps.id).collect();
        assert!(ids.contains(&1), "symbol from hinted file must be present");
        assert!(!ids.contains(&2), "symbol from non-hinted file must be absent when file hints are given");
    }

    #[test]
    fn pivot_ranking_case_variant_keywords_do_not_inflate_kw_score() {
        // Intent "Build BUILD build" should deduplicate to unique_words={"build"}.
        // Symbol A (name="builder") matches "build" → kw_score=1.
        // Symbol C (name="debug_build", in_degree=2) also matches "build" → kw_score=1, in_degree=2.
        // With correct dedup: both calls return [C, A] (C wins by in_degree tiebreak).
        // With broken dedup: second call gives A kw_score=3 → A would appear first.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::builder','builder','fn',1,10,NULL,NULL,NULL)", []).unwrap();
        // Symbol C: "debug_build" contains "build" as a substring → also matches LIKE "%build%", in_degree=2.
        conn.execute("INSERT INTO symbols VALUES (3,1,'pkg::debug_build','debug_build','fn',11,20,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (10,1,'pkg::caller1','caller1','fn',100,110,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (11,1,'pkg::caller2','caller2','fn',111,120,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,10,3,'calls')", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (2,11,3,'calls')", []).unwrap();

        let result_single = find_pivot_symbols(&conn, "build", &[], 5).unwrap();
        let result_triple = find_pivot_symbols(&conn, "Build BUILD build", &[], 5).unwrap();

        let ids_single: Vec<i64> = result_single.iter().map(|ps| ps.id).collect();
        let ids_triple: Vec<i64> = result_triple.iter().map(|ps| ps.id).collect();
        // Both must return C (id=3) first because in_degree=2 > in_degree=0 at equal kw_score
        assert_eq!(ids_single[0], 3, "debug_build (in_degree=2) must rank first for 'build'");
        assert_eq!(ids_triple[0], 3, "debug_build must still rank first for 'Build BUILD build' (dedup must prevent inflation)");
        assert_eq!(ids_single, ids_triple, "both calls must return identical ordering");
    }

    #[test]
    fn pivot_ranking_deterministic_id_tiebreak() {
        // 3 symbols all matching the same single keyword, all in_degree=0.
        // Must be returned sorted by id ASC.
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        // Insert in reverse order to confirm ordering is by id, not insertion order
        conn.execute("INSERT INTO symbols VALUES (30,1,'pkg::context_c','context_c','fn',1,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (10,1,'pkg::context_a','context_a','fn',11,20,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (20,1,'pkg::context_b','context_b','fn',21,30,NULL,NULL,NULL)", []).unwrap();

        let result = find_pivot_symbols(&conn, "context data", &[], 10).unwrap();
        let ctx_ids: Vec<i64> = result.iter().map(|ps| ps.id).filter(|&id| id == 10 || id == 20 || id == 30).collect();
        assert_eq!(ctx_ids, vec![10, 20, 30], "symbols with equal score must be ordered by id ASC");
    }

    #[test]
    fn impact_includes_uses_type_edges() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/types.ts','h')", []).unwrap();
        conn.execute("INSERT INTO files VALUES (2,'src/handler.ts','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'src/types.ts::MyInterface','MyInterface','interface',1,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,2,'src/handler.ts::handleRequest','handleRequest','fn',1,5,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,2,1,'uses_type')", []).unwrap();

        let result = get_impact(&conn, "src/types.ts::MyInterface", 2, &ContentPolicy::default()).unwrap();
        assert!(result.contains("handleRequest"), "type user must appear in impact results");
        assert!(result.contains("uses_type"), "edge kind must be shown in output");
    }

    #[test]
    fn impact_excludes_unrelated_edge_kinds() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/types.ts','h')", []).unwrap();
        conn.execute("INSERT INTO files VALUES (2,'src/other.ts','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'src/types.ts::Target','Target','class',1,10,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (2,2,'src/other.ts::Ref','Ref','fn',1,5,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,2,1,'references')", []).unwrap();

        let result = get_impact(&conn, "src/types.ts::Target", 2, &ContentPolicy::default()).unwrap();
        assert!(!result.contains("Ref"), "references edge must NOT appear in impact results");
    }

    #[test]
    fn impact_traverses_uses_type_transitively() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/types.ts','h')", []).unwrap();
        conn.execute("INSERT INTO files VALUES (2,'src/handler.ts','h')", []).unwrap();
        conn.execute("INSERT INTO files VALUES (3,'src/caller.ts','h')", []).unwrap();
        // A: class/interface (type target)
        conn.execute("INSERT INTO symbols VALUES (1,1,'src/types.ts::A','A','interface',1,10,NULL,NULL,NULL)", []).unwrap();
        // B: function that uses_type A
        conn.execute("INSERT INTO symbols VALUES (2,2,'src/handler.ts::B','B','fn',1,5,NULL,NULL,NULL)", []).unwrap();
        // C: function that calls B
        conn.execute("INSERT INTO symbols VALUES (3,3,'src/caller.ts::C','C','fn',1,5,NULL,NULL,NULL)", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (1,2,1,'uses_type')", []).unwrap();
        conn.execute("INSERT INTO edges VALUES (2,3,2,'calls')", []).unwrap();

        let result = get_impact(&conn, "src/types.ts::A", 3, &ContentPolicy::default()).unwrap();
        assert!(result.contains("B"), "B (uses_type A) must appear at depth 1");
        assert!(result.contains("C"), "C (calls B) must appear at depth 2");
        assert!(result.contains("uses_type"), "uses_type edge kind must be shown");
        assert!(result.contains("calls"), "calls edge kind must be shown");
    }

    #[test]
    fn impact_max_per_hop_with_uses_type() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/types.ts','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'src/types.ts::Target','Target','interface',1,10,NULL,NULL,NULL)", []).unwrap();
        // Insert 101 symbols all with uses_type edges to Target
        for i in 2..=102 {
            conn.execute(
                &format!("INSERT INTO files VALUES ({i},'src/dep{i}.ts','h')"),
                [],
            ).unwrap();
            conn.execute(
                &format!("INSERT INTO symbols VALUES ({i},{i},'src/dep{i}.ts::Dep{i}','Dep{i}','fn',1,5,NULL,NULL,NULL)"),
                [],
            ).unwrap();
            conn.execute(
                &format!("INSERT INTO edges VALUES ({i},{i},1,'uses_type')"),
                [],
            ).unwrap();
        }

        let result = get_impact(&conn, "src/types.ts::Target", 1, &ContentPolicy::default()).unwrap();
        assert!(result.contains("Results truncated"), "truncation warning must appear when >100 dependents");
    }

    // ─── Task 11: get_context_with_pivots and rank_symbols_by_keywords ──────

    #[test]
    fn get_context_with_pivots_uses_explicit_fqns() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1, 'src/auth.rs', 'h')", []).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'src/auth.rs::login', 'login', 'function', 1, 10, NULL, NULL, NULL)",
            [],
        ).unwrap();
        let tmpdir = tempfile::tempdir().unwrap();
        let src = tmpdir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("auth.rs"), "fn login() { todo!() }\n").unwrap();

        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix login bug",
            &["src/auth.rs::login".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(result.contains("login"), "pivot symbol must appear in output");
        assert!(result.contains("Pivot Symbols"), "must have pivot section");
    }

    #[test]
    fn get_context_with_pivots_no_match() {
        let conn = build_test_db();
        let tmpdir = tempfile::tempdir().unwrap();
        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix bug",
            &["nonexistent::symbol".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(result.contains("No symbols found matching provided FQNs"),
            "must indicate no match; got: {result}");
    }

    #[test]
    fn get_context_with_pivots_includes_observations() {
        let tmpdb = tempfile::tempdir().unwrap();
        let db_path = tmpdb.path().join(".olaf").join("index.db");
        let conn = crate::db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO files (path, blake3_hash, last_indexed_at) VALUES ('src/a.rs', 'h', 0)",
            [],
        ).unwrap();
        let file_id: i64 = conn.query_row("SELECT id FROM files WHERE path='src/a.rs'", [], |r| r.get::<_, i64>(0)).unwrap();
        conn.execute(
            "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) \
             VALUES (?1, 'src/a.rs::handler', 'handler', 'function', 1, 5, 'h')",
            rusqlite::params![file_id],
        ).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, started_at) VALUES ('s1', 1704067200000)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO observations (session_id, content, kind, symbol_fqn, created_at) \
             VALUES ('s1', 'previous bug fix attempt failed', 'error', 'src/a.rs::handler', 1704067200000)",
            [],
        ).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let src = tmpdir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.rs"), "fn handler() {}\n").unwrap();

        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix error",
            &["src/a.rs::handler".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(result.contains("previous bug fix attempt failed"),
            "observation must appear in output; got: {result}");
    }

    #[test]
    fn rank_symbols_by_keywords_matches_find_pivot_logic() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1, 'src/a.rs', 'h')", []).unwrap();
        conn.execute("INSERT INTO files VALUES (2, 'src/b.rs', 'h')", []).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'src/a.rs::authenticate', 'authenticate', 'fn', 1, 10, NULL, NULL, NULL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (2, 2, 'src/b.rs::authorization', 'authorization', 'fn', 1, 10, NULL, NULL, NULL)",
            [],
        ).unwrap();
        // Give symbol 2 higher in-degree
        conn.execute("INSERT INTO edges VALUES (1, 1, 2, 'calls')", []).unwrap();

        let keywords = vec!["auth".to_string()];
        let result = rank_symbols_by_keywords(&conn, &keywords, 5).unwrap();
        assert_eq!(result.len(), 2, "both symbols match 'auth'");
        // Same kw_score (1 each), so in_degree breaks tie: symbol 2 has in_degree=1
        assert_eq!(result[0].id, 2, "higher in-degree symbol first");
        assert_eq!(result[1].id, 1);
    }

    #[test]
    fn rank_symbols_by_keywords_no_match_returns_empty() {
        let conn = build_test_db();
        let keywords = vec!["nonexistent_keyword_xyz".to_string()];
        let result = rank_symbols_by_keywords(&conn, &keywords, 5).unwrap();
        assert!(result.is_empty(), "no match must return empty vec (no first-in-DB fallback)");
    }

    #[test]
    fn rank_symbols_by_keywords_limit_zero_returns_empty() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/a.rs','h')", []).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (1,1,'src/a.rs::authentication','authentication','fn',1,10,NULL,NULL,NULL)",
            [],
        ).unwrap();
        let keywords = vec!["authentication".to_string()];
        let result = rank_symbols_by_keywords(&conn, &keywords, 0).unwrap();
        assert!(result.is_empty(), "limit=0 must return empty vec without panicking or invalid SQL");
    }

    #[test]
    fn rank_symbols_by_keywords_three_tier_sort_and_metadata() {
        // Tests all three sort dimensions and verifies kw_score/in_degree survive into PivotScore.
        //
        // Keywords: ["alpha", "beta"] (both len > 3)
        //   sym 10: name="alphabeta" — contains both "alpha" and "beta" → kw_score=2
        //   sym 20: name="alpha_high" — contains "alpha" only → kw_score=1, in_degree=5
        //   sym 30: name="alpha_low_a" — contains "alpha" only → kw_score=1, in_degree=2, id=30
        //   sym 40: name="alpha_low_b" — contains "alpha" only → kw_score=1, in_degree=2, id=40
        //
        // Expected order: sym10 (kw dominates), sym20 (in_deg tiebreak), sym30 (id tiebreak), sym40
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/a.rs','h')", []).unwrap();

        conn.execute(
            "INSERT INTO symbols VALUES (10,1,'src::alphabeta','alphabeta','fn',1,10,NULL,NULL,NULL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (20,1,'src::alpha_high','alpha_high','fn',1,10,NULL,NULL,NULL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (30,1,'src::alpha_low_a','alpha_low_a','fn',1,10,NULL,NULL,NULL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (40,1,'src::alpha_low_b','alpha_low_b','fn',1,10,NULL,NULL,NULL)",
            [],
        ).unwrap();

        // sym 20: 5 incoming edges (in_degree=5)
        for src in [101i64, 102, 103, 104, 105] {
            conn.execute(
                "INSERT INTO edges (source_id, target_id, kind) VALUES (?1, 20, 'calls')",
                params![src],
            ).unwrap();
        }
        // sym 30 and sym 40: 2 incoming edges each (in_degree=2), same to test id tiebreak
        for src in [201i64, 202] {
            conn.execute(
                "INSERT INTO edges (source_id, target_id, kind) VALUES (?1, 30, 'calls')",
                params![src],
            ).unwrap();
            conn.execute(
                "INSERT INTO edges (source_id, target_id, kind) VALUES (?1, 40, 'calls')",
                params![src],
            ).unwrap();
        }

        let keywords = vec!["alpha".to_string(), "beta".to_string()];
        let result = rank_symbols_by_keywords(&conn, &keywords, 10).unwrap();
        assert_eq!(result.len(), 4);

        // Primary sort: kw_score DESC — sym 10 (kw=2) beats all kw=1 symbols
        assert_eq!(result[0].id, 10, "kw_score=2 must beat kw_score=1 regardless of in_degree");
        assert!(matches!(result[0].reason, SelectionReason::Keyword { kw_score: 2, .. }),
            "kw_score=2 must survive into PivotScore.reason; got {:?}", result[0].reason);
        assert!(matches!(result[0].reason, SelectionReason::Keyword { in_degree: 0, .. }),
            "in_degree=0 must survive into PivotScore.reason; got {:?}", result[0].reason);

        // First tiebreak: in_degree DESC — sym 20 (in_deg=5) beats sym 30 and sym 40 (in_deg=2)
        assert_eq!(result[1].id, 20, "in_degree=5 must beat in_degree=2 at equal kw_score");
        assert!(matches!(result[1].reason, SelectionReason::Keyword { kw_score: 1, in_degree: 5 }),
            "kw_score=1 and in_degree=5 must survive for sym 20; got {:?}", result[1].reason);

        // Second tiebreak: id ASC — sym 30 (id=30) beats sym 40 (id=40) at equal kw+in_deg
        assert_eq!(result[2].id, 30, "lower id=30 must beat id=40 at equal kw_score and in_degree");
        assert_eq!(result[3].id, 40);
        assert!(matches!(result[2].reason, SelectionReason::Keyword { kw_score: 1, in_degree: 2 }),
            "metadata for id=30 must be correct; got {:?}", result[2].reason);
        assert!(matches!(result[3].reason, SelectionReason::Keyword { kw_score: 1, in_degree: 2 }),
            "metadata for id=40 must be correct; got {:?}", result[3].reason);
    }

    // ─── Story 9.8: Score explainability tests ──────

    #[test]
    fn selection_reason_keyword_path() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::handler','handler','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        let result = find_pivot_symbols(&conn, "handler request", &[], 5).unwrap();
        assert!(!result.is_empty());
        assert!(matches!(result[0].reason, SelectionReason::Keyword { .. }), "keyword path must produce Keyword reason");
        if let SelectionReason::Keyword { kw_score, .. } = &result[0].reason {
            assert!(*kw_score >= 1, "kw_score must be at least 1");
        }
    }

    #[test]
    fn selection_reason_file_hint_path() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'src/auth.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'auth::login','login','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        let result = find_pivot_symbols(&conn, "anything", &["auth.rs".to_string()], 5).unwrap();
        assert!(!result.is_empty());
        assert!(matches!(result[0].reason, SelectionReason::FileHint { .. }), "file-hint path must produce FileHint reason");
        if let SelectionReason::FileHint { hint } = &result[0].reason {
            assert_eq!(hint, "auth.rs");
        }
    }

    #[test]
    fn selection_reason_fallback_path() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::something','something','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        // Use a keyword that won't match any symbol
        let result = find_pivot_symbols(&conn, "zzzz_nomatch_zzzz", &[], 5).unwrap();
        assert!(!result.is_empty(), "fallback must return at least one symbol");
        assert!(matches!(result[0].reason, SelectionReason::Fallback), "fallback path must produce Fallback reason");
    }

    #[test]
    fn selection_reason_caller_supplied_path() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::target','target','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let (_, notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix bug", &["pkg::target".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(notes.contains("caller-supplied"), "CallerSupplied pivots must show 'caller-supplied' in retrieval notes");
    }

    #[test]
    fn retrieval_notes_contain_rendered_pivots_only() {
        let conn = build_test_db();
        conn.execute("INSERT INTO files VALUES (1,'f.rs','h')", []).unwrap();
        conn.execute("INSERT INTO symbols VALUES (1,1,'pkg::handler','handler','fn',1,10,NULL,NULL,NULL)", []).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let (result, notes) = get_context(&conn, tmpdir.path(), "handler request", &[], 4000, None, &ContentPolicy::default()).unwrap();
        assert!(result.contains("Pivot Symbols"), "brief must have pivots");
        assert!(notes.contains("## Retrieval Notes"), "retrieval notes must be generated");
        assert!(notes.contains("handler"), "rendered pivot must appear in retrieval notes");
    }

    #[test]
    fn observation_recency_labels_appended() {
        use crate::memory::store::{ObservationRow, ScoredObservation};

        // Recent observation (primary_signal=recency, score >= 0.7)
        let recent = ScoredObservation {
            obs: ObservationRow {
                id: 1, session_id: "s1".to_string(), created_at: 0,
                kind: "discovery".to_string(), content: "found pattern".to_string(),
                symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None, confidence: Some(0.8), branch: None, importance: crate::memory::store::Importance::Medium,
            },
            relevance_score: 0.85,
            primary_signal: "recency".to_string(),
            score_breakdown: Default::default(),
        };
        let entry = format_scored_observation_entry(&recent);
        assert!(entry.contains("(recent"), "score >= 0.7 must get a (recent…) label");
        assert!(entry.contains("found pattern"), "content must be preserved");

        // Aged observation (primary_signal=recency, score < 0.7)
        let aged = ScoredObservation {
            obs: ObservationRow {
                id: 2, session_id: "s1".to_string(), created_at: 0,
                kind: "note".to_string(), content: "old note".to_string(),
                symbol_fqn: None, file_path: None, is_stale: false, stale_reason: None, confidence: None, branch: None, importance: crate::memory::store::Importance::Medium,
            },
            relevance_score: 0.3,
            primary_signal: "recency".to_string(),
            score_breakdown: Default::default(),
        };
        let entry = format_scored_observation_entry(&aged);
        assert!(entry.contains("(aged)"), "low-score non-stale recency must get (aged) label");

        // Stale observation (primary_signal=stale)
        let stale = ScoredObservation {
            obs: ObservationRow {
                id: 3, session_id: "s1".to_string(), created_at: 0,
                kind: "bug".to_string(), content: "stale finding".to_string(),
                symbol_fqn: None, file_path: None, is_stale: true,
                stale_reason: Some("symbol changed".to_string()), confidence: None, branch: None, importance: crate::memory::store::Importance::Medium,
            },
            relevance_score: 0.0,
            primary_signal: "stale".to_string(),
            score_breakdown: Default::default(),
        };
        let entry = format_scored_observation_entry(&stale);
        assert!(entry.contains("(stale)"), "is_stale=true must get (stale) label regardless of score");
        assert!(entry.contains("STALE"), "stale marker must be preserved");
        assert!(entry.contains("symbol changed"), "stale_reason must be preserved");
    }
}

#[cfg(test)]
mod eval {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Instant;
    use tempfile::tempdir;

    #[derive(Debug, serde::Deserialize)]
    struct TestCaseFile {
        case: Vec<TestCase>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct TestCase {
        name: String,
        intent: String,
        #[serde(default)]
        file_hints: Vec<String>,
        expected_pivots: Vec<String>,
        tag: String,
        #[serde(default)]
        exercises: Vec<String>,
        #[serde(default)]
        min_recall_at_3: Option<f32>,
        #[serde(default)]
        expected_policy: Option<ExpectedPolicy>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct ExpectedPolicy {
        depth: usize,
        include_inbound: bool,
        inbound_first: bool,
    }

    #[derive(Debug, serde::Deserialize)]
    struct BaselineFile {
        min_recall_at_3: f32,
        #[serde(default)]
        tag: HashMap<String, TagBaseline>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct TagBaseline {
        min_recall_at_3: f32,
    }

    #[derive(Debug, Clone, serde::Serialize)]
    struct ReasonDistribution {
        keyword: usize,
        file_hint: usize,
        fallback: usize,
        caller_supplied: usize,
    }

    #[derive(Debug, serde::Serialize)]
    struct CaseResult {
        name: String,
        tag: String,
        intent: String,
        recall_at_3: f32,
        recall_at_5: f32,
        mrr: f32,
        expected_pivots: Vec<String>,
        actual_pivots: Vec<PivotResult>,
        reason_distribution: ReasonDistribution,
        policy_pass: Option<bool>,
        policy_details: Option<String>,
        brief_tokens: Option<usize>,
        latency_ms: Option<u128>,
        exercises: Vec<String>,
    }

    #[derive(Debug, serde::Serialize)]
    struct PivotResult {
        fqn: String,
        reason: String,
    }

    #[derive(Debug, serde::Serialize)]
    struct AggregateResult {
        global_recall_at_3: f32,
        per_tag: HashMap<String, TagResult>,
        reason_distribution: ReasonDistribution,
        cases: Vec<CaseResult>,
        coverage_matrix: HashMap<String, Vec<String>>,
    }

    #[derive(Debug, serde::Serialize)]
    struct TagResult {
        recall_at_3: f32,
        recall_at_5: f32,
        case_count: usize,
    }

    /// All retrieval-quality constants that must be exercised by at least one case.
    const REQUIRED_CONSTANTS: &[&str] = &[
        "CANDIDATE_GATHER_LIMIT",
        "DEFAULT_PIVOT_POOL",
        "LOW_CONFIDENCE_THRESHOLD",
        "LOW_CONFIDENCE_PIVOT_POOL",
        "INBOUND_BLEND_THRESHOLD",
        "REFACTOR_DEPTH_THRESHOLD",
    ];

    fn compute_reason_distribution(pivots: &[PivotScore]) -> ReasonDistribution {
        let mut dist = ReasonDistribution { keyword: 0, file_hint: 0, fallback: 0, caller_supplied: 0 };
        for p in pivots {
            match &p.reason {
                SelectionReason::Keyword { .. } => dist.keyword += 1,
                SelectionReason::FileHint { .. } => dist.file_hint += 1,
                SelectionReason::Fallback => dist.fallback += 1,
                SelectionReason::CallerSupplied => dist.caller_supplied += 1,
            }
        }
        dist
    }

    fn fixture_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/bench_corpus")
    }

    fn format_reason(reason: &SelectionReason) -> String {
        match reason {
            SelectionReason::Keyword { kw_score, in_degree } => {
                format!("Keyword(kw={}, deg={})", kw_score, in_degree)
            }
            SelectionReason::FileHint { hint } => format!("FileHint({})", hint),
            SelectionReason::CallerSupplied => "CallerSupplied".to_string(),
            SelectionReason::Fallback => "Fallback".to_string(),
        }
    }

    fn compute_recall_at_k(expected: &[String], actual_fqns: &[String], k: usize) -> f32 {
        if expected.is_empty() {
            // No expected pivots — recall is N/A. Return NaN so callers can distinguish
            // from real scores; aggregate code filters NaN out.
            return f32::NAN;
        }
        let top_k: std::collections::HashSet<&str> =
            actual_fqns.iter().take(k).map(|s| s.as_str()).collect();
        let hits = expected.iter().filter(|e| top_k.contains(e.as_str())).count();
        hits as f32 / expected.len() as f32
    }

    fn compute_mrr(expected: &[String], actual_fqns: &[String]) -> f32 {
        if expected.is_empty() {
            return f32::NAN;
        }
        for (rank, fqn) in actual_fqns.iter().enumerate() {
            if expected.contains(fqn) {
                return 1.0 / (rank + 1) as f32;
            }
        }
        0.0
    }

    #[test]
    fn eval_retrieval_harness() {
        // --- Setup: index fixture sources into tempdir ---
        let tmp = tempdir().expect("failed to create tempdir");
        let db_path = tmp.path().join("index.db");
        let mut conn = crate::db::open(&db_path).expect("db::open failed");

        let corpus_path = fixture_path();
        let stats = crate::index::full::run(&mut conn, &corpus_path)
            .expect("index::full::run failed on bench_corpus");
        assert!(stats.symbols > 0, "bench_corpus must produce symbols; got 0");

        // --- Load test cases and baselines ---
        let cases_toml = std::fs::read_to_string(corpus_path.join("cases.toml"))
            .expect("failed to read cases.toml");
        let case_file: TestCaseFile =
            toml::from_str(&cases_toml).expect("failed to parse cases.toml");

        let baseline_toml = std::fs::read_to_string(corpus_path.join("baseline.toml"))
            .expect("failed to read baseline.toml");
        let baselines: BaselineFile =
            toml::from_str(&baseline_toml).expect("failed to parse baseline.toml");

        // --- Per-case evaluation ---
        let mut case_results: Vec<CaseResult> = Vec::new();
        let mut tag_recalls_3: HashMap<String, Vec<f32>> = HashMap::new();
        let mut tag_recalls_5: HashMap<String, Vec<f32>> = HashMap::new();
        let mut coverage_matrix: HashMap<String, Vec<String>> = HashMap::new();

        for tc in &case_file.case {
            // Build coverage matrix
            for constant in &tc.exercises {
                coverage_matrix
                    .entry(constant.clone())
                    .or_default()
                    .push(tc.name.clone());
            }

            // Derive traversal policy
            let profile = detect_intent_profile(&tc.intent);
            let policy = derive_traversal_policy(&profile);

            // Assert traversal policy if expected
            let (policy_pass, policy_details) = if let Some(ep) = &tc.expected_policy {
                let pass = policy.depth == ep.depth
                    && policy.include_inbound == ep.include_inbound
                    && policy.inbound_first == ep.inbound_first;
                let details = if pass {
                    format!(
                        "PASS: depth={}, include_inbound={}, inbound_first={}",
                        policy.depth, policy.include_inbound, policy.inbound_first
                    )
                } else {
                    format!(
                        "FAIL: expected depth={}/include_inbound={}/inbound_first={}, got depth={}/include_inbound={}/inbound_first={}",
                        ep.depth, ep.include_inbound, ep.inbound_first,
                        policy.depth, policy.include_inbound, policy.inbound_first
                    )
                };
                (Some(pass), Some(details))
            } else {
                (None, None)
            };

            // Find pivot symbols
            let pivots = find_pivot_symbols(
                &conn,
                &tc.intent,
                &tc.file_hints,
                policy.pivot_pool_size,
            )
            .expect("find_pivot_symbols failed");

            let actual_fqns: Vec<String> = pivots.iter().map(|p| p.fqn.clone()).collect();
            let reason_dist = compute_reason_distribution(&pivots);

            let recall_3 = compute_recall_at_k(&tc.expected_pivots, &actual_fqns, 3);
            let recall_5 = compute_recall_at_k(&tc.expected_pivots, &actual_fqns, 5);
            let mrr = compute_mrr(&tc.expected_pivots, &actual_fqns);

            // Only accumulate non-NaN recalls (fallback cases with empty expected_pivots are NaN)
            if recall_3.is_finite() {
                tag_recalls_3.entry(tc.tag.clone()).or_default().push(recall_3);
            }
            if recall_5.is_finite() {
                tag_recalls_5.entry(tc.tag.clone()).or_default().push(recall_5);
            }

            // Informational: get_context end-to-end measurement (panic-safe)
            let (brief_tokens, latency_ms) = {
                let corpus = corpus_path.clone();
                let intent = tc.intent.clone();
                let hints = tc.file_hints.clone();
                let start = Instant::now();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    get_context(&conn, &corpus, &intent, &hints, 4000, None, &ContentPolicy::default())
                }));
                match result {
                    Ok(Ok((output, notes))) => {
                        let elapsed = start.elapsed().as_millis();
                        let tokens = estimate_tokens(&output) + estimate_tokens(&notes);
                        (Some(tokens), Some(elapsed))
                    }
                    Ok(Err(e)) => {
                        eprintln!("  [info] get_context error for case '{}': {}", tc.name, e);
                        (None, None)
                    }
                    Err(_) => {
                        eprintln!("  [info] get_context panicked for case '{}' (non-fatal)", tc.name);
                        (None, None)
                    }
                }
            };

            case_results.push(CaseResult {
                name: tc.name.clone(),
                tag: tc.tag.clone(),
                intent: tc.intent.clone(),
                recall_at_3: if recall_3.is_finite() { recall_3 } else { -1.0 },
                recall_at_5: if recall_5.is_finite() { recall_5 } else { -1.0 },
                mrr: if mrr.is_finite() { mrr } else { -1.0 },
                expected_pivots: tc.expected_pivots.clone(),
                actual_pivots: pivots
                    .iter()
                    .map(|p| PivotResult {
                        fqn: p.fqn.clone(),
                        reason: format_reason(&p.reason),
                    })
                    .collect(),
                reason_distribution: reason_dist,
                policy_pass,
                policy_details,
                brief_tokens,
                latency_ms,
                exercises: tc.exercises.clone(),
            });
        }

        // --- Aggregate metrics ---
        let all_recall_3: Vec<f32> = case_results.iter().map(|r| r.recall_at_3).collect();
        let global_recall_3 = if all_recall_3.is_empty() {
            0.0
        } else {
            all_recall_3.iter().sum::<f32>() / all_recall_3.len() as f32
        };

        let mut per_tag: HashMap<String, TagResult> = HashMap::new();
        for (tag, recalls) in &tag_recalls_3 {
            let avg_3 = recalls.iter().sum::<f32>() / recalls.len() as f32;
            let recalls_5 = tag_recalls_5.get(tag).unwrap();
            let avg_5 = recalls_5.iter().sum::<f32>() / recalls_5.len() as f32;
            per_tag.insert(
                tag.clone(),
                TagResult {
                    recall_at_3: avg_3,
                    recall_at_5: avg_5,
                    case_count: recalls.len(),
                },
            );
        }

        // Aggregate reason distribution
        let mut agg_reason = ReasonDistribution { keyword: 0, file_hint: 0, fallback: 0, caller_supplied: 0 };
        for cr in &case_results {
            agg_reason.keyword += cr.reason_distribution.keyword;
            agg_reason.file_hint += cr.reason_distribution.file_hint;
            agg_reason.fallback += cr.reason_distribution.fallback;
            agg_reason.caller_supplied += cr.reason_distribution.caller_supplied;
        }

        let result = AggregateResult {
            global_recall_at_3: global_recall_3,
            per_tag,
            reason_distribution: agg_reason,
            cases: case_results,
            coverage_matrix,
        };

        // --- Write JSON results ---
        let json_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/bench_retrieval_results.json");
        if let Some(parent) = json_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let json = serde_json::to_string_pretty(&result).expect("failed to serialize results");
        std::fs::write(&json_path, &json).expect("failed to write bench results JSON");

        // --- Print summary to stderr ---
        eprintln!("\n=== Retrieval Evaluation Harness ===");
        eprintln!("Global recall@3: {:.3}", global_recall_3);
        for (tag, tr) in &result.per_tag {
            eprintln!(
                "  [{}] recall@3={:.3} recall@5={:.3} (n={})",
                tag, tr.recall_at_3, tr.recall_at_5, tr.case_count
            );
        }
        eprintln!("Results: {}", json_path.display());

        // --- Gating assertions ---
        let mut failures: Vec<String> = Vec::new();

        // Traversal policy assertions (AC4)
        for cr in &result.cases {
            if let Some(false) = cr.policy_pass {
                failures.push(format!(
                    "Policy assertion failed for '{}': {}",
                    cr.name,
                    cr.policy_details.as_deref().unwrap_or("unknown")
                ));
            }
        }

        // Fallback behavior assertions: cases with empty expected_pivots must
        // return pivots via Fallback reason (not vacuously pass recall)
        for (tc, cr) in case_file.case.iter().zip(result.cases.iter()) {
            if tc.expected_pivots.is_empty() && tc.tag == "fallback" {
                if cr.reason_distribution.fallback == 0 {
                    failures.push(format!(
                        "Fallback case '{}' returned 0 Fallback-reason pivots (got {} Keyword, {} FileHint)",
                        cr.name, cr.reason_distribution.keyword, cr.reason_distribution.file_hint
                    ));
                }
                if cr.actual_pivots.is_empty() {
                    failures.push(format!(
                        "Fallback case '{}' returned no pivots at all",
                        cr.name
                    ));
                }
            }
        }

        // Coverage matrix enforcement (AC5): every required constant must be exercised
        for &constant in REQUIRED_CONSTANTS {
            if !result.coverage_matrix.contains_key(constant) {
                failures.push(format!(
                    "Coverage gap: constant '{}' is not exercised by any test case",
                    constant
                ));
            }
        }

        // Global recall@3 (AC3a)
        if global_recall_3 < baselines.min_recall_at_3 {
            failures.push(format!(
                "Global recall@3 {:.3} < baseline {:.3}",
                global_recall_3, baselines.min_recall_at_3
            ));
        }

        // Per-tag recall@3 (AC3b) — skip tags with no finite recalls (e.g. fallback)
        for (tag, tr) in &result.per_tag {
            if let Some(tb) = baselines.tag.get(tag)
                && tr.recall_at_3 < tb.min_recall_at_3
            {
                failures.push(format!(
                    "Tag '{}' recall@3 {:.3} < baseline {:.3}",
                    tag, tr.recall_at_3, tb.min_recall_at_3
                ));
            }
        }

        // Per-case min_recall_at_3 (AC3c)
        for (tc, cr) in case_file.case.iter().zip(result.cases.iter()) {
            if let Some(min) = tc.min_recall_at_3
                && cr.recall_at_3 >= 0.0 && cr.recall_at_3 < min
            {
                failures.push(format!(
                    "Case '{}' recall@3 {:.3} < per-case min {:.3}",
                    cr.name, cr.recall_at_3, min
                ));
            }
        }

        if !failures.is_empty() {
            eprintln!("\n=== REGRESSION FAILURES ===");
            for f in &failures {
                eprintln!("  FAIL: {}", f);
            }
            eprintln!("Full details: {}", json_path.display());
            panic!(
                "Retrieval evaluation failed with {} issue(s). See target/bench_retrieval_results.json",
                failures.len()
            );
        }

        eprintln!("=== ALL GATES PASSED ===\n");
    }

    // ─── Story 11.2 — project-scoped observations in context assembly ────────

    fn setup_project_obs_db() -> (Connection, tempfile::TempDir) {
        let tmpdb = tempfile::tempdir().unwrap();
        let db_path = tmpdb.path().join(".olaf").join("index.db");
        let conn = crate::db::open(&db_path).unwrap();
        // Insert a symbol so pivots work
        conn.execute("INSERT INTO files (path, blake3_hash, last_indexed_at) VALUES ('src/a.rs', 'h', 0)", []).unwrap();
        let file_id: i64 = conn.query_row("SELECT id FROM files WHERE path='src/a.rs'", [], |r| r.get::<_, i64>(0)).unwrap();
        conn.execute(
            "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) \
             VALUES (?1, 'src/a.rs::handler', 'handler', 'function', 1, 5, 'h')",
            rusqlite::params![file_id],
        ).unwrap();
        conn.execute("INSERT INTO sessions (id, started_at) VALUES ('s1', 1704067200000)", []).unwrap();
        (conn, tmpdb)
    }

    #[test]
    fn project_obs_relevant_included_with_label() {
        let (conn, _tmpdb) = setup_project_obs_db();
        // Insert a project-scoped observation with content matching intent
        conn.execute(
            "INSERT INTO observations (session_id, content, kind, symbol_fqn, file_path, created_at, importance) \
             VALUES ('s1', 'authentication uses JWT tokens for session management', 'insight', NULL, NULL, 1704067200000, 'medium')",
            [],
        ).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let src = tmpdir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.rs"), "fn handler() {}\n").unwrap();

        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix authentication JWT tokens issue",
            &["src/a.rs::handler".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(result.contains("[project]"),
            "project-scoped observation must be labeled [project]; got: {result}");
        assert!(result.contains("authentication uses JWT tokens"),
            "relevant project obs must be included; got: {result}");
    }

    #[test]
    fn project_obs_irrelevant_excluded() {
        let (conn, _tmpdb) = setup_project_obs_db();
        // Insert a project-scoped observation with content NOT matching intent
        conn.execute(
            "INSERT INTO observations (session_id, content, kind, symbol_fqn, file_path, created_at, importance) \
             VALUES ('s1', 'database uses PostgreSQL for storage', 'insight', NULL, NULL, 1704067200000, 'medium')",
            [],
        ).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let src = tmpdir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.rs"), "fn handler() {}\n").unwrap();

        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix authentication JWT tokens issue",
            &["src/a.rs::handler".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(!result.contains("database uses PostgreSQL"),
            "irrelevant project obs must be excluded; got: {result}");
    }

    #[test]
    fn project_obs_respects_token_sub_budget() {
        let (conn, _tmpdb) = setup_project_obs_db();
        // Insert project obs with matching content
        conn.execute(
            "INSERT INTO observations (session_id, content, kind, symbol_fqn, file_path, created_at, importance) \
             VALUES ('s1', 'authentication handler uses middleware pattern for request validation', 'insight', NULL, NULL, 1704067200000, 'medium')",
            [],
        ).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let src = tmpdir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.rs"), "fn handler() {}\n").unwrap();

        // Very small budget — project sub-budget will be tiny (20% of ~5% of 50 = ~0-1 tokens)
        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix authentication handler middleware",
            &["src/a.rs::handler".to_string()], 50, None, &ContentPolicy::default(),
        ).unwrap();
        // With a 50-token budget, memory_budget ~ 5 tokens, project sub ~ 1 token
        // The project obs entry is longer than 1 token, so it should be excluded
        assert!(!result.contains("[project]"),
            "project obs should be excluded when budget is too small; got: {result}");
    }

    #[test]
    fn project_obs_punctuation_heavy_intents_match() {
        let (conn, _tmpdb) = setup_project_obs_db();
        conn.execute(
            "INSERT INTO observations (session_id, content, kind, symbol_fqn, file_path, created_at, importance) \
             VALUES ('s1', 'CI/CD pipeline uses GitHub Actions for node.js deployment', 'insight', NULL, NULL, 1704067200000, 'medium')",
            [],
        ).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let src = tmpdir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.rs"), "fn handler() {}\n").unwrap();

        // Intent with punctuation-heavy terms
        let (result, _notes) = get_context_with_pivots(
            &conn, tmpdir.path(), "fix CI/CD pipeline node.js deployment",
            &["src/a.rs::handler".to_string()], 4000, None, &ContentPolicy::default(),
        ).unwrap();
        assert!(result.contains("[project]") && result.contains("CI/CD pipeline"),
            "punctuation-heavy intents should match; got: {result}");
    }
}
