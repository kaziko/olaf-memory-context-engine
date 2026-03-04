use std::collections::{HashSet, VecDeque};
use std::path::Path;
use rusqlite::{Connection, OptionalExtension, params};

use crate::graph::skeleton::skeletonize;

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
#[allow(dead_code)]
pub(crate) struct IntentProfile {
    pub bugfix_score: usize,
    pub refactor_score: usize,
    pub impl_score: usize,
    pub total: usize,
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

#[allow(dead_code)]
pub(crate) fn detect_intent(intent: &str) -> IntentMode {
    detect_intent_profile(intent).dominant_mode
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

/// Layer 2 sensitive-file exclusion (defense-in-depth).
/// KEEP IN SYNC with `index::is_sensitive` in src/index/mod.rs.
/// Cannot import that function directly — would create a circular dependency.
fn is_output_sensitive(file_path: &str) -> bool {
    let path = std::path::Path::new(file_path);
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    if matches!(file_name, ".env" | "id_rsa") { return true; }
    if file_name.starts_with(".env.") || file_name.starts_with("id_rsa.") { return true; }
    if let Some(ext) = path.extension().and_then(|e| e.to_str())
        && matches!(ext, "pem" | "key" | "p12") { return true; }
    false
}

fn estimate_tokens(s: &str) -> usize {
    s.len().div_ceil(4)
}

fn find_pivot_symbols(conn: &Connection, intent: &str, file_hints: &[String], pool_size: usize) -> Result<Vec<i64>, QueryError> {
    let mut ids: Vec<i64> = Vec::new();
    let mut seen: HashSet<i64> = HashSet::new();
    if !file_hints.is_empty() {
        for hint in file_hints {
            let pattern = format!("%{hint}%");
            let mut stmt = conn.prepare(
                "SELECT s.id FROM symbols s JOIN files f ON f.id=s.file_id
                 WHERE f.path LIKE ?1 ORDER BY (s.end_line-s.start_line) DESC LIMIT 10"
            )?;
            let rows: Vec<i64> = stmt.query_map(params![pattern], |r| r.get(0))?
                .collect::<Result<_,_>>()?;
            for id in rows {
                if seen.insert(id) { ids.push(id); }
            }
        }
        if !ids.is_empty() { return Ok(ids); }
    }

    // Keyword match: use words > 3 chars from intent
    let words: Vec<&str> = intent.split_whitespace().filter(|w| w.len() > 3).collect();
    for word in &words {
        let pattern = format!("%{}%", word.to_lowercase());
        let mut stmt = conn.prepare(
            "SELECT id FROM symbols WHERE lower(name) LIKE ?1 LIMIT ?2"
        )?;
        let rows: Vec<i64> = stmt.query_map(params![pattern, pool_size as i64], |r| r.get(0))?
            .collect::<Result<_,_>>()?;
        for id in rows {
            if seen.insert(id) { ids.push(id); }
        }
    }

    // Fallback: any symbols
    if ids.is_empty() {
        let mut stmt = conn.prepare("SELECT id FROM symbols LIMIT ?1")?;
        ids = stmt.query_map(params![pool_size as i64], |r| r.get(0))?.collect::<Result<_,_>>()?;
    }

    Ok(ids)
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

    while let Some((current_id, current_depth)) = queue.pop_front() {
        if current_depth >= policy.depth { continue; }

        if policy.inbound_first {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT source_id FROM edges WHERE target_id=?1 ORDER BY source_id LIMIT 20"
            )?;
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
        }

        // Outbound edges
        let mut stmt = conn.prepare(
            "SELECT DISTINCT target_id FROM edges WHERE source_id=?1 ORDER BY target_id LIMIT 20"
        )?;
        let outbound: Vec<i64> = stmt.query_map(params![current_id], |r| r.get(0))?
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
            let mut stmt = conn.prepare(
                "SELECT DISTINCT source_id FROM edges WHERE target_id=?1 ORDER BY source_id LIMIT 20"
            )?;
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

fn format_observation_entry(obs: &crate::memory::store::ObservationRow) -> String {
    let mut entry = String::new();
    if obs.is_stale {
        let reason = obs.stale_reason.as_deref().unwrap_or("unknown reason");
        entry.push_str(&format!("- \u{26a0} [STALE \u{2014} {}] [{}] {}\n", reason, obs.kind, obs.content));
    } else {
        entry.push_str(&format!("- [{}] {}\n", obs.kind, obs.content));
    }
    if let Some(fqn) = &obs.symbol_fqn {
        entry.push_str(&format!("  Symbol: {}\n", fqn));
    }
    if let Some(fp) = &obs.file_path {
        entry.push_str(&format!("  File: {}\n", fp));
    }
    entry
}

fn format_pivot_entry(row: &SymbolRow, source: &str) -> String {
    format!(
        "## {} (`{}`)\nFile: `{}` lines {}-{}\n\n```\n{}\n```\n\n",
        row.name, row.fqn, row.file_path, row.start_line, row.end_line, source
    )
}

pub(crate) fn get_context(
    conn: &Connection,
    project_root: &Path,
    intent: &str,
    file_hints: &[String],
    token_budget: usize,
) -> Result<String, QueryError> {
    let profile = detect_intent_profile(intent);
    let policy = derive_traversal_policy(&profile);

    // Build the intent metadata header — always emitted so callers see detected intent even on empty index.
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
    // Conditional: emit intent_mix only when confident AND 2nd-highest weight is material
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
    // Emit fallback reason when a mode was detected but confidence was too low to use it
    if matches!(profile.execution_mode, IntentMode::Balanced)
        && !matches!(profile.dominant_mode, IntentMode::Balanced)
    {
        output.push_str("intent_fallback_reason: low confidence\n");
    }

    let pivot_ids = find_pivot_symbols(conn, intent, file_hints, policy.pivot_pool_size)?;

    if pivot_ids.is_empty() {
        output.push_str("\nNo symbols found. Run `olaf index` first.\n");
        return Ok(output);
    }

    let (pivots, supporting_with_reasons) = traverse_bfs(conn, &pivot_ids, &policy)?;

    let pivot_budget = token_budget * 70 / 100;
    let skeleton_budget = token_budget * 20 / 100;
    let memory_budget = token_budget * 10 / 100;

    output.push_str("\n## Pivot Symbols\n\n");

    let mut pivot_tokens = 0usize;
    let mut all_fqns: HashSet<String> = HashSet::new();
    let mut all_file_paths: HashSet<String> = HashSet::new();

    for id in &pivots {
        let Some(row) = load_symbol_row(conn, *id)? else { continue };
        if is_output_sensitive(&row.file_path) { continue; }

        all_fqns.insert(row.fqn.clone());
        all_file_paths.insert(row.file_path.clone());

        let source = match read_symbol_source(project_root, &row.file_path, row.start_line, row.end_line) {
            Ok(s) if !s.is_empty() => s,
            _ => {
                // File not readable or corrupt line range — fall back to signature only
                row.signature.as_deref().unwrap_or("(source unavailable)").to_string()
            }
        };

        let entry = format_pivot_entry(&row, &source);
        let entry_tokens = estimate_tokens(&entry);
        if pivot_tokens + entry_tokens > pivot_budget { break; }
        output.push_str(&entry);
        pivot_tokens += entry_tokens;
    }

    if !supporting_with_reasons.is_empty() {
        output.push_str("## Supporting Symbols\n\n");
        let mut skeleton_tokens = 0usize;
        for (id, reason) in &supporting_with_reasons {
            let Some(row) = load_symbol_row(conn, *id)? else { continue };
            if is_output_sensitive(&row.file_path) { continue; }

            all_fqns.insert(row.fqn.clone());
            all_file_paths.insert(row.file_path.clone());

            let skeleton = skeletonize(conn, row.id)?;
            let reason_line = format!("Why: {reason}\n");
            let skeleton_tokens_needed = estimate_tokens(&skeleton);
            let reason_tokens_needed = estimate_tokens(&reason_line);

            if skeleton_tokens + skeleton_tokens_needed > skeleton_budget { break; }

            output.push_str(&skeleton);
            skeleton_tokens += skeleton_tokens_needed;

            // Per-entry: include reason only if it fits; if not, drop just this reason (not the symbol).
            // This allows later shorter reasons to still fit rather than killing all remaining reasons.
            if skeleton_tokens + reason_tokens_needed <= skeleton_budget {
                output.push_str(&reason_line);
                skeleton_tokens += reason_tokens_needed;
            }
        }
    }

    // Memory injection — 10% budget
    let fqns: Vec<&str> = all_fqns.iter().map(|s| s.as_str()).collect();
    let file_paths: Vec<&str> = all_file_paths.iter().map(|s| s.as_str()).collect();

    if !fqns.is_empty() || !file_paths.is_empty() {
        let observations = crate::memory::store::get_observations_for_context(
            conn, &fqns, &file_paths, 50,
        )
        .unwrap_or_default();

        if !observations.is_empty() {
            let mut mem_section = String::new();
            let mut mem_tokens = 0usize;
            for obs in &observations {
                let entry = format_observation_entry(obs);
                let entry_tokens = estimate_tokens(&entry);
                if mem_tokens + entry_tokens > memory_budget {
                    break;
                }
                mem_section.push_str(&entry);
                mem_tokens += entry_tokens;
            }
            if !mem_section.is_empty() {
                output.push_str("## Session Memory\n\n");
                output.push_str(&mem_section);
            }
        }
    }

    Ok(output)
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

pub(crate) fn get_file_skeleton(conn: &Connection, file_path: &str) -> Result<String, QueryError> {
    // Input-level sensitive check — returns "not permitted" to the caller
    if is_output_sensitive(file_path) {
        return Ok(format!("Access to sensitive file '{file_path}' is not permitted.\n"));
    }

    // Stage 1: exact file match
    let mut candidates = query_file_candidates(conn, "WHERE path = ?1", file_path)?;

    // Stage 2: suffix match only if no exact match
    if candidates.is_empty() {
        let suffix = format!("%{file_path}");
        candidates = query_file_candidates(conn, "WHERE path LIKE ?1", &suffix)?;
    }

    // Sensitive filter on candidates: silently remove — don't reveal that sensitive paths exist
    candidates.retain(|p| !is_output_sensitive(p));

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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

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
        let result = get_context(&conn, root, "fix the crash", &[], 4000).unwrap();
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
}

const MAX_IMPACT_PER_HOP: usize = 100;
const MAX_IMPACT_DEPTH: usize = 10;

pub(crate) fn get_impact(
    conn: &Connection,
    symbol_fqn: &str,
    depth: usize,
) -> Result<String, QueryError> {
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

    while let Some((current_id, current_depth)) = queue.pop_front() {
        if current_depth >= depth { continue; }
        let mut stmt = conn.prepare(
            "SELECT DISTINCT s.id, s.fqn, s.name, f.path, e.kind
             FROM edges e JOIN symbols s ON s.id=e.source_id JOIN files f ON f.id=s.file_id
             WHERE e.target_id=?1
               AND e.kind IN ('calls', 'extends', 'implements')
             LIMIT ?2"
        )?;
        let rows: Vec<(i64, String, String, String, String)> = stmt.query_map(
            params![current_id, MAX_IMPACT_PER_HOP as i64],
            |r| Ok((r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
                    r.get::<_,String>(3)?, r.get::<_,String>(4)?))
        )?.collect::<Result<_,_>>()?;
        if rows.len() == MAX_IMPACT_PER_HOP { truncated = true; }
        for (id, fqn, name, path, kind) in rows {
            if visited.insert(id) {
                queue.push_back((id, current_depth + 1));
                if !is_output_sensitive(&path) {
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
    output.push_str("Note: import relationships are not tracked — only calls, extends, and implements edges.\n\n");

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
