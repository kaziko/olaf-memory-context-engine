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

pub(crate) enum IntentMode {
    Debug,
    BlastRadius,
    Balanced,
}

pub(crate) fn detect_intent(intent: &str) -> IntentMode {
    let lower = intent.to_lowercase();
    if lower.contains("fix bug") || lower.contains("debug") || lower.contains(" bug") || lower.starts_with("bug") {
        IntentMode::Debug
    } else if lower.contains("refactor") {
        IntentMode::BlastRadius
    } else {
        IntentMode::Balanced
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

fn find_pivot_symbols(conn: &Connection, intent: &str, file_hints: &[String]) -> Result<Vec<i64>, QueryError> {
    let mut ids: Vec<i64> = Vec::new();
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
                if !ids.contains(&id) { ids.push(id); }
            }
        }
        if !ids.is_empty() { return Ok(ids); }
    }

    // Keyword match: use words > 3 chars from intent
    let words: Vec<&str> = intent.split_whitespace().filter(|w| w.len() > 3).collect();
    for word in &words {
        let pattern = format!("%{}%", word.to_lowercase());
        let mut stmt = conn.prepare(
            "SELECT id FROM symbols WHERE lower(name) LIKE ?1 LIMIT 5"
        )?;
        let rows: Vec<i64> = stmt.query_map(params![pattern], |r| r.get(0))?
            .collect::<Result<_,_>>()?;
        for id in rows {
            if !ids.contains(&id) { ids.push(id); }
        }
    }

    // Fallback: any symbols
    if ids.is_empty() {
        let mut stmt = conn.prepare("SELECT id FROM symbols LIMIT 5")?;
        ids = stmt.query_map([], |r| r.get(0))?.collect::<Result<_,_>>()?;
    }

    Ok(ids)
}

fn traverse_bfs(
    conn: &Connection,
    pivot_ids: &[i64],
    mode: &IntentMode,
    depth: usize,
) -> Result<(Vec<i64>, Vec<i64>), QueryError> {
    let pivot_set: HashSet<i64> = pivot_ids.iter().copied().collect();
    let mut visited: HashSet<i64> = pivot_set.clone();
    let mut queue: VecDeque<(i64, usize)> = pivot_ids.iter().map(|&id| (id, 0)).collect();
    let mut supporting: Vec<i64> = Vec::new();

    while let Some((current_id, current_depth)) = queue.pop_front() {
        if current_depth >= depth { continue; }

        // Outbound edges
        let mut stmt = conn.prepare(
            "SELECT DISTINCT target_id FROM edges WHERE source_id=?1 LIMIT 20"
        )?;
        let outbound: Vec<i64> = stmt.query_map(params![current_id], |r| r.get(0))?
            .collect::<Result<_,_>>()?;
        for id in outbound {
            if visited.insert(id) {
                queue.push_back((id, current_depth + 1));
                if !pivot_set.contains(&id) { supporting.push(id); }
            }
        }

        // Inbound edges for debug and blast-radius modes
        let include_inbound = matches!(mode, IntentMode::Debug | IntentMode::BlastRadius);
        if include_inbound {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT source_id FROM edges WHERE target_id=?1 LIMIT 20"
            )?;
            let inbound: Vec<i64> = stmt.query_map(params![current_id], |r| r.get(0))?
                .collect::<Result<_,_>>()?;
            for id in inbound {
                if visited.insert(id) {
                    queue.push_back((id, current_depth + 1));
                    if !pivot_set.contains(&id) { supporting.push(id); }
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
    let pivot_ids = find_pivot_symbols(conn, intent, file_hints)?;

    if pivot_ids.is_empty() {
        return Ok(format!(
            "No symbols found for intent: {intent}\n\nRun `olaf index` first."
        ));
    }

    let mode = detect_intent(intent);
    let bfs_depth = match mode {
        IntentMode::BlastRadius => 3,
        _ => 2,
    };

    let (pivots, supporting) = traverse_bfs(conn, &pivot_ids, &mode, bfs_depth)?;

    let pivot_budget = token_budget * 70 / 100;
    let skeleton_budget = token_budget * 20 / 100;

    let mut output = format!("# Context Brief: {intent}\n\n## Pivot Symbols\n\n");
    let mut pivot_tokens = 0usize;

    for id in &pivots {
        let Some(row) = load_symbol_row(conn, *id)? else { continue };
        if is_output_sensitive(&row.file_path) { continue; }

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

    if !supporting.is_empty() {
        output.push_str("## Supporting Symbols\n\n");
        let mut skeleton_tokens = 0usize;
        for id in &supporting {
            let Some(row) = load_symbol_row(conn, *id)? else { continue };
            if is_output_sensitive(&row.file_path) { continue; }

            let skeleton = skeletonize(conn, row.id)?;
            let entry_tokens = estimate_tokens(&skeleton);
            if skeleton_tokens + entry_tokens > skeleton_budget { break; }
            output.push_str(&skeleton);
            skeleton_tokens += entry_tokens;
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
