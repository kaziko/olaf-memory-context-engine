use std::collections::VecDeque;

use crate::policy::ContentPolicy;

/// Layer 2 sensitive-file exclusion (defense-in-depth).
/// KEEP IN SYNC with `is_output_sensitive` in `graph/query.rs`.
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

pub(crate) const MAX_PATHS_DEFAULT: usize = 5;
pub(crate) const MAX_PATHS_LIMIT: usize = 20;
const MAX_DEPTH_LIMIT: usize = 15;
const MAX_NEIGHBORS_PER_HOP: usize = 50;

pub(crate) struct PathNode {
    pub fqn: String,
    pub file_path: String,
}

pub(crate) struct TraceResult {
    pub paths: Vec<Vec<PathNode>>,
    pub depth_limit_hit: bool,
    pub neighbor_cap_hit: bool,
}

pub(crate) fn trace_flow(
    conn: &rusqlite::Connection,
    source_id: i64,
    target_id: i64,
    max_paths: usize,
) -> Result<TraceResult, rusqlite::Error> {
    let max_paths = max_paths.clamp(1, MAX_PATHS_LIMIT);

    if source_id == target_id {
        let node = resolve_node(conn, source_id)?;
        return Ok(TraceResult { paths: vec![vec![node]], depth_limit_hit: false, neighbor_cap_hit: false });
    }

    let mut queue: VecDeque<Vec<i64>> = VecDeque::new();
    queue.push_back(vec![source_id]);

    let mut id_paths: Vec<Vec<i64>> = Vec::new();
    let mut shortest_depth: Option<usize> = None;
    let mut depth_limit_hit = false;
    let mut neighbor_cap_hit = false;

    'bfs: while let Some(path) = queue.pop_front() {
        let current_id = *path.last().unwrap();
        let current_depth = path.len() - 1;

        // Don't expand beyond found shortest depth
        if let Some(d) = shortest_depth
            && current_depth >= d
        {
            continue;
        }

        // Hard depth cap
        if current_depth >= MAX_DEPTH_LIMIT {
            depth_limit_hit = true;
            continue;
        }

        let neighbors = get_neighbors(conn, current_id, &mut neighbor_cap_hit)?;
        for (neighbor_id, _, _) in neighbors {
            if path.contains(&neighbor_id) { continue; } // per-path cycle detection

            let mut new_path = path.clone();
            new_path.push(neighbor_id);

            if neighbor_id == target_id {
                shortest_depth = Some(new_path.len() - 1);
                id_paths.push(new_path);
                if id_paths.len() >= max_paths { break 'bfs; }
            } else {
                queue.push_back(new_path);
            }
        }
    }

    let paths = id_paths.into_iter()
        .map(|p| p.into_iter().map(|id| resolve_node(conn, id)).collect::<Result<Vec<_>, _>>())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TraceResult { paths, depth_limit_hit, neighbor_cap_hit })
}

fn get_neighbors(
    conn: &rusqlite::Connection,
    symbol_id: i64,
    cap_hit: &mut bool,
) -> Result<Vec<(i64, String, String)>, rusqlite::Error> {
    let fetch_limit = (MAX_NEIGHBORS_PER_HOP + 1) as i64;
    let mut stmt = conn.prepare_cached(
        "SELECT s.id, s.fqn, f.path
         FROM edges e
         JOIN symbols s ON s.id = e.target_id
         JOIN files f ON f.id = s.file_id
         WHERE e.source_id = ?1
           AND e.kind IN ('calls', 'extends', 'implements')
         GROUP BY s.id
         ORDER BY MIN(e.id) ASC, s.id ASC
         LIMIT ?2"
    )?;
    let mut rows: Vec<(i64, String, String)> = stmt.query_map(
        rusqlite::params![symbol_id, fetch_limit],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    )?.collect::<Result<_, _>>()?;

    if rows.len() > MAX_NEIGHBORS_PER_HOP {
        rows.truncate(MAX_NEIGHBORS_PER_HOP);
        *cap_hit = true;
    }
    Ok(rows)
}

fn resolve_node(conn: &rusqlite::Connection, id: i64) -> Result<PathNode, rusqlite::Error> {
    conn.query_row(
        "SELECT s.fqn, f.path FROM symbols s JOIN files f ON f.id = s.file_id WHERE s.id = ?1",
        [id],
        |r| Ok(PathNode { fqn: r.get(0)?, file_path: r.get(1)? }),
    )
}

pub(crate) fn format_trace_result(source_fqn: &str, target_fqn: &str, result: &TraceResult, content_policy: &ContentPolicy) -> String {
    // Direct-query guard: denied FQNs (by fqn_prefix or path rules) return "not found"
    if content_policy.is_denied_by_fqn(source_fqn) {
        return format!("Symbol not found: {source_fqn}\n\nRun `olaf index` first.");
    }
    if content_policy.is_denied_by_fqn(target_fqn) {
        return format!("Symbol not found: {target_fqn}\n\nRun `olaf index` first.");
    }

    let mut out = format!("# Trace Flow: {source_fqn} → {target_fqn}\n\n");

    // Defense-in-depth: filter paths that contain any sensitive or denied node.
    let visible_paths: Vec<&Vec<PathNode>> = result.paths.iter()
        .filter(|path| !path.iter().any(|n|
            is_output_sensitive(&n.file_path) || content_policy.is_denied(&n.file_path, Some(&n.fqn))
        ))
        .collect();

    if visible_paths.is_empty() {
        out.push_str("No execution path found between these symbols.\n\n");
        out.push_str("This may mean:\n");
        out.push_str("- The symbols are not connected via calls/extends/implements edges\n");
        out.push_str("- The index is out of date — run `olaf index` to refresh\n");
    } else {
        out.push_str(&format!("{} path(s) found\n\n", visible_paths.len()));
        for (i, path) in visible_paths.iter().enumerate() {
            let hops = path.len() - 1;
            out.push_str(&format!("### Path {} ({} hop{})\n\n", i + 1, hops, if hops == 1 { "" } else { "s" }));
            for node in path.iter() {
                out.push_str(&format!("- `{}` ({})\n", node.fqn, node.file_path));
            }
            out.push('\n');
        }
    }

    if result.depth_limit_hit {
        out.push_str(&format!("⚠ Depth limit ({MAX_DEPTH_LIMIT}) reached — some paths may not have been explored\n"));
    }
    if result.neighbor_cap_hit {
        out.push_str(&format!("⚠ Neighbor cap ({MAX_NEIGHBORS_PER_HOP}) hit on at least one node — some paths may not have been explored\n"));
    }
    out
}

#[cfg(test)]
fn build_trace_test_db() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT NOT NULL, hash TEXT, lang TEXT, last_indexed_at INTEGER);
        CREATE TABLE symbols (id INTEGER PRIMARY KEY, file_id INTEGER, fqn TEXT, name TEXT, kind TEXT,
                              start_line INTEGER, end_line INTEGER, signature TEXT, source_hash TEXT, docstring TEXT);
        CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT);
        INSERT INTO files VALUES (1, 'src/a.rs', 'h', 'rust', 0);
    ").unwrap();
    conn
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_direct_call() {
        let conn = build_trace_test_db();
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A', 'A', 'fn', 1, 2, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (2, 1, 'B', 'B', 'fn', 3, 4, NULL, NULL, NULL);
            INSERT INTO edges VALUES (1, 1, 2, 'calls');
        ").unwrap();

        let result = trace_flow(&conn, 1, 2, 5).unwrap();
        assert_eq!(result.paths.len(), 1);
        assert_eq!(result.paths[0].len(), 2);
        assert_eq!(result.paths[0][0].fqn, "A");
        assert_eq!(result.paths[0][1].fqn, "B");
    }

    #[test]
    fn test_two_hop() {
        let conn = build_trace_test_db();
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A', 'A', 'fn', 1, 2, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (2, 1, 'B', 'B', 'fn', 3, 4, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (3, 1, 'C', 'C', 'fn', 5, 6, NULL, NULL, NULL);
            INSERT INTO edges VALUES (1, 1, 2, 'calls');
            INSERT INTO edges VALUES (2, 2, 3, 'calls');
        ").unwrap();

        let result = trace_flow(&conn, 1, 3, 5).unwrap();
        assert_eq!(result.paths.len(), 1);
        assert_eq!(result.paths[0].len(), 3);
        assert_eq!(result.paths[0][0].fqn, "A");
        assert_eq!(result.paths[0][1].fqn, "B");
        assert_eq!(result.paths[0][2].fqn, "C");
    }

    #[test]
    fn test_no_path() {
        let conn = build_trace_test_db();
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A', 'A', 'fn', 1, 2, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (2, 1, 'B', 'B', 'fn', 3, 4, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (3, 1, 'C', 'C', 'fn', 5, 6, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (4, 1, 'D', 'D', 'fn', 7, 8, NULL, NULL, NULL);
            INSERT INTO edges VALUES (1, 1, 2, 'calls');
            INSERT INTO edges VALUES (2, 3, 4, 'calls');
        ").unwrap();

        let result = trace_flow(&conn, 1, 4, 5).unwrap();
        assert!(result.paths.is_empty());
    }

    #[test]
    fn test_cycle_terminates() {
        let conn = build_trace_test_db();
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A', 'A', 'fn', 1, 2, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (2, 1, 'B', 'B', 'fn', 3, 4, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (3, 1, 'C', 'C', 'fn', 5, 6, NULL, NULL, NULL);
            INSERT INTO edges VALUES (1, 1, 2, 'calls');
            INSERT INTO edges VALUES (2, 2, 1, 'calls');
        ").unwrap();

        // C has no edges — trace_flow(A, C) should terminate with empty result
        let result = trace_flow(&conn, 1, 3, 5).unwrap();
        assert!(result.paths.is_empty());
    }

    #[test]
    fn test_multiple_shortest_paths() {
        let conn = build_trace_test_db();
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A', 'A', 'fn', 1, 2, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (2, 1, 'B', 'B', 'fn', 3, 4, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (3, 1, 'C', 'C', 'fn', 5, 6, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (4, 1, 'D', 'D', 'fn', 7, 8, NULL, NULL, NULL);
            INSERT INTO edges VALUES (1, 1, 2, 'calls');
            INSERT INTO edges VALUES (2, 2, 4, 'calls');
            INSERT INTO edges VALUES (3, 1, 3, 'calls');
            INSERT INTO edges VALUES (4, 3, 4, 'calls');
        ").unwrap();

        let result = trace_flow(&conn, 1, 4, 5).unwrap();
        assert_eq!(result.paths.len(), 2);
        for path in &result.paths {
            assert_eq!(path.len(), 3); // 2 hops = 3 nodes
            assert_eq!(path[0].fqn, "A");
            assert_eq!(path[2].fqn, "D");
        }
    }

    #[test]
    fn test_source_equals_target() {
        let conn = build_trace_test_db();
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A', 'A', 'fn', 1, 2, NULL, NULL, NULL);
        ").unwrap();

        let result = trace_flow(&conn, 1, 1, 5).unwrap();
        assert_eq!(result.paths.len(), 1);
        assert_eq!(result.paths[0].len(), 1);
        assert_eq!(result.paths[0][0].fqn, "A");
    }

    #[test]
    fn test_format_depth_limit_warning() {
        let result = TraceResult {
            paths: vec![],
            depth_limit_hit: true,
            neighbor_cap_hit: false,
        };
        let out = format_trace_result("A", "B", &result, &ContentPolicy::default());
        assert!(out.contains("Depth limit"), "must contain depth limit warning; got: {out}");
        assert!(!out.contains("Neighbor cap"), "must not contain neighbor cap warning; got: {out}");
    }

    #[test]
    fn test_format_neighbor_cap_warning() {
        let result = TraceResult {
            paths: vec![],
            depth_limit_hit: false,
            neighbor_cap_hit: true,
        };
        let out = format_trace_result("A", "B", &result, &ContentPolicy::default());
        assert!(out.contains("Neighbor cap"), "must contain neighbor cap warning; got: {out}");
        assert!(!out.contains("Depth limit"), "must not contain depth limit warning; got: {out}");
    }

    #[test]
    fn test_format_sensitive_path_filtered() {
        let result = TraceResult {
            paths: vec![vec![
                PathNode { fqn: "A".into(), file_path: "src/a.ts".into() },
                PathNode { fqn: "B".into(), file_path: ".env".into() },
            ]],
            depth_limit_hit: false,
            neighbor_cap_hit: false,
        };
        let out = format_trace_result("A", "B", &result, &ContentPolicy::default());
        assert!(out.contains("No execution path found"), "sensitive path must be filtered; got: {out}");
    }

    #[test]
    fn test_max_paths_cap() {
        let conn = build_trace_test_db();
        // A(1), B1..B6(2..7), D(8)
        conn.execute_batch("
            INSERT INTO symbols VALUES (1, 1, 'A',  'A',  'fn', 1,  2,  NULL, NULL, NULL);
            INSERT INTO symbols VALUES (2, 1, 'B1', 'B1', 'fn', 3,  4,  NULL, NULL, NULL);
            INSERT INTO symbols VALUES (3, 1, 'B2', 'B2', 'fn', 5,  6,  NULL, NULL, NULL);
            INSERT INTO symbols VALUES (4, 1, 'B3', 'B3', 'fn', 7,  8,  NULL, NULL, NULL);
            INSERT INTO symbols VALUES (5, 1, 'B4', 'B4', 'fn', 9,  10, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (6, 1, 'B5', 'B5', 'fn', 11, 12, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (7, 1, 'B6', 'B6', 'fn', 13, 14, NULL, NULL, NULL);
            INSERT INTO symbols VALUES (8, 1, 'D',  'D',  'fn', 15, 16, NULL, NULL, NULL);
            INSERT INTO edges VALUES (1,  1, 2, 'calls');
            INSERT INTO edges VALUES (2,  1, 3, 'calls');
            INSERT INTO edges VALUES (3,  1, 4, 'calls');
            INSERT INTO edges VALUES (4,  1, 5, 'calls');
            INSERT INTO edges VALUES (5,  1, 6, 'calls');
            INSERT INTO edges VALUES (6,  1, 7, 'calls');
            INSERT INTO edges VALUES (7,  2, 8, 'calls');
            INSERT INTO edges VALUES (8,  3, 8, 'calls');
            INSERT INTO edges VALUES (9,  4, 8, 'calls');
            INSERT INTO edges VALUES (10, 5, 8, 'calls');
            INSERT INTO edges VALUES (11, 6, 8, 'calls');
            INSERT INTO edges VALUES (12, 7, 8, 'calls');
        ").unwrap();

        let result = trace_flow(&conn, 1, 8, 3).unwrap();
        assert_eq!(result.paths.len(), 3);
    }
}
