use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, params};

use super::rules::{extract_tokens, jaccard};
use super::store::is_sensitive_path;

/// Minimum Jaccard similarity to consider two observations duplicates.
const JACCARD_THRESHOLD: f64 = 0.85;

struct ObsCandidate {
    id: i64,
    content: String,
    symbol_fqn: Option<String>,
    file_path: Option<String>,
    confidence: Option<f64>,
    created_at: i64,
    branch: Option<String>,
}

/// Consolidate near-duplicate auto-generated observations by soft-deleting duplicates.
/// Sets `consolidated_into = survivor_id` on duplicate rows and increments
/// `consolidation_count` on the survivor. Returns the number of observations consolidated.
///
/// Uses a DEFERRED transaction (allows concurrent reads during candidate detection).
pub fn consolidate_observations(
    conn: &mut Connection,
    branch: Option<&str>,
) -> anyhow::Result<usize> {
    let tx = conn.transaction()?; // DEFERRED by default
    let count = find_and_merge_duplicates(&tx, branch)?;
    tx.commit()?;
    Ok(count)
}

fn find_and_merge_duplicates(
    conn: &Connection,
    branch: Option<&str>,
) -> anyhow::Result<usize> {
    // Fetch auto-generated observations that haven't been consolidated yet
    let ninety_days_ago = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
        - (90 * 24 * 3600);

    let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(b) = branch
    {
        (
            "SELECT id, content, symbol_fqn, file_path, confidence, created_at, branch \
             FROM observations \
             WHERE auto_generated = 1 \
               AND consolidated_into IS NULL \
               AND kind IN ('insight', 'decision', 'error', 'anti_pattern') \
               AND created_at > ?1 \
               AND branch = ?2"
                .to_string(),
            vec![Box::new(ninety_days_ago), Box::new(b.to_string())],
        )
    } else {
        (
            "SELECT id, content, symbol_fqn, file_path, confidence, created_at, branch \
             FROM observations \
             WHERE auto_generated = 1 \
               AND consolidated_into IS NULL \
               AND kind IN ('insight', 'decision', 'error', 'anti_pattern') \
               AND created_at > ?1 \
               AND branch IS NULL"
                .to_string(),
            vec![Box::new(ninety_days_ago)],
        )
    };

    let mut stmt = conn.prepare(&sql)?;
    let params_refs: Vec<&dyn rusqlite::types::ToSql> =
        params_vec.iter().map(|p| p.as_ref()).collect();
    let rows: Vec<ObsCandidate> = stmt
        .query_map(params_refs.as_slice(), |r| {
            Ok(ObsCandidate {
                id: r.get(0)?,
                content: r.get(1)?,
                symbol_fqn: r.get(2)?,
                file_path: r.get(3)?,
                confidence: r.get(4)?,
                created_at: r.get(5)?,
                branch: r.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut scoped: Vec<ScopedObs> = Vec::new();
    for obs in rows {
        let scope_key = if let Some(ref fp) = obs.file_path {
            fp.clone()
        } else if let Some(ref fqn) = obs.symbol_fqn {
            match fqn.find("::") {
                Some(idx) => fqn[..idx].to_string(),
                None => continue,
            }
        } else {
            continue;
        };

        if is_sensitive_path(&scope_key) {
            continue;
        }

        let token_vec = extract_tokens(&obs.content);
        let tokens: HashSet<String> = token_vec.into_iter().collect();
        if tokens.is_empty() {
            continue;
        }

        scoped.push(ScopedObs {
            scope_key,
            obs,
            tokens,
        });
    }

    // Group by (scope_key, branch)
    let mut groups: HashMap<(String, Option<String>), Vec<usize>> = HashMap::new();
    for (i, s) in scoped.iter().enumerate() {
        groups
            .entry((s.scope_key.clone(), s.obs.branch.clone()))
            .or_default()
            .push(i);
    }

    let mut total_consolidated = 0usize;

    for indices in groups.values() {
        if indices.len() < 2 {
            continue;
        }

        // Single-linkage clustering with Jaccard >= JACCARD_THRESHOLD
        let mut clusters = single_linkage_cluster(indices, &scoped);

        for cluster in &mut clusters {
            if cluster.len() < 2 {
                continue;
            }

            // Survivor: highest confidence (NULL=0.5), most recent created_at, highest id as tiebreaker
            cluster.sort_by(|&a, &b| {
                let conf_a = scoped[a].obs.confidence.unwrap_or(0.5);
                let conf_b = scoped[b].obs.confidence.unwrap_or(0.5);
                conf_b
                    .total_cmp(&conf_a)
                    .then_with(|| scoped[b].obs.created_at.cmp(&scoped[a].obs.created_at))
                    .then_with(|| scoped[b].obs.id.cmp(&scoped[a].obs.id))
            });

            let survivor_idx = cluster[0];
            let survivor_id = scoped[survivor_idx].obs.id;
            let duplicates: Vec<i64> = cluster[1..]
                .iter()
                .map(|&i| scoped[i].obs.id)
                .collect();

            if duplicates.is_empty() {
                continue;
            }

            // Soft-delete duplicates
            for dup_id in &duplicates {
                conn.execute(
                    "UPDATE observations SET consolidated_into = ?1 WHERE id = ?2",
                    params![survivor_id, dup_id],
                )?;
            }

            // Increment consolidation_count on survivor
            conn.execute(
                "UPDATE observations SET consolidation_count = consolidation_count + ?1 WHERE id = ?2",
                params![duplicates.len() as i64, survivor_id],
            )?;

            total_consolidated += duplicates.len();
        }
    }

    Ok(total_consolidated)
}

/// Single-linkage clustering: if any pair in a cluster has Jaccard >= threshold,
/// they belong to the same cluster.
fn single_linkage_cluster(
    indices: &[usize],
    scoped: &[ScopedObs],
) -> Vec<Vec<usize>> {
    let n = indices.len();
    // Union-Find
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while parent[root] != root {
            root = parent[root];
        }
        // Path compression
        let mut cur = i;
        while parent[cur] != root {
            let next = parent[cur];
            parent[cur] = root;
            cur = next;
        }
        root
    }

    fn union(parent: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[ra] = rb;
        }
    }

    for i in 0..n {
        for j in (i + 1)..n {
            let sim = jaccard(&scoped[indices[i]].tokens, &scoped[indices[j]].tokens);
            if sim >= JACCARD_THRESHOLD {
                union(&mut parent, i, j);
            }
        }
    }

    // Collect clusters
    let mut cluster_map: HashMap<usize, Vec<usize>> = HashMap::new();
    for (i, &idx) in indices.iter().enumerate().take(n) {
        let root = find(&mut parent, i);
        cluster_map.entry(root).or_default().push(idx);
    }

    cluster_map.into_values().collect()
}

struct ScopedObs {
    scope_key: String,
    obs: ObsCandidate,
    tokens: HashSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    fn open_test_db() -> (rusqlite::Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open(&db_path).expect("open DB");
        (conn, dir)
    }

    fn insert_session(conn: &Connection, id: &str) {
        conn.execute(
            "INSERT OR IGNORE INTO sessions (id, started_at, agent) VALUES (?1, ?2, 'test')",
            params![id, now_secs()],
        )
        .unwrap();
    }

    fn insert_auto_obs(
        conn: &Connection,
        session_id: &str,
        content: &str,
        file_path: Option<&str>,
        branch: Option<&str>,
        confidence: Option<f64>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, file_path, auto_generated, branch, confidence) \
             VALUES (?1, ?2, 'insight', ?3, ?4, 1, ?5, ?6)",
            params![session_id, now_secs(), content, file_path, branch, confidence],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_manual_obs(
        conn: &Connection,
        session_id: &str,
        content: &str,
        file_path: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, file_path, auto_generated) \
             VALUES (?1, ?2, 'insight', ?3, ?4, 0)",
            params![session_id, now_secs(), content, file_path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn is_consolidated(conn: &Connection, id: i64) -> bool {
        conn.query_row(
            "SELECT consolidated_into FROM observations WHERE id = ?1",
            params![id],
            |r| r.get::<_, Option<i64>>(0),
        )
        .unwrap()
        .is_some()
    }

    fn get_consolidation_count(conn: &Connection, id: i64) -> i64 {
        conn.query_row(
            "SELECT consolidation_count FROM observations WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn test_no_duplicates_returns_zero() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        insert_auto_obs(&conn, "s1", "alpha beta gamma delta epsilon zeta eta theta", Some("src/foo.rs"), None, None);
        insert_auto_obs(&conn, "s1", "completely different unique content words here now", Some("src/bar.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_exact_duplicates_consolidated() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        let id1 = insert_auto_obs(&conn, "s1", "the function parse_config reads configuration from disk and validates schema", Some("src/config.rs"), None, None);
        let id2 = insert_auto_obs(&conn, "s2", "the function parse_config reads configuration from disk and validates schema", Some("src/config.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 1);
        assert!(!is_consolidated(&conn, id1) || !is_consolidated(&conn, id2), "one must be survivor");
        // The one that is NOT consolidated is the survivor
        let survivor = if is_consolidated(&conn, id1) { id2 } else { id1 };
        assert_eq!(get_consolidation_count(&conn, survivor), 1);
    }

    #[test]
    fn test_near_duplicates_consolidated() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        // Near-duplicate: 8 tokens identical, 1 token differs → Jaccard = 7/9 < 0.85
        // Use 7+ shared top tokens so that with top-8 cutoff, at least 7/8 match → 7/9 still < 0.85
        // Instead: make both have exactly the same top-8 tokens
        let id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration validates schema correctly", Some("src/config.rs"), None, None);
        let id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration validates schema correctly well", Some("src/config.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 1);
        assert!(!is_consolidated(&conn, id1) || !is_consolidated(&conn, id2));
    }

    #[test]
    fn test_manual_observations_not_consolidated() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let id1 = insert_manual_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"));
        let id2 = insert_manual_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"));
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0);
        assert!(!is_consolidated(&conn, id1));
        assert!(!is_consolidated(&conn, id2));
    }

    #[test]
    fn test_survivor_has_highest_confidence() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let low = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.3));
        let high = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.9));
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 1);
        assert!(!is_consolidated(&conn, high), "high-confidence must survive");
        assert!(is_consolidated(&conn, low), "low-confidence must be consolidated");
    }

    #[test]
    fn test_branch_scoping_isolates() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"), Some("main"), None);
        let id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"), Some("feature"), None);

        // Consolidate for branch "main" only
        let count = consolidate_observations(&mut conn, Some("main")).unwrap();
        assert_eq!(count, 0, "different branches should not be merged");
        assert!(!is_consolidated(&conn, id1));
        assert!(!is_consolidated(&conn, id2));
    }

    #[test]
    fn test_different_scope_keys_not_merged() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"), None, None);
        let id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/other.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0, "different file scopes should not merge");
        assert!(!is_consolidated(&conn, id1));
        assert!(!is_consolidated(&conn, id2));
    }

    #[test]
    fn test_three_duplicates_two_consolidated() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.5));
        let id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.8));
        let id3 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.3));
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 2);
        assert!(!is_consolidated(&conn, id2), "highest confidence survives");
        assert!(is_consolidated(&conn, id1));
        assert!(is_consolidated(&conn, id3));
        assert_eq!(get_consolidation_count(&conn, id2), 2);
    }

    #[test]
    fn test_already_consolidated_skipped() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, None);
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, None);

        // First pass
        let count1 = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count1, 1);

        // Second pass — no new consolidations
        let count2 = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count2, 0);
    }

    #[test]
    fn test_stale_observations_still_consolidated() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"), None, None);
        // Mark as stale — staleness is orthogonal, should still consolidate
        conn.execute("UPDATE observations SET is_stale = 1 WHERE id = ?1", params![id1]).unwrap();
        let id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 1, "stale observations should still be consolidated");
        assert!(!is_consolidated(&conn, id1) || !is_consolidated(&conn, id2), "one must be survivor");
    }

    #[test]
    fn test_sensitive_paths_excluded() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some(".env"), None, None);
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some(".env"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0, "sensitive paths should be excluded");
    }

    #[test]
    fn test_null_branch_obs_excluded_with_branch_filter() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let _id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, None);
        let _id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), Some("main"), None);
        // One-partition scoping: Some("main") only matches branch = 'main', not NULL branch
        let count = consolidate_observations(&mut conn, Some("main")).unwrap();
        assert_eq!(count, 0, "only one main-branch obs, nothing to merge");
    }

    #[test]
    fn test_no_scope_key_skipped() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        // Observations with no file_path and no symbol_fqn are skipped
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", None, None, None);
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", None, None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_below_threshold_not_merged() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        // Two observations with low similarity (different key tokens)
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration schema validation disk", Some("src/config.rs"), None, None);
        insert_auto_obs(&conn, "s1", "struct database connection pool initialized properly working", Some("src/config.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_symbol_fqn_scope_key() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let ts = now_secs();
        // Use symbol_fqn instead of file_path for scope key
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, auto_generated) \
             VALUES ('s1', ?1, 'insight', 'function parse_config reads configuration from disk validates schema correctly', 'src/config.rs::parse_config', 1)",
            params![ts],
        ).unwrap();
        let id1 = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, symbol_fqn, auto_generated) \
             VALUES ('s1', ?1, 'insight', 'function parse_config reads configuration from disk validates schema correctly', 'src/config.rs::parse_config', 1)",
            params![ts],
        ).unwrap();
        let id2 = conn.last_insert_rowid();
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 1);
        assert!(!is_consolidated(&conn, id1) || !is_consolidated(&conn, id2));
    }

    #[test]
    fn test_empty_database_returns_zero() {
        let (mut conn, _dir) = open_test_db();
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_single_observation_returns_zero() {
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema", Some("src/config.rs"), None, None);
        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_survivor_delete_cascades_to_duplicates() {
        // Verify that deleting a survivor also deletes its consolidated duplicates
        // (not resurrecting them via SET NULL).
        let (mut conn, _dir) = open_test_db();
        insert_session(&conn, "s1");
        let id1 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.9));
        let id2 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.3));
        let id3 = insert_auto_obs(&conn, "s1", "function parse_config reads configuration from disk validates schema correctly", Some("src/config.rs"), None, Some(0.1));

        let count = consolidate_observations(&mut conn, None).unwrap();
        assert_eq!(count, 2);
        assert!(!is_consolidated(&conn, id1), "highest confidence survives");
        assert!(is_consolidated(&conn, id2));
        assert!(is_consolidated(&conn, id3));

        // Delete the survivor — duplicates must cascade-delete, not resurrect
        conn.execute("DELETE FROM observations WHERE id = ?1", params![id1]).unwrap();

        let remaining: i64 = conn.query_row(
            "SELECT COUNT(*) FROM observations WHERE id IN (?1, ?2, ?3)",
            params![id1, id2, id3],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(remaining, 0, "deleting survivor must cascade-delete consolidated duplicates");
    }
}
