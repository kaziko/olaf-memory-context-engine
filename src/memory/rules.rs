use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension, params};

use super::store::is_sensitive_path;

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ProjectRule {
    pub id: i64,
    pub content: String,
    pub scope_fingerprint: String,
    pub support_count: i64,
    pub session_count: i64,
    pub last_seen_at: i64,
    pub is_active: i32,
    pub stale_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub branch: Option<String>,
    pub symbol_fqns: Vec<String>,
    pub file_paths: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct RuleCandidate {
    pub content: String,
    pub scope_fingerprint: String,
    pub source_observation_ids: Vec<i64>,
    pub symbol_fqns: Vec<String>,
    pub file_paths: Vec<String>,
    pub support_count: usize,
    pub session_count: usize,
    pub last_seen_at: i64,
    pub branch: Option<String>,
}

// ~50 common English stop words
pub(crate) const STOP_WORDS: &[&str] = &[
    "the", "and", "for", "that", "with", "this", "from", "have", "been",
    "will", "would", "could", "should", "about", "into", "when", "where",
    "which", "their", "there", "then", "than", "other", "some", "only",
    "also", "after", "before", "more", "most", "such", "each", "make",
    "like", "over", "very", "just", "does", "being", "used", "using",
    "because", "between", "through", "while", "these", "those", "what",
    "your", "they", "were", "are", "was", "not", "but", "can", "all",
];

/// Compute a deterministic scope fingerprint from file paths and content tokens.
pub(crate) fn compute_scope_fingerprint(file_paths: &[String], content_tokens: &[String]) -> String {
    let mut sorted_paths = file_paths.to_vec();
    sorted_paths.sort();
    let mut sorted_tokens = content_tokens.to_vec();
    sorted_tokens.sort();

    let input = format!(
        "files:{}|tokens:{}",
        sorted_paths.join(","),
        sorted_tokens.join(",")
    );
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

/// Extract content tokens from text for Jaccard similarity.
pub(crate) fn extract_tokens(content: &str) -> Vec<String> {
    let stop_set: HashSet<&str> = STOP_WORDS.iter().copied().collect();
    let mut freq: HashMap<String, usize> = HashMap::new();

    for word in content.split_whitespace() {
        let lower = word.to_lowercase();
        // Remove non-alphanumeric from edges
        let trimmed: &str = lower.trim_matches(|c: char| !c.is_alphanumeric());
        if trimmed.len() < 3 {
            continue;
        }
        if stop_set.contains(trimmed) {
            continue;
        }
        *freq.entry(trimmed.to_string()).or_default() += 1;
    }

    let mut tokens: Vec<(String, usize)> = freq.into_iter().collect();
    tokens.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    tokens.into_iter().take(8).map(|(t, _)| t).collect()
}

/// Compute Jaccard similarity between two token sets.
pub(crate) fn jaccard(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let intersection = a.intersection(b).count();
    let union = a.union(b).count();
    intersection as f64 / union as f64
}

struct ObsInfo {
    id: i64,
    session_id: String,
    content: String,
    symbol_fqn: Option<String>,
    file_path: Option<String>,
    confidence: Option<f64>,
    created_at: i64,
    branch: Option<String>,
}

/// Detect rule candidates from observation patterns.
fn detect_rule_candidates(
    conn: &Connection,
    branch: Option<&str>,
) -> anyhow::Result<Vec<RuleCandidate>> {
    let ninety_days_ago = now_secs() - (90 * 24 * 3600);

    let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(b) = branch
    {
        (
            "SELECT id, session_id, content, symbol_fqn, file_path, confidence, created_at, branch \
             FROM observations \
             WHERE kind IN ('insight', 'decision') \
               AND is_stale = 0 \
               AND consolidated_into IS NULL \
               AND created_at > ?1 \
               AND (branch = ?2 OR branch IS NULL)"
                .to_string(),
            vec![Box::new(ninety_days_ago), Box::new(b.to_string())],
        )
    } else {
        (
            "SELECT id, session_id, content, symbol_fqn, file_path, confidence, created_at, branch \
             FROM observations \
             WHERE kind IN ('insight', 'decision') \
               AND is_stale = 0 \
               AND consolidated_into IS NULL \
               AND created_at > ?1"
                .to_string(),
            vec![Box::new(ninety_days_ago)],
        )
    };

    let mut stmt = conn.prepare(&sql)?;
    let params_refs: Vec<&dyn rusqlite::types::ToSql> =
        params_vec.iter().map(|p| p.as_ref()).collect();
    let rows: Vec<ObsInfo> = stmt
        .query_map(params_refs.as_slice(), |r| {
            Ok(ObsInfo {
                id: r.get(0)?,
                session_id: r.get(1)?,
                content: r.get(2)?,
                symbol_fqn: r.get(3)?,
                file_path: r.get(4)?,
                confidence: r.get(5)?,
                created_at: r.get(6)?,
                branch: r.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Derive scope key per observation
    struct ScopedObs {
        scope_key: String,
        obs: ObsInfo,
        tokens: HashSet<String>,
    }

    let mut scoped: Vec<ScopedObs> = Vec::new();
    for obs in rows {
        let scope_key = if let Some(ref fp) = obs.file_path {
            fp.clone()
        } else if let Some(ref fqn) = obs.symbol_fqn {
            // Extract file path prefix (everything before first "::")
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

    // Group by scope key
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, so) in scoped.iter().enumerate() {
        groups.entry(so.scope_key.clone()).or_default().push(i);
    }

    let mut candidates = Vec::new();

    for indices in groups.values() {
        if indices.len() < 3 {
            continue;
        }

        // Build pairwise similarity and single-linkage clusters
        let n = indices.len();
        let mut cluster_id: Vec<usize> = (0..n).collect();

        // Union-Find helpers
        fn find(parent: &mut [usize], i: usize) -> usize {
            let mut r = i;
            while parent[r] != r {
                parent[r] = parent[parent[r]];
                r = parent[r];
            }
            r
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
                if sim > 0.4 {
                    union(&mut cluster_id, i, j);
                }
            }
        }

        // Collect clusters
        let mut clusters: HashMap<usize, Vec<usize>> = HashMap::new();
        for i in 0..n {
            let root = find(&mut cluster_id, i);
            clusters.entry(root).or_default().push(i);
        }

        for (_root, members) in clusters {
            if members.len() < 3 {
                continue;
            }

            // Cluster validation: compute centroid token set
            let mut centroid_freq: HashMap<String, usize> = HashMap::new();
            for &m in &members {
                for token in &scoped[indices[m]].tokens {
                    *centroid_freq.entry(token.clone()).or_default() += 1;
                }
            }
            let mut centroid_sorted: Vec<(String, usize)> = centroid_freq.into_iter().collect();
            centroid_sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let centroid: HashSet<String> = centroid_sorted
                .iter()
                .take(8)
                .map(|(t, _)| t.clone())
                .collect();

            // Validate each member against centroid
            let valid_members: Vec<usize> = members
                .into_iter()
                .filter(|&m| jaccard(&scoped[indices[m]].tokens, &centroid) > 0.3)
                .collect();

            if valid_members.len() < 3 {
                continue;
            }

            // Check 3+ distinct sessions
            let session_ids: HashSet<&str> = valid_members
                .iter()
                .map(|&m| scoped[indices[m]].obs.session_id.as_str())
                .collect();
            if session_ids.len() < 3 {
                continue;
            }

            // Check branch consistency: all same branch or all NULL
            let branches: HashSet<Option<&str>> = valid_members
                .iter()
                .map(|&m| scoped[indices[m]].obs.branch.as_deref())
                .collect();
            if branches.len() > 1 {
                continue; // Mixed branches — discard
            }
            let cluster_branch = branches.into_iter().next().unwrap();

            // Pick most representative observation: highest confidence, most recent as tiebreaker
            let best_idx = valid_members
                .iter()
                .copied()
                .max_by(|&a, &b| {
                    let ca = scoped[indices[a]].obs.confidence.unwrap_or(0.0);
                    let cb = scoped[indices[b]].obs.confidence.unwrap_or(0.0);
                    ca.partial_cmp(&cb)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| {
                            scoped[indices[a]]
                                .obs
                                .created_at
                                .cmp(&scoped[indices[b]].obs.created_at)
                        })
                })
                .unwrap();

            let content = scoped[indices[best_idx]].obs.content.clone();

            // Collect cluster's top-8 tokens for fingerprint
            let cluster_tokens: Vec<String> = centroid_sorted
                .iter()
                .take(8)
                .map(|(t, _)| t.clone())
                .collect();

            // Collect distinct symbol_fqns and file_paths
            let symbol_fqns: Vec<String> = valid_members
                .iter()
                .filter_map(|&m| scoped[indices[m]].obs.symbol_fqn.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let file_paths: Vec<String> = valid_members
                .iter()
                .filter_map(|&m| scoped[indices[m]].obs.file_path.clone())
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();

            // For fingerprint, use scope keys (derived paths) when explicit file_paths are empty.
            // This prevents symbol-only clusters from degenerating to "tokens only" fingerprints.
            let fingerprint_paths = if file_paths.is_empty() {
                valid_members
                    .iter()
                    .map(|&m| scoped[indices[m]].scope_key.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                file_paths.clone()
            };
            let scope_fingerprint = compute_scope_fingerprint(&fingerprint_paths, &cluster_tokens);

            let obs_ids: Vec<i64> = valid_members
                .iter()
                .map(|&m| scoped[indices[m]].obs.id)
                .collect();

            let last_seen_at = valid_members
                .iter()
                .map(|&m| scoped[indices[m]].obs.created_at)
                .max()
                .unwrap_or(0);

            candidates.push(RuleCandidate {
                content,
                scope_fingerprint,
                source_observation_ids: obs_ids,
                symbol_fqns,
                file_paths,
                support_count: valid_members.len(),
                session_count: session_ids.len(),
                last_seen_at,
                branch: cluster_branch.map(|s| s.to_string()),
            });
        }
    }

    Ok(candidates)
}

/// Write rule candidates to the database, deduplicating by scope_fingerprint.
/// Returns count of newly created rules.
fn write_rule_candidates(
    conn: &Connection,
    candidates: &[RuleCandidate],
) -> anyhow::Result<usize> {
    let now = now_secs();
    let mut new_count = 0;

    for candidate in candidates {
        // Check if rule with this fingerprint AND same branch already exists.
        // Branch-scoped lookup prevents cross-branch rule merging.
        // Uses optional() to properly surface MoreThanOneRow if duplicates exist.
        let existing: Option<(i64, i32)> = if candidate.branch.is_some() {
            conn.query_row(
                "SELECT id, is_active FROM project_rules WHERE scope_fingerprint = ?1 \
                 AND COALESCE(branch, '') = COALESCE(?2, '')",
                params![candidate.scope_fingerprint, candidate.branch],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
        } else {
            conn.query_row(
                "SELECT id, is_active FROM project_rules WHERE scope_fingerprint = ?1 \
                 AND branch IS NULL",
                params![candidate.scope_fingerprint],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
        };

        if let Some((rule_id, is_active)) = existing {
            // Insert new observation IDs (ignore duplicates)
            for obs_id in &candidate.source_observation_ids {
                conn.execute(
                    "INSERT OR IGNORE INTO rule_observations (rule_id, observation_id) VALUES (?1, ?2)",
                    params![rule_id, obs_id],
                )?;
            }

            // Merge new symbols and files into the rule's scope
            for fqn in &candidate.symbol_fqns {
                conn.execute(
                    "INSERT OR IGNORE INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, ?2)",
                    params![rule_id, fqn],
                )?;
            }
            for fp in &candidate.file_paths {
                conn.execute(
                    "INSERT OR IGNORE INTO rule_files (rule_id, file_path) VALUES (?1, ?2)",
                    params![rule_id, fp],
                )?;
            }

            // Recompute counts from ground truth via JOIN to handle any orphaned rows
            let support_count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM rule_observations ro \
                 JOIN observations o ON o.id = ro.observation_id \
                 WHERE ro.rule_id = ?1 \
                   AND o.consolidated_into IS NULL",
                params![rule_id],
                |r| r.get(0),
            )?;
            let session_count: i64 = conn.query_row(
                "SELECT COUNT(DISTINCT o.session_id) FROM rule_observations ro \
                 JOIN observations o ON o.id = ro.observation_id \
                 WHERE ro.rule_id = ?1 \
                   AND o.consolidated_into IS NULL",
                params![rule_id],
                |r| r.get(0),
            )?;

            // Determine new is_active
            let new_is_active = if is_active == -1 {
                // Stale rule — do NOT reactivate
                -1
            } else if is_active == 0 && session_count >= 4 {
                // Promote pending to active
                1
            } else {
                is_active
            };

            conn.execute(
                "UPDATE project_rules SET support_count = ?1, session_count = ?2, \
                 updated_at = ?3, last_seen_at = ?4, is_active = ?5 WHERE id = ?6",
                params![
                    support_count,
                    session_count,
                    now,
                    candidate.last_seen_at,
                    new_is_active,
                    rule_id
                ],
            )?;
        } else {
            // Insert new rule
            conn.execute(
                "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
                 last_seen_at, is_active, created_at, updated_at, branch) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8)",
                params![
                    candidate.content,
                    candidate.scope_fingerprint,
                    candidate.support_count as i64,
                    candidate.session_count as i64,
                    candidate.last_seen_at,
                    now,
                    now,
                    candidate.branch,
                ],
            )?;
            let rule_id = conn.last_insert_rowid();

            // Insert symbols
            for fqn in &candidate.symbol_fqns {
                conn.execute(
                    "INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, ?2)",
                    params![rule_id, fqn],
                )?;
            }

            // Insert files
            for fp in &candidate.file_paths {
                conn.execute(
                    "INSERT INTO rule_files (rule_id, file_path) VALUES (?1, ?2)",
                    params![rule_id, fp],
                )?;
            }

            // Insert observation provenance
            for obs_id in &candidate.source_observation_ids {
                conn.execute(
                    "INSERT INTO rule_observations (rule_id, observation_id) VALUES (?1, ?2)",
                    params![rule_id, obs_id],
                )?;
            }

            new_count += 1;
        }
    }

    Ok(new_count)
}

/// Query active rules relevant to the given symbols and file paths.
pub(crate) fn get_active_rules(
    conn: &Connection,
    symbol_fqns: &[String],
    file_paths: &[String],
    branch: Option<&str>,
    limit: usize,
) -> anyhow::Result<Vec<ProjectRule>> {
    if symbol_fqns.is_empty() && file_paths.is_empty() {
        return Ok(vec![]);
    }

    // Build dynamic SQL with bind params
    // branch=None means "no filter" (return rules from all branches),
    // branch=Some("main") means "main + NULL-branch rules"
    let mut param_idx = 1;
    let limit_param = param_idx;
    param_idx += 1;

    let (branch_clause, branch_param_idx) = if branch.is_some() {
        let bp = param_idx;
        param_idx += 1;
        (format!("AND (r.branch = ?{} OR r.branch IS NULL)", bp), Some(bp))
    } else {
        (String::new(), None)
    };

    let sym_placeholders: Vec<String> = symbol_fqns
        .iter()
        .map(|_| {
            let p = format!("?{}", param_idx);
            param_idx += 1;
            p
        })
        .collect();

    let file_placeholders: Vec<String> = file_paths
        .iter()
        .map(|_| {
            let p = format!("?{}", param_idx);
            param_idx += 1;
            p
        })
        .collect();

    let sym_subquery = if sym_placeholders.is_empty() {
        "SELECT rule_id, 0 as cnt FROM rule_symbols WHERE 0".to_string()
    } else {
        format!(
            "SELECT rule_id, COUNT(*) as cnt FROM rule_symbols \
             WHERE symbol_fqn IN ({}) GROUP BY rule_id",
            sym_placeholders.join(", ")
        )
    };

    let file_subquery = if file_placeholders.is_empty() {
        "SELECT rule_id, 0 as cnt FROM rule_files WHERE 0".to_string()
    } else {
        format!(
            "SELECT rule_id, COUNT(*) as cnt FROM rule_files \
             WHERE file_path IN ({}) GROUP BY rule_id",
            file_placeholders.join(", ")
        )
    };

    let sql = format!(
        "SELECT r.id, r.content, r.scope_fingerprint, r.support_count, r.session_count, \
         r.last_seen_at, r.is_active, r.stale_reason, r.created_at, r.updated_at, r.branch, \
         COALESCE(sym_match.cnt, 0) + COALESCE(file_match.cnt, 0) as relevance \
         FROM project_rules r \
         LEFT JOIN ({}) sym_match ON sym_match.rule_id = r.id \
         LEFT JOIN ({}) file_match ON file_match.rule_id = r.id \
         WHERE r.is_active = 1 \
           {} \
           AND (COALESCE(sym_match.cnt, 0) + COALESCE(file_match.cnt, 0)) > 0 \
         ORDER BY relevance DESC, r.support_count DESC, r.updated_at DESC \
         LIMIT ?{}",
        sym_subquery, file_subquery, branch_clause, limit_param
    );

    let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    // ?1 = limit
    all_params.push(Box::new(limit as i64));
    // ?2 = branch (only if Some)
    if let Some(b) = branch {
        assert!(branch_param_idx.is_some());
        all_params.push(Box::new(b.to_string()));
    }
    // symbol fqns
    for fqn in symbol_fqns {
        all_params.push(Box::new(fqn.clone()));
    }
    // file paths
    for fp in file_paths {
        all_params.push(Box::new(fp.clone()));
    }

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        all_params.iter().map(|p| p.as_ref()).collect();

    let mut stmt = conn.prepare(&sql)?;
    let mut rules: Vec<ProjectRule> = stmt
        .query_map(param_refs.as_slice(), |r| {
            Ok(ProjectRule {
                id: r.get(0)?,
                content: r.get(1)?,
                scope_fingerprint: r.get(2)?,
                support_count: r.get(3)?,
                session_count: r.get(4)?,
                last_seen_at: r.get(5)?,
                is_active: r.get(6)?,
                stale_reason: r.get(7)?,
                created_at: r.get(8)?,
                updated_at: r.get(9)?,
                branch: r.get(10)?,
                symbol_fqns: vec![],
                file_paths: vec![],
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Populate symbol_fqns and file_paths for each rule via batched queries
    let rule_ids: Vec<i64> = rules.iter().map(|r| r.id).collect();
    if !rule_ids.is_empty() {
        let placeholders = rule_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sym_sql = format!(
            "SELECT rule_id, symbol_fqn FROM rule_symbols WHERE rule_id IN ({placeholders}) ORDER BY symbol_fqn"
        );
        let mut sym_stmt = conn.prepare(&sym_sql)?;
        let mut sym_map: HashMap<i64, Vec<String>> = HashMap::new();
        let sym_rows: Vec<(i64, String)> = sym_stmt
            .query_map(rusqlite::params_from_iter(rule_ids.iter()), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (rule_id, fqn) in sym_rows {
            sym_map.entry(rule_id).or_default().push(fqn);
        }

        let file_placeholders = &placeholders;
        let file_sql = format!(
            "SELECT rule_id, file_path FROM rule_files WHERE rule_id IN ({file_placeholders}) ORDER BY file_path"
        );
        let mut file_stmt = conn.prepare(&file_sql)?;
        let mut file_map: HashMap<i64, Vec<String>> = HashMap::new();
        let file_rows: Vec<(i64, String)> = file_stmt
            .query_map(rusqlite::params_from_iter(rule_ids.iter()), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (rule_id, path) in file_rows {
            file_map.entry(rule_id).or_default().push(path);
        }

        for rule in &mut rules {
            rule.symbol_fqns = sym_map.remove(&rule.id).unwrap_or_default();
            rule.file_paths = file_map.remove(&rule.id).unwrap_or_default();
        }
    }

    Ok(rules)
}

/// Mark active/pending rules stale when their linked symbols change.
///
/// When `branch` is `Some`, only invalidates rules on that branch or NULL-branch.
/// When `branch` is `None`, invalidates matching rules on ALL branches (used when
/// the caller doesn't know the current branch — matches observation staleness behavior).
pub(crate) fn mark_rules_stale(
    conn: &Connection,
    symbol_fqns: &[String],
    reason: &str,
    branch: Option<&str>,
) -> anyhow::Result<usize> {
    if symbol_fqns.is_empty() {
        return Ok(0);
    }

    let mut param_idx = 3; // ?1=reason, ?2=updated_at
    let branch_clause = if branch.is_some() {
        let bp = param_idx;
        param_idx += 1;
        format!("AND (branch = ?{} OR branch IS NULL)", bp)
    } else {
        String::new()
    };

    let placeholders: String = (0..symbol_fqns.len())
        .map(|i| {
            let p = format!("?{}", param_idx + i);
            p
        })
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "UPDATE project_rules SET is_active = -1, stale_reason = ?1, updated_at = ?2 \
         WHERE id IN ( \
           SELECT DISTINCT rule_id FROM rule_symbols WHERE symbol_fqn IN ({}) \
         ) AND is_active != -1 {}",
        placeholders, branch_clause
    );

    let now = now_secs();
    let mut all_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    all_params.push(Box::new(reason.to_string()));
    all_params.push(Box::new(now));
    if let Some(b) = branch {
        all_params.push(Box::new(b.to_string()));
    }
    for fqn in symbol_fqns {
        all_params.push(Box::new(fqn.clone()));
    }

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        all_params.iter().map(|p| p.as_ref()).collect();

    let count = conn.execute(&sql, param_refs.as_slice())?;
    Ok(count)
}

/// Wrapper: detect patterns and write rules in a DEFERRED transaction.
/// Called from session-end handler, outside the IMMEDIATE pipeline tx.
pub fn detect_and_write_rules(
    conn: &mut Connection,
    branch: Option<&str>,
) -> anyhow::Result<usize> {
    let tx = conn.transaction()?; // DEFERRED by default
    let candidates = detect_rule_candidates(&tx, branch)?;
    if candidates.is_empty() {
        return Ok(0);
    }
    let count = write_rule_candidates(&tx, &candidates)?;
    tx.commit()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn open_test_db() -> rusqlite::Connection {
        let dir = tempdir().unwrap();
        crate::db::open(&dir.path().join("test.db")).unwrap()
    }

    fn insert_session(conn: &Connection, id: &str) {
        conn.execute(
            "INSERT INTO sessions (id, started_at) VALUES (?1, ?2)",
            params![id, 1000],
        )
        .unwrap();
    }

    fn insert_obs(
        conn: &Connection,
        session_id: &str,
        kind: &str,
        content: &str,
        file_path: Option<&str>,
        symbol_fqn: Option<&str>,
        branch: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, file_path, symbol_fqn, branch) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![session_id, now_secs(), kind, content, file_path, symbol_fqn, branch],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[allow(dead_code, clippy::too_many_arguments)]
    fn insert_obs_with_confidence(
        conn: &Connection,
        session_id: &str,
        kind: &str,
        content: &str,
        file_path: Option<&str>,
        symbol_fqn: Option<&str>,
        branch: Option<&str>,
        confidence: f64,
    ) -> i64 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, file_path, symbol_fqn, branch, confidence) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![session_id, now_secs(), kind, content, file_path, symbol_fqn, branch, confidence],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn test_compute_scope_fingerprint_deterministic() {
        let fp1 = compute_scope_fingerprint(
            &["b.rs".into(), "a.rs".into()],
            &["token2".into(), "token1".into()],
        );
        let fp2 = compute_scope_fingerprint(
            &["a.rs".into(), "b.rs".into()],
            &["token1".into(), "token2".into()],
        );
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_compute_scope_fingerprint_different_inputs() {
        let fp1 = compute_scope_fingerprint(&["a.rs".into()], &["token1".into()]);
        let fp2 = compute_scope_fingerprint(&["b.rs".into()], &["token1".into()]);
        assert_ne!(fp1, fp2);

        let fp3 = compute_scope_fingerprint(&["a.rs".into()], &["token2".into()]);
        assert_ne!(fp1, fp3);
    }

    #[test]
    fn test_detect_candidates_minimum_threshold() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        // 3 observations, 3 sessions, same file_path
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s2", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s3", "insight", content, Some("src/auth.rs"), None, None);

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 1);

        // Now test with only 2 observations
        let conn2 = open_test_db();
        insert_session(&conn2, "s1");
        insert_session(&conn2, "s2");
        insert_obs(&conn2, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn2, "s2", "insight", content, Some("src/auth.rs"), None, None);

        let candidates2 = detect_rule_candidates(&conn2, None).unwrap();
        assert_eq!(candidates2.len(), 0);
    }

    #[test]
    fn test_detect_candidates_requires_3_sessions() {
        let conn = open_test_db();
        insert_session(&conn, "s1");

        let content = "always check middleware chain before modifying authentication routes handler";
        // 3 observations but all in SAME session
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_detect_candidates_requires_3_sessions_boundary() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        // 3 observations across exactly 2 sessions → 0 candidates
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s2", "insight", content, Some("src/auth.rs"), None, None);

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 0);

        // Add 3rd session → 1 candidate
        insert_obs(&conn, "s3", "insight", content, Some("src/auth.rs"), None, None);
        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_detect_candidates_kind_filter() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        // kind = tool_call, not insight/decision
        insert_obs(&conn, "s1", "tool_call", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s2", "tool_call", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s3", "tool_call", content, Some("src/auth.rs"), None, None);

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_detect_candidates_excludes_stale() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, None);
        insert_obs(&conn, "s2", "insight", content, Some("src/auth.rs"), None, None);
        let id3 = insert_obs(&conn, "s3", "insight", content, Some("src/auth.rs"), None, None);

        // Mark one stale → only 2 remain → 0 candidates
        conn.execute(
            "UPDATE observations SET is_stale = 1, stale_reason = 'test' WHERE id = ?1",
            params![id3],
        )
        .unwrap();

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_detect_candidates_excludes_sensitive_paths() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        insert_obs(&conn, "s1", "insight", content, Some(".env"), None, None);
        insert_obs(&conn, "s2", "insight", content, Some(".env"), None, None);
        insert_obs(&conn, "s3", "insight", content, Some(".env"), None, None);

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_detect_candidates_discards_mixed_branch() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, Some("main"));
        insert_obs(&conn, "s2", "insight", content, Some("src/auth.rs"), None, Some("main"));
        insert_obs(&conn, "s3", "insight", content, Some("src/auth.rs"), None, Some("feature/x"));

        let candidates = detect_rule_candidates(&conn, None).unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_detect_candidates_same_branch_clusters() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");

        let content = "always check middleware chain before modifying authentication routes handler";
        insert_obs(&conn, "s1", "insight", content, Some("src/auth.rs"), None, Some("main"));
        insert_obs(&conn, "s2", "insight", content, Some("src/auth.rs"), None, Some("main"));
        insert_obs(&conn, "s3", "insight", content, Some("src/auth.rs"), None, Some("main"));

        let candidates = detect_rule_candidates(&conn, Some("main")).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].branch, Some("main".to_string()));
    }

    #[test]
    fn test_write_candidates_dedup_by_fingerprint() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");
        insert_session(&conn, "s4");

        // Create real observations for FK integrity
        let o1 = insert_obs(&conn, "s1", "insight", "x", Some("src/auth.rs"), None, None);
        let o2 = insert_obs(&conn, "s2", "insight", "x", Some("src/auth.rs"), None, None);
        let o3 = insert_obs(&conn, "s3", "insight", "x", Some("src/auth.rs"), None, None);
        let o4 = insert_obs(&conn, "s4", "insight", "y", Some("src/auth.rs"), None, None);
        let o5 = insert_obs(&conn, "s4", "insight", "z", Some("src/auth.rs"), None, None);

        let fp = compute_scope_fingerprint(&["src/auth.rs".into()], &["check".into(), "middleware".into()]);

        let c1 = RuleCandidate {
            content: "rule content 1".into(),
            scope_fingerprint: fp.clone(),
            source_observation_ids: vec![o1, o2, o3],
            symbol_fqns: vec![],
            file_paths: vec!["src/auth.rs".into()],
            support_count: 3,
            session_count: 3,
            last_seen_at: now_secs(),
            branch: None,
        };
        let count1 = write_rule_candidates(&conn, &[c1]).unwrap();
        assert_eq!(count1, 1);

        let c2 = RuleCandidate {
            content: "rule content 2".into(),
            scope_fingerprint: fp,
            source_observation_ids: vec![o4, o5],
            symbol_fqns: vec![],
            file_paths: vec!["src/auth.rs".into()],
            support_count: 2,
            session_count: 1,
            last_seen_at: now_secs(),
            branch: None,
        };
        let count2 = write_rule_candidates(&conn, &[c2]).unwrap();
        assert_eq!(count2, 0); // Not new — deduplicated

        // Verify only 1 rule in DB
        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM project_rules", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 1);
    }

    #[test]
    fn test_write_candidates_different_fingerprints() {
        let conn = open_test_db();
        // No observation IDs needed — empty vec means no FK inserts

        let c1 = RuleCandidate {
            content: "rule A".into(),
            scope_fingerprint: compute_scope_fingerprint(&["a.rs".into()], &["token_a".into()]),
            source_observation_ids: vec![],
            symbol_fqns: vec![],
            file_paths: vec!["a.rs".into()],
            support_count: 3,
            session_count: 3,
            last_seen_at: now_secs(),
            branch: None,
        };
        let c2 = RuleCandidate {
            content: "rule B".into(),
            scope_fingerprint: compute_scope_fingerprint(&["b.rs".into()], &["token_b".into()]),
            source_observation_ids: vec![],
            symbol_fqns: vec![],
            file_paths: vec!["b.rs".into()],
            support_count: 3,
            session_count: 3,
            last_seen_at: now_secs(),
            branch: None,
        };
        let count = write_rule_candidates(&conn, &[c1, c2]).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_rule_promotion_pending_to_active() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");
        insert_session(&conn, "s4");

        let fp = compute_scope_fingerprint(&["src/auth.rs".into()], &["check".into()]);

        // First write: 3 sessions → pending (is_active = 0)
        let obs1 = insert_obs(&conn, "s1", "insight", "x", Some("src/auth.rs"), None, None);
        let obs2 = insert_obs(&conn, "s2", "insight", "x", Some("src/auth.rs"), None, None);
        let obs3 = insert_obs(&conn, "s3", "insight", "x", Some("src/auth.rs"), None, None);

        let c1 = RuleCandidate {
            content: "rule content".into(),
            scope_fingerprint: fp.clone(),
            source_observation_ids: vec![obs1, obs2, obs3],
            symbol_fqns: vec![],
            file_paths: vec!["src/auth.rs".into()],
            support_count: 3,
            session_count: 3,
            last_seen_at: now_secs(),
            branch: None,
        };
        write_rule_candidates(&conn, &[c1]).unwrap();

        let is_active: i32 = conn
            .query_row("SELECT is_active FROM project_rules WHERE scope_fingerprint = ?1", params![fp], |r| r.get(0))
            .unwrap();
        assert_eq!(is_active, 0, "initial rule should be pending");

        // Second write: add obs from session s4 → session_count = 4 → promote
        let obs4 = insert_obs(&conn, "s4", "insight", "x", Some("src/auth.rs"), None, None);
        let c2 = RuleCandidate {
            content: "rule content".into(),
            scope_fingerprint: fp.clone(),
            source_observation_ids: vec![obs4],
            symbol_fqns: vec![],
            file_paths: vec!["src/auth.rs".into()],
            support_count: 1,
            session_count: 1,
            last_seen_at: now_secs(),
            branch: None,
        };
        write_rule_candidates(&conn, &[c2]).unwrap();

        let is_active: i32 = conn
            .query_row("SELECT is_active FROM project_rules WHERE scope_fingerprint = ?1", params![fp], |r| r.get(0))
            .unwrap();
        assert_eq!(is_active, 1, "rule should be promoted to active");
    }

    #[test]
    fn test_stale_rule_not_reactivated() {
        let conn = open_test_db();
        insert_session(&conn, "s1");
        insert_session(&conn, "s2");
        insert_session(&conn, "s3");
        insert_session(&conn, "s4");

        let fp = compute_scope_fingerprint(&["src/auth.rs".into()], &["check".into()]);

        let obs1 = insert_obs(&conn, "s1", "insight", "x", Some("src/auth.rs"), None, None);
        let obs2 = insert_obs(&conn, "s2", "insight", "x", Some("src/auth.rs"), None, None);
        let obs3 = insert_obs(&conn, "s3", "insight", "x", Some("src/auth.rs"), None, None);

        let c1 = RuleCandidate {
            content: "rule content".into(),
            scope_fingerprint: fp.clone(),
            source_observation_ids: vec![obs1, obs2, obs3],
            symbol_fqns: vec!["src/auth.rs::check".into()],
            file_paths: vec!["src/auth.rs".into()],
            support_count: 3,
            session_count: 3,
            last_seen_at: now_secs(),
            branch: None,
        };
        write_rule_candidates(&conn, &[c1]).unwrap();

        // Mark stale
        conn.execute(
            "UPDATE project_rules SET is_active = -1, stale_reason = 'test' WHERE scope_fingerprint = ?1",
            params![fp],
        )
        .unwrap();

        // Write overlapping candidate with 4th session
        let obs4 = insert_obs(&conn, "s4", "insight", "x", Some("src/auth.rs"), None, None);
        let c2 = RuleCandidate {
            content: "rule content".into(),
            scope_fingerprint: fp.clone(),
            source_observation_ids: vec![obs4],
            symbol_fqns: vec![],
            file_paths: vec!["src/auth.rs".into()],
            support_count: 1,
            session_count: 1,
            last_seen_at: now_secs(),
            branch: None,
        };
        write_rule_candidates(&conn, &[c2]).unwrap();

        let is_active: i32 = conn
            .query_row("SELECT is_active FROM project_rules WHERE scope_fingerprint = ?1", params![fp], |r| r.get(0))
            .unwrap();
        assert_eq!(is_active, -1, "stale rule must NOT be reactivated");
    }

    #[test]
    fn test_get_active_rules_relevance_ranking() {
        let conn = open_test_db();
        let now = now_secs();

        // Rule 1: linked to sym_a
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('rule A', 'fp_a', 3, 3, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r1 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::foo')", params![r1]).unwrap();

        // Rule 2: linked to sym_a AND sym_b (higher overlap)
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('rule B', 'fp_b', 3, 3, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r2 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::foo')", params![r2]).unwrap();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::bar')", params![r2]).unwrap();

        let rules = get_active_rules(
            &conn,
            &["src/a.rs::foo".into(), "src/a.rs::bar".into()],
            &[],
            None,
            10,
        )
        .unwrap();

        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].content, "rule B", "higher overlap should rank first");
        assert_eq!(rules[1].content, "rule A");
    }

    #[test]
    fn test_get_active_rules_branch_filter() {
        let conn = open_test_db();
        let now = now_secs();

        // Rule on main
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at, branch) VALUES ('main rule', 'fp_main', 3, 3, ?1, 1, ?1, ?1, 'main')",
            params![now],
        ).unwrap();
        let r1 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::foo')", params![r1]).unwrap();

        // Rule on feature/x
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at, branch) VALUES ('feature rule', 'fp_feat', 3, 3, ?1, 1, ?1, ?1, 'feature/x')",
            params![now],
        ).unwrap();
        let r2 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::foo')", params![r2]).unwrap();

        // Rule with NULL branch (global)
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('global rule', 'fp_global', 3, 3, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r3 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::foo')", params![r3]).unwrap();

        let rules = get_active_rules(&conn, &["src/a.rs::foo".into()], &[], Some("main"), 10).unwrap();
        let contents: Vec<&str> = rules.iter().map(|r| r.content.as_str()).collect();
        assert!(contents.contains(&"main rule"));
        assert!(contents.contains(&"global rule"));
        assert!(!contents.contains(&"feature rule"));
    }

    #[test]
    fn test_get_active_rules_requires_overlap() {
        let conn = open_test_db();
        let now = now_secs();

        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('unrelated rule', 'fp_x', 3, 3, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r1 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/other.rs::baz')", params![r1]).unwrap();

        // Query with symbols that don't match
        let rules = get_active_rules(&conn, &["src/a.rs::foo".into()], &[], None, 10).unwrap();
        assert!(rules.is_empty(), "rule with no overlap must not be returned");
    }

    #[test]
    fn test_mark_rules_stale() {
        let conn = open_test_db();
        let now = now_secs();

        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('active rule', 'fp_1', 3, 4, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r1 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/auth.rs::login')", params![r1]).unwrap();

        let count = mark_rules_stale(&conn, &["src/auth.rs::login".into()], "Symbol signature changed", None).unwrap();
        assert_eq!(count, 1);

        let (is_active, reason): (i32, Option<String>) = conn
            .query_row(
                "SELECT is_active, stale_reason FROM project_rules WHERE id = ?1",
                params![r1],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(is_active, -1);
        assert_eq!(reason.unwrap(), "Symbol signature changed");
    }

    #[test]
    fn test_mark_rules_stale_pending_also_invalidated() {
        let conn = open_test_db();
        let now = now_secs();

        // Pending rule (is_active = 0)
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('pending rule', 'fp_2', 3, 3, ?1, 0, ?1, ?1)",
            params![now],
        ).unwrap();
        let r1 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/x.rs::foo')", params![r1]).unwrap();

        let count = mark_rules_stale(&conn, &["src/x.rs::foo".into()], "Linked symbol removed", None).unwrap();
        assert_eq!(count, 1);

        let is_active: i32 = conn
            .query_row("SELECT is_active FROM project_rules WHERE id = ?1", params![r1], |r| r.get(0))
            .unwrap();
        assert_eq!(is_active, -1, "pending rule must also be marked inactive");
    }

    #[test]
    fn test_get_active_rules_batched_associations() {
        let conn = open_test_db();
        let now = now_secs();

        // Rule 1 with two symbol FQNs and one file path
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('rule one', 'fp_r1', 3, 3, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r1 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::alpha')", params![r1]).unwrap();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/a.rs::beta')", params![r1]).unwrap();
        conn.execute("INSERT INTO rule_files (rule_id, file_path) VALUES (?1, 'src/a.rs')", params![r1]).unwrap();

        // Rule 2 with one symbol FQN and two file paths
        conn.execute(
            "INSERT INTO project_rules (content, scope_fingerprint, support_count, session_count, \
             last_seen_at, is_active, created_at, updated_at) VALUES ('rule two', 'fp_r2', 3, 3, ?1, 1, ?1, ?1)",
            params![now],
        ).unwrap();
        let r2 = conn.last_insert_rowid();
        conn.execute("INSERT INTO rule_symbols (rule_id, symbol_fqn) VALUES (?1, 'src/b.rs::gamma')", params![r2]).unwrap();
        conn.execute("INSERT INTO rule_files (rule_id, file_path) VALUES (?1, 'src/b.rs')", params![r2]).unwrap();
        conn.execute("INSERT INTO rule_files (rule_id, file_path) VALUES (?1, 'src/c.rs')", params![r2]).unwrap();

        let rules = get_active_rules(
            &conn,
            &["src/a.rs::alpha".into(), "src/b.rs::gamma".into()],
            &["src/a.rs".into()],
            None,
            10,
        ).unwrap();

        assert_eq!(rules.len(), 2);

        let rule1 = rules.iter().find(|r| r.content == "rule one").expect("rule one must be present");
        assert_eq!(rule1.symbol_fqns, vec!["src/a.rs::alpha", "src/a.rs::beta"], "rule1 symbol_fqns must be in ORDER BY symbol_fqn order");
        assert_eq!(rule1.file_paths, vec!["src/a.rs"], "rule1 file_paths must match");

        let rule2 = rules.iter().find(|r| r.content == "rule two").expect("rule two must be present");
        assert_eq!(rule2.symbol_fqns, vec!["src/b.rs::gamma"], "rule2 symbol_fqns must match");
        assert_eq!(rule2.file_paths, vec!["src/b.rs", "src/c.rs"], "rule2 file_paths must be in ORDER BY file_path order");
    }
}
