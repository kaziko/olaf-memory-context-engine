use std::collections::HashMap;

/// Snapshot of a persisted symbol — loaded before update to enable diff.
pub(crate) struct SymbolSnapshot {
    pub fqn: String,
    pub kind: String,
    pub signature: Option<String>,
    pub source_hash: String,
}

/// Structural diff between old and new symbol sets for a single file.
///
/// `pub` (not `pub(crate)`) because `ReindexOutcome::Changed(StructuralDiff)` is
/// exported `pub` from the index module and Rust requires all types in a public
/// variant to be at least as public as the enum itself.
pub struct StructuralDiff {
    pub file_path: String,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    /// `(fqn, old_sig, new_sig)` — only when BOTH old and new sigs are present and differ.
    pub signature_changed: Vec<(String, String, String)>,
    /// Symbols whose source_hash changed but signature is unchanged or unknown.
    /// Body-only changes do NOT produce observations or stale flags.
    pub body_only: Vec<String>,
    /// `(old_fqn, new_fqn)` — symbols detected as renames via matching (signature, kind).
    pub renamed: Vec<(String, String)>,
}

impl StructuralDiff {
    pub fn has_structural_changes(&self) -> bool {
        !self.added.is_empty()
            || !self.removed.is_empty()
            || !self.signature_changed.is_empty()
            || !self.renamed.is_empty()
    }
}

/// Load symbol snapshots for a file from the DB before update.
pub(crate) fn load_file_symbols(
    tx: &rusqlite::Transaction,
    file_id: i64,
) -> Result<Vec<SymbolSnapshot>, rusqlite::Error> {
    let mut stmt =
        tx.prepare("SELECT fqn, kind, signature, source_hash FROM symbols WHERE file_id = ?1")?;
    let rows = stmt.query_map([file_id], |row| {
        Ok(SymbolSnapshot {
            fqn: row.get(0)?,
            kind: row.get(1)?,
            signature: row.get(2)?,
            source_hash: row.get(3)?,
        })
    })?;
    rows.collect()
}

/// Compute structural diff between old persisted symbols and newly parsed symbols.
pub(crate) fn compute(
    file_path: &str,
    old_syms: &[SymbolSnapshot],
    new_syms: &[crate::parser::Symbol],
) -> StructuralDiff {
    let old_map: HashMap<&str, &SymbolSnapshot> =
        old_syms.iter().map(|s| (s.fqn.as_str(), s)).collect();
    let new_map: HashMap<&str, &crate::parser::Symbol> =
        new_syms.iter().map(|s| (s.fqn.as_str(), s)).collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut signature_changed = Vec::new();
    let mut body_only = Vec::new();

    for new_sym in new_syms {
        let fqn = new_sym.fqn.as_str();
        if let Some(old) = old_map.get(fqn) {
            if old.source_hash != new_sym.source_hash {
                // Hash differs — check if signature changed
                match (&old.signature, &new_sym.signature) {
                    (Some(old_sig), Some(new_sig)) if old_sig != new_sig => {
                        signature_changed.push((fqn.to_string(), old_sig.clone(), new_sig.clone()));
                    }
                    // Either sig is None, or sigs are equal → body-only
                    _ => body_only.push(fqn.to_string()),
                }
            }
            // Hash matches → no change
        } else {
            added.push(fqn.to_string());
        }
    }

    for old_sym in old_syms {
        if !new_map.contains_key(old_sym.fqn.as_str()) {
            removed.push(old_sym.fqn.clone());
        }
    }

    // Rename detection: correlate removed+added by matching (signature, kind)
    let mut renamed = Vec::new();
    let renamed_old_set: std::collections::HashSet<String>;
    let renamed_new_set: std::collections::HashSet<String>;
    {
        // Build (signature, kind) -> [fqn] maps for removed and added,
        // filtering to symbols where signature is Some on both sides
        let mut removed_by_sig_kind: HashMap<(&str, &str), Vec<&str>> = HashMap::new();
        for fqn in &removed {
            if let Some(old) = old_map.get(fqn.as_str())
                && let Some(ref sig) = old.signature
            {
                removed_by_sig_kind
                    .entry((sig.as_str(), old.kind.as_str()))
                    .or_default()
                    .push(fqn.as_str());
            }
        }

        let mut added_by_sig_kind: HashMap<(&str, &str), Vec<&str>> = HashMap::new();
        for fqn in &added {
            if let Some(new_sym) = new_map.get(fqn.as_str())
                && let Some(ref sig) = new_sym.signature
            {
                added_by_sig_kind
                    .entry((sig.as_str(), new_sym.kind.as_str()))
                    .or_default()
                    .push(fqn.as_str());
            }
        }

        let mut old_set = std::collections::HashSet::new();
        let mut new_set = std::collections::HashSet::new();

        for (key, rem_fqns) in &removed_by_sig_kind {
            if rem_fqns.len() == 1
                && let Some(add_fqns) = added_by_sig_kind.get(key)
                && add_fqns.len() == 1
            {
                let old_fqn = rem_fqns[0].to_string();
                let new_fqn = add_fqns[0].to_string();
                renamed.push((old_fqn.clone(), new_fqn.clone()));
                old_set.insert(old_fqn);
                new_set.insert(new_fqn);
            }
        }

        renamed_old_set = old_set;
        renamed_new_set = new_set;
    }

    removed.retain(|fqn| !renamed_old_set.contains(fqn));
    added.retain(|fqn| !renamed_new_set.contains(fqn));
    renamed.sort();

    StructuralDiff { file_path: file_path.to_string(), added, removed, signature_changed, body_only, renamed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Symbol, SymbolKind};

    fn make_snap(fqn: &str, kind: &str, sig: Option<&str>, hash: &str) -> SymbolSnapshot {
        SymbolSnapshot {
            fqn: fqn.to_string(),
            kind: kind.to_string(),
            signature: sig.map(|s| s.to_string()),
            source_hash: hash.to_string(),
        }
    }

    fn make_sym(fqn: &str, sig: Option<&str>, hash: &str) -> Symbol {
        Symbol {
            fqn: fqn.to_string(),
            name: fqn.rsplit("::").next().unwrap_or(fqn).to_string(),
            kind: SymbolKind::Function,
            start_line: 1,
            end_line: 10,
            signature: sig.map(|s| s.to_string()),
            docstring: None,
            source_hash: hash.to_string(),
            parent_fqn: None,
        }
    }

    #[test]
    fn added_symbol() {
        let old = vec![];
        let new = vec![make_sym("f.rs::foo", Some("fn foo()"), "h1")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.added.len(), 1);
        assert!(diff.removed.is_empty());
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn removed_symbol() {
        let old = vec![make_snap("f.rs::foo", "function", Some("fn foo()"), "h1")];
        let new = vec![];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.removed.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn signature_changed() {
        let old = vec![make_snap("f.rs::foo", "function", Some("fn foo()"), "h1")];
        let new = vec![make_sym("f.rs::foo", Some("fn foo(x: i32)"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.signature_changed.len(), 1);
        let (fqn, old_sig, new_sig) = &diff.signature_changed[0];
        assert_eq!(fqn, "f.rs::foo");
        assert_eq!(old_sig, "fn foo()");
        assert_eq!(new_sig, "fn foo(x: i32)");
    }

    #[test]
    fn body_only_same_sig() {
        let old = vec![make_snap("f.rs::foo", "function", Some("fn foo()"), "h1")];
        let new = vec![make_sym("f.rs::foo", Some("fn foo()"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.body_only.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn null_old_sig_is_conservative_body_only() {
        // Conservative design: old sig=None with hash change → body-only to avoid noise
        let old = vec![make_snap("f.rs::foo", "function", None, "h1")];
        let new = vec![make_sym("f.rs::foo", Some("fn foo()"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.body_only.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn has_structural_changes_body_only_only_is_false() {
        let old = vec![make_snap("f.rs::foo", "function", Some("fn foo()"), "h1")];
        let new = vec![make_sym("f.rs::foo", Some("fn foo()"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert!(!diff.has_structural_changes());
    }

    #[test]
    fn has_structural_changes_added_is_true() {
        let old = vec![];
        let new = vec![make_sym("f.rs::foo", None, "h1")];
        let diff = compute("f.rs", &old, &new);
        assert!(diff.has_structural_changes());
    }

    #[test]
    fn expression_bodied_both_sigs_none_hash_differs_is_body_only() {
        // Arrow fn or type alias: extract_signature returns None → body edit is body_only
        let old = vec![make_snap("f.ts::foo", "function", None, "h1")];
        let new = vec![make_sym("f.ts::foo", None, "h2")];
        let diff = compute("f.ts", &old, &new);
        assert_eq!(diff.body_only.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    // --- Task 5 rename detection tests ---

    // 5.4: rename detection — unique match
    #[test]
    fn rename_detected_unique_sig_kind() {
        let old = vec![make_snap("f.rs::old_name", "function", Some("fn()"), "h1")];
        let new = vec![make_sym("f.rs::new_name", Some("fn()"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.renamed.len(), 1);
        assert_eq!(diff.renamed[0], ("f.rs::old_name".into(), "f.rs::new_name".into()));
        assert!(diff.removed.is_empty());
        assert!(diff.added.is_empty());
    }

    // 5.5: rename ambiguity — 2 removed + 2 added same sig+kind
    #[test]
    fn rename_ambiguous_stays_in_removed_added() {
        let old = vec![
            make_snap("f.rs::a", "function", Some("fn()"), "h1"),
            make_snap("f.rs::b", "function", Some("fn()"), "h2"),
        ];
        let new = vec![
            make_sym("f.rs::c", Some("fn()"), "h3"),
            make_sym("f.rs::d", Some("fn()"), "h4"),
        ];
        let diff = compute("f.rs", &old, &new);
        assert!(diff.renamed.is_empty());
        assert_eq!(diff.removed.len(), 2);
        assert_eq!(diff.added.len(), 2);
    }

    // 5.6: kind mismatch — not renamed
    #[test]
    fn rename_kind_mismatch_not_renamed() {
        let old = vec![make_snap("f.rs::foo", "function", Some("fn()"), "h1")];
        let new_syms = vec![Symbol {
            fqn: "f.rs::bar".into(),
            name: "bar".into(),
            kind: SymbolKind::Class,
            start_line: 1,
            end_line: 10,
            signature: Some("fn()".into()),
            docstring: None,
            source_hash: "h2".into(),
            parent_fqn: None,
        }];
        let diff = compute("f.rs", &old, &new_syms);
        assert!(diff.renamed.is_empty());
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.added.len(), 1);
    }

    // 5.7: None signature — not renamed
    #[test]
    fn rename_none_sig_not_renamed() {
        let old = vec![make_snap("f.rs::foo", "function", None, "h1")];
        let new = vec![make_sym("f.rs::bar", None, "h2")];
        let diff = compute("f.rs", &old, &new);
        assert!(diff.renamed.is_empty());
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.added.len(), 1);
    }

    // Story 15.1: enum variant payload change is structural
    #[test]
    fn enum_variant_payload_change_is_structural() {
        let old = vec![make_snap("f.rs::Error::Db", "enum_variant", Some("Db(DbError)"), "h1")];
        let new = vec![make_sym("f.rs::Error::Db", Some("Db(String)"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.signature_changed.len(), 1, "variant payload change must be signature_changed");
        assert!(diff.body_only.is_empty(), "must NOT be body_only");
    }

    // Story 15.1: struct field type change is structural
    #[test]
    fn struct_field_type_change_is_structural() {
        let old = vec![make_snap("f.rs::Config::name", "field", Some("name: String"), "h1")];
        let new = vec![make_sym("f.rs::Config::name", Some("name: &str"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.signature_changed.len(), 1, "field type change must be signature_changed");
        assert!(diff.body_only.is_empty(), "must NOT be body_only");
    }
}
