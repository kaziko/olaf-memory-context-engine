use std::collections::HashMap;

/// Snapshot of a persisted symbol — loaded before update to enable diff.
pub(crate) struct SymbolSnapshot {
    pub fqn: String,
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
}

impl StructuralDiff {
    pub fn has_structural_changes(&self) -> bool {
        !self.added.is_empty() || !self.removed.is_empty() || !self.signature_changed.is_empty()
    }
}

/// Load symbol snapshots for a file from the DB before update.
pub(crate) fn load_file_symbols(
    tx: &rusqlite::Transaction,
    file_id: i64,
) -> Result<Vec<SymbolSnapshot>, rusqlite::Error> {
    let mut stmt =
        tx.prepare("SELECT fqn, signature, source_hash FROM symbols WHERE file_id = ?1")?;
    let rows = stmt.query_map([file_id], |row| {
        Ok(SymbolSnapshot {
            fqn: row.get(0)?,
            signature: row.get(1)?,
            source_hash: row.get(2)?,
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

    StructuralDiff { file_path: file_path.to_string(), added, removed, signature_changed, body_only }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Symbol, SymbolKind};

    fn make_snap(fqn: &str, sig: Option<&str>, hash: &str) -> SymbolSnapshot {
        SymbolSnapshot {
            fqn: fqn.to_string(),
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
        let old = vec![make_snap("f.rs::foo", Some("fn foo()"), "h1")];
        let new = vec![];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.removed.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn signature_changed() {
        let old = vec![make_snap("f.rs::foo", Some("fn foo()"), "h1")];
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
        let old = vec![make_snap("f.rs::foo", Some("fn foo()"), "h1")];
        let new = vec![make_sym("f.rs::foo", Some("fn foo()"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.body_only.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn null_old_sig_is_conservative_body_only() {
        // Conservative design: old sig=None with hash change → body-only to avoid noise
        let old = vec![make_snap("f.rs::foo", None, "h1")];
        let new = vec![make_sym("f.rs::foo", Some("fn foo()"), "h2")];
        let diff = compute("f.rs", &old, &new);
        assert_eq!(diff.body_only.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }

    #[test]
    fn has_structural_changes_body_only_only_is_false() {
        let old = vec![make_snap("f.rs::foo", Some("fn foo()"), "h1")];
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
        let old = vec![make_snap("f.ts::foo", None, "h1")];
        let new = vec![make_sym("f.ts::foo", None, "h2")];
        let diff = compute("f.ts", &old, &new);
        assert_eq!(diff.body_only.len(), 1);
        assert!(diff.signature_changed.is_empty());
    }
}
