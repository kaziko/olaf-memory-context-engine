use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct WorkspaceWarning {
    pub message: String,
}

impl std::fmt::Display for WorkspaceWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Debug)]
pub struct WorkspaceConfig {
    pub members: Vec<WorkspaceMember>,
    pub warnings: Vec<WorkspaceWarning>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceMember {
    pub path: PathBuf,
    pub label: String,
    pub role: Option<String>,
}

// --- TOML deserialization types ---

#[derive(Deserialize)]
struct TomlFile {
    workspace: TomlWorkspace,
}

#[derive(Deserialize)]
struct TomlWorkspace {
    members: Vec<TomlMember>,
}

#[derive(Deserialize)]
struct TomlMember {
    path: String,
    label: String,
    role: Option<String>,
}

/// Parse `.olaf/workspace.toml` relative to `project_root`.
///
/// Returns `(None, [])` when file absent.
/// Returns `(None, [warning])` when file is malformed.
/// Returns `(Some(config), warnings)` on success, with warnings for missing/duplicate paths.
pub fn parse_workspace_config(project_root: &Path) -> (Option<WorkspaceConfig>, Vec<WorkspaceWarning>) {
    let toml_path = project_root.join(".olaf").join("workspace.toml");

    if !toml_path.exists() {
        return (None, vec![]);
    }

    let content = match std::fs::read_to_string(&toml_path) {
        Ok(c) => c,
        Err(e) => {
            return (
                None,
                vec![WorkspaceWarning {
                    message: format!("Failed to read workspace.toml: {e}"),
                }],
            );
        }
    };

    let parsed: TomlFile = match toml::from_str(&content) {
        Ok(p) => p,
        Err(e) => {
            return (
                None,
                vec![WorkspaceWarning {
                    message: format!("Malformed workspace.toml: {e}"),
                }],
            );
        }
    };

    let toml_dir = toml_path.parent().unwrap().parent().unwrap_or(project_root);
    let mut warnings = Vec::new();
    let mut members = Vec::new();
    let mut seen_paths: Vec<PathBuf> = Vec::new();

    for entry in parsed.workspace.members {
        let raw_path = toml_dir.join(&entry.path);
        let resolved = resolve_member_path(&raw_path);

        if seen_paths.iter().any(|p| p == &resolved) {
            warnings.push(WorkspaceWarning {
                message: format!(
                    "Duplicate member '{}': path resolves to {} (already listed)",
                    entry.label,
                    resolved.display()
                ),
            });
            continue;
        }

        if !resolved.exists() {
            warnings.push(WorkspaceWarning {
                message: format!(
                    "Skipped member '{}': path does not exist ({})",
                    entry.label,
                    resolved.display()
                ),
            });
        }

        seen_paths.push(resolved.clone());
        members.push(WorkspaceMember {
            path: resolved,
            label: entry.label,
            role: entry.role,
        });
    }

    (
        Some(WorkspaceConfig { members, warnings: warnings.clone() }),
        warnings,
    )
}

/// Resolve a member path to a canonical absolute path.
/// For existing paths: `canonicalize()`.
/// For missing paths: canonicalize the longest existing ancestor, append remaining segments.
fn resolve_member_path(raw_path: &Path) -> PathBuf {
    if let Ok(canonical) = raw_path.canonicalize() {
        return canonical;
    }
    normalize_missing_path(raw_path)
}

/// For missing paths, canonicalize the longest existing ancestor and append remaining segments.
fn normalize_missing_path(path: &Path) -> PathBuf {
    let components: Vec<_> = path.components().collect();

    for i in (0..components.len()).rev() {
        let ancestor: PathBuf = components[..=i].iter().collect();
        if let Ok(canonical) = ancestor.canonicalize() {
            let remaining: PathBuf = components[i + 1..].iter().collect();
            return canonical.join(remaining);
        }
    }

    // Fallback: return as-is (shouldn't happen for absolute paths)
    path.to_path_buf()
}

/// Serialize a workspace config to TOML string.
pub fn serialize_workspace_config(config: &WorkspaceConfig, base_dir: &Path) -> String {
    let mut toml = String::from("[workspace]\nmembers = [\n");
    for m in &config.members {
        let rel = pathdiff_or_absolute(&m.path, base_dir);
        if let Some(ref role) = m.role {
            toml.push_str(&format!(
                "  {{ path = \"{}\", label = \"{}\", role = \"{}\" }},\n",
                rel, m.label, role
            ));
        } else {
            toml.push_str(&format!(
                "  {{ path = \"{}\", label = \"{}\" }},\n",
                rel, m.label
            ));
        }
    }
    toml.push_str("]\n");
    toml
}

// --- Workspace state model ---

pub struct LocalRepo {
    pub conn: Connection,
    pub project_root: PathBuf,
}

pub struct RemoteMember {
    pub label: String,
    pub project_root: PathBuf,
    pub conn: Connection,
}

pub struct MemberRef<'a> {
    pub index: usize,
    pub label: &'a str,
    pub project_root: &'a Path,
    pub conn: &'a Connection,
}

pub struct Workspace {
    local: LocalRepo,
    members: Vec<RemoteMember>,
    pub warnings: Vec<WorkspaceWarning>,
}

impl Workspace {
    /// Construct workspace from config. Opens remote member DBs read-only,
    /// skips failures into warnings.
    pub fn load(
        local_conn: Connection,
        local_root: PathBuf,
        config: &WorkspaceConfig,
    ) -> Self {
        let mut members = Vec::new();
        let mut warnings = config.warnings.clone();
        let local_canonical = local_root.canonicalize().unwrap_or_else(|_| local_root.clone());

        for m in &config.members {
            // Skip the local repo if it appears in workspace members
            if m.path == local_canonical {
                continue;
            }

            if !m.path.exists() {
                // Already warned during parse, skip
                continue;
            }

            let db_path = m.path.join(".olaf").join("index.db");
            match crate::db::open_readonly(&db_path) {
                Ok(conn) => {
                    members.push(RemoteMember {
                        label: m.label.clone(),
                        project_root: m.path.clone(),
                        conn,
                    });
                }
                Err(e) => {
                    warnings.push(WorkspaceWarning {
                        message: format!(
                            "Skipped member '{}': database error ({}): {e}",
                            m.label,
                            db_path.display()
                        ),
                    });
                }
            }
        }

        Workspace {
            local: LocalRepo {
                conn: local_conn,
                project_root: local_root,
            },
            members,
            warnings,
        }
    }

    /// Fallback constructor when no workspace.toml exists or parsing fails.
    pub fn single(conn: Connection, root: PathBuf, warnings: Vec<WorkspaceWarning>) -> Self {
        Workspace {
            local: LocalRepo {
                conn,
                project_root: root,
            },
            members: vec![],
            warnings,
        }
    }

    /// Mutable access to local DB for writes.
    pub fn local_conn(&mut self) -> &mut Connection {
        &mut self.local.conn
    }

    /// Immutable access to local DB.
    pub fn local_conn_ref(&self) -> &Connection {
        &self.local.conn
    }

    /// Mutable conn + immutable root — avoids double borrow.
    pub fn local_parts(&mut self) -> (&mut Connection, &Path) {
        (&mut self.local.conn, &self.local.project_root)
    }

    /// Local project root.
    pub fn local_root(&self) -> &Path {
        &self.local.project_root
    }

    /// All connections for read queries. Local is index 0, remotes follow.
    pub fn all_read_conns(&self) -> Vec<MemberRef<'_>> {
        let mut refs = vec![MemberRef {
            index: 0,
            label: "local",
            project_root: &self.local.project_root,
            conn: &self.local.conn,
        }];

        for (i, m) in self.members.iter().enumerate() {
            refs.push(MemberRef {
                index: i + 1,
                label: &m.label,
                project_root: &m.project_root,
                conn: &m.conn,
            });
        }

        refs
    }

    /// Whether this workspace has remote members.
    pub fn has_remotes(&self) -> bool {
        !self.members.is_empty()
    }

    /// Format workspace warnings as a markdown section, including runtime freshness checks.
    /// Freshness is evaluated at call time (not startup) so long-lived servers get accurate data.
    pub fn format_warnings_with_freshness(&self) -> String {
        let mut all_warnings: Vec<String> = self.warnings.iter().map(|w| w.message.clone()).collect();

        // Check remote freshness at call time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let one_hour = 3600;

        for m in &self.members {
            let last_indexed: Option<i64> = m.conn
                .query_row("SELECT MAX(last_indexed_at) FROM files", [], |r| r.get(0))
                .ok()
                .flatten();

            if let Some(ts) = last_indexed {
                let age = now - ts;
                if age > one_hour {
                    let hours = age / 3600;
                    all_warnings.push(format!(
                        "Remote member '{}' last indexed {} hour{} ago (advisory)",
                        m.label, hours, if hours == 1 { "" } else { "s" }
                    ));
                }
            }
        }

        if all_warnings.is_empty() {
            return String::new();
        }
        let mut out = String::from("\n## Workspace Warnings\n");
        for msg in &all_warnings {
            out.push_str(&format!("- {}\n", msg));
        }
        out
    }

    /// Format workspace warnings as a markdown section.
    pub fn format_warnings(&self) -> String {
        if self.warnings.is_empty() {
            return String::new();
        }
        let mut out = String::from("\n## Workspace Warnings\n");
        for w in &self.warnings {
            out.push_str(&format!("- {}\n", w.message));
        }
        out
    }
}

fn pathdiff_or_absolute(path: &Path, base: &Path) -> String {
    if let Some(rel) = pathdiff(path, base) {
        rel.to_string_lossy().to_string()
    } else {
        path.to_string_lossy().to_string()
    }
}

/// Resolve a member path (public wrapper for CLI use).
pub fn resolve_path_public(path: &Path) -> PathBuf {
    resolve_member_path(path)
}

/// Compute a relative path from `path` to `base`, returning absolute path as fallback.
pub fn pathdiff_public(path: &Path, base: &Path) -> String {
    pathdiff_or_absolute(path, base)
}

fn pathdiff(path: &Path, base: &Path) -> Option<PathBuf> {
    let path = path.components().collect::<Vec<_>>();
    let base = base.components().collect::<Vec<_>>();

    let common = path.iter().zip(base.iter()).take_while(|(a, b)| a == b).count();

    if common == 0 {
        return None;
    }

    let mut result = PathBuf::new();
    for _ in common..base.len() {
        result.push("..");
    }
    for c in &path[common..] {
        result.push(c);
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_absent_workspace_toml_returns_none() {
        let dir = tempdir().unwrap();
        let (config, warnings) = parse_workspace_config(dir.path());
        assert!(config.is_none());
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_valid_workspace_toml() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();

        // Create a member directory
        let member_dir = dir.path().join("backend");
        std::fs::create_dir_all(&member_dir).unwrap();

        let toml_content = r#"
[workspace]
members = [
  { path = "backend", label = "backend", role = "api" },
]
"#;
        std::fs::write(olaf_dir.join("workspace.toml"), toml_content).unwrap();

        let (config, warnings) = parse_workspace_config(dir.path());
        let config = config.expect("should parse successfully");
        assert_eq!(config.members.len(), 1);
        assert_eq!(config.members[0].label, "backend");
        assert_eq!(config.members[0].role.as_deref(), Some("api"));
        assert!(config.members[0].path.is_absolute());
        // No warnings for valid, existing path
        let missing_warnings: Vec<_> = warnings.iter().filter(|w| w.message.contains("does not exist")).collect();
        assert!(missing_warnings.is_empty());
    }

    #[test]
    fn test_missing_member_produces_warning() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();

        let toml_content = r#"
[workspace]
members = [
  { path = "nonexistent-repo", label = "ghost" },
]
"#;
        std::fs::write(olaf_dir.join("workspace.toml"), toml_content).unwrap();

        let (config, warnings) = parse_workspace_config(dir.path());
        let config = config.expect("should parse even with missing members");
        assert_eq!(config.members.len(), 1);
        assert!(warnings.iter().any(|w| w.message.contains("does not exist")));
    }

    #[test]
    fn test_malformed_toml_returns_none_with_warning() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();

        std::fs::write(olaf_dir.join("workspace.toml"), "this is not valid toml {{{{").unwrap();

        let (config, warnings) = parse_workspace_config(dir.path());
        assert!(config.is_none());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("Malformed"));
    }

    #[test]
    fn test_duplicate_paths_via_symlinks() {
        let dir = tempdir().unwrap();
        let olaf_dir = dir.path().join(".olaf");
        std::fs::create_dir_all(&olaf_dir).unwrap();

        // Create real directory
        let real_dir = dir.path().join("real-repo");
        std::fs::create_dir_all(&real_dir).unwrap();

        // Create symlink to the same directory
        let link_path = dir.path().join("link-repo");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real_dir, &link_path).unwrap();

        #[cfg(not(unix))]
        {
            // Skip on non-unix platforms
            return;
        }

        let toml_content = r#"
[workspace]
members = [
  { path = "real-repo", label = "original" },
  { path = "link-repo", label = "duplicate" },
]
"#;
        std::fs::write(olaf_dir.join("workspace.toml"), toml_content).unwrap();

        let (config, warnings) = parse_workspace_config(dir.path());
        let config = config.expect("should parse");
        // Only one member should remain after dedup
        assert_eq!(config.members.len(), 1);
        assert_eq!(config.members[0].label, "original");
        assert!(warnings.iter().any(|w| w.message.contains("Duplicate")));
    }

    #[test]
    fn test_workspace_single_mode() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let conn = crate::db::open(&db_path).unwrap();

        let ws = Workspace::single(conn, dir.path().to_path_buf(), vec![]);
        assert!(!ws.has_remotes());
        let refs = ws.all_read_conns();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].index, 0);
        assert_eq!(refs[0].label, "local");
    }

    #[test]
    fn test_workspace_single_with_warnings() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let conn = crate::db::open(&db_path).unwrap();

        let warnings = vec![WorkspaceWarning {
            message: "Malformed workspace.toml: test".to_string(),
        }];
        let ws = Workspace::single(conn, dir.path().to_path_buf(), warnings);
        assert!(!ws.has_remotes());
        assert_eq!(ws.warnings.len(), 1);
        let fmt = ws.format_warnings();
        assert!(fmt.contains("## Workspace Warnings"));
        assert!(fmt.contains("Malformed"));
    }

    #[test]
    fn test_workspace_load_skips_missing_member() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let conn = crate::db::open(&db_path).unwrap();

        let config = WorkspaceConfig {
            members: vec![WorkspaceMember {
                path: dir.path().join("nonexistent"),
                label: "ghost".to_string(),
                role: None,
            }],
            warnings: vec![],
        };

        let ws = Workspace::load(conn, dir.path().to_path_buf(), &config);
        assert!(!ws.has_remotes());
        // The member path doesn't exist, so it's skipped
    }

    #[test]
    fn test_workspace_load_with_valid_remote() {
        let dir = tempdir().unwrap();

        // Create local DB
        let local_db = dir.path().join("local").join("index.db");
        std::fs::create_dir_all(local_db.parent().unwrap()).unwrap();
        let local_conn = crate::db::open(&local_db).unwrap();

        // Create remote repo with its own DB
        let remote_root = dir.path().join("remote");
        let remote_db = remote_root.join(".olaf").join("index.db");
        std::fs::create_dir_all(remote_db.parent().unwrap()).unwrap();
        let _remote_conn = crate::db::open(&remote_db).unwrap();
        drop(_remote_conn);

        let config = WorkspaceConfig {
            members: vec![WorkspaceMember {
                path: remote_root.canonicalize().unwrap(),
                label: "remote".to_string(),
                role: Some("lib".to_string()),
            }],
            warnings: vec![],
        };

        let ws = Workspace::load(local_conn, dir.path().join("local"), &config);
        assert!(ws.has_remotes());
        let refs = ws.all_read_conns();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].index, 0);
        assert_eq!(refs[0].label, "local");
        assert_eq!(refs[1].index, 1);
        assert_eq!(refs[1].label, "remote");
    }

    #[test]
    fn test_normalize_missing_path() {
        let dir = tempdir().unwrap();
        // Create a partial ancestor
        let existing = dir.path().join("exists");
        std::fs::create_dir_all(&existing).unwrap();

        let missing = existing.join("does-not-exist").join("deep");
        let result = normalize_missing_path(&missing);

        // Should contain the canonicalized ancestor + remaining segments
        assert!(result.is_absolute());
        assert!(result.ends_with("does-not-exist/deep") || result.ends_with("does-not-exist\\deep"));
    }
}
