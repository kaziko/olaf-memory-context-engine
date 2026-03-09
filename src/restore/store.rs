// Restore store — filesystem snapshot operations for Story 4.3+

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Per-process monotonic counter — makes snapshot filenames unique even when
/// two calls land within the same millisecond in the same process.
static SNAP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("path escapes project root: {0}")]
    PathOutsideRoot(String),
    #[error("snapshot not found: {0} — available: {1}")]
    SnapshotNotFound(String, String),
}

/// A restore point entry returned by `list_restore_points`.
#[derive(Debug)]
pub struct RestorePoint {
    pub id: String,    // full filename stem, e.g. "1740000000000-12345-7"
    pub millis: u128,  // parsed from id (first component before '-')
    pub size: u64,     // .snap file size in bytes
}

/// Canonicalize a path even when the target doesn't exist.
///
/// Strategy:
/// 1. Try a direct `canonicalize()` (works when the file exists).
/// 2. Manually resolve `..`/`.` with a component stack so the path has no special
///    components — this is essential because `Path::file_name()` returns `None` for
///    paths ending in `..`, which would silently drop escape components when walking up.
/// 3. Canonicalize the deepest existing prefix, then re-append the non-existent tail.
///
/// This resolves OS-level symlinks (e.g. macOS `/var` → `/private/var`) regardless of
/// whether the leaf file is present or whether the path contains `..` traversals.
fn canonicalize_best_effort(path: &std::path::Path) -> std::path::PathBuf {
    if let Ok(c) = path.canonicalize() {
        return c;
    }
    // Step 2: manually resolve '..' and '.' so the path is free of special components.
    // Without this, 'file_name()' on a path ending in '..' returns None, causing '..
    // components to be silently dropped when we walk up looking for an existing ancestor.
    let mut parts: Vec<std::ffi::OsString> = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => { parts.pop(); }
            std::path::Component::CurDir => {}
            c => parts.push(c.as_os_str().to_os_string()),
        }
    }
    let resolved: std::path::PathBuf = parts.iter().collect();

    if let Ok(c) = resolved.canonicalize() {
        return c;
    }
    // Step 3: walk up the resolved (no-'..' / no-'.') path to the nearest existing
    // ancestor, canonicalize it, and re-append the non-existent tail.
    let mut ancestor = resolved.as_path();
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match ancestor.parent() {
            Some(parent) => {
                if let Some(name) = ancestor.file_name() {
                    tail.push(name.to_os_string());
                }
                ancestor = parent;
                if let Ok(c) = ancestor.canonicalize() {
                    let mut result = c;
                    for component in tail.iter().rev() {
                        result = result.join(component);
                    }
                    return result;
                }
            }
            None => return resolved, // filesystem root — return as-is
        }
    }
}

/// Convert any input path (absolute or relative) to a normalized project-relative string.
/// Rejects paths that escape the project root via ParentDir (..) components.
/// Strips CurDir (.) components for stable hash keys.
pub fn normalize_rel_path(
    project_root: &std::path::Path,
    input: &str,
) -> Result<String, RestoreError> {
    // P2 fix: canonicalize the comparison root so lexical strip_prefix works even when the
    // project_root contains symlinks (e.g. /var → /private/var on macOS).
    let canonical_root = project_root.canonicalize()
        .unwrap_or_else(|_| project_root.to_path_buf());

    let abs = if std::path::Path::new(input).is_absolute() {
        // P2 fix: resolve symlinks in absolute paths so strip_prefix matches canonical_root.
        // When the target doesn't exist yet (e.g. list for a deleted file), canonicalize()
        // fails. Walk up to the nearest existing ancestor, canonicalize it, then re-append
        // the non-existent tail — this handles macOS /var → /private/var for any depth.
        canonicalize_best_effort(std::path::Path::new(input))
    } else {
        // P2 fix: join onto canonical_root (not raw project_root) so strip_prefix below
        // sees matching prefixes even on macOS where /var is a symlink to /private/var.
        canonical_root.join(input)
    };

    let raw_rel = abs.strip_prefix(&canonical_root)
        .map_err(|_| RestoreError::PathOutsideRoot(input.to_string()))?;
    let mut normalized = std::path::PathBuf::new();
    for component in raw_rel.components() {
        match component {
            std::path::Component::ParentDir => {
                return Err(RestoreError::PathOutsideRoot(input.to_string()));
            }
            std::path::Component::CurDir => {} // normalize src/./a.rs → src/a.rs
            c => normalized.push(c),
        }
    }
    // P1 fix: reject inputs that normalize to the project root itself (e.g. "." or "/project")
    // — operating on a directory path produces wrong error class (-32603 instead of -32602).
    let s = normalized.to_string_lossy().into_owned();
    if s.is_empty() {
        return Err(RestoreError::PathOutsideRoot(input.to_string()));
    }
    Ok(s)
}

// path_hash is pub(crate) — only used within the library (unit tests + snapshot())


/// Hash the project-relative file path using BLAKE3.
/// Hashes the relative path string (e.g. "src/main.rs"), NOT the absolute path,
/// so the hash is stable regardless of where the project is cloned on disk.
pub(crate) fn path_hash(rel_file_path: &str) -> String {
    blake3::hash(rel_file_path.as_bytes()).to_hex().to_string()
}

/// Snapshot the current contents of a file before it is edited.
///
/// The caller MUST have already verified that the path is inside `cwd` and
/// computed the relative path before calling this function.
///
/// - `cwd`: absolute project root (used to locate the file and the `.olaf/` dir)
/// - `rel_file_path`: path relative to `cwd` (e.g. `"src/main.rs"`)
///
/// Returns `Ok(())` if the file does not exist (AC3: new-file Write is a no-op).
/// Returns `Ok(())` on success. Returns `Err(RestoreError::Io)` only for errors
/// other than `NotFound` — callers in `cli/observe.rs` convert to `anyhow::Error`
/// and the outer `run()` swallows it, ensuring exit 0 (AC7).
/// Create a snapshot of a file before it is modified.
///
/// `storage_root` is where `.olaf/restores/` lives (the canonical project root).
/// `source_root` is where the actual file lives — same as `storage_root` for normal
/// repos, but may be a worktree path for worktree-isolated subagents.
pub fn snapshot(storage_root: &std::path::Path, rel_file_path: &str, source_root: Option<&std::path::Path>) -> Result<(), RestoreError> {
    let read_root = source_root.unwrap_or(storage_root);
    let abs_path = read_root.join(rel_file_path);

    // TOCTOU-safe read: single syscall, match on NotFound for AC3
    let contents = match std::fs::read(&abs_path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(RestoreError::Io(e)),
    };

    let hash = path_hash(rel_file_path);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis(); // u128
    let pid = std::process::id(); // u32
    let seq = SNAP_SEQ.fetch_add(1, Ordering::Relaxed); // unique within process per call

    let snap_dir = storage_root.join(".olaf").join("restores").join(&hash);
    std::fs::create_dir_all(&snap_dir)?;

    // Filename: <millis>-<pid>-<seq>.snap
    // - millis: Story 4.4 sorts by numeric prefix (up to first '-')
    // - pid: prevents concurrent-process collision at same millisecond
    // - seq: prevents same-process same-millisecond collision (e.g. burst edits)
    let tmp_path = snap_dir.join(format!("{}-{}-{}.snap.tmp", ts, pid, seq));
    let snap_path = snap_dir.join(format!("{}-{}-{}.snap", ts, pid, seq));

    std::fs::write(&tmp_path, &contents)?;
    std::fs::rename(&tmp_path, &snap_path)?; // atomic on POSIX (NFR13)

    Ok(())
}

/// List all available restore points for a file, sorted newest-first.
///
/// Returns `Ok(vec![])` if the snap directory does not exist.
/// Each `RestorePoint.id` is the full filename stem (e.g. `"1740000000000-12345-7"`).
pub fn list_restore_points(cwd: &std::path::Path, rel_file_path: &str) -> Result<Vec<RestorePoint>, RestoreError> {
    let snap_dir = cwd.join(".olaf").join("restores").join(path_hash(rel_file_path));
    if !snap_dir.exists() {
        return Ok(vec![]);
    }

    let mut points: Vec<RestorePoint> = std::fs::read_dir(&snap_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.file_name().to_string_lossy().ends_with(".snap")
        })
        .filter_map(|entry| {
            let fname = entry.file_name();
            let fname_str = fname.to_string_lossy();
            let stem = fname_str.strip_suffix(".snap")?;
            // Parse millis from first '-' separated component
            let millis: u128 = stem.split('-').next()?.parse().ok()?;
            let size = entry.metadata().ok()?.len();
            Some(RestorePoint {
                id: stem.to_string(),
                millis,
                size,
            })
        })
        .collect();

    // Sort descending by millis (numeric), then by id descending (tie-break)
    // NEVER sort lexicographically — u128 millis values are not zero-padded
    points.sort_by(|a, b| b.millis.cmp(&a.millis).then_with(|| b.id.cmp(&a.id)));

    Ok(points)
}

/// Restore a file to a specific snapshot.
///
/// Atomically overwrites the live file with snapshot contents (temp write + rename).
/// Preserves original file permissions. Returns `RestoreError::SnapshotNotFound`
/// (with available IDs listed) if `snapshot_id` does not exist.
pub fn restore_to_snapshot(cwd: &std::path::Path, rel_file_path: &str, snapshot_id: &str) -> Result<(), RestoreError> {
    // Validate snapshot_id contains no path separators or '..'
    if snapshot_id.contains('/') || snapshot_id.contains('\\') || snapshot_id.contains("..") {
        return Err(RestoreError::PathOutsideRoot(snapshot_id.to_string()));
    }

    let snap_path = cwd
        .join(".olaf")
        .join("restores")
        .join(path_hash(rel_file_path))
        .join(format!("{snapshot_id}.snap"));

    if !snap_path.exists() {
        let points = list_restore_points(cwd, rel_file_path)?;
        let available_str = points.iter().map(|p| p.id.as_str()).collect::<Vec<_>>().join(", ");
        return Err(RestoreError::SnapshotNotFound(snapshot_id.to_string(), available_str));
    }

    let contents = std::fs::read(&snap_path)?;
    let abs_path = cwd.join(rel_file_path);

    // Preserve permissions (NFR13): capture before creating temp
    let original_perms = std::fs::metadata(&abs_path).ok().map(|m| m.permissions());

    // Atomic write: temp in same directory as target (same filesystem)
    let tmp_path = abs_path.with_extension("snap_restore_tmp");
    std::fs::write(&tmp_path, &contents)?;
    if let Some(perms) = original_perms {
        let _ = std::fs::set_permissions(&tmp_path, perms); // best-effort, non-fatal
    }
    std::fs::rename(&tmp_path, &abs_path)?; // atomic on POSIX (NFR13)

    Ok(())
}

/// Find the snapshot ID for a given millisecond timestamp.
///
/// If multiple snapshots share the same millis, deterministically selects the one
/// with the highest sequence number. Returns `Ok(None)` if no match.
pub fn find_snap_id_by_millis(cwd: &std::path::Path, rel_file_path: &str, millis: u128) -> Result<Option<String>, RestoreError> {
    let points = list_restore_points(cwd, rel_file_path)?;
    let matching: Vec<&RestorePoint> = points.iter().filter(|p| p.millis == millis).collect();
    if matching.is_empty() {
        return Ok(None);
    }
    // Select highest seq: parse 3rd dash-component
    let best = matching.iter().max_by_key(|p| {
        let mut parts = p.id.splitn(3, '-');
        parts.next(); // millis
        parts.next(); // pid
        parts.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0)
    });
    Ok(best.map(|p| p.id.clone()))
}

/// Delete `.snap` files older than 7 days, optionally protecting snapshots
/// newer than `protect_newer_than_ms` (current-session guard).
///
/// Removes empty hash subdirectories after cleanup.
/// Never fails if `.olaf/restores/` doesn't exist.
pub fn cleanup_old_restore_points(cwd: &std::path::Path, protect_newer_than_ms: Option<u128>) -> Result<usize, RestoreError> {
    let seven_days_ms: u128 = 7 * 24 * 60 * 60 * 1000;
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    let cutoff_ms = now_ms.saturating_sub(seven_days_ms);

    let restores_dir = cwd.join(".olaf").join("restores");
    if !restores_dir.exists() {
        return Ok(0);
    }

    let mut deleted_count = 0usize;

    for hash_entry in std::fs::read_dir(&restores_dir)? {
        let hash_entry = hash_entry?;
        let hash_dir = hash_entry.path();
        if !hash_dir.is_dir() {
            continue;
        }

        let snap_entries: Vec<_> = std::fs::read_dir(&hash_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
            .collect();

        for snap_entry in snap_entries {
            let fname = snap_entry.file_name();
            let fname_str = fname.to_string_lossy();
            let stem = fname_str.strip_suffix(".snap").unwrap_or(&fname_str);
            let millis: u128 = stem.split('-').next().and_then(|s| s.parse().ok()).unwrap_or(0);

            // IMPORTANT: use is_none_or (NOT unwrap_or(0)) for the session guard.
            // unwrap_or(0) would set guard=0, making millis<0 always false for u128,
            // silently disabling all cleanup.
            let should_delete = millis < cutoff_ms
                && protect_newer_than_ms.is_none_or(|guard| millis < guard);

            if should_delete && std::fs::remove_file(snap_entry.path()).is_ok() {
                deleted_count += 1;
            }
        }

        // Remove empty hash directories
        if std::fs::read_dir(&hash_dir).map(|mut d| d.next().is_none()).unwrap_or(false) {
            let _ = std::fs::remove_dir(&hash_dir);
        }
    }

    Ok(deleted_count)
}

// ─── Unit Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // 1.4: snapshot creates file at correct path with correct contents
    #[test]
    fn test_snapshot_creates_file_at_correct_path() {
        let tmpdir = tempfile::tempdir().unwrap();
        let src_dir = tmpdir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("main.rs"), b"fn main() {}").unwrap();

        snapshot(tmpdir.path(), "src/main.rs", None).unwrap();

        let expected_hash = path_hash("src/main.rs");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&expected_hash);
        assert!(snap_dir.exists(), "snap dir should exist");

        let snaps: Vec<_> = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
            .collect();
        assert_eq!(snaps.len(), 1, "exactly one .snap file");

        let contents = std::fs::read(snaps[0].path()).unwrap();
        assert_eq!(contents, b"fn main() {}");
    }

    // 1.5: non-existent file → Ok(()), no .olaf/restores created
    #[test]
    fn test_snapshot_nonexistent_file_is_noop() {
        let tmpdir = tempfile::tempdir().unwrap();

        let result = snapshot(tmpdir.path(), "src/main.rs", None);
        assert!(result.is_ok());

        let restores_dir = tmpdir.path().join(".olaf").join("restores");
        assert!(!restores_dir.exists(), ".olaf/restores must NOT be created");
    }

    // 1.6: after successful snapshot, no .tmp files remain
    #[test]
    fn test_snapshot_atomic_no_tmp_left() {
        let tmpdir = tempfile::tempdir().unwrap();
        std::fs::write(tmpdir.path().join("file.txt"), b"hello").unwrap();

        snapshot(tmpdir.path(), "file.txt", None).unwrap();

        let hash = path_hash("file.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);

        let tmp_files: Vec<_> = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(tmp_files.is_empty(), "no .tmp files should remain after rename");
    }

    // 1.7: two rapid snapshots produce two distinct .snap files
    #[test]
    fn test_snapshot_two_rapid_snapshots_produce_two_files() {
        let tmpdir = tempfile::tempdir().unwrap();
        std::fs::write(tmpdir.path().join("data.txt"), b"version1").unwrap();

        snapshot(tmpdir.path(), "data.txt", None).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        std::fs::write(tmpdir.path().join("data.txt"), b"version2").unwrap();
        snapshot(tmpdir.path(), "data.txt", None).unwrap();

        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);

        let snaps: Vec<_> = std::fs::read_dir(&snap_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
            .collect();
        assert_eq!(snaps.len(), 2, "two distinct .snap files");
    }

    // 1.8: path_hash is deterministic; different paths produce different hashes
    #[test]
    fn test_path_hash_is_deterministic() {
        let h1 = path_hash("src/main.rs");
        let h2 = path_hash("src/main.rs");
        assert_eq!(h1, h2, "same path → same hash");

        let h3 = path_hash("src/lib.rs");
        assert_ne!(h1, h3, "different paths → different hashes");
    }

    // 1.8 (story): normalize_rel_path with absolute input
    // Note: file must exist so canonicalize() resolves symlinks (e.g. /var → /private/var on macOS).
    // In real usage, absolute paths always refer to existing files (snapshot before edit / restore).
    #[test]
    fn test_normalize_rel_path_absolute_input() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        let src_dir = root.join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("a.rs"), b"").unwrap();
        let abs = root.join("src").join("a.rs");
        let result = normalize_rel_path(root, abs.to_str().unwrap()).unwrap();
        assert_eq!(result, "src/a.rs");
    }

    // 1.9 (story): normalize_rel_path with relative input
    #[test]
    fn test_normalize_rel_path_relative_input() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        let result = normalize_rel_path(root, "src/a.rs").unwrap();
        assert_eq!(result, "src/a.rs");
    }

    // 1.10 (story): normalize_rel_path strips CurDir
    #[test]
    fn test_normalize_rel_path_curdir_stripped() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        let result = normalize_rel_path(root, "src/./a.rs").unwrap();
        assert_eq!(result, "src/a.rs");
    }

    // 1.11 (story): normalize_rel_path rejects ParentDir
    #[test]
    fn test_normalize_rel_path_parentdir_rejected() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        let malicious = root.join("src").join("..").join("..").join("..").join("etc").join("passwd");
        let result = normalize_rel_path(root, malicious.to_str().unwrap());
        assert!(matches!(result, Err(RestoreError::PathOutsideRoot(_))));
    }

    // Regression: non-existent absolute path containing '..' that escapes the root must be
    // rejected even under a symlinked tmpdir (macOS /var → /private/var).
    // Before the canonicalize_best_effort fix, 'file_name()' returning None for paths
    // ending in '..' caused '..'' components to be silently dropped, letting an escape path
    // pass as if it were inside the project.
    #[test]
    fn test_normalize_rel_path_nonexistent_absolute_escape_via_dotdot() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        // Non-existent file: <root>/sub/../../../etc/passwd — escapes root but doesn't exist.
        // On macOS, root is under /var/folders/... (symlinked root). The '..' chain takes us
        // above the tmpdir; the path must be rejected regardless of symlinks or file existence.
        let escape = root.join("sub").join("..").join("..").join("..").join("etc").join("passwd");
        let result = normalize_rel_path(root, escape.to_str().unwrap());
        assert!(
            matches!(result, Err(RestoreError::PathOutsideRoot(_))),
            "non-existent absolute escape path must be PathOutsideRoot, got {result:?}"
        );
    }

    // Regression: non-existent absolute path that is legitimately inside the project root
    // must be accepted (returns Ok) even on symlinked roots (macOS /var → /private/var).
    // Before the fix, canonicalize() failing on a non-existent file caused fallback to the
    // raw /var/... path, which then mismatched the canonicalized /private/var/... root and
    // was incorrectly rejected as "path escapes project root".
    #[test]
    fn test_normalize_rel_path_nonexistent_absolute_inside_root_via_symlink() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        // Build an absolute path to a non-existent file inside the project root.
        // On macOS: root = /var/folders/...; canonical_root = /private/var/folders/...
        // The raw absolute path uses the unresolved /var/... prefix — without the fix this
        // would fail strip_prefix(canonical_root) and raise PathOutsideRoot incorrectly.
        let nonexistent = root.join("src").join("does_not_exist.rs");
        let result = normalize_rel_path(root, nonexistent.to_str().unwrap());
        assert!(
            matches!(result, Ok(ref s) if s == "src/does_not_exist.rs"),
            "non-existent absolute in-project path must normalize to rel path, got {result:?}"
        );
    }

    // 1.12 (story): normalize_rel_path rejects paths outside root
    #[test]
    fn test_normalize_rel_path_outside_root() {
        let tmpdir = tempfile::tempdir().unwrap();
        let root = tmpdir.path();
        let result = normalize_rel_path(root, "/other/file.rs");
        assert!(matches!(result, Err(RestoreError::PathOutsideRoot(_))));
    }

    // 1.13 (story): list_restore_points returns empty when dir doesn't exist
    #[test]
    fn test_list_restore_points_empty_dir() {
        let tmpdir = tempfile::tempdir().unwrap();
        let points = list_restore_points(tmpdir.path(), "src/a.rs").unwrap();
        assert!(points.is_empty());
    }

    // 1.14 (story): list_restore_points sorted newest-first
    #[test]
    fn test_list_restore_points_sorted_newest_first() {
        let tmpdir = tempfile::tempdir().unwrap();
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Write three snap files with different millis
        std::fs::write(snap_dir.join("1000-1-0.snap"), b"a").unwrap();
        std::fs::write(snap_dir.join("3000-1-0.snap"), b"b").unwrap();
        std::fs::write(snap_dir.join("2000-1-0.snap"), b"c").unwrap();

        let points = list_restore_points(tmpdir.path(), "data.txt").unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].millis, 3000);
        assert_eq!(points[1].millis, 2000);
        assert_eq!(points[2].millis, 1000);
    }

    // 1.15 (story): list_restore_points returns full stem as id
    #[test]
    fn test_list_restore_points_returns_full_id() {
        let tmpdir = tempfile::tempdir().unwrap();
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();

        let stem = "1740000000000-12345-7";
        std::fs::write(snap_dir.join(format!("{stem}.snap")), b"content").unwrap();

        let points = list_restore_points(tmpdir.path(), "data.txt").unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].id, stem);
    }

    // 1.16 (story): restore_to_snapshot overwrites file with snapshot content
    #[test]
    fn test_restore_to_snapshot_overwrites_file() {
        let tmpdir = tempfile::tempdir().unwrap();
        let file_path = tmpdir.path().join("data.txt");
        std::fs::write(&file_path, b"original content").unwrap();

        // Create a snapshot manually
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();
        let snap_id = "1000000000000-1-0";
        std::fs::write(snap_dir.join(format!("{snap_id}.snap")), b"snapshot content").unwrap();

        // Overwrite live file
        std::fs::write(&file_path, b"bad content").unwrap();

        // Restore
        restore_to_snapshot(tmpdir.path(), "data.txt", snap_id).unwrap();

        let restored = std::fs::read(&file_path).unwrap();
        assert_eq!(restored, b"snapshot content");
    }

    // 1.17 (story): restore_to_snapshot preserves permissions
    #[cfg(unix)]
    #[test]
    fn test_restore_to_snapshot_preserves_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let tmpdir = tempfile::tempdir().unwrap();
        let file_path = tmpdir.path().join("script.sh");
        std::fs::write(&file_path, b"#!/bin/sh\necho hello").unwrap();

        // Set executable
        let mut perms = std::fs::metadata(&file_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&file_path, perms).unwrap();

        // Create snapshot
        let hash = path_hash("script.sh");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();
        let snap_id = "1000000000000-1-0";
        std::fs::write(snap_dir.join(format!("{snap_id}.snap")), b"#!/bin/sh\necho world").unwrap();

        // Restore
        restore_to_snapshot(tmpdir.path(), "script.sh", snap_id).unwrap();

        let mode = std::fs::metadata(&file_path).unwrap().permissions().mode();
        assert!(mode & 0o111 != 0, "executable bit must be preserved");
    }

    // 1.18 (story): restore_to_snapshot returns SnapshotNotFound for unknown id
    #[test]
    fn test_restore_to_snapshot_not_found() {
        let tmpdir = tempfile::tempdir().unwrap();
        // No snap files
        let result = restore_to_snapshot(tmpdir.path(), "data.txt", "nonexistent-id");
        assert!(matches!(result, Err(RestoreError::SnapshotNotFound(_, _))));
    }

    // 1.19 (story): restore_to_snapshot rejects path traversal in snapshot_id
    #[test]
    fn test_restore_to_snapshot_rejects_path_traversal_id() {
        let tmpdir = tempfile::tempdir().unwrap();
        let result = restore_to_snapshot(tmpdir.path(), "data.txt", "../evil");
        assert!(matches!(result, Err(RestoreError::PathOutsideRoot(_))));
    }

    // 1.20 (story): find_snap_id_by_millis picks highest seq
    #[test]
    fn test_find_snap_id_by_millis_picks_highest_seq() {
        let tmpdir = tempfile::tempdir().unwrap();
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();

        std::fs::write(snap_dir.join("5000-1-0.snap"), b"seq0").unwrap();
        std::fs::write(snap_dir.join("5000-1-1.snap"), b"seq1").unwrap();

        let id = find_snap_id_by_millis(tmpdir.path(), "data.txt", 5000).unwrap().unwrap();
        assert_eq!(id, "5000-1-1");
    }

    // 1.21 (story): cleanup deletes old snaps, keeps new ones
    #[test]
    fn test_cleanup_deletes_old_snaps_keeps_new() {
        let tmpdir = tempfile::tempdir().unwrap();
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Old snap: millis = 1 (epoch+1ms — definitely >7 days ago)
        std::fs::write(snap_dir.join("1-1-0.snap"), b"old").unwrap();

        // New snap: millis = now
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
        std::fs::write(snap_dir.join(format!("{now_ms}-1-0.snap")), b"new").unwrap();

        let deleted = cleanup_old_restore_points(tmpdir.path(), None).unwrap();
        assert_eq!(deleted, 1, "only old snap should be deleted");

        let remaining: Vec<_> = std::fs::read_dir(&snap_dir).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".snap"))
            .collect();
        assert_eq!(remaining.len(), 1, "new snap must remain");
    }
    // 1.22 (story): session guard protects snap taken during current session.
    // Guard semantics: snaps with millis >= guard are from the session → protected.
    // Snaps with millis < guard are pre-session → eligible (combined with 7-day cutoff).
    #[test]
    fn test_cleanup_session_guard_protects_recent() {
        let tmpdir = tempfile::tempdir().unwrap();
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Snap at millis=1000 (past 7-day cutoff — ancient epoch time)
        let snap_millis: u128 = 1000;
        std::fs::write(snap_dir.join(format!("{snap_millis}-1-0.snap")), b"snap").unwrap();

        // Session started at millis=500 (before the snap was taken).
        // snap_millis=1000 >= guard=500 → millis < guard is false → NOT eligible → protected.
        let guard_ms: u128 = 500;
        let deleted = cleanup_old_restore_points(tmpdir.path(), Some(guard_ms)).unwrap();
        assert_eq!(deleted, 0, "snap from during current session must be protected");
    }
    // 1.23 (story): cleanup removes empty hash dirs
    #[test]
    fn test_cleanup_removes_empty_hash_dirs() {
        let tmpdir = tempfile::tempdir().unwrap();
        let hash = path_hash("data.txt");
        let snap_dir = tmpdir.path().join(".olaf").join("restores").join(&hash);
        std::fs::create_dir_all(&snap_dir).unwrap();

        // Old snap only
        std::fs::write(snap_dir.join("1-1-0.snap"), b"old").unwrap();

        let deleted = cleanup_old_restore_points(tmpdir.path(), None).unwrap();
        assert_eq!(deleted, 1);
        assert!(!snap_dir.exists(), "empty hash dir should be removed");
    }

    // 1.24 (story): cleanup is noop when .olaf/restores/ doesn't exist
    #[test]
    fn test_cleanup_no_restores_dir_is_noop() {
        let tmpdir = tempfile::tempdir().unwrap();
        let result = cleanup_old_restore_points(tmpdir.path(), None).unwrap();
        assert_eq!(result, 0);
    }
}
