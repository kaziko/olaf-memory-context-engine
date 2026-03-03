// Restore store — filesystem snapshot operations for Story 4.3+

use std::sync::atomic::{AtomicU64, Ordering};

/// Per-process monotonic counter — makes snapshot filenames unique even when
/// two calls land within the same millisecond in the same process.
static SNAP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
pub fn snapshot(cwd: &std::path::Path, rel_file_path: &str) -> Result<(), RestoreError> {
    let abs_path = cwd.join(rel_file_path);

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

    let snap_dir = cwd.join(".olaf").join("restores").join(&hash);
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

        snapshot(tmpdir.path(), "src/main.rs").unwrap();

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

        let result = snapshot(tmpdir.path(), "src/main.rs");
        assert!(result.is_ok());

        let restores_dir = tmpdir.path().join(".olaf").join("restores");
        assert!(!restores_dir.exists(), ".olaf/restores must NOT be created");
    }

    // 1.6: after successful snapshot, no .tmp files remain
    #[test]
    fn test_snapshot_atomic_no_tmp_left() {
        let tmpdir = tempfile::tempdir().unwrap();
        std::fs::write(tmpdir.path().join("file.txt"), b"hello").unwrap();

        snapshot(tmpdir.path(), "file.txt").unwrap();

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

        snapshot(tmpdir.path(), "data.txt").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        std::fs::write(tmpdir.path().join("data.txt"), b"version2").unwrap();
        snapshot(tmpdir.path(), "data.txt").unwrap();

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
}
