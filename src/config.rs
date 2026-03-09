use std::path::{Path, PathBuf};

/// Resolve a path that might be a git worktree back to the main repository root.
///
/// If `cwd` is inside a git worktree (`.git` is a file, not a directory),
/// follows the `gitdir:` pointer to find the main `.git/` directory,
/// then returns its parent as the canonical project root.
///
/// Returns:
/// - The main repo root if `cwd` is a worktree
/// - `cwd` unchanged if it's a normal repo or not a git repo
pub fn resolve_worktree_root(cwd: &Path) -> PathBuf {
    let git_path = cwd.join(".git");

    if !git_path.is_file() {
        // Normal repo (.git/ dir) or not a git repo — return as-is
        return cwd.to_path_buf();
    }

    // .git is a file → this is a worktree. Parse gitdir: pointer.
    let content = match std::fs::read_to_string(&git_path) {
        Ok(c) => c,
        Err(_) => return cwd.to_path_buf(),
    };

    let gitdir_str = match content.trim().strip_prefix("gitdir:") {
        Some(s) => s.trim(),
        None => return cwd.to_path_buf(),
    };

    let gitdir: PathBuf = if Path::new(gitdir_str).is_absolute() {
        PathBuf::from(gitdir_str)
    } else {
        cwd.join(gitdir_str)
    };

    // gitdir typically points to: <main-repo>/.git/worktrees/<name>
    // For submodules it points to: <parent-repo>/.git/modules/<name>
    // We must only resolve worktrees, NOT submodules (which are separate repos).
    let canonical = gitdir.canonicalize().unwrap_or(gitdir);

    // Check that this is actually a worktree (has a "worktrees" ancestor component)
    let is_worktree = canonical.components().any(|c| {
        matches!(c, std::path::Component::Normal(n) if n == "worktrees")
    });
    if !is_worktree {
        return cwd.to_path_buf();
    }

    // Walk up from gitdir looking for the .git directory boundary.
    let mut ancestor = canonical.as_path();
    loop {
        if ancestor.file_name().is_some_and(|n| n == ".git") {
            // Found the .git directory — parent is the main repo root
            if let Some(repo_root) = ancestor.parent() {
                return repo_root.to_path_buf();
            }
            break;
        }
        match ancestor.parent() {
            Some(p) => ancestor = p,
            None => break,
        }
    }

    // Fallback: couldn't resolve, return original cwd
    cwd.to_path_buf()
}

/// Detect the current git branch for the given project root.
///
/// Reads `.git/HEAD` directly — does NOT shell out to `git`.
/// Handles both normal repos (`.git/` directory) and git worktrees (`.git` file).
///
/// Returns:
/// - `Some("branch-name")` when on a named branch
/// - `None` when in detached HEAD state, not a git repo, or any read error
pub fn detect_git_branch(project_root: &Path) -> Option<String> {
    let git_path = project_root.join(".git");

    let head_path: PathBuf = if git_path.is_dir() {
        // Normal repo: .git is a directory
        git_path.join("HEAD")
    } else if git_path.is_file() {
        // Worktree: .git is a file containing "gitdir: <path>"
        let content = std::fs::read_to_string(&git_path).ok()?;
        let gitdir_str = content.trim().strip_prefix("gitdir:")?;
        let gitdir_str = gitdir_str.trim();
        let gitdir: PathBuf = if Path::new(gitdir_str).is_absolute() {
            PathBuf::from(gitdir_str)
        } else {
            // Resolve relative path against project_root
            project_root.join(gitdir_str)
        };
        gitdir.join("HEAD")
    } else {
        // No .git — not a git repo
        return None;
    };

    let head_content = std::fs::read_to_string(&head_path).ok()?;
    let head = head_content.trim();

    head.strip_prefix("ref: refs/heads/").map(|branch| branch.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write_head(git_dir: &Path, content: &str) {
        fs::write(git_dir.join("HEAD"), content).unwrap();
    }

    #[test]
    fn test_detect_branch_normal() {
        let dir = tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        fs::create_dir(&git_dir).unwrap();
        write_head(&git_dir, "ref: refs/heads/main\n");
        assert_eq!(detect_git_branch(dir.path()), Some("main".to_string()));
    }

    #[test]
    fn test_detect_branch_feature_slash() {
        let dir = tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        fs::create_dir(&git_dir).unwrap();
        write_head(&git_dir, "ref: refs/heads/feature/auth\n");
        assert_eq!(detect_git_branch(dir.path()), Some("feature/auth".to_string()));
    }

    #[test]
    fn test_detect_branch_detached() {
        let dir = tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        fs::create_dir(&git_dir).unwrap();
        write_head(&git_dir, "abc1234567890abcdef1234567890abcdef123456\n");
        assert_eq!(detect_git_branch(dir.path()), None);
    }

    #[test]
    fn test_detect_branch_no_git() {
        let dir = tempdir().unwrap();
        assert_eq!(detect_git_branch(dir.path()), None);
    }

    // --- resolve_worktree_root tests ---

    #[test]
    fn test_resolve_worktree_root_normal_repo() {
        let dir = tempdir().unwrap();
        let git_dir = dir.path().join(".git");
        fs::create_dir(&git_dir).unwrap();
        write_head(&git_dir, "ref: refs/heads/main\n");
        assert_eq!(resolve_worktree_root(dir.path()), dir.path().to_path_buf());
    }

    #[test]
    fn test_resolve_worktree_root_no_git() {
        let dir = tempdir().unwrap();
        assert_eq!(resolve_worktree_root(dir.path()), dir.path().to_path_buf());
    }

    #[test]
    fn test_resolve_worktree_root_absolute_gitdir() {
        let base = tempdir().unwrap();
        // Main repo
        let main_repo = base.path().join("main_repo");
        let main_git = main_repo.join(".git");
        let worktree_gitdir = main_git.join("worktrees").join("wt1");
        fs::create_dir_all(&worktree_gitdir).unwrap();
        write_head(&worktree_gitdir, "ref: refs/heads/wt-branch\n");

        // Worktree directory with .git file
        let wt_dir = base.path().join("worktree_wt1");
        fs::create_dir_all(&wt_dir).unwrap();
        fs::write(
            wt_dir.join(".git"),
            format!("gitdir: {}\n", worktree_gitdir.to_string_lossy()),
        ).unwrap();

        let resolved = resolve_worktree_root(&wt_dir);
        // Should resolve to main_repo
        assert_eq!(resolved.canonicalize().unwrap(), main_repo.canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_worktree_root_relative_gitdir() {
        let base = tempdir().unwrap();
        // Main repo
        let main_repo = base.path().join("main_repo");
        let main_git = main_repo.join(".git");
        let worktree_gitdir = main_git.join("worktrees").join("wt2");
        fs::create_dir_all(&worktree_gitdir).unwrap();
        write_head(&worktree_gitdir, "ref: refs/heads/wt-branch\n");

        // Worktree directory — sibling of main_repo
        let wt_dir = base.path().join("worktree_wt2");
        fs::create_dir_all(&wt_dir).unwrap();
        fs::write(
            wt_dir.join(".git"),
            "gitdir: ../main_repo/.git/worktrees/wt2\n",
        ).unwrap();

        let resolved = resolve_worktree_root(&wt_dir);
        assert_eq!(resolved.canonicalize().unwrap(), main_repo.canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_worktree_root_submodule_not_resolved() {
        // Submodule .git file points to parent's .git/modules/<name>, NOT worktrees
        let base = tempdir().unwrap();
        // Parent repo with modules dir
        let parent_repo = base.path().join("parent_repo");
        let modules_gitdir = parent_repo.join(".git").join("modules").join("sub1");
        fs::create_dir_all(&modules_gitdir).unwrap();
        write_head(&modules_gitdir, "ref: refs/heads/main\n");

        // Submodule directory with .git file
        let sub_dir = base.path().join("submodule_dir");
        fs::create_dir_all(&sub_dir).unwrap();
        fs::write(
            sub_dir.join(".git"),
            format!("gitdir: {}\n", modules_gitdir.to_string_lossy()),
        ).unwrap();

        // Should NOT resolve to parent — submodule is its own project
        assert_eq!(resolve_worktree_root(&sub_dir), sub_dir.to_path_buf());
    }

    #[test]
    fn test_resolve_worktree_root_malformed_git_file() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(".git"), "not a gitdir pointer\n").unwrap();
        // Should fall back to original cwd
        assert_eq!(resolve_worktree_root(dir.path()), dir.path().to_path_buf());
    }

    // --- detect_git_branch tests ---

    #[test]
    fn test_detect_branch_worktree_absolute() {
        let base = tempdir().unwrap();
        // Main repo stores the worktree HEAD
        let worktree_git_dir = base.path().join("main_repo").join(".git").join("worktrees").join("wt");
        fs::create_dir_all(&worktree_git_dir).unwrap();
        write_head(&worktree_git_dir, "ref: refs/heads/feature/wt-branch\n");

        // Separate worktree project dir with .git file pointing to absolute path
        let wt_dir = base.path().join("worktree");
        fs::create_dir_all(&wt_dir).unwrap();
        let gitdir_path = worktree_git_dir.to_string_lossy().to_string();
        fs::write(wt_dir.join(".git"), format!("gitdir: {}\n", gitdir_path)).unwrap();

        assert_eq!(detect_git_branch(&wt_dir), Some("feature/wt-branch".to_string()));
    }

    #[test]
    fn test_detect_branch_worktree_relative() {
        // Create a structure: main_repo/.git/worktrees/wt/HEAD
        // worktree dir with .git file pointing to ../main_repo/.git/worktrees/wt
        let base = tempdir().unwrap();
        let main_git = base.path().join("main_repo").join(".git");
        let worktree_git_dir = main_git.join("worktrees").join("wt");
        fs::create_dir_all(&worktree_git_dir).unwrap();
        write_head(&worktree_git_dir, "ref: refs/heads/relative-branch\n");

        // Create worktree project dir
        let worktree_dir = base.path().join("worktree");
        fs::create_dir_all(&worktree_dir).unwrap();

        // .git file with relative path from worktree_dir to worktree_git_dir
        // worktree_git_dir is at base/main_repo/.git/worktrees/wt
        // worktree_dir is at base/worktree
        // relative: ../main_repo/.git/worktrees/wt
        fs::write(
            worktree_dir.join(".git"),
            "gitdir: ../main_repo/.git/worktrees/wt\n",
        ).unwrap();

        assert_eq!(detect_git_branch(&worktree_dir), Some("relative-branch".to_string()));
    }
}
