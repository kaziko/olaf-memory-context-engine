use std::path::{Path, PathBuf};

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

    if let Some(branch) = head.strip_prefix("ref: refs/heads/") {
        Some(branch.to_string())
    } else {
        // Detached HEAD (raw SHA or other ref)
        None
    }
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
