pub(crate) mod full;
pub(crate) mod incremental;

// Minimal public re-export for the binary crate (cli/index.rs).
// The `full` module itself stays pub(crate) — only the run entry-point and its
// return type cross the library/binary boundary.
pub use full::{IndexStats, run};
// Expose incremental run for integration tests and the MCP query path (Story 2.2).
// The module itself stays pub(crate) — callers use olaf::index::run_incremental().
pub use incremental::run as run_incremental;

use std::path::Path;

/// Returns `true` if the file should be excluded for security reasons.
///
/// This is Layer 1 of the defense-in-depth sensitive exclusion strategy
/// (architecture.md §Security). It prevents sensitive files from ever
/// being read, parsed, or entered in the `files` table.
///
/// Patterns matched:
/// - Exact names: `.env`, `id_rsa`
/// - Prefix patterns: `.env.*`, `id_rsa.*`
/// - Extension patterns: `*.pem`, `*.key`, `*.p12`
pub(crate) fn is_sensitive(path: &Path) -> bool {
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    // Exact names
    if matches!(file_name, ".env" | "id_rsa") {
        return true;
    }
    // Prefix patterns
    if file_name.starts_with(".env.") || file_name.starts_with("id_rsa.") {
        return true;
    }
    // Extension patterns
    if let Some(ext) = path.extension().and_then(|e| e.to_str())
        && matches!(ext, "pem" | "key" | "p12")
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_is_sensitive_exact_names() {
        assert!(is_sensitive(Path::new(".env")));
        assert!(is_sensitive(Path::new("id_rsa")));
        assert!(!is_sensitive(Path::new("main.rs")));
    }

    #[test]
    fn test_is_sensitive_prefix_patterns() {
        assert!(is_sensitive(Path::new(".env.local")));
        assert!(is_sensitive(Path::new(".env.production")));
        assert!(is_sensitive(Path::new("id_rsa.pub")));
        assert!(!is_sensitive(Path::new("env_config.rs")));
    }

    #[test]
    fn test_is_sensitive_extension_patterns() {
        assert!(is_sensitive(Path::new("cert.pem")));
        assert!(is_sensitive(Path::new("secret.key")));
        assert!(is_sensitive(Path::new("keystore.p12")));
        assert!(!is_sensitive(Path::new("main.rs")));
        assert!(!is_sensitive(Path::new("app.ts")));
    }
}
