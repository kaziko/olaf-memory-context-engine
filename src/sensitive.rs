use std::path::Path;

/// Returns true if the file should be excluded for security reasons.
///
/// Single canonical implementation used at multiple defense-in-depth layers:
/// file indexing, graph output, memory retrieval, and MCP tool responses.
///
/// Patterns (ASCII case-insensitive): .env, id_rsa, .env.*, id_rsa.*, *.pem, *.key, *.p12
///
/// Limitation: returns false for non-UTF8 filenames (file_name().to_str() -> None).
/// This is an existing limitation, not proof such files cannot appear in the index.
pub fn is_sensitive<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();
    let file_name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    // Exact names (ASCII case-insensitive)
    if file_name.eq_ignore_ascii_case(".env") || file_name.eq_ignore_ascii_case("id_rsa") {
        return true;
    }
    // Prefix patterns (ASCII case-insensitive)
    let lower = file_name.to_ascii_lowercase();
    if lower.starts_with(".env.") || lower.starts_with("id_rsa.") {
        return true;
    }
    // Extension patterns (ASCII case-insensitive)
    if let Some(ext) = path.extension().and_then(|e| e.to_str())
        && matches!(ext.to_ascii_lowercase().as_str(), "pem" | "key" | "p12")
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

    #[test]
    fn test_is_sensitive_directory_prefixed_paths() {
        assert!(is_sensitive(Path::new("certs/server.pem")));
        assert!(is_sensitive(Path::new("config/.env.local")));
    }

    #[test]
    fn test_is_sensitive_case_insensitive() {
        assert!(is_sensitive(".ENV"));
        assert!(is_sensitive("CERT.PEM"));
        assert!(is_sensitive("Id_Rsa"));
        assert!(is_sensitive(".ENV.local"));
        assert!(is_sensitive("ID_RSA.pub"));
    }

    #[test]
    #[cfg(unix)]
    fn test_is_sensitive_non_utf8_filename_returns_false() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        // Non-UTF8 filename: file_name().to_str() returns None, so we return false.
        // Limitation: the indexer uses to_string_lossy(), so non-UTF8 filenames can
        // appear in the index lossily. This is an existing limitation preserved by
        // this refactor, not proof such files cannot appear.
        let non_utf8 = OsStr::from_bytes(&[0xFF, 0xFE]);
        let path = Path::new(non_utf8);
        assert!(!is_sensitive(path));
    }
}
