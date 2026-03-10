use glob::Pattern;
use log::warn;
use std::path::Path;

#[derive(Debug, Default, Clone, serde::Deserialize)]
pub struct PolicyConfig {
    #[serde(default)]
    pub deny: Vec<PolicyRule>,
    #[serde(default)]
    pub redact: Vec<PolicyRule>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PolicyRule {
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub fqn_prefix: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Default)]
pub struct ContentPolicy {
    deny_paths: Vec<Pattern>,
    deny_fqn_prefixes: Vec<String>,
    redact_paths: Vec<Pattern>,
    redact_fqn_prefixes: Vec<String>,
}

const MATCH_OPTIONS: glob::MatchOptions = glob::MatchOptions {
    case_sensitive: true,
    require_literal_separator: false,
    require_literal_leading_dot: false,
};

impl ContentPolicy {
    pub fn load(project_root: &Path) -> Self {
        let policy_path = project_root.join(".olaf/policy.toml");
        let content = match std::fs::read_to_string(&policy_path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(e) => {
                warn!("Failed to read .olaf/policy.toml: {e}");
                return Self::default();
            }
        };

        let config: PolicyConfig = match toml::from_str(&content) {
            Ok(c) => c,
            Err(e) => {
                warn!("Invalid .olaf/policy.toml: {e}");
                return Self::default();
            }
        };

        Self::from_config(&config)
    }

    fn from_config(config: &PolicyConfig) -> Self {
        let mut deny_paths = Vec::new();
        let mut deny_fqn_prefixes = Vec::new();
        let mut redact_paths = Vec::new();
        let mut redact_fqn_prefixes = Vec::new();

        for rule in &config.deny {
            if rule.path.is_none() && rule.fqn_prefix.is_none() {
                warn!("Deny rule has neither path nor fqn_prefix, skipping");
                continue;
            }
            if let Some(ref p) = rule.path {
                match Pattern::new(p) {
                    Ok(pat) => deny_paths.push(pat),
                    Err(e) => warn!("Invalid deny path glob '{p}': {e}"),
                }
            }
            if let Some(ref prefix) = rule.fqn_prefix {
                deny_fqn_prefixes.push(prefix.clone());
            }
        }

        for rule in &config.redact {
            if rule.path.is_none() && rule.fqn_prefix.is_none() {
                warn!("Redact rule has neither path nor fqn_prefix, skipping");
                continue;
            }
            if let Some(ref p) = rule.path {
                match Pattern::new(p) {
                    Ok(pat) => redact_paths.push(pat),
                    Err(e) => warn!("Invalid redact path glob '{p}': {e}"),
                }
            }
            if let Some(ref prefix) = rule.fqn_prefix {
                redact_fqn_prefixes.push(prefix.clone());
            }
        }

        Self {
            deny_paths,
            deny_fqn_prefixes,
            redact_paths,
            redact_fqn_prefixes,
        }
    }

    fn fqn_matches_prefix(fqn: &str, prefix: &str) -> bool {
        fqn == prefix || fqn.starts_with(&format!("{prefix}::"))
    }

    pub fn is_denied(&self, file_path: &str, fqn: Option<&str>) -> bool {
        for pat in &self.deny_paths {
            if pat.matches_with(file_path, MATCH_OPTIONS) {
                return true;
            }
        }
        if let Some(fqn) = fqn {
            for prefix in &self.deny_fqn_prefixes {
                if Self::fqn_matches_prefix(fqn, prefix) {
                    return true;
                }
            }
        }
        false
    }

    pub fn is_redacted(&self, file_path: &str, fqn: Option<&str>) -> bool {
        // Deny takes precedence — if denied, not redacted (excluded entirely)
        if self.is_denied(file_path, fqn) {
            return false;
        }
        for pat in &self.redact_paths {
            if pat.matches_with(file_path, MATCH_OPTIONS) {
                return true;
            }
        }
        if let Some(fqn) = fqn {
            for prefix in &self.redact_fqn_prefixes {
                if Self::fqn_matches_prefix(fqn, prefix) {
                    return true;
                }
            }
        }
        false
    }

    /// Check if a symbol FQN is denied, including path-based deny rules.
    /// Extracts the file component from the FQN (prefix before first `::`)
    /// and checks it against path deny patterns. Use this for direct-query
    /// guards where only an FQN is available (no separate file_path).
    pub fn is_denied_by_fqn(&self, fqn: &str) -> bool {
        let file_component = fqn.split("::").next().unwrap_or("");
        self.is_denied(file_component, Some(fqn))
    }

    pub fn is_empty(&self) -> bool {
        self.deny_paths.is_empty()
            && self.deny_fqn_prefixes.is_empty()
            && self.redact_paths.is_empty()
            && self.redact_fqn_prefixes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn policy_from_toml(toml_str: &str) -> ContentPolicy {
        let config: PolicyConfig = toml::from_str(toml_str).unwrap();
        ContentPolicy::from_config(&config)
    }

    #[test]
    fn test_empty_policy_denies_nothing() {
        let policy = ContentPolicy::default();
        assert!(!policy.is_denied("any/file.rs", None));
        assert!(!policy.is_denied("any/file.rs", Some("any::Symbol")));
        assert!(!policy.is_redacted("any/file.rs", None));
        assert!(policy.is_empty());
    }

    #[test]
    fn test_deny_path_glob() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "internal/billing/**"
        "#);
        assert!(policy.is_denied("internal/billing/service.rs", None));
        assert!(policy.is_denied("internal/billing/deep/nested.rs", None));
        assert!(!policy.is_denied("internal/auth/service.rs", None));
    }

    #[test]
    fn test_deny_fqn_prefix() {
        let policy = policy_from_toml(r#"
            [[deny]]
            fqn_prefix = "src/billing.rs::BillingService"
        "#);
        assert!(policy.is_denied("", Some("src/billing.rs::BillingService")));
        assert!(policy.is_denied("", Some("src/billing.rs::BillingService::charge")));
        assert!(!policy.is_denied("", Some("src/auth.rs::AuthService")));
    }

    #[test]
    fn test_fqn_prefix_boundary() {
        let policy = policy_from_toml(r#"
            [[deny]]
            fqn_prefix = "src/mod.rs::Foo"
        "#);
        assert!(policy.is_denied("", Some("src/mod.rs::Foo")));
        assert!(policy.is_denied("", Some("src/mod.rs::Foo::bar")));
        assert!(!policy.is_denied("", Some("src/mod.rs::FooBar")));
    }

    #[test]
    fn test_redact_path_glob() {
        let policy = policy_from_toml(r#"
            [[redact]]
            path = "secrets/**"
        "#);
        assert!(policy.is_redacted("secrets/config.rs", None));
        assert!(!policy.is_denied("secrets/config.rs", None));
    }

    #[test]
    fn test_redact_fqn_prefix() {
        let policy = policy_from_toml(r#"
            [[redact]]
            fqn_prefix = "src/internal.rs::InternalAPI"
        "#);
        assert!(policy.is_redacted("", Some("src/internal.rs::InternalAPI")));
        assert!(policy.is_redacted("", Some("src/internal.rs::InternalAPI::method")));
    }

    #[test]
    fn test_deny_takes_precedence_over_redact() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "secrets/**"
            [[redact]]
            path = "secrets/**"
        "#);
        assert!(policy.is_denied("secrets/key.rs", None));
        assert!(!policy.is_redacted("secrets/key.rs", None));
    }

    #[test]
    fn test_no_policy_file_returns_default() {
        let tmp = TempDir::new().unwrap();
        let policy = ContentPolicy::load(tmp.path());
        assert!(policy.is_empty());
    }

    #[test]
    fn test_malformed_policy_returns_default() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join(".olaf");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("policy.toml"), "this is not valid { toml").unwrap();
        let policy = ContentPolicy::load(tmp.path());
        assert!(policy.is_empty());
    }

    #[test]
    fn test_invalid_glob_skipped() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "[invalid"
            [[deny]]
            path = "valid/**"
        "#);
        // Invalid glob skipped, valid one works
        assert!(policy.is_denied("valid/file.rs", None));
        assert!(!policy.is_denied("[invalid", None));
    }

    #[test]
    fn test_rule_with_neither_path_nor_fqn_skipped() {
        let policy = policy_from_toml(r#"
            [[deny]]
            reason = "Just a reason, no path or fqn"
            [[deny]]
            path = "real/**"
        "#);
        assert!(policy.is_denied("real/file.rs", None));
        assert!(!policy.is_empty());
    }

    #[test]
    fn test_multiple_deny_rules() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "secret/**"
            [[deny]]
            path = "internal/**"
            [[deny]]
            fqn_prefix = "src/private.rs::Hidden"
        "#);
        assert!(policy.is_denied("secret/key.rs", None));
        assert!(policy.is_denied("internal/billing.rs", None));
        assert!(policy.is_denied("", Some("src/private.rs::Hidden::method")));
        assert!(!policy.is_denied("public/api.rs", None));
    }

    #[test]
    fn test_glob_double_star_crosses_dirs() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "internal/**"
        "#);
        assert!(policy.is_denied("internal/deep/nested/file.rs", None));
    }

    #[test]
    fn test_glob_single_star_within_component() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "src/*.secret.rs"
        "#);
        assert!(policy.is_denied("src/my.secret.rs", None));
        assert!(!policy.is_denied("src/my.public.rs", None));
    }

    #[test]
    fn test_is_denied_by_fqn_extracts_file_component() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "src/secret.rs"
        "#);
        // FQN "src/secret.rs::Foo::bar" → file component "src/secret.rs" matches path deny
        assert!(policy.is_denied_by_fqn("src/secret.rs::Foo::bar"));
        assert!(policy.is_denied_by_fqn("src/secret.rs::Foo"));
        // Non-matching path
        assert!(!policy.is_denied_by_fqn("src/public.rs::Foo"));
    }

    #[test]
    fn test_is_denied_by_fqn_path_glob() {
        let policy = policy_from_toml(r#"
            [[deny]]
            path = "internal/**"
        "#);
        assert!(policy.is_denied_by_fqn("internal/billing.rs::Service::charge"));
        assert!(!policy.is_denied_by_fqn("src/app.rs::Main"));
    }

    #[test]
    fn test_is_denied_by_fqn_also_checks_fqn_prefix() {
        let policy = policy_from_toml(r#"
            [[deny]]
            fqn_prefix = "src/mod.rs::Secret"
        "#);
        assert!(policy.is_denied_by_fqn("src/mod.rs::Secret::method"));
        assert!(!policy.is_denied_by_fqn("src/mod.rs::Public::method"));
    }
}
