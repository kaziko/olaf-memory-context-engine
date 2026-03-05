use std::path::Path;

use serde::Deserialize;

use crate::index::diff::StructuralDiff;

/// Parsed representation of a Claude Code PostToolUse hook payload.
///
/// `session_id` and `cwd` are required. All other fields are optional so
/// that partially-formed or future-extended payloads don't fail deserialization.
#[derive(Debug, Deserialize)]
pub struct HookPayload {
    pub session_id: String,
    pub cwd: String,
    #[serde(default)]
    pub hook_event_name: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_input: Option<serde_json::Value>,
    #[serde(default)]
    pub tool_response: Option<serde_json::Value>,
    #[serde(default)]
    pub tool_use_id: Option<String>,
    #[serde(default)]
    pub transcript_path: Option<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
}

/// Structured result of parsing a PostToolUse hook payload, ready for DB insertion.
#[derive(Debug)]
pub struct PostToolUseResult {
    pub session_id: String,
    /// Relative path (relative to project root). None for Bash observations.
    pub file_path: Option<String>,
    /// Human-readable English sentence describing the tool action.
    pub content: String,
    /// `"file_change"` for Edit/Write, `"tool_call"` for Bash.
    pub kind: &'static str,
}

/// Format a structural diff into a human-readable observation string.
///
/// Returns `None` if the diff has no structural changes (body-only or empty).
/// Caps output at 5 change descriptions to avoid overly verbose observations.
pub fn format_structural_observation(diff: &StructuralDiff) -> Option<String> {
    if !diff.has_structural_changes() {
        return None;
    }

    let total = diff.added.len() + diff.signature_changed.len() + diff.removed.len();
    let mut parts: Vec<String> = Vec::new();

    for fqn in &diff.added {
        parts.push(format!("added `{}`", short_name(fqn)));
    }
    for (fqn, old_sig, new_sig) in &diff.signature_changed {
        parts.push(format!(
            "signature of `{}` changed from `{}` to `{}`",
            short_name(fqn),
            old_sig,
            new_sig
        ));
    }
    for fqn in &diff.removed {
        parts.push(format!("removed `{}`", short_name(fqn)));
    }

    if parts.len() > 5 {
        let excess = total - 5;
        parts.truncate(5);
        parts.push(format!("and {} more", excess));
    }

    Some(format!("Modified `{}`: {}", diff.file_path, parts.join(", ")))
}

fn short_name(fqn: &str) -> &str {
    fqn.rsplit("::").next().unwrap_or(fqn)
}

/// Parse a PostToolUse hook payload into an observation result.
///
/// Returns `None` when:
/// - `tool_name` is missing (None) or unsupported (e.g., "Read", "Glob")
/// - Required fields within `tool_input` are absent
/// - `file_path` is outside the project root (strip_prefix fails)
pub fn parse_post_tool_use(payload: &HookPayload) -> Option<PostToolUseResult> {
    let tool_name = payload.tool_name.as_deref()?;

    match tool_name {
        "Edit" => {
            let tool_input = payload.tool_input.as_ref()?;
            let abs_path = tool_input.get("file_path")?.as_str()?;
            let rel_path = relativize_path(abs_path, &payload.cwd)?;

            let old_len = tool_input
                .get("old_string")
                .and_then(|v| v.as_str())
                .map(|s| s.len())
                .unwrap_or(0);
            let content = format!("Edited {rel_path}: replaced {old_len} chars");

            Some(PostToolUseResult {
                session_id: payload.session_id.clone(),
                file_path: Some(rel_path),
                content,
                kind: "file_change",
            })
        }
        "Write" => {
            let tool_input = payload.tool_input.as_ref()?;
            let abs_path = tool_input.get("file_path")?.as_str()?;
            let rel_path = relativize_path(abs_path, &payload.cwd)?;

            let byte_count = tool_input
                .get("content")
                .and_then(|v| v.as_str())
                .map(|s| s.len())
                .unwrap_or(0);
            let content = format!("Wrote {rel_path}: {byte_count} bytes");

            Some(PostToolUseResult {
                session_id: payload.session_id.clone(),
                file_path: Some(rel_path),
                content,
                kind: "file_change",
            })
        }
        "Bash" => {
            let tool_input = payload.tool_input.as_ref()?;
            let command = tool_input.get("command")?.as_str()?;
            // Truncate to first 120 chars of command (char boundary safe)
            let truncated = truncate_to_chars(command, 120);
            let content = format!("Ran command: {truncated}");

            Some(PostToolUseResult {
                session_id: payload.session_id.clone(),
                file_path: None,
                content,
                kind: "tool_call",
            })
        }
        _ => None,
    }
}

/// Convert an absolute file path to a path relative to `cwd`.
/// Returns `None` if the path is not under `cwd` (file outside project root).
fn relativize_path(abs_path: &str, cwd: &str) -> Option<String> {
    let abs = Path::new(abs_path);
    let base = Path::new(cwd);
    abs.strip_prefix(base)
        .ok()
        .and_then(|rel| rel.to_str())
        .map(|s| s.to_string())
}

/// Truncate `s` to at most `max_chars` Unicode scalar values.
fn truncate_to_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_payload(tool_name: Option<&str>, tool_input: Option<serde_json::Value>) -> HookPayload {
        HookPayload {
            session_id: "test-session".to_string(),
            cwd: "/project".to_string(),
            hook_event_name: Some("PostToolUse".to_string()),
            tool_name: tool_name.map(|s| s.to_string()),
            tool_input,
            tool_response: None,
            tool_use_id: None,
            transcript_path: None,
            permission_mode: None,
        }
    }

    // Task 6.1: Edit tool payload
    #[test]
    fn test_parse_edit_tool() {
        let payload = make_payload(
            Some("Edit"),
            Some(json!({
                "file_path": "/project/src/main.rs",
                "old_string": "hello",
                "new_string": "world"
            })),
        );
        let result = parse_post_tool_use(&payload).unwrap();
        assert_eq!(result.kind, "file_change");
        assert_eq!(result.file_path.as_deref(), Some("src/main.rs"));
        assert_eq!(result.content, "Edited src/main.rs: replaced 5 chars");
    }

    // Task 6.2: Write tool payload
    #[test]
    fn test_parse_write_tool() {
        let payload = make_payload(
            Some("Write"),
            Some(json!({
                "file_path": "/project/src/lib.rs",
                "content": "fn main() {}"
            })),
        );
        let result = parse_post_tool_use(&payload).unwrap();
        assert_eq!(result.kind, "file_change");
        assert_eq!(result.file_path.as_deref(), Some("src/lib.rs"));
        assert_eq!(result.content, "Wrote src/lib.rs: 12 bytes");
    }

    // Task 6.3: Bash tool payload
    #[test]
    fn test_parse_bash_tool() {
        let payload = make_payload(
            Some("Bash"),
            Some(json!({ "command": "cargo test" })),
        );
        let result = parse_post_tool_use(&payload).unwrap();
        assert_eq!(result.kind, "tool_call");
        assert!(result.file_path.is_none());
        assert_eq!(result.content, "Ran command: cargo test");
    }

    // Task 6.3: Bash command truncation at 120 chars
    #[test]
    fn test_parse_bash_tool_truncates_long_command() {
        let long_cmd = "x".repeat(200);
        let payload = make_payload(
            Some("Bash"),
            Some(json!({ "command": long_cmd })),
        );
        let result = parse_post_tool_use(&payload).unwrap();
        // content = "Ran command: " + 120 x's
        assert_eq!(result.content, format!("Ran command: {}", "x".repeat(120)));
    }

    // Task 6.4: Unknown tool name returns None
    #[test]
    fn test_parse_unknown_tool_returns_none() {
        let payload = make_payload(Some("Read"), Some(json!({ "file_path": "/project/a.rs" })));
        assert!(parse_post_tool_use(&payload).is_none());
    }

    // Task 6.5: Missing tool_name → deserializes and parse returns None
    #[test]
    fn test_parse_missing_tool_name_returns_none() {
        let json_str = r#"{"session_id":"s1","cwd":"/project"}"#;
        let payload: HookPayload = serde_json::from_str(json_str).expect("must deserialize");
        assert!(payload.tool_name.is_none());
        assert!(parse_post_tool_use(&payload).is_none());
    }

    // Task 6.6: Missing tool_input → deserializes and parse returns None
    #[test]
    fn test_parse_missing_tool_input_returns_none() {
        let payload = make_payload(Some("Edit"), None);
        assert!(parse_post_tool_use(&payload).is_none());
    }

    // Task 6.7: Absolute to relative path conversion
    #[test]
    fn test_file_path_relativization() {
        let payload = HookPayload {
            session_id: "s1".to_string(),
            cwd: "/home/user/project".to_string(),
            hook_event_name: None,
            tool_name: Some("Write".to_string()),
            tool_input: Some(json!({
                "file_path": "/home/user/project/src/auth.rs",
                "content": "hello"
            })),
            tool_response: None,
            tool_use_id: None,
            transcript_path: None,
            permission_mode: None,
        };
        let result = parse_post_tool_use(&payload).unwrap();
        assert_eq!(result.file_path.as_deref(), Some("src/auth.rs"));
    }

    // Task 6.8: file_path outside cwd returns None
    #[test]
    fn test_file_path_outside_cwd_returns_none() {
        let payload = HookPayload {
            session_id: "s1".to_string(),
            cwd: "/home/user/project".to_string(),
            hook_event_name: None,
            tool_name: Some("Edit".to_string()),
            tool_input: Some(json!({
                "file_path": "/etc/passwd",
                "old_string": "x",
                "new_string": "y"
            })),
            tool_response: None,
            tool_use_id: None,
            transcript_path: None,
            permission_mode: None,
        };
        assert!(parse_post_tool_use(&payload).is_none());
    }

    // Task 6.9: Extra unknown fields deserialize successfully
    #[test]
    fn test_extra_unknown_fields_deserialize_ok() {
        let json_str = r#"{
            "session_id": "s1",
            "cwd": "/project",
            "tool_name": "Bash",
            "tool_input": {"command": "ls"},
            "unknown_future_field": "ignored",
            "another_field": 42
        }"#;
        let payload: HookPayload = serde_json::from_str(json_str).expect("must deserialize with unknown fields");
        assert_eq!(payload.tool_name.as_deref(), Some("Bash"));
    }

    fn make_diff(
        added: Vec<&str>,
        sig_changed: Vec<(&str, &str, &str)>,
        removed: Vec<&str>,
        body_only: Vec<&str>,
    ) -> StructuralDiff {
        StructuralDiff {
            file_path: "src/auth.rs".into(),
            added: added.into_iter().map(|s| s.to_string()).collect(),
            signature_changed: sig_changed
                .into_iter()
                .map(|(f, o, n)| (f.to_string(), o.to_string(), n.to_string()))
                .collect(),
            removed: removed.into_iter().map(|s| s.to_string()).collect(),
            body_only: body_only.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn format_body_only_returns_none() {
        let diff = make_diff(vec![], vec![], vec![], vec!["src/auth.rs::foo"]);
        assert!(format_structural_observation(&diff).is_none());
    }

    #[test]
    fn format_added_contains_added_text() {
        let diff = make_diff(vec!["src/auth.rs::foo"], vec![], vec![], vec![]);
        let result = format_structural_observation(&diff).unwrap();
        assert!(result.contains("added `foo`"), "got: {result}");
    }

    #[test]
    fn format_sig_changed_contains_both_sigs() {
        let diff = make_diff(
            vec![],
            vec![("src/auth.rs::foo", "fn foo()", "fn foo(x: i32)")],
            vec![],
            vec![],
        );
        let result = format_structural_observation(&diff).unwrap();
        assert!(result.contains("fn foo()"), "got: {result}");
        assert!(result.contains("fn foo(x: i32)"), "got: {result}");
    }

    #[test]
    fn format_removed_contains_removed_text() {
        let diff = make_diff(vec![], vec![], vec!["src/auth.rs::bar"], vec![]);
        let result = format_structural_observation(&diff).unwrap();
        assert!(result.contains("removed `bar`"), "got: {result}");
    }

    #[test]
    fn format_size_cap_at_five() {
        let added: Vec<&str> = vec![
            "f.rs::a1", "f.rs::a2", "f.rs::a3", "f.rs::a4", "f.rs::a5", "f.rs::a6", "f.rs::a7",
        ];
        let diff = make_diff(added, vec![], vec![], vec![]);
        let result = format_structural_observation(&diff).unwrap();
        assert!(result.contains("and 2 more"), "got: {result}");
    }

    #[test]
    fn format_empty_diff_returns_none() {
        let diff = make_diff(vec![], vec![], vec![], vec![]);
        assert!(format_structural_observation(&diff).is_none());
    }
}
