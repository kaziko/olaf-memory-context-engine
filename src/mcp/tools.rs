use std::collections::HashSet;
use std::path::Path;
use regex::Regex;
use rusqlite::OptionalExtension;
use serde_json::Value;

/// Error types for tool dispatch — maps to MCP error codes in server.rs.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ToolError {
    /// Tool name unknown → MCP -32601
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    /// Required parameter missing or wrong type → MCP -32602
    #[error("invalid params: {0}")]
    InvalidParams(String),
    /// Any other internal failure → MCP -32603
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Returns the list of available tool definitions.
pub(crate) fn list() -> Vec<Value> {
    vec![
        serde_json::json!({
            "name": "get_brief",
            "description": "Get a context brief for any task. Runs context retrieval automatically; includes impact analysis when symbol_fqn is provided. Start here — use get_context or get_impact only when you need fine-grained control.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Natural language description of the task, e.g. 'fix bug in auth module'"
                    },
                    "file_hints": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional file paths or partial paths to prioritize as pivot symbols"
                    },
                    "token_budget": {
                        "type": "integer",
                        "description": "Maximum tokens for the combined response (default: 4000)"
                    },
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Optional: FQN of primary symbol for impact analysis (e.g. 'src/auth.rs::authenticate'). Omit to skip impact graph."
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Impact traversal depth (default: 3, max: 10)"
                    }
                },
                "required": ["intent"]
            }
        }),
        serde_json::json!({
            "name": "get_context",
            "description": "Token-budgeted context brief for a given intent. For most tasks, prefer get_brief which wraps this with optional impact analysis. Use get_context when you need context-only retrieval.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Natural language description of what you want to do, e.g. 'fix bug in auth module'"
                    },
                    "file_hints": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional file paths or partial paths to prioritize as pivot symbols"
                    },
                    "token_budget": {
                        "type": "integer",
                        "description": "Maximum tokens for the response (default: 4000)"
                    }
                },
                "required": ["intent"]
            }
        }),
        serde_json::json!({
            "name": "get_impact",
            "description": "Find symbols that call, extend, implement, or use a given symbol FQN as a type. Import relationships are not yet tracked at symbol level. For combined context+impact, use get_brief with symbol_fqn. Use get_impact when you already have a specific symbol and want only its dependents.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Fully qualified name, e.g. 'src/auth.ts::AuthService::login'"
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Levels of dependents to traverse (default: 3)"
                    }
                },
                "required": ["symbol_fqn"]
            }
        }),
        serde_json::json!({
            "name": "analyze_failure",
            "description": "Parse a stack trace, error message, or test failure output and return a context brief focused on the failure path. Extracts file paths, line numbers, and symbols from the trace to seed precise pivots in bug-fix mode, and surfaces prior failure observations. Use when a test fails or a runtime error occurs — pass the raw output directly.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "trace": { "type": "string", "description": "Raw stack trace, error message, or test failure output" },
                    "token_budget": { "type": "integer", "description": "Max tokens for context brief (default 10000)" }
                },
                "required": ["trace"]
            }
        }),
        serde_json::json!({
            "name": "get_file_skeleton",
            "description": "Get all symbol signatures, docstrings, and dependency edges for a file — no implementation bodies. Accepts exact or partial file paths.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "File path or partial path, e.g. 'src/auth.ts' or 'auth.ts'"
                    }
                },
                "required": ["file_path"]
            }
        }),
        serde_json::json!({
            "name": "trace_flow",
            "description": "Find execution paths between two symbols in the call graph. Traverses calls/extends/implements edges. Returns shortest paths up to max_paths.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_fqn": { "type": "string", "description": "FQN of the starting symbol, e.g. 'src/auth.rs::login'" },
                    "target_fqn": { "type": "string", "description": "FQN of the destination symbol, e.g. 'src/db.rs::query'" },
                    "max_paths":  { "type": "integer", "description": "Maximum paths to return (default: 5, max: 20)" }
                },
                "required": ["source_fqn", "target_fqn"]
            }
        }),
        serde_json::json!({
            "name": "index_status",
            "description": "Get index health: file count, symbol count, edge count, observation count, last indexed timestamp.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
        serde_json::json!({
            "name": "save_observation",
            "description": "Save an observation (insight, decision, error, etc.) linked to a symbol FQN or file path. Persists to session memory for retrieval in future sessions. At least one of symbol_fqn or file_path is required.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Plain English description of the observation"
                    },
                    "kind": {
                        "type": "string",
                        "description": "One of: insight, decision, error, tool_call, file_change, anti_pattern"
                    },
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Optional: symbol FQN to link this observation to, e.g. 'src/auth.ts::AuthService::login'"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Optional: file path to link this observation to, e.g. 'src/auth.ts'"
                    }
                },
                "required": ["content", "kind"]
            }
        }),
        serde_json::json!({
            "name": "get_session_history",
            "description": "Get observations and changes from recent sessions, optionally filtered by file or symbol.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Optional: filter to observations linked to this file path"
                    },
                    "symbol_fqn": {
                        "type": "string",
                        "description": "Optional: filter to observations linked to this symbol FQN (e.g. 'src/auth.ts::AuthService::login')"
                    },
                    "sessions_back": {
                        "type": "integer",
                        "description": "How many recent sessions to include (default: 5, max: 50)"
                    },
                    "sort_mode": {
                        "type": "string",
                        "description": "Presentation mode: 'session' (default, grouped by session) or 'relevance' (flat ranked by relevance score)",
                        "enum": ["session", "relevance"]
                    }
                }
            }
        }),
        serde_json::json!({
            "name": "list_restore_points",
            "description": "List available pre-edit snapshots for a file, sorted newest-first. Returns snapshot IDs to use with undo_change.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Project-relative or absolute path to the file"
                    }
                },
                "required": ["file_path"]
            }
        }),
        serde_json::json!({
            "name": "undo_change",
            "description": "Restore a file to a specific pre-edit snapshot using a snapshot ID from list_restore_points. Writes a decision observation recording the revert.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Project-relative or absolute path to the file to restore"
                    },
                    "snapshot_id": {
                        "type": "string",
                        "description": "Snapshot ID from list_restore_points output (e.g. '1740000000000-12345-7')"
                    }
                },
                "required": ["file_path", "snapshot_id"]
            }
        }),
    ]
}

/// Dispatches a tools/call request to the appropriate handler.
/// Returns the tool's text response (server.rs wraps it in MCP content format).
pub(crate) fn dispatch(conn: &mut rusqlite::Connection, project_root: &Path, session_id: &str, params: Option<&Value>) -> Result<String, ToolError> {
    let tool_name = params
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: name".to_string()))?;

    let args = params.and_then(|p| p.get("arguments"));
    match tool_name {
        "analyze_failure"  => handle_analyze_failure(conn, project_root, session_id, args),
        "get_context"      => handle_get_context(conn, project_root, args),
        "get_impact"       => handle_get_impact(conn, args),
        "get_file_skeleton" => handle_get_file_skeleton(conn, args),
        "index_status"     => handle_index_status(conn),
        "save_observation"     => handle_save_observation(conn, session_id, args),
        "get_session_history"  => handle_get_session_history(conn, args),
        "list_restore_points"  => handle_list_restore_points(project_root, args),
        "undo_change"          => handle_undo_change(conn, project_root, session_id, args),
        "get_brief"            => handle_get_brief(conn, project_root, args),
        "trace_flow"           => handle_trace_flow(conn, args),
        _ => Err(ToolError::UnknownTool(tool_name.to_string())),
    }
}

/// Normalize a file path for MCP handlers: converts to project-relative and rejects escapes.
fn mcp_normalize(project_root: &Path, file_path: &str) -> Result<String, ToolError> {
    crate::restore::normalize_rel_path(project_root, file_path)
        .map_err(|e| ToolError::InvalidParams(e.to_string()))
}

// ─── analyze_failure data structures ──────────────────────────────────────────

#[cfg_attr(test, derive(Debug))]
struct TraceFrame {
    file_path: Option<String>,
    line: Option<u32>,
    symbol_name: Option<String>,
}

struct TraceExtraction {
    frames: Vec<TraceFrame>,
    error_summary: String,
}

struct FrameResolution {
    file_path: Option<String>,
    line: Option<u32>,
    resolved_fqn: Option<String>,
    tier: &'static str,
}

struct PivotResolution {
    pivot_fqns: Vec<String>,
    frame_details: Vec<FrameResolution>,
}

// ─── trace parsing ───────────────────────────────────────────────────────────

fn parse_trace(raw: &str, project_root: &Path) -> TraceExtraction {
    let error_summary = raw.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .chars().take(200).collect::<String>();

    let mut frames: Vec<TraceFrame> = Vec::new();
    let is_python = raw.contains("most recent call last") || raw.contains("Traceback");

    // File path + line patterns
    let re_rust = Regex::new(r"at\s+\.?/?(.+\.rs):(\d+)").unwrap();
    let re_ts = Regex::new(r"at\s+(?:\S+\s+)?\(?(.+\.[tj]sx?):(\d+):\d+\)?").unwrap();
    let re_python = Regex::new(r#"File\s+"(.+\.py)",\s+line\s+(\d+)"#).unwrap();
    let re_go = Regex::new(r"\s+(.+\.go):(\d+)\s").unwrap();
    let re_php = Regex::new(r"(.+\.php)[\(:](\d+)\)?").unwrap();
    let re_generic = Regex::new(r"([\w./\\-]+\.\w{1,4}):(\d+)").unwrap();

    // Symbol name patterns
    let re_sym_rust = Regex::new(r"(\w+(?:::\w+)*)\(\)").unwrap();
    let re_sym_ts = Regex::new(r"at\s+(\w[\w.]*)\s+\(").unwrap();
    let re_sym_python = Regex::new(r"in\s+(\w+)\s*$").unwrap();
    let re_sym_go = Regex::new(r"(\w+(?:\.\w+)*)\(\)").unwrap();

    // Assertion-style patterns
    let re_rust_test = Regex::new(r"---- (\S+) stdout ----").unwrap();
    let re_jest = Regex::new(r"FAIL\s+(.+\.[tj]sx?)").unwrap();
    let re_pytest = Regex::new(r"FAILED\s+(.+\.py)::(\w+)").unwrap();

    for line in raw.lines() {
        let trimmed = line.trim();

        // Assertion-style: Rust test
        if let Some(caps) = re_rust_test.captures(trimmed) {
            frames.push(TraceFrame {
                file_path: None,
                line: None,
                symbol_name: Some(caps[1].to_string()),
            });
            continue;
        }

        // Assertion-style: Jest
        if let Some(caps) = re_jest.captures(trimmed) {
            let raw_path = &caps[1];
            let normalized = try_normalize_path(project_root, raw_path);
            frames.push(TraceFrame {
                file_path: normalized,
                line: None,
                symbol_name: None,
            });
            continue;
        }

        // Assertion-style: pytest
        if let Some(caps) = re_pytest.captures(trimmed) {
            let raw_path = &caps[1];
            let normalized = try_normalize_path(project_root, raw_path);
            frames.push(TraceFrame {
                file_path: normalized,
                line: None,
                symbol_name: Some(caps[2].to_string()),
            });
            continue;
        }

        // File path + line extraction (try language-specific first, then generic)
        let (file_path, line_num) = try_extract_file_line(
            trimmed, project_root,
            &[&re_rust, &re_ts, &re_python, &re_go, &re_php, &re_generic],
        );

        // Symbol name extraction
        let symbol_name = try_extract_symbol(
            trimmed,
            &[&re_sym_rust, &re_sym_ts, &re_sym_python, &re_sym_go],
        );

        if file_path.is_some() || symbol_name.is_some() {
            frames.push(TraceFrame { file_path, line: line_num, symbol_name });
        }
    }

    // Drop all-None frames
    frames.retain(|f| f.file_path.is_some() || f.line.is_some() || f.symbol_name.is_some());

    // Python: reverse so failing frame comes first
    if is_python {
        frames.reverse();
    }

    // Cap at 30 frames
    frames.truncate(30);

    TraceExtraction { frames, error_summary }
}

fn try_normalize_path(project_root: &Path, raw_path: &str) -> Option<String> {
    crate::restore::normalize_rel_path(project_root, raw_path).ok()
}

fn try_extract_file_line(
    line: &str,
    project_root: &Path,
    patterns: &[&Regex],
) -> (Option<String>, Option<u32>) {
    for re in patterns {
        if let Some(caps) = re.captures(line) {
            let raw_path = &caps[1];
            let line_num = caps.get(2).and_then(|m| m.as_str().parse::<u32>().ok());
            match try_normalize_path(project_root, raw_path) {
                Some(p) => return (Some(p), line_num),
                None => return (None, None), // system/library path — don't store
            }
        }
    }
    (None, None)
}

fn try_extract_symbol(line: &str, patterns: &[&Regex]) -> Option<String> {
    for re in patterns {
        if let Some(caps) = re.captures(line) {
            return Some(caps[1].to_string());
        }
    }
    None
}

// ─── pivot resolution ────────────────────────────────────────────────────────

fn resolve_trace_pivots(conn: &rusqlite::Connection, extraction: &TraceExtraction) -> Result<PivotResolution, ToolError> {
    let mut pivot_fqns: Vec<String> = Vec::new();
    let mut seen_fqns: HashSet<String> = HashSet::new();
    let mut frame_details: Vec<FrameResolution> = Vec::new();

    for frame in &extraction.frames {
        let mut resolved_fqn: Option<String> = None;
        let mut tier: &str = "unresolved";

        // Tier 1 — Line-precise
        if let (Some(path), Some(line)) = (&frame.file_path, frame.line) {
            match crate::graph::store::lookup_symbol_at_line(conn, path, line) {
                Ok(Some(fqn)) => { resolved_fqn = Some(fqn); tier = "line"; }
                Ok(None) => {}
                Err(e) => return Err(ToolError::Internal(anyhow::anyhow!("Tier 1 lookup failed: {e}"))),
            }
        }

        // Tier 2 — Nearest-symbol fallback (file-based)
        if resolved_fqn.is_none()
            && let Some(path) = &frame.file_path
        {
            let line_val = frame.line.unwrap_or(0) as i64;
            let mut stmt = conn.prepare(
                "SELECT s.fqn FROM symbols s \
                 JOIN files f ON s.file_id = f.id \
                 WHERE f.path = ?1 \
                 ORDER BY MIN(ABS(s.start_line - ?2), ABS(s.end_line - ?2)) ASC, \
                          s.start_line ASC, s.id ASC \
                 LIMIT 5"
            ).map_err(|e| ToolError::Internal(anyhow::anyhow!("Tier 2 prepare failed: {e}")))?;
            let fqns: Vec<String> = stmt
                .query_map(rusqlite::params![path, line_val], |r| r.get::<_, String>(0))
                .map_err(|e| ToolError::Internal(anyhow::anyhow!("Tier 2 query failed: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| ToolError::Internal(anyhow::anyhow!("Tier 2 row read failed: {e}")))?;

            if let Some(fqn) = fqns.into_iter().next() {
                resolved_fqn = Some(fqn);
                tier = "file";
            }
        }

        // Tier 3 — Symbol name search
        if resolved_fqn.is_none()
            && let Some(name) = &frame.symbol_name
            && name.len() >= 4
        {
            let mut stmt = conn.prepare(
                "SELECT fqn FROM symbols WHERE name = ?1 LIMIT 2"
            ).map_err(|e| ToolError::Internal(anyhow::anyhow!("Tier 3 prepare failed: {e}")))?;
            let fqns: Vec<String> = stmt
                .query_map(rusqlite::params![name], |r| r.get::<_, String>(0))
                .map_err(|e| ToolError::Internal(anyhow::anyhow!("Tier 3 query failed: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| ToolError::Internal(anyhow::anyhow!("Tier 3 row read failed: {e}")))?;

            if fqns.len() == 1 {
                resolved_fqn = Some(fqns.into_iter().next().unwrap());
                tier = "name";
            }
        }

        if let Some(fqn) = &resolved_fqn
            && seen_fqns.insert(fqn.clone())
        {
            pivot_fqns.push(fqn.clone());
        }

        frame_details.push(FrameResolution {
            file_path: frame.file_path.clone(),
            line: frame.line,
            resolved_fqn,
            tier,
        });
    }

    // Cap pivots at 20
    pivot_fqns.truncate(20);

    Ok(PivotResolution { pivot_fqns, frame_details })
}

// ─── trace sanitization ─────────────────────────────────────────────────────

fn sanitize_trace_output(raw: &str) -> String {
    let lines: Vec<&str> = raw.lines().collect();

    // Head/tail truncation
    let truncated = if lines.len() > 100 {
        let head = &lines[..80];
        let tail = &lines[lines.len() - 20..];
        let omitted = lines.len() - 100;
        let mut result: Vec<String> = head.iter().map(|l| l.to_string()).collect();
        result.push(format!("... ({omitted} lines omitted) ..."));
        result.extend(tail.iter().map(|l| l.to_string()));
        result
    } else {
        lines.iter().map(|l| l.to_string()).collect()
    };

    // Sensitive line redaction patterns
    let re_env = Regex::new(r"\.env\b").unwrap();
    let re_pem = Regex::new(r"\.pem\b").unwrap();
    let re_p12 = Regex::new(r"\.p12\b").unwrap();
    let re_id_rsa = Regex::new(r"\bid_rsa\b").unwrap();
    let re_secret = Regex::new(r"(?i)(secret|password|credential|_token)\s*[=:]").unwrap();

    // Absolute path stripping
    let re_unix_path = Regex::new(r"(/(?:Users|home|var|usr|opt|tmp|root|etc|srv|mnt|private|Library|Volumes)/\S+)").unwrap();
    let re_win_path = Regex::new(r"([A-Z]:\\(?:Users\\|)\S+)").unwrap();

    let mut output_lines: Vec<String> = Vec::with_capacity(truncated.len());

    for line in &truncated {
        // Check for sensitive content
        if re_env.is_match(line) || re_pem.is_match(line) || re_p12.is_match(line)
            || re_id_rsa.is_match(line) || re_secret.is_match(line)
        {
            output_lines.push("[redacted: sensitive content]".to_string());
            continue;
        }

        // Strip absolute paths
        let mut sanitized = re_unix_path.replace_all(line, |caps: &regex::Captures| {
            let full = &caps[1];
            let filename = full.rsplit('/').next().unwrap_or(full);
            format!("<abs>/{filename}")
        }).to_string();

        sanitized = re_win_path.replace_all(&sanitized, |caps: &regex::Captures| {
            let full = &caps[1];
            let filename = full.rsplit('\\').next().unwrap_or(full);
            format!("<abs>\\{filename}")
        }).to_string();

        output_lines.push(sanitized);
    }

    output_lines.join("\n")
}

// ─── frame resolution table ─────────────────────────────────────────────────

fn format_frame_table(details: &[FrameResolution]) -> String {
    let mut table = String::from("### Frame Resolution\n| # | Path | Line | Resolved Symbol | Method |\n|-|-|-|-|-|\n");
    for (i, fr) in details.iter().enumerate() {
        let path = fr.file_path.as_deref().unwrap_or("\u{2014}");
        let line = fr.line.map(|l| l.to_string()).unwrap_or_else(|| "\u{2014}".to_string());
        let fqn = fr.resolved_fqn.as_deref().unwrap_or("\u{2014}");
        let tier = fr.tier;
        table.push_str(&format!("| {} | {} | {} | {} | {} |\n", i + 1, path, line, fqn, tier));
    }
    table
}

// ─── analyze_failure handler ─────────────────────────────────────────────────

fn handle_analyze_failure(
    conn: &mut rusqlite::Connection,
    project_root: &Path,
    _session_id: &str,
    args: Option<&serde_json::Value>,
) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let trace = args.get("trace").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing 'trace'".into()))?;
    if trace.trim().is_empty() {
        return Err(ToolError::InvalidParams("trace is empty".into()));
    }
    let token_budget = args.get("token_budget")
        .and_then(|v| v.as_u64())
        .map(|v| v.clamp(100, 1_000_000) as usize)
        .unwrap_or(10_000);

    crate::index::run_incremental(conn, project_root)?;

    let extraction = parse_trace(trace, project_root);
    let resolution = resolve_trace_pivots(conn, &extraction)?;

    let mut output = String::new();

    if !resolution.pivot_fqns.is_empty() {
        // Path A: Pivots resolved from trace
        let intent = format!("fix error: {}", extraction.error_summary);
        let brief = crate::graph::query::get_context_with_pivots(
            conn, project_root, &intent, &resolution.pivot_fqns, token_budget
        ).map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

        output.push_str("## Failure Analysis\n\n");
        output.push_str(&format!("**Error:** {}\n\n", extraction.error_summary));
        if !resolution.frame_details.is_empty() {
            output.push_str(&format_frame_table(&resolution.frame_details));
            output.push('\n');
        }
        output.push_str(&brief);
    } else {
        // Path B: Keyword fallback
        let stop_words: HashSet<&str> = [
            "error", "failed", "fatal", "panic", "exception",
            "thrown", "undefined", "null", "none", "true", "false", "with", "from",
            "that", "this", "have", "been", "were", "will", "would", "could", "should",
        ].into_iter().collect();

        let mut keywords: Vec<String> = Vec::new();
        let mut seen_kw: HashSet<String> = HashSet::new();
        for word in trace.split(|c: char| c.is_whitespace() || c.is_ascii_punctuation()) {
            if word.len() >= 4 {
                let lower = word.to_lowercase();
                if !stop_words.contains(lower.as_str()) && seen_kw.insert(lower.clone()) {
                    keywords.push(lower);
                    if keywords.len() >= 10 { break; }
                }
            }
        }

        let mut keyword_pivots = Vec::new();
        if !keywords.is_empty() {
            keyword_pivots = crate::graph::query::rank_symbols_by_keywords(conn, &keywords, 5)
                .map_err(|e| ToolError::Internal(anyhow::anyhow!("keyword search failed: {e}")))?;
        }

        if !keyword_pivots.is_empty() {
            // Path B: keywords matched symbols
            let pivot_fqns: Vec<String> = keyword_pivots.into_iter().map(|(_, fqn)| fqn).collect();
            let intent = format!("fix error: {}", extraction.error_summary);
            let brief = crate::graph::query::get_context_with_pivots(
                conn, project_root, &intent, &pivot_fqns, token_budget
            ).map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

            output.push_str("## Failure Analysis\n\n");
            output.push_str(&format!("**Error:** {}\n\n", extraction.error_summary));
            if !resolution.frame_details.is_empty() {
                output.push_str(&format_frame_table(&resolution.frame_details));
                output.push('\n');
            }
            output.push_str(&brief);
        } else {
            // Path C: No pivots, no keywords
            output.push_str("## Failure Analysis\n\n");
            output.push_str(&format!("**Error:** {}\n", extraction.error_summary));
            output.push_str("Could not resolve input to indexed code.\n");
            output.push_str("This may be due to unsupported trace formats, ambiguous symbol names,\n");
            output.push_str("unindexed code, or input without parseable structure.\n");
            output.push_str("The raw input is preserved below.\n\n");
            if !resolution.frame_details.is_empty() {
                output.push_str(&format_frame_table(&resolution.frame_details));
                output.push('\n');
            }
            output.push_str("### Raw Trace\n```\n");
            output.push_str(&sanitize_trace_output(trace));
            output.push_str("\n```\n");
        }
    }

    truncate_to_budget(&mut output, token_budget);
    Ok(output)
}

fn handle_get_context(conn: &mut rusqlite::Connection, project_root: &Path, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);
    let intent = args.get("intent").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: intent".to_string()))?;
    let file_hints: Vec<String> = args.get("file_hints").and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    let token_budget = args.get("token_budget").and_then(|v| v.as_u64()).map(|v| v as usize).unwrap_or(4000);

    crate::index::run_incremental(conn, project_root)?;

    crate::graph::query::get_context(conn, project_root, intent, &file_hints, token_budget)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

fn handle_get_impact(conn: &rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);
    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: symbol_fqn".to_string()))?;
    let depth = args.get("depth").and_then(|v| v.as_u64()).map(|v| v as usize).unwrap_or(3);

    crate::graph::query::get_impact(conn, symbol_fqn, depth)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

fn handle_get_file_skeleton(conn: &rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);
    let file_path = args.get("file_path").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: file_path".to_string()))?;
    let file_path = file_path.trim();
    if file_path.is_empty() {
        return Err(ToolError::InvalidParams("file_path must not be empty".to_string()));
    }
    crate::graph::query::get_file_skeleton(conn, file_path)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

fn handle_index_status(conn: &rusqlite::Connection) -> Result<String, ToolError> {
    crate::graph::query::index_status(conn)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))
}

const VALID_KINDS: &[&str] = &["insight", "decision", "error", "tool_call", "file_change", "anti_pattern"];

fn handle_save_observation(conn: &mut rusqlite::Connection, session_id: &str, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let content = args.get("content").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: content".into()))?;
    let kind = args.get("kind").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: kind".into()))?;

    if !VALID_KINDS.contains(&kind) {
        return Err(ToolError::InvalidParams(
            format!("invalid kind '{kind}'; must be one of: insight, decision, error, tool_call, file_change, anti_pattern")
        ));
    }

    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());
    let file_path  = args.get("file_path").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());

    // NFR7: reject observations linked to sensitive files
    if let Some(fp) = file_path
        && crate::memory::store::is_sensitive_path(fp)
    {
        return Err(ToolError::InvalidParams(
            "file_path refers to a sensitive file — observation rejected per NFR7".into(),
        ));
    }
    if let Some(fqn) = symbol_fqn
        && let Some(prefix) = fqn.split("::").next()
        && crate::memory::store::is_sensitive_path(prefix)
    {
        return Err(ToolError::InvalidParams(
            "symbol_fqn refers to a sensitive file — observation rejected per NFR7".into(),
        ));
    }

    if symbol_fqn.is_none() && file_path.is_none() {
        return Err(ToolError::InvalidParams(
            "at least one of symbol_fqn or file_path is required".into()
        ));
    }

    crate::memory::store::upsert_session(conn, session_id, "claude-code")
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;
    let id = crate::memory::store::insert_observation(conn, session_id, kind, content, symbol_fqn, file_path)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    Ok(format!("Observation saved (id={id}, kind={kind})."))
}

fn handle_get_session_history(conn: &mut rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let sessions_back = args.get("sessions_back")
        .and_then(|v| v.as_i64())
        .map(|v| (v.clamp(1, 50)) as usize)
        .unwrap_or(5);

    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());
    let file_path = args.get("file_path").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty());

    let sort_mode = args.get("sort_mode").and_then(|v| v.as_str()).unwrap_or("session");
    if sort_mode != "session" && sort_mode != "relevance" {
        return Err(ToolError::InvalidParams(
            format!("Invalid sort_mode '{}'. Must be 'session' or 'relevance'.", sort_mode),
        ));
    }

    let session_ids = crate::memory::store::get_recent_session_ids(conn, sessions_back)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    if session_ids.is_empty() {
        return Ok("No sessions found.".into());
    }

    let observations = crate::memory::store::get_observations_filtered(conn, &session_ids, symbol_fqn, file_path)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    if observations.is_empty() {
        return Ok("No observations found matching the given filters.".into());
    }

    let total = observations.len();
    let scored = crate::memory::store::score_observations(observations);
    let capped = total.min(200);

    match sort_mode {
        "relevance" => format_relevance_mode(&scored, capped, total),
        _ => format_session_mode(conn, &scored, capped, total, &session_ids),
    }
}

fn format_session_mode(
    conn: &rusqlite::Connection,
    scored: &[crate::memory::store::ScoredObservation],
    capped: usize,
    total: usize,
    session_ids: &[String],
) -> Result<String, ToolError> {
    let mut session_order: Vec<String> = Vec::new();
    let mut by_session: std::collections::HashMap<String, Vec<&crate::memory::store::ScoredObservation>> =
        std::collections::HashMap::new();

    for so in scored.iter().take(capped) {
        if !by_session.contains_key(&so.obs.session_id) {
            session_order.push(so.obs.session_id.clone());
        }
        by_session.entry(so.obs.session_id.clone()).or_default().push(so);
    }

    let session_timestamps: std::collections::HashMap<String, i64> = session_ids.iter().filter_map(|sid| {
        conn.query_row(
            "SELECT started_at FROM sessions WHERE id = ?1",
            rusqlite::params![sid],
            |r| r.get(0),
        ).ok().map(|ts: i64| (sid.clone(), ts))
    }).collect();

    let mut output = format!("# Session History (last {} sessions)\n", session_ids.len());
    let mut stale_count = 0usize;
    let mut min_score = f64::MAX;
    let mut max_score = f64::MIN;

    for sid in &session_order {
        let ts = session_timestamps.get(sid).copied().unwrap_or(0);
        let dt = chrono::DateTime::from_timestamp(ts, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "unknown".into());

        output.push_str(&format!("\n## Session {} ({})\n\n", sid, dt));

        let obs_list = &by_session[sid];
        for so in obs_list {
            min_score = min_score.min(so.relevance_score);
            max_score = max_score.max(so.relevance_score);

            if so.obs.is_stale {
                stale_count += 1;
                let reason = so.obs.stale_reason.as_deref().unwrap_or("unknown reason");
                output.push_str(&format!(
                    "- \u{26a0} [STALE \u{2014} {}] [{}] [score: {:.2}] {}\n",
                    reason, so.obs.kind, so.relevance_score, so.obs.content
                ));
            } else {
                output.push_str(&format!(
                    "- [{}] [score: {:.2}] {}\n",
                    so.obs.kind, so.relevance_score, so.obs.content
                ));
            }
            if let Some(fqn) = &so.obs.symbol_fqn {
                output.push_str(&format!("  Symbol: {}\n", fqn));
            }
            if let Some(fp) = &so.obs.file_path {
                output.push_str(&format!("  File: {}\n", fp));
            }
        }
    }

    let stale_suffix = if stale_count > 0 {
        format!(" ({} stale)", stale_count)
    } else {
        String::new()
    };
    output.push_str(&format!(
        "\n{} sessions, {} observations{}\n",
        session_order.len(),
        capped,
        stale_suffix
    ));

    if min_score <= max_score {
        output.push_str(&format!("Relevance: {:.2}\u{2013}{:.2}\n", min_score, max_score));
    }

    if total > 200 {
        output.push_str(&format!(
            "\n(Showing 200 of {} observations. Use filters to narrow results.)\n",
            total
        ));
    }

    Ok(output)
}

fn format_relevance_mode(
    scored: &[crate::memory::store::ScoredObservation],
    capped: usize,
    total: usize,
) -> Result<String, ToolError> {
    // Sort ALL scored observations by relevance first, THEN take top `capped`.
    // This ensures relevance mode surfaces the most relevant from the full fetched set,
    // not just the newest 200.
    let mut sorted: Vec<&crate::memory::store::ScoredObservation> = scored.iter().collect();
    sorted.sort_by(|a, b| {
        b.relevance_score.partial_cmp(&a.relevance_score).unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.obs.is_stale.cmp(&b.obs.is_stale))
            .then_with(|| b.obs.created_at.cmp(&a.obs.created_at))
    });
    sorted.truncate(capped);

    // Count actual distinct sessions in the displayed results
    let actual_sessions: std::collections::HashSet<&str> = sorted.iter()
        .map(|so| so.obs.session_id.as_str())
        .collect();
    let mut output = format!("# Session History — Relevance Ranked (from {} sessions)\n\n", actual_sessions.len());
    let mut stale_count = 0usize;
    let mut min_score = f64::MAX;
    let mut max_score = f64::MIN;

    for (i, so) in sorted.iter().enumerate() {
        min_score = min_score.min(so.relevance_score);
        max_score = max_score.max(so.relevance_score);

        if so.obs.is_stale {
            stale_count += 1;
            let reason = so.obs.stale_reason.as_deref().unwrap_or("unknown reason");
            output.push_str(&format!(
                "{}. [score: {:.2}] \u{26a0} [STALE \u{2014} {}] [{}] {}\n",
                i + 1, so.relevance_score, reason, so.obs.kind, so.obs.content
            ));
        } else {
            output.push_str(&format!(
                "{}. [score: {:.2}] [{}] {}\n",
                i + 1, so.relevance_score, so.obs.kind, so.obs.content
            ));
        }
        if let Some(fqn) = &so.obs.symbol_fqn {
            output.push_str(&format!("  Symbol: {}\n", fqn));
        }
        if let Some(fp) = &so.obs.file_path {
            output.push_str(&format!("  File: {}\n", fp));
        }
    }

    output.push_str(&format!(
        "\n{} observations{}\n",
        sorted.len(),
        if stale_count > 0 { format!(" ({} stale)", stale_count) } else { String::new() }
    ));

    if min_score <= max_score {
        output.push_str(&format!("Relevance: {:.2}\u{2013}{:.2}\n", min_score, max_score));
    }

    if total > 200 {
        output.push_str(&format!(
            "\n(Showing 200 of {} observations. Use filters to narrow results.)\n",
            total
        ));
    }

    Ok(output)
}

fn handle_list_restore_points(project_root: &Path, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let file_path = args.get("file_path").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: file_path".into()))?;

    // NFR7: reject sensitive file paths
    if crate::memory::store::is_sensitive_path(file_path) {
        return Err(ToolError::InvalidParams("sensitive file path rejected per NFR7".into()));
    }

    let rel = mcp_normalize(project_root, file_path)?;

    let points = crate::restore::list_restore_points(project_root, &rel)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    if points.is_empty() {
        return Ok(format!("No restore points available for {rel}"));
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let mut output = format!("Restore points for {} ({} available):\n", rel, points.len());
    for point in &points {
        let age = relative_age_ms(point.millis, now_ms);
        output.push_str(&format!("  {}  {} bytes  {}\n", point.id, point.size, age));
    }
    Ok(output)
}

fn handle_undo_change(conn: &mut rusqlite::Connection, project_root: &Path, session_id: &str, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let file_path = args.get("file_path").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: file_path".into()))?;
    let snapshot_id = args.get("snapshot_id").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: snapshot_id".into()))?;

    // NFR7: reject sensitive file paths
    if crate::memory::store::is_sensitive_path(file_path) {
        return Err(ToolError::InvalidParams("sensitive file path rejected per NFR7".into()));
    }

    let rel = mcp_normalize(project_root, file_path)?;

    crate::restore::restore_to_snapshot(project_root, &rel, snapshot_id)
        .map_err(|e| match e {
            crate::restore::RestoreError::SnapshotNotFound(id, available) =>
                ToolError::InvalidParams(format!("Snapshot '{id}' not found. Available: {available}")),
            crate::restore::RestoreError::PathOutsideRoot(p) =>
                ToolError::InvalidParams(format!("Invalid snapshot_id: {p}")),
            other => ToolError::Internal(anyhow::anyhow!("{other}")),
        })?;

    // Write a persistent decision observation (non-auto, survives compression)
    crate::memory::store::upsert_session(conn, session_id, "claude-code")
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;
    crate::memory::store::insert_observation(
        conn, session_id, "decision",
        &format!("Reverted {} — restore point {} applied", rel, snapshot_id),
        None, Some(&rel),
    ).map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    Ok(format!("Restored {} to snapshot {}.", rel, snapshot_id))
}

/// Maximum token budget accepted from callers. Prevents u64→usize cast overflow and
/// keeps `max_bytes = budget * 4` well within usize range on 32-bit targets (4 GB limit).
const MAX_TOKEN_BUDGET: u64 = 1_000_000;

fn handle_get_brief(
    conn: &mut rusqlite::Connection,
    project_root: &Path,
    args: Option<&Value>,
) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    // 2.1 Parse args
    let intent = args.get("intent").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: intent".to_string()))?;
    let file_hints: Vec<String> = args.get("file_hints").and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    let token_budget = args.get("token_budget").and_then(|v| v.as_u64())
        .map(|v| v.min(MAX_TOKEN_BUDGET) as usize)
        .unwrap_or(4000)
        .max(100);
    let symbol_fqn = args.get("symbol_fqn").and_then(|v| v.as_str());
    let depth = args.get("depth").and_then(|v| v.as_u64())
        .map(|v| (v as usize).clamp(1, 10))
        .unwrap_or(3);

    // 2.2 Trigger incremental re-index
    crate::index::run_incremental(conn, project_root)?;

    // 2.3 Compute context budget and call get_context
    let ctx_budget = token_budget * 80 / 100;
    let context_output = crate::graph::query::get_context(conn, project_root, intent, &file_hints, ctx_budget)
        .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    // 2.4 Build impact section
    let impact_output = if let Some(fqn) = symbol_fqn {
        crate::graph::query::get_impact(conn, fqn, depth)
            .map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?
    } else {
        "No primary symbol specified — provide symbol_fqn for impact analysis.\n".to_string()
    };

    // 2.5 Assemble output (both sections already have #-level headings; use only --- as separator)
    let mut output = format!("{context_output}\n---\n{impact_output}");

    // 2.6 Hard-truncate to enforce token budget
    truncate_to_budget(&mut output, token_budget);

    // 2.7 Return
    Ok(output)
}

fn handle_trace_flow(conn: &rusqlite::Connection, args: Option<&Value>) -> Result<String, ToolError> {
    let empty = serde_json::json!({});
    let args = args.unwrap_or(&empty);

    let source_fqn = args.get("source_fqn").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: source_fqn".to_string()))?;
    let target_fqn = args.get("target_fqn").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidParams("missing required field: target_fqn".to_string()))?;
    let max_paths = args.get("max_paths").and_then(|v| v.as_u64())
        .map(|v| (v as usize).clamp(1, crate::graph::trace::MAX_PATHS_LIMIT))
        .unwrap_or(crate::graph::trace::MAX_PATHS_DEFAULT);

    let source_id: Option<i64> = conn.query_row(
        "SELECT id FROM symbols WHERE fqn = ?1",
        rusqlite::params![source_fqn],
        |r| r.get(0),
    ).optional().map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    let source_id = match source_id {
        Some(id) => id,
        None => return Ok(format!("Symbol not found: {source_fqn}\n\nRun 'olaf index' first.")),
    };

    let target_id: Option<i64> = conn.query_row(
        "SELECT id FROM symbols WHERE fqn = ?1",
        rusqlite::params![target_fqn],
        |r| r.get(0),
    ).optional().map_err(|e| ToolError::Internal(anyhow::anyhow!("{e}")))?;

    let target_id = match target_id {
        Some(id) => id,
        None => return Ok(format!("Symbol not found: {target_fqn}\n\nRun 'olaf index' first.")),
    };

    let result = crate::graph::trace::trace_flow(conn, source_id, target_id, max_paths)
        .map_err(anyhow::Error::from)?;

    Ok(crate::graph::trace::format_trace_result(source_fqn, target_fqn, &result))
}

/// Truncates `s` so that `s.len().div_ceil(4) <= token_budget` after appending the note.
/// Finds the nearest valid UTF-8 char boundary to avoid panics on multibyte chars.
fn truncate_to_budget(s: &mut String, token_budget: usize) {
    const NOTE: &str = "\n(response truncated to fit token_budget)\n";
    let max_bytes = token_budget.saturating_mul(4);
    if s.len().div_ceil(4) <= token_budget {
        return;
    }
    // Reserve space for the note so the final output stays within budget.
    let cutoff = max_bytes.saturating_sub(NOTE.len());
    // Walk back to a valid UTF-8 char boundary (s.is_char_boundary(0) is always true).
    let boundary = (0..=cutoff).rev().find(|&i| s.is_char_boundary(i)).unwrap_or(0);
    s.truncate(boundary);
    s.push_str(NOTE);
    // Postcondition: s.len().div_ceil(4) <= token_budget (NOTE.len() reserved above).
}

/// Format a millisecond timestamp as a human-readable relative age string.
fn relative_age_ms(millis: u128, now_ms: u128) -> String {
    let diff = now_ms.saturating_sub(millis);
    if diff < 60_000 {
        format!("{} seconds ago", diff / 1000)
    } else if diff < 3_600_000 {
        format!("{} minutes ago", diff / 60_000)
    } else if diff < 86_400_000 {
        format!("{} hours ago", diff / 3_600_000)
    } else {
        format!("{} days ago", diff / 86_400_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_contains_trace_flow() {
        let tools = list();
        let matches: Vec<_> = tools.iter().filter(|t| t["name"] == "trace_flow").collect();
        assert_eq!(matches.len(), 1, "list() must contain exactly one trace_flow entry");
    }

    #[test]
    fn test_trace_flow_missing_source_fqn() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let args = serde_json::json!({ "target_fqn": "src/b.rs::bar" });
        let result = handle_trace_flow(&conn, Some(&args));
        assert!(matches!(result, Err(ToolError::InvalidParams(_))));
    }

    #[test]
    fn test_trace_flow_missing_target_fqn() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let args = serde_json::json!({ "source_fqn": "src/a.rs::foo" });
        let result = handle_trace_flow(&conn, Some(&args));
        assert!(matches!(result, Err(ToolError::InvalidParams(_))));
    }

    // 3.5 — list() contains exactly one entry with name "get_brief" and it is first
    #[test]
    fn test_list_contains_get_brief() {
        let tools = list();
        let matches: Vec<_> = tools.iter().filter(|t| t["name"] == "get_brief").collect();
        assert_eq!(matches.len(), 1, "list() must contain exactly one get_brief entry");
        assert_eq!(tools[0]["name"], "get_brief", "get_brief must be the first tool in list()");
    }

    // 3.5b — descriptions contain expected hierarchy language
    #[test]
    fn test_tool_description_hierarchy() {
        let tools = list();
        let get_brief = tools.iter().find(|t| t["name"] == "get_brief").expect("get_brief missing");
        let get_context = tools.iter().find(|t| t["name"] == "get_context").expect("get_context missing");
        let get_impact = tools.iter().find(|t| t["name"] == "get_impact").expect("get_impact missing");

        let brief_desc = get_brief["description"].as_str().unwrap();
        let ctx_desc = get_context["description"].as_str().unwrap();
        let impact_desc = get_impact["description"].as_str().unwrap();

        assert!(brief_desc.contains("Start here"), "get_brief description must contain 'Start here'");
        assert!(ctx_desc.contains("prefer get_brief"), "get_context description must contain 'prefer get_brief'");
        assert!(impact_desc.contains("use get_brief with symbol_fqn"), "get_impact description must reference 'use get_brief with symbol_fqn'");
    }

    // 3.6 — truncation logic: 2000 ASCII chars (500 est-tokens), budget 100
    #[test]
    fn test_truncate_to_budget_basic() {
        let mut s = "a".repeat(2000);
        truncate_to_budget(&mut s, 100);
        assert!(s.len().div_ceil(4) <= 100, "truncated div_ceil(len/4)={} must be <= 100", s.len().div_ceil(4));
        assert!(
            s.ends_with("(response truncated to fit token_budget)\n"),
            "must end with truncation note; got: {:?}", &s[s.len().saturating_sub(50)..]
        );
    }

    // 3.7 — Unicode safety: string with multibyte chars, no panic
    #[test]
    fn test_truncate_to_budget_unicode_safety() {
        // "—" (U+2014 EM DASH) is 3 bytes in UTF-8
        let em_dash = "\u{2014}";
        // Build a string long enough to require truncation. Use budget=20 (max_bytes=80)
        // so the NOTE (42 bytes) fits. handle_get_brief always clamps to max(100) anyway.
        let mut s = em_dash.repeat(200); // 600 bytes = 150 est-tokens
        let budget = 20;
        truncate_to_budget(&mut s, budget);
        assert!(s.len().div_ceil(4) <= budget, "truncated div_ceil(len/4)={} must be <= {budget}", s.len().div_ceil(4));
    }

    // ─── Task 5.2: analyze_failure in tool list ─────────────────────────────

    #[test]
    fn test_list_contains_analyze_failure() {
        let tools = list();
        let matches: Vec<_> = tools.iter().filter(|t| t["name"] == "analyze_failure").collect();
        assert_eq!(matches.len(), 1, "list() must contain exactly one analyze_failure entry");
        let desc = matches[0]["description"].as_str().unwrap();
        assert!(desc.contains("stack trace"), "description must mention 'stack trace'");
    }

    // ─── Task 8: trace parsing unit tests ───────────────────────────────────

    fn test_project_root() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("src")).unwrap();
        tmp
    }

    #[test]
    fn parse_trace_rust_backtrace() {
        let tmp = test_project_root();
        std::fs::write(tmp.path().join("src/handlers.rs"), "").unwrap();
        let trace = "thread 'main' panicked at 'error':\n  0: process_request()\n           at ./src/handlers.rs:128\n";
        let ext = parse_trace(trace, tmp.path());
        let frame = ext.frames.iter().find(|f| f.file_path.as_deref() == Some("src/handlers.rs"));
        assert!(frame.is_some(), "must extract src/handlers.rs");
        let frame = frame.unwrap();
        assert_eq!(frame.line, Some(128));
    }

    #[test]
    fn parse_trace_typescript_error() {
        let tmp = test_project_root();
        std::fs::write(tmp.path().join("src/app.ts"), "").unwrap();
        let trace = "Error: something\n    at handleRequest (src/app.ts:10:5)\n";
        let ext = parse_trace(trace, tmp.path());
        let frame = ext.frames.iter().find(|f| f.file_path.as_deref() == Some("src/app.ts"));
        assert!(frame.is_some(), "must extract src/app.ts");
        assert_eq!(frame.unwrap().line, Some(10));
    }

    #[test]
    fn parse_trace_python_traceback() {
        let tmp = test_project_root();
        let app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("views.py"), "").unwrap();
        let trace = "Traceback (most recent call last):\n  File \"app/views.py\", line 42, in process_order\n    raise ValueError\nValueError\n";
        let ext = parse_trace(trace, tmp.path());
        let frame = ext.frames.iter().find(|f| f.file_path.as_deref() == Some("app/views.py"));
        assert!(frame.is_some(), "must extract app/views.py");
        assert_eq!(frame.unwrap().line, Some(42));
    }

    #[test]
    fn parse_trace_go_panic() {
        let tmp = test_project_root();
        let cmd_dir = tmp.path().join("cmd");
        std::fs::create_dir_all(&cmd_dir).unwrap();
        std::fs::write(cmd_dir.join("server.go"), "").unwrap();
        let trace = "goroutine 1 [running]:\nmain.ServeHTTP()\n\tcmd/server.go:42 +0x1a\n";
        let ext = parse_trace(trace, tmp.path());
        let frame = ext.frames.iter().find(|f| f.file_path.as_deref() == Some("cmd/server.go"));
        assert!(frame.is_some(), "must extract cmd/server.go");
        assert_eq!(frame.unwrap().line, Some(42));
    }

    #[test]
    fn parse_trace_assertion_failure() {
        let tmp = test_project_root();
        std::fs::write(tmp.path().join("src/app.test.ts"), "").unwrap();
        let trace = "---- test_my_function stdout ----\nFAIL src/app.test.ts\nFAILED tests/test_file.py::test_name\n";
        let ext = parse_trace(trace, tmp.path());
        assert!(ext.frames.iter().any(|f| f.symbol_name.as_deref() == Some("test_my_function")),
            "must extract Rust test name");
    }

    #[test]
    fn parse_trace_name_only_preserved() {
        let tmp = test_project_root();
        let trace = "  0: alpha_handler()\n  1: beta_processor()\n";
        let ext = parse_trace(trace, tmp.path());
        let names: Vec<_> = ext.frames.iter()
            .filter_map(|f| f.symbol_name.as_deref())
            .collect();
        assert!(names.contains(&"alpha_handler"), "must have alpha_handler");
        assert!(names.contains(&"beta_processor"), "must have beta_processor");
        assert!(ext.frames.len() >= 2, "two frames preserved (no dedup on input)");
    }

    #[test]
    fn parse_trace_system_paths_dropped() {
        let tmp = test_project_root();
        let trace = "  0: system_func()\n           at /usr/lib/libfoo.so:42\n";
        let ext = parse_trace(trace, tmp.path());
        for frame in &ext.frames {
            assert!(frame.file_path.is_none(),
                "system paths must not be stored; got: {:?}", frame.file_path);
        }
    }

    #[test]
    fn parse_trace_empty() {
        let tmp = test_project_root();
        let ext = parse_trace("", tmp.path());
        assert!(ext.frames.is_empty());
        assert!(ext.error_summary.is_empty());
    }

    #[test]
    fn parse_trace_error_summary_truncation() {
        let tmp = test_project_root();
        let long_line = "E".repeat(500);
        let ext = parse_trace(&long_line, tmp.path());
        assert_eq!(ext.error_summary.len(), 200);
    }

    #[test]
    fn parse_trace_frame_cap() {
        let tmp = test_project_root();
        let mut trace = String::new();
        for i in 0..50 {
            trace.push_str(&format!("  {i}: func_{i}()\n"));
        }
        let ext = parse_trace(&trace, tmp.path());
        assert!(ext.frames.len() <= 30, "frames must be capped at 30, got {}", ext.frames.len());
    }

    #[test]
    fn parse_trace_python_reversal() {
        let tmp = test_project_root();
        let trace = "Traceback (most recent call last):\n  first_func()\n  failing_func()\n";
        let ext = parse_trace(trace, tmp.path());
        if let Some(first) = ext.frames.first() {
            assert_eq!(first.symbol_name.as_deref(), Some("failing_func"),
                "Python: failing function must come first after reversal");
        }
    }

    // ─── Task 9: pivot resolution unit tests ────────────────────────────────

    fn open_test_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT NOT NULL, hash TEXT);
             CREATE TABLE symbols (
                 id INTEGER PRIMARY KEY, file_id INTEGER NOT NULL, fqn TEXT NOT NULL,
                 name TEXT NOT NULL, kind TEXT, start_line INTEGER NOT NULL,
                 end_line INTEGER NOT NULL, signature TEXT, docstring TEXT, source_hash TEXT
             );
             CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT);",
        ).unwrap();
        conn
    }

    fn insert_test_symbol(conn: &mut rusqlite::Connection, file_path: &str, fqn: &str, name: &str, start: i64, end: i64) {
        let tx = conn.transaction().unwrap();
        tx.execute("INSERT OR IGNORE INTO files (path, hash) VALUES (?1, 'h')", rusqlite::params![file_path]).unwrap();
        let file_id: i64 = tx.query_row("SELECT id FROM files WHERE path = ?1", rusqlite::params![file_path], |r| r.get::<_, i64>(0)).unwrap();
        tx.execute(
            "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line) VALUES (?1, ?2, ?3, 'function', ?4, ?5)",
            rusqlite::params![file_id, fqn, name, start, end],
        ).unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn resolve_pivots_tier1_line_match() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/handler.rs", "src/handler.rs::process", "process", 40, 50);
        let extraction = TraceExtraction {
            frames: vec![TraceFrame { file_path: Some("src/handler.rs".into()), line: Some(42), symbol_name: None }],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.frame_details[0].tier, "line");
        assert_eq!(res.pivot_fqns, vec!["src/handler.rs::process"]);
    }

    #[test]
    fn resolve_pivots_tier2_file_fallback() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/db.rs", "src/db.rs::query", "query", 10, 20);
        let extraction = TraceExtraction {
            frames: vec![TraceFrame { file_path: Some("src/db.rs".into()), line: Some(99), symbol_name: None }],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.frame_details[0].tier, "file");
        assert!(res.pivot_fqns.contains(&"src/db.rs::query".to_string()));
    }

    #[test]
    fn resolve_pivots_tier2_nearest_line() {
        let mut conn = open_test_db();
        for i in 0..10 {
            let start = i * 10 + 1;
            let end = start + 8;
            insert_test_symbol(&mut conn, "src/big.rs",
                &format!("src/big.rs::fn_{i}"), &format!("fn_{i}"), start, end);
        }
        // fn_4 spans 41-49, fn_5 spans 51-59. Line 50 is between them.
        // Distance to fn_4: MIN(|41-50|, |49-50|) = 1
        // Distance to fn_5: MIN(|51-50|, |59-50|) = 1
        // Tie broken by start_line ASC → fn_4 first
        let extraction = TraceExtraction {
            frames: vec![TraceFrame { file_path: Some("src/big.rs".into()), line: Some(50), symbol_name: None }],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.frame_details[0].tier, "file");
        assert_eq!(res.pivot_fqns[0], "src/big.rs::fn_4");
    }

    #[test]
    fn resolve_pivots_tier3_name_search() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/util.rs", "src/util.rs::parse_input", "parse_input", 1, 10);
        let extraction = TraceExtraction {
            frames: vec![TraceFrame { file_path: None, line: None, symbol_name: Some("parse_input".into()) }],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.frame_details[0].tier, "name");
        assert!(res.pivot_fqns.contains(&"src/util.rs::parse_input".to_string()));
    }

    #[test]
    fn resolve_pivots_tier3_skips_short_names() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/a.rs", "src/a.rs::run", "run", 1, 5);
        let extraction = TraceExtraction {
            frames: vec![TraceFrame { file_path: None, line: None, symbol_name: Some("run".into()) }],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.frame_details[0].tier, "unresolved");
    }

    #[test]
    fn resolve_pivots_tier3_skips_ambiguous() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/a.rs", "src/a.rs::process", "process", 1, 5);
        insert_test_symbol(&mut conn, "src/b.rs", "src/b.rs::process", "process", 1, 5);
        let extraction = TraceExtraction {
            frames: vec![TraceFrame { file_path: None, line: None, symbol_name: Some("process".into()) }],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.frame_details[0].tier, "unresolved");
    }

    #[test]
    fn resolve_pivots_dedup_output() {
        let mut conn = open_test_db();
        insert_test_symbol(&mut conn, "src/a.rs", "src/a.rs::handler", "handler", 10, 20);
        let extraction = TraceExtraction {
            frames: vec![
                TraceFrame { file_path: Some("src/a.rs".into()), line: Some(15), symbol_name: None },
                TraceFrame { file_path: Some("src/a.rs".into()), line: Some(15), symbol_name: None },
            ],
            error_summary: "error".into(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert_eq!(res.pivot_fqns.len(), 1, "FQN list must be deduped");
        assert_eq!(res.frame_details.len(), 2, "frame details preserves both");
    }

    #[test]
    fn resolve_pivots_empty() {
        let conn = open_test_db();
        let extraction = TraceExtraction {
            frames: vec![],
            error_summary: String::new(),
        };
        let res = resolve_trace_pivots(&conn, &extraction).unwrap();
        assert!(res.pivot_fqns.is_empty());
        assert!(res.frame_details.is_empty());
    }

    // ─── Task 10: sanitization unit tests ───────────────────────────────────

    #[test]
    fn sanitize_redacts_sensitive_lines() {
        let input = "normal line\nloading .env file\npassword = \"hunter2\"\napi_token = \"abc\"\ntoken_budget = 100\napi_token.rs:42\n";
        let output = sanitize_trace_output(input);
        assert!(output.contains("[redacted: sensitive content]"), "sensitive lines must be redacted");
        assert!(output.contains("token_budget"), "token_budget must NOT be redacted");
        assert!(output.contains("api_token.rs"), "api_token.rs must NOT be redacted");
        assert!(!output.contains("hunter2"), "password value must not appear");
    }

    #[test]
    fn sanitize_strips_absolute_unix_paths() {
        let input = "error at /var/log/app/src/main.rs:42\n";
        let output = sanitize_trace_output(input);
        assert!(output.contains("<abs>/main.rs:42"), "must strip to <abs>/filename; got: {output}");
        assert!(!output.contains("/var/log"), "raw absolute path must be stripped");
    }

    #[test]
    fn sanitize_strips_absolute_windows_paths() {
        let input = "error at C:\\code\\src\\main.rs:10\n";
        let output = sanitize_trace_output(input);
        assert!(output.contains("<abs>\\main.rs:10"), "must strip to <abs>\\filename; got: {output}");
        assert!(!output.contains("C:\\code"), "raw absolute path must be stripped");
    }

    #[test]
    fn sanitize_strips_midline_paths() {
        let input = "    at /home/user/project/src/app.ts:10:5\n  File \"/usr/lib/python/views.py\", line 42\n";
        let output = sanitize_trace_output(input);
        assert!(output.contains("<abs>/app.ts:10:5"), "must strip mid-line unix path");
        assert!(output.contains("<abs>/views.py\""), "must strip mid-line python path");
        assert!(!output.contains("/home/user"), "raw home path must be stripped");
    }

    #[test]
    fn sanitize_head_tail_truncation() {
        let mut input = String::new();
        for i in 0..200 {
            input.push_str(&format!("line {i}\n"));
        }
        let output = sanitize_trace_output(&input);
        assert!(output.contains("lines omitted"), "must have omission marker");
        let output_lines: Vec<&str> = output.lines().collect();
        // 80 head + 1 marker + 20 tail = 101
        assert_eq!(output_lines.len(), 101, "must have 101 lines; got {}", output_lines.len());
    }
}
