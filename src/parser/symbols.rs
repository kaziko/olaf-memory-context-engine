#[derive(Debug, Clone)]
pub struct Symbol {
    pub fqn: String,
    pub name: String,
    pub kind: SymbolKind,
    pub start_line: u32, // 1-based
    pub end_line: u32,   // 1-based
    pub signature: Option<String>,
    pub docstring: Option<String>,
    /// blake3 hash of this symbol's source bytes — used for staleness detection (Story 3.3)
    pub source_hash: String,
}

#[derive(Debug, Clone)]
pub struct Edge {
    /// FQN of the source symbol, or the file path for `Imports` edges.
    ///
    /// For all edge kinds except `Imports` this is a symbol FQN (must exist in the symbols
    /// table once persisted). For `Imports` edges the source is the containing file's relative
    /// path — a permitted exception because the file itself is the logical importer, not any
    /// single symbol within it. See `import_statement` handling in `parser/typescript.rs`.
    pub source_fqn: String,
    /// FQN or module path of the target (may be unresolved — see Edge Contract in Dev Notes)
    pub target_fqn: String,
    pub kind: EdgeKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SymbolKind {
    Function,
    Method,
    Class,
    Interface,
    TypeAlias,
    Variable,
    Namespace, // PHP (Story 1.3)
}

impl SymbolKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::Method => "method",
            Self::Class => "class",
            Self::Interface => "interface",
            Self::TypeAlias => "type_alias",
            Self::Variable => "variable",
            Self::Namespace => "namespace",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum EdgeKind {
    Calls,
    Imports,
    Extends,
    Implements,
    UsesType,
    References,
    HooksInto, // PHP-only (Story 1.3)
    FiresHook, // PHP-only (Story 1.3)
    UsesTrait, // PHP-only (Story 1.3)
}

impl EdgeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Calls => "calls",
            Self::Imports => "imports",
            Self::Extends => "extends",
            Self::Implements => "implements",
            Self::UsesType => "uses_type",
            Self::References => "references",
            Self::HooksInto => "hooks_into",
            Self::FiresHook => "fires_hook",
            Self::UsesTrait => "uses_trait",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("tree-sitter parse returned None (source may be empty or invalid)")]
    ParseFailed,
    #[error("tree-sitter language error: {0}")]
    LanguageError(#[from] tree_sitter::LanguageError),
    #[error("UTF-8 error in source: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
}

/// Extract the signature portion of a symbol's source — everything up to the body start.
///
/// Two-step: prefer tree-sitter body child (exact, immune to `{` in default values),
/// then fall back to byte scanning for `{` or `:\n`.
/// Returns `None` for expression-bodied symbols (arrow fns, type aliases) where no
/// body delimiter exists — these produce body_only diffs, not signature_changed.
pub(crate) fn extract_signature(source: &[u8], node: tree_sitter::Node) -> Option<String> {
    let node_start = node.start_byte();
    let node_end = node.end_byte();

    // Step 1: find named body child — exact body start, avoids `{` in defaults
    let sig_end: Option<usize> = (0..node.child_count())
        .filter_map(|i| node.child(i))
        .find(|c| {
            matches!(c.kind(), "block" | "statement_block" | "compound_statement" | "body")
        })
        .map(|body| body.start_byte())
        // Step 2: fallback byte scan if no body child found
        .or_else(|| {
            let node_src = &source[node_start..node_end];
            let brace = node_src.iter().position(|&b| b == b'{').map(|i| node_start + i);
            let colon_nl = node_src
                .windows(2)
                .position(|w| w[0] == b':' && w[1] == b'\n')
                .map(|i| node_start + i + 1); // include ':'
            match (brace, colon_nl) {
                (Some(b), Some(c)) => Some(b.min(c)),
                (Some(b), None) => Some(b),
                (None, Some(c)) => Some(c),
                // No body delimiter: expression-bodied nodes → signature = None
                (None, None) => None,
            }
        });

    let sig_end = sig_end?;
    std::str::from_utf8(&source[node_start..sig_end])
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Convenience constructor — avoids duplicating hash/line-number logic in every parser.
pub(crate) fn make_symbol(
    relative_path: &str,
    parent: Option<&str>,
    name: &str,
    kind: SymbolKind,
    node: tree_sitter::Node<'_>,
    source: &[u8],
) -> Symbol {
    Symbol {
        fqn: make_fqn(relative_path, parent, name),
        name: name.to_string(),
        kind,
        start_line: node.start_position().row as u32 + 1,
        end_line: node.end_position().row as u32 + 1,
        signature: extract_signature(source, node),
        docstring: None,
        source_hash: blake3::hash(&source[node.start_byte()..node.end_byte()])
            .to_hex()
            .to_string(),
    }
}

/// Sole FQN construction function.
///
/// Format:
///   - `"relative/path.ts::ClassName::method"` (nested — parent is Some)
///   - `"relative/path.ts::symbol"` (top-level — parent is None)
pub fn make_fqn(relative_path: &str, parent: Option<&str>, name: &str) -> String {
    match parent {
        Some(p) => format!("{}::{}::{}", relative_path, p, name),
        None => format!("{}::{}", relative_path, name),
    }
}
