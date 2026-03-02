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
    /// FQN of the source symbol (always resolved — must exist in the symbol list)
    pub source_fqn: String,
    /// FQN or module path of the target (may be unresolved — see Edge Contract)
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
