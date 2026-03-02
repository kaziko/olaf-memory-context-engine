pub(crate) mod symbols;
pub(crate) mod typescript;
pub(crate) mod python;
pub(crate) mod rust_lang;
pub(crate) mod php;

pub use symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind};

use std::path::Path;

pub enum Language {
    TypeScript,
    Tsx,
    JavaScript,
    Jsx,
    Python,
    Rust,
    Php,
}

/// Detect language from relative file path extension.
/// Returns `None` for unknown/unsupported extensions.
pub fn detect_language(relative_path: &str) -> Option<Language> {
    let ext = Path::new(relative_path).extension()?.to_str()?;
    match ext {
        "ts" | "mts" | "cts" => Some(Language::TypeScript),
        "tsx" => Some(Language::Tsx),
        "js" | "mjs" | "cjs" => Some(Language::JavaScript),
        "jsx" => Some(Language::Jsx),
        "py" => Some(Language::Python),
        "rs" => Some(Language::Rust),
        "php" => Some(Language::Php),
        _ => None,
    }
}

/// Parse a source file and return extracted symbols and edges.
///
/// `relative_path` must be relative to the project root (no leading `./`).
/// Unknown extensions log a warning and return empty — no panic, no error.
/// This is the sole public entry point for the parser subsystem.
pub fn parse_file(
    relative_path: &str,
    source: &[u8],
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let Some(lang) = detect_language(relative_path) else {
        log::warn!("unsupported file type skipped: {}", relative_path);
        return Ok((vec![], vec![]));
    };
    match lang {
        Language::TypeScript => {
            typescript::parse(relative_path, source, typescript::TsDialect::TypeScript)
        }
        Language::Tsx => typescript::parse(relative_path, source, typescript::TsDialect::Tsx),
        Language::JavaScript => {
            typescript::parse(relative_path, source, typescript::TsDialect::JavaScript)
        }
        Language::Jsx => typescript::parse(relative_path, source, typescript::TsDialect::Jsx),
        Language::Python => python::parse(relative_path, source),
        Language::Rust => rust_lang::parse(relative_path, source),
        Language::Php => php::parse(relative_path, source),
    }
}
