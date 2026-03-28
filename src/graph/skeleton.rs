use std::borrow::Cow;
use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::graph::store::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DetailLevel {
    Minimal,
    #[default]
    Standard,
    Detailed,
}

impl std::fmt::Display for DetailLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetailLevel::Minimal => write!(f, "minimal"),
            DetailLevel::Standard => write!(f, "standard"),
            DetailLevel::Detailed => write!(f, "detailed"),
        }
    }
}

/// Maps raw DB kind strings (snake_case) to Title Case display form.
/// Returns `Cow::Borrowed` for known kinds (zero allocation), `Cow::Owned` for unknown fallback.
fn display_kind(kind: &str) -> Cow<'static, str> {
    match kind {
        "function" => "Function".into(),
        "method" => "Method".into(),
        "struct" => "Struct".into(),
        "enum" => "Enum".into(),
        "trait" => "Trait".into(),
        "class" => "Class".into(),
        "interface" => "Interface".into(),
        "module" => "Module".into(),
        "constant" => "Constant".into(),
        "variable" => "Variable".into(),
        "field" => "Field".into(),
        "property" => "Property".into(),
        "type_alias" => "Type Alias".into(),
        "trait_method" => "Trait Method".into(),
        "enum_variant" => "Enum Variant".into(),
        "impl_block" => "Impl Block".into(),
        "associated_type" => "Associated Type".into(),
        "namespace" => "Namespace".into(),
        "constructor" => "Constructor".into(),
        "decorator" => "Decorator".into(),
        _ => Cow::Owned(
            kind.split('_')
                .map(|w| {
                    let mut c = w.chars();
                    match c.next() {
                        Some(first) => {
                            let upper: String = first.to_uppercase().collect();
                            format!("{upper}{}", c.as_str())
                        }
                        None => String::new(),
                    }
                })
                .collect::<Vec<_>>()
                .join(" "),
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkeletonSymbol {
    pub id: i64,
    pub name: String,
    pub fqn: String,
    pub file_path: String,
    pub start_line: i64,
    pub end_line: i64,
    pub kind: String,
    pub signature: Option<String>,
    pub docstring: Option<String>,
    pub parent_id: Option<i64>,
}

fn append_symbol_header(
    output: &mut String,
    symbol: &SkeletonSymbol,
    deps_map: &HashMap<i64, Vec<(String, String)>>,
    detail: DetailLevel,
) {
    if detail == DetailLevel::Minimal {
        output.push_str(&format!(
            "### {} ({}) — lines {}-{}\n",
            symbol.name, display_kind(&symbol.kind), symbol.start_line, symbol.end_line
        ));
        return;
    }
    output.push_str(&format!(
        "### {} (`{}`)\nFile: `{}` lines {}-{}\n",
        symbol.name, symbol.fqn, symbol.file_path, symbol.start_line, symbol.end_line
    ));
    if let Some(sig) = &symbol.signature {
        output.push_str(&format!("Signature: `{sig}`\n"));
    }
    if let Some(doc) = &symbol.docstring {
        output.push_str(&format!("{doc}\n"));
    }
    if let Some(edges) = deps_map.get(&symbol.id)
        && !edges.is_empty()
    {
        let rendered = edges
            .iter()
            .map(|(name, kind)| format!("{name} ({kind})"))
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!("Dependencies: {rendered}\n"));
    }
}

fn nested_entry_title(symbol: &SkeletonSymbol) -> String {
    match symbol.signature.as_deref() {
        Some("[redacted by policy]") => format!("{} [redacted by policy]", symbol.name),
        Some(sig) if !sig.is_empty() => sig.to_string(),
        _ => symbol.name.clone(),
    }
}

pub(crate) fn format_parent_with_children(
    parent: &SkeletonSymbol,
    children: &[SkeletonSymbol],
    methods: &[SkeletonSymbol],
    deps_map: &HashMap<i64, Vec<(String, String)>>,
    detail: DetailLevel,
) -> String {
    const MAX_NESTED_ENTRIES: usize = 50;
    const MAX_DETAILED_ENTRIES: usize = 200;

    let mut output = String::new();
    append_symbol_header(&mut output, parent, deps_map, detail);

    // Minimal: only parent header, skip children and methods entirely
    if detail == DetailLevel::Minimal {
        output.push('\n');
        return output;
    }

    let cap = if detail == DetailLevel::Detailed {
        MAX_DETAILED_ENTRIES
    } else {
        MAX_NESTED_ENTRIES
    };

    let mut rendered = 0usize;
    for child in children.iter().take(cap) {
        output.push_str(&format!("#### {}\n", nested_entry_title(child)));
        // Detailed: render dependency edges on children (not just methods)
        if detail == DetailLevel::Detailed {
            let is_redacted = child.signature.as_deref() == Some("[redacted by policy]");
            if !is_redacted
                && let Some(edges) = deps_map.get(&child.id)
                && !edges.is_empty()
            {
                let dep_line = edges
                    .iter()
                    .map(|(name, kind)| format!("{name} ({kind})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(&format!("Dependencies: {dep_line}\n"));
            }
        }
        rendered += 1;
    }

    let method_budget = cap.saturating_sub(rendered);
    for method in methods.iter().take(method_budget) {
        let is_redacted = method.signature.as_deref() == Some("[redacted by policy]");
        output.push_str(&format!("#### {}\n", nested_entry_title(method)));
        if !is_redacted
            && let Some(edges) = deps_map.get(&method.id)
            && !edges.is_empty()
        {
            let dep_line = edges
                .iter()
                .map(|(name, kind)| format!("{name} ({kind})"))
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&format!("Dependencies: {dep_line}\n"));
        }
        rendered += 1;
    }

    let hidden = children.len() + methods.len() - rendered;
    if hidden > 0 {
        output.push_str(&format!("... and {hidden} more\n"));
    }

    output.push('\n');
    output
}

pub(crate) fn format_standalone(
    symbol: &SkeletonSymbol,
    deps_map: &HashMap<i64, Vec<(String, String)>>,
    detail: DetailLevel,
) -> String {
    let mut output = String::new();
    append_symbol_header(&mut output, symbol, deps_map, detail);
    output.push('\n');
    output
}

pub(crate) fn skeletonize(
    conn: &Connection,
    symbol_id: i64,
    detail: DetailLevel,
) -> Result<String, StoreError> {
    let (fqn, name, file_path, kind, start, end, sig, doc) = conn.query_row(
        "SELECT s.fqn, s.name, f.path, s.kind, s.start_line, s.end_line, s.signature, s.docstring
         FROM symbols s JOIN files f ON f.id=s.file_id WHERE s.id=?1",
        params![symbol_id],
        |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
                 r.get::<_,String>(3)?,
                 r.get::<_,i64>(4)?, r.get::<_,i64>(5)?,
                 r.get::<_,Option<String>>(6)?, r.get::<_,Option<String>>(7)?)),
    )?;

    if detail == DetailLevel::Minimal {
        // Per-symbol file path IS included (no global header in skeletonize context)
        // Skip edges query entirely — saves a DB round-trip per supporting symbol
        let mut s = format!(
            "### {} ({}) — lines {}-{}\nFile: `{}`\n",
            name, display_kind(&kind), start, end, file_path
        );
        s.push('\n');
        return Ok(s);
    }

    let mut s = format!("### {} (`{}`)\nFile: `{}` lines {}-{}\n", name, fqn, file_path, start, end);
    if let Some(sig) = sig { s.push_str(&format!("Signature: `{sig}`\n")); }
    if let Some(doc) = doc { s.push_str(&format!("{doc}\n")); }
    let mut stmt = conn.prepare(
        "SELECT s2.name, e.kind FROM edges e JOIN symbols s2 ON s2.id=e.target_id
         WHERE e.source_id=?1 LIMIT 10")?;
    let edges: Vec<String> = stmt.query_map(params![symbol_id], |r| {
        Ok(format!("{} ({})", r.get::<_,String>(0)?, r.get::<_,String>(1)?))
    })?.collect::<Result<_,_>>()?;
    if !edges.is_empty() {
        s.push_str(&format!("Dependencies: {}\n", edges.join(", ")));
    }
    s.push('\n');
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn make_symbol(id: i64, name: &str, kind: &str, signature: Option<&str>) -> SkeletonSymbol {
        SkeletonSymbol {
            id,
            name: name.to_string(),
            fqn: format!("src/lib.rs::{name}"),
            file_path: "src/lib.rs".to_string(),
            start_line: 1,
            end_line: 10,
            kind: kind.to_string(),
            signature: signature.map(str::to_string),
            docstring: None,
            parent_id: None,
        }
    }

    fn setup_skeleton_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT NOT NULL, hash TEXT);
             CREATE TABLE symbols (
                 id INTEGER PRIMARY KEY, file_id INTEGER NOT NULL, fqn TEXT NOT NULL,
                 name TEXT NOT NULL, kind TEXT, start_line INTEGER NOT NULL,
                 end_line INTEGER NOT NULL, signature TEXT, docstring TEXT, source_hash TEXT,
                 parent_id INTEGER DEFAULT NULL
             );
             CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT);",
        ).unwrap();
        conn.execute("INSERT INTO files (id, path) VALUES (1, 'src/lib.rs')", []).unwrap();
        conn
    }

    #[test]
    fn skeletonize_symbol_with_no_edges() {
        let conn = setup_skeleton_db();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'lib::Foo', 'Foo', 'struct', 1, 10, 'pub struct Foo', 'A foo struct', NULL, NULL)",
            [],
        ).unwrap();
        let result = skeletonize(&conn, 1, DetailLevel::Standard).unwrap();
        assert!(result.contains("Foo"));
        assert!(result.contains("pub struct Foo"));
        assert!(result.contains("A foo struct"));
        assert!(!result.contains("Dependencies"));
    }

    #[test]
    fn skeletonize_symbol_with_only_docstring_no_signature() {
        let conn = setup_skeleton_db();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'lib::Bar', 'Bar', 'function', 5, 15, NULL, 'Does something important', NULL, NULL)",
            [],
        ).unwrap();
        let result = skeletonize(&conn, 1, DetailLevel::Standard).unwrap();
        assert!(result.contains("Bar"));
        assert!(result.contains("Does something important"));
        assert!(!result.contains("Signature"));
    }

    #[test]
    fn format_parent_with_children_renders_enum_variants_nested() {
        let parent = make_symbol(1, "ToolError", "enum", Some("pub enum ToolError"));
        let children = vec![
            SkeletonSymbol {
                id: 2,
                name: "Db".to_string(),
                fqn: "src/lib.rs::ToolError::Db".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2,
                end_line: 2,
                kind: "enum_variant".to_string(),
                signature: Some("Db(DbError)".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
            SkeletonSymbol {
                id: 3,
                name: "Parse".to_string(),
                fqn: "src/lib.rs::ToolError::Parse".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 3,
                end_line: 3,
                kind: "enum_variant".to_string(),
                signature: Some("Parse(String)".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
        ];

        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new(), DetailLevel::Standard);

        assert!(output.contains("### ToolError (`src/lib.rs::ToolError`)"));
        assert!(output.contains("#### Db(DbError)"));
        assert!(output.contains("#### Parse(String)"));
    }

    #[test]
    fn format_parent_with_children_renders_struct_fields_nested() {
        let parent = make_symbol(1, "Config", "struct", Some("pub struct Config"));
        let children = vec![
            SkeletonSymbol {
                id: 2,
                name: "name".to_string(),
                fqn: "src/lib.rs::Config::name".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2,
                end_line: 2,
                kind: "field".to_string(),
                signature: Some("pub name: String".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
            SkeletonSymbol {
                id: 3,
                name: "port".to_string(),
                fqn: "src/lib.rs::Config::port".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 3,
                end_line: 3,
                kind: "field".to_string(),
                signature: Some("pub port: u16".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
        ];

        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new(), DetailLevel::Standard);

        assert!(output.contains("#### pub name: String"));
        assert!(output.contains("#### pub port: u16"));
    }

    #[test]
    fn format_parent_with_children_renders_trait_members_nested() {
        let parent = make_symbol(1, "Handler", "trait", Some("pub trait Handler"));
        let children = vec![
            SkeletonSymbol {
                id: 2,
                name: "Output".to_string(),
                fqn: "src/lib.rs::Handler::Output".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2,
                end_line: 2,
                kind: "associated_type".to_string(),
                signature: Some("type Output;".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
            SkeletonSymbol {
                id: 3,
                name: "MIN".to_string(),
                fqn: "src/lib.rs::Handler::MIN".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 3,
                end_line: 3,
                kind: "constant".to_string(),
                signature: Some("const MIN: usize = 0;".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
            SkeletonSymbol {
                id: 4,
                name: "handle".to_string(),
                fqn: "src/lib.rs::Handler::handle".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 4,
                end_line: 4,
                kind: "trait_method".to_string(),
                signature: Some("fn handle(&self, input: &str) -> Self::Output;".to_string()),
                docstring: None,
                parent_id: Some(1),
            },
        ];

        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new(), DetailLevel::Standard);

        assert!(output.contains("#### type Output;"));
        assert!(output.contains("#### const MIN: usize = 0;"));
        assert!(output.contains("#### fn handle(&self, input: &str) -> Self::Output;"));
    }

    #[test]
    fn format_parent_with_children_caps_children_and_methods_at_fifty() {
        let parent = make_symbol(1, "Huge", "struct", Some("pub struct Huge"));
        let children = (0..30)
            .map(|i| SkeletonSymbol {
                id: i + 2,
                name: format!("field_{i}"),
                fqn: format!("src/lib.rs::Huge::field_{i}"),
                file_path: "src/lib.rs".to_string(),
                start_line: i + 2,
                end_line: i + 2,
                kind: "field".to_string(),
                signature: Some(format!("field_{i}: usize")),
                docstring: None,
                parent_id: Some(1),
            })
            .collect::<Vec<_>>();
        let methods = (0..25)
            .map(|i| SkeletonSymbol {
                id: i + 100,
                name: format!("method_{i}"),
                fqn: format!("src/lib.rs::Huge::method_{i}"),
                file_path: "src/lib.rs".to_string(),
                start_line: i + 40,
                end_line: i + 40,
                kind: "method".to_string(),
                signature: Some(format!("fn method_{i}(&self)")),
                docstring: None,
                parent_id: None,
            })
            .collect::<Vec<_>>();

        let output = format_parent_with_children(&parent, &children, &methods, &HashMap::new(), DetailLevel::Standard);

        assert_eq!(output.matches("#### ").count(), 50);
        assert!(output.contains("... and 5 more"));
        assert!(!output.contains("method_20"), "methods beyond the cap must be omitted");
    }

    #[test]
    fn minimal_format_parent_omits_children() {
        let parent = make_symbol(1, "Config", "struct", Some("pub struct Config"));
        let children = vec![
            SkeletonSymbol {
                id: 2, name: "name".to_string(),
                fqn: "src/lib.rs::Config::name".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2, end_line: 2,
                kind: "field".to_string(),
                signature: Some("pub name: String".to_string()),
                docstring: None, parent_id: Some(1),
            },
        ];
        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new(), DetailLevel::Minimal);
        assert!(output.contains("### Config (Struct) — lines 1-10"));
        assert!(!output.contains("####"), "minimal must omit all children");
        assert!(!output.contains("Signature:"), "minimal must omit signatures");
        assert!(!output.contains("File:"), "minimal parent has no per-symbol File: line");
    }

    #[test]
    fn standard_format_parent_unchanged() {
        let parent = make_symbol(1, "Config", "struct", Some("pub struct Config"));
        let children = vec![
            SkeletonSymbol {
                id: 2, name: "name".to_string(),
                fqn: "src/lib.rs::Config::name".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2, end_line: 2,
                kind: "field".to_string(),
                signature: Some("pub name: String".to_string()),
                docstring: None, parent_id: Some(1),
            },
        ];
        let output_standard = format_parent_with_children(&parent, &children, &[], &HashMap::new(), DetailLevel::Standard);
        // Standard must contain FQN, file, signature header, and children
        assert!(output_standard.contains("### Config (`src/lib.rs::Config`)"));
        assert!(output_standard.contains("File: `src/lib.rs`"));
        assert!(output_standard.contains("Signature: `pub struct Config`"));
        assert!(output_standard.contains("#### pub name: String"));
    }

    #[test]
    fn detailed_format_parent_shows_child_deps() {
        let parent = make_symbol(1, "Handler", "struct", Some("pub struct Handler"));
        let children = vec![
            SkeletonSymbol {
                id: 2, name: "db".to_string(),
                fqn: "src/lib.rs::Handler::db".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2, end_line: 2,
                kind: "field".to_string(),
                signature: Some("pub db: Database".to_string()),
                docstring: None, parent_id: Some(1),
            },
        ];
        let mut deps = HashMap::new();
        deps.insert(2, vec![("Database".to_string(), "calls".to_string())]);
        let output = format_parent_with_children(&parent, &children, &[], &deps, DetailLevel::Detailed);
        assert!(output.contains("#### pub db: Database"));
        assert!(output.contains("Dependencies: Database (calls)"), "detailed must show child deps");
    }

    #[test]
    fn detailed_format_parent_higher_cap() {
        let parent = make_symbol(1, "Big", "struct", Some("pub struct Big"));
        let children = (0..55)
            .map(|i| SkeletonSymbol {
                id: i + 2,
                name: format!("field_{i}"),
                fqn: format!("src/lib.rs::Big::field_{i}"),
                file_path: "src/lib.rs".to_string(),
                start_line: i + 2,
                end_line: i + 2,
                kind: "field".to_string(),
                signature: Some(format!("field_{i}: usize")),
                docstring: None,
                parent_id: Some(1),
            })
            .collect::<Vec<_>>();
        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new(), DetailLevel::Detailed);
        assert_eq!(output.matches("#### ").count(), 55, "detailed must show all 55 children (cap=200)");
        assert!(!output.contains("... and"), "no truncation with 55 children in detailed mode");
    }

    #[test]
    fn detailed_redacted_child_hides_deps() {
        let parent = make_symbol(1, "Secret", "struct", Some("pub struct Secret"));
        let children = vec![
            SkeletonSymbol {
                id: 2, name: "token".to_string(),
                fqn: "src/lib.rs::Secret::token".to_string(),
                file_path: "src/lib.rs".to_string(),
                start_line: 2, end_line: 2,
                kind: "field".to_string(),
                signature: Some("[redacted by policy]".to_string()),
                docstring: None, parent_id: Some(1),
            },
        ];
        let mut deps = HashMap::new();
        deps.insert(2, vec![("Crypto".to_string(), "calls".to_string())]);
        let output = format_parent_with_children(&parent, &children, &[], &deps, DetailLevel::Detailed);
        assert!(output.contains("#### token [redacted by policy]"));
        assert!(!output.contains("Dependencies:"), "redacted child must not show deps in detailed mode");
    }

    #[test]
    fn minimal_append_symbol_header_compact() {
        let sym = make_symbol(1, "process", "function", Some("fn process(x: i32) -> bool"));
        let mut output = String::new();
        append_symbol_header(&mut output, &sym, &HashMap::new(), DetailLevel::Minimal);
        assert_eq!(output, "### process (Function) — lines 1-10\n");
        assert!(!output.contains("File:"), "minimal header has no File: line");
        assert!(!output.contains("Signature:"), "minimal header has no Signature:");
    }

    #[test]
    fn skeletonize_minimal_skips_deps() {
        let conn = setup_skeleton_db();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'lib::run', 'run', 'function', 5, 20, 'fn run()', NULL, NULL, NULL)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO symbols VALUES (2, 1, 'lib::helper', 'helper', 'function', 25, 30, NULL, NULL, NULL, NULL)",
            [],
        ).unwrap();
        conn.execute("INSERT INTO edges VALUES (1, 1, 2, 'calls')", []).unwrap();

        let result = skeletonize(&conn, 1, DetailLevel::Minimal).unwrap();
        assert!(result.contains("### run (Function) — lines 5-20"));
        assert!(result.contains("File: `src/lib.rs`"));
        assert!(!result.contains("Dependencies"), "minimal skeletonize must skip deps query");
        assert!(!result.contains("Signature:"), "minimal skeletonize must skip signature");
    }
}
