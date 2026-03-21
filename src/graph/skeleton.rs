use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::graph::store::StoreError;

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
) {
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
) -> String {
    const MAX_NESTED_ENTRIES: usize = 50;

    let mut output = String::new();
    append_symbol_header(&mut output, parent, deps_map);

    let mut rendered = 0usize;
    for child in children.iter().take(MAX_NESTED_ENTRIES) {
        output.push_str(&format!("#### {}\n", nested_entry_title(child)));
        rendered += 1;
    }

    let method_budget = MAX_NESTED_ENTRIES.saturating_sub(rendered);
    for method in methods.iter().take(method_budget) {
        let is_redacted = method.signature.as_deref() == Some("[redacted by policy]");
        output.push_str(&format!("#### {}\n", nested_entry_title(method)));
        if !is_redacted {
            if let Some(edges) = deps_map.get(&method.id)
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
) -> String {
    let mut output = String::new();
    append_symbol_header(&mut output, symbol, deps_map);
    output.push('\n');
    output
}

pub(crate) fn skeletonize(conn: &Connection, symbol_id: i64) -> Result<String, StoreError> {
    let (fqn, name, file_path, start, end, sig, doc) = conn.query_row(
        "SELECT s.fqn, s.name, f.path, s.start_line, s.end_line, s.signature, s.docstring
         FROM symbols s JOIN files f ON f.id=s.file_id WHERE s.id=?1",
        params![symbol_id],
        |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
                 r.get::<_,i64>(3)?, r.get::<_,i64>(4)?,
                 r.get::<_,Option<String>>(5)?, r.get::<_,Option<String>>(6)?)),
    )?;
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
        let result = skeletonize(&conn, 1).unwrap();
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
        let result = skeletonize(&conn, 1).unwrap();
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

        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new());

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

        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new());

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

        let output = format_parent_with_children(&parent, &children, &[], &HashMap::new());

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

        let output = format_parent_with_children(&parent, &children, &methods, &HashMap::new());

        assert_eq!(output.matches("#### ").count(), 50);
        assert!(output.contains("... and 5 more"));
        assert!(!output.contains("method_20"), "methods beyond the cap must be omitted");
    }
}
