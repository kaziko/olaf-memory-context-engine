// Named rust_lang.rs to avoid collision with the `rust` keyword
use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, make_symbol};

pub(crate) fn parse(
    relative_path: &str,
    source: &[u8],
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter_rust::LANGUAGE.into())?;
    let tree = parser.parse(source, None).ok_or(ParserError::ParseFailed)?;
    let root = tree.root_node();
    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    extract_nodes(root, source, relative_path, None, &mut symbols, &mut edges)?;
    Ok((symbols, edges))
}

/// Extract the simple type name from a tree-sitter type node.
/// Strips generic parameters and path qualifiers so FQNs are stable join keys.
///
/// Examples:
///   `Foo`          (type_identifier)             → "Foo"
///   `Foo<T>`       (generic_type)                → "Foo"
///   `crate::Foo`   (scoped_type_identifier)      → "Foo"
///   `Foo<T, Bar>`  (generic_type → generic_type) → "Foo"
fn extract_simple_type_name<'a>(
    node: tree_sitter::Node<'_>,
    source: &'a [u8],
) -> Result<&'a str, ParserError> {
    match node.kind() {
        "type_identifier" | "identifier" => Ok(node.utf8_text(source)?),
        "generic_type" => {
            // `Foo<T>` — the `type` field is the base name (a type_identifier or scoped)
            if let Some(name_node) = node.child_by_field_name("type") {
                extract_simple_type_name(name_node, source)
            } else {
                // Fallback: take first child
                node.child(0)
                    .map(|n| extract_simple_type_name(n, source))
                    .unwrap_or(Err(ParserError::ParseFailed))
            }
        }
        "scoped_type_identifier" => {
            // `crate::Foo` — the `name` field is the terminal identifier
            if let Some(name_node) = node.child_by_field_name("name") {
                extract_simple_type_name(name_node, source)
            } else {
                // Fallback: take last child
                let last_idx = node.child_count().saturating_sub(1);
                node.child(last_idx)
                    .map(|n| extract_simple_type_name(n, source))
                    .unwrap_or(Err(ParserError::ParseFailed))
            }
        }
        _ => {
            // Unknown node kind — use raw text, strip everything after '<' and after last '::'
            let text = node.utf8_text(source)?;
            let after_path = text.rsplit("::").next().unwrap_or(text);
            let before_generics = after_path.split('<').next().unwrap_or(after_path);
            Ok(before_generics.trim())
        }
    }
}

fn extract_nodes(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    parent_type: Option<&str>, // normalized name of the type being impl'd
    symbols: &mut Vec<Symbol>,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    match node.kind() {
        "function_item" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let kind = if parent_type.is_some() {
                    SymbolKind::Method
                } else {
                    SymbolKind::Function
                };
                symbols.push(make_symbol(
                    relative_path,
                    parent_type,
                    name,
                    kind,
                    node,
                    source,
                ));
            }
        }
        "struct_item" | "enum_item" => {
            // No native Struct/Enum kind — use Class
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Class,
                    node,
                    source,
                ));
            }
        }
        "trait_item" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Interface,
                    node,
                    source,
                ));
            }
        }
        "impl_item" => {
            // Not a symbol — extract implementing type, recurse into body with normalized type name
            if let Some(type_node) = node.child_by_field_name("type") {
                let type_name = extract_simple_type_name(type_node, source)?;
                if let Some(body) = node.child_by_field_name("body") {
                    extract_nodes(body, source, relative_path, Some(type_name), symbols, edges)?;
                }
            }
        }
        "use_declaration" => {
            let raw = node.utf8_text(source)?;
            // Find "use " to skip any leading visibility modifier (pub, pub(crate), etc.)
            let target = raw
                .find("use ")
                .map(|pos| raw[pos + 4..].trim_end_matches(';').trim())
                .unwrap_or("")
                .to_string();
            edges.push(Edge {
                source_fqn: relative_path.to_string(),
                target_fqn: target,
                kind: EdgeKind::Imports,
            });
        }
        _ => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                extract_nodes(child, source, relative_path, parent_type, symbols, edges)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_file_returns_no_symbols() {
        let (symbols, edges) = parse("empty.rs", b"").unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_macro_heavy_file_does_not_panic() {
        let src = b"macro_rules! my_macro {\n    ($x:expr) => { $x + 1 };\n}\nmy_macro!(42);\n";
        let result = parse("macros.rs", src);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_unsafe_block_extracts_fn() {
        let src = b"pub fn danger() {\n    unsafe { };\n}\n";
        let (symbols, _edges) = parse("unsafe.rs", src).unwrap();
        assert!(symbols.iter().any(|s| s.name == "danger"));
    }

    #[test]
    fn parse_file_with_only_comments() {
        let src = b"// just a comment\n/// doc comment\n/* block */\n";
        let (symbols, edges) = parse("comments.rs", src).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }
}
