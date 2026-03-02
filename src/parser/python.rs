use tree_sitter::Parser;

use super::symbols::{make_fqn, make_symbol, Edge, EdgeKind, ParserError, Symbol, SymbolKind};

pub(crate) fn parse(
    relative_path: &str,
    source: &[u8],
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter_python::LANGUAGE.into())?;
    let tree = parser.parse(source, None).ok_or(ParserError::ParseFailed)?;
    let root = tree.root_node();
    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    extract_nodes(root, source, relative_path, None, None, &mut symbols, &mut edges)?;
    Ok((symbols, edges))
}

fn extract_nodes(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    parent_class: Option<&str>,
    _current_fqn: Option<&str>, // for future Calls edges — pass through but don't emit yet
    symbols: &mut Vec<Symbol>,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    match node.kind() {
        "function_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let kind = if parent_class.is_some() {
                    SymbolKind::Method
                } else {
                    SymbolKind::Function
                };
                let fqn = make_fqn(relative_path, parent_class, name);
                symbols.push(make_symbol(relative_path, parent_class, name, kind, node, source));
                // Recurse into body with updated _current_fqn
                if let Some(body) = node.child_by_field_name("body") {
                    extract_nodes(
                        body,
                        source,
                        relative_path,
                        parent_class,
                        Some(&fqn),
                        symbols,
                        edges,
                    )?;
                }
            }
        }
        "class_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(relative_path, None, name, SymbolKind::Class, node, source));
                if let Some(body) = node.child_by_field_name("body") {
                    let class_fqn = make_fqn(relative_path, None, name);
                    extract_nodes(
                        body,
                        source,
                        relative_path,
                        Some(name),
                        Some(&class_fqn),
                        symbols,
                        edges,
                    )?;
                }
            }
        }
        "decorated_definition" => {
            // @decorator\ndef foo(): ... or @decorator\nclass Foo: ...
            if let Some(inner) = node.child_by_field_name("definition") {
                extract_nodes(inner, source, relative_path, parent_class, _current_fqn, symbols, edges)?;
            }
        }
        "import_statement" => {
            // Can have multiple imported names: `import os, sys, pathlib`
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                let target = match child.kind() {
                    "dotted_name" => Some(child.utf8_text(source)?.to_string()),
                    "aliased_import" => {
                        // "name" field = module, "alias" field = local alias — always use "name"
                        child
                            .child_by_field_name("name")
                            .map(|n| n.utf8_text(source))
                            .transpose()?
                            .map(|s| s.to_string())
                    }
                    _ => None,
                };
                if let Some(t) = target {
                    edges.push(Edge {
                        source_fqn: relative_path.to_string(),
                        target_fqn: t,
                        kind: EdgeKind::Imports,
                    });
                }
            }
        }
        "import_from_statement" => {
            // "from X import Y" — target is the module (X), not the imported symbol (Y)
            if let Some(module_node) = node.child_by_field_name("module_name") {
                edges.push(Edge {
                    source_fqn: relative_path.to_string(),
                    target_fqn: module_node.utf8_text(source)?.to_string(),
                    kind: EdgeKind::Imports,
                });
            }
        }
        _ => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                extract_nodes(child, source, relative_path, parent_class, _current_fqn, symbols, edges)?;
            }
        }
    }
    Ok(())
}
