use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, extract_signature, make_fqn, make_symbol};

pub(crate) fn parse(
    relative_path: &str,
    source: &[u8],
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter_php::LANGUAGE_PHP.into())?;
    let tree = parser.parse(source, None).ok_or(ParserError::ParseFailed)?;
    let root = tree.root_node();
    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    let mut current_namespace: Option<String> = None;
    extract_nodes(
        root,
        source,
        relative_path,
        &mut current_namespace,
        None,
        None,
        &mut symbols,
        &mut edges,
    )?;
    Ok((symbols, edges))
}

#[allow(clippy::too_many_arguments)]
fn extract_nodes(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    current_namespace: &mut Option<String>, // MUTABLE — unbraced namespace updates siblings
    parent_class: Option<&str>,             // namespace-qualified class name, e.g. "MyPlugin\Cart"
    current_fqn: Option<&str>,
    symbols: &mut Vec<Symbol>,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    match node.kind() {
        "namespace_definition" => {
            let ns_name = node
                .child_by_field_name("name")
                .map(|n| n.utf8_text(source))
                .transpose()?
                .map(|s| s.to_string());

            // Emit Namespace symbol if name is present
            if let Some(ref name) = ns_name {
                symbols.push(Symbol {
                    fqn: make_fqn(relative_path, None, name),
                    name: name.clone(),
                    kind: SymbolKind::Namespace,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: extract_signature(source, node),
                    docstring: None,
                    source_hash: blake3::hash(&source[node.start_byte()..node.end_byte()])
                        .to_hex()
                        .to_string(),
                });
            }

            if let Some(body) = node.child_by_field_name("body") {
                // BRACED namespace: recurse into body with a fresh local namespace;
                // outer current_namespace is NOT modified — scope ends with the block
                let mut inner_ns = ns_name.clone();
                let mut walker = body.walk();
                for child in body.children(&mut walker) {
                    extract_nodes(
                        child,
                        source,
                        relative_path,
                        &mut inner_ns,
                        None,
                        None,
                        symbols,
                        edges,
                    )?;
                }
            } else {
                // UNBRACED namespace: update current_namespace for all subsequent siblings
                *current_namespace = ns_name;
            }
        }
        "namespace_use_declaration" => {
            // `use WP_Post;` — emit Imports edge, source = file path
            let mut walker = node.walk();
            for child in node.named_children(&mut walker) {
                if child.kind() == "namespace_use_clause" {
                    let target = child.utf8_text(source)?.to_string();
                    edges.push(Edge {
                        source_fqn: relative_path.to_string(),
                        target_fqn: target,
                        kind: EdgeKind::Imports,
                    });
                }
            }
        }
        "class_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let raw_name = name_node.utf8_text(source)?;
                let qualified_name = match current_namespace.as_deref() {
                    Some(ns) => format!("{}\\{}", ns, raw_name),
                    None => raw_name.to_string(),
                };
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    &qualified_name,
                    SymbolKind::Class,
                    node,
                    source,
                ));
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        extract_nodes(
                            child,
                            source,
                            relative_path,
                            current_namespace,
                            Some(&qualified_name),
                            None,
                            symbols,
                            edges,
                        )?;
                    }
                }
            }
        }
        "method_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let method_fqn = make_fqn(relative_path, parent_class, name);
                symbols.push(make_symbol(
                    relative_path,
                    parent_class,
                    name,
                    SymbolKind::Method,
                    node,
                    source,
                ));
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        extract_nodes(
                            child,
                            source,
                            relative_path,
                            current_namespace,
                            None, // no longer at class level
                            Some(&method_fqn),
                            symbols,
                            edges,
                        )?;
                    }
                }
            }
        }
        "function_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let raw_name = name_node.utf8_text(source)?;
                let qualified_name = match current_namespace.as_deref() {
                    Some(ns) => format!("{}\\{}", ns, raw_name),
                    None => raw_name.to_string(),
                };
                let fn_fqn = make_fqn(relative_path, None, &qualified_name);
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    &qualified_name,
                    SymbolKind::Function,
                    node,
                    source,
                ));
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        extract_nodes(
                            child,
                            source,
                            relative_path,
                            current_namespace,
                            None,
                            Some(&fn_fqn),
                            symbols,
                            edges,
                        )?;
                    }
                }
            }
        }
        "use_declaration" => {
            // Inside a class body: trait usage → UsesTrait edge
            if let Some(class_fqn) = parent_class.map(|c| make_fqn(relative_path, None, c)) {
                let mut walker = node.walk();
                for child in node.named_children(&mut walker) {
                    if child.kind() == "name"
                        || child.kind() == "qualified_name"
                        || child.kind() == "named_type"
                    {
                        let trait_name = child.utf8_text(source)?;
                        edges.push(Edge {
                            source_fqn: class_fqn.clone(),
                            target_fqn: trait_name.to_string(),
                            kind: EdgeKind::UsesTrait,
                        });
                    }
                }
            }
        }
        "function_call_expression" => {
            if let Some(function_node) = node.child_by_field_name("function")
                && let Some(enclosing_fqn) = current_fqn
            {
                let fn_name = function_node.utf8_text(source)?;
                let hook_kind = match fn_name {
                    "add_action" | "add_filter" => Some(EdgeKind::HooksInto),
                    "do_action" | "apply_filters" => Some(EdgeKind::FiresHook),
                    _ => None,
                };
                if let Some(kind) = hook_kind
                    && let Some(args) = node.child_by_field_name("arguments")
                    && let Some(first_arg) = args.named_child(0)
                    && let Some(str_node) = first_arg.named_child(0)
                    && (str_node.kind() == "string" || str_node.kind() == "encapsed_string")
                {
                    let hook_text = str_node.utf8_text(source)?;
                    let hook_name = hook_text.trim_matches('"').trim_matches('\'');
                    edges.push(Edge {
                        source_fqn: enclosing_fqn.to_string(),
                        target_fqn: hook_name.to_string(),
                        kind,
                    });
                }
            }
            // Always recurse into children to handle nested calls
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                extract_nodes(
                    child,
                    source,
                    relative_path,
                    current_namespace,
                    parent_class,
                    current_fqn,
                    symbols,
                    edges,
                )?;
            }
        }
        _ => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                extract_nodes(
                    child,
                    source,
                    relative_path,
                    current_namespace,
                    parent_class,
                    current_fqn,
                    symbols,
                    edges,
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_php_file() {
        let src = b"<?php\n?>";
        let (symbols, edges) = parse("empty.php", src).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_mixed_html_php() {
        let src = b"<html><body><?php function greet() { echo 'hi'; } ?></body></html>";
        let result = parse("mixed.php", src);
        assert!(result.is_ok());
        let (symbols, _) = result.unwrap();
        assert!(symbols.iter().any(|s| s.name == "greet"));
    }

    #[test]
    fn parse_unclosed_php_tag() {
        let src = b"<?php\nfunction broken() { return 1; }\n";
        let result = parse("unclosed.php", src);
        assert!(result.is_ok());
        let (symbols, _) = result.unwrap();
        assert!(symbols.iter().any(|s| s.name == "broken"));
    }

    #[test]
    fn parse_php_only_comments() {
        let src = b"<?php\n// comment\n/* block */\n?>";
        let (symbols, edges) = parse("comments.php", src).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }
}
