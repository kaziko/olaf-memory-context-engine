use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, make_fqn};

pub(crate) enum TsDialect {
    TypeScript,
    Tsx,
    JavaScript,
    Jsx,
}

pub(crate) fn parse(
    relative_path: &str,
    source: &[u8],
    dialect: TsDialect,
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let mut parser = Parser::new();

    match dialect {
        TsDialect::TypeScript => {
            parser.set_language(&tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into())?;
        }
        TsDialect::Tsx => {
            parser.set_language(&tree_sitter_typescript::LANGUAGE_TSX.into())?;
        }
        TsDialect::JavaScript | TsDialect::Jsx => {
            parser.set_language(&tree_sitter_javascript::LANGUAGE.into())?;
        }
    }

    let tree = parser.parse(source, None).ok_or(ParserError::ParseFailed)?;
    let root = tree.root_node();

    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    extract_nodes(
        root,
        source,
        relative_path,
        None,
        None,
        &mut symbols,
        &mut edges,
    )?;
    Ok((symbols, edges))
}

/// Recursive AST walker.
///
/// `parent_class` — name of the enclosing class (for FQN construction of methods).
/// `current_fqn`  — FQN of the innermost enclosing function or method symbol; used as
///                  `source_fqn` for best-effort `Calls` and `UsesType` edges.
///                  `None` at file scope and inside class bodies (not yet inside a function).
fn extract_nodes(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    parent_class: Option<&str>,
    current_fqn: Option<&str>,
    symbols: &mut Vec<Symbol>,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    match node.kind() {
        "function_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let fqn = make_fqn(relative_path, parent_class, name);
                let source_hash = blake3::hash(&source[node.start_byte()..node.end_byte()])
                    .to_hex()
                    .to_string();
                symbols.push(Symbol {
                    fqn: fqn.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Function,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });
                // Recurse into body with this function as the enclosing symbol
                for child in node.children(&mut node.walk()) {
                    extract_nodes(
                        child,
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

        "class_declaration" | "class" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let fqn = make_fqn(relative_path, None, name);
                let source_hash = blake3::hash(&source[node.start_byte()..node.end_byte()])
                    .to_hex()
                    .to_string();
                symbols.push(Symbol {
                    fqn: fqn.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Class,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });

                // class_heritage wraps extends_clause / implements_clause (TS) or
                // holds the parent class identifier directly (JS).
                // class_body is a separate direct child.
                for child in node.children(&mut node.walk()) {
                    match child.kind() {
                        "class_heritage" => {
                            for hchild in child.children(&mut child.walk()) {
                                match hchild.kind() {
                                    "extends_clause" => {
                                        // TS: children are "extends" keyword + identifier/type_identifier
                                        for echild in hchild.children(&mut hchild.walk()) {
                                            if echild.kind() == "identifier"
                                                || echild.kind() == "type_identifier"
                                            {
                                                let target = echild.utf8_text(source)?;
                                                edges.push(Edge {
                                                    source_fqn: fqn.clone(),
                                                    target_fqn: target.to_string(),
                                                    kind: EdgeKind::Extends,
                                                });
                                            }
                                        }
                                    }
                                    "implements_clause" => {
                                        // TS: children are "implements" keyword + type_identifier(s)
                                        for ichild in hchild.children(&mut hchild.walk()) {
                                            let target = if ichild.kind() == "type_identifier"
                                                || ichild.kind() == "identifier"
                                            {
                                                Some(ichild.utf8_text(source)?.to_string())
                                            } else if ichild.kind() == "generic_type" {
                                                ichild
                                                    .child(0)
                                                    .and_then(|n| n.utf8_text(source).ok())
                                                    .map(|s| s.to_string())
                                            } else {
                                                None
                                            };
                                            if let Some(t) = target {
                                                edges.push(Edge {
                                                    source_fqn: fqn.clone(),
                                                    target_fqn: t,
                                                    kind: EdgeKind::Implements,
                                                });
                                            }
                                        }
                                    }
                                    "identifier" | "type_identifier" => {
                                        // JS: parent class identifier is a direct child of class_heritage
                                        let target = hchild.utf8_text(source)?;
                                        edges.push(Edge {
                                            source_fqn: fqn.clone(),
                                            target_fqn: target.to_string(),
                                            kind: EdgeKind::Extends,
                                        });
                                    }
                                    _ => {}
                                }
                            }
                        }
                        "class_body" => {
                            // current_fqn is None inside class body — not yet inside a method
                            extract_nodes(
                                child,
                                source,
                                relative_path,
                                Some(name),
                                None,
                                symbols,
                                edges,
                            )?;
                        }
                        _ => {}
                    }
                }
            }
        }

        "method_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let fqn = make_fqn(relative_path, parent_class, name);
                let source_hash = blake3::hash(&source[node.start_byte()..node.end_byte()])
                    .to_hex()
                    .to_string();
                symbols.push(Symbol {
                    fqn: fqn.clone(),
                    name: name.to_string(),
                    kind: SymbolKind::Method,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });
                // Recurse into method body with this method as the enclosing symbol
                for child in node.children(&mut node.walk()) {
                    extract_nodes(
                        child,
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

        "interface_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let fqn = make_fqn(relative_path, parent_class, name);
                let source_hash = blake3::hash(&source[node.start_byte()..node.end_byte()])
                    .to_hex()
                    .to_string();
                symbols.push(Symbol {
                    fqn,
                    name: name.to_string(),
                    kind: SymbolKind::Interface,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });
            }
        }

        "type_alias_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let fqn = make_fqn(relative_path, parent_class, name);
                let source_hash = blake3::hash(&source[node.start_byte()..node.end_byte()])
                    .to_hex()
                    .to_string();
                symbols.push(Symbol {
                    fqn,
                    name: name.to_string(),
                    kind: SymbolKind::TypeAlias,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });
            }
        }

        "lexical_declaration" | "variable_declaration" => {
            // Look for `const foo = () => {}` or `const foo = function() {}`
            for child in node.children(&mut node.walk()) {
                if child.kind() == "variable_declarator"
                    && let Some(name_node) = child.child_by_field_name("name")
                    && let Some(value_node) = child.child_by_field_name("value")
                    && (value_node.kind() == "arrow_function"
                        || value_node.kind() == "function"
                        || value_node.kind() == "function_expression")
                {
                    let name = name_node.utf8_text(source)?;
                    let fqn = make_fqn(relative_path, parent_class, name);
                    let source_hash = blake3::hash(&source[child.start_byte()..child.end_byte()])
                        .to_hex()
                        .to_string();
                    symbols.push(Symbol {
                        fqn: fqn.clone(),
                        name: name.to_string(),
                        kind: SymbolKind::Function,
                        start_line: child.start_position().row as u32 + 1,
                        end_line: child.end_position().row as u32 + 1,
                        signature: None,
                        docstring: None,
                        source_hash,
                    });
                    // Recurse into arrow/function body with this symbol as the enclosing context
                    extract_nodes(
                        value_node,
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

        "export_statement" => {
            // Recurse into inner declaration — export itself is not a symbol
            for child in node.children(&mut node.walk()) {
                extract_nodes(
                    child,
                    source,
                    relative_path,
                    parent_class,
                    current_fqn,
                    symbols,
                    edges,
                )?;
            }
        }

        "import_statement" => {
            // source_fqn is the file path (permitted exception — see Edge doc in symbols.rs)
            // target_fqn is the module specifier — may not exist in the symbols table
            if let Some(source_node) = node.child_by_field_name("source") {
                let module_path = source_node
                    .utf8_text(source)?
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                edges.push(Edge {
                    source_fqn: relative_path.to_string(),
                    target_fqn: module_path,
                    kind: EdgeKind::Imports,
                });
            }
        }

        "call_expression" => {
            // Best-effort: only simple identifier callees (skip chained/computed)
            // Only emit when we know the enclosing symbol FQN
            if let Some(caller_fqn) = current_fqn
                && let Some(function_node) = node.child_by_field_name("function")
                && function_node.kind() == "identifier"
            {
                let callee = function_node.utf8_text(source)?;
                edges.push(Edge {
                    source_fqn: caller_fqn.to_string(),
                    target_fqn: callee.to_string(),
                    kind: EdgeKind::Calls,
                });
            }
            // Always recurse into arguments with the same enclosing context
            for child in node.children(&mut node.walk()) {
                extract_nodes(
                    child,
                    source,
                    relative_path,
                    parent_class,
                    current_fqn,
                    symbols,
                    edges,
                )?;
            }
        }

        "type_annotation" => {
            // Best-effort: capture simple identifier types as UsesType edges
            // Only emit when we know the enclosing symbol FQN
            if let Some(caller_fqn) = current_fqn {
                for child in node.children(&mut node.walk()) {
                    if child.kind() == "type_identifier" || child.kind() == "predefined_type" {
                        let type_name = child.utf8_text(source)?;
                        if !matches!(
                            type_name,
                            "string" | "number" | "boolean" | "void" | "any" | "never" | "unknown"
                        ) {
                            edges.push(Edge {
                                source_fqn: caller_fqn.to_string(),
                                target_fqn: type_name.to_string(),
                                kind: EdgeKind::UsesType,
                            });
                        }
                    }
                }
            }
        }

        _ => {
            // Generic recursion — pass both parent_class and current_fqn unchanged
            for child in node.children(&mut node.walk()) {
                extract_nodes(
                    child,
                    source,
                    relative_path,
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
