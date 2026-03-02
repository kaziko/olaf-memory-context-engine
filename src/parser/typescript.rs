use tree_sitter::Parser;

use super::symbols::{make_fqn, Edge, EdgeKind, ParserError, Symbol, SymbolKind};

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
    extract_nodes(root, source, relative_path, None, &mut symbols, &mut edges)?;
    Ok((symbols, edges))
}

fn extract_nodes<'a>(
    node: tree_sitter::Node<'a>,
    source: &[u8],
    relative_path: &str,
    parent_class: Option<&str>,
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
                    fqn,
                    name: name.to_string(),
                    kind: SymbolKind::Function,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });
                // Recurse into body to catch nested calls/types
                for child in node.children(&mut node.walk()) {
                    extract_nodes(child, source, relative_path, parent_class, symbols, edges)?;
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

                // Handle extends_clause and implements_clause before body recursion
                for child in node.children(&mut node.walk()) {
                    match child.kind() {
                        "extends_clause" => {
                            // extends_clause has children: "extends" keyword + type/identifier
                            for grandchild in child.children(&mut child.walk()) {
                                if grandchild.kind() == "identifier"
                                    || grandchild.kind() == "type_identifier"
                                {
                                    let target = grandchild.utf8_text(source)?;
                                    edges.push(Edge {
                                        source_fqn: fqn.clone(),
                                        target_fqn: target.to_string(),
                                        kind: EdgeKind::Extends,
                                    });
                                }
                            }
                        }
                        "implements_clause" => {
                            for grandchild in child.children(&mut child.walk()) {
                                if grandchild.kind() == "type_identifier"
                                    || grandchild.kind() == "identifier"
                                    || grandchild.kind() == "generic_type"
                                {
                                    // For generic_type, get the actual type name (first child)
                                    let target = if grandchild.kind() == "generic_type" {
                                        grandchild
                                            .child(0)
                                            .and_then(|n| n.utf8_text(source).ok())
                                            .unwrap_or("")
                                    } else {
                                        grandchild.utf8_text(source)?
                                    };
                                    if !target.is_empty() {
                                        edges.push(Edge {
                                            source_fqn: fqn.clone(),
                                            target_fqn: target.to_string(),
                                            kind: EdgeKind::Implements,
                                        });
                                    }
                                }
                            }
                        }
                        "class_body" => {
                            extract_nodes(
                                child,
                                source,
                                relative_path,
                                Some(name),
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
                    fqn,
                    name: name.to_string(),
                    kind: SymbolKind::Method,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: None,
                    docstring: None,
                    source_hash,
                });
                // Do NOT recurse into method body for nested class detection
                // but DO recurse for best-effort call/type edges
                for child in node.children(&mut node.walk()) {
                    extract_nodes(child, source, relative_path, parent_class, symbols, edges)?;
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
                    let source_hash =
                        blake3::hash(&source[child.start_byte()..child.end_byte()])
                            .to_hex()
                            .to_string();
                    symbols.push(Symbol {
                        fqn,
                        name: name.to_string(),
                        kind: SymbolKind::Function,
                        start_line: child.start_position().row as u32 + 1,
                        end_line: child.end_position().row as u32 + 1,
                        signature: None,
                        docstring: None,
                        source_hash,
                    });
                    // Recurse into arrow function body for calls/types
                    extract_nodes(
                        value_node,
                        source,
                        relative_path,
                        parent_class,
                        symbols,
                        edges,
                    )?;
                }
            }
        }

        "export_statement" => {
            // Recurse into inner declaration — export itself is not a symbol
            for child in node.children(&mut node.walk()) {
                extract_nodes(child, source, relative_path, parent_class, symbols, edges)?;
            }
        }

        "import_statement" => {
            // source_fqn is the file itself (path reference, not a symbol FQN)
            // target_fqn is the module specifier — may not exist in symbols table
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
            if let Some(function_node) = node.child_by_field_name("function")
                && function_node.kind() == "identifier"
            {
                let callee = function_node.utf8_text(source)?;
                // Only emit if there's a known parent context (inside a function/method)
                if parent_class.is_some() {
                    let caller_fqn = make_fqn(relative_path, parent_class, "");
                    // Trim trailing "::" from the caller FQN
                    let caller_fqn = caller_fqn.trim_end_matches("::").to_string();
                    edges.push(Edge {
                        source_fqn: caller_fqn,
                        target_fqn: callee.to_string(),
                        kind: EdgeKind::Calls,
                    });
                }
            }
            // Always recurse into arguments
            for child in node.children(&mut node.walk()) {
                extract_nodes(child, source, relative_path, parent_class, symbols, edges)?;
            }
        }

        "type_annotation" => {
            // Best-effort: capture simple identifier types as UsesType edges
            for child in node.children(&mut node.walk()) {
                if child.kind() == "type_identifier" || child.kind() == "predefined_type" {
                    // Skip primitive types
                    let type_name = child.utf8_text(source)?;
                    if !matches!(
                        type_name,
                        "string" | "number" | "boolean" | "void" | "any" | "never" | "unknown"
                    ) {
                        let source_fqn = if let Some(parent) = parent_class {
                            make_fqn(relative_path, Some(parent), "")
                                .trim_end_matches("::")
                                .to_string()
                        } else {
                            relative_path.to_string()
                        };
                        edges.push(Edge {
                            source_fqn,
                            target_fqn: type_name.to_string(),
                            kind: EdgeKind::UsesType,
                        });
                    }
                }
            }
        }

        _ => {
            // Generic recursion for all unhandled node kinds
            for child in node.children(&mut node.walk()) {
                extract_nodes(child, source, relative_path, parent_class, symbols, edges)?;
            }
        }
    }
    Ok(())
}
