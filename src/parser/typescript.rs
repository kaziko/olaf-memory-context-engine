use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, extract_signature, make_child_symbol, make_fqn};

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
                    signature: extract_signature(source, node),
                    docstring: None,
                    source_hash,
                    parent_fqn: None,
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
                    signature: extract_signature(source, node),
                    docstring: None,
                    source_hash,
                    parent_fqn: None,
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
                    signature: extract_signature(source, node),
                    docstring: None,
                    source_hash,
                    parent_fqn: None,
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
                    signature: extract_signature(source, node),
                    docstring: None,
                    source_hash,
                    parent_fqn: None,
                });
                // Extract interface member signatures as children
                if let Some(body) = node.child_by_field_name("body") {
                    for child in body.children(&mut body.walk()) {
                        match child.kind() {
                            "property_signature" | "method_signature" => {
                                if let Some(member_name) = child.child_by_field_name("name") {
                                    let mname = member_name.utf8_text(source)?;
                                    let kind = if child.kind() == "property_signature" {
                                        SymbolKind::Field
                                    } else {
                                        SymbolKind::TraitMethod
                                    };
                                    symbols.push(make_child_symbol(
                                        relative_path, name, mname, kind, child, source,
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        "public_field_definition" | "field_definition" => {
            // tree-sitter-typescript uses public_field_definition with a named "name" grammar field.
            // tree-sitter-javascript uses field_definition with a named "property" grammar field.
            // Do NOT fall back to named_child(0): when a decorator is present, named_child(0)
            // returns the decorator node, not the property name.
            if let Some(parent) = parent_class {
                let name_node_opt = node.child_by_field_name("name")
                    .or_else(|| node.child_by_field_name("property"));
                if let Some(name_node) = name_node_opt {
                    let name = name_node.utf8_text(source)?;
                    symbols.push(make_child_symbol(
                        relative_path, parent, name, SymbolKind::Field, node, source,
                    ));
                }
            }
        }

        "enum_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let fqn = make_fqn(relative_path, parent_class, name);
                let source_hash = blake3::hash(&source[node.start_byte()..node.end_byte()])
                    .to_hex()
                    .to_string();
                symbols.push(Symbol {
                    fqn,
                    name: name.to_string(),
                    kind: SymbolKind::Enum,
                    start_line: node.start_position().row as u32 + 1,
                    end_line: node.end_position().row as u32 + 1,
                    signature: extract_signature(source, node),
                    docstring: None,
                    source_hash,
                    parent_fqn: None,
                });
                // Extract enum members as children
                if let Some(body) = node.child_by_field_name("body") {
                    for child in body.children(&mut body.walk()) {
                        if (child.kind() == "enum_assignment" || child.kind() == "property_identifier")
                            && let Some(member_name) = child.child_by_field_name("name")
                                .or_else(|| if child.kind() == "property_identifier" { Some(child) } else { None })
                            {
                                let mname = member_name.utf8_text(source)?;
                                symbols.push(make_child_symbol(
                                    relative_path, name, mname,
                                    SymbolKind::EnumVariant, child, source,
                                ));
                            }
                    }
                }
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
                    signature: extract_signature(source, node).or_else(|| {
                        // type_alias_declaration has no body delimiter ({, body child, or :\n),
                        // so extract_signature returns None. Fall back to first line of node text,
                        // which captures the full declaration e.g. `type Result<T> = Success<T> | Error`.
                        std::str::from_utf8(&source[node.start_byte()..node.end_byte()])
                            .ok()
                            .and_then(|s| s.lines().next())
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                    }),
                    docstring: None,
                    source_hash,
                    parent_fqn: None,
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
                        signature: extract_signature(source, value_node),
                        docstring: None,
                        source_hash,
                        parent_fqn: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_file_returns_no_symbols() {
        let (symbols, edges) = parse("empty.ts", b"", TsDialect::TypeScript).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_file_with_only_comments() {
        let src = b"// This is a comment\n/* block comment */\n";
        let (symbols, edges) = parse("comments.ts", src, TsDialect::TypeScript).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_malformed_syntax_does_not_panic() {
        let src = b"function foo( { return; }\nclass {";
        let result = parse("bad.ts", src, TsDialect::TypeScript);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_jsx_dialect_empty_file() {
        let (symbols, edges) = parse("empty.jsx", b"", TsDialect::Jsx).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn interface_members_extracted_as_children() {
        let src = b"
interface Config {
    name: string;
    getValue(key: string): number;
}
";
        let (symbols, _) = parse("test.ts", src, TsDialect::TypeScript).unwrap();
        let iface = symbols.iter().find(|s| s.name == "Config").unwrap();
        assert_eq!(iface.kind, SymbolKind::Interface);

        let fields: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Field).collect();
        let methods: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::TraitMethod).collect();
        assert_eq!(fields.len(), 1, "interface property signature");
        assert_eq!(fields[0].name, "name");
        assert_eq!(methods.len(), 1, "interface method signature");
        assert_eq!(methods[0].name, "getValue");
    }

    #[test]
    fn enum_members_use_correct_symbol_kind() {
        let src = b"
enum Status {
    Active = \"active\",
    Inactive = \"inactive\",
}
";
        let (symbols, _) = parse("test.ts", src, TsDialect::TypeScript).unwrap();
        let enum_sym = symbols.iter().find(|s| s.name == "Status").unwrap();
        assert_eq!(enum_sym.kind, SymbolKind::Enum, "enum_declaration must use SymbolKind::Enum, not Class");

        let variants: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::EnumVariant).collect();
        assert_eq!(variants.len(), 2);
        assert!(variants.iter().any(|s| s.name == "Active"));
        assert!(variants.iter().any(|s| s.name == "Inactive"));
    }

    #[test]
    fn type_alias_signature_captured() {
        let src = b"type Result<T> = Success<T> | Error;\n";
        let (symbols, _) = parse("test.ts", src, TsDialect::TypeScript).unwrap();
        let alias = symbols.iter().find(|s| s.name == "Result").unwrap();
        assert_eq!(alias.kind, SymbolKind::TypeAlias);
        let sig = alias.signature.as_deref().unwrap_or("");
        assert!(!sig.is_empty(), "type alias signature must not be None");
        assert_eq!(sig, "type Result<T> = Success<T> | Error;",
            "signature must be the exact first line of the declaration");
    }

    #[test]
    fn js_class_field_extracted() {
        let src = b"
class Animal {
    name = \"default\";
    constructor(name) { this.name = name; }
}
";
        let (symbols, _) = parse("test.js", src, TsDialect::JavaScript).unwrap();
        let fields: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Field).collect();
        assert!(!fields.is_empty(), "JS class field_definition must be extracted as Field child");
        assert!(fields.iter().any(|s| s.name == "name"), "field 'name' must be extracted");
    }

    #[test]
    fn js_decorated_class_field_name_not_decorator() {
        // Regression guard for named_child(0) fallback bug: when a JS class field has a
        // decorator, named_child(0) returns the decorator node, not the property name.
        // child_by_field_name("property") must be used instead.
        let src = b"
class Tracker {
    @observed count = 0;
}
";
        let (symbols, _) = parse("test.js", src, TsDialect::JavaScript).unwrap();
        let fields: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Field).collect();
        assert!(!fields.is_empty(), "decorated JS class field must be extracted");
        assert!(
            fields.iter().any(|s| s.name == "count"),
            "field name must be 'count', not the decorator; got: {:?}",
            fields.iter().map(|s| &s.name).collect::<Vec<_>>()
        );
    }
}
