use tree_sitter::Parser;

use std::collections::HashSet;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, make_child_symbol, make_fqn, make_symbol};

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
                symbols.push(make_symbol(
                    relative_path,
                    parent_class,
                    name,
                    kind,
                    node,
                    source,
                ));
                // Recurse into body with parent_class reset to None — nested functions
                // inside a method body are not class members.
                if let Some(body) = node.child_by_field_name("body") {
                    extract_nodes(
                        body,
                        source,
                        relative_path,
                        None,
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
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Class,
                    node,
                    source,
                ));
                if let Some(body) = node.child_by_field_name("body") {
                    let class_fqn = make_fqn(relative_path, None, name);

                    // Walk direct class body children to:
                    // (a) emit Field symbols for typed annotations (e.g. `name: str = "x"`), and
                    // (b) build a suppress set for extract_init_attrs so typed fields always win
                    //     over __init__ self-assignments, regardless of source order.
                    //
                    // Direct-children-only is intentional — it excludes typed assignments nested
                    // inside class-level if/for/try blocks, which must not become Field children.
                    // In tree-sitter-python, class body statements are wrapped in expression_statement,
                    // so we unwrap one level before checking for `assignment` with a type field.
                    let mut typed_field_names: HashSet<String> = HashSet::new();
                    let mut pre_walker = body.walk();
                    for child in body.children(&mut pre_walker) {
                        let assign = if child.kind() == "assignment" {
                            Some(child)
                        } else if child.kind() == "expression_statement" {
                            child.named_child(0).filter(|n| n.kind() == "assignment")
                        } else {
                            None
                        };
                        if let Some(assign) = assign {
                            if assign.child_by_field_name("type").is_some() {
                                if let Some(left) = assign.child_by_field_name("left") {
                                    if left.kind() == "identifier" {
                                        if let Ok(n) = left.utf8_text(source) {
                                            typed_field_names.insert(n.to_string());
                                            symbols.push(make_child_symbol(
                                                relative_path,
                                                name,
                                                n,
                                                SymbolKind::Field,
                                                assign,
                                                source,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Find __init__ in the class body and extract direct self-attribute assignments,
                    // suppressing any names already covered by typed class-body fields.
                    let mut body_walker = body.walk();
                    for child in body.children(&mut body_walker) {
                        let fn_node = if child.kind() == "function_definition" {
                            Some(child)
                        } else if child.kind() == "decorated_definition" {
                            child
                                .child_by_field_name("definition")
                                .filter(|d| d.kind() == "function_definition")
                        } else {
                            None
                        };
                        if let Some(fn_node) = fn_node {
                            if let Some(fn_name_node) = fn_node.child_by_field_name("name") {
                                if fn_name_node.utf8_text(source)? == "__init__" {
                                    if let Some(init_body) = fn_node.child_by_field_name("body") {
                                        extract_init_attrs(
                                            relative_path,
                                            name,
                                            &init_body,
                                            source,
                                            &typed_field_names,
                                            symbols,
                                        )?;
                                    }
                                }
                            }
                        }
                    }

                    // Normal recursion handles method symbols, edges, etc.
                    // Typed Field children are already emitted above — do not re-emit via recursion.
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
                extract_nodes(
                    inner,
                    source,
                    relative_path,
                    parent_class,
                    _current_fqn,
                    symbols,
                    edges,
                )?;
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
                extract_nodes(
                    child,
                    source,
                    relative_path,
                    parent_class,
                    _current_fqn,
                    symbols,
                    edges,
                )?;
            }
        }
    }
    Ok(())
}

/// Extract direct `self.<attr> = value` assignments from an `__init__` body node.
///
/// Walks only the immediate children of the body (not recursive) to avoid capturing
/// attributes assigned inside nested scopes like `if`/`for`/`try` blocks.
/// Names present in `suppress` are skipped — they are covered by typed class-body fields
/// which always take precedence.
fn extract_init_attrs(
    relative_path: &str,
    parent_class: &str,
    init_body: &tree_sitter::Node<'_>,
    source: &[u8],
    suppress: &HashSet<String>,
    symbols: &mut Vec<Symbol>,
) -> Result<(), ParserError> {
    let mut walker = init_body.walk();
    for stmt in init_body.children(&mut walker) {
        if stmt.kind() != "expression_statement" {
            continue;
        }
        let Some(expr) = stmt.named_child(0) else { continue };
        if expr.kind() != "assignment" {
            continue;
        }
        let Some(left) = expr.child_by_field_name("left") else { continue };
        if left.kind() != "attribute" {
            continue;
        }
        // Require object to be `self`
        let Some(obj) = left.child_by_field_name("object") else { continue };
        if obj.utf8_text(source)? != "self" {
            continue;
        }
        let Some(attr_field) = left.child_by_field_name("attribute") else { continue };
        let attr_name = attr_field.utf8_text(source)?;
        if suppress.contains(attr_name) {
            continue;
        }
        // Pass the assignment node (expr) — its text is `self.path = path`, not just `self.path`
        symbols.push(make_child_symbol(
            relative_path,
            parent_class,
            attr_name,
            SymbolKind::Field,
            expr,
            source,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_file_returns_no_symbols() {
        let (symbols, edges) = parse("empty.py", b"").unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_file_with_only_comments() {
        let src = b"# just a comment\n# another comment\n";
        let (symbols, edges) = parse("comments.py", src).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_malformed_syntax_does_not_panic() {
        let src = b"def foo(\n  x = 1\n    y = 2\n";
        let result = parse("indent.py", src);
        assert!(result.is_ok());
    }

    #[test]
    fn python_typed_class_var_extracted() {
        let src = b"class Foo:\n    name: str = \"x\"\n";
        let (symbols, _) = parse("foo.py", src).unwrap();
        let field = symbols.iter().find(|s| s.name == "name" && s.kind == SymbolKind::Field);
        assert!(field.is_some(), "Typed class-body assignment must emit a Field child");
    }

    #[test]
    fn python_untyped_class_var_not_extracted() {
        let src = b"class Foo:\n    count = 0\n";
        let (symbols, _) = parse("foo.py", src).unwrap();
        let field = symbols.iter().find(|s| s.kind == SymbolKind::Field);
        assert!(field.is_none(), "Plain assignment without type annotation must not emit a Field");
    }

    #[test]
    fn python_init_self_attr_extracted() {
        let src = b"class Foo:\n    def __init__(self):\n        self.x = 42\n";
        let (symbols, _) = parse("foo.py", src).unwrap();
        let field = symbols.iter().find(|s| s.name == "x" && s.kind == SymbolKind::Field);
        assert!(field.is_some(), "__init__ direct self.x assignment must emit a Field child");
        let sig = field.unwrap().signature.as_deref().unwrap_or("");
        assert!(sig.contains("self.x"), "Signature must be the assignment node text; got: {sig:?}");
    }

    #[test]
    fn python_init_nested_attr_not_extracted() {
        let src = b"class Foo:\n    def __init__(self):\n        if True:\n            self.x = 1\n";
        let (symbols, _) = parse("foo.py", src).unwrap();
        let field = symbols.iter().find(|s| s.kind == SymbolKind::Field);
        assert!(field.is_none(), "Nested self.x (inside if) must not emit a Field — direct-only rule");
    }

    #[test]
    fn python_nested_typed_class_var_not_extracted() {
        // A typed assignment inside a class-level if block must not become a Field —
        // only direct class body children qualify.
        let src = b"class Foo:\n    if True:\n        debug: bool = True\n";
        let (symbols, _) = parse("foo.py", src).unwrap();
        let fields: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Field).collect();
        assert!(
            fields.is_empty(),
            "Typed assignment nested inside class-level if block must not emit a Field; got: {fields:?}"
        );
    }

    #[test]
    fn python_typed_field_suppresses_init_duplicate() {
        // __init__ appears BEFORE the typed class variable in source —
        // the suppress-set mechanism must ensure the typed annotation wins.
        let src = b"class Foo:\n    def __init__(self):\n        self.name = \"default\"\n    name: str = \"x\"\n";
        let (symbols, _) = parse("foo.py", src).unwrap();
        let fields: Vec<_> = symbols.iter().filter(|s| s.name == "name" && s.kind == SymbolKind::Field).collect();
        assert_eq!(fields.len(), 1, "Exactly one Field child for 'name' expected; got: {fields:?}");
        let sig = fields[0].signature.as_deref().unwrap_or("");
        assert!(
            sig.contains("str"),
            "The typed annotation must win; expected 'str' in signature, got: {sig:?}"
        );
    }
}
