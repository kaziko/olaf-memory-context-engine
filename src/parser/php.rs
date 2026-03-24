use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, extract_signature, make_child_symbol, make_fqn, make_symbol};

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

fn qualify_php_name(ns: Option<&str>, raw_name: &str) -> String {
    match ns {
        Some(ns) => format!("{}\\{}", ns, raw_name),
        None => raw_name.to_string(),
    }
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
                    parent_fqn: None,
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
                let qualified_name = qualify_php_name(current_namespace.as_deref(), raw_name);
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
        "property_declaration" => {
            // Class property — extract as Field child. Pass the outer property_declaration node
            // (not the inner property_element) so that visibility and type modifiers like
            // `public string` appear in the signature.
            if let Some(parent) = parent_class {
                let mut walker = node.walk();
                for child in node.children(&mut walker) {
                    if child.kind() == "property_element"
                        && let Some(var) = child.child(0)
                            && var.kind() == "variable_name" {
                                let prop_name = var.utf8_text(source)?.trim_start_matches('$');
                                symbols.push(make_child_symbol(
                                    relative_path, parent, prop_name,
                                    SymbolKind::Field, node, source,
                                ));
                            }
                }
            }
        }
        "const_declaration" => {
            // Class-level constants — hybrid signature rule:
            // Single const_element → outer node preserves visibility (e.g., `public const STATUS = 1`)
            // Multiple const_elements → inner nodes for per-constant specificity (`A = 1`, `B = 2`)
            if let Some(parent) = parent_class {
                let elements: Vec<_> = {
                    let mut w = node.walk();
                    node.children(&mut w)
                        .filter(|c| c.kind() == "const_element")
                        .collect()
                };
                let use_outer = elements.len() == 1;
                for elem in &elements {
                    // const_element has no field-name children in tree-sitter-php grammar;
                    // find the `name` node by kind among named children.
                    let mut ew = elem.walk();
                    if let Some(name_node) = elem.named_children(&mut ew).find(|c| c.kind() == "name") {
                        let const_name = name_node.utf8_text(source)?;
                        let sig_node = if use_outer { node } else { *elem };
                        symbols.push(make_child_symbol(
                            relative_path,
                            parent,
                            const_name,
                            SymbolKind::Constant,
                            sig_node,
                            source,
                        ));
                    }
                }
            }
        }
        "interface_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let raw_name = name_node.utf8_text(source)?;
                let qualified_name = qualify_php_name(current_namespace.as_deref(), raw_name);
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    &qualified_name,
                    SymbolKind::Interface,
                    node,
                    source,
                ));
                // body field is typed as declaration_list in tree-sitter-php grammar
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        if child.kind() == "method_declaration"
                            && let Some(mname_node) = child.child_by_field_name("name")
                        {
                            let mname = mname_node.utf8_text(source)?;
                            symbols.push(make_child_symbol(
                                relative_path, &qualified_name, mname,
                                SymbolKind::TraitMethod, child, source,
                            ));
                        }
                    }
                }
            }
        }
        "function_definition" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let raw_name = name_node.utf8_text(source)?;
                let qualified_name = qualify_php_name(current_namespace.as_deref(), raw_name);
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
            // Inside a class body: trait usage → UsesTrait edge + child symbol per trait
            if let Some(parent) = parent_class {
                let class_fqn = make_fqn(relative_path, None, parent);
                let mut walker = node.walk();
                for child in node.named_children(&mut walker) {
                    if child.kind() == "name"
                        || child.kind() == "qualified_name"
                        || child.kind() == "named_type"
                    {
                        let raw_name = child.utf8_text(source)?;
                        // FQ names (starting with \): strip leading \, don't re-prefix
                        let qualified = if raw_name.starts_with('\\') {
                            raw_name.trim_start_matches('\\').to_string()
                        } else {
                            qualify_php_name(current_namespace.as_deref(), raw_name)
                        };
                        edges.push(Edge {
                            source_fqn: class_fqn.clone(),
                            target_fqn: qualified.clone(),
                            kind: EdgeKind::UsesTrait,
                        });
                        // Child symbol per trait — visible in skeleton as #### entry.
                        // Use raw_name for the symbol name (short, unqualified) so
                        // FQN stays clean (file::Parent::Loggable not file::Parent::Ns\Loggable).
                        // The qualified form lives in the edge target_fqn for graph resolution.
                        let short_name = raw_name.trim_start_matches('\\');
                        symbols.push(make_child_symbol(
                            relative_path,
                            parent,
                            short_name,
                            SymbolKind::Field,
                            child,
                            source,
                        ));
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

    #[test]
    fn php_property_visibility_in_signature() {
        let src = b"<?php\nclass Widget {\n    public string $name = \"x\";\n}\n";
        let (symbols, _) = parse("widget.php", src).unwrap();
        let field = symbols.iter().find(|s| s.name == "name").expect("Field 'name' not found");
        let sig = field.signature.as_deref().unwrap_or("");
        assert!(
            sig.contains("public"),
            "Field signature must include visibility modifier 'public'; got: {sig:?}"
        );
    }

    #[test]
    fn php_interface_methods_extracted() {
        let src = b"<?php\ninterface Renderable {\n    public function render(): string;\n    public function getLabel(): string;\n}\n";
        let (symbols, _) = parse("iface.php", src).unwrap();
        let iface = symbols.iter().find(|s| s.name == "Renderable").expect("Interface not found");
        assert_eq!(iface.kind, SymbolKind::Interface);
        let methods: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::TraitMethod).collect();
        assert_eq!(methods.len(), 2, "Expected 2 TraitMethod children; got: {methods:?}");
        let names: Vec<&str> = methods.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"render"), "Missing 'render' method");
        assert!(names.contains(&"getLabel"), "Missing 'getLabel' method");
    }

    #[test]
    fn php_class_constant_extracted() {
        let src = b"<?php\nclass Foo {\n    const STATUS = 1;\n}\n";
        let (symbols, _) = parse("foo.php", src).unwrap();
        let c = symbols.iter().find(|s| s.name == "STATUS" && s.kind == SymbolKind::Constant);
        assert!(c.is_some(), "Class constant STATUS must be extracted as Constant child");
    }

    #[test]
    fn php_single_constant_preserves_visibility() {
        let src = b"<?php\nclass Foo {\n    public const STATUS = 1;\n}\n";
        let (symbols, _) = parse("foo.php", src).unwrap();
        let c = symbols.iter().find(|s| s.name == "STATUS").expect("Constant not found");
        let sig = c.signature.as_deref().unwrap_or("");
        assert!(
            sig.contains("public"),
            "Single constant signature must contain visibility; got: {sig:?}"
        );
    }

    #[test]
    fn php_multi_constant_each_separate() {
        let src = b"<?php\nclass Foo {\n    const A = 1, B = 2;\n}\n";
        let (symbols, _) = parse("foo.php", src).unwrap();
        let consts: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Constant).collect();
        assert_eq!(consts.len(), 2, "Each const_element must emit a separate Constant; got: {consts:?}");
        let names: Vec<&str> = consts.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"A"), "Missing constant A");
        assert!(names.contains(&"B"), "Missing constant B");
    }

    #[test]
    fn php_trait_use_emits_child_symbols() {
        let src = b"<?php\nclass Foo {\n    use Loggable, Serializable;\n}\n";
        let (symbols, edges) = parse("foo.php", src).unwrap();
        let fields: Vec<_> = symbols.iter().filter(|s| s.kind == SymbolKind::Field).collect();
        assert_eq!(fields.len(), 2, "Each trait must emit a Field child; got: {fields:?}");
        let trait_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::UsesTrait).collect();
        assert_eq!(trait_edges.len(), 2, "Each trait must emit a UsesTrait edge; got: {trait_edges:?}");
    }

    #[test]
    fn php_trait_use_namespace_qualified() {
        let src = b"<?php\nnamespace App;\nclass Foo {\n    use Loggable;\n}\n";
        let (symbols, edges) = parse("foo.php", src).unwrap();
        let field = symbols.iter().find(|s| s.kind == SymbolKind::Field);
        assert!(field.is_some(), "Trait use must emit a Field child");
        assert_eq!(field.unwrap().name, "Loggable", "Symbol name must be short/unqualified");
        let edge = edges.iter().find(|e| e.kind == EdgeKind::UsesTrait);
        assert!(edge.is_some());
        assert_eq!(edge.unwrap().target_fqn, "App\\Loggable", "Edge target must be namespace-prefixed");
    }

    #[test]
    fn php_fully_qualified_trait_not_double_prefixed() {
        // `use \App\Loggable;` inside `namespace MyNs;`:
        // Leading `\` is stripped, original FQN "App\Loggable" preserved.
        // Must NOT be re-prefixed with current namespace (MyNs\App\Loggable).
        let src = b"<?php\nnamespace MyNs;\nclass Foo {\n    use \\App\\Loggable;\n}\n";
        let (symbols, edges) = parse("foo.php", src).unwrap();
        let field = symbols.iter().find(|s| s.kind == SymbolKind::Field);
        assert!(field.is_some(), "FQ trait use must emit a Field child");
        assert_eq!(
            field.unwrap().name, "App\\Loggable",
            "Leading '\\' removed, original FQN preserved (no re-prefixing with current namespace MyNs)"
        );
        let edge = edges.iter().find(|e| e.kind == EdgeKind::UsesTrait);
        assert!(edge.is_some());
        assert_eq!(
            edge.unwrap().target_fqn, "App\\Loggable",
            "Edge target: leading '\\' removed, original FQN preserved (no re-prefixing with MyNs)"
        );
    }

    #[test]
    fn php_relative_qualified_trait_not_double_prefixed() {
        // `use Sub\Loggable;` inside `namespace MyNs;` — relative qualified name
        // should NOT be double-prefixed as `MyNs\Sub\Loggable` in the symbol name
        let src = b"<?php\nnamespace MyNs;\nclass Foo {\n    use Sub\\Loggable;\n}\n";
        let (symbols, edges) = parse("foo.php", src).unwrap();
        let field = symbols.iter().find(|s| s.kind == SymbolKind::Field);
        assert!(field.is_some(), "Relative qualified trait use must emit a Field child");
        // Symbol name: short form from source
        let name = &field.unwrap().name;
        assert!(
            !name.starts_with("MyNs\\"),
            "Symbol name must not be namespace-prefixed; got: {name:?}"
        );
        // Edge target: namespace-qualified for graph resolution
        let edge = edges.iter().find(|e| e.kind == EdgeKind::UsesTrait);
        assert!(edge.is_some());
        let target = &edge.unwrap().target_fqn;
        assert_eq!(target, "MyNs\\Sub\\Loggable", "Edge target must be namespace-prefixed");
    }
}
