use olaf::parser::{EdgeKind, Language, detect_language, parse_file};

#[test]
fn test_detect_language_extensions() {
    assert!(matches!(
        detect_language("foo.ts"),
        Some(Language::TypeScript)
    ));
    assert!(matches!(detect_language("foo.tsx"), Some(Language::Tsx)));
    assert!(matches!(
        detect_language("foo.js"),
        Some(Language::JavaScript)
    ));
    assert!(matches!(detect_language("foo.jsx"), Some(Language::Jsx)));
    assert!(matches!(detect_language("foo.py"), Some(Language::Python)));
    assert!(matches!(detect_language("foo.rs"), Some(Language::Rust)));
    assert!(detect_language("README.md").is_none());
    assert!(detect_language("Makefile").is_none());
    assert!(detect_language("foo.xyz").is_none());
}

#[test]
fn test_unknown_extension_returns_empty_without_panic() {
    let (syms, edges) = parse_file("README.md", b"# Hello").unwrap();
    assert!(syms.is_empty() && edges.is_empty());
}

#[test]
fn test_parse_typescript_exact_symbol_set() {
    let source = std::fs::read("tests/fixtures/typescript/sample.ts").unwrap();
    let (symbols, _) = parse_file("tests/fixtures/typescript/sample.ts", &source).unwrap();

    let mut fqns: Vec<String> = symbols.iter().map(|s| s.fqn.clone()).collect();
    fqns.sort();
    assert_eq!(
        fqns,
        vec![
            "tests/fixtures/typescript/sample.ts::Greeter",
            "tests/fixtures/typescript/sample.ts::Greeter::constructor",
            "tests/fixtures/typescript/sample.ts::Greeter::greet",
            "tests/fixtures/typescript/sample.ts::formatGreeting",
        ],
        "exact symbol set mismatch"
    );
}

#[test]
fn test_parse_javascript_exact_symbol_set() {
    let source = std::fs::read("tests/fixtures/typescript/sample.js").unwrap();
    let (symbols, _) = parse_file("tests/fixtures/typescript/sample.js", &source).unwrap();

    let mut fqns: Vec<String> = symbols.iter().map(|s| s.fqn.clone()).collect();
    fqns.sort();
    assert_eq!(
        fqns,
        vec![
            "tests/fixtures/typescript/sample.js::Reader",
            "tests/fixtures/typescript/sample.js::Reader::read",
            "tests/fixtures/typescript/sample.js::processData",
            "tests/fixtures/typescript/sample.js::transform",
        ],
        "exact symbol set mismatch"
    );
}

#[test]
fn test_imports_edge_source_is_file_path() {
    let source = std::fs::read("tests/fixtures/typescript/sample.ts").unwrap();
    let (_, edges) = parse_file("tests/fixtures/typescript/sample.ts", &source).unwrap();

    let import_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .collect();
    assert!(
        !import_edges.is_empty(),
        "expected at least one imports edge"
    );
    // source_fqn for imports is the file path, not a symbol FQN
    assert!(
        import_edges
            .iter()
            .all(|e| e.source_fqn == "tests/fixtures/typescript/sample.ts"),
        "imports edge source_fqn must be the file path"
    );
    // target is the module specifier string
    assert!(
        import_edges.iter().any(|e| e.target_fqn == "events"),
        "expected imports edge targeting 'events'"
    );
}

#[test]
fn test_extends_edge() {
    let source = std::fs::read("tests/fixtures/typescript/sample.ts").unwrap();
    let (_, edges) = parse_file("tests/fixtures/typescript/sample.ts", &source).unwrap();

    let extends_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Extends)
        .collect();
    assert_eq!(extends_edges.len(), 1, "expected exactly one extends edge");
    assert_eq!(
        extends_edges[0].source_fqn, "tests/fixtures/typescript/sample.ts::Greeter",
        "extends source must be the class FQN"
    );
    assert_eq!(
        extends_edges[0].target_fqn, "EventEmitter",
        "extends target must be the parent class name"
    );
}

#[test]
fn test_implements_edge() {
    let source = b"interface IFoo {} class Bar implements IFoo {}";
    let (_, edges) = parse_file("src/bar.ts", source).unwrap();

    let impl_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Implements)
        .collect();
    assert_eq!(impl_edges.len(), 1, "expected exactly one implements edge");
    assert_eq!(impl_edges[0].source_fqn, "src/bar.ts::Bar");
    assert_eq!(impl_edges[0].target_fqn, "IFoo");
}

#[test]
fn test_calls_edge_attributed_to_calling_symbol() {
    // function caller calls callee() — Calls edge source must be caller's FQN
    let source = b"function callee() {} function caller() { callee(); }";
    let (_, edges) = parse_file("src/x.ts", source).unwrap();

    let calls_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::Calls).collect();
    assert!(!calls_edges.is_empty(), "expected at least one calls edge");
    assert!(
        calls_edges
            .iter()
            .any(|e| e.source_fqn == "src/x.ts::caller" && e.target_fqn == "callee"),
        "calls edge must have the calling function as source; got: {:?}",
        calls_edges
            .iter()
            .map(|e| (&e.source_fqn, &e.target_fqn))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_calls_edge_attributed_to_method_not_class() {
    let source = b"class Foo { bar() { baz(); } }";
    let (_, edges) = parse_file("src/foo.ts", source).unwrap();

    let calls_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::Calls).collect();
    assert!(!calls_edges.is_empty(), "expected calls edge inside method");
    assert!(
        calls_edges
            .iter()
            .any(|e| e.source_fqn == "src/foo.ts::Foo::bar"),
        "calls edge source must be the method FQN, not the class; got: {:?}",
        calls_edges
            .iter()
            .map(|e| &e.source_fqn)
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_calls_edge_not_emitted_at_file_scope() {
    // A bare call at file scope has no enclosing symbol FQN — should be silently skipped
    let source = b"someFunction();";
    let (_, edges) = parse_file("src/x.ts", source).unwrap();
    let calls_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::Calls).collect();
    assert!(
        calls_edges.is_empty(),
        "calls at file scope must be skipped (no enclosing symbol); got: {:?}",
        calls_edges
    );
}

#[test]
fn test_uses_type_edge_attributed_to_function() {
    let source = b"type MyType = string; function doSomething(x: MyType): void {}";
    let (_, edges) = parse_file("src/x.ts", source).unwrap();

    let type_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::UsesType)
        .collect();
    assert!(
        type_edges
            .iter()
            .any(|e| e.source_fqn == "src/x.ts::doSomething" && e.target_fqn == "MyType"),
        "uses_type edge source must be the function FQN; got: {:?}",
        type_edges
            .iter()
            .map(|e| (&e.source_fqn, &e.target_fqn))
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_tsx_parses_symbols() {
    // TSX uses the same grammar as TS for class/function extraction
    let source = b"function MyComponent() { return null; }";
    let (symbols, _) = parse_file("src/App.tsx", source).unwrap();
    assert!(
        symbols.iter().any(|s| s.fqn == "src/App.tsx::MyComponent"),
        "TSX file must extract function symbols"
    );
}

#[test]
fn test_jsx_parses_symbols() {
    let source = b"function Widget() { return null; }";
    let (symbols, _) = parse_file("src/Widget.jsx", source).unwrap();
    assert!(
        symbols.iter().any(|s| s.fqn == "src/Widget.jsx::Widget"),
        "JSX file must extract function symbols"
    );
}

#[test]
fn test_fqn_format_class_method() {
    let source = b"class Foo { bar() {} }";
    let (symbols, _) = parse_file("src/foo.ts", source).unwrap();

    assert!(
        symbols.iter().any(|s| s.fqn == "src/foo.ts::Foo::bar"),
        "class method FQN must be path::Class::method; got: {:?}",
        symbols.iter().map(|s| &s.fqn).collect::<Vec<_>>()
    );
}

#[test]
fn test_line_numbers_are_one_based() {
    let source = b"\nfunction foo() {}"; // foo starts at line 2
    let (symbols, _) = parse_file("src/foo.ts", source).unwrap();
    if let Some(sym) = symbols.iter().find(|s| s.name == "foo") {
        assert_eq!(
            sym.start_line, 2,
            "start_line must be 1-based (tree-sitter row + 1)"
        );
    } else {
        panic!("expected function foo to be extracted");
    }
}

#[test]
fn test_source_hash_populated() {
    let source = b"function foo() {}";
    let (symbols, _) = parse_file("src/foo.ts", source).unwrap();
    assert!(
        symbols.iter().all(|s| !s.source_hash.is_empty()),
        "every symbol must have a non-empty source_hash"
    );
}

// ──── Code-review regression tests (bugs fixed after Story 1.3 review) ────

#[test]
fn test_php_namespaced_functions_are_disambiguated() {
    // Regression: top-level functions must be namespace-qualified to prevent FQN collisions.
    // Before fix: both produced `src/demo.php::f`; after fix they are distinct.
    let source = b"<?php\nnamespace A { function f() {} }\nnamespace B { function f() {} }";
    let (symbols, _) = parse_file("src/demo.php", source).unwrap();
    let fqns: Vec<_> = symbols.iter().map(|s| s.fqn.as_str()).collect();
    assert!(
        fqns.iter().any(|f| *f == "src/demo.php::A\\f"),
        "function f in namespace A must be qualified as A\\f; got: {:?}",
        fqns
    );
    assert!(
        fqns.iter().any(|f| *f == "src/demo.php::B\\f"),
        "function f in namespace B must be qualified as B\\f; got: {:?}",
        fqns
    );
    assert_eq!(
        fqns.iter().filter(|f| f.ends_with("::f")).count(),
        0,
        "no unqualified ::f FQN should exist; got: {:?}",
        fqns
    );
}

#[test]
fn test_python_nested_function_inside_method_is_not_a_method() {
    // Regression: `inner` defined inside a method must be Function, not Method.
    // Before fix: parent_class was carried into function bodies, misclassifying inner.
    let source = b"class A:\n    def m(self):\n        def inner(): pass";
    let (symbols, _) = parse_file("src/demo.py", source).unwrap();
    let inner = symbols.iter().find(|s| s.name == "inner");
    assert!(inner.is_some(), "inner function must be extracted");
    let inner = inner.unwrap();
    assert_eq!(
        inner.kind,
        olaf::parser::SymbolKind::Function,
        "nested function inside method must be SymbolKind::Function, not Method; fqn={}",
        inner.fqn
    );
    assert!(
        !inner.fqn.contains("::A::"),
        "inner function FQN must not include the class prefix; got: {}",
        inner.fqn
    );
}

#[test]
fn test_rust_pub_use_import_strips_visibility_modifier() {
    // Regression: `pub use foo;` must yield target "foo", not "pub use foo".
    let source = b"pub use std::path::Path;";
    let (_, edges) = parse_file("src/demo.rs", source).unwrap();
    let import_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .collect();
    assert_eq!(import_edges.len(), 1, "expected exactly 1 imports edge");
    assert_eq!(
        import_edges[0].target_fqn, "std::path::Path",
        "pub use target must strip visibility modifier; got: {:?}",
        import_edges[0].target_fqn
    );
}

// ──── Story 1.3: Python parser ────

#[test]
fn test_parse_python_exact_symbol_set() {
    let source = std::fs::read("tests/fixtures/python/sample.py").unwrap();
    let (symbols, edges) = parse_file("tests/fixtures/python/sample.py", &source).unwrap();

    let mut fqns: Vec<String> = symbols.iter().map(|s| s.fqn.clone()).collect();
    fqns.sort();
    assert_eq!(
        fqns,
        vec![
            "tests/fixtures/python/sample.py::FileProcessor",
            "tests/fixtures/python/sample.py::FileProcessor::process",
            "tests/fixtures/python/sample.py::FileProcessor::validate",
            "tests/fixtures/python/sample.py::read_file",
        ],
        "Python exact symbol set mismatch"
    );

    // Exact imports: verify count, unique targets, and source attribution
    let import_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .collect();
    assert_eq!(
        import_edges.len(),
        3,
        "expected exactly 3 Imports edges (os, sys, pathlib) — catches duplicate-edge bugs"
    );

    let import_targets: std::collections::BTreeSet<_> =
        import_edges.iter().map(|e| e.target_fqn.as_str()).collect();
    assert_eq!(
        import_targets,
        ["os", "pathlib", "sys"]
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        "import targets must be module names, not aliases"
    );

    // All import edges must have file path as source_fqn (permitted exception per edge contract)
    for e in &import_edges {
        assert_eq!(
            e.source_fqn, "tests/fixtures/python/sample.py",
            "Imports edge source must be file path, not a symbol FQN"
        );
    }
}

#[test]
fn test_detect_language_python() {
    assert!(matches!(detect_language("foo.py"), Some(Language::Python)));
}

// ──── Story 1.3: Rust parser ────

#[test]
fn test_parse_rust_exact_symbol_set() {
    let source = std::fs::read("tests/fixtures/rust/sample.rs").unwrap();
    let (symbols, edges) = parse_file("tests/fixtures/rust/sample.rs", &source).unwrap();

    let mut fqns: Vec<String> = symbols.iter().map(|s| s.fqn.clone()).collect();
    fqns.sort();
    assert_eq!(
        fqns,
        vec![
            "tests/fixtures/rust/sample.rs::FileReader",
            "tests/fixtures/rust/sample.rs::FileReader::exists",
            "tests/fixtures/rust/sample.rs::FileReader::new",
            "tests/fixtures/rust/sample.rs::Readable",
        ],
        "Rust exact symbol set mismatch"
    );

    // Exact imports assertion — verify target and source, not just count
    let import_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .collect();
    assert_eq!(import_edges.len(), 1, "expected exactly 1 imports edge");
    assert_eq!(
        import_edges[0].source_fqn, "tests/fixtures/rust/sample.rs",
        "Imports edge source must be file path"
    );
    assert_eq!(
        import_edges[0].target_fqn, "std::path::Path",
        "Imports edge target must be the full use path"
    );
}

#[test]
fn test_rust_impl_method_fqn_attributed_to_struct() {
    let source = b"struct Foo {} impl Foo { fn bar(&self) {} }";
    let (symbols, _) = parse_file("src/foo.rs", source).unwrap();
    assert!(
        symbols.iter().any(|s| s.fqn == "src/foo.rs::Foo::bar"),
        "impl method FQN must be path::Struct::method; got: {:?}",
        symbols.iter().map(|s| &s.fqn).collect::<Vec<_>>()
    );
}

#[test]
fn test_rust_impl_generic_type_strips_parameters() {
    // `impl Foo<T>` — method FQN must use "Foo", not "Foo<T>"
    let source = b"struct Foo<T>(T); impl<T> Foo<T> { fn get(&self) {} }";
    let (symbols, _) = parse_file("src/foo.rs", source).unwrap();
    assert!(
        symbols.iter().any(|s| s.fqn == "src/foo.rs::Foo::get"),
        "generic impl method FQN must strip type parameters; got: {:?}",
        symbols.iter().map(|s| &s.fqn).collect::<Vec<_>>()
    );
    assert!(
        !symbols.iter().any(|s| s.fqn.contains('<')),
        "no FQN should contain '<'; got: {:?}",
        symbols.iter().map(|s| &s.fqn).collect::<Vec<_>>()
    );
}

#[test]
fn test_rust_impl_qualified_path_uses_terminal_name() {
    // `impl m::Bar` — method FQN must use terminal type name "Bar", not full path "m::Bar"
    let source = b"mod m { pub struct Bar; } impl m::Bar { fn go(&self) {} }";
    let (symbols, _) = parse_file("src/foo.rs", source).unwrap();
    assert!(
        symbols.iter().any(|s| s.fqn == "src/foo.rs::Bar::go"),
        "scoped impl path must use terminal type name; got: {:?}",
        symbols.iter().map(|s| &s.fqn).collect::<Vec<_>>()
    );
}

#[test]
fn test_detect_language_rust() {
    assert!(matches!(detect_language("foo.rs"), Some(Language::Rust)));
}

// ──── Story 1.3: PHP parser ────

#[test]
fn test_parse_php_exact_symbol_set() {
    let source = std::fs::read("tests/fixtures/php/sample.php").unwrap();
    let (symbols, _) = parse_file("tests/fixtures/php/sample.php", &source).unwrap();

    let mut fqns: Vec<String> = symbols.iter().map(|s| s.fqn.clone()).collect();
    fqns.sort();
    assert_eq!(
        fqns,
        vec![
            "tests/fixtures/php/sample.php::MyPlugin",
            "tests/fixtures/php/sample.php::MyPlugin\\PostHandler",
            "tests/fixtures/php/sample.php::MyPlugin\\PostHandler::handle",
            "tests/fixtures/php/sample.php::MyPlugin\\PostHandler::on_save",
            "tests/fixtures/php/sample.php::MyPlugin\\bootstrap",
        ],
        "PHP exact symbol set mismatch"
    );
}

#[test]
fn test_php_namespace_backslash_separator_in_fqn() {
    let source = b"<?php\nnamespace A\\B;\nclass Foo {}";
    let (symbols, _) = parse_file("src/foo.php", source).unwrap();
    assert!(
        symbols.iter().any(|s| s.fqn == "src/foo.php::A\\B\\Foo"),
        "PHP namespace must use backslash separator; got: {:?}",
        symbols.iter().map(|s| &s.fqn).collect::<Vec<_>>()
    );
}

#[test]
fn test_php_hooks_into_edges_exact() {
    // Guards against the source_fqn attribution bug fixed in Story 1.2:
    // source must be the enclosing METHOD/FUNCTION FQN, never the class FQN or file path.
    let source = std::fs::read("tests/fixtures/php/sample.php").unwrap();
    let (_, edges) = parse_file("tests/fixtures/php/sample.php", &source).unwrap();

    let mut hooks_into: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::HooksInto)
        .collect();
    hooks_into.sort_by_key(|e| e.target_fqn.as_str());

    assert_eq!(
        hooks_into.len(),
        2,
        "expected exactly 2 hooks_into edges (add_action + add_filter)"
    );

    // handle() calls add_action('save_post', ...) — source must be method FQN
    assert_eq!(hooks_into[0].target_fqn, "save_post");
    assert_eq!(
        hooks_into[0].source_fqn, "tests/fixtures/php/sample.php::MyPlugin\\PostHandler::handle",
        "hooks_into source must be method FQN, not class or file"
    );

    // bootstrap() calls add_filter('the_content', ...) — source must be namespace-qualified FQN
    assert_eq!(hooks_into[1].target_fqn, "the_content");
    assert_eq!(
        hooks_into[1].source_fqn, "tests/fixtures/php/sample.php::MyPlugin\\bootstrap",
        "hooks_into source must be enclosing function FQN (namespace-qualified)"
    );
}

#[test]
fn test_php_fires_hook_edge_exact() {
    let source = std::fs::read("tests/fixtures/php/sample.php").unwrap();
    let (_, edges) = parse_file("tests/fixtures/php/sample.php", &source).unwrap();

    let fires_hook: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::FiresHook)
        .collect();

    assert_eq!(
        fires_hook.len(),
        1,
        "expected exactly 1 fires_hook edge (do_action)"
    );
    assert_eq!(fires_hook[0].target_fqn, "myPlugin_post_saved");
    assert_eq!(
        fires_hook[0].source_fqn, "tests/fixtures/php/sample.php::MyPlugin\\PostHandler::on_save",
        "fires_hook source must be enclosing method FQN"
    );
}

#[test]
fn test_php_uses_trait_edge_exact() {
    let source = std::fs::read("tests/fixtures/php/sample.php").unwrap();
    let (_, edges) = parse_file("tests/fixtures/php/sample.php", &source).unwrap();

    let uses_trait: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::UsesTrait)
        .collect();

    assert_eq!(
        uses_trait.len(),
        1,
        "expected exactly 1 uses_trait edge (`use Loggable`)"
    );
    assert_eq!(uses_trait[0].target_fqn, "Loggable");
    assert_eq!(
        uses_trait[0].source_fqn, "tests/fixtures/php/sample.php::MyPlugin\\PostHandler",
        "uses_trait source must be the class FQN, not a method FQN"
    );
}

#[test]
fn test_php_namespace_use_declaration_imports_edge() {
    // `use WP_Post;` at namespace level → Imports edge, source = file path, target = "WP_Post"
    let source = std::fs::read("tests/fixtures/php/sample.php").unwrap();
    let (_, edges) = parse_file("tests/fixtures/php/sample.php", &source).unwrap();

    let import_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .collect();
    assert_eq!(
        import_edges.len(),
        1,
        "expected exactly 1 Imports edge from `use WP_Post`; got: {:?}",
        import_edges
            .iter()
            .map(|e| (&e.source_fqn, &e.target_fqn))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        import_edges[0].source_fqn, "tests/fixtures/php/sample.php",
        "Imports edge source must be file path"
    );
    assert_eq!(
        import_edges[0].target_fqn, "WP_Post",
        "Imports edge target must be 'WP_Post'"
    );
}

#[test]
fn test_detect_language_php() {
    assert!(matches!(detect_language("foo.php"), Some(Language::Php)));
}

#[test]
fn test_php_braced_namespace_does_not_leak_to_siblings() {
    // `namespace A { class X {} }` followed by `class Y {}` at file scope.
    // Y must NOT inherit namespace A — braced namespace scope ends with the closing brace.
    let source = b"<?php\nnamespace A {\n  class X {}\n}\nclass Y {}";
    let (symbols, _) = parse_file("src/foo.php", source).unwrap();
    let fqns: Vec<_> = symbols.iter().map(|s| s.fqn.as_str()).collect();
    assert!(
        fqns.iter().any(|f| *f == "src/foo.php::A\\X"),
        "X must be in namespace A; got: {:?}",
        fqns
    );
    assert!(
        fqns.iter().any(|f| *f == "src/foo.php::Y"),
        "Y must NOT be namespace-qualified (braced scope ended); got: {:?}",
        fqns
    );
    assert!(
        !fqns.iter().any(|f| *f == "src/foo.php::A\\Y"),
        "Y must not inherit namespace A — braced scope must not leak; got: {:?}",
        fqns
    );
}

#[test]
fn test_php_unbraced_namespace_applies_to_subsequent_declarations() {
    // `namespace A;` followed by two classes — both must be in namespace A.
    let source = b"<?php\nnamespace A;\nclass X {}\nclass Y {}";
    let (symbols, _) = parse_file("src/foo.php", source).unwrap();
    let fqns: Vec<_> = symbols.iter().map(|s| s.fqn.as_str()).collect();
    assert!(
        fqns.iter().any(|f| *f == "src/foo.php::A\\X"),
        "X must be in namespace A after unbraced declaration; got: {:?}",
        fqns
    );
    assert!(
        fqns.iter().any(|f| *f == "src/foo.php::A\\Y"),
        "Y must also be in namespace A — unbraced namespace continues for all siblings; got: {:?}",
        fqns
    );
}
