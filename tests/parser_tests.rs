use olaf::parser::{detect_language, parse_file, EdgeKind, Language};

#[test]
fn test_detect_language_extensions() {
    assert!(matches!(detect_language("foo.ts"), Some(Language::TypeScript)));
    assert!(matches!(detect_language("foo.tsx"), Some(Language::Tsx)));
    assert!(matches!(detect_language("foo.js"), Some(Language::JavaScript)));
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

    let import_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::Imports).collect();
    assert!(!import_edges.is_empty(), "expected at least one imports edge");
    // source_fqn for imports is the file path, not a symbol FQN
    assert!(
        import_edges.iter().all(|e| e.source_fqn == "tests/fixtures/typescript/sample.ts"),
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

    let extends_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::Extends).collect();
    assert_eq!(extends_edges.len(), 1, "expected exactly one extends edge");
    assert_eq!(
        extends_edges[0].source_fqn,
        "tests/fixtures/typescript/sample.ts::Greeter",
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

    let impl_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::Implements).collect();
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
        calls_edges.iter().any(|e| e.source_fqn == "src/x.ts::caller"
            && e.target_fqn == "callee"),
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
        calls_edges.iter().any(|e| e.source_fqn == "src/foo.ts::Foo::bar"),
        "calls edge source must be the method FQN, not the class; got: {:?}",
        calls_edges.iter().map(|e| &e.source_fqn).collect::<Vec<_>>()
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

    let type_edges: Vec<_> = edges.iter().filter(|e| e.kind == EdgeKind::UsesType).collect();
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
            sym.start_line,
            2,
            "start_line must be 1-based (tree-sitter row + 1)"
        );
    } else {
        panic!("expected function foo to be extracted");
    }
}

#[test]
fn test_python_stub_returns_empty() {
    let (syms, edges) = parse_file("src/foo.py", b"def hello(): pass").unwrap();
    assert!(
        syms.is_empty() && edges.is_empty(),
        "Python stub must return empty until Story 1.3"
    );
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
