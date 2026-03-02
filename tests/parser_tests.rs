use olaf::parser::{detect_language, parse_file, Language};

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
    let (symbols, edges) = parse_file("tests/fixtures/typescript/sample.ts", &source).unwrap();

    // Exact FQN set — no duplicates, no extras
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

    // At least one imports edge
    let import_edges: Vec<_> = edges
        .iter()
        .filter(|e| e.kind.as_str() == "imports")
        .collect();
    assert!(!import_edges.is_empty(), "expected imports edge for 'events'");
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
