/// One-shot AST dump tests — run manually to discover node kinds.
/// These are #[ignore] and should not run in CI.

#[test]
#[ignore] // run: cargo test dump_python_ast -- --nocapture --ignored
fn dump_python_ast() {
    let source = b"import os\nimport sys as system\nfrom pathlib import Path\nclass Foo:\n  def bar(self): pass\ndef baz(): pass";
    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&tree_sitter_python::LANGUAGE.into()).unwrap();
    let tree = parser.parse(source, None).unwrap();
    fn dump(node: tree_sitter::Node, depth: usize) {
        println!("{}{} [{}..{}]", "  ".repeat(depth), node.kind(),
                 node.start_byte(), node.end_byte());
        let mut c = node.walk();
        for child in node.children(&mut c) { dump(child, depth + 1); }
    }
    dump(tree.root_node(), 0);
}

#[test]
#[ignore] // run: cargo test dump_rust_ast -- --nocapture --ignored
fn dump_rust_ast() {
    let source = b"use std::path::Path;\nstruct Foo {}\ntrait Bar {}\nimpl Foo { fn hello(&self) {} }";
    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&tree_sitter_rust::LANGUAGE.into()).unwrap();
    let tree = parser.parse(source, None).unwrap();
    fn dump(node: tree_sitter::Node, depth: usize) {
        println!("{}{} [{}..{}]", "  ".repeat(depth), node.kind(),
                 node.start_byte(), node.end_byte());
        let mut c = node.walk();
        for child in node.children(&mut c) { dump(child, depth + 1); }
    }
    dump(tree.root_node(), 0);
}

#[test]
#[ignore] // run: cargo test dump_php_ast -- --nocapture --ignored
fn dump_php_ast() {
    let source = b"<?php\nnamespace MyPlugin;\nuse WP_Post;\nclass Foo {\n  use Loggable;\n  public function handle() { add_action('save_post', [$this, 'on_save']); }\n}\nfunction bootstrap() { add_filter('the_content', 'f'); }";
    let mut parser = tree_sitter::Parser::new();
    parser.set_language(&tree_sitter_php::LANGUAGE_PHP.into()).unwrap();
    let tree = parser.parse(source, None).unwrap();
    fn dump(node: tree_sitter::Node, depth: usize) {
        println!("{}{} [{}..{}]", "  ".repeat(depth), node.kind(),
                 node.start_byte(), node.end_byte());
        let mut c = node.walk();
        for child in node.children(&mut c) { dump(child, depth + 1); }
    }
    dump(tree.root_node(), 0);
}
