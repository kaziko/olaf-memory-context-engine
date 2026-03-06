use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, make_symbol};

pub(crate) fn parse(
    relative_path: &str,
    source: &[u8],
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter_go::LANGUAGE.into())?;
    let tree = parser.parse(source, None).ok_or(ParserError::ParseFailed)?;
    let root = tree.root_node();
    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    extract_nodes(root, source, relative_path, &mut symbols, &mut edges)?;
    Ok((symbols, edges))
}

/// Resolve a type node to its simple identifier text.
/// Handles: `type_identifier`, `pointer_type` (strips `*`), `generic_type` (strips type args),
/// and `qualified_type` (e.g. `pkg.Foo` → `Foo`).
fn extract_type_name<'a>(node: tree_sitter::Node<'_>, source: &'a [u8]) -> Option<&'a str> {
    match node.kind() {
        "type_identifier" => node.utf8_text(source).ok(),
        "pointer_type" => {
            // pointer_type has no named fields — child is the pointed-to type
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                if let Some(name) = extract_type_name(child, source) {
                    return Some(name);
                }
            }
            None
        }
        "generic_type" => {
            // generic_type has a "type" field: type_identifier, qualified_type, or negated_type
            node.child_by_field_name("type")
                .and_then(|n| extract_type_name(n, source))
        }
        "qualified_type" => {
            // e.g. `pkg.Foo` — "name" field is the terminal type_identifier
            node.child_by_field_name("name")
                .and_then(|n| n.utf8_text(source).ok())
        }
        _ => None,
    }
}

/// Extract the receiver type name from a method_declaration receiver field.
/// The receiver is a parameter_list containing one parameter whose type may be
/// pointer_type, type_identifier, generic_type, or qualified_type.
fn extract_receiver_type<'a>(
    receiver: tree_sitter::Node<'_>,
    source: &'a [u8],
) -> Option<&'a str> {
    let mut walker = receiver.walk();
    for child in receiver.children(&mut walker) {
        if child.kind() == "parameter_declaration"
            && let Some(type_node) = child.child_by_field_name("type")
        {
            return extract_type_name(type_node, source);
        }
    }
    None
}

fn extract_nodes(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    symbols: &mut Vec<Symbol>,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    match node.kind() {
        "function_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Function,
                    node,
                    source,
                ));
            }
        }
        "method_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let receiver_type = node
                    .child_by_field_name("receiver")
                    .and_then(|r| extract_receiver_type(r, source));
                symbols.push(make_symbol(
                    relative_path,
                    receiver_type,
                    name,
                    SymbolKind::Method,
                    node,
                    source,
                ));
            }
        }
        "type_declaration" => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                match child.kind() {
                    "type_spec" => extract_type_spec(child, source, relative_path, symbols)?,
                    "type_alias" => {
                        if let Some(name_node) = child.child_by_field_name("name") {
                            let name = name_node.utf8_text(source)?;
                            symbols.push(make_symbol(
                                relative_path,
                                None,
                                name,
                                SymbolKind::TypeAlias,
                                child,
                                source,
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
        "const_declaration" => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                if child.kind() == "const_spec" {
                    // "name" field is multiple=true — iterate all identifier children
                    let mut name_walker = child.walk();
                    for name_node in child.children_by_field_name("name", &mut name_walker) {
                        if name_node.kind() == "identifier" {
                            let name = name_node.utf8_text(source)?;
                            symbols.push(make_symbol(
                                relative_path,
                                None,
                                name,
                                SymbolKind::Variable,
                                child,
                                source,
                            ));
                        }
                    }
                }
            }
        }
        "import_declaration" => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                if child.kind() == "import_spec" {
                    extract_import_edge(child, source, relative_path, edges)?;
                } else if child.kind() == "import_spec_list" {
                    let mut inner_walker = child.walk();
                    for spec in child.children(&mut inner_walker) {
                        if spec.kind() == "import_spec" {
                            extract_import_edge(spec, source, relative_path, edges)?;
                        }
                    }
                }
            }
        }
        _ => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                extract_nodes(child, source, relative_path, symbols, edges)?;
            }
        }
    }
    Ok(())
}

fn extract_type_spec(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    symbols: &mut Vec<Symbol>,
) -> Result<(), ParserError> {
    let Some(name_node) = node.child_by_field_name("name") else {
        return Ok(());
    };
    let name = name_node.utf8_text(source)?;

    let kind = if let Some(type_node) = node.child_by_field_name("type") {
        match type_node.kind() {
            "struct_type" => SymbolKind::Class,
            "interface_type" => SymbolKind::Interface,
            _ => SymbolKind::Class,
        }
    } else {
        SymbolKind::Class
    };

    symbols.push(make_symbol(relative_path, None, name, kind, node, source));
    Ok(())
}

fn extract_import_edge(
    spec: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    if let Some(path_node) = spec.child_by_field_name("path") {
        let raw = path_node.utf8_text(source)?;
        // Strip surrounding quotes (interpreted_string_literal uses `"`, raw_string_literal uses `` ` ``)
        let target = raw.trim_matches(|c| c == '"' || c == '`');
        if !target.is_empty() {
            edges.push(Edge {
                source_fqn: relative_path.to_string(),
                target_fqn: target.to_string(),
                kind: EdgeKind::Imports,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::symbols::{EdgeKind, SymbolKind};

    const SAMPLE: &str = r#"package main

import (
	"fmt"
	"os"
)

const MaxRetries, DefaultTimeout = 3, 30

type Server struct {
	Host string
	Port int
}

type Greeter interface {
	Greet(name string) string
}

type StringAlias = string

func NewServer(host string, port int) *Server {
	return &Server{Host: host, Port: port}
}

func (s *Server) Start() error {
	fmt.Println("starting")
	return nil
}

func (s Server) Address() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

func Env(key string) string {
	return os.Getenv(key)
}
"#;

    #[test]
    fn test_parse_symbols() {
        let (symbols, _edges) = parse("pkg/server.go", SAMPLE.as_bytes()).unwrap();
        let fqns: Vec<&str> = symbols.iter().map(|s| s.fqn.as_str()).collect();

        assert!(fqns.contains(&"pkg/server.go::MaxRetries"), "missing MaxRetries; got {fqns:?}");
        assert!(
            fqns.contains(&"pkg/server.go::DefaultTimeout"),
            "missing DefaultTimeout (multi-name const); got {fqns:?}"
        );
        assert!(fqns.contains(&"pkg/server.go::Server"), "missing struct; got {fqns:?}");
        assert!(fqns.contains(&"pkg/server.go::Greeter"), "missing interface; got {fqns:?}");
        assert!(
            fqns.contains(&"pkg/server.go::StringAlias"),
            "missing type alias; got {fqns:?}"
        );
        assert!(fqns.contains(&"pkg/server.go::NewServer"), "missing function; got {fqns:?}");
        assert!(
            fqns.contains(&"pkg/server.go::Server::Start"),
            "missing pointer-receiver method; got {fqns:?}"
        );
        assert!(
            fqns.contains(&"pkg/server.go::Server::Address"),
            "missing value-receiver method; got {fqns:?}"
        );
    }

    #[test]
    fn test_parse_symbol_kinds() {
        let (symbols, _) = parse("pkg/server.go", SAMPLE.as_bytes()).unwrap();
        let find = |fqn: &str| symbols.iter().find(|s| s.fqn == fqn).unwrap().kind.clone();

        assert_eq!(find("pkg/server.go::MaxRetries"), SymbolKind::Variable);
        assert_eq!(find("pkg/server.go::DefaultTimeout"), SymbolKind::Variable);
        assert_eq!(find("pkg/server.go::Server"), SymbolKind::Class);
        assert_eq!(find("pkg/server.go::Greeter"), SymbolKind::Interface);
        assert_eq!(find("pkg/server.go::StringAlias"), SymbolKind::TypeAlias);
        assert_eq!(find("pkg/server.go::NewServer"), SymbolKind::Function);
        assert_eq!(find("pkg/server.go::Server::Start"), SymbolKind::Method);
        assert_eq!(find("pkg/server.go::Server::Address"), SymbolKind::Method);
    }

    #[test]
    fn test_parse_import_edges() {
        let (_symbols, edges) = parse("pkg/server.go", SAMPLE.as_bytes()).unwrap();
        let targets: Vec<&str> = edges
            .iter()
            .filter(|e| e.kind == EdgeKind::Imports)
            .map(|e| e.target_fqn.as_str())
            .collect();

        assert!(targets.contains(&"fmt"), "missing fmt import; got {targets:?}");
        assert!(targets.contains(&"os"), "missing os import; got {targets:?}");
        for e in &edges {
            assert_eq!(e.source_fqn, "pkg/server.go");
        }
    }

    #[test]
    fn test_raw_string_import() {
        let src = b"package p\nimport `example.com/x`\n";
        let (_symbols, edges) = parse("p/main.go", src).unwrap();
        let targets: Vec<&str> = edges.iter().map(|e| e.target_fqn.as_str()).collect();
        assert!(
            targets.contains(&"example.com/x"),
            "raw-string import backticks not stripped; got {targets:?}"
        );
    }

    #[test]
    fn test_generic_receiver() {
        let src = b"package p\nfunc (s *Set[T]) Add(v T) {}\n";
        let (symbols, _) = parse("p/set.go", src).unwrap();
        let fqns: Vec<&str> = symbols.iter().map(|s| s.fqn.as_str()).collect();
        assert!(
            fqns.contains(&"p/set.go::Set::Add"),
            "generic receiver method FQN wrong; got {fqns:?}"
        );
    }

    #[test]
    fn test_empty_file() {
        let (symbols, edges) = parse("empty.go", b"package main\n").unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }
}
