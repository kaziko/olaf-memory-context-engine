// Named rust_lang.rs to avoid collision with the `rust` keyword
use tree_sitter::Parser;

use super::symbols::{Edge, EdgeKind, ParserError, Symbol, SymbolKind, make_child_symbol, make_symbol};

pub(crate) fn parse(
    relative_path: &str,
    source: &[u8],
) -> Result<(Vec<Symbol>, Vec<Edge>), ParserError> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter_rust::LANGUAGE.into())?;
    let tree = parser.parse(source, None).ok_or(ParserError::ParseFailed)?;
    let root = tree.root_node();
    let mut symbols = Vec::new();
    let mut edges = Vec::new();
    extract_nodes(root, source, relative_path, None, &mut symbols, &mut edges)?;
    Ok((symbols, edges))
}

/// Extract trait text preserving generics and path qualifiers for FQN disambiguation.
/// Unlike `extract_simple_type_name` (which strips generics/paths for stable type FQNs),
/// this preserves the full trait identity to prevent collisions between e.g.
/// `From<String>` and `From<&str>`, or `a::Display` and `b::Display`.
/// Strips ALL whitespace so formatting differences don't cause FQN churn.
fn extract_trait_identity(node: tree_sitter::Node<'_>, source: &[u8]) -> Result<String, ParserError> {
    let text = node.utf8_text(source)?;
    Ok(text.chars().filter(|c| !c.is_whitespace()).collect())
}

/// Extract the simple type name from a tree-sitter type node.
/// Strips generic parameters and path qualifiers so FQNs are stable join keys.
///
/// Examples:
///   `Foo`          (type_identifier)             → "Foo"
///   `Foo<T>`       (generic_type)                → "Foo"
///   `crate::Foo`   (scoped_type_identifier)      → "Foo"
///   `Foo<T, Bar>`  (generic_type → generic_type) → "Foo"
fn extract_simple_type_name<'a>(
    node: tree_sitter::Node<'_>,
    source: &'a [u8],
) -> Result<&'a str, ParserError> {
    match node.kind() {
        "type_identifier" | "identifier" => Ok(node.utf8_text(source)?),
        "generic_type" => {
            // `Foo<T>` — the `type` field is the base name (a type_identifier or scoped)
            if let Some(name_node) = node.child_by_field_name("type") {
                extract_simple_type_name(name_node, source)
            } else {
                // Fallback: take first child
                node.child(0)
                    .map(|n| extract_simple_type_name(n, source))
                    .unwrap_or(Err(ParserError::ParseFailed))
            }
        }
        "scoped_type_identifier" => {
            // `crate::Foo` — the `name` field is the terminal identifier
            if let Some(name_node) = node.child_by_field_name("name") {
                extract_simple_type_name(name_node, source)
            } else {
                // Fallback: take last child
                let last_idx = node.child_count().saturating_sub(1);
                node.child(last_idx)
                    .map(|n| extract_simple_type_name(n, source))
                    .unwrap_or(Err(ParserError::ParseFailed))
            }
        }
        _ => {
            // Unknown node kind — use raw text, strip everything after '<' and after last '::'
            let text = node.utf8_text(source)?;
            let after_path = text.rsplit("::").next().unwrap_or(text);
            let before_generics = after_path.split('<').next().unwrap_or(after_path);
            Ok(before_generics.trim())
        }
    }
}

fn extract_nodes(
    node: tree_sitter::Node<'_>,
    source: &[u8],
    relative_path: &str,
    parent_type: Option<&str>, // normalized name of the type being impl'd
    symbols: &mut Vec<Symbol>,
    edges: &mut Vec<Edge>,
) -> Result<(), ParserError> {
    match node.kind() {
        "function_item" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                let kind = if parent_type.is_some() {
                    SymbolKind::Method
                } else {
                    SymbolKind::Function
                };
                symbols.push(make_symbol(
                    relative_path,
                    parent_type,
                    name,
                    kind,
                    node,
                    source,
                ));
            }
        }
        "struct_item" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Struct,
                    node,
                    source,
                ));
                // Extract field declarations as children
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        if child.kind() == "field_declaration"
                            && let Some(field_name_node) = child.child_by_field_name("name")
                        {
                            let field_name = field_name_node.utf8_text(source)?;
                            symbols.push(make_child_symbol(
                                relative_path, name, field_name,
                                SymbolKind::Field, child, source,
                            ));
                        }
                    }
                }
            }
        }
        "enum_item" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Enum,
                    node,
                    source,
                ));
                // Extract enum variants as children
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        if child.kind() == "enum_variant"
                            && let Some(variant_name_node) = child.child_by_field_name("name")
                        {
                            let variant_name = variant_name_node.utf8_text(source)?;
                            symbols.push(make_child_symbol(
                                relative_path, name, variant_name,
                                SymbolKind::EnumVariant, child, source,
                            ));
                        }
                    }
                }
            }
        }
        "trait_item" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = name_node.utf8_text(source)?;
                symbols.push(make_symbol(
                    relative_path,
                    None,
                    name,
                    SymbolKind::Trait,
                    node,
                    source,
                ));
                // Extract trait items as children (method signatures, associated types)
                if let Some(body) = node.child_by_field_name("body") {
                    let mut walker = body.walk();
                    for child in body.children(&mut walker) {
                        match child.kind() {
                            "function_signature_item" => {
                                if let Some(method_name_node) = child.child_by_field_name("name") {
                                    let method_name = method_name_node.utf8_text(source)?;
                                    symbols.push(make_child_symbol(
                                        relative_path, name, method_name,
                                        SymbolKind::TraitMethod, child, source,
                                    ));
                                }
                            }
                            "associated_type" => {
                                if let Some(type_name_node) = child.child_by_field_name("name") {
                                    let type_name = type_name_node.utf8_text(source)?;
                                    symbols.push(make_child_symbol(
                                        relative_path, name, type_name,
                                        SymbolKind::AssociatedType, child, source,
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        "impl_item" => {
            // Not a symbol — extract implementing type, recurse into body with normalized type name
            if let Some(type_node) = node.child_by_field_name("type") {
                let type_name = extract_simple_type_name(type_node, source)?;

                let parent = if let Some(trait_node) = node.child_by_field_name("trait") {
                    let trait_id = extract_trait_identity(trait_node, source)?;
                    format!("{}::<{}>", type_name, trait_id)
                } else {
                    type_name.to_string()
                };

                if let Some(body) = node.child_by_field_name("body") {
                    extract_nodes(body, source, relative_path, Some(&parent), symbols, edges)?;
                }
            }
        }
        "use_declaration" => {
            let raw = node.utf8_text(source)?;
            // Find "use " to skip any leading visibility modifier (pub, pub(crate), etc.)
            let target = raw
                .find("use ")
                .map(|pos| raw[pos + 4..].trim_end_matches(';').trim())
                .unwrap_or("")
                .to_string();
            edges.push(Edge {
                source_fqn: relative_path.to_string(),
                target_fqn: target,
                kind: EdgeKind::Imports,
            });
        }
        _ => {
            let mut walker = node.walk();
            for child in node.children(&mut walker) {
                extract_nodes(child, source, relative_path, parent_type, symbols, edges)?;
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
        let (symbols, edges) = parse("empty.rs", b"").unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn parse_macro_heavy_file_does_not_panic() {
        let src = b"macro_rules! my_macro {\n    ($x:expr) => { $x + 1 };\n}\nmy_macro!(42);\n";
        let result = parse("macros.rs", src);
        assert!(result.is_ok());
    }

    #[test]
    fn parse_unsafe_block_extracts_fn() {
        let src = b"pub fn danger() {\n    unsafe { };\n}\n";
        let (symbols, _edges) = parse("unsafe.rs", src).unwrap();
        assert!(symbols.iter().any(|s| s.name == "danger"));
    }

    #[test]
    fn parse_file_with_only_comments() {
        let src = b"// just a comment\n/// doc comment\n/* block */\n";
        let (symbols, edges) = parse("comments.rs", src).unwrap();
        assert!(symbols.is_empty());
        assert!(edges.is_empty());
    }

    #[test]
    fn trait_impl_same_method_name_distinct_fqns() {
        let src = b"
struct Foo;
impl std::fmt::Display for Foo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }
}
impl std::fmt::Debug for Foo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }
}
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let fqns: Vec<&str> = symbols.iter().map(|s| s.fqn.as_str()).collect();
        assert!(fqns.contains(&"test.rs::Foo::<std::fmt::Display>::fmt"), "missing Display::fmt, got: {:?}", fqns);
        assert!(fqns.contains(&"test.rs::Foo::<std::fmt::Debug>::fmt"), "missing Debug::fmt, got: {:?}", fqns);
        assert_eq!(symbols.iter().filter(|s| s.name == "fmt").count(), 2);
    }

    #[test]
    fn generic_trait_impl_distinct_fqns() {
        let src = b"
struct Foo;
impl From<String> for Foo { fn from(s: String) -> Self { Foo } }
impl From<&str> for Foo { fn from(s: &str) -> Self { Foo } }
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let fqns: Vec<&str> = symbols.iter().map(|s| s.fqn.as_str()).collect();
        assert!(fqns.contains(&"test.rs::Foo::<From<String>>::from"), "missing From<String>::from, got: {:?}", fqns);
        assert!(fqns.contains(&"test.rs::Foo::<From<&str>>::from"), "missing From<&str>::from, got: {:?}", fqns);
    }

    #[test]
    fn inherent_vs_trait_impl_distinct_fqns() {
        let src = b"
struct Foo;
trait Bar { fn method(&self); }
impl Foo { fn method(&self) {} }
impl Bar for Foo { fn method(&self) {} }
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let fqns: Vec<&str> = symbols.iter().map(|s| s.fqn.as_str()).collect();
        assert!(fqns.contains(&"test.rs::Foo::method"), "missing Foo::method, got: {:?}", fqns);
        assert!(fqns.contains(&"test.rs::Foo::<Bar>::method"), "missing Foo::<Bar>::method, got: {:?}", fqns);
    }

    #[test]
    fn inherent_impl_unchanged() {
        let src = b"
struct Foo;
impl Foo { fn bar(&self) {} }
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        assert!(symbols.iter().any(|s| s.fqn == "test.rs::Foo::bar"));
    }

    #[test]
    fn path_qualified_trait_preserved() {
        let src = b"
struct Foo;
impl std::fmt::Display for Foo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }
}
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        assert!(symbols.iter().any(|s| s.fqn == "test.rs::Foo::<std::fmt::Display>::fmt"));
    }

    #[test]
    fn generic_type_param_trait_preserved() {
        let src = b"
struct Foo;
impl<T> From<T> for Foo { fn from(t: T) -> Self { Foo } }
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let fqns: Vec<&str> = symbols.iter().map(|s| s.fqn.as_str()).collect();
        assert!(fqns.iter().any(|f| f.contains("From<T>")), "expected From<T> in FQN, got: {:?}", fqns);
    }

    #[test]
    fn whitespace_in_trait_is_canonicalized() {
        // tree-sitter may produce nodes with internal whitespace like `From < String >`
        // extract_trait_identity must strip it so FQNs are stable regardless of formatting
        let src_compact = b"
struct Foo;
impl From<String> for Foo { fn from(s: String) -> Self { Foo } }
";
        let src_spaced = b"
struct Foo;
impl From  <  String  > for Foo { fn from(s: String) -> Self { Foo } }
";
        let (syms_compact, _) = parse("test.rs", src_compact).unwrap();
        let (syms_spaced, _) = parse("test.rs", src_spaced).unwrap();
        let fqn_compact: Vec<&str> = syms_compact.iter().filter(|s| s.name == "from").map(|s| s.fqn.as_str()).collect();
        let fqn_spaced: Vec<&str> = syms_spaced.iter().filter(|s| s.name == "from").map(|s| s.fqn.as_str()).collect();
        assert_eq!(fqn_compact, fqn_spaced, "whitespace differences must not change FQN");
        assert_eq!(fqn_compact[0], "test.rs::Foo::<From<String>>::from");
    }

    #[test]
    fn enum_variants_extracted_as_children() {
        let src = b"
pub enum Error {
    Sqlite(rusqlite::Error),
    Io(std::io::Error),
    Custom(String),
}
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let parent = symbols.iter().find(|s| s.name == "Error").unwrap();
        assert_eq!(parent.kind, SymbolKind::Enum);
        assert!(parent.parent_fqn.is_none());

        let variants: Vec<&Symbol> = symbols.iter().filter(|s| s.kind == SymbolKind::EnumVariant).collect();
        assert_eq!(variants.len(), 3);
        assert!(variants.iter().any(|v| v.name == "Sqlite" && v.parent_fqn.as_deref() == Some("test.rs::Error")));
        assert!(variants.iter().any(|v| v.name == "Custom"));
        // Variant signature must contain full text (bodyless declaration)
        let sqlite = variants.iter().find(|v| v.name == "Sqlite").unwrap();
        assert!(sqlite.signature.as_ref().unwrap().contains("rusqlite::Error"),
            "variant signature must contain type; got: {:?}", sqlite.signature);
    }

    #[test]
    fn struct_fields_extracted_as_children() {
        let src = b"
pub struct Config {
    pub name: String,
    pub port: u16,
}
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let parent = symbols.iter().find(|s| s.name == "Config").unwrap();
        assert_eq!(parent.kind, SymbolKind::Struct);

        let fields: Vec<&Symbol> = symbols.iter().filter(|s| s.kind == SymbolKind::Field).collect();
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().any(|f| f.name == "name" && f.fqn == "test.rs::Config::name"));
        assert!(fields.iter().any(|f| f.name == "port" && f.fqn == "test.rs::Config::port"));
    }

    #[test]
    fn trait_methods_and_associated_types_extracted() {
        let src = b"
pub trait Handler {
    type Output;
    fn handle(&self, input: &str) -> Self::Output;
}
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let parent = symbols.iter().find(|s| s.name == "Handler").unwrap();
        assert_eq!(parent.kind, SymbolKind::Trait);

        let trait_methods: Vec<&Symbol> = symbols.iter().filter(|s| s.kind == SymbolKind::TraitMethod).collect();
        assert_eq!(trait_methods.len(), 1);
        assert_eq!(trait_methods[0].name, "handle");
        assert_eq!(trait_methods[0].parent_fqn.as_deref(), Some("test.rs::Handler"));

        let assoc_types: Vec<&Symbol> = symbols.iter().filter(|s| s.kind == SymbolKind::AssociatedType).collect();
        assert_eq!(assoc_types.len(), 1);
        assert_eq!(assoc_types[0].name, "Output");
    }

    #[test]
    fn impl_methods_remain_top_level() {
        // impl methods must NOT get parent_fqn — they stay top-level
        let src = b"
struct Foo;
impl Foo {
    fn method(&self) {}
}
";
        let (symbols, _) = parse("test.rs", src).unwrap();
        let method = symbols.iter().find(|s| s.name == "method").unwrap();
        assert!(method.parent_fqn.is_none(), "impl methods must not have parent_fqn");
        assert_eq!(method.kind, SymbolKind::Method);
    }
}
