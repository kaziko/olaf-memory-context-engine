use rusqlite::{Connection, params};
use crate::graph::store::StoreError;

pub(crate) fn skeletonize(conn: &Connection, symbol_id: i64) -> Result<String, StoreError> {
    let (fqn, name, file_path, start, end, sig, doc) = conn.query_row(
        "SELECT s.fqn, s.name, f.path, s.start_line, s.end_line, s.signature, s.docstring
         FROM symbols s JOIN files f ON f.id=s.file_id WHERE s.id=?1",
        params![symbol_id],
        |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?,
                 r.get::<_,i64>(3)?, r.get::<_,i64>(4)?,
                 r.get::<_,Option<String>>(5)?, r.get::<_,Option<String>>(6)?)),
    )?;
    let mut s = format!("### {} (`{}`)\nFile: `{}` lines {}-{}\n", name, fqn, file_path, start, end);
    if let Some(sig) = sig { s.push_str(&format!("Signature: `{sig}`\n")); }
    if let Some(doc) = doc { s.push_str(&format!("{doc}\n")); }
    let mut stmt = conn.prepare(
        "SELECT s2.name, e.kind FROM edges e JOIN symbols s2 ON s2.id=e.target_id
         WHERE e.source_id=?1 LIMIT 10")?;
    let edges: Vec<String> = stmt.query_map(params![symbol_id], |r| {
        Ok(format!("{} ({})", r.get::<_,String>(0)?, r.get::<_,String>(1)?))
    })?.collect::<Result<_,_>>()?;
    if !edges.is_empty() {
        s.push_str(&format!("Dependencies: {}\n", edges.join(", ")));
    }
    s.push('\n');
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_skeleton_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT NOT NULL, hash TEXT);
             CREATE TABLE symbols (
                 id INTEGER PRIMARY KEY, file_id INTEGER NOT NULL, fqn TEXT NOT NULL,
                 name TEXT NOT NULL, kind TEXT, start_line INTEGER NOT NULL,
                 end_line INTEGER NOT NULL, signature TEXT, docstring TEXT, source_hash TEXT,
                 parent_id INTEGER DEFAULT NULL
             );
             CREATE TABLE edges (id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, target_id INTEGER NOT NULL, kind TEXT);",
        ).unwrap();
        conn.execute("INSERT INTO files (id, path) VALUES (1, 'src/lib.rs')", []).unwrap();
        conn
    }

    #[test]
    fn skeletonize_symbol_with_no_edges() {
        let conn = setup_skeleton_db();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'lib::Foo', 'Foo', 'struct', 1, 10, 'pub struct Foo', 'A foo struct', NULL, NULL)",
            [],
        ).unwrap();
        let result = skeletonize(&conn, 1).unwrap();
        assert!(result.contains("Foo"));
        assert!(result.contains("pub struct Foo"));
        assert!(result.contains("A foo struct"));
        assert!(!result.contains("Dependencies"));
    }

    #[test]
    fn skeletonize_symbol_with_only_docstring_no_signature() {
        let conn = setup_skeleton_db();
        conn.execute(
            "INSERT INTO symbols VALUES (1, 1, 'lib::Bar', 'Bar', 'function', 5, 15, NULL, 'Does something important', NULL, NULL)",
            [],
        ).unwrap();
        let result = skeletonize(&conn, 1).unwrap();
        assert!(result.contains("Bar"));
        assert!(result.contains("Does something important"));
        assert!(!result.contains("Signature"));
    }
}
