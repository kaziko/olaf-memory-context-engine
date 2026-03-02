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
