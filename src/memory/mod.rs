pub(crate) mod antipattern;
pub(crate) mod capture;
pub(crate) mod compression;
pub(crate) mod staleness;
pub(crate) mod store;

pub use antipattern::detect_and_write_anti_patterns;
pub use capture::{HookPayload, PostToolUseResult, format_structural_observation, parse_post_tool_use};
pub use compression::{run_compression, DEFAULT_COMPRESSION_THRESHOLD_SECS};
pub use store::{
    SessionSummary, get_session_observations, insert_auto_observation, is_sensitive_path,
    list_sessions, mark_session_ended, upsert_session,
};

/// Atomically detect anti-patterns and compress a session in a single BEGIN IMMEDIATE
/// transaction. IMMEDIATE acquires the write lock before the compressed check, so concurrent
/// hook processes cannot both pass the guard and write duplicate anti_pattern observations.
/// Returns Ok(false) if the session was already compressed (no-op). Ok(true) if it ran.
pub fn run_session_end_pipeline(
    conn: &mut rusqlite::Connection,
    session_id: &str,
) -> Result<bool, store::StoreError> {
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    // Re-check compressed inside the write-locked transaction (prevents TOCTOU race)
    let already_compressed: bool = match tx.query_row(
        "SELECT compressed FROM sessions WHERE id = ?1",
        rusqlite::params![session_id],
        |r| r.get::<_, i64>(0),
    ) {
        Ok(v) => v != 0,
        Err(rusqlite::Error::QueryReturnedNoRows) => false,
        Err(e) => return Err(store::StoreError::Sqlite(e)),
    };
    if already_compressed {
        return Ok(false); // tx rolled back on drop
    }
    // Deref coercion: &Transaction → &Connection for functions expecting &Connection
    antipattern::detect_and_write_anti_patterns(&tx, session_id)?;
    store::compress_session(&tx, session_id)?;
    tx.commit()?;
    Ok(true)
}
