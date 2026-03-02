use rusqlite::Transaction;

// Full staleness logic implemented in Story 3.3.
// changed_symbols: (symbol_id, old_source_hash, new_source_hash)
// Story 3.3 will mark observations stale where old_source_hash != new_source_hash.
pub(crate) fn mark_stale_for_changed_symbols(
    _tx: &Transaction,
    _changed_symbols: &[(i64, String, String)],
) -> Result<(), rusqlite::Error> {
    Ok(())
}
