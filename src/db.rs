use std::path::Path;
use std::time::Duration;

use rusqlite_migration::{M, Migrations};

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("Migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

const MIGRATION_001: &str = "
CREATE TABLE IF NOT EXISTS files (
    id              INTEGER PRIMARY KEY,
    path            TEXT NOT NULL UNIQUE,
    blake3_hash     TEXT NOT NULL,
    language        TEXT,
    last_indexed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS symbols (
    id          INTEGER PRIMARY KEY,
    file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    fqn         TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    start_line  INTEGER NOT NULL,
    end_line    INTEGER NOT NULL,
    signature   TEXT,
    docstring   TEXT,
    source_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    id        INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    target_id INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    kind      TEXT NOT NULL,
    UNIQUE(source_id, target_id, kind)
);

CREATE TABLE IF NOT EXISTS sessions (
    id         TEXT PRIMARY KEY,
    started_at INTEGER NOT NULL,
    ended_at   INTEGER,
    agent      TEXT DEFAULT 'claude-code',
    compressed INTEGER DEFAULT 0,
    summary    TEXT
);

CREATE TABLE IF NOT EXISTS observations (
    id             INTEGER PRIMARY KEY,
    session_id     TEXT NOT NULL REFERENCES sessions(id),
    created_at     INTEGER NOT NULL,
    kind           TEXT NOT NULL,
    content        TEXT NOT NULL,
    symbol_fqn     TEXT,
    file_path      TEXT,
    is_stale       INTEGER DEFAULT 0,
    stale_reason   TEXT,
    auto_generated INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id);
CREATE INDEX IF NOT EXISTS idx_symbols_fqn ON symbols(fqn);
CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name);
CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
CREATE INDEX IF NOT EXISTS idx_observations_session ON observations(session_id);
CREATE INDEX IF NOT EXISTS idx_observations_symbol ON observations(symbol_fqn);
CREATE INDEX IF NOT EXISTS idx_observations_file ON observations(file_path);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
";

/// Open or create the SQLite database at `db_path`.
///
/// Sets WAL journal mode, enables foreign key enforcement, and sets a
/// busy timeout. Applies all schema migrations. On corrupt databases,
/// renames the file and creates a fresh one.
///
/// Every connection in the codebase MUST go through this function.
pub fn open(db_path: &Path) -> Result<rusqlite::Connection, DbError> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut conn = open_with_recovery(db_path)?;

    // WAL mode — load-bearing for multi-process concurrency (NFR11)
    conn.pragma_update(None, "journal_mode", "WAL")?;
    // FK enforcement — ON DELETE CASCADE only works with this set
    conn.pragma_update(None, "foreign_keys", "ON")?;
    // Prevents SQLITE_BUSY across simultaneous olaf processes
    conn.busy_timeout(Duration::from_millis(5000))?;

    apply_migrations(&mut conn)?;
    Ok(conn)
}

fn open_with_recovery(db_path: &Path) -> Result<rusqlite::Connection, DbError> {
    // SQLite's Connection::open() is lazy — corruption is detected on first read.
    // Open the connection, then probe with a schema query to catch corrupt files early.
    let conn = match rusqlite::Connection::open(db_path) {
        Ok(c) => c,
        Err(e) => return handle_corruption_or_propagate(e, db_path),
    };

    // Probe: reading sqlite_master forces SQLite to parse the DB header
    match conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |_| Ok(())) {
        Ok(_) => Ok(conn),
        Err(e) => {
            // Drop connection before rename — releases file handle on all platforms (incl. Windows)
            drop(conn);
            handle_corruption_or_propagate(e, db_path)
        }
    }
}

fn handle_corruption_or_propagate(
    e: rusqlite::Error,
    db_path: &Path,
) -> Result<rusqlite::Connection, DbError> {
    use rusqlite::ErrorCode;
    match e.sqlite_error_code() {
        Some(ErrorCode::NotADatabase) | Some(ErrorCode::DatabaseCorrupt) => {
            // Rename corrupt file — preserve evidence, don't delete
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let backup = db_path.with_extension(format!("db.corrupt.{}", ts));
            std::fs::rename(db_path, &backup)?;
            eprintln!(
                "warn: corrupt DB renamed to {:?} — rebuilding index",
                backup
            );
            Ok(rusqlite::Connection::open(db_path)?)
        }
        // All other errors (permissions, path issues): propagate — do NOT silently recover
        _ => Err(DbError::Sqlite(e)),
    }
}

fn apply_migrations(conn: &mut rusqlite::Connection) -> Result<(), DbError> {
    let migrations = Migrations::new(vec![
        M::up(MIGRATION_001),
        // Future migrations: append M::up(MIGRATION_002), etc. — never edit existing entries
    ]);
    migrations.to_latest(conn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_db_opens_creates_tables_and_sets_pragmas() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join(".olaf/index.db");
        let conn = open(&db_path).expect("open should succeed");

        // WAL mode
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");

        // FK enforcement
        let fk: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fk, 1, "foreign_keys must be ON");

        // All 5 tables
        for table in ["files", "symbols", "edges", "sessions", "observations"] {
            let n: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    rusqlite::params![table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "table '{}' must exist", table);
        }
    }

    #[test]
    fn test_foreign_key_enforcement() {
        let dir = tempdir().unwrap();
        let conn = open(&dir.path().join("index.db")).unwrap();
        // Insert symbol with non-existent file_id — must fail with FK violation
        let result = conn.execute(
            "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash)
             VALUES (9999, 'fake::sym', 'sym', 'function', 1, 10, 'abc')",
            [],
        );
        assert!(result.is_err(), "FK violation should be rejected");
    }

    #[test]
    fn test_corruption_recovery_renames_not_deletes() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join(".olaf/index.db");
        std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
        std::fs::write(&db_path, b"SQLite garbage bytes - not a real db").unwrap();

        let conn = open(&db_path).expect("should recover from corruption");

        // Tables must exist in rebuilt DB
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n >= 5, "all tables should exist after recovery");

        // Backup file must exist (renamed, not deleted)
        let backups: Vec<_> = std::fs::read_dir(db_path.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".corrupt."))
            .collect();
        assert_eq!(backups.len(), 1, "corrupt file should be renamed to backup");
    }
}
