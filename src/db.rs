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

const MIGRATION_002: &str = "
ALTER TABLE edges ADD COLUMN source_origin TEXT NOT NULL DEFAULT 'static';
";

const MIGRATION_003: &str = "
ALTER TABLE observations ADD COLUMN confidence REAL DEFAULT NULL;
";

const MIGRATION_005: &str = "
ALTER TABLE observations ADD COLUMN branch TEXT DEFAULT NULL;
CREATE INDEX idx_observations_branch ON observations(branch);
";

const MIGRATION_006: &str = "
CREATE TABLE project_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content TEXT NOT NULL,
    scope_fingerprint TEXT NOT NULL,
    support_count INTEGER NOT NULL DEFAULT 1,
    session_count INTEGER NOT NULL DEFAULT 1,
    last_seen_at INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 0,
    stale_reason TEXT DEFAULT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    branch TEXT DEFAULT NULL
);
CREATE INDEX idx_project_rules_active ON project_rules(is_active);
CREATE INDEX idx_project_rules_branch ON project_rules(branch);
CREATE UNIQUE INDEX idx_project_rules_identity ON project_rules(scope_fingerprint, COALESCE(branch, ''));

CREATE TABLE rule_symbols (
    rule_id INTEGER NOT NULL REFERENCES project_rules(id) ON DELETE CASCADE,
    symbol_fqn TEXT NOT NULL,
    PRIMARY KEY (rule_id, symbol_fqn)
);
CREATE INDEX idx_rule_symbols_fqn ON rule_symbols(symbol_fqn);

CREATE TABLE rule_files (
    rule_id INTEGER NOT NULL REFERENCES project_rules(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    PRIMARY KEY (rule_id, file_path)
);
CREATE INDEX idx_rule_files_path ON rule_files(file_path);

CREATE TABLE rule_observations (
    rule_id INTEGER NOT NULL REFERENCES project_rules(id) ON DELETE CASCADE,
    observation_id INTEGER NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
    PRIMARY KEY (rule_id, observation_id)
);
";

const MIGRATION_007: &str = "
CREATE TABLE activity_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    source TEXT NOT NULL,
    session_id TEXT,
    event_type TEXT NOT NULL,
    tool_name TEXT,
    summary TEXT NOT NULL,
    duration_ms INTEGER,
    is_error INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
CREATE INDEX idx_activity_events_ts ON activity_events(timestamp);
";

const MIGRATION_008: &str = "
ALTER TABLE observations ADD COLUMN consolidated_into INTEGER DEFAULT NULL REFERENCES observations(id) ON DELETE SET NULL;
ALTER TABLE observations ADD COLUMN consolidation_count INTEGER DEFAULT 0;
CREATE INDEX idx_observations_consolidated ON observations(consolidated_into);
";

// Fix purge-resurrection bug: when a survivor observation is deleted, its consolidated
// duplicates must also be deleted (not resurrected via SET NULL). SQLite cannot alter FK
// constraints, so we use a trigger to cascade-delete consolidated duplicates.
const MIGRATION_009: &str = "
CREATE TRIGGER IF NOT EXISTS observations_cascade_consolidated
    BEFORE DELETE ON observations
    WHEN old.consolidation_count > 0
    BEGIN
        DELETE FROM observations WHERE consolidated_into = old.id;
    END;
";

const MIGRATION_004: &str = "
CREATE VIRTUAL TABLE IF NOT EXISTS observations_fts
    USING fts5(content, content='observations', content_rowid='id', tokenize='porter ascii');

CREATE TRIGGER IF NOT EXISTS observations_fts_ai
    AFTER INSERT ON observations BEGIN
        INSERT INTO observations_fts(rowid, content) VALUES (new.id, new.content);
    END;

CREATE TRIGGER IF NOT EXISTS observations_fts_ad
    AFTER DELETE ON observations BEGIN
        INSERT INTO observations_fts(observations_fts, rowid, content) VALUES ('delete', old.id, old.content);
    END;

CREATE TRIGGER IF NOT EXISTS observations_fts_au
    AFTER UPDATE OF content ON observations BEGIN
        INSERT INTO observations_fts(observations_fts, rowid, content) VALUES ('delete', old.id, old.content);
        INSERT INTO observations_fts(rowid, content) VALUES (new.id, new.content);
    END;

INSERT INTO observations_fts(observations_fts) VALUES('rebuild');
";

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

/// Open an existing database read-only for cross-repo workspace queries.
///
/// Does NOT create directories, apply migrations, or recover corrupt DBs.
/// Verifies WAL mode (must have been set by the owning process).
/// Returns `DbError` on missing file, non-WAL DB, or corruption.
pub fn open_readonly(db_path: &Path) -> Result<rusqlite::Connection, DbError> {
    use rusqlite::OpenFlags;

    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let conn = rusqlite::Connection::open_with_flags(db_path, flags)?;

    // Verify WAL mode — do NOT attempt to set it (mutating pragma on read-only handle)
    let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    if mode != "wal" {
        return Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some(format!("expected WAL mode but got '{mode}'")),
        )));
    }

    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(Duration::from_millis(5000))?;

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

const MIGRATION_010: &str = "
ALTER TABLE observations ADD COLUMN importance TEXT NOT NULL DEFAULT 'medium' CHECK(importance IN ('critical','high','medium','low'));
CREATE INDEX IF NOT EXISTS idx_observations_importance_session ON observations(importance, session_id);
";

const MIGRATION_011: &str = "
CREATE TABLE observation_embeddings (
    observation_id INTEGER PRIMARY KEY REFERENCES observations(id) ON DELETE CASCADE,
    model_id TEXT NOT NULL,
    model_rev TEXT NOT NULL,
    dims INTEGER NOT NULL,
    embedding BLOB NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX idx_obs_embeddings_model ON observation_embeddings(model_id, model_rev);
";

/// Number of schema migrations. Used by `workspace doctor` to compare remote DB versions.
/// Update this when adding new migrations.
pub const MIGRATION_COUNT: i64 = 11;

fn apply_migrations(conn: &mut rusqlite::Connection) -> Result<(), DbError> {
    let migrations = Migrations::new(vec![
        M::up(MIGRATION_001),
        M::up(MIGRATION_002),
        M::up(MIGRATION_003),
        M::up(MIGRATION_004),
        M::up(MIGRATION_005),
        M::up(MIGRATION_006),
        M::up(MIGRATION_007),
        M::up(MIGRATION_008),
        M::up(MIGRATION_009),
        M::up(MIGRATION_010),
        M::up(MIGRATION_011),
        // Future migrations: append M::up(MIGRATION_012), etc. — never edit existing entries
        // Also update MIGRATION_COUNT above.
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
    fn test_migration_002_adds_source_origin() {
        let dir = tempdir().unwrap();
        let conn = open(&dir.path().join("index.db")).unwrap();

        // Verify source_origin column exists and defaults to 'static'
        // Insert a file and symbol first (FK requirements)
        conn.execute(
            "INSERT INTO files (path, blake3_hash, language, last_indexed_at) VALUES ('test.rs', 'h', 'rust', 1000)",
            [],
        ).unwrap();
        let file_id: i64 = conn.query_row("SELECT id FROM files WHERE path = 'test.rs'", [], |r| r.get(0)).unwrap();
        conn.execute(
            "INSERT INTO symbols (file_id, fqn, name, kind, start_line, end_line, source_hash) VALUES (?1, 'test.rs::f', 'f', 'function', 1, 2, 'h')",
            rusqlite::params![file_id],
        ).unwrap();
        let sym_id: i64 = conn.query_row("SELECT id FROM symbols WHERE fqn = 'test.rs::f'", [], |r| r.get(0)).unwrap();

        // Insert edge without specifying source_origin — should default to 'static'
        conn.execute(
            "INSERT INTO edges (source_id, target_id, kind) VALUES (?1, ?1, 'calls')",
            rusqlite::params![sym_id],
        ).unwrap();
        let origin: String = conn.query_row(
            "SELECT source_origin FROM edges WHERE source_id = ?1",
            rusqlite::params![sym_id],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(origin, "static", "source_origin must default to 'static'");
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
    fn test_open_readonly_valid_db() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        // Create a valid WAL-mode DB first
        let _conn = open(&db_path).expect("initial open");
        drop(_conn);

        // Now open read-only
        let conn = open_readonly(&db_path).expect("read-only open should succeed");
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn test_open_readonly_missing_file() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("nonexistent.db");
        let result = open_readonly(&db_path);
        assert!(result.is_err(), "should fail on missing file");
    }

    #[test]
    fn test_open_readonly_does_not_create_dirs() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("new-dir").join("index.db");
        let result = open_readonly(&db_path);
        assert!(result.is_err());
        assert!(!dir.path().join("new-dir").exists(), "should not create directories");
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
