use std::path::Path;

use olaf::{db, index, index::reindex_single_file, index::run_incremental};
use tempfile::tempdir;

/// Open an in-memory test DB at a temp path and return the connection.
fn open_test_db(dir: &Path) -> rusqlite::Connection {
    let db_path = dir.join("index.db");
    db::open(&db_path).expect("db::open failed")
}

fn query_count(conn: &rusqlite::Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |r| r.get(0))
        .unwrap()
}

fn query_count_where(conn: &rusqlite::Connection, table: &str, col: &str, val: &str) -> i64 {
    conn.query_row(
        &format!("SELECT COUNT(*) FROM {} WHERE {} = ?1", table, col),
        rusqlite::params![val],
        |r| r.get(0),
    )
    .unwrap()
}

fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/index")
}

#[test]
fn test_full_index_parses_supported_files() {
    let dir = tempdir().unwrap();
    let mut conn = open_test_db(dir.path());

    let root = fixture_path();
    let stats = index::run(&mut conn, &root).expect("index::run failed");

    // Fixture has 2 Rust files (src/main.rs and src/lib.rs)
    assert_eq!(stats.files, 2, "should index exactly 2 supported files");
    // Each file should produce at least one symbol
    assert!(stats.symbols >= 2, "should index at least 2 symbols");

    let file_count = query_count(&conn, "files");
    assert_eq!(file_count, 2);
}

#[test]
fn test_sensitive_files_excluded() {
    let dir = tempdir().unwrap();
    let mut conn = open_test_db(dir.path());

    let root = fixture_path();
    index::run(&mut conn, &root).expect("index::run failed");

    // secrets/.env and secrets/deploy.pem must never appear in files table
    let env_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '%/.env' OR path = '.env'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(env_count, 0, ".env must not be in files table");

    let pem_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '%.pem'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pem_count, 0, ".pem files must not be in files table");
}

#[test]
fn test_gitignore_respected() {
    let dir = tempdir().unwrap();
    let mut conn = open_test_db(dir.path());

    let root = fixture_path();
    index::run(&mut conn, &root).expect("index::run failed");

    // debug.log excluded by *.log in .gitignore
    let log_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '%.log'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(log_count, 0, "*.log files must be excluded by .gitignore");

    // build/output.rs excluded by build/ in .gitignore
    let build_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE 'build/%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(build_count, 0, "build/ dir must be excluded by .gitignore");
}

#[test]
fn test_idempotent_index() {
    let dir = tempdir().unwrap();
    let mut conn = open_test_db(dir.path());

    let root = fixture_path();

    // Run 1
    let stats1 = index::run(&mut conn, &root).expect("first index run failed");
    let files1 = query_count(&conn, "files");
    let syms1 = query_count(&conn, "symbols");

    // Run 2 — identical project, no changes
    let stats2 = index::run(&mut conn, &root).expect("second index run failed");
    let files2 = query_count(&conn, "files");
    let syms2 = query_count(&conn, "symbols");

    assert_eq!(stats1.files, stats2.files, "file count must be stable");
    assert_eq!(files1, files2, "files table row count must be stable");
    assert_eq!(syms1, syms2, "symbols table row count must be stable");
}

#[test]
fn test_olaf_dir_not_walked() {
    let dir = tempdir().unwrap();

    // Create a fake .olaf dir with a source file inside it
    let olaf_dir = dir.path().join(".olaf");
    std::fs::create_dir_all(&olaf_dir).unwrap();
    std::fs::write(olaf_dir.join("internal.rs"), "fn secret_internal() {}").unwrap();

    // Also create a real supported source file
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let internal_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '.olaf/%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(internal_count, 0, ".olaf/ contents must never appear in files table");
}

#[test]
fn test_unsupported_extension_skipped() {
    let dir = tempdir().unwrap();

    // Create unsupported files alongside a supported one
    std::fs::write(dir.path().join("README.md"), "# readme").unwrap();
    std::fs::write(dir.path().join("notes.txt"), "some notes").unwrap();
    std::fs::write(dir.path().join("main.py"), "def hello(): pass").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let txt_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '%.txt'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(txt_count, 0, ".txt files must not be in files table");

    let md_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '%.md'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(md_count, 0, ".md files must not be in files table");

    let py_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path LIKE '%.py'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(py_count, 1, "Python file must be indexed");
}

#[test]
fn test_stale_files_deleted() {
    let dir = tempdir().unwrap();

    // Create two source files
    let a_path = dir.path().join("a.rs");
    let b_path = dir.path().join("b.rs");
    std::fs::write(&a_path, "fn func_a() {}").unwrap();
    std::fs::write(&b_path, "fn func_b() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();

    // First index — both files appear
    index::run(&mut conn, dir.path()).expect("first index failed");
    let count_before = query_count(&conn, "files");
    assert_eq!(count_before, 2);

    // Delete b.rs
    std::fs::remove_file(&b_path).unwrap();

    // Re-index — stale b.rs must be removed
    index::run(&mut conn, dir.path()).expect("second index failed");
    let count_after = query_count(&conn, "files");
    assert_eq!(count_after, 1, "deleted file must be removed from files table");

    let b_in_db: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path = 'b.rs'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(b_in_db, 0, "b.rs must have zero rows after deletion");
}

/// Regression: previously indexed source files must be cleaned up even when
/// re-index finds no supported files (e.g., all `.rs` files deleted, only README.md left).
#[test]
fn test_stale_files_deleted_when_no_supported_files_remain() {
    let dir = tempdir().unwrap();

    // First run: index a Rust source file
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();
    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("first index failed");
    assert_eq!(query_count(&conn, "files"), 1, "should have 1 file after first run");

    // Delete the source file; leave only an unsupported file
    std::fs::remove_file(dir.path().join("main.rs")).unwrap();
    std::fs::write(dir.path().join("README.md"), "# docs").unwrap();

    // Second run: no supported files — stale main.rs row must be removed
    index::run(&mut conn, dir.path()).expect("second index failed");
    assert_eq!(
        query_count(&conn, "files"),
        0,
        "stale row must be removed when no supported files remain"
    );
}

/// Regression: stale rows must be deleted even when the project directory is
/// completely empty (candidates_seen = 0, so the previous counter-based guard
/// would skip cleanup and leave stale rows).
#[test]
fn test_stale_files_deleted_when_project_dir_empty() {
    let dir = tempdir().unwrap();

    // First run: index one source file
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();
    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("first index failed");
    assert_eq!(query_count(&conn, "files"), 1);

    // Delete ALL files — project directory is now empty
    std::fs::remove_file(dir.path().join("main.rs")).unwrap();

    // Re-index empty directory → stale main.rs row must be removed
    index::run(&mut conn, dir.path()).expect("second index of empty dir failed");
    assert_eq!(
        query_count(&conn, "files"),
        0,
        "stale row must be removed when project directory is empty"
    );
}

/// Calls edges must be persisted for function→function intra-project calls.
/// Parsers emit bare names as target_fqn; kind-filtered fallback resolves them.
#[test]
fn test_calls_edges_persisted_for_intra_project_calls() {
    let dir = tempdir().unwrap();

    // Single TypeScript file: main calls helper — both are functions
    std::fs::write(
        dir.path().join("app.ts"),
        "export function helper() {}\nexport function main() { helper(); }",
    )
    .unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let calls_count = query_count_where(&conn, "edges", "kind", "calls");
    assert!(
        calls_count >= 1,
        "function→function call edge must be persisted; got {}",
        calls_count
    );
}

/// Regression: calls edges must NOT resolve a call `foo()` to a class named `foo`
/// in a different file, even if it is the only symbol with that name.
/// Only `function`/`method` symbols are valid targets for `calls` edges.
#[test]
fn test_calls_edges_do_not_resolve_to_class() {
    let dir = tempdir().unwrap();

    // a.ts: function caller() calls foo() — bare name
    std::fs::write(
        dir.path().join("a.ts"),
        "export function caller() { foo(); }",
    )
    .unwrap();
    // b.ts: class foo — same name, wrong kind
    std::fs::write(dir.path().join("b.ts"), "export class foo {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let calls_count = query_count_where(&conn, "edges", "kind", "calls");
    assert_eq!(
        calls_count,
        0,
        "foo() must not produce a calls edge to class foo — kind mismatch must block it; got {}",
        calls_count
    );
}

#[test]
fn test_import_edges_not_persisted() {
    let dir = tempdir().unwrap();

    // TypeScript file with an import statement
    std::fs::write(
        dir.path().join("app.ts"),
        "import { foo } from './lib';\nexport function main() { foo(); }",
    )
    .unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let imports_count = query_count_where(&conn, "edges", "kind", "imports");
    assert_eq!(imports_count, 0, "Imports edges must not be persisted (AC9)");
}

// ── Incremental re-index tests (Story 1.5) ──────────────────────────────────

#[test]
fn test_incremental_unchanged_returns_zero_files() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();
    std::fs::write(dir.path().join("lib.rs"), "pub fn helper() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    let stats = run_incremental(&mut conn, dir.path()).expect("incremental failed");
    assert_eq!(stats.files, 0, "no files changed — incremental must re-parse 0 files");
}

#[test]
fn test_incremental_changed_file_reparsed() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    let hash_before: String = conn
        .query_row("SELECT blake3_hash FROM files WHERE path = 'main.rs'", [], |r| r.get(0))
        .unwrap();

    // Overwrite with different content (different hash)
    std::fs::write(dir.path().join("main.rs"), "fn main() { println!(\"hi\"); }").unwrap();

    let stats = run_incremental(&mut conn, dir.path()).expect("incremental failed");
    assert_eq!(stats.files, 1, "changed file must be re-parsed");

    // Assert the stored hash actually changed — not just non-empty (which was true before too)
    let hash_after: String = conn
        .query_row("SELECT blake3_hash FROM files WHERE path = 'main.rs'", [], |r| r.get(0))
        .unwrap();
    assert_ne!(hash_after, hash_before, "blake3_hash must be updated to the new content's hash");
}

#[test]
fn test_incremental_new_file_indexed() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    // Add a new file
    std::fs::write(dir.path().join("new_module.rs"), "pub fn new_fn() {}").unwrap();

    let stats = run_incremental(&mut conn, dir.path()).expect("incremental failed");
    assert_eq!(stats.files, 1, "new file must be indexed");

    let new_in_db: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE path = 'new_module.rs'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_in_db, 1, "new file must appear in files table");
}

#[test]
fn test_incremental_deleted_file_removed() {
    let dir = tempdir().unwrap();
    let a_path = dir.path().join("a.rs");
    let b_path = dir.path().join("b.rs");
    std::fs::write(&a_path, "fn func_a() {}").unwrap();
    std::fs::write(&b_path, "fn func_b() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    // Delete b.rs
    std::fs::remove_file(&b_path).unwrap();

    let stats = run_incremental(&mut conn, dir.path()).expect("incremental failed");
    // Deleted files are NOT counted in files (AC6)
    assert_eq!(stats.files, 0, "deleted file must not count as re-indexed");

    let b_in_db: i64 = conn
        .query_row("SELECT COUNT(*) FROM files WHERE path = 'b.rs'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(b_in_db, 0, "deleted file must be removed from files table");
}

#[test]
fn test_incremental_preserves_inbound_edges() {
    let dir = tempdir().unwrap();

    // Use TypeScript — we know from test_calls_edges_persisted_for_intra_project_calls that
    // the TS parser emits calls edges for function→function calls, guaranteeing edges > 0.
    //
    // File A (caller.ts): calls helper() from File B (lib.ts)
    // File B (lib.ts): defines helper() — this is the target we will incrementally re-index
    std::fs::write(dir.path().join("lib.ts"), "export function helper() {}").unwrap();
    std::fs::write(
        dir.path().join("caller.ts"),
        "export function caller() { helper(); }",
    )
    .unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    let edges_after_full: i64 =
        conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
    assert!(
        edges_after_full > 0,
        "full index must produce at least one calls edge (caller → helper)"
    );

    // Modify B (lib.ts, the target) with a comment change that preserves helper() FQN.
    // This forces a hash mismatch so incremental re-parses lib.ts, but the FQN is stable
    // so update_file_symbols must UPDATE in-place (not DELETE+INSERT).
    std::fs::write(dir.path().join("lib.ts"), "// updated\nexport function helper() {}").unwrap();
    // Leave caller.ts (A) unchanged

    let stats = run_incremental(&mut conn, dir.path()).expect("incremental failed");
    assert_eq!(stats.files, 1, "only lib.ts (B) should be re-indexed");

    // lib.ts symbols must survive with the same FQN
    let lib_sym_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM symbols s JOIN files f ON s.file_id = f.id WHERE f.path LIKE '%lib.ts'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(lib_sym_count > 0, "lib.ts symbols must survive incremental re-index");

    // The core regression guard: inbound edges to lib.ts's helper() must not have been
    // cascade-deleted when update_file_symbols updated the symbol in-place.
    // If update_file_symbols had used DELETE+INSERT, helper's symbol id would change and
    // all FK edges pointing to the old id would be wiped by ON DELETE CASCADE.
    let edges_after_incremental: i64 =
        conn.query_row("SELECT COUNT(*) FROM edges", [], |r| r.get(0)).unwrap();
    assert_eq!(
        edges_after_incremental, edges_after_full,
        "incremental re-index of B must not cascade-delete edges from A to B's surviving symbols"
    );
}

/// Regression: `reindex_single_file` must resolve and insert new calls edges when a file gains
/// new function-to-function calls — the shared `resolve_and_insert_file_edges` helper path.
#[test]
fn test_reindex_single_file_inserts_new_edges() {
    let dir = tempdir().unwrap();

    // Initial state: helper exists but main does not yet call it
    std::fs::write(dir.path().join("app.ts"), "export function helper() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    let edges_before: i64 =
        conn.query_row("SELECT COUNT(*) FROM edges WHERE kind = 'calls'", [], |r| r.get(0)).unwrap();
    assert_eq!(edges_before, 0, "no calls edges before edit");

    // Edit: add main() which calls helper() — new calls edge should appear
    std::fs::write(
        dir.path().join("app.ts"),
        "export function helper() {}\nexport function main() { helper(); }",
    )
    .unwrap();

    let outcome = reindex_single_file(&mut conn, dir.path(), "app.ts")
        .expect("reindex_single_file must not error");
    assert!(
        matches!(outcome, index::ReindexOutcome::Changed(_)),
        "modified file must produce Changed outcome"
    );

    let edges_after: i64 =
        conn.query_row("SELECT COUNT(*) FROM edges WHERE kind = 'calls'", [], |r| r.get(0)).unwrap();
    assert!(
        edges_after >= 1,
        "reindex_single_file must insert the new main→helper calls edge; got {edges_after}"
    );
}

#[test]
#[ignore]
fn test_incremental_performance() {
    use std::time::Instant;

    let dir = tempdir().unwrap();

    // Generate 499 unchanged + 1 file that will be changed
    for i in 0..499 {
        let content = format!("pub fn generated_fn_{i}() {{}}\n");
        std::fs::write(dir.path().join(format!("file_{i:04}.rs")), content).unwrap();
    }
    std::fs::write(dir.path().join("changing.rs"), "pub fn original() {}").unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("full index failed");

    // Change only one file
    std::fs::write(dir.path().join("changing.rs"), "pub fn updated() {}").unwrap();

    let start = Instant::now();
    let stats = run_incremental(&mut conn, dir.path()).expect("incremental failed");
    let elapsed = start.elapsed();

    assert_eq!(stats.files, 1, "only the changed file should be re-indexed");
    assert!(
        elapsed.as_millis() <= 200,
        "incremental re-index of 1 file in 500-file project must complete in ≤200ms (took {:?})",
        elapsed
    );
}

#[test]
#[ignore]
fn test_performance_500_files() {
    use std::time::Instant;

    let dir = tempdir().unwrap();

    // Generate 500 minimal Rust files
    for i in 0..500 {
        let content = format!("pub fn generated_fn_{i}() {{}}\n");
        std::fs::write(dir.path().join(format!("file_{i:04}.rs")), content).unwrap();
    }

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();

    let start = Instant::now();
    let stats = index::run(&mut conn, dir.path()).expect("index failed");
    let elapsed = start.elapsed();

    assert_eq!(stats.files, 500, "should index all 500 files");
    assert!(
        elapsed.as_secs() < 5,
        "full index of 500 files must complete in under 5 seconds (took {:?})",
        elapsed
    );
}

/// E2E: Go function in file_a calls unique function in file_b → edge persists after indexer resolution.
#[test]
fn test_go_calls_edge_persists_cross_file() {
    let dir = tempdir().unwrap();

    std::fs::write(
        dir.path().join("a.go"),
        "package main\nfunc Caller() { UniqueTarget() }\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.go"),
        "package main\nfunc UniqueTarget() {}\n",
    )
    .unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let calls_count = query_count_where(&conn, "edges", "kind", "calls");
    assert!(
        calls_count >= 1,
        "Go cross-file call edge must persist when target is unambiguous; got {}",
        calls_count
    );
    // Verify the specific edge FQNs, not just count
    let has_expected: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM edges e
             JOIN symbols s1 ON e.source_id = s1.id
             JOIN symbols s2 ON e.target_id = s2.id
             WHERE s1.fqn = 'a.go::Caller' AND s2.fqn = 'b.go::UniqueTarget' AND e.kind = 'calls'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
        > 0;
    assert!(
        has_expected,
        "expected Caller -> UniqueTarget edge with resolved FQNs"
    );
}

/// E2E: Go function calls a name that exists in multiple files → edge dropped (ambiguous).
#[test]
fn test_go_ambiguous_call_edge_dropped() {
    let dir = tempdir().unwrap();

    std::fs::write(
        dir.path().join("a.go"),
        "package main\nfunc Caller() { Ambiguous() }\n",
    )
    .unwrap();
    // Same function name in two different files — ambiguous
    std::fs::write(
        dir.path().join("b.go"),
        "package main\nfunc Ambiguous() {}\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("c.go"),
        "package main\nfunc Ambiguous() {}\n",
    )
    .unwrap();

    let db_path = dir.path().join("index.db");
    let mut conn = db::open(&db_path).unwrap();
    index::run(&mut conn, dir.path()).expect("index::run failed");

    let calls_count = query_count_where(&conn, "edges", "kind", "calls");
    assert_eq!(
        calls_count, 0,
        "ambiguous Go call should NOT persist — 2 candidates; got {}",
        calls_count
    );
}
