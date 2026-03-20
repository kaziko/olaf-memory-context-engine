use assert_cmd::Command;
use std::fs;

/// Helper: create an initialized + indexed olaf project with some observations.
#[allow(deprecated)]
fn setup_project_with_observations(dir: &std::path::Path) {
    fs::write(dir.join("main.rs"), "pub fn main() {}").unwrap();

    // init + index
    Command::cargo_bin("olaf").unwrap()
        .current_dir(dir).arg("init").output().unwrap();
    Command::cargo_bin("olaf").unwrap()
        .current_dir(dir).arg("index").output().unwrap();

    // Insert observations directly into the DB
    let db_path = dir.join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO sessions (id, started_at, agent) VALUES ('s1', 1000, 'test')",
        [],
    ).unwrap();
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance)
         VALUES ('s1', 1000, 'insight', 'auth module needs refactoring', 0, 0, 'medium')",
        [],
    ).unwrap();
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance)
         VALUES ('s1', 1001, 'decision', 'use tokio for async runtime', 0, 0, 'high')",
        [],
    ).unwrap();
    // Noise kind — should be skipped by embed
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance)
         VALUES ('s1', 1002, 'context_retrieval', 'noise entry', 0, 1, 'low')",
        [],
    ).unwrap();
}

// ── CLI integration tests (environment-independent) ──

#[test]
#[cfg(not(feature = "embeddings"))]
fn embed_without_feature_flag_shows_helpful_error() {
    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path())
        .arg("embed")
        .output()
        .unwrap();

    assert!(!output.status.success(), "should fail without embeddings feature");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("embeddings") && stderr.contains("feature"),
        "should mention the embeddings feature flag; got: {stderr}"
    );
}

#[test]
fn embed_without_database_shows_error() {
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path())
        .arg("embed")
        .output()
        .unwrap();

    assert!(!output.status.success());
}

#[test]
fn embed_rejects_unknown_flag() {
    // Verifies clap parses the Embed subcommand correctly
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path())
        .arg("embed")
        .arg("--nonexistent-flag")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unexpected argument") || stderr.contains("error"),
        "clap should reject unknown flags; got: {stderr}"
    );
}

#[test]
fn embed_rebuild_flag_is_parsed_by_clap() {
    // --rebuild should be accepted by clap (not "unexpected argument").
    // The command may still fail for other reasons (no runtime, no feature),
    // but the flag itself must be recognized.
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path())
        .arg("embed")
        .arg("--rebuild")
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unexpected argument"),
        "--rebuild should be accepted by clap; got: {stderr}"
    );
}

#[test]
fn embed_batch_size_flag_is_parsed_by_clap() {
    // --batch-size N should be accepted by clap.
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path())
        .arg("embed")
        .arg("--batch-size")
        .arg("32")
        .output()
        .unwrap();

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unexpected argument"),
        "--batch-size should be accepted by clap; got: {stderr}"
    );
}

#[test]
fn embed_batch_size_requires_value() {
    // --batch-size without a value should be rejected by clap.
    let dir = tempfile::tempdir().unwrap();

    #[allow(deprecated)]
    let output = Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path())
        .arg("embed")
        .arg("--batch-size")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("requires a value") || stderr.contains("error"),
        "--batch-size without value should fail; got: {stderr}"
    );
}

// ── Library-level embed pipeline tests (no runtime/network dependency) ──

#[test]
fn embed_idempotency_via_db_state() {
    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let unembedded = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    assert_eq!(unembedded.len(), 2, "should find 2 eligible observations");

    for id in &unembedded {
        olaf::memory::embedder::store_embedding(
            &conn, *id, "test-model", "v1", 4, &[0.1, 0.2, 0.3, 0.4],
        ).unwrap();
    }

    let unembedded_again = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    assert_eq!(unembedded_again.len(), 0, "second run should find nothing to embed");
}

#[test]
fn embed_rebuild_clears_and_re_exposes() {
    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let ids = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    for id in &ids {
        olaf::memory::embedder::store_embedding(
            &conn, *id, "test-model", "v1", 4, &[1.0, 2.0, 3.0, 4.0],
        ).unwrap();
    }
    assert_eq!(
        olaf::memory::embedder::get_unembedded_observation_ids(&conn, "test-model", "v1")
            .unwrap().len(),
        0
    );

    let deleted = olaf::memory::embedder::delete_all_embeddings(&conn).unwrap();
    assert_eq!(deleted, 2);

    let after_rebuild = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    assert_eq!(after_rebuild.len(), 2, "rebuild should re-expose all eligible observations");
}

#[test]
fn embed_skips_noise_kinds() {
    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    for kind in &["tool_call", "file_change"] {
        conn.execute(
            &format!(
                "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance)
                 VALUES ('s1', 1003, '{}', 'noise', 0, 1, 'low')", kind
            ),
            [],
        ).unwrap();
    }

    let unembedded = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    assert_eq!(unembedded.len(), 2, "should skip all noise kinds");
}

#[test]
fn embed_skips_stale_observations() {
    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    // Add a stale observation
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, is_stale, stale_reason, auto_generated, importance)
         VALUES ('s1', 1004, 'insight', 'stale insight', 1, 'outdated', 0, 'medium')",
        [],
    ).unwrap();

    let unembedded = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    assert_eq!(unembedded.len(), 2, "should skip stale observations");
}

#[test]
fn embed_skips_consolidated_observations() {
    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    // Add a consolidated observation (consolidated_into points to another obs)
    conn.execute(
        "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance, consolidated_into)
         VALUES ('s1', 1005, 'insight', 'consolidated', 0, 0, 'medium', 1)",
        [],
    ).unwrap();

    let unembedded = olaf::memory::embedder::get_unembedded_observation_ids(
        &conn, "test-model", "v1",
    ).unwrap();
    assert_eq!(unembedded.len(), 2, "should skip consolidated observations");
}

/// Exercises the full embed pipeline (query → canonical text → embed → store → load)
/// using deterministic test vectors — no ONNX runtime or network needed.
#[test]
fn embed_full_pipeline_deterministic() {
    use olaf::memory::embedder::*;

    let dir = tempfile::tempdir().unwrap();
    setup_project_with_observations(dir.path());

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let model_id = "test-model";
    let model_rev = "v1";
    let dims: i32 = 4;

    // Step 1: Get unembedded IDs
    let unembedded = get_unembedded_observation_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(unembedded.len(), 2);

    // Step 2: Load observations and verify canonical text
    let observations = get_observations_by_ids(&conn, &unembedded).unwrap();
    assert_eq!(observations.len(), 2);
    let texts: Vec<String> = observations.iter().map(canonical_text).collect();
    assert!(texts[0].contains("insight"), "canonical text should include kind");
    assert!(texts[0].contains("auth module"), "canonical text should include content");

    // Step 3: Create deterministic embeddings (no model needed)
    let embeddings: Vec<Vec<f32>> = vec![
        vec![0.5, 0.5, 0.5, 0.5],   // obs 1
        vec![-0.3, 0.7, 0.1, -0.2],  // obs 2
    ];

    // Step 4: Store embeddings
    for (obs, embedding) in observations.iter().zip(embeddings.iter()) {
        store_embedding(&conn, obs.id, model_id, model_rev, dims, embedding).unwrap();
    }

    // Step 5: Verify idempotency
    let still_unembedded = get_unembedded_observation_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(still_unembedded.len(), 0, "all should be embedded now");

    // Step 6: Load and verify roundtrip
    let loaded = load_embeddings(&conn, &unembedded, model_id, model_rev).unwrap();
    assert_eq!(loaded.len(), 2);
    for (obs, original) in observations.iter().zip(embeddings.iter()) {
        assert_eq!(&loaded[&obs.id], original, "loaded embedding should match stored");
    }

    // Step 7: Cosine similarity produces expected results
    let query_vec = vec![0.5, 0.5, 0.5, 0.5]; // identical to obs 1
    let sim_1 = cosine_similarity(&query_vec, &embeddings[0]);
    let sim_2 = cosine_similarity(&query_vec, &embeddings[1]);
    assert!((sim_1 - 1.0).abs() < 1e-6, "identical vectors: cosine should be 1.0, got {sim_1}");
    assert!(sim_1 > sim_2, "obs 1 should be more similar to query than obs 2");

    // Step 8: Model compatibility — different model_rev returns empty
    let wrong_rev = load_embeddings(&conn, &unembedded, model_id, "v2").unwrap();
    assert!(wrong_rev.is_empty(), "different model_rev should return empty");

    // Step 9: Rebuild clears everything
    let deleted = delete_all_embeddings(&conn).unwrap();
    assert_eq!(deleted, 2);
    let after_rebuild = get_unembedded_observation_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(after_rebuild.len(), 2, "rebuild should re-expose all");
}

/// Exercises the full symbol embed pipeline (query unembedded → canonical text → store → load)
/// using deterministic test vectors — no ONNX runtime or network needed.
#[test]
fn symbol_embed_full_pipeline_deterministic() {
    use olaf::memory::embedder::*;

    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("lib.rs"), "pub fn alpha() {}\npub fn beta() {}").unwrap();

    #[allow(deprecated)]
    Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path()).arg("init").output().unwrap();
    #[allow(deprecated)]
    Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path()).arg("index").output().unwrap();

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let model_id = "test-model";
    let model_rev = "v1";
    let dims: i32 = 4;

    // Verify symbols were indexed
    let sym_count: i64 = conn.query_row("SELECT COUNT(*) FROM symbols", [], |r| r.get(0)).unwrap();
    assert!(sym_count >= 1, "indexing should produce at least one symbol");

    // Step 1: All symbols are initially unembedded
    let unembedded = get_unembedded_symbol_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(unembedded.len() as i64, sym_count);

    // Step 2: Store a deterministic embedding for each symbol
    let dummy_embedding = vec![0.5f32, 0.5, 0.5, 0.5];
    for &sym_id in &unembedded {
        store_symbol_embedding(&conn, sym_id, model_id, model_rev, dims, &dummy_embedding).unwrap();
    }

    // Step 3: All symbols now have embeddings — query returns empty
    let still_unembedded = get_unembedded_symbol_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(still_unembedded.len(), 0, "all symbols should be embedded now");

    // Step 4: Load and verify round-trip
    let loaded = load_symbol_embeddings_for_ids(&conn, &unembedded, model_id, model_rev).unwrap();
    assert_eq!(loaded.len(), unembedded.len());
    for &sym_id in &unembedded {
        assert_eq!(loaded[&sym_id], dummy_embedding, "loaded embedding must match stored");
    }

    // Step 5: Wrong model_rev returns empty
    let wrong_rev = load_symbol_embeddings_for_ids(&conn, &unembedded, model_id, "v99").unwrap();
    assert!(wrong_rev.is_empty(), "wrong model_rev should return empty");

    // Step 6: delete_all_symbol_embeddings clears everything and re-exposes symbols
    let deleted = delete_all_symbol_embeddings(&conn).unwrap();
    assert_eq!(deleted as i64, sym_count);

    let after_clear = get_unembedded_symbol_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(after_clear.len() as i64, sym_count, "clear should re-expose all symbols");

    // Step 7: Idempotency — storing the same embedding twice doesn't duplicate rows
    store_symbol_embedding(&conn, unembedded[0], model_id, model_rev, dims, &dummy_embedding).unwrap();
    store_symbol_embedding(&conn, unembedded[0], model_id, model_rev, dims, &[0.1, 0.2, 0.3, 0.4]).unwrap();
    let row_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM symbol_embeddings WHERE symbol_id = ?1",
        rusqlite::params![unembedded[0]],
        |r| r.get(0),
    ).unwrap();
    assert_eq!(row_count, 1, "INSERT OR REPLACE must not duplicate rows");

    // Step 8: Verify the FTS5 index is populated (prerequisite for retrieval).
    // rank_symbols_by_keywords and FakeEmbedder are pub(crate) and not accessible here.
    // The three-signal retrieval path is covered by rrf_three_signal_with_embeddings
    // in graph/query.rs which can use FakeEmbedder directly.
    let fts_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM symbols_fts", [], |r| r.get(0),
    ).unwrap();
    assert!(fts_count > 0, "FTS5 index must be populated for retrieval to work");
    assert_eq!(fts_count, sym_count, "every symbol must have an FTS5 entry");
}

/// Regression test: symbol phase must run even when all observations are already embedded.
///
/// `olaf embed` had a bug where `if obs_total == 0 { return Ok(()); }` silently skipped
/// the entire symbol phase. This test verifies the data contract: after pre-embedding all
/// observations (obs_total = 0), symbols remain unembedded and are still processable.
///
/// Note: this exercises library functions rather than the CLI binary because `olaf embed`
/// requires an ONNX runtime to load the model, making it unsuitable for CI. The CLI
/// control flow itself is covered by code review; this test protects the underlying data
/// layer that the CLI depends on.
#[test]
fn symbol_phase_data_contract_when_obs_already_embedded() {
    use olaf::memory::embedder::*;

    let dir = tempfile::tempdir().unwrap();
    // observations (from setup_project_with_observations) + a source file for symbols
    setup_project_with_observations(dir.path());
    fs::write(dir.path().join("lib.rs"), "pub fn process() {}\npub fn handle() {}").unwrap();

    #[allow(deprecated)]
    Command::cargo_bin("olaf").unwrap()
        .current_dir(dir.path()).arg("index").output().unwrap();

    let db_path = dir.path().join(".olaf/index.db");
    let conn = olaf::db::open(&db_path).unwrap();

    let model_id = "test-model";
    let model_rev = "v1";
    let dims = 4i32;
    let dummy = vec![0.5f32, 0.5, 0.5, 0.5];

    // Pre-embed ALL observations — this is the state that triggered the early-return bug.
    let obs_ids = get_unembedded_observation_ids(&conn, model_id, model_rev).unwrap();
    assert!(!obs_ids.is_empty(), "setup should produce embeddable observations");
    for id in &obs_ids {
        store_embedding(&conn, *id, model_id, model_rev, dims, &dummy).unwrap();
    }
    let remaining_obs = get_unembedded_observation_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(remaining_obs.len(), 0, "all observations must be pre-embedded");

    // With obs_total = 0, the symbol phase must still see unembedded symbols.
    let unembedded_syms = get_unembedded_symbol_ids(&conn, model_id, model_rev).unwrap();
    assert!(!unembedded_syms.is_empty(), "symbols must be unembedded after obs phase completes");

    // Run the symbol phase (the library calls that `olaf embed` would make).
    for &sym_id in &unembedded_syms {
        store_symbol_embedding(&conn, sym_id, model_id, model_rev, dims, &dummy).unwrap();
    }

    let after = get_unembedded_symbol_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(after.len(), 0, "symbol phase must embed all symbols");

    // Sanity: observation embeddings are still intact.
    let obs_after = get_unembedded_observation_ids(&conn, model_id, model_rev).unwrap();
    assert_eq!(obs_after.len(), 0, "obs embeddings must not be disturbed by symbol phase");
}
