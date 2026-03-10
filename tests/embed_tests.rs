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
