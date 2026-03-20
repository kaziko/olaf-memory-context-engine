use super::store::ObservationRow;
use std::collections::HashMap;

/// Cosine similarity between two equal-length f32 slices.
/// Returns 0.0 for zero-length or zero-magnitude vectors.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

/// Build the canonical text representation of an observation for embedding.
/// Combines kind, content, symbol_fqn, and file_path for better disambiguation.
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn canonical_text(obs: &ObservationRow) -> String {
    let mut parts = Vec::with_capacity(4);
    parts.push(obs.kind.as_str());
    parts.push(obs.content.as_str());
    if let Some(ref fqn) = obs.symbol_fqn {
        parts.push(fqn.as_str());
    }
    if let Some(ref fp) = obs.file_path {
        parts.push(fp.as_str());
    }
    parts.join(" ")
}

/// Trait for embedding text into vectors. Production uses fastembed; tests use FakeEmbedder.
pub trait EmbedText {
    #[allow(dead_code)] // Used by CLI embed command (cfg-gated)
    fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError>;
    fn embed_query(&self, query: &str) -> Result<Vec<f32>, EmbedError>;
    fn model_id(&self) -> &str;
    fn model_rev(&self) -> &str;
    #[allow(dead_code)] // Used by CLI embed command (cfg-gated)
    fn dims(&self) -> usize;
}

#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    #[allow(dead_code)] // Constructed by FastEmbedder (cfg-gated)
    #[error("embedding model error: {0}")]
    Model(String),
}

// ── Production embedder (feature-gated) ──

#[cfg(feature = "embeddings")]
pub struct FastEmbedder {
    model: fastembed::TextEmbedding,
    model_id: String,
    model_rev: String,
    dims: usize,
}

#[cfg(feature = "embeddings")]
impl FastEmbedder {
    pub fn new(cache_dir: &std::path::Path) -> Result<Self, EmbedError> {
        use fastembed::{InitOptions, EmbeddingModel};

        let options = InitOptions::new(EmbeddingModel::AllMiniLML6V2)
            .with_cache_dir(cache_dir.to_path_buf())
            .with_show_download_progress(true);

        let model = fastembed::TextEmbedding::try_new(options)
            .map_err(|e| EmbedError::Model(e.to_string()))?;

        Ok(Self {
            model,
            model_id: "all-MiniLM-L6-v2".to_string(),
            model_rev: "v1".to_string(),
            dims: 384,
        })
    }
}

#[cfg(feature = "embeddings")]
impl EmbedText for FastEmbedder {
    fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let docs: Vec<String> = texts.iter().map(|s| s.to_string()).collect();
        self.model
            .embed(docs, None)
            .map_err(|e| EmbedError::Model(e.to_string()))
    }

    fn embed_query(&self, query: &str) -> Result<Vec<f32>, EmbedError> {
        let results = self.model
            .embed(vec![query.to_string()], None)
            .map_err(|e| EmbedError::Model(e.to_string()))?;
        results.into_iter().next().ok_or_else(|| EmbedError::Model("empty result".into()))
    }

    fn model_id(&self) -> &str { &self.model_id }
    fn model_rev(&self) -> &str { &self.model_rev }
    fn dims(&self) -> usize { self.dims }
}

// ── Test embedder (always available for tests) ──

/// Deterministic embedder for tests. Returns vectors based on simple hashing
/// so tests don't require model downloads or network access.
#[cfg(test)]
pub struct FakeEmbedder {
    dims: usize,
}

#[cfg(test)]
impl FakeEmbedder {
    pub fn new(dims: usize) -> Self {
        Self { dims }
    }

    fn hash_to_vector(&self, text: &str) -> Vec<f32> {
        let mut vec = vec![0.0f32; self.dims];
        // Simple deterministic hash: distribute bytes across dimensions
        for (i, byte) in text.bytes().enumerate() {
            let idx = i % self.dims;
            vec[idx] += (byte as f32 - 128.0) / 128.0;
        }
        // Normalize to unit vector
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut vec {
                *v /= norm;
            }
        }
        vec
    }
}

#[cfg(test)]
impl EmbedText for FakeEmbedder {
    fn embed_texts(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|t| self.hash_to_vector(t)).collect())
    }

    fn embed_query(&self, query: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(self.hash_to_vector(query))
    }

    fn model_id(&self) -> &str { "fake-model" }
    fn model_rev(&self) -> &str { "v1" }
    fn dims(&self) -> usize { self.dims }
}

// ── Embedding storage helpers ──

/// Store an embedding for an observation.
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn store_embedding(
    conn: &rusqlite::Connection,
    obs_id: i64,
    model_id: &str,
    model_rev: &str,
    dims: i32,
    embedding: &[f32],
) -> Result<(), super::store::StoreError> {
    let blob = embedding_to_blob(embedding);
    conn.execute(
        "INSERT OR REPLACE INTO observation_embeddings (observation_id, model_id, model_rev, dims, embedding, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, strftime('%s', 'now'))",
        rusqlite::params![obs_id, model_id, model_rev, dims, blob],
    )?;
    Ok(())
}

/// Load embeddings for a set of observation IDs, filtered by model_id + model_rev.
pub fn load_embeddings(
    conn: &rusqlite::Connection,
    obs_ids: &[i64],
    model_id: &str,
    model_rev: &str,
) -> Result<HashMap<i64, Vec<f32>>, super::store::StoreError> {
    if obs_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders: Vec<String> = (0..obs_ids.len()).map(|i| format!("?{}", i + 3)).collect();
    let sql = format!(
        "SELECT observation_id, embedding FROM observation_embeddings
         WHERE model_id = ?1 AND model_rev = ?2 AND observation_id IN ({})",
        placeholders.join(", ")
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params.push(Box::new(model_id.to_string()));
    params.push(Box::new(model_rev.to_string()));
    for id in obs_ids {
        params.push(Box::new(*id));
    }
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        let obs_id: i64 = row.get(0)?;
        let blob: Vec<u8> = row.get(1)?;
        Ok((obs_id, blob))
    })?;

    let mut result = HashMap::new();
    for row in rows {
        let (obs_id, blob) = row?;
        result.insert(obs_id, blob_to_embedding(&blob));
    }
    Ok(result)
}

#[allow(dead_code)] // Used by store_embedding (cfg-gated caller)
fn embedding_to_blob(embedding: &[f32]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(embedding.len() * 4);
    for &val in embedding {
        blob.extend_from_slice(&val.to_le_bytes());
    }
    blob
}

fn blob_to_embedding(blob: &[u8]) -> Vec<f32> {
    blob.chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

/// Delete all embeddings (used by --rebuild).
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn delete_all_embeddings(conn: &rusqlite::Connection) -> Result<u64, super::store::StoreError> {
    let count = conn.execute("DELETE FROM observation_embeddings", [])?;
    Ok(count as u64)
}

/// Get observation IDs that are eligible for embedding but don't have one yet.
/// Eligible = non-stale AND not noise kinds (context_retrieval, tool_call, file_change)
/// AND not consolidated.
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn get_unembedded_observation_ids(
    conn: &rusqlite::Connection,
    model_id: &str,
    model_rev: &str,
) -> Result<Vec<i64>, super::store::StoreError> {
    let mut stmt = conn.prepare(
        "SELECT o.id FROM observations o
         LEFT JOIN observation_embeddings e
           ON o.id = e.observation_id AND e.model_id = ?1 AND e.model_rev = ?2
         WHERE e.observation_id IS NULL
           AND o.is_stale = 0
           AND o.kind NOT IN ('context_retrieval', 'tool_call', 'file_change')
           AND o.consolidated_into IS NULL
         ORDER BY o.id"
    )?;
    let ids = stmt.query_map(rusqlite::params![model_id, model_rev], |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(ids)
}

/// Load observation rows by IDs for embedding.
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn get_observations_by_ids(
    conn: &rusqlite::Connection,
    ids: &[i64],
) -> Result<Vec<ObservationRow>, super::store::StoreError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders: Vec<String> = (0..ids.len()).map(|i| format!("?{}", i + 1)).collect();
    let sql = format!(
        "SELECT id, session_id, created_at, kind, content, symbol_fqn, file_path,
                is_stale, stale_reason, confidence, branch, importance
         FROM observations WHERE id IN ({})",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<Box<dyn rusqlite::types::ToSql>> = ids.iter().map(|id| Box::new(*id) as Box<dyn rusqlite::types::ToSql>).collect();
    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

    let rows = stmt.query_map(param_refs.as_slice(), |row| {
        Ok(ObservationRow {
            id: row.get(0)?,
            session_id: row.get(1)?,
            created_at: row.get(2)?,
            kind: row.get(3)?,
            content: row.get(4)?,
            symbol_fqn: row.get(5)?,
            file_path: row.get(6)?,
            is_stale: row.get::<_, i64>(7)? != 0,
            stale_reason: row.get(8)?,
            confidence: row.get(9)?,
            branch: row.get(10)?,
            importance: row.get(11)?,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(super::store::StoreError::Sqlite)
}

// ── Symbol embedding helpers ──

/// Build the canonical text for a symbol to embed.
/// Concatenates `"{fqn} {name} {signature} {docstring}"`, trimmed.
/// Returns `None` if all of name, signature, and docstring are empty
/// (fqn-only symbols have no meaningful semantic content).
pub fn symbol_canonical_text(
    fqn: &str,
    name: &str,
    signature: &str,
    docstring: &str,
) -> Option<String> {
    if name.is_empty() && signature.is_empty() && docstring.is_empty() {
        return None;
    }
    let text = format!("{} {} {} {}", fqn, name, signature, docstring);
    Some(text.trim().to_string())
}

/// Get symbol IDs that don't yet have an embedding for the given model version.
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn get_unembedded_symbol_ids(
    conn: &rusqlite::Connection,
    model_id: &str,
    model_rev: &str,
) -> Result<Vec<i64>, super::store::StoreError> {
    let mut stmt = conn.prepare(
        "SELECT s.id FROM symbols s
         LEFT JOIN symbol_embeddings e
           ON s.id = e.symbol_id AND e.model_id = ?1 AND e.model_rev = ?2
         WHERE e.symbol_id IS NULL
         ORDER BY s.id",
    )?;
    let ids = stmt
        .query_map(rusqlite::params![model_id, model_rev], |row| row.get(0))?
        .collect::<Result<Vec<i64>, _>>()?;
    Ok(ids)
}

/// Store an embedding for a symbol (upsert).
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn store_symbol_embedding(
    conn: &rusqlite::Connection,
    symbol_id: i64,
    model_id: &str,
    model_rev: &str,
    dims: i32,
    embedding: &[f32],
) -> Result<(), super::store::StoreError> {
    let blob = embedding_to_blob(embedding);
    conn.execute(
        "INSERT OR REPLACE INTO symbol_embeddings (symbol_id, model_id, model_rev, dims, embedding, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, strftime('%s', 'now'))",
        rusqlite::params![symbol_id, model_id, model_rev, dims, blob],
    )?;
    Ok(())
}

/// Bulk-load embeddings for a set of symbol IDs, filtered by model_id + model_rev.
/// Batches into chunks of 999 to respect SQLite's variable limit.
pub fn load_symbol_embeddings_for_ids(
    conn: &rusqlite::Connection,
    ids: &[i64],
    model_id: &str,
    model_rev: &str,
) -> Result<HashMap<i64, Vec<f32>>, super::store::StoreError> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut result = HashMap::new();
    // Chunk at 997 to stay within SQLite's 999 bind-variable limit.
    // Each chunk also binds model_id (?1) and model_rev (?2), so max = 997 + 2 = 999.
    for chunk in ids.chunks(997) {
        let placeholders: Vec<String> = (0..chunk.len()).map(|i| format!("?{}", i + 3)).collect();
        let sql = format!(
            "SELECT symbol_id, embedding FROM symbol_embeddings
             WHERE model_id = ?1 AND model_rev = ?2 AND symbol_id IN ({})",
            placeholders.join(", ")
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        params.push(Box::new(model_id.to_string()));
        params.push(Box::new(model_rev.to_string()));
        for id in chunk {
            params.push(Box::new(*id));
        }
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            let sym_id: i64 = row.get(0)?;
            let blob: Vec<u8> = row.get(1)?;
            Ok((sym_id, blob))
        })?;
        for row in rows {
            let (sym_id, blob) = row?;
            result.insert(sym_id, blob_to_embedding(&blob));
        }
    }
    Ok(result)
}

/// Delete all symbol embeddings (used by `olaf embed --rebuild`).
#[allow(dead_code)] // Used by CLI embed command (cfg-gated)
pub fn delete_all_symbol_embeddings(
    conn: &rusqlite::Connection,
) -> Result<usize, super::store::StoreError> {
    let count = conn.execute("DELETE FROM symbol_embeddings", [])?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical_vectors() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim - 1.0).abs() < 1e-6, "identical vectors should have cosine ~1.0, got {sim}");
    }

    #[test]
    fn test_cosine_orthogonal_vectors() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6, "orthogonal vectors should have cosine ~0.0, got {sim}");
    }

    #[test]
    fn test_cosine_opposite_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim + 1.0).abs() < 1e-6, "opposite vectors should have cosine ~-1.0, got {sim}");
    }

    #[test]
    fn test_cosine_known_angle() {
        // 45-degree angle → cosine = √2/2 ≈ 0.7071
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        let expected = std::f32::consts::FRAC_1_SQRT_2;
        assert!((sim - expected).abs() < 1e-5, "expected {expected}, got {sim}");
    }

    #[test]
    fn test_cosine_empty_vectors() {
        let sim = cosine_similarity(&[], &[]);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_cosine_zero_vector() {
        let a = vec![0.0, 0.0];
        let b = vec![1.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_cosine_mismatched_length() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_canonical_text_full() {
        let obs = ObservationRow {
            id: 1, session_id: "s1".into(), created_at: 0, kind: "insight".into(),
            content: "auth bug found".into(), symbol_fqn: Some("src/auth.rs::login".into()),
            file_path: Some("src/auth.rs".into()), is_stale: false, stale_reason: None,
            confidence: None, branch: None, importance: super::super::store::Importance::Medium,
        };
        let text = canonical_text(&obs);
        assert_eq!(text, "insight auth bug found src/auth.rs::login src/auth.rs");
    }

    #[test]
    fn test_canonical_text_no_anchors() {
        let obs = ObservationRow {
            id: 1, session_id: "s1".into(), created_at: 0, kind: "decision".into(),
            content: "use tokio runtime".into(), symbol_fqn: None,
            file_path: None, is_stale: false, stale_reason: None,
            confidence: None, branch: None, importance: super::super::store::Importance::Medium,
        };
        let text = canonical_text(&obs);
        assert_eq!(text, "decision use tokio runtime");
    }

    #[test]
    fn test_fake_embedder_deterministic() {
        let embedder = FakeEmbedder::new(8);
        let v1 = embedder.embed_query("hello world").unwrap();
        let v2 = embedder.embed_query("hello world").unwrap();
        assert_eq!(v1, v2, "same input must produce same vector");
    }

    #[test]
    fn test_fake_embedder_different_inputs_differ() {
        let embedder = FakeEmbedder::new(8);
        let v1 = embedder.embed_query("hello world").unwrap();
        let v2 = embedder.embed_query("goodbye moon").unwrap();
        assert_ne!(v1, v2, "different inputs should produce different vectors");
    }

    #[test]
    fn test_fake_embedder_unit_length() {
        let embedder = FakeEmbedder::new(384);
        let v = embedder.embed_query("test text").unwrap();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "vector should be unit length, got norm {norm}");
    }

    #[test]
    fn test_fake_embedder_dims() {
        let embedder = FakeEmbedder::new(384);
        assert_eq!(embedder.dims(), 384);
        assert_eq!(embedder.model_id(), "fake-model");
        assert_eq!(embedder.model_rev(), "v1");
        let v = embedder.embed_query("test").unwrap();
        assert_eq!(v.len(), 384);
    }

    #[test]
    fn test_embedding_blob_roundtrip() {
        let original = vec![1.0f32, -0.5, 0.333, std::f32::consts::PI];
        let blob = embedding_to_blob(&original);
        let restored = blob_to_embedding(&blob);
        assert_eq!(original, restored);
    }

    #[test]
    fn test_fake_embedder_embed_texts_batch() {
        let embedder = FakeEmbedder::new(8);
        let texts = vec!["hello", "world", "test"];
        let results = embedder.embed_texts(&texts).unwrap();
        assert_eq!(results.len(), 3);
        // Each should be deterministic
        assert_eq!(results[0], embedder.embed_query("hello").unwrap());
    }

    fn open_test_db() -> (rusqlite::Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let conn = crate::db::open(&db_path).expect("open DB");
        conn.execute("INSERT INTO sessions (id, started_at, agent) VALUES ('s1', 1000, 'test')", []).unwrap();
        (conn, dir)
    }

    fn insert_test_obs(conn: &rusqlite::Connection, content: &str) -> i64 {
        conn.execute(
            "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance)
             VALUES ('s1', 1000, 'insight', ?1, 0, 0, 'medium')",
            rusqlite::params![content],
        ).unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn test_store_load_embedding_roundtrip() {
        let (conn, _dir) = open_test_db();
        let obs_id = insert_test_obs(&conn, "test observation");

        let embedding = vec![0.1f32, 0.2, -0.3, 0.4];
        store_embedding(&conn, obs_id, "test-model", "v1", 4, &embedding).unwrap();

        let loaded = load_embeddings(&conn, &[obs_id], "test-model", "v1").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[&obs_id], embedding);
    }

    #[test]
    fn test_load_returns_empty_when_model_rev_differs() {
        let (conn, _dir) = open_test_db();
        let obs_id = insert_test_obs(&conn, "test observation");

        let embedding = vec![0.1f32, 0.2, -0.3, 0.4];
        store_embedding(&conn, obs_id, "test-model", "v1", 4, &embedding).unwrap();

        let loaded = load_embeddings(&conn, &[obs_id], "test-model", "v2").unwrap();
        assert!(loaded.is_empty(), "different model_rev should return empty");
    }

    #[test]
    fn test_load_returns_empty_when_model_id_differs() {
        let (conn, _dir) = open_test_db();
        let obs_id = insert_test_obs(&conn, "test observation");

        let embedding = vec![0.1f32, 0.2];
        store_embedding(&conn, obs_id, "model-a", "v1", 2, &embedding).unwrap();

        let loaded = load_embeddings(&conn, &[obs_id], "model-b", "v1").unwrap();
        assert!(loaded.is_empty(), "different model_id should return empty");
    }

    #[test]
    fn test_cascade_delete_removes_embedding() {
        let (conn, _dir) = open_test_db();
        let obs_id = insert_test_obs(&conn, "will be deleted");

        let embedding = vec![1.0f32, 2.0, 3.0];
        store_embedding(&conn, obs_id, "test-model", "v1", 3, &embedding).unwrap();

        // Verify embedding exists
        let loaded = load_embeddings(&conn, &[obs_id], "test-model", "v1").unwrap();
        assert_eq!(loaded.len(), 1);

        // Delete the observation
        conn.execute("DELETE FROM observations WHERE id = ?1", rusqlite::params![obs_id]).unwrap();

        // Embedding should be gone via CASCADE
        let loaded = load_embeddings(&conn, &[obs_id], "test-model", "v1").unwrap();
        assert!(loaded.is_empty(), "CASCADE should delete embedding when observation deleted");
    }

    #[test]
    fn test_get_unembedded_observation_ids() {
        let (conn, _dir) = open_test_db();
        let id1 = insert_test_obs(&conn, "first");
        let id2 = insert_test_obs(&conn, "second");
        let _id3 = {
            conn.execute(
                "INSERT INTO observations (session_id, created_at, kind, content, is_stale, auto_generated, importance)
                 VALUES ('s1', 1000, 'context_retrieval', 'noise', 0, 1, 'low')",
                [],
            ).unwrap();
            conn.last_insert_rowid()
        };

        // Embed only the first
        store_embedding(&conn, id1, "test-model", "v1", 4, &[1.0, 2.0, 3.0, 4.0]).unwrap();

        let unembedded = get_unembedded_observation_ids(&conn, "test-model", "v1").unwrap();
        assert_eq!(unembedded, vec![id2], "should skip already-embedded and noise kinds");
    }

    #[test]
    fn test_delete_all_embeddings() {
        let (conn, _dir) = open_test_db();
        let id1 = insert_test_obs(&conn, "first");
        let id2 = insert_test_obs(&conn, "second");
        store_embedding(&conn, id1, "m", "v1", 2, &[1.0, 2.0]).unwrap();
        store_embedding(&conn, id2, "m", "v1", 2, &[3.0, 4.0]).unwrap();

        let deleted = delete_all_embeddings(&conn).unwrap();
        assert_eq!(deleted, 2);

        let loaded = load_embeddings(&conn, &[id1, id2], "m", "v1").unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_store_embedding_upsert() {
        let (conn, _dir) = open_test_db();
        let obs_id = insert_test_obs(&conn, "test");

        store_embedding(&conn, obs_id, "m", "v1", 2, &[1.0, 2.0]).unwrap();
        store_embedding(&conn, obs_id, "m", "v1", 2, &[3.0, 4.0]).unwrap();

        let loaded = load_embeddings(&conn, &[obs_id], "m", "v1").unwrap();
        assert_eq!(loaded[&obs_id], vec![3.0f32, 4.0], "second store should replace first");
    }
}
