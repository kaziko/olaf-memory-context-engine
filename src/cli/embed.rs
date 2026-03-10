pub(crate) fn run(rebuild: bool, batch_size: usize) -> anyhow::Result<()> {
    #[cfg(not(feature = "embeddings"))]
    {
        let _ = (rebuild, batch_size);
        eprintln!("Error: the `embeddings` feature is not enabled in this build.");
        eprintln!();
        eprintln!("To use `olaf embed`, rebuild with:");
        eprintln!("  cargo install olaf --features embeddings");
        eprintln!();
        eprintln!("Or build from source:");
        eprintln!("  cargo build --release --features embeddings");
        std::process::exit(1);
    }

    #[cfg(feature = "embeddings")]
    {
        use anyhow::Context;
        use olaf::memory::embedder::{
            EmbedText, FastEmbedder, canonical_text,
            delete_all_embeddings, get_observations_by_ids,
            get_unembedded_observation_ids, store_embedding,
        };

        let cwd = std::env::current_dir()?;
        let db_path = cwd.join(".olaf/index.db");

        if !db_path.exists() {
            anyhow::bail!("No Olaf database found. Run `olaf init` and `olaf index` first.");
        }

        let conn = olaf::db::open(&db_path).context("failed to open database")?;
        let model_cache_dir = cwd.join(".olaf").join("models");

        let start = std::time::Instant::now();

        // catch_unwind: ort panics (instead of Err) when libonnxruntime is missing with load-dynamic
        let embedder = std::panic::catch_unwind(|| FastEmbedder::new(&model_cache_dir))
            .map_err(|_| anyhow::anyhow!(
                "ONNX Runtime not found. Install libonnxruntime or set ORT_DYLIB_PATH.\n\
                 See: https://onnxruntime.ai/docs/install/"
            ))?
            .map_err(|e| anyhow::anyhow!("failed to load embedding model: {e}"))?;

        let model_id = embedder.model_id().to_string();
        let model_rev = embedder.model_rev().to_string();
        let dims = embedder.dims() as i32;

        if rebuild {
            let deleted = delete_all_embeddings(&conn)?;
            println!("Cleared {} existing embeddings", deleted);
        }

        let unembedded = get_unembedded_observation_ids(&conn, &model_id, &model_rev)?;
        let total = unembedded.len();

        if total == 0 {
            println!("All eligible observations already have embeddings. Nothing to do.");
            return Ok(());
        }

        println!("Embedding {} observations (batch_size={})", total, batch_size);

        let mut embedded_count = 0usize;
        for chunk in unembedded.chunks(batch_size) {
            let observations = get_observations_by_ids(&conn, chunk)?;
            let texts: Vec<String> = observations.iter().map(canonical_text).collect();
            let text_refs: Vec<&str> = texts.iter().map(|s: &String| s.as_str()).collect();

            let embeddings = embedder.embed_texts(&text_refs)
                .map_err(|e| anyhow::anyhow!("embedding failed: {e}"))?;

            for (obs, embedding) in observations.iter().zip(embeddings.iter()) {
                store_embedding(&conn, obs.id, &model_id, &model_rev, dims, embedding)?;
            }

            embedded_count += chunk.len();
            println!("[{}/{}] observations embedded", embedded_count, total);
        }

        let duration = start.elapsed();
        println!(
            "Done. Embedded {} observations in {:.1}s ({:.0} obs/s)",
            embedded_count,
            duration.as_secs_f64(),
            embedded_count as f64 / duration.as_secs_f64().max(0.001),
        );

        Ok(())
    }
}
