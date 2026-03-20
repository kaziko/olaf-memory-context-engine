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
            EmbedText, FastEmbedder, canonical_text, symbol_canonical_text,
            delete_all_embeddings, delete_all_symbol_embeddings,
            get_observations_by_ids, get_unembedded_observation_ids, store_embedding,
            get_unembedded_symbol_ids, store_symbol_embedding,
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
            let deleted_obs = delete_all_embeddings(&conn)?;
            let deleted_sym = delete_all_symbol_embeddings(&conn)?;
            println!("Cleared {} observation embeddings, {} symbol embeddings", deleted_obs, deleted_sym);
        }

        // ── Observation embedding phase ───────────────────────────────────────
        let unembedded = get_unembedded_observation_ids(&conn, &model_id, &model_rev)?;
        let obs_total = unembedded.len();

        if obs_total == 0 {
            println!("All eligible observations already have embeddings.");
        } else {
            println!("Embedding {} observations (batch_size={})", obs_total, batch_size);
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
                println!("[{}/{}] observations embedded", embedded_count, obs_total);
            }
            println!(
                "Embedded {} observations in {:.1}s",
                obs_total,
                start.elapsed().as_secs_f64(),
            );
        }

        // ── Symbol embedding phase ────────────────────────────────────────────
        let sym_start = std::time::Instant::now();
        let unembedded_syms = get_unembedded_symbol_ids(&conn, &model_id, &model_rev)?;
        let sym_total = unembedded_syms.len();

        if sym_total == 0 {
            println!("All eligible symbols already have embeddings.");
        } else {
            println!("Embedding {} symbols (batch_size={})", sym_total, batch_size);

            // Pre-fetch all symbol text fields with one prepared statement (hoisted outside
            // the embedding loop to comply with the no-prepare-in-loop rule).
            let embeddable: Vec<(i64, String)> = {
                let mut stmt = conn.prepare(
                    "SELECT id, fqn, name, COALESCE(signature,''), COALESCE(docstring,'') \
                     FROM symbols WHERE id = ?1",
                )?;
                unembedded_syms.iter().filter_map(|&id| {
                    stmt.query_row(rusqlite::params![id], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    }).ok()
                })
                .filter_map(|(id, fqn, name, sig, doc)| {
                    symbol_canonical_text(&fqn, &name, &sig, &doc).map(|t| (id, t))
                })
                .collect()
            };

            let mut sym_count = 0usize;
            for chunk in embeddable.chunks(batch_size) {
                let ids: Vec<i64> = chunk.iter().map(|(id, _)| *id).collect();
                let text_refs: Vec<&str> = chunk.iter().map(|(_, t)| t.as_str()).collect();

                let embeddings = embedder.embed_texts(&text_refs)
                    .map_err(|e| anyhow::anyhow!("symbol embedding failed: {e}"))?;
                for (sym_id, embedding) in ids.iter().zip(embeddings.iter()) {
                    store_symbol_embedding(&conn, *sym_id, &model_id, &model_rev, dims, embedding)?;
                }

                sym_count += chunk.len();
                println!("[{}/{}] symbols embedded", sym_count, embeddable.len());
            }

            let elapsed = sym_start.elapsed();
            println!(
                "Embedded {} symbols in {:.1}s ({:.0} sym/s)",
                embeddable.len(),
                elapsed.as_secs_f64(),
                embeddable.len() as f64 / elapsed.as_secs_f64().max(0.001),
            );
        }

        let total_elapsed = start.elapsed();
        println!("Done in {:.1}s total.", total_elapsed.as_secs_f64());

        Ok(())
    }
}
