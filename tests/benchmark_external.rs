//! External repository benchmark — measures Olaf performance on a real-world repo.
//!
//! Run with: `cargo test --release benchmark_external -- --ignored`
//! Optional: `BENCHMARK_REPO_DIR=/path/to/local/clone` to skip network clone.

use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::process::{Command, Stdio};
use std::time::Instant;

// ─── TOML Configuration Structs ──────────────────────────────────────────────

#[derive(Deserialize)]
struct BenchmarkConfig {
    repo: RepoConfig,
    thresholds: Thresholds,
    #[serde(default)]
    query: Vec<QueryConfig>,
}

#[derive(Deserialize)]
struct RepoConfig {
    url: String,
    commit_sha: String,
    min_indexable_files: usize,
}

#[derive(Deserialize)]
struct QueryConfig {
    name: String,
    intent: String,
    tag: String,
    #[serde(default)]
    expected_pivots: Vec<String>,
    symbol_fqn: Option<String>,
    file_hints: Option<Vec<String>>,
    #[serde(default)]
    baseline_steps: Vec<BaselineStep>,
}

#[derive(Deserialize)]
struct BaselineStep {
    action: String,
    tokens: Option<usize>,
    estimated_output_tokens: Option<usize>,
}

#[derive(Deserialize)]
struct Thresholds {
    budget: usize,
    min_mean_savings_a_pct: f64,
    max_p95_warm_latency_ms: f64,
    min_recall_hit_rate: f64,
}

// ─── Result Structs ──────────────────────────────────────────────────────────

#[derive(Serialize)]
struct BenchmarkResults {
    repo: RepoResults,
    indexing: IndexingResults,
    cold_first_query_ms: f64,
    queries: Vec<QueryResult>,
    aggregate: AggregateResults,
    environment: EnvironmentInfo,
    threshold_warnings: Vec<String>,
}

#[derive(Serialize)]
struct RepoResults {
    url: String,
    commit_sha: String,
    name: String,
}

#[derive(Serialize)]
struct IndexingResults {
    wall_clock_ms: f64,
    file_count: usize,
    symbol_count: usize,
    edge_count: usize,
}

#[derive(Serialize)]
struct QueryResult {
    name: String,
    tag: String,
    measurements: Vec<BudgetMeasurement>,
    baseline_a_tokens: usize,
    baseline_b_tokens: Option<usize>,
    savings_b_pct: Option<f64>,
    recall_hit_rate: Option<f64>,
}

#[derive(Serialize)]
struct BudgetMeasurement {
    budget: usize,
    latency_ms: f64,
    olaf_tokens: usize,
    savings_a_pct: f64,
}

#[derive(Serialize)]
struct AggregateResults {
    per_budget: Vec<BudgetAggregate>,
    warm_latency_p50_ms: f64,
    warm_latency_p95_ms: f64,
    warm_latency_max_ms: f64,
}

#[derive(Serialize)]
struct BudgetAggregate {
    budget: usize,
    mean_savings_a_pct: f64,
    median_savings_a_pct: f64,
}

#[derive(Serialize)]
struct EnvironmentInfo {
    os: String,
    cpu: String,
    ram_bytes: u64,
    build_profile: String,
    olaf_commit_sha: String,
}

// ─── MCP Communication ──────────────────────────────────────────────────────

struct McpClient {
    stdin: BufWriter<std::process::ChildStdin>,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: u64,
}

impl McpClient {
    fn spawn(repo_dir: &std::path::Path) -> (Self, std::process::Child) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_olaf"))
            .arg("serve")
            .current_dir(repo_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit()) // surface server errors/panics for debugging
            .spawn()
            .expect("failed to spawn olaf serve");

        let stdin = BufWriter::new(child.stdin.take().unwrap());
        let stdout = BufReader::new(child.stdout.take().unwrap());

        (McpClient { stdin, stdout, next_id: 1 }, child)
    }

    fn send_request(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        let id = self.next_id;
        self.next_id += 1;

        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        writeln!(self.stdin, "{}", serde_json::to_string(&req).unwrap()).unwrap();
        self.stdin.flush().unwrap();

        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("failed to read response");
        serde_json::from_str(&line)
            .unwrap_or_else(|e| panic!("non-JSON response: {e}\nLine: {line:?}"))
    }

    fn send_request_timed(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> (serde_json::Value, f64) {
        let start = Instant::now();
        let response = self.send_request(method, params);
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        (response, elapsed_ms)
    }

    fn initialize(&mut self) {
        let resp = self.send_request(
            "initialize",
            serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "benchmark-external", "version": "1.0" }
            }),
        );
        assert!(
            resp.get("result").is_some(),
            "initialize failed: {resp}"
        );
    }

    fn get_brief(
        &mut self,
        intent: &str,
        token_budget: usize,
        symbol_fqn: Option<&str>,
        file_hints: Option<&[String]>,
    ) -> (serde_json::Value, f64) {
        let mut args = serde_json::json!({
            "intent": intent,
            "token_budget": token_budget,
        });

        if let Some(fqn) = symbol_fqn {
            args["symbol_fqn"] = serde_json::Value::String(fqn.to_string());
        }
        if let Some(hints) = file_hints {
            args["file_hints"] = serde_json::json!(hints);
        }

        self.send_request_timed(
            "tools/call",
            serde_json::json!({
                "name": "get_brief",
                "arguments": args,
            }),
        )
    }

    fn close(self, mut child: std::process::Child) {
        drop(self.stdin);
        let status = child.wait().expect("failed to wait for olaf serve");
        assert!(status.success(), "olaf serve exited non-zero: {status:?}");
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn extract_response_text<'a>(response: &'a serde_json::Value, query_name: &str) -> &'a str {
    if let Some(err) = response.get("error") {
        panic!("MCP error for query '{query_name}': {err}");
    }
    let result = response
        .get("result")
        .unwrap_or_else(|| panic!("MCP response missing 'result' for query '{query_name}': {response}"));
    if result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false) {
        panic!("tool returned isError=true for query '{query_name}': {result}");
    }
    result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("MCP response missing content text for query '{query_name}': {result}"))
}

fn estimate_tokens(text: &str) -> usize {
    text.len().div_ceil(4)
}

fn parse_index_stderr(stderr: &str) -> (usize, usize, usize) {
    // Format: "indexed {N} files, {N} symbols, {N} edges"
    let mut files = None;
    let mut symbols = None;
    let mut edges = None;

    for line in stderr.lines() {
        if line.contains("files") && line.contains("symbols") && line.contains("edges") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            for (i, &word) in parts.iter().enumerate() {
                if i == 0 {
                    continue;
                }
                if word.starts_with("files") {
                    files = Some(parts[i - 1].parse::<usize>().unwrap_or(0));
                } else if word.starts_with("symbols") {
                    symbols = Some(parts[i - 1].trim_end_matches(',').parse::<usize>().unwrap_or(0));
                } else if word.starts_with("edges") {
                    edges = Some(parts[i - 1].trim_end_matches(',').parse::<usize>().unwrap_or(0));
                }
            }
        }
    }

    let files = files.unwrap_or_else(|| panic!(
        "failed to parse file count from olaf index stderr — format may have changed.\nStderr: {stderr}"
    ));
    let symbols = symbols.unwrap_or_else(|| panic!(
        "failed to parse symbol count from olaf index stderr — format may have changed.\nStderr: {stderr}"
    ));
    let edges = edges.unwrap_or_else(|| panic!(
        "failed to parse edge count from olaf index stderr — format may have changed.\nStderr: {stderr}"
    ));

    (files, symbols, edges)
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn median(values: &[f64]) -> f64 {
    percentile(values, 50.0)
}

fn compute_baseline_a_tokens(steps: &[BaselineStep]) -> usize {
    steps
        .iter()
        .map(|s| match s.action.as_str() {
            "read" => s.tokens.unwrap_or(0),
            "grep" => s.estimated_output_tokens.unwrap_or(0),
            _ => 0,
        })
        .sum()
}

fn check_recall(response_text: &str, expected_pivots: &[String]) -> (usize, usize) {
    let hits = expected_pivots
        .iter()
        .filter(|pivot| response_text.contains(pivot.as_str()))
        .count();
    (hits, expected_pivots.len())
}

fn compute_baseline_b(response_text: &str, repo_dir: &std::path::Path) -> usize {
    let mut seen = std::collections::HashSet::new();
    let mut total_tokens = 0usize;
    for line in response_text.lines() {
        if let Some(file_path) = line.strip_prefix("File: ") {
            let file_path = file_path.trim().trim_matches('`');
            if !seen.insert(file_path.to_string()) {
                continue; // already counted this file
            }
            let full_path = repo_dir.join(file_path);
            if let Ok(contents) = std::fs::read_to_string(&full_path) {
                total_tokens += contents.len().div_ceil(4);
            }
        }
    }
    total_tokens
}

fn collect_environment_info(olaf_commit_sha: String) -> EnvironmentInfo {
    let os = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);

    // CPU: sysctl on macOS, /proc/cpuinfo on Linux
    let cpu = if cfg!(target_os = "macos") {
        Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".to_string())
    } else {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .and_then(|l| l.split(':').nth(1))
                    .map(|v| v.trim().to_string())
            })
            .unwrap_or_else(|| "unknown".to_string())
    };

    // RAM: sysctl on macOS, /proc/meminfo on Linux
    let ram_bytes = if cfg!(target_os = "macos") {
        Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("MemTotal"))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
            .unwrap_or(0)
    };

    EnvironmentInfo {
        os,
        cpu,
        ram_bytes,
        build_profile: "release".to_string(),
        olaf_commit_sha,
    }
}

// ─── Main Benchmark ─────────────────────────────────────────────────────────

#[test]
#[ignore]
fn benchmark_external_repo() {
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/benchmark_external.toml");
    let config_text = std::fs::read_to_string(&fixture_path)
        .expect("failed to read benchmark_external.toml");
    let config: BenchmarkConfig =
        toml::from_str(&config_text).expect("failed to parse benchmark_external.toml");

    assert!(
        !config.query.is_empty(),
        "benchmark_external.toml contains no queries"
    );

    // ── Acquire repo ──
    let (_tmpdir, repo_dir) = acquire_repo(&config.repo);
    eprintln!("Repo directory: {}", repo_dir.display());

    // ── Index ──
    let indexing = run_index(&repo_dir);
    eprintln!(
        "Indexed: {} files, {} symbols, {} edges in {:.1}ms",
        indexing.file_count, indexing.symbol_count, indexing.edge_count, indexing.wall_clock_ms,
    );
    assert!(
        indexing.file_count >= config.repo.min_indexable_files,
        "repo has {} indexable files, need >= {}",
        indexing.file_count,
        config.repo.min_indexable_files,
    );

    // ── Spawn single olaf serve process ──
    let (mut client, child) = McpClient::spawn(&repo_dir);
    client.initialize();

    // ── Cold query (warm-up, triggers run_incremental) ──
    let first_query = &config.query[0];
    let (_cold_resp, cold_first_query_ms) = client.get_brief(
        &first_query.intent,
        config.thresholds.budget,
        first_query.symbol_fqn.as_deref(),
        first_query.file_hints.as_deref(),
    );
    eprintln!("Cold first query: {cold_first_query_ms:.1}ms");

    // ── Run all queries at all budgets ──
    let budgets = [2000usize, 4000, 8000];
    let mut query_results: Vec<QueryResult> = Vec::new();

    for qcfg in &config.query {
        let baseline_a = compute_baseline_a_tokens(&qcfg.baseline_steps);
        let mut measurements = Vec::new();
        let mut budget_4000_text = String::new();

        for &budget in &budgets {
            let (resp, latency_ms) = client.get_brief(
                &qcfg.intent,
                budget,
                qcfg.symbol_fqn.as_deref(),
                qcfg.file_hints.as_deref(),
            );

            let text = extract_response_text(&resp, &qcfg.name).to_string();
            let olaf_tokens = estimate_tokens(&text);

            let savings_a_pct = if baseline_a > 0 {
                (1.0 - (olaf_tokens as f64 / baseline_a as f64)) * 100.0
            } else {
                0.0
            };

            if budget == 4000 {
                budget_4000_text = text;
            }

            measurements.push(BudgetMeasurement {
                budget,
                latency_ms,
                olaf_tokens,
                savings_a_pct,
            });
        }

        // Baseline B at budget=4000 only
        let (baseline_b_tokens, savings_b_pct) = if !budget_4000_text.is_empty() {
            let b = compute_baseline_b(&budget_4000_text, &repo_dir);
            if b > 0 {
                let olaf_4k = measurements
                    .iter()
                    .find(|m| m.budget == 4000)
                    .map(|m| m.olaf_tokens)
                    .unwrap_or(0);
                let savings = (1.0 - (olaf_4k as f64 / b as f64)) * 100.0;
                (Some(b), Some(savings))
            } else {
                (Some(0), None)
            }
        } else {
            (None, None)
        };

        // Recall check at budget=4000
        let recall = if !qcfg.expected_pivots.is_empty() {
            let (hits, total) = check_recall(&budget_4000_text, &qcfg.expected_pivots);
            Some(hits as f64 / total as f64)
        } else {
            None
        };

        let m4k = measurements.iter().find(|m| m.budget == 4000);
        eprintln!(
            "  {}: baseline_a={} olaf_4k={} savings_a={:.1}% latency={:.1}ms recall={:.2}",
            qcfg.name,
            baseline_a,
            m4k.map(|m| m.olaf_tokens).unwrap_or(0),
            m4k.map(|m| m.savings_a_pct).unwrap_or(0.0),
            m4k.map(|m| m.latency_ms).unwrap_or(0.0),
            recall.unwrap_or(-1.0),
        );

        query_results.push(QueryResult {
            name: qcfg.name.clone(),
            tag: qcfg.tag.clone(),
            measurements,
            baseline_a_tokens: baseline_a,
            baseline_b_tokens,
            savings_b_pct,
            recall_hit_rate: recall,
        });
    }

    // ── Shut down serve process ──
    client.close(child);

    // ── Aggregate ──
    let aggregate = compute_aggregates(&query_results, &budgets);

    // ── Threshold warnings ──
    let mut warnings = Vec::new();
    let budget_4k_agg = aggregate
        .per_budget
        .iter()
        .find(|a| a.budget == config.thresholds.budget);
    if let Some(agg) = budget_4k_agg
        && agg.mean_savings_a_pct < config.thresholds.min_mean_savings_a_pct
    {
        warnings.push(format!(
            "WARN: mean savings {:.1}% < threshold {:.1}%",
            agg.mean_savings_a_pct, config.thresholds.min_mean_savings_a_pct,
        ));
    }
    if aggregate.warm_latency_p95_ms > config.thresholds.max_p95_warm_latency_ms {
        warnings.push(format!(
            "WARN: p95 latency {:.1}ms > threshold {:.1}ms",
            aggregate.warm_latency_p95_ms, config.thresholds.max_p95_warm_latency_ms,
        ));
    }
    let recall_rates: Vec<f64> = query_results
        .iter()
        .filter_map(|q| q.recall_hit_rate)
        .collect();
    if !recall_rates.is_empty() {
        let mean_recall = recall_rates.iter().sum::<f64>() / recall_rates.len() as f64;
        if mean_recall < config.thresholds.min_recall_hit_rate {
            warnings.push(format!(
                "WARN: mean recall {:.2} < threshold {:.2}",
                mean_recall, config.thresholds.min_recall_hit_rate,
            ));
        }
    }

    for w in &warnings {
        eprintln!("{w}");
    }

    // ── Olaf commit SHA ──
    let olaf_sha = String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .output()
            .map(|o| o.stdout)
            .unwrap_or_default(),
    )
    .trim()
    .to_string();

    // ── Write results ──
    let results = BenchmarkResults {
        repo: RepoResults {
            url: config.repo.url.clone(),
            commit_sha: config.repo.commit_sha.clone(),
            name: config.repo.url
                .trim_end_matches(".git")
                .rsplit('/')
                .next()
                .unwrap_or("unknown")
                .to_string(),
        },
        indexing,
        cold_first_query_ms,
        queries: query_results,
        aggregate,
        environment: collect_environment_info(olaf_sha),
        threshold_warnings: warnings,
    };

    let results_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/benchmark_results.json");
    if let Some(parent) = results_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let json = serde_json::to_string_pretty(&results).unwrap();
    std::fs::write(&results_path, &json).expect("failed to write benchmark_results.json");
    eprintln!("\nResults written to: {}", results_path.display());

    // Print summary
    eprintln!("\n═══ BENCHMARK SUMMARY ═══");
    eprintln!(
        "Repo: {} ({} files, {} symbols, {} edges)",
        results.repo.name,
        results.indexing.file_count,
        results.indexing.symbol_count,
        results.indexing.edge_count,
    );
    eprintln!("Index time: {:.1}ms", results.indexing.wall_clock_ms);
    eprintln!("Cold first query: {:.1}ms", results.cold_first_query_ms);
    for agg in &results.aggregate.per_budget {
        eprintln!(
            "Budget {}: mean savings {:.1}%, median {:.1}%",
            agg.budget, agg.mean_savings_a_pct, agg.median_savings_a_pct,
        );
    }
    eprintln!(
        "Warm latency (budget=4000): p50={:.1}ms p95={:.1}ms max={:.1}ms",
        results.aggregate.warm_latency_p50_ms,
        results.aggregate.warm_latency_p95_ms,
        results.aggregate.warm_latency_max_ms,
    );
}

// ─── Repo Acquisition ────────────────────────────────────────────────────────

fn acquire_repo(cfg: &RepoConfig) -> (Option<tempfile::TempDir>, std::path::PathBuf) {
    if let Ok(dir) = std::env::var("BENCHMARK_REPO_DIR") {
        let path = std::path::PathBuf::from(dir);
        assert!(path.exists(), "BENCHMARK_REPO_DIR does not exist: {}", path.display());
        verify_commit_sha(&path, &cfg.commit_sha);
        return (None, path);
    }

    let tmpdir = tempfile::tempdir().expect("failed to create tempdir");
    let repo_path = tmpdir.path().join("repo");
    eprintln!("Cloning {} at commit {} ...", cfg.url, cfg.commit_sha);

    // Init bare repo, fetch only the pinned commit at depth 1, then checkout.
    // This guarantees the benchmark runs against exactly the configured SHA.
    let run = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo_path)
            .output()
            .unwrap_or_else(|e| panic!("git {} failed to run: {e}", args[0]));
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args[0],
            String::from_utf8_lossy(&output.stderr),
        );
    };
    std::fs::create_dir_all(&repo_path).expect("failed to create repo dir");
    run(&["init"]);
    run(&["remote", "add", "origin", &cfg.url]);
    run(&["fetch", "--depth", "1", "origin", &cfg.commit_sha]);
    run(&["checkout", "FETCH_HEAD"]);

    verify_commit_sha(&repo_path, &cfg.commit_sha);
    (Some(tmpdir), repo_path)
}

fn verify_commit_sha(repo_dir: &std::path::Path, expected: &str) {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo_dir)
        .output()
        .expect("failed to get repo HEAD");
    let actual = String::from_utf8_lossy(&output.stdout).trim().to_string();
    assert!(
        actual.starts_with(expected) || expected.starts_with(&actual),
        "repo HEAD is {actual}, expected {expected} — refusing to benchmark wrong snapshot",
    );
}

fn run_index(repo_dir: &std::path::Path) -> IndexingResults {
    std::fs::create_dir_all(repo_dir.join(".olaf")).ok();

    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_olaf"))
        .arg("index")
        .current_dir(repo_dir)
        .output()
        .expect("failed to run olaf index");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    assert!(
        output.status.success(),
        "olaf index failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let (files, symbols, edges) = parse_index_stderr(&stderr);

    IndexingResults {
        wall_clock_ms: elapsed_ms,
        file_count: files,
        symbol_count: symbols,
        edge_count: edges,
    }
}

fn compute_aggregates(results: &[QueryResult], budgets: &[usize]) -> AggregateResults {
    let per_budget: Vec<BudgetAggregate> = budgets
        .iter()
        .map(|&budget| {
            let savings: Vec<f64> = results
                .iter()
                .filter_map(|q| {
                    q.measurements
                        .iter()
                        .find(|m| m.budget == budget)
                        .map(|m| m.savings_a_pct)
                })
                .collect();

            let mean = if savings.is_empty() {
                0.0
            } else {
                savings.iter().sum::<f64>() / savings.len() as f64
            };

            let mut sorted = savings.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let med = median(&sorted);

            BudgetAggregate {
                budget,
                mean_savings_a_pct: mean,
                median_savings_a_pct: med,
            }
        })
        .collect();

    // Warm latency from budget=4000 measurements (excluding first query which was cold)
    let mut latencies: Vec<f64> = results
        .iter()
        .filter_map(|q| {
            q.measurements
                .iter()
                .find(|m| m.budget == 4000)
                .map(|m| m.latency_ms)
        })
        .collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    AggregateResults {
        per_budget,
        warm_latency_p50_ms: percentile(&latencies, 50.0),
        warm_latency_p95_ms: percentile(&latencies, 95.0),
        warm_latency_max_ms: latencies.last().copied().unwrap_or(0.0),
    }
}
