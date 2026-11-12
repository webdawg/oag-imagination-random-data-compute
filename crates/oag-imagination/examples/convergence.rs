//! Measures how the tensor pad's chosen-neighbour distribution evolves as it learns.
//!
//! ```text
//! cargo run --example convergence [-- neighbours N] [-- decisions N]
//! ```
//!
//! The pad's documented claim is that traffic strengthens the path it travels, so repeated
//! requests reuse it. That is a statement about convergence, and it had never been measured --
//! only asserted, and then only as a static distribution over fixed seeds. This runs the real
//! engine over a deterministic source and prints the chosen-neighbour share at checkpoints, so the
//! trajectory is a number rather than an impression.
//!
//! The resolver answers nothing, so every request is routed onward and the pad is exercised on its
//! own. The source is `BLAKE3(counter)`, so a given run is exactly reproducible.

use std::time::Instant;

use oag_imagination::{Engine, FnResolver, HandleOutcome, PAD_DIM, RandomSource, Request, Target};

/// A deterministic 32-byte source: `BLAKE3(counter)`. Reproducibility matters here -- the whole
/// point is that a run can be compared against another run.
#[derive(Debug, Default)]
struct CounterSource {
    counter: u64,
}

impl RandomSource for CounterSource {
    fn next(&mut self) -> [u8; 32] {
        let hash = blake3::hash(&self.counter.to_le_bytes());
        self.counter += 1;
        *hash.as_bytes()
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mut neighbour_count = 4usize;
    let mut decisions = 20_000usize;
    let mut offset = 0u64;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--neighbours" => neighbour_count = args.next().and_then(|v| v.parse().ok()).unwrap_or(4),
            "--decisions" => decisions = args.next().and_then(|v| v.parse().ok()).unwrap_or(20_000),
            // Shifts the source's starting point, which is the control: if a different offset
            // crowns a different neighbour, the winner is decided by the early draws rather than
            // by anything about the neighbours.
            "--offset" => offset = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
    }

    let neighbours: Vec<String> = (0..neighbour_count).map(|i| format!("node-{i}")).collect();
    let mut engine = Engine::with_source(
        "measuring-node".to_string(),
        vec![1.0; PAD_DIM],
        neighbours.clone(),
        Box::new(CounterSource { counter: offset }),
    );
    let resolver = FnResolver(|_: &Target, _: &serde_json::Value| None);

    println!(
        "pad convergence: {neighbour_count} neighbours, {decisions} routing decisions, source offset {offset}"
    );
    println!("each request starts a fresh chain, so the routing seed varies per request\n");
    println!("{:>10}  {:>12}  {}", "decision", "pad mean", "chosen share per neighbour");

    let mut counts = vec![0usize; neighbours.len()];
    let started = Instant::now();

    for i in 0..decisions {
        let mut request = Request {
            id: format!("req-{i}"),
            target: serde_json::json!("target"),
            payload: serde_json::json!({}),
            contributions: vec![],
            answer: None,
        };
        if let HandleOutcome::RouteTo(next) = engine.handle(&mut request, &resolver).await {
            if let Some(index) = neighbours.iter().position(|n| n == &next) {
                counts[index] += 1;
            }
        }

        if (i + 1) % (decisions / 20).max(1) == 0 {
            let pad_mean: f32 = engine.pad.iter().sum::<f32>() / engine.pad.len() as f32;
            let shares: Vec<String> = counts
                .iter()
                .map(|c| format!("{:.3}", *c as f32 / (i + 1) as f32))
                .collect();
            println!("{:>10}  {:>12.4}  {}", i + 1, pad_mean, shares.join("  "));
        }
    }

    let elapsed = started.elapsed();
    println!(
        "\n{} decisions in {:.2?} ({:.0} decisions/sec)",
        decisions,
        elapsed,
        decisions as f64 / elapsed.as_secs_f64().max(1e-9)
    );
    let leader = counts.iter().enumerate().max_by_key(|(_, c)| **c).map(|(i, _)| i);
    if let Some(leader) = leader {
        println!(
            "final leader: node-{leader} at {:.1}% of {} routable decisions",
            100.0 * counts[leader] as f64 / decisions as f64,
            decisions
        );
    }
    println!(
        "\nRead the last few rows: if the shares have stopped moving, the pad has converged -- and \
         the neighbour it converged on is whichever one the first decisions happened to reinforce, \
         because every hop is reinforced by the same constant regardless of how it turned out."
    );
}
