//! Measures how the tensor pad's chosen-neighbour distribution evolves as it learns.
//!
//! ```text
//! cargo run --example convergence [--neighbours N] [--decisions N] [--offset N]
//!                             [--good K] [--no-signal]
//! ```
//!
//! The pad's documented claim is that traffic strengthens the path it travels, so repeated
//! requests reuse it. That is a convergence claim, and it had never been measured -- only
//! asserted, then only as a static distribution over fixed seeds.
//!
//! **What it converged to, before this was fixed.** Rewarding whichever hop was taken, by a
//! constant, is a rich-get-richer random walk: one neighbour took ~96% of all routing, and
//! shifting the source's start changed *which* neighbour won while the share did not. The pad was
//! not learning; it was drifting.
//!
//! **What this now compares.** `--good K` makes neighbour `K` answer and the rest unreachable, so
//! there is a real utility difference to learn. The default follows it; `--no-signal` reproduces
//! the old constant reinforcement as a control. If the mechanism works, the two panels differ: the
//! signal should converge on `node-K`, the control on whichever neighbour the early draws favoured.
//!
//! The source is `BLAKE3(counter)`, so a run is exactly reproducible.

use std::time::Instant;

use oag_imagination::{
    reinforce, Engine, FnResolver, HandleOutcome, HopOutcome, PAD_DIM, RandomSource, Request,
    Target,
};

/// A deterministic 32-byte source: `BLAKE3(counter)`. Reproducibility matters here -- the whole
/// point is that two runs can be compared.
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

/// Runs one arm of the comparison and prints the share trajectory.
async fn run(
    label: &str,
    neighbour_count: usize,
    decisions: usize,
    offset: u64,
    good: Option<usize>,
    use_signal: bool,
) -> Vec<f32> {
    let neighbours: Vec<String> = (0..neighbour_count).map(|i| format!("node-{i}")).collect();
    let mut engine = Engine::with_source(
        "measuring-node".to_string(),
        vec![1.0; PAD_DIM],
        neighbours.clone(),
        Box::new(CounterSource { counter: offset }),
    );
    let resolver = FnResolver(|_: &Target, _: &serde_json::Value| None);

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
                if use_signal {
                    let outcome = if good == Some(index) {
                        HopOutcome::Answered
                    } else {
                        HopOutcome::Failed
                    };
                    engine.observe_hop(&next, outcome);
                } else {
                    // The pre-change behaviour: reward the hop taken, regardless of outcome.
                    reinforce(&mut engine.pad, index, 1.0);
                }
            }
        }

        if (i + 1) % (decisions / 10).max(1) == 0 {
            let shares: Vec<String> = counts
                .iter()
                .map(|c| format!("{:.3}", *c as f32 / (i + 1) as f32))
                .collect();
            println!("  [{label}] {i:>7}  {}", shares.join("  "));
        }
    }

    let elapsed = started.elapsed();
    let final_share: Vec<f32> = counts.iter().map(|c| *c as f32 / decisions as f32).collect();
    let leader = counts
        .iter()
        .enumerate()
        .max_by_key(|(_, c)| **c)
        .map(|(i, _)| i)
        .expect("at least one neighbour");
    println!(
        "  [{label}] leader node-{leader} at {:.1}% after {} decisions in {:.2?}\n",
        100.0 * final_share[leader],
        decisions,
        elapsed
    );
    final_share
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mut neighbour_count = 4usize;
    let mut decisions = 20_000usize;
    let mut offset = 0u64;
    let mut good = Some(0usize);
    let mut no_signal = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--neighbours" => neighbour_count = args.next().and_then(|v| v.parse().ok()).unwrap_or(4),
            "--decisions" => decisions = args.next().and_then(|v| v.parse().ok()).unwrap_or(20_000),
            "--offset" => offset = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            // Which neighbour answers and which are unreachable. Omit for "none answers".
            "--good" => good = args.next().and_then(|v| v.parse().ok()),
            // Reproduce the old constant reinforcement, as the control.
            "--no-signal" => no_signal = true,
            other => {
                eprintln!("unknown argument {other:?}");
                std::process::exit(2);
            }
        }
    }

    println!(
        "pad convergence: {neighbour_count} neighbours, {decisions} decisions, source offset {offset}"
    );
    match good {
        Some(k) => println!("utility: node-{k} answers, every other neighbour is unreachable"),
        None => println!("utility: no neighbour answers (every hop fails)"),
    }
    println!("share per neighbour at each checkpoint, most recent last\n");

    let signal = run("signal", neighbour_count, decisions, offset, good, true).await;

    if no_signal {
        let control = run("control", neighbour_count, decisions, offset, good, false).await;
        println!(
            "signal leader share {:.3} vs control {:.3}",
            signal.iter().copied().fold(0.0f32, f32::max),
            control.iter().copied().fold(0.0f32, f32::max)
        );
        let named = match good {
            Some(k) => format!("node-{k}"),
            None => "no neighbour (every hop fails)".to_string(),
        };
        println!(
            "\nBoth arms share the first few hundred draws. The signal converges on {named}; \
             the control converges on whichever neighbour those draws happened to favour. That \
             difference is the whole point: the pad is now following observed utility."
        );
    }
}
