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

//! **Recovery is bounded, not just observed.** `--switch-at` changes the answering neighbour
//! mid-run and reports when the new neighbour's weight overtakes the old one's. Before the
//! `PAD_FLOOR_FRAC` floor, a switch at decision 10,000 took 1,061 decisions to recover, because the
//! stored ratio was about 1e45. With the floor the ratio is capped at about
//! `neighbours / PAD_FLOOR_FRAC`, so recovery is bounded at `ln(ratio) / HOP_STEP` -- measured at 69
//! decisions for four neighbours. The floor is what makes the pad able to *unlearn* a path, not just
//! learn one.

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
    switch_at: Option<usize>,
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
    // Counts since the switch, to measure how fast the pad abandons a neighbour that stopped
    // answering -- the recovery horizon that `HOP_STEP` implies.
    let mut since_switch = vec![0usize; neighbours.len()];
    let mut recovery: Option<usize> = None;
    let started = Instant::now();

    for i in 0..decisions {
        let mut request = Request {
            id: format!("req-{i}"),
            target: serde_json::json!("target"),
            payload: serde_json::json!({}),
            contributions: vec![],
            answer: None,
        };
        let switched = switch_at.is_some_and(|at| i >= at);
        let good_now = match (good, switch_at, switched) {
            (Some(k), Some(_), true) => Some((k + 1) % neighbour_count),
            (Some(k), _, _) => Some(k),
            (None, _, _) => None,
        };
        if let HandleOutcome::RouteTo(next) = engine.handle(&mut request, &resolver).await {
            if let Some(index) = neighbours.iter().position(|n| n == &next) {
                counts[index] += 1;
                if switched {
                    since_switch[index] += 1;
                    if let (Some(at), Some(before), Some(after)) = (switch_at, good, good_now) {
                        // Compare the pad's own weights, not the selection counts: the counts carry
                        // the whole pre-switch history, so a neighbour that dominated for 10,000
                        // decisions stays "ahead" on counts long after the pad has flipped.
                        if recovery.is_none()
                            && engine.pad.get(after) > engine.pad.get(before)
                        {
                            recovery = Some(i - at + 1);
                        }
                    }
                }
                if use_signal {
                    let outcome = if good_now == Some(index) {
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
        "  [{label}] leader node-{leader} at {:.1}% after {} decisions in {:.2?}",
        100.0 * final_share[leader],
        decisions,
        elapsed
    );
    if let (Some(at), Some(recovery)) = (switch_at, recovery) {
        let after = good.map(|k| (k + 1) % neighbour_count);
        println!(
            "  [{label}] at decision {at} the answering neighbour changed; the new one led \
             {} decisions later",
            recovery
        );
        let name = |k: Option<usize>| match k {
            Some(k) => format!("node-{k}"),
            None => "none".to_string(),
        };
        println!(
            "  [{label}]   (answering neighbour: {} -> {})",
            name(good),
            name(after)
        );
    }
    println!();
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
    let mut switch_at: Option<usize> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--neighbours" => neighbour_count = args.next().and_then(|v| v.parse().ok()).unwrap_or(4),
            "--decisions" => decisions = args.next().and_then(|v| v.parse().ok()).unwrap_or(20_000),
            "--offset" => offset = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            // Which neighbour answers and which are unreachable. Omit for "none answers".
            "--good" => good = args.next().and_then(|v| v.parse().ok()),
            // Reproduce the old constant reinforcement, as the control.
            "--no-signal" => no_signal = true,
            // At this decision, the answering neighbour changes to the next one -- the topology
            // change the pad has to recover from.
            "--switch-at" => switch_at = args.next().and_then(|v| v.parse().ok()),
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
    println!("share per neighbour at each checkpoint, most recent last");
    if let Some(at) = switch_at {
        println!("at decision {at} the answering neighbour changes to the next one\n");
    } else {
        println!();
    }

    let signal = run("signal", neighbour_count, decisions, offset, good, true, switch_at).await;

    if no_signal {
        let control = run("control", neighbour_count, decisions, offset, good, false, switch_at).await;
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
