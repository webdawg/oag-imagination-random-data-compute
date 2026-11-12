//! The imagination engine: a routing engine over `oag`'s tensor pads.
//!
//! A request enters a node. The node uses its tensor pad to pick the next hop. Each hop adds a
//! contribution -- a random message or real data -- chained to the previous one. The request
//! travels through 6, 10, or 50 nodes, whoever knows. The final response carries the whole
//! metadata chain back.
//!
//! The pad is a node's unique representation of the picture of all the nodes around it. More
//! combined traffic through a tensor path makes the pad along that path stronger, so repeated
//! requests for the same thing tend to reuse the same path. The pad is *not* over-designed here:
//! the engine designs around it, and the traffic shapes it. See
//! `spec/02-tensor-pad-routing.md`.
//!
//! **What this explicitly is not.** Not a new network protocol -- the engine is transport
//! agnostic; `socket` carries it over TCP today, and Reticulum / IPFS / tunnels are the same
//! engine over a different carrier. Not a trustless beacon: the pad is a representation, not a
//! guarantee. Not a configured topology: the neighbors come from the layer below (`oag`).

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::datapad::Shard;
use crate::source::{RandomSource, SoftwareRandom};
use crate::target::{Resolver, Target};

pub const CONTRIBUTION_DOMAIN: &[u8] = b"OAG-IMAGINATION:CONTRIBUTION:v1:";
pub const ROUTE_DOMAIN: &[u8] = b"OAG-IMAGINATION:ROUTE:v1:";
pub const PAD_DIM: usize = 32;

/// A request can be anything: a URL, a query, a payload, an API spend, a store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    /// What the client is looking for -- a CID, a URL, a query, a key.
    pub target: serde_json::Value,
    /// The request payload.
    pub payload: serde_json::Value,
    /// The metadata chain gathered along the way.
    #[serde(default)]
    pub contributions: Vec<Contribution>,
    /// The answer, filled in by the node that can serve the target.
    #[serde(default)]
    pub answer: Option<serde_json::Value>,
}

/// One node's contribution to a request: a random message or real data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contribution {
    pub node_id: String,
    /// Hex-encoded 32-byte value.
    pub value: String,
    /// Hex-encoded previous chain value; all zeros for the first contribution.
    pub previous: String,
}

impl Contribution {
    /// The chain value of this contribution.
    pub fn chain_value(&self) -> Result<[u8; 32], EngineError> {
        let value = decode_hex32(&self.value)?;
        let previous = decode_hex32(&self.previous)?;
        Ok(chain_contribution(&previous, &value))
    }
}

/// Default cap on how many answered requests the dream log keeps.
/// Cap on how many hops a single request may travel.
///
/// The engine has no visited-set: the pad picks the next hop by weight, and a cycle between two
/// nodes is a possible outcome rather than an impossible one. Without a hop budget such a request
/// would accumulate one contribution per hop and be re-sent in full at each, amplifying bandwidth
/// and memory until the carrier's `MAX_MESSAGE_BYTES` happened to reject it -- a bound, but an
/// accidental one measured in megabytes of waste per cycle.
///
/// 64 is well above the design intent in `spec/02` ("travels through 6, 10, or 50 nodes") and
/// matches the spirit of the base project's `relay_hops_remaining`, which bounds the same shape of
/// amplification on the relay path.
pub const MAX_ROUTE_HOPS: usize = 64;

pub const DEFAULT_DREAM_LOG_MAX_ENTRIES: usize = 1_000;

/// Default cap on the dream log's total *serialized* size, across all entries.
///
/// A count cap alone is not enough here, and this is the number that matters: a single request can
/// be up to the carrier's `MAX_MESSAGE_BYTES` (16 MiB over TCP), so a count-only bound would still
/// permit the log to grow to tens of gigabytes. Both caps are enforced, and the oldest entries are
/// evicted until both hold.
pub const DEFAULT_DREAM_LOG_MAX_BYTES: usize = 8 * 1024 * 1024;

/// The serialized size of one dream, cached at record time.
///
/// Recomputing it on every scrape would mean re-serializing every stored request, which is the
/// wrong cost to put on a read path that a responder may poll often.
#[derive(Debug)]
struct DreamEntry {
    request: Request,
    size: usize,
}

/// The saved dreams.
///
/// **Bounded, deliberately.** An unbounded `Vec<Request>` is a memory-exhaustion vector reachable
/// by any caller that can make this node *answer* a request: `handle` records every answered request
/// as a clone, and the request carries caller-controlled JSON (`target`, `payload`) plus the whole
/// contribution chain gathered along the route. Before this bound existed, a peer could grow the
/// node's memory without limit -- and because a single request may be megabytes, a count cap on its
/// own would not have been enough. Both a count cap and a serialized-byte cap are enforced, and the
/// oldest entry is evicted when either is exceeded.
///
/// **In-memory only.** A restart forgets the log. That is the same posture as the base project's
/// tensor pad being re-learned, and it is recorded rather than hidden: "recording generated
/// scenarios" is satisfied for the running process, not across restarts. Persisting it would need
/// its own storage decision (where, how long, who may read it), which is not made here.
#[derive(Debug)]
pub struct DreamLog {
    dreams: VecDeque<DreamEntry>,
    max_entries: usize,
    max_bytes: usize,
    current_bytes: usize,
}

impl Default for DreamLog {
    fn default() -> Self {
        Self::new()
    }
}

impl DreamLog {
    pub fn new() -> Self {
        Self::with_bounds(DEFAULT_DREAM_LOG_MAX_ENTRIES, DEFAULT_DREAM_LOG_MAX_BYTES)
    }

    /// A log with explicit bounds, for a deployment that wants a different trade-off or a test
    /// that wants to exercise eviction cheaply.
    pub fn with_bounds(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            dreams: VecDeque::new(),
            max_entries,
            max_bytes,
            current_bytes: 0,
        }
    }

    pub fn record(&mut self, dream: Request) {
        let size = serde_json::to_vec(&dream).map(|v| v.len()).unwrap_or(0);
        self.current_bytes += size;
        self.dreams.push_back(DreamEntry { request: dream, size });
        self.evict();
    }

    /// Drops the oldest entries until both caps hold.
    fn evict(&mut self) {
        while self.current_bytes > self.max_bytes || self.dreams.len() > self.max_entries {
            let Some(oldest) = self.dreams.pop_front() else { break };
            self.current_bytes -= oldest.size;
        }
    }

    /// Every stored dream, oldest first.
    pub fn dreams(&self) -> impl Iterator<Item = &Request> {
        self.dreams.iter().map(|entry| &entry.request)
    }

    /// Total serialized bytes currently retained -- the other half of the bound.
    pub fn bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn len(&self) -> usize {
        self.dreams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dreams.is_empty()
    }

    /// Every dream whose request target matches `target` -- a responder (or an
    /// operator) asking "what has this node been asked for".
    pub fn for_target(&self, target: &serde_json::Value) -> Vec<&Request> {
        self.dreams
            .iter()
            .map(|entry| &entry.request)
            .filter(|d| d.target == *target)
            .collect()
    }

    /// Every dream whose chain includes `node_id` -- "what has this node
    /// contributed to".
    pub fn for_node(&self, node_id: &str) -> Vec<&Request> {
        self.dreams
            .iter()
            .map(|entry| &entry.request)
            .filter(|d| d.contributions.iter().any(|c| c.node_id == node_id))
            .collect()
    }

    /// The most recent dream, if any.
    pub fn last(&self) -> Option<&Request> {
        self.dreams.back().map(|entry| &entry.request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("hex decoding failed: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("expected 32 bytes, got {0}")]
    BadLength(usize),
    #[error("serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

fn decode_hex32(s: &str) -> Result<[u8; 32], EngineError> {
    let bytes = hex::decode(s)?;
    if bytes.len() != 32 {
        return Err(EngineError::BadLength(bytes.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Combines `previous` and a node's `value` into the next chain value.
pub fn chain_contribution(previous: &[u8; 32], value: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONTRIBUTION_DOMAIN);
    hasher.update(previous);
    hasher.update(value);
    *hasher.finalize().as_bytes()
}

/// The all-zero genesis previous value for the first contribution.
pub fn genesis_previous() -> [u8; 32] {
    [0u8; 32]
}

/// Picks the next hop using the pad. The pad's values are weights over the neighbors; the node
/// picks weighted by the pad, so traffic follows the strongest pads. A zero pad does not make a
/// neighbor unreachable -- every neighbor keeps a small floor.
pub fn route(pad: &[f32], neighbors: &[String], seed: &[u8; 32]) -> Option<String> {
    if neighbors.is_empty() {
        return None;
    }
    let weights: Vec<f64> = (0..neighbors.len())
        .map(|i| pad.get(i).copied().unwrap_or(0.0).max(0.0) as f64 + 1e-6)
        .collect();
    let total: f64 = weights.iter().sum();

    let mut hasher = blake3::Hasher::new();
    hasher.update(ROUTE_DOMAIN);
    hasher.update(seed);
    let hash = hasher.finalize();
    let h = hash.as_bytes();
    let point = (u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as f64 / u32::MAX as f64) * total;

    let mut acc = 0.0;
    for (i, w) in weights.iter().enumerate() {
        acc += w;
        if point < acc {
            return Some(neighbors[i].clone());
        }
    }
    neighbors.last().cloned()
}

/// Combines two pads into one -- to serve data spanning more than one node's view.
pub fn combine_pads(a: &[f32], b: &[f32]) -> Vec<f32> {
    a.iter().zip(b.iter()).map(|(x, y)| x + y).collect()
}

/// How a forwarded hop turned out, as the carrier that forwarded it observes.
///
/// The pad is worth something only if reinforcement is driven by this. Rewarding whichever hop was
/// taken, by a constant, is a rich-get-richer random walk that locks onto an arbitrary neighbour --
/// measured in `examples/convergence.rs`, where shifting the source's start changes *which*
/// neighbour wins while the ~96% share does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopOutcome {
    /// The chain was answered past this hop. The hop carried the request somewhere useful.
    Answered,
    /// The hop forwarded onward and nobody further down the chain answered. It did its part.
    Forwarded,
    /// The hop could not be reached: connect, write, or read failed.
    Failed,
}

/// How much a hop being answered multiplies that neighbour's weight.
pub const HOP_ANSWERED_GAIN: f32 = 1.05;

/// How much an unreachable hop multiplies that neighbour's weight. Less than one, so a neighbour
/// that keeps failing loses share to the ones that do not -- the weaken/dissolve half, driven by
/// observation rather than by elapsed time.
pub const HOP_FAILED_PENALTY: f32 = 0.9;

/// Strengthens the pad for the neighbor at `index` -- more traffic through a path makes the pad
/// along that path stronger.
///
/// An out-of-range index is ignored rather than panicking: the pad and the neighbour list are
/// built by different layers, and a mismatch must degrade to "this path does not learn" rather
/// than take the process down. [`Engine::with_source`] seeds the pad to one slot per neighbour so
/// the case does not arise there.
pub fn reinforce(pad: &mut [f32], index: usize, amount: f32) {
    if let Some(slot) = pad.get_mut(index) {
        *slot += amount;
    }
}

/// The mean every pad is normalized to.
///
/// A routing decision only reads weight *ratios*, so the pad's absolute scale carries no routing
/// information -- but it is not free, because the pad is also this node's published representation
/// and `combine_pads` adds pads element-wise. An unbounded pad makes both traffic-volume-weighted:
/// a node that has forwarded a million requests dominates one that has forwarded ten, and two
/// nodes' pads cannot be compared. Keeping the mean at 1.0 leaves routing exactly as it was and
/// makes combination reflect two preferences rather than two traffic counts.
pub const PAD_MEAN: f32 = 1.0;

/// The weight a pad slot gets when it carries no information about its neighbour -- the mean of
/// the slots that do.
///
/// A slot that is missing, or zero, must be *explorable*. `route` picks weighted by the pad, so
/// such a slot shares only the `1e-6` floor with slots that have been reinforced, and is never
/// chosen often enough to be reinforced in turn. That deadlock (no traffic, so no reinforcement,
/// so no traffic) means the pad could strengthen a path and let others lose share, but could not
/// *form* one. Starting at the mean gives an unexplored neighbour an average share on first
/// appearance -- optimistic initialisation, with no rate to tune.
///
/// An all-zero pad has mean zero, and that is correct: the floor already routes over a fresh pad
/// uniformly, so there is nothing to explore ahead of anything else.
fn seed_weight(pad: &[f32]) -> f32 {
    if pad.is_empty() {
        0.0
    } else {
        pad.iter().sum::<f32>() / pad.len() as f32
    }
}

/// Rescale a pad so its values average to [`PAD_MEAN`].
///
/// Routing only ever reads weight *ratios*, so the pad's absolute scale carries no routing
/// information -- but it is not free, because the pad is also this node's published representation
/// and `combine_pads` adds pads element-wise. An unbounded pad makes both traffic-volume-weighted:
/// a node that has forwarded a million requests dominates one that has forwarded ten, and two
/// nodes' pads cannot be compared. Normalizing keeps the mean at 1.0, leaves routing exactly as it
/// was, and makes combination reflect the two preferences rather than the two traffic counts.
///
/// An all-zero pad is left alone: scaling it would divide by zero, and the floor already routes
/// over a fresh pad uniformly.
fn normalize(pad: &mut [f32]) {
    let sum: f32 = pad.iter().sum();
    if sum > 0.0 {
        let scale = pad.len() as f32 / sum;
        for slot in pad.iter_mut() {
            *slot *= scale;
        }
    }
}

/// The engine: a node's pad, its neighbors, and its saved dreams.
#[derive(Debug)]
pub struct Engine {
    pub node_id: String,
    pub pad: Vec<f32>,
    pub neighbors: Vec<String>,
    pub dreams: DreamLog,
    /// The data shard this node holds, if any -- the pad's data-pad role.
    pub shard: Option<Shard>,
    /// The source of this node's contributions: software randomness, a mixed source that sometimes
    /// passes real data, or a hardware source (muon detectors, ...).
    pub source: Box<dyn RandomSource + Send + Sync>,
}

/// What a node did with a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOutcome {
    /// The node answered; the request carries the answer.
    Answered,
    /// The node routed it to another node.
    RouteTo(String),
    /// The node had nowhere to route it.
    NoRoute,
    /// The node could not answer and the request had already travelled [`MAX_ROUTE_HOPS`] hops, so
    /// forwarding it further was refused rather than allowed to amplify.
    TooManyHops,
}

impl Engine {
    pub fn new(node_id: String, pad: Vec<f32>, neighbors: Vec<String>) -> Self {
        Self::with_source(node_id, pad, neighbors, Box::new(SoftwareRandom))
    }

    /// Same as `new`, with an explicit randomness source.
    pub fn with_source(
        node_id: String,
        pad: Vec<f32>,
        neighbors: Vec<String>,
        source: Box<dyn RandomSource + Send + Sync>,
    ) -> Self {
        // One slot per neighbour, and no slot left without information. The pad comes from the
        // layer below and the neighbour list from elsewhere in it, and nothing reconciles them:
        // a neighbour past the end of the pad could be routed to (the floor keeps it reachable)
        // but never reinforced, because `reinforce` had no slot to add to. A zero slot is the same
        // trap from the other side. Both are seeded to the neutral weight, then the whole pad is
        // normalized so it stays comparable across nodes.
        let seed = seed_weight(&pad);
        let mut pad = pad;
        if pad.len() < neighbors.len() {
            pad.resize(neighbors.len(), seed);
        }
        for slot in pad.iter_mut() {
            if *slot == 0.0 {
                *slot = seed;
            }
        }
        normalize(&mut pad);
        Self { node_id, pad, neighbors, dreams: DreamLog::new(), shard: None, source }
    }

    /// This node's data pad: its routing weights plus its shard, if it holds one.
    pub fn data_pad(&self) -> crate::datapad::DataPad {
        crate::datapad::DataPad {
            pad_id: self.node_id.clone(),
            values: self.pad.clone(),
            shard: self.shard.clone(),
        }
    }

    /// Processes a request: adds this node's contribution, then either answers it (if `resolver`
    /// returns `Some`) or routes it to the next hop.
    ///
    /// The pad is *not* changed here. Whether the chosen hop was any good is not known until the
    /// carrier has forwarded the request and seen what came back, so reinforcement happens in
    /// [`Engine::observe_hop`]. Rewarding the hop here, by a constant, is a rich-get-richer random
    /// walk -- see `HopOutcome`.
    pub async fn handle(&mut self, request: &mut Request, resolver: &dyn Resolver) -> HandleOutcome {
        let value = self.source.next();
        let previous = request
            .contributions
            .last()
            .map(|c| c.chain_value().unwrap_or_else(|_| genesis_previous()))
            .unwrap_or_else(genesis_previous);
        request.contributions.push(Contribution {
            node_id: self.node_id.clone(),
            value: hex::encode(value),
            previous: hex::encode(previous),
        });

        if let Some(answer) = resolver.resolve(&Target::infer(&request.target), &request.payload).await {
            request.answer = Some(answer);
            self.dreams.record(request.clone());
            return HandleOutcome::Answered;
        }

        let seed = request
            .contributions
            .last()
            .map(|c| c.chain_value().unwrap_or_else(|_| genesis_previous()))
            .unwrap_or_else(genesis_previous);
        // Stop forwarding a request that has already travelled the budget. Checked *after* the
        // resolver attempt: a node that can answer should answer regardless of how long the chain
        // behind it is.
        if request.contributions.len() >= MAX_ROUTE_HOPS {
            return HandleOutcome::TooManyHops;
        }

        match route(&self.pad, &self.neighbors, &seed) {
            Some(next) => {
                // Deliberately no reinforcement here. At this moment the engine has no idea
                // whether the hop was any good, and rewarding it anyway is what makes the pad
                // lock onto an arbitrary neighbour. The carrier calls `observe_hop` once it
                // knows; see `HopOutcome`.
                HandleOutcome::RouteTo(next)
            }
            None => HandleOutcome::NoRoute,
        }
    }
}

impl Engine {
    /// Records how the hop to `neighbour` turned out and moves the pad accordingly.
    ///
    /// This is the only thing that should change the pad's routing weights: `handle` routes but
    /// deliberately does not reinforce, because it cannot yet know the outcome. Multiplicative
    /// updates keep every weight positive -- an additive penalty could drive a slot negative, and
    /// `normalize` divides by the sum, so a negative total would invert the ordering -- and
    /// `normalize` afterwards keeps the mean at [`PAD_MEAN`].
    ///
    /// An unknown neighbour is ignored, matching [`reinforce`]'s out-of-range behaviour.
    pub fn observe_hop(&mut self, neighbour: &str, outcome: HopOutcome) {
        let Some(index) = self.neighbors.iter().position(|n| n == neighbour) else {
            return;
        };
        match outcome {
            HopOutcome::Answered => self.pad[index] *= HOP_ANSWERED_GAIN,
            HopOutcome::Forwarded => {}
            HopOutcome::Failed => self.pad[index] *= HOP_FAILED_PENALTY,
        }
        normalize(&mut self.pad);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::LocalResolver;

    fn index_of(neighbors: &[String], target: &str) -> usize {
        neighbors.iter().position(|n| n == target).expect("neighbour must be present")
    }

    fn request(id: &str) -> Request {
        Request {
            id: id.to_string(),
            target: serde_json::json!("target"),
            payload: serde_json::json!({}),
            contributions: vec![],
            answer: None,
        }
    }

    /// The defect this guards: `DreamLog` was an unbounded `Vec<Request>`, and `handle` records
    /// every answered request as a clone. Any caller who can make this node answer a request --
    /// which is any peer that can route one here -- could grow its memory without limit, and
    /// because a request carries caller-controlled JSON the growth was amplified per request.
    #[tokio::test]
    async fn the_dream_log_evicts_the_oldest_entry_past_its_count_cap() {
        let mut engine = Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![],
        );
        engine.dreams = DreamLog::with_bounds(2, DEFAULT_DREAM_LOG_MAX_BYTES);
        let mut resolver = LocalResolver::new();
        resolver.insert("target", serde_json::json!("answer"));

        for id in ["req-1", "req-2", "req-3"] {
            let mut request = request(id);
            engine.handle(&mut request, &resolver).await;
        }

        assert_eq!(engine.dreams.len(), 2, "the count cap must hold");
        // Oldest first, so the two survivors are the two most recent.
        let ids: Vec<&str> = engine.dreams.dreams().map(|d| d.id.as_str()).collect();
        assert_eq!(ids, vec!["req-2", "req-3"]);
        assert_eq!(engine.dreams.last().unwrap().id, "req-3");
    }

    /// A count cap alone is not enough: one request may be megabytes (`MAX_MESSAGE_BYTES` is 16 MiB
    /// over TCP), so the byte cap is the one that actually bounds memory. With a tiny byte cap and
    /// a large payload, the log must evict rather than retain.
    #[tokio::test]
    async fn the_dream_log_evicts_past_its_byte_cap() {
        let mut engine = Engine::new("node-a".to_string(), vec![1.0; PAD_DIM], vec![]);
        engine.dreams = DreamLog::with_bounds(DEFAULT_DREAM_LOG_MAX_ENTRIES, 64);
        let mut resolver = LocalResolver::new();
        resolver.insert("target", serde_json::json!("answer"));

        for i in 0..5 {
            let mut request = request(&format!("req-{i}"));
            request.payload = serde_json::json!("x".repeat(256));
            engine.handle(&mut request, &resolver).await;
        }

        assert!(engine.dreams.bytes() <= 64, "the byte cap must hold, got {}", engine.dreams.bytes());
        assert!(engine.dreams.len() < 5, "oversized entries must have been evicted");
    }

    /// One entry larger than the whole budget must not be retained: otherwise a single request
    /// would defeat the bound entirely.
    #[tokio::test]
    async fn a_single_entry_larger_than_the_budget_is_not_retained() {
        let mut engine = Engine::new("node-a".to_string(), vec![1.0; PAD_DIM], vec![]);
        engine.dreams = DreamLog::with_bounds(DEFAULT_DREAM_LOG_MAX_ENTRIES, 8);
        let mut resolver = LocalResolver::new();
        resolver.insert("target", serde_json::json!("answer"));

        let mut request = request("req-big");
        request.payload = serde_json::json!("x".repeat(1024));
        engine.handle(&mut request, &resolver).await;

        assert!(engine.dreams.is_empty(), "an entry larger than the budget must be evicted, got {} bytes", engine.dreams.bytes());
    }

    #[tokio::test]
    async fn the_dream_log_queries_by_target_and_node() {
        let mut engine = Engine::new("node-a".to_string(), vec![1.0; PAD_DIM], vec!["node-b".to_string()]);
        let mut resolver = LocalResolver::new();
        resolver.insert("target", serde_json::json!("answer"));
        resolver.insert("other-target", serde_json::json!("answer"));
        let mut first = request("req-q1");
        engine.handle(&mut first, &resolver).await;
        let mut second = request("req-q2");
        second.target = serde_json::json!("other-target");
        engine.handle(&mut second, &resolver).await;

        let target = serde_json::json!("target");
        assert_eq!(engine.dreams.for_target(&target).len(), 1);
        assert_eq!(engine.dreams.for_node("node-a").len(), 2);
        assert_eq!(engine.dreams.last().unwrap().id, "req-q2");
    }

    /// The hop budget is what stops a cycling request from amplifying: the engine has no
    /// visited-set, so a cycle between two pads is a possible routing outcome, and without this
    /// cap such a request would grow one contribution per hop and be re-sent in full each time
    /// until the carrier's message-size limit happened to reject it.
    #[tokio::test]
    async fn a_request_at_the_hop_budget_is_not_forwarded() {
        let mut engine = Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec!["node-b".to_string()],
        );
        // A resolver that can never answer, so the only paths are forwarding and giving up.
        let resolver = LocalResolver::new();

        // The chain length is checked *after* this node adds its own contribution, so a node
        // forwards only while the chain it would pass on is still under the budget.
        let contributions = |n: usize| (0..n).map(|i| Contribution {
            node_id: format!("prev-{i}"),
            value: "00".to_string(),
            previous: "00".to_string(),
        }).collect::<Vec<_>>();

        // Two below the budget: this node's contribution makes it MAX-1, so it still routes.
        let mut near_limit = request("req-near-limit");
        near_limit.contributions = contributions(MAX_ROUTE_HOPS - 2);
        assert!(matches!(
            engine.handle(&mut near_limit, &resolver).await,
            HandleOutcome::RouteTo(_)
        ));
        assert_eq!(near_limit.contributions.len(), MAX_ROUTE_HOPS - 1);

        // One below the budget: this node's contribution reaches MAX, so forwarding is refused.
        let mut at_limit = request("req-at-limit");
        at_limit.contributions = contributions(MAX_ROUTE_HOPS - 1);
        assert_eq!(
            engine.handle(&mut at_limit, &resolver).await,
            HandleOutcome::TooManyHops
        );
        // The refusal still added this node's own contribution -- the bound is on forwarding, not
        // on recording that this node took part.
        assert_eq!(at_limit.contributions.len(), MAX_ROUTE_HOPS);
    }

    /// The budget bounds forwarding, not answering: a node that can answer should answer however
    /// long the chain behind the request is.
    #[tokio::test]
    async fn the_hop_budget_does_not_stop_a_node_that_can_answer() {
        let mut engine = Engine::new("node-a".to_string(), vec![1.0; PAD_DIM], vec![]);
        let mut resolver = LocalResolver::new();
        resolver.insert("target", serde_json::json!("answer"));

        let mut request = request("req-long-chain");
        request.contributions = (0..MAX_ROUTE_HOPS + 500)
            .map(|i| Contribution {
                node_id: format!("prev-{i}"),
                value: "00".to_string(),
                previous: "00".to_string(),
            })
            .collect();

        assert_eq!(engine.handle(&mut request, &resolver).await, HandleOutcome::Answered);
        assert_eq!(request.answer, Some(serde_json::json!("answer")));
    }

    #[tokio::test]
    async fn a_request_travels_and_gets_a_chain_of_contributions() {
        let mut engine = Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec!["node-b".to_string(), "node-c".to_string()],
        );
        let resolver = LocalResolver::new();
        let mut req = request("req-1");
        let outcome = engine.handle(&mut req, &resolver).await;
        // Both neighbors have equal weight, so the pad-guided pick is one of them.
        let next = match outcome {
            HandleOutcome::RouteTo(next) => next,
            other => panic!("expected a route, got {other:?}"),
        };
        assert!(next == "node-b" || next == "node-c");
        assert_eq!(req.contributions.len(), 1);
        assert_eq!(req.contributions[0].node_id, "node-a");

        // Routing alone must not move the pad: at this moment the engine cannot know whether the
        // hop was any good, and rewarding it anyway is what made the pad lock onto an arbitrary
        // neighbour.
        let after_route = engine.pad.clone();
        assert!(
            after_route.iter().all(|v| (v - 1.0).abs() < 1e-6),
            "routing must not change the pad, got {:?}",
            engine.pad
        );

        // The carrier reports the outcome, and only then does the pad move -- toward a hop that
        // answered, away from one that could not be reached.
        engine.observe_hop(&next, HopOutcome::Answered);
        assert!(
            engine.pad.iter().any(|v| *v > 1.0),
            "an answered hop must strengthen the neighbour that carried it"
        );
        let answered = engine.pad.clone();
        engine.observe_hop(&next, HopOutcome::Failed);
        assert!(
            engine.pad[index_of(&engine.neighbors, &next)] < answered[index_of(&engine.neighbors, &next)],
            "an unreachable hop must weaken the neighbour"
        );
    }

    #[tokio::test]
    async fn a_resolver_answers_and_the_dream_is_saved() {
        let mut engine = Engine::new("node-a".to_string(), vec![1.0; PAD_DIM], vec![]);
        let mut resolver = LocalResolver::new();
        resolver.insert("target", serde_json::json!("answer"));
        let mut req = request("req-2");
        let outcome = engine.handle(&mut req, &resolver).await;
        assert_eq!(outcome, HandleOutcome::Answered);
        assert_eq!(req.answer, Some(serde_json::json!("answer")));
        assert_eq!(engine.dreams.len(), 1);
    }

    /// The pad's central claim -- traffic through a path strengthens it, so repeated requests
    /// reuse it -- measured as a distribution over fixed seeds. `route` is deterministic given
    /// (pad, neighbours, seed), so this is an exact measurement, not a flaky sample.
    #[test]
    fn reinforcing_a_neighbour_shifts_routing_toward_it() {
        let neighbors = vec!["node-a".to_string(), "node-b".to_string()];
        let seeds: Vec<[u8; 32]> = (0..256u32)
            .map(|i| {
                let mut seed = [0u8; 32];
                seed[..4].copy_from_slice(&i.to_le_bytes());
                seed
            })
            .collect();
        let share_of_a = |pad: &[f32]| {
            seeds
                .iter()
                .filter(|seed| route(pad, &neighbors, seed).as_deref() == Some("node-a"))
                .count()
        };

        let even = share_of_a(&[1.0, 1.0]);
        assert!(
            (96..=160).contains(&even),
            "equal pads must split traffic about evenly, got {even}/256"
        );

        let reinforced = share_of_a(&[100.0, 1.0]);
        assert!(
            reinforced >= 240,
            "a pad reinforced 100x must carry nearly all the traffic, got {reinforced}/256"
        );
    }

    /// A neighbour the pad does not cover must be *explorable*, not merely reachable through the
    /// `1e-6` floor. Without a slot, `reinforce` has nothing to add to, so the neighbour is never
    /// chosen often enough to be reinforced in turn -- the pad can lose share but never form a
    /// pathway. `Engine::with_source` seeds such a slot, and a zero slot the same way.
    #[test]
    fn a_neighbour_beyond_the_pad_is_explorable_not_frozen() {
        let mut engine = Engine::new(
            "node-a".to_string(),
            vec![100.0, 1.0],
            vec!["node-b".to_string(), "node-c".to_string(), "node-d".to_string()],
        );
        assert_eq!(engine.pad.len(), 3, "the pad must cover every neighbour");

        let neighbors = engine.neighbors.clone();
        let seeds: Vec<[u8; 32]> = (0..256u32)
            .map(|i| {
                let mut seed = [0u8; 32];
                seed[..4].copy_from_slice(&i.to_le_bytes());
                seed
            })
            .collect();
        let share_of_d = |pad: &[f32]| {
            seeds
                .iter()
                .filter(|seed| route(pad, &neighbors, seed).as_deref() == Some("node-d"))
                .count()
        };

        // The pad is normalized to PAD_MEAN, so the seeded slot is the mean scaled by the pad's
        // own total, and the pad's average is exactly PAD_MEAN.
        let seed = seed_weight(&[100.0, 1.0]);
        let scale = 3.0 / (100.0 + 1.0 + seed);
        assert!(
            (engine.pad[2] - seed * scale).abs() < 1e-5,
            "a newly covered neighbour starts at the mean share, got {}",
            engine.pad[2]
        );
        let mean = engine.pad.iter().sum::<f32>() / engine.pad.len() as f32;
        assert!((mean - PAD_MEAN).abs() < 1e-5, "the pad must be normalized, mean {mean}");

        let before = share_of_d(&engine.pad);
        assert!(
            before > 40,
            "a newly seeded neighbour must be explored at a meaningful rate, got {before}/256"
        );

        // And it is reinforced normally once traffic reaches it, closing the loop the floor
        // alone could not.
        reinforce(&mut engine.pad, 2, 1.0);
        normalize(&mut engine.pad);
        let after = share_of_d(&engine.pad);
        assert!(
            after > before,
            "a reinforced neighbour must take a larger share, {before} -> {after}"
        );
    }

    /// Routing reads weight ratios only, so the pad's absolute scale is free -- and the published
    /// representation and `combine_pads` must not depend on how much traffic a node happens to have
    /// seen. Two nodes with the same preference produce the same pad.
    #[test]
    fn two_nodes_with_the_same_preference_produce_the_same_pad() {
        let a = Engine::new(
            "a".to_string(),
            vec![100.0, 1.0],
            vec!["n1".to_string(), "n2".to_string()],
        );
        let b = Engine::new(
            "b".to_string(),
            vec![10.0, 0.1],
            vec!["n1".to_string(), "n2".to_string()],
        );
        for (x, y) in a.pad.iter().zip(&b.pad) {
            assert!((x - y).abs() < 1e-5, "same preference, different traffic: {x} vs {y}");
        }
    }

    /// A zero slot is the same trap as a missing one: it shares only the floor, so it is never
    /// reinforced and never recovers. A pad from the layer below can have slots that were never
    /// used, so this is reachable without any misconfiguration.
    #[test]
    fn a_zero_slot_is_seeded_like_a_missing_one() {
        let engine = Engine::new(
            "node-a".to_string(),
            vec![100.0, 0.0, 0.0],
            vec!["node-b".to_string(), "node-c".to_string(), "node-d".to_string()],
        );
        assert!(
            engine.pad.iter().all(|v| *v > 0.0),
            "no slot may be left at zero: {:?}",
            engine.pad
        );
        let mean = engine.pad.iter().sum::<f32>() / engine.pad.len() as f32;
        assert!((mean - PAD_MEAN).abs() < 1e-5, "the pad must be normalized, mean {mean}");
    }

    #[tokio::test]
    async fn routing_with_no_neighbors_is_a_no_route() {
        let mut engine = Engine::new("node-a".to_string(), vec![], vec![]);
        let resolver = LocalResolver::new();
        let mut req = request("req-3");
        assert_eq!(engine.handle(&mut req, &resolver).await, HandleOutcome::NoRoute);
    }

    #[test]
    fn contributions_chain_and_combine_pads() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let chained = chain_contribution(&a, &b);
        assert_ne!(chained, a);
        assert_ne!(chained, b);

        let combined = combine_pads(&[1.0, 2.0], &[3.0, 4.0]);
        assert_eq!(combined, vec![4.0, 6.0]);
    }
}
