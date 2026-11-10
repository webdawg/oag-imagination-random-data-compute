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

/// Strengthens the pad for the neighbor at `index` -- more traffic through a path makes the pad
/// along that path stronger.
pub fn reinforce(pad: &mut [f32], index: usize, amount: f32) {
    if let Some(slot) = pad.get_mut(index) {
        *slot += amount;
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
    /// returns `Some`) or routes it to the next hop. The pad is reinforced for the hop it chose,
    /// so traffic strengthens the path it travels.
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
        match route(&self.pad, &self.neighbors, &seed) {
            Some(next) => {
                if let Some(index) = self.neighbors.iter().position(|n| n == &next) {
                    reinforce(&mut self.pad, index, 1.0);
                }
                HandleOutcome::RouteTo(next)
            }
            None => HandleOutcome::NoRoute,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::LocalResolver;

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
        assert!(matches!(outcome, HandleOutcome::RouteTo(ref next) if next == "node-b" || next == "node-c"));
        assert_eq!(req.contributions.len(), 1);
        assert_eq!(req.contributions[0].node_id, "node-a");

        // The pad strengthened for the chosen hop.
        assert!(engine.pad.iter().any(|v| *v > 1.0));
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
