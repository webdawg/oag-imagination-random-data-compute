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

use serde::{Deserialize, Serialize};

use crate::datapad::Shard;
use crate::source::{RandomSource, SoftwareRandom};

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

/// The saved dreams.
#[derive(Debug, Default)]
pub struct DreamLog {
    dreams: Vec<Request>,
}

impl DreamLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, dream: Request) {
        self.dreams.push(dream);
    }

    pub fn dreams(&self) -> &[Request] {
        &self.dreams
    }

    pub fn len(&self) -> usize {
        self.dreams.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dreams.is_empty()
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
    pub fn handle(
        &mut self,
        request: &mut Request,
        resolver: impl Fn(&Request) -> Option<serde_json::Value>,
    ) -> HandleOutcome {
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

        if let Some(answer) = resolver(request) {
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

    fn request(id: &str) -> Request {
        Request {
            id: id.to_string(),
            target: serde_json::json!("target"),
            payload: serde_json::json!({}),
            contributions: vec![],
            answer: None,
        }
    }

    #[test]
    fn a_request_travels_and_gets_a_chain_of_contributions() {
        let mut engine = Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec!["node-b".to_string(), "node-c".to_string()],
        );
        let mut req = request("req-1");
        let outcome = engine.handle(&mut req, |_| None);
        // Both neighbors have equal weight, so the pad-guided pick is one of them.
        assert!(matches!(outcome, HandleOutcome::RouteTo(ref next) if next == "node-b" || next == "node-c"));
        assert_eq!(req.contributions.len(), 1);
        assert_eq!(req.contributions[0].node_id, "node-a");

        // The pad strengthened for the chosen hop.
        assert!(engine.pad.iter().any(|v| *v > 1.0));
    }

    #[test]
    fn a_resolver_answers_and_the_dream_is_saved() {
        let mut engine = Engine::new("node-a".to_string(), vec![1.0; PAD_DIM], vec![]);
        let mut req = request("req-2");
        let outcome = engine.handle(&mut req, |_| Some(serde_json::json!("answer")));
        assert_eq!(outcome, HandleOutcome::Answered);
        assert_eq!(req.answer, Some(serde_json::json!("answer")));
        assert_eq!(engine.dreams.len(), 1);
    }

    #[test]
    fn routing_with_no_neighbors_is_a_no_route() {
        let mut engine = Engine::new("node-a".to_string(), vec![], vec![]);
        let mut req = request("req-3");
        assert_eq!(engine.handle(&mut req, |_| None), HandleOutcome::NoRoute);
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
