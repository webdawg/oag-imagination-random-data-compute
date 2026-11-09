//! `oag-imagination-random-data-compute` -- the imagination layer.
//!
//! **Helps AI dream.** A layer parallel to [`oag-mesh`](https://github.com/webdawg/oag-mesh),
//! both one level above [`open-agent-graph`](https://github.com/webdawg/open-agent-graph).
//!
//! It does not rebuild foundational items. It uses `oag`'s event log, replication, signing, and
//! tensor pads, and adds one algorithm on top: a routing engine that keeps `oag` moving.
//!
//! - `engine` -- the routing engine: a request enters a node, the node uses its tensor pad to
//!   pick the next hop, each hop adds a contribution (a random message or real data), and the
//!   final response carries the whole metadata chain back.
//! - `socket` -- a thin TCP carrier. The engine is transport-agnostic; Reticulum, IPFS, and
//!   tunnels are the same engine over a different carrier.
//!
//! See `spec/01-overview.md`, `spec/02-tensor-pad-routing.md`, and `spec/03-transports.md`.

pub mod engine;
pub mod socket;

pub use engine::{
    chain_contribution, combine_pads, reinforce, route, Contribution, DreamLog, Engine, EngineError,
    HandleOutcome, Request, CONTRIBUTION_DOMAIN, PAD_DIM, ROUTE_DOMAIN,
};
pub use socket::{read_request, send, serve, write_request, MAX_MESSAGE_BYTES};
