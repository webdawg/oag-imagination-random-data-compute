# Overview

**Status: the routing engine is implemented (`spec/02`), over TCP (`spec/03`).**

## What this layer is

The imagination layer is an algorithm that runs on every `oag` node and keeps
`open-agent-graph` moving. It does not rebuild foundational items: it uses `oag`'s event log,
replication, signing, and tensor pads.

Three ideas:

1. **A request can be anything.** Connect to a legacy server (https), a communication request to
   a node, an API spend, storing information. The possibilities are infinite.
2. **The tensor pad routes it.** A node's pad is its unique representation of the picture of all
   the nodes around it. The node uses the pad to pick the next hop. More traffic through a path
   strengthens the pad along it, so repeated requests reuse the same path.
3. **Each node drops a random message or real data** as part of constructing the request. The
   final response carries the whole metadata chain back.

## What this layer explicitly is not

- **Not a new network protocol.** The engine is transport-agnostic. TCP today; Reticulum, IPFS,
  and tunnels are the same engine over a different carrier.
- **Not a configured topology.** No configuration, ever. The neighbors come from the layer below
  (`oag`).
- **Not a trustless beacon.** The pad is a representation, not a guarantee.
- **Not a data store for real claims.** That is `oag` and `oag-mesh`.

## Wiring into `oag`

The layer does not keep its own copy of the pad or the peer list. `node::NodeAdapter` reads the
node's pad from the `tensor_pads` table and its neighbors from the `peers` and `peer_addresses`
tables -- the layer below -- and builds the engine from what it finds. A node with no pad yet gets
an empty one, matching the base project's own default.

The adapter only reads. It does not update the pad or the peer list; the base project's own
writers (`oag-sync`, `oag-brain`) remain the only writers.

## Randomness sources

A node's contribution comes from a pluggable `RandomSource` (`source.rs`):

- `SoftwareRandom` -- `oag_crypto::random_bytes_32`.
- `MixedSource` -- with a configurable probability, returns randomness; otherwise asks a provider
  for real data, falling back to randomness when it has none. This is the "all nodes will
  sometimes choose to pass non random information forward" behavior.
- `HardwareRandom` -- the seam for muon detectors, radioactive decay, atmospheric noise, etc.
  The hardware interface is still to be figured out; the source falls back to software randomness
  until a driver exists, and a driver replaces `next` without changing anything else.

## Open design questions

- The hardware randomness interface is still to be figured out. Muon detectors, answers to
  questions -- `HardwareRandom` is the seam.
- The pad is deliberately not over-designed. The mechanisms around it change it, and the random
  chaos eventually solidifies into something real.
