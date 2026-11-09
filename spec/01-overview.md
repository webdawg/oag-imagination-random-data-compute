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

## Open design questions

- The randomness source per node is undefined. Muon detectors, answers to questions -- the
  hardware interface is still to be figured out.
- The pad is deliberately not over-designed. The mechanisms around it change it, and the random
  chaos eventually solidifies into something real.
