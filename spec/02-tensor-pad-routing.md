# Tensor Pad Routing

**Source**: `crates/oag-imagination/src/engine.rs`.

## The mechanism

A request enters a node. The node:

1. Adds a contribution -- a random message or real data -- chained to the previous contribution.
2. Checks whether it can answer the request (a resolver).
3. If yes, fills in the answer and saves the dream.
4. If no, uses its tensor pad to pick the next hop, reinforces the pad for that hop, and
   forwards.

The request travels through 6, 10, or 50 nodes, whoever knows. The final response carries the
whole metadata chain back.

## The pad

The pad is a 32-element float vector -- a node's unique representation of the picture of all the
nodes around it. The node's values are weights over its neighbors; the node picks the next hop
weighted by the pad. More traffic through a path strengthens the pad along that path, so repeated
requests for the same thing tend to reuse it.

The pad is not over-designed. The mechanisms around it change it, and the traffic shapes it.

## Pads can be combined

Two or more pads can be combined (added element-wise) to represent a larger neighborhood or to
serve a request that spans more than one node's view.

## Pads as data pads (not yet)

A pad is also a unit of data. In the future, a file can be constructed from many pads -- a
thousand pads, each holding a piece, combined to reconstruct a single file. This is the same
"pads can be combined" idea, extended from serving a request to serving a file. The data-pad
mechanism is not built yet; the base project's `spec/23-tensor-pad-and-evolution-layer.md` records
it as the future shape the pad is designed to allow.

## What this explicitly is not

- **Not a routing table.** The pad is a representation, not a table to tune.
- **Not a configured topology.** The neighbors come from `oag`.
- **Not a guarantee.** The pad guides the search; it does not guarantee the endpoint is found.
