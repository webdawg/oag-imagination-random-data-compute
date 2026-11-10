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

## A request may travel at most `MAX_ROUTE_HOPS` hops

The engine has no visited-set: the pad picks the next hop by weight, so a cycle between two nodes
is a possible routing outcome rather than an impossible one. Without a hop budget such a request
would accumulate one contribution per hop and be re-sent in full each time, amplifying bandwidth and
memory until the carrier's `MAX_MESSAGE_BYTES` happened to reject it -- a bound, but an accidental
one measured in megabytes of waste per cycle.

`MAX_ROUTE_HOPS` (64) is well above this file's own "6, 10, or 50 nodes" design intent and matches
the spirit of the base project's `relay_hops_remaining`, which bounds the same shape of
amplification on the relay path. A node that has exhausted the budget returns `TooManyHops` instead
of forwarding; it still records its own contribution, because the bound is on forwarding, not on
the node acknowledging that it took part.

**The budget bounds forwarding, not answering.** A node that can answer does so however long the
chain behind the request is -- the answer is the whole point, and refusing it would turn a
bounded-chain problem into a failed request.

## The dream log is bounded

"Saves the dream" used to mean an unbounded `Vec<Request>`: every answered request was cloned into
a vector that nothing ever evicted. That is a memory-exhaustion vector reachable by any caller who
can make this node *answer* a request -- which is any peer that can route one here -- and it is
amplified per request, because a request carries caller-controlled JSON (`target`, `payload`) plus
the whole contribution chain gathered along the route.

A count cap on its own would not have been enough, and that is the important part: a single request
may be up to the carrier's `MAX_MESSAGE_BYTES` (16 MiB over TCP), so "keep the last 1,000 dreams"
still permits the log to reach tens of gigabytes. The log therefore enforces **both** a count cap
(`DEFAULT_DREAM_LOG_MAX_ENTRIES`, 1,000) and a serialized-byte cap
(`DEFAULT_DREAM_LOG_MAX_BYTES`, 8 MiB), evicting the oldest entry until both hold. An entry larger
than the whole byte budget is not retained at all -- otherwise one request would defeat the bound.

`DreamLog::with_bounds` exists so a deployment can retune the trade-off and a test can exercise
eviction cheaply.

**In-memory only, and that is a scope statement rather than an oversight.** "Recording generated
scenarios" holds for the running process, not across restarts. Persisting the log would need its
own decisions -- where it lives, how long it is kept, who may read it -- none of which are made
here. The base project's tensor pad is the same shape: a value a process holds and re-learns.

## The pad

The pad is a 32-element float vector -- a node's unique representation of the picture of all the
nodes around it. The node's values are weights over its neighbors; the node picks the next hop
weighted by the pad. More traffic through a path strengthens the pad along that path, so repeated
requests for the same thing tend to reuse it.

The pad is not over-designed. The mechanisms around it change it, and the traffic shapes it.

## Pads can be combined

Two or more pads can be combined (added element-wise) to represent a larger neighborhood or to
serve a request that spans more than one node's view.

## Pads as data pads

A pad is also a unit of data. A file is split into many shards (a thousand by default), each
attached to a different node's pad, and a manifest names which pad holds which shard. To
reconstruct the file, gather the pads in manifest order and concatenate the shards.

`datapad::split` and `datapad::reconstruct` implement this. A manifest that promises a thousand
pads names a thousand pads -- a small file leaves the tail of them empty. A missing shard, an
out-of-order shard, or a hash mismatch is an error, not a guess.

The gathering -- querying each node for its pad -- uses whichever carrier is available; the
split and reconstruction are carrier-independent. See the base project's
`spec/23-tensor-pad-and-evolution-layer.md` for the same idea recorded at the base layer.

## What this explicitly is not

- **Not a routing table.** The pad is a representation, not a table to tune.
- **Not a configured topology.** The neighbors come from `oag`.
- **Not a guarantee.** The pad guides the search; it does not guarantee the endpoint is found.
