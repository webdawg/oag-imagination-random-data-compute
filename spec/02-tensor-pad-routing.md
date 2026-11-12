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

### The pad must cover every neighbour, and they must be explorable

`route` picks weighted by the pad, so a neighbour with weight `0` -- or with no slot at all --
shares only the `1e-6` floor with slots that have been reinforced, and is never chosen often
enough to be reinforced in turn. That deadlock (no traffic, so no reinforcement, so no traffic)
meant the pad could strengthen a path and let the others lose share, but could never *form* one:
a neighbour the pad did not cover was frozen out for the life of the process.

The pad and the neighbour list come from different layers and nothing reconciled them, so this was
reachable with no misconfiguration -- `node.rs` passes the stored pad and the neighbour list
straight to the engine. `Engine::with_source` now seeds the pad to one slot per neighbour, filling
missing slots with the **mean of the existing slots**. A fresh all-zero pad has mean zero, which is
exactly right: the floor already makes every neighbour equal there. A learned pad gives a newly
covered neighbour an average share on first appearance, so it is actually tried.

Measured, because the claim was previously only asserted: `reinforcing_a_neighbour_shifts_routing_toward_it`
runs `route` over 256 fixed seeds and checks the distribution -- equal pads split about evenly, a
pad reinforced 100x carries nearly all the traffic. `route` is deterministic given
(pad, neighbours, seed), so the test is an exact measurement rather than a flaky sample.

**What the pad still cannot do: weaken or dissolve.** `reinforce` only adds. A path that stops
carrying traffic keeps its absolute weight forever; its *share* falls as others are reinforced, so
it becomes negligible, but it never decays and can never be retired or out-competed by a path that
was never tried. The mission's "pathways can strengthen, weaken, form, or dissolve" is therefore
half implemented: strengthen and (after the seeding fix) form are real, weaken and dissolve are not.
Adding decay is a design change with a rate to justify and is deliberately not smuggled in here.

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
