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
would share only a floor with slots that have been reinforced, and be never chosen often enough
to be reinforced in turn. That deadlock (no traffic, so no reinforcement, so no traffic) meant the
pad could strengthen a path and let the others lose share, but could never *form* one: a neighbour
the pad did not cover was frozen out for the life of the process.

The floor is **relative**, not absolute. An earlier version added a constant `1e-6`, which was
meaningless against a pad normalized to `PAD_MEAN` over `PAD_DIM` slots: a slot penalized without
limit fell to denormal `f32` (about `1e-45`), where the constant gave it a share of about `1e-8`
and it was effectively frozen. Measured in `examples/convergence.rs`: a switch at decision 10,000
took **1,061** decisions to recover, because the stored ratio between the strongest and weakest
routing slot was about `1e45` and the ratio decays by `exp(-HOP_STEP)` per decision.

`PAD_FLOOR_FRAC` (0.01) floors every routing slot at a fraction of the routing slots' mean, both
in `route` and in the stored pad after every reinforcement. That caps the stored ratio at about
`neighbours / PAD_FLOOR_FRAC`, so recovery is bounded at `ln(neighbours / PAD_FLOOR_FRAC) /
HOP_STEP` decisions -- about 60 for four neighbours -- however long the lock-in lasted. Measured
after the change: the same switch recovers in **69** decisions. The cost is that a neighbour the pad
has learned to avoid keeps a small share of traffic (`PAD_FLOOR_FRAC / neighbours`); that is the
price of being able to unlearn it.

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

### The pad is a normalized preference, not a traffic counter

Routing reads weight *ratios* only, so a pad's absolute scale carries no routing information. That
does not make the scale free: the pad is also this node's published representation and
`combine_pads` adds pads element-wise. Unbounded, the pad makes both traffic-volume-weighted -- a
node that has forwarded a million requests dominates one that has forwarded ten, and two nodes' pads
cannot be compared at all.

Every pad is therefore normalized to a mean of `1.0` (`PAD_MEAN`), at construction and after every
reinforcement. This leaves routing exactly as it was and makes the representation comparable. The
cost is that absolute traffic volume is no longer recoverable from the pad; nothing in this crate
used it, and a deployment that wants it should record it separately rather than reading it back out
of the weights.

A slot that is missing **or zero** is seeded to the mean of the slots that carry information, which
is the same optimistic-initialisation rule applied uniformly. A zero slot is the missing-slot trap
from the other side: it shares only the `1e-6` floor, so it is never chosen often enough to be
reinforced, and a pad from the layer below can have zero slots without any misconfiguration.

### What the pad converges to, measured

`examples/convergence.rs` runs the real engine over a deterministic source (`BLAKE3(counter)`) with
a resolver that never answers, so every request is routed and the pad is exercised on its own. Over
20,000 decisions with four neighbours, one neighbour takes about 96% of all routing, with the share
climbing 0.568 at 1,000 decisions, 0.866 at 5,000, 0.933 at 10,000 and 0.966 at 20,000. The pad mean
stays at exactly 1.0000 throughout.

**Which** neighbour wins is decided by the early draws, not by anything about the neighbours:
shifting the source's starting point (`--offset`) crowns `node-2` for offsets 0-3 and `node-3` for
offsets 4-5, at the same ~96%.

That is the honest reading of the mechanism. `reinforce` adds a constant `1.0` to whichever hop was
taken, regardless of whether it answered, forwarded, or failed -- so the reinforcement carries no
information about the hop's quality. The pad does not learn which neighbour is better; it is a
rich-get-richer random walk that locks onto whichever neighbour the first few hundred decisions
favoured. The "repeated requests reuse the same path" claim is true but vacuous in the sense that
matters: it reuses *an* arbitrary path, not a *better* one.

### Reinforcement follows the hop's outcome

Rewarding whichever hop was taken, by a constant, is what made the pad lock onto an arbitrary
neighbour. Reinforcement is therefore driven by [`HopOutcome`], which the carrier observes and the
engine cannot: `handle` routes and changes nothing, and the carrier calls `Engine::observe_hop`
once it knows how the hop went.

| outcome | effect |
|---|---|
| `Answered` -- the chain was answered past this hop | weight `*= exp(HOP_STEP)` |
| `Forwarded` -- the hop passed it on, nobody answered | unchanged |
| `Failed` -- connect, write, or read failed | weight `*= exp(-HOP_STEP)` |

`HOP_STEP` is `0.1`, a single log-space step in exponential-weights form. The answered/failed ratio
per comparison is `exp(2 * HOP_STEP)`, about `1.22`, and the weight-ratio half-life is
`ln 2 / HOP_STEP`, about 6.9 observations. It replaced two independent round numbers (`1.05` and
`0.90`) because the *ratio* is what carries meaning, and one step makes the memory horizon derivable
rather than tuned.

Updates are multiplicative and followed by the normalization above. Multiplicative keeps every
weight positive: an additive penalty could drive a slot negative, and `normalize` divides by the
sum, so a negative total would invert the ordering rather than merely shrinking it. `Forwarded` is
deliberately neutral -- the hop did its part, and there is no evidence against it.

**Measured, as an A/B.** `examples/convergence.rs --no-signal` runs both arms over the same
deterministic source. With `--good 0`, node-0 answers and the other three are unreachable:

| arm | leader | share |
|---|---|---|
| signal | `node-0`, the one that answers | 99.9% |
| control (old constant reinforcement) | `node-2`, an arbitrary one | 59.6% |

Both arms share the first few hundred draws, so the difference is not the draw -- it is that one arm
has something to learn and the other does not. The control's 59.6% is lower than the 96.7% measured
earlier for the same mechanism, because the pre-change code reinforced without normalizing and so
assembled a larger total; the lock-in is the same phenomenon at a different scale.

**Recovery after a topology change, measured.** `--switch-at 10000` makes the answering neighbour
change at decision 10,000. The new neighbour's *weight* overtakes the old one's **1,061 decisions
later** -- far slower than the 6.9-observation half-life would suggest, and the reason is worth
stating: the half-life describes how fast a weight *ratio* moves per observation, but a deeply
locked-in neighbour is chosen almost every time, so it is penalised almost every time *and* its
selection rate falls as its weight falls. Recovering from a ~1000:1 lock-in therefore takes ~1,000
decisions rather than ~66. That is the honest cost of a small step: stable against noise, slow to
abandon a path that has started failing. A larger step trades those directly, and choosing it is a
deployment decision rather than a coding one.

**Scope.** Only the TCP carrier feeds outcomes back, because it is the only carrier that forwards:
the UDP carrier answers locally by design, so there is no hop to observe. A node whose neighbours
are reached over UDP therefore keeps a pad that does not learn, which is the honest consequence of
that carrier's shape rather than an oversight.

**Weakening and dissolving are relative, and that is enough for routing.** Because routing reads
ratios, a path that stops carrying traffic loses share as others are reinforced and can be
out-competed by a newly seeded neighbour. Its *absolute* weight never falls, but nothing reads
absolute weights any more. What the pad genuinely cannot do is forget: a path that was reinforced
once keeps its weight forever, so a topology change that makes it useless does not remove it, only
dilutes it. Whether an explicit decay term is worth its rate is an open design question, recorded
in the mission research journal rather than settled here.

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
