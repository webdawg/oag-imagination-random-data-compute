# oag-imagination-random-data-compute

**Helps AI dream.**

A layer parallel to [`oag-mesh`](https://github.com/webdawg/oag-mesh): where the mesh
corroborates real data and claims, this layer generates and records synthetic data -- *dreams* --
from verifiable randomness.

- **imagination** -- an AI generates a dream: a synthetic scenario, seeded by randomness.
- **random data** -- a verifiable random beacon provides the seed.
- **compute** -- deterministic computation over the dream log.

## Layer

| Layer | Repo | What it is |
|---|---|---|
| 0 | [`open-agent-graph`](https://github.com/webdawg/open-agent-graph) | one peer's signed event graph |
| 1 | [`oag-mesh`](https://github.com/webdawg/oag-mesh) | a mesh of peers -- clusters, federated trust, onion routing |
| 1 (parallel) | `oag-imagination-random-data-compute` | synthetic data -- dreams from verifiable randomness |

This layer is **parallel** to `oag-mesh`, not above it: the mesh handles real data and claims,
this handles synthetic data and computation.

## Running it

```bash
cargo run --example node -- <oag.sqlite> <node-peer-id-hex> <listen-addr>
```

The adapter reads the node's tensor pad and peer list from the `oag` database; the engine
serves imagination requests over TCP. A node with no pad yet starts empty, and the peers come
from `oag`'s own `peers`/`peer_addresses` tables -- nothing is configured here.

## Status

The routing engine (`spec/02-tensor-pad-routing.md`) is implemented, over TCP and UDP
(`spec/03-transports.md`). A request enters a node, the node uses its tensor pad to pick the
next hop, each hop adds a contribution, and the final response carries the whole metadata chain
back.

Carriers are the same engine over a different transport (`socket::Transport`): TCP, UDP, HTTP
(`HttpTransport`), an HTTP `CONNECT` tunnel (`ProxiedTransport`), and IPFS in both directions
(`IpfsTransport` reads by CID, `PublishingIpfsTransport` publishes and returns the CID). A
Reticulum is implemented in a separate crate, `oag-imagination-reticulum`, which fills the
`ReticulumCarrier` seam with a real Reticulum `Link` -- so this layer still does not depend on the
Reticulum crate, and a node running over TCP or HTTP never pays for it. No carrier is configured:
the node reads its pad and peers from `oag`'s own database.

The founding statements are recorded verbatim in `USER_INPUT_RECORD.md`.

## License

AGPL-3.0-or-later. See [`LICENSE`](LICENSE).
