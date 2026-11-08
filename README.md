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

## Status

The random beacon (`spec/02-random-beacon.md`) is implemented: a verifiable, signed,
deterministic sequence of random values derived from a published genesis seed. The dream log
(`spec/03-dream-log.md`) and dream compute (`spec/04-dream-compute.md`) are specified but not
yet built.

## License

AGPL-3.0-or-later. See [`LICENSE`](LICENSE).
