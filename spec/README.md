# oag-imagination — Specification

One file per feature, same convention as [`oag-mesh`](https://github.com/webdawg/oag-mesh) and
the base project.

## Index

| File | Feature |
|---|---|
| [01-overview.md](01-overview.md) | Scope, relationship to `oag` and `oag-mesh`, and what this layer is not |
| [02-tensor-pad-routing.md](02-tensor-pad-routing.md) | The routing engine: a request travels through the node group, guided by the tensor pads, gathering contributions |
| [03-transports.md](03-transports.md) | The carriers: TCP today; Reticulum, IPFS, and tunnels are the same engine over a different carrier |

## Founding context

See this repository's `USER_INPUT_RECORD.md` for the verbatim founding statements, and the base
repository's `USER_INPUT_RECORD.md` for the layer-up context.
