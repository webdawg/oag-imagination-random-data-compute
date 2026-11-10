# Transports

**Source**: `crates/oag-imagination/src/socket.rs`.

The engine is transport-agnostic. This file lists the carriers.

## Reticulum (seam only)

`ReticulumCarrier` is the seam for the Reticulum carrier: the base project's Reticulum
transport (`oag-reticulum`) adapts to it, and the imagination layer does not depend on the
Reticulum crate. The adapter is the same transport with a different handler. A carrier only
moves a message to a destination and returns the reply; the routing logic stays in the
engine.

## TCP (implemented)

A thin length-prefixed JSON protocol over TCP. A message is either an imagination `Request` (the
routing engine) or a `PadRequest`/`PadResponse` pair (the data-pad gathering path).

- `socket::serve` / `socket::serve_on` -- serve the engine; route requests onward and return the
  chain; answer a `PadRequest` with the node's data pad.
- `socket::send` -- send a request and read the response.
- `socket::gather_file` -- ask each node for its pad, gather the shards in manifest order, and
  reconstruct the file. A missing pad is an error, not a guess.

## UDP (implemented)

A second socket carrier: one datagram in, one datagram out. `UdpTransport` (client) and
`serve_udp` / `serve_udp_on` (server). Datagrams are capped at the UDP payload limit
(`MAX_DATAGRAM_BYTES`); a larger message is an error rather than a silent truncation.

UDP is connectionless, so a routed request is answered by the node it reached rather than
forwarded on a connection. A deployment that needs multi-hop routing over UDP would forward the
reply to the client itself; the carrier is for the local answer.

## HTTP CONNECT tunnel (implemented)

`ProxiedTransport` tunnels another carrier through an HTTP proxy: connect to the proxy, send
`CONNECT` for the target, then run the message exchange over the tunnel. The inner carrier is
unchanged; only the path to the target changes -- the "tunnel existing protocols" carrier. A
real minimal proxy (CONNECT then forward) is used in the test.

## HTTP (implemented)

`HttpTransport` POSTs the message as JSON to `<address>/oag-imagination/v1/message` and reads
the JSON reply -- the carrier that lets the engine talk to an HTTP API (the base project's REST
surface) rather than a raw socket.

## IPFS (implemented)

Both directions, over the same `Transport` trait. `IpfsTransport` reads: the address is a CID and
the message published there is the reply. `PublishingIpfsTransport` writes: it publishes the
message via the `IpfsPublisher` seam (`HttpIpfsPublisher` posts to `/api/v0/add`) and returns the
CID as the answer. `CidResolver`/`HttpGatewayFetcher` answer `Target::Cid` by fetching through an
IPFS HTTP gateway.

## Tunnels (implemented)

`ProxiedTransport` (above) is the tunnel carrier. There is no separate "tunnels" carrier
beyond it.

## Reticulum (not yet)

The same engine over a Reticulum `Link`. The base project already has a Reticulum transport
(`oag-reticulum`); the imagination engine uses the same carrier. `ReticulumCarrier` is the seam
and the adapter is not written.

## The carrier trait

`socket::Transport` is the seam: `fn send(&self, address, message) -> a future returning the
reply`. `TcpTransport` implements it; a test in `socket.rs` implements it in memory. The HTTP,
tunnel, and IPFS carriers implement the same trait -- a carrier only moves a message and returns
the reply; the routing logic is the engine's, not the carrier's. `send_with` and
`gather_file_with` take an explicit carrier. `ReticulumCarrier` is a second, Option-returning
seam for the one carrier whose transport has no built-in implementation yet.

## What this explicitly is not

- **Not a new application protocol.** The engine is the same over every carrier.
- **Not a configured transport.** The carrier is chosen by the layer below.
