# Transports

**Source**: `crates/oag-imagination/src/socket.rs`.

The engine is transport-agnostic. This file lists the carriers.

## Reticulum (implemented, in a separate crate)

**Source**: `crates/oag-imagination-reticulum/src/lib.rs`.

`ReticulumCarrier` was the seam; `oag-imagination-reticulum` is the adapter that fills it. The
imagination *layer* still has no dependency on the `reticulum` crate -- the adapter is a separate
crate, so a node running over TCP, HTTP, or IPFS never pays for this one.

- `ReticulumTransport::connect(uplink_tcp, identity_seed)` -- the client half. Implements both
  `ReticulumCarrier` (the Option-returning seam) and `Transport` (so `send_with` works), moving
  one `Message` to a destination address and returning the reply. `with_announce_wait` overrides
  how long a send waits for the destination's announce.
- `serve(engine, resolver, identity_seed, listen_tcp)` -- the server half. Announces
  `("oag-imagination", "engine")` and answers requests through the engine, mirroring
  `socket::serve_on`. Announces every 5s so a client that subscribes between announcements does
  not wait a full interval.
- Framing is a 4-byte big-endian length prefix over the JSON body, split into 1400-byte chunks --
  deliberately the same wire format as the base project's own Reticulum transport.

**Identity.** The base project derives its Reticulum identity from the peer's Ed25519 signing key.
This crate has no `oag-crypto` dependency, so it takes a seed *string* and derives deterministically
via `PrivateIdentity::new_from_name`. The seed must be stable across restarts or the node's address
changes and its peers can no longer reach it.

**Failure posture.** A send that cannot complete -- no announce, link not activated, no reply --
returns `None` (or an `io::Error` through `Transport`) rather than panicking or hanging past its
timeout. This is the same "degrade, don't drop" rule the other carriers follow.

## TCP (implemented)

A thin length-prefixed JSON protocol over TCP. A message is either an imagination `Request` (the
routing engine) or a `PadRequest`/`PadResponse` pair (the data-pad gathering path).

- `socket::serve` / `socket::serve_on` -- serve the engine; route requests onward and return the
  chain; answer a `PadRequest` with the node's data pad.
- `socket::send` -- send a request and read the response.
- `socket::gather_file` -- ask each node for its pad, gather the shards in manifest order, and
  reconstruct the file. A missing pad is an error, not a guess.

**Every wait on the TCP carrier is bounded.** `INBOUND_READ_TIMEOUT` (30s) closes a connection
whose peer completes the handshake and then sends nothing; `UPSTREAM_TIMEOUT` (30s) bounds both the
connect to the next hop and the wait for its reply. Before these, a stalling client held a spawned
task and a socket for as long as it liked -- a slowloris-shaped availability hole, with no
authentication in front of it -- and a black-holed next hop held the task for the operating system's
connect timeout, which is minutes. Both are the same posture as the base project's outbound request
bound (`spec/18`) and `oag-mesh`'s. A hop that times out returns the request unanswered rather than
leaving it hanging, matching the crate's "degrade, don't drop" rule.

`DEFAULT_MAX_CONNECTIONS` (1024) caps how many connections are served at once. Timeouts make each
connection finite but not their number, and enough simultaneous slow clients still exhaust tasks and
sockets; the cap is an availability backpressure mechanism -- a full server stops accepting rather
than queueing connections nobody is reading, so an accepted client waits for a slot instead of being
served concurrently. It is deliberately generous, being a backstop rather than a throughput knob.

`serve_on_with_limits` takes a `Limits` (both timeouts and the cap) so a deployment can retune them
and a test can make them small enough to observe; `serve`/`serve_on` use the defaults.

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

## Reticulum tests

`crates/oag-imagination-reticulum/tests/loopback.rs` drives a real `TcpServer`/`TcpClient` pair on
loopback -- no graph, no TCP engine carrier, no public Reticulum network:

- `a_request_travels_over_a_real_reticulum_link` -- a real request reaches a real `serve`
  instance over a real `Link`, is answered by the engine's resolver, and the reply carries the
  server node's contribution.
- `a_send_to_an_unknown_destination_fails_softly` -- no server is listening, so no announce can
  ever arrive; the send must return `None` at its own timeout rather than hanging.
- `a_bad_destination_address_is_a_clean_error` -- a malformed address hash fails softly too.

## The carrier trait

`socket::Transport` is the seam: `fn send(&self, address, message) -> a future returning the
reply`. `TcpTransport` implements it; a test in `socket.rs` implements it in memory. The HTTP,
tunnel, and IPFS carriers implement the same trait -- a carrier only moves a message and returns
the reply; the routing logic is the engine's, not the carrier's. `send_with` and
`gather_file_with` take an explicit carrier. `ReticulumCarrier` is a second, Option-returning
seam, kept separate from `Transport` because a Reticulum send fails in ways a TCP send does not
(no announce, link not activated) and the caller should see that as "no answer" rather than as an
I/O error. `ReticulumTransport` implements both, mapping `None` to an `io::Error` for the
`Transport` half.

## What this explicitly is not

- **Not a new application protocol.** The engine is the same over every carrier.
- **Not a configured transport.** The carrier is chosen by the layer below.
