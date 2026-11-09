# Transports

**Source**: `crates/oag-imagination/src/socket.rs`.

The engine is transport-agnostic. This file lists the carriers.

## TCP (implemented)

A thin length-prefixed JSON protocol over TCP. A request enters a node; the node routes it; the
final response carries the chain back. `socket::serve` and `socket::send`.

## Reticulum (not yet)

The same engine over a Reticulum `Link`. The base project already has a Reticulum transport
(`oag-reticulum`); the imagination engine uses the same carrier.

## IPFS (not yet)

The same engine over IPFS. A request can be a CID; the engine routes it over IPFS.

## Tunnels (not yet)

The same engine tunneling existing protocols.

## What this explicitly is not

- **Not a new application protocol.** The engine is the same over every carrier.
- **Not a configured transport.** The carrier is chosen by the layer below.
