//! Run the imagination engine against a real `oag` node's storage.
//!
//! ```text
//! cargo run --example node -- <oag.sqlite> <node-peer-id-hex> <listen-addr>
//! ```
//!
//! The adapter reads the node's tensor pad and peer list from the `oag` database; the
//! engine serves imagination requests over TCP. The resolver is a composite: a local
//! map, a store (reads and writes), a URL fetch, a CID fetch through an IPFS gateway, and
//! a peer fetch through `oag`'s transfer protocol. Nothing is configured here -- the
//! adapter reads the layer below, and the composite is assembled from the resolvers the
//! node happens to have.

use std::sync::Arc;

use tokio::sync::Mutex;

use oag_imagination::{
    socket, CompositeResolver, CidResolver, HttpGatewayFetcher, HttpPeerFetcher, LocalResolver,
    NodeAdapter, PeerResolver, StoreResolver, UrlResolver,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let database = args.next().expect("usage: node <oag.sqlite> <node-peer-id-hex> <listen-addr>");
    let node_peer_id = args.next().expect("missing node peer id (hex)");
    let listen = args.next().expect("missing listen address");

    let mut id = [0u8; 32];
    hex::decode_to_slice(&node_peer_id, &mut id)?;

    let adapter = NodeAdapter::load(std::path::Path::new(&database), &id).await?;
    let node_id = adapter.engine.node_id.clone();
    let neighbor_count = adapter.engine.neighbors.len();
    let engine = Arc::new(Mutex::new(adapter.engine));

    // The composite: try the local map, then the store, then a URL, a CID, and a peer.
    let mut resolver = CompositeResolver::new();
    resolver.add(Box::new(LocalResolver::new()));
    resolver.add(Box::new(StoreResolver::new()));
    resolver.add(Box::new(UrlResolver::new()));
    resolver.add(Box::new(CidResolver::new(HttpGatewayFetcher::new("https://ipfs.io"))));
    resolver.add(Box::new(PeerResolver::new(HttpPeerFetcher::new(
        oag_crypto::PeerIdentity::generate(),
    ))));
    let resolver = Arc::new(resolver);

    eprintln!(
        "oag-imagination: node {} listening on {} ({} neighbor(s))",
        node_id, listen, neighbor_count
    );
    socket::serve(engine, &listen, resolver).await?;
    Ok(())
}
