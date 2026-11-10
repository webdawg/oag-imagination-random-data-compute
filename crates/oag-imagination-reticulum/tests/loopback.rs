//! End-to-end: a real request travels over a real Reticulum `Link` between a
//! `ReticulumTransport` client and a `serve` instance, with no graph, no TCP
//! engine carrier, and no public Reticulum network involved.

use std::sync::Arc;
use std::time::Duration;

use oag_imagination::{Engine, LocalResolver, Message, Request};
use oag_imagination_reticulum::{serve, ReticulumTransport};
use tokio::sync::Mutex;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn request(id: &str) -> Request {
    Request {
        id: id.to_string(),
        target: serde_json::json!("test://reticulum-thing"),
        payload: serde_json::json!({}),
        contributions: vec![],
        answer: None,
    }
}

#[tokio::test]
async fn a_request_travels_over_a_real_reticulum_link() {
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let server_seed = "oag-imagination-loopback-server";
    let client_seed = "oag-imagination-loopback-client";

    let mut resolver = LocalResolver::new();
    resolver.insert("test://reticulum-thing", serde_json::json!("answered over reticulum"));
    let resolver: Arc<dyn oag_imagination::Resolver> = Arc::new(resolver);

    let engine = Arc::new(Mutex::new(Engine::new(
        "server-node".to_string(),
        vec![1.0; oag_imagination::PAD_DIM],
        vec![], // no neighbours: this node must answer locally, not route onward
    )));

    tokio::spawn(serve(engine, resolver, server_seed, listen.clone()));

    let client = ReticulumTransport::connect(&listen, client_seed)
        .await
        .expect("client should dial the loopback interface");

    let server_address = client.local_address(server_seed).to_hex_string();

    // The server announces every few seconds; a send right after startup would
    // race that. Retry the send rather than sleeping an arbitrary amount.
    let reply = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if let Some(reply) = oag_imagination::ReticulumCarrier::send(
                &client,
                &server_address,
                &Message::Request(request("req-1")),
            )
            .await
            {
                return reply;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("a reply should arrive over the loopback link");

    match reply {
        Message::Request(mut answered) => {
            assert_eq!(answered.answer, Some(serde_json::json!("answered over reticulum")));
            assert_eq!(answered.contributions.len(), 1, "the server node should have contributed");
            assert_eq!(answered.contributions[0].node_id, "server-node");
        }
        other => panic!("expected a request reply, got {other:?}"),
    }
}

#[tokio::test]
async fn a_send_to_an_unknown_destination_fails_softly() {
    // No server is listening on this port, so no announce can ever arrive. The
    // carrier must return None rather than panicking or hanging past its timeout.
    let listen = format!("127.0.0.1:{}", free_port());
    let client = ReticulumTransport::connect(&listen, "oag-imagination-loopback-absent")
        .await
        .expect("connecting needs no server")
        .with_announce_wait(Duration::from_secs(2));

    let started = std::time::Instant::now();
    let reply = oag_imagination::ReticulumCarrier::send(
        &client,
        // A syntactically valid address hash that nobody will ever announce.
        &"0".repeat(32),
        &Message::Request(request("req-2")),
    )
    .await;

    assert!(reply.is_none(), "an unreachable destination must fail softly, got {reply:?}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a failed send must return at its own timeout, not hang"
    );
}

#[tokio::test]
async fn a_bad_destination_address_is_a_clean_error() {
    let listen = format!("127.0.0.1:{}", free_port());
    let client = ReticulumTransport::connect(&listen, "oag-imagination-loopback-bad-address")
        .await
        .expect("connecting needs no server");

    let reply = oag_imagination::ReticulumCarrier::send(
        &client,
        "not-hex-at-all",
        &Message::Request(request("req-3")),
    )
    .await;
    assert!(reply.is_none(), "a malformed address must fail softly, got {reply:?}");
}
