//! A thin TCP socket transport for the imagination engine.
//!
//! The engine is transport-agnostic; this carries it over TCP. Reticulum, IPFS, and tunnels are
//! the same engine over a different carrier -- see `spec/03-transports.md`.
//!
//! The wire shape is a 4-byte big-endian length prefix followed by a JSON message, in both
//! directions. A message is either an imagination `Request` (the routing engine) or a
//! `PadRequest`/`PadResponse` pair (the data-pad gathering path). The final response to a
//! request carries the whole metadata chain back, so a client that connects to any node gets the
//! answer plus every contribution along the path.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Mutex;

use crate::datapad::{self, DataPad, DataPadError, FileManifest};
use crate::engine::{Engine, HandleOutcome, Request};
use crate::target::Resolver;

/// The largest message a peer will accept, matching the base project's body-size caps.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "body")]
pub enum Message {
    Request(Request),
    PadRequest(PadRequest),
    PadResponse(DataPad),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PadRequest {
    pub pad_id: String,
}

pub async fn read_message(stream: &mut TcpStream) -> std::io::Result<Message> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_MESSAGE_BYTES {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "message too large"));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub async fn write_message(stream: &mut TcpStream, message: &Message) -> std::io::Result<()> {
    let buf = serde_json::to_vec(message)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&(buf.len() as u32).to_be_bytes()).await?;
    stream.write_all(&buf).await?;
    Ok(())
}

/// A carrier for the engine's messages. The engine is transport-agnostic: TCP is implemented,
/// and Reticulum, IPFS, and tunnels implement this same trait. A carrier only has to move a
/// message to an address and return the reply -- the routing logic is the engine's, not the
/// carrier's.
pub trait Transport: Send + Sync {
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>>;
}

/// The TCP carrier: a length-prefixed JSON message over a TCP stream.
#[derive(Debug, Default)]
pub struct TcpTransport;

impl Transport for TcpTransport {
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            let mut stream = TcpStream::connect(address).await?;
            write_message(&mut stream, message).await?;
            read_message(&mut stream).await
        })
    }
}

/// Sends a request to a node over the TCP carrier and returns its response -- the answer plus the
/// metadata chain.
pub async fn send(address: &str, request: &Request) -> std::io::Result<Request> {
    send_with(&TcpTransport, address, request).await
}

/// Same as `send`, over an explicit carrier -- the seam Reticulum, IPFS, and tunnels plug into.
pub async fn send_with(
    transport: &dyn Transport,
    address: &str,
    request: &Request,
) -> std::io::Result<Request> {
    match transport.send(address, &Message::Request(request.clone())).await? {
        Message::Request(response) => Ok(response),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected a request response",
        )),
    }
}

/// The UDP carrier: one datagram in, one datagram out. UDP is connectionless, so a routed
/// request is answered by the node it reached rather than forwarded on a connection -- the
/// carrier is for the local answer, and a deployment that needs multi-hop routing over UDP
/// would forward the reply to the client itself. Datagrams are capped at the UDP payload
/// limit; a larger message is an error rather than a silent truncation.
#[derive(Debug, Default)]
pub struct UdpTransport;

impl Transport for UdpTransport {
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            let buf = serde_json::to_vec(message)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let socket = UdpSocket::bind("0.0.0.0:0").await?;
            socket.connect(address).await?;
            socket.send(&buf).await?;
            let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
            let len = socket.recv(&mut buf).await?;
            serde_json::from_slice(&buf[..len])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }
}

/// The largest UDP payload the carrier will send or receive.
pub const MAX_DATAGRAM_BYTES: usize = 65_507;

/// Serves the engine over UDP. Each datagram carries one message exchange.
pub async fn serve_udp(
    engine: Arc<Mutex<Engine>>,
    addr: &str,
    resolver: Arc<dyn Resolver>,
) -> std::io::Result<()> {
    let socket = UdpSocket::bind(addr).await?;
    serve_udp_on(engine, socket, resolver).await
}

/// Serves the engine over an already-bound UDP socket.
pub async fn serve_udp_on(
    engine: Arc<Mutex<Engine>>,
    socket: UdpSocket,
    resolver: Arc<dyn Resolver>,
) -> std::io::Result<()> {
    loop {
        let mut buf = vec![0u8; MAX_DATAGRAM_BYTES];
        let (len, peer) = socket.recv_from(&mut buf).await?;
        let message: Message = match serde_json::from_slice(&buf[..len]) {
            Ok(message) => message,
            Err(_) => continue,
        };
        if let Message::Request(mut request) = message {
            let _ = {
                let mut engine = engine.lock().await;
                engine.handle(&mut request, &*resolver).await
            };
            if let Ok(out) = serde_json::to_vec(&Message::Request(request)) {
                let _ = socket.send_to(&out, peer).await;
            }
        }
    }
}

/// Serves the engine over TCP. Each connection carries one message exchange. When the engine
/// routes a request onward, the server forwards it to the next node and returns that node's
/// response, so the client sees the whole chain. A `PadRequest` returns this node's data pad.
pub async fn serve(
    engine: Arc<Mutex<Engine>>,
    addr: &str,
    resolver: Arc<dyn Resolver>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_on(engine, listener, resolver).await
}

/// Serves the engine over an already-bound listener -- the same loop `serve` runs, exposed so a
/// caller (or a test) can bind to `127.0.0.1:0` and learn the assigned port.
pub async fn serve_on(
    engine: Arc<Mutex<Engine>>,
    listener: TcpListener,
    resolver: Arc<dyn Resolver>,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let engine = engine.clone();
        let resolver = resolver.clone();
        tokio::spawn(async move {
            let message = match read_message(&mut stream).await {
                Ok(message) => message,
                Err(_) => return,
            };
            match message {
                Message::Request(mut request) => {
                    let outcome = {
                        let mut engine = engine.lock().await;
                        engine.handle(&mut request, &*resolver).await
                    };
                    if let HandleOutcome::RouteTo(next) = outcome {
                        if let Ok(mut upstream) = TcpStream::connect(&next).await {
                            if write_message(&mut upstream, &Message::Request(request.clone()))
                                .await
                                .is_ok()
                            {
                                if let Ok(Message::Request(response)) = read_message(&mut upstream).await {
                                    let _ = write_message(&mut stream, &Message::Request(response)).await;
                                    return;
                                }
                            }
                        }
                    }
                    let _ = write_message(&mut stream, &Message::Request(request)).await;
                }
                Message::PadRequest(_) => {
                    let pad = engine.lock().await.data_pad();
                    let _ = write_message(&mut stream, &Message::PadResponse(pad)).await;
                }
                Message::PadResponse(_) => {}
            }
        });
    }
}

/// Gathers a file's shards from the given nodes over the TCP carrier and reconstructs it.
pub async fn gather_file(manifest: &FileManifest, nodes: &[String]) -> Result<Vec<u8>, DataPadError> {
    gather_file_with(&TcpTransport, manifest, nodes).await
}

/// Same as `gather_file`, over an explicit carrier.
pub async fn gather_file_with(
    transport: &dyn Transport,
    manifest: &FileManifest,
    nodes: &[String],
) -> Result<Vec<u8>, DataPadError> {
    let mut pads = Vec::new();
    for node in nodes {
        let response = transport
            .send(node, &Message::PadRequest(PadRequest { pad_id: String::new() }))
            .await
            .map_err(|e| DataPadError::Transport(e.to_string()))?;
        match response {
            Message::PadResponse(pad) => pads.push(pad),
            _ => return Err(DataPadError::NotAPad),
        }
    }
    let shards = datapad::gather(manifest, &pads)?;
    datapad::reconstruct(manifest, shards)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datapad::split_to_pads;
    use crate::engine::{Engine, PAD_DIM, Request};
    use crate::target::{FnResolver, Target};
    use tokio::net::UdpSocket;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn request(id: &str) -> Request {
        Request {
            id: id.to_string(),
            target: serde_json::json!("target"),
            payload: serde_json::json!({}),
            contributions: vec![],
            answer: None,
        }
    }

    #[tokio::test]
    async fn a_request_travels_over_tcp_and_carries_the_chain_back() {
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let engine_b = Arc::new(Mutex::new(Engine::new("node-b".to_string(), vec![0.0; PAD_DIM], vec![])));
        let handle_b = tokio::spawn(async move {
            serve_on(
                engine_b,
                listener_b,
                Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| Some(serde_json::json!("answer-from-b")))),
            )
            .await
        });

        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let engine_a = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![addr_b.to_string()],
        )));
        let handle_a = tokio::spawn(async move {
            serve_on(engine_a, listener_a, Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| None))).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let response = send(&addr_a.to_string(), &request("req-1")).await.unwrap();
        assert_eq!(response.answer, Some(serde_json::json!("answer-from-b")));
        assert_eq!(response.contributions.len(), 2);

        handle_a.abort();
        handle_b.abort();
    }

    #[tokio::test]
    async fn a_file_is_gathered_from_two_nodes_and_reconstructed() {
        let data: Vec<u8> = (0..100u8).collect();
        let pad_ids = vec!["node-a".to_string(), "node-b".to_string()];
        let (manifest, shards) = split_to_pads(&data, &pad_ids).unwrap();

        // Node A holds shard 0; node B holds shard 1.
        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let mut engine_a = Engine::new("node-a".to_string(), vec![0.0; PAD_DIM], vec![]);
        engine_a.shard = Some(shards[0].clone());
        let engine_a = Arc::new(Mutex::new(engine_a));
        let handle_a = tokio::spawn(async move {
            serve_on(engine_a, listener_a, Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| None))).await
        });

        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let mut engine_b = Engine::new("node-b".to_string(), vec![0.0; PAD_DIM], vec![]);
        engine_b.shard = Some(shards[1].clone());
        let engine_b = Arc::new(Mutex::new(engine_b));
        let handle_b = tokio::spawn(async move {
            serve_on(engine_b, listener_b, Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| None))).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let nodes = vec![addr_a.to_string(), addr_b.to_string()];
        let reconstructed = gather_file(&manifest, &nodes).await.unwrap();
        assert_eq!(reconstructed, data);

        handle_a.abort();
        handle_b.abort();
    }

    #[tokio::test]
    async fn a_request_travels_over_udp() {
        let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![0.0; PAD_DIM],
            vec![],
        )));
        let resolver = Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| {
            Some(serde_json::json!("udp-answer"))
        }));
        let handle = tokio::spawn(async move { serve_udp_on(engine, listener, resolver).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let response = send_with(&UdpTransport, &addr.to_string(), &request("req-udp")).await.unwrap();
        assert_eq!(response.answer, Some(serde_json::json!("udp-answer")));
        handle.abort();
    }

    #[tokio::test]
    async fn a_custom_carrier_can_carry_a_request() {
        use std::future::Future;
        use std::pin::Pin;

        struct InMemory;

        impl Transport for InMemory {
            fn send<'a>(
                &'a self,
                _address: &'a str,
                message: &'a Message,
            ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
                Box::pin(async move {
                    match message {
                        Message::Request(request) => {
                            let mut response = request.clone();
                            response.answer = Some(serde_json::json!("in-memory"));
                            Ok(Message::Request(response))
                        }
                        other => Ok(other.clone()),
                    }
                })
            }
        }

        let response = send_with(&InMemory, "any-address", &request("req-in-memory")).await.unwrap();
        assert_eq!(response.answer, Some(serde_json::json!("in-memory")));
    }

    #[tokio::test]
    async fn a_missing_pad_is_an_error() {
        let data = b"a file";
        let (manifest, _shards) = split_to_pads(data, &["node-a".to_string()]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // A node with no shard.
        let engine = Arc::new(Mutex::new(Engine::new("node-a".to_string(), vec![0.0; PAD_DIM], vec![])));
        let handle = tokio::spawn(async move {
            serve_on(engine, listener, Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| None))).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = gather_file(&manifest, &[addr.to_string()]).await;
        assert!(matches!(result, Err(DataPadError::CountMismatch { .. })));
        handle.abort();
    }
}
