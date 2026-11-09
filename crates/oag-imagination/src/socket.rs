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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::datapad::{self, DataPad, DataPadError, FileManifest};
use crate::engine::{Engine, HandleOutcome, Request};

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

/// Sends a request to a node and returns its response -- the answer plus the metadata chain.
pub async fn send(address: &str, request: &Request) -> std::io::Result<Request> {
    let mut stream = TcpStream::connect(address).await?;
    write_message(&mut stream, &Message::Request(request.clone())).await?;
    match read_message(&mut stream).await? {
        Message::Request(response) => Ok(response),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "expected a request response",
        )),
    }
}

/// Serves the engine over TCP. Each connection carries one message exchange. When the engine
/// routes a request onward, the server forwards it to the next node and returns that node's
/// response, so the client sees the whole chain. A `PadRequest` returns this node's data pad.
pub async fn serve<F>(engine: Arc<Mutex<Engine>>, addr: &str, resolver: F) -> std::io::Result<()>
where
    F: Fn(&Request) -> Option<serde_json::Value> + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    serve_on(engine, listener, resolver).await
}

/// Serves the engine over an already-bound listener -- the same loop `serve` runs, exposed so a
/// caller (or a test) can bind to `127.0.0.1:0` and learn the assigned port.
pub async fn serve_on<F>(
    engine: Arc<Mutex<Engine>>,
    listener: TcpListener,
    resolver: F,
) -> std::io::Result<()>
where
    F: Fn(&Request) -> Option<serde_json::Value> + Send + Sync + 'static,
{
    let resolver = Arc::new(resolver);
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
                        engine.handle(&mut request, |r| resolver(r))
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

/// Gathers a file's shards from the given nodes and reconstructs it. Each node is asked for its
/// data pad; the shards are collected in manifest order and reconstructed.
pub async fn gather_file(manifest: &FileManifest, nodes: &[String]) -> Result<Vec<u8>, DataPadError> {
    let mut pads = Vec::new();
    for node in nodes {
        let mut stream = TcpStream::connect(node)
            .await
            .map_err(|_| DataPadError::NodeUnreachable(node.clone()))?;
        write_message(&mut stream, &Message::PadRequest(PadRequest { pad_id: String::new() }))
            .await
            .map_err(|e| DataPadError::Transport(e.to_string()))?;
        match read_message(&mut stream).await.map_err(|e| DataPadError::Transport(e.to_string()))? {
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
            serve_on(engine_b, listener_b, |_| Some(serde_json::json!("answer-from-b"))).await
        });

        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let engine_a = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![addr_b.to_string()],
        )));
        let handle_a = tokio::spawn(async move { serve_on(engine_a, listener_a, |_| None).await });

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
        let handle_a = tokio::spawn(async move { serve_on(engine_a, listener_a, |_| None).await });

        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let mut engine_b = Engine::new("node-b".to_string(), vec![0.0; PAD_DIM], vec![]);
        engine_b.shard = Some(shards[1].clone());
        let engine_b = Arc::new(Mutex::new(engine_b));
        let handle_b = tokio::spawn(async move { serve_on(engine_b, listener_b, |_| None).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let nodes = vec![addr_a.to_string(), addr_b.to_string()];
        let reconstructed = gather_file(&manifest, &nodes).await.unwrap();
        assert_eq!(reconstructed, data);

        handle_a.abort();
        handle_b.abort();
    }

    #[tokio::test]
    async fn a_missing_pad_is_an_error() {
        let data = b"a file";
        let (manifest, _shards) = split_to_pads(data, &["node-a".to_string()]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // A node with no shard.
        let engine = Arc::new(Mutex::new(Engine::new("node-a".to_string(), vec![0.0; PAD_DIM], vec![])));
        let handle = tokio::spawn(async move { serve_on(engine, listener, |_| None).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let result = gather_file(&manifest, &[addr.to_string()]).await;
        assert!(matches!(result, Err(DataPadError::CountMismatch { .. })));
        handle.abort();
    }
}
