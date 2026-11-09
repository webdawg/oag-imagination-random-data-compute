//! A thin TCP socket transport for the imagination engine.
//!
//! The engine is transport-agnostic; this carries it over TCP. Reticulum, IPFS, and tunnels are
//! the same engine over a different carrier -- see `spec/03-transports.md`.
//!
//! The wire shape is a 4-byte big-endian length prefix followed by the JSON request, in both
//! directions. The final response carries the whole metadata chain back, so a client that
//! connects to any node gets the answer plus every contribution along the path.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::engine::{Engine, HandleOutcome, Request};

/// The largest message a peer will accept, matching the base project's body-size caps.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

pub async fn read_request(stream: &mut TcpStream) -> std::io::Result<Request> {
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

pub async fn write_request(stream: &mut TcpStream, request: &Request) -> std::io::Result<()> {
    let buf =
        serde_json::to_vec(request).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    stream.write_all(&(buf.len() as u32).to_be_bytes()).await?;
    stream.write_all(&buf).await?;
    Ok(())
}

/// Sends a request to a node and returns its response -- the answer plus the metadata chain.
pub async fn send(address: &str, request: &Request) -> std::io::Result<Request> {
    let mut stream = TcpStream::connect(address).await?;
    write_request(&mut stream, request).await?;
    read_request(&mut stream).await
}

/// Serves the engine over TCP. Each connection carries one request and one response. When the
/// engine routes a request onward, the server forwards it to the next node and returns that
/// node's response, so the client sees the whole chain.
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
            let mut request = match read_request(&mut stream).await {
                Ok(request) => request,
                Err(_) => return,
            };
            let outcome = {
                let mut engine = engine.lock().await;
                engine.handle(&mut request, |r| resolver(r))
            };
            if let HandleOutcome::RouteTo(next) = outcome {
                if let Ok(mut upstream) = TcpStream::connect(&next).await {
                    if write_request(&mut upstream, &request).await.is_ok() {
                        if let Ok(response) = read_request(&mut upstream).await {
                            let _ = write_request(&mut stream, &response).await;
                            return;
                        }
                    }
                }
            }
            let _ = write_request(&mut stream, &request).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        // Node B answers any request.
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_b = listener_b.local_addr().unwrap();
        let engine_b = Arc::new(Mutex::new(Engine::new("node-b".to_string(), vec![0.0; PAD_DIM], vec![])));
        let handle_b = tokio::spawn(async move {
            serve_on(engine_b, listener_b, |_| Some(serde_json::json!("answer-from-b"))).await
        });

        // Node A routes to B.
        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let engine_a = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![addr_b.to_string()],
        )));
        let handle_a = tokio::spawn(async move {
            serve_on(engine_a, listener_a, |_| None).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let response = send(&addr_a.to_string(), &request("req-1")).await.unwrap();
        assert_eq!(response.answer, Some(serde_json::json!("answer-from-b")));
        // A contributed, then B contributed.
        assert_eq!(response.contributions.len(), 2);
        assert_eq!(response.contributions[0].node_id, "node-a");
        assert_eq!(response.contributions[1].node_id, "node-b");

        handle_a.abort();
        handle_b.abort();
    }
}
