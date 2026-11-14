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
use crate::engine::{Engine, HandleOutcome, HopOutcome, Request};
use crate::target::Resolver;

/// The largest message a peer will accept, matching the base project's body-size caps.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// How long an inbound connection may take to send its message before it is dropped.
///
/// Without this, a peer that completes the TCP handshake and then sends nothing holds a spawned
/// task and a socket for as long as it likes, and enough of those exhaust the process -- a
/// slowloris-shaped availability hole with no authentication in front of it. Same posture as the
/// base project's outbound request bound (`spec/18`) and `oag-mesh`'s bound on outbound requests.
pub const INBOUND_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// How long to wait for a forwarded request's upstream reply, or for the connect that precedes
/// it, before giving up on that hop. A black-holed address otherwise holds the task for the
/// operating system's connect timeout, which is minutes.
pub const UPSTREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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

/// A test HTTP endpoint: reads one POST body and returns the message with an answer filled in.
#[cfg(test)]
async fn http_endpoint(mut stream: TcpStream) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf[..n]).to_string();
    // The body starts after the first blank line.
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
    let mut message: Message = serde_json::from_str(body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if let Message::Request(ref mut request) = message {
        request.answer = Some(serde_json::json!("over http"));
    }
    let out = serde_json::to_vec(&message)?;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        out.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(&out).await?;
    Ok(())
}

/// A carrier that tunnels another carrier through an HTTP CONNECT proxy: connect to the proxy,
/// ask it to open a tunnel to the target, then run the inner carrier's exchange over it. This is
/// the "tunnel existing protocols" carrier -- the inner carrier is unchanged; only the path to
/// the target changes.
pub struct ProxiedTransport {
    proxy: String,
}

impl std::fmt::Debug for ProxiedTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxiedTransport").field("proxy", &self.proxy).finish()
    }
}

impl ProxiedTransport {
    pub fn new(proxy: impl Into<String>) -> Self {
        Self { proxy: proxy.into() }
    }

    /// Strips a scheme from an address, leaving `host:port` for the CONNECT request.
    fn target(address: &str) -> &str {
        address
            .trim_start_matches("http://")
            .trim_start_matches("https://")
    }
}

impl Transport for ProxiedTransport {
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            let target = Self::target(address).to_string();
            let mut stream = TcpStream::connect(&self.proxy).await?;
            let connect = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n");
            stream.write_all(connect.as_bytes()).await?;
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await?;
            let response = String::from_utf8_lossy(&buf[..n]);
            if !response.starts_with("HTTP/1.1 200") && !response.starts_with("HTTP/1.0 200") {
                return Err(std::io::Error::other("tunnel was not established"));
            }
            // The tunnel is up; run the inner carrier's exchange over it.
            write_message(&mut stream, message).await?;
            read_message(&mut stream).await
        })
    }
}

/// A seam for publishing a message to IPFS and getting its CID. The base project's
/// `PinningProvider` (or any IPFS client) adapts to this trait.
pub trait IpfsPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;
}

/// Publishes via an IPFS HTTP API (`POST /api/v0/add`).
#[derive(Debug)]
pub struct HttpIpfsPublisher {
    api: String,
    client: reqwest::Client,
}

impl HttpIpfsPublisher {
    pub fn new(api: impl Into<String>) -> Self {
        Self { api: api.into(), client: reqwest::Client::new() }
    }
}

impl IpfsPublisher for HttpIpfsPublisher {
    fn publish<'a>(
        &'a self,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(message).ok()?;
            let part = reqwest::multipart::Part::bytes(bytes).file_name("message");
            let form = reqwest::multipart::Form::new().part("file", part);
            let response = self
                .client
                .post(format!("{}/api/v0/add", self.api.trim_end_matches('/')))
                .multipart(form)
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            let body = response.text().await.ok()?;
            let parsed: serde_json::Value = serde_json::from_str(&body).ok()?;
            parsed.get("Hash").and_then(|h| h.as_str()).map(|s| s.to_string())
        })
    }
}

/// A carrier that publishes the message to IPFS and returns the CID as the answer -- the
/// write half of the IPFS carrier, against `IpfsTransport`'s read half.
pub struct PublishingIpfsTransport<P: IpfsPublisher> {
    publisher: P,
}

impl<P: IpfsPublisher> std::fmt::Debug for PublishingIpfsTransport<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishingIpfsTransport").finish()
    }
}

impl<P: IpfsPublisher> PublishingIpfsTransport<P> {
    pub fn new(publisher: P) -> Self {
        Self { publisher }
    }
}

impl<P: IpfsPublisher> Transport for PublishingIpfsTransport<P> {
    fn send<'a>(
        &'a self,
        _address: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            let cid = self
                .publisher
                .publish(message)
                .await
                .ok_or_else(|| std::io::Error::other("publish failed"))?;
            let mut response = message.clone();
            if let Message::Request(ref mut request) = response {
                request.answer = Some(serde_json::json!({ "cid": cid }));
            }
            Ok(response)
        })
    }
}

/// A carrier that retrieves a message from IPFS by CID: the address is the CID, and the
/// message published at that CID is the response. Uses the same `CidFetcher` seam as
/// `CidResolver`, so the base project's `PinningProvider` plugs in without the IPFS crate.
pub struct IpfsTransport<F: crate::target::CidFetcher> {
    fetcher: F,
}

impl<F: crate::target::CidFetcher> IpfsTransport<F> {
    pub fn new(fetcher: F) -> Self {
        Self { fetcher }
    }
}

impl<F: crate::target::CidFetcher> std::fmt::Debug for IpfsTransport<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpfsTransport").finish()
    }
}

impl<F: crate::target::CidFetcher> Transport for IpfsTransport<F> {
    fn send<'a>(
        &'a self,
        address: &'a str,
        _message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            let bytes = self
                .fetcher
                .fetch(address)
                .await
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "CID not found"))?;
            serde_json::from_slice(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }
}

/// An HTTP carrier: POST the message as JSON to an endpoint and read the JSON reply. This is
/// the carrier that lets the engine talk to an HTTP API -- the base project's REST surface --
/// rather than a raw socket. The endpoint is `<address>/oag-imagination/v1/message`.
#[derive(Debug, Default)]
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new() -> Self {
        Self { client: reqwest::Client::new() }
    }
}

impl Transport for HttpTransport {
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            let url = format!("{}/oag-imagination/v1/message", address.trim_end_matches('/'));
            let response = self
                .client
                .post(&url)
                .json(message)
                .send()
                .await
                .map_err(std::io::Error::other)?;
            if !response.status().is_success() {
                return Err(std::io::Error::other(format!(
                    "endpoint returned {}",
                    response.status()
                )));
            }
            response
                .json()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }
}

/// A seam for a Reticulum carrier. The base project's Reticulum transport
/// (`oag-reticulum`) adapts to this trait; the imagination layer does not depend on the
/// Reticulum crate. A carrier only moves a message to a destination and returns the reply
/// -- the routing logic is the engine's, not the carrier's.
///
/// The base project's Reticulum transport carries `oag/sync`'s hello/events/peers calls
/// today; the adapter for this trait is the same transport with a different handler, which
/// is why the seam exists here rather than a Reticulum-specific carrier.
pub trait ReticulumCarrier: Send + Sync {
    fn send<'a>(
        &'a self,
        destination: &'a str,
        message: &'a Message,
    ) -> Pin<Box<dyn Future<Output = Option<Message>> + Send + 'a>>;
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

/// The default cap on concurrently served connections.
///
/// Timeouts make each connection finite, but not their number: enough simultaneous slow clients
/// still exhaust tasks and sockets. This is an availability backstop rather than a throughput
/// tuning knob, which is why it is generous.
pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// The bounds the serving loop applies, so a caller can retune them and a test can make them small
/// enough to observe.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub inbound_read: std::time::Duration,
    pub upstream: std::time::Duration,
    pub max_connections: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            inbound_read: INBOUND_READ_TIMEOUT,
            upstream: UPSTREAM_TIMEOUT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

/// Serves the engine over an already-bound listener -- the same loop `serve` runs, exposed so a
/// caller (or a test) can bind to `127.0.0.1:0` and learn the assigned port.
pub async fn serve_on(
    engine: Arc<Mutex<Engine>>,
    listener: TcpListener,
    resolver: Arc<dyn Resolver>,
) -> std::io::Result<()> {
    serve_on_with_limits(engine, listener, resolver, Limits::default()).await
}

/// As [`serve_on`], with explicit [`Limits`].
pub async fn serve_on_with_limits(
    engine: Arc<Mutex<Engine>>,
    listener: TcpListener,
    resolver: Arc<dyn Resolver>,
    limits: Limits,
) -> std::io::Result<()> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(limits.max_connections));
    loop {
        // Acquire before accepting, so a full server stops taking new connections rather than
        // accepting them into a queue nobody is reading. The permit moves into the spawned task
        // and is held until that connection finishes.
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            return Ok(());
        };
        let (mut stream, _) = listener.accept().await?;
        let engine = engine.clone();
        let resolver = resolver.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let message = match tokio::time::timeout(limits.inbound_read, read_message(&mut stream)).await {
                Ok(Ok(message)) => message,
                // A read error or a timeout: drop the connection. Neither is worth a log line
                // per attempt, because a scanning peer can produce unlimited both.
                _ => return,
            };
            match message {
                Message::Request(mut request) => {
                    let outcome = {
                        let mut engine = engine.lock().await;
                        engine.handle(&mut request, &*resolver).await
                    };
                    if let HandleOutcome::RouteTo(next) = outcome {
                        // Both the connect and the reply are bounded. On timeout the request is
                        // returned unanswered rather than left hanging, which is the same
                        // "degrade, don't drop" posture the rest of this crate takes: the caller
                        // gets its request back and can try another hop.
                        if forward_and_observe(&next, &request, &mut stream, &engine, limits.upstream)
                            .await
                            .is_some()
                        {
                            return;
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

/// How many pad requests may be in flight at once while gathering a file.
///
/// A file split across the default `DEFAULT_SHARD_COUNT` (1000) pads would otherwise cost a
/// thousand sequential round-trips -- one per pad, each waiting for the previous. The gather is
/// all-or-nothing (`datapad::gather` needs every shard), so fanning out is the only lever; this
/// bounds how many peers are asked at once so a large manifest does not mean a thousand
/// simultaneous connections.
pub const MAX_CONCURRENT_GATHERS: usize = 32;

/// Gathers a file's shards from the given nodes over the TCP carrier and reconstructs it.
pub async fn gather_file(manifest: &FileManifest, nodes: &[String]) -> Result<Vec<u8>, DataPadError> {
    gather_file_with(Arc::new(TcpTransport), manifest, nodes).await
}

/// Same as `gather_file`, over an explicit carrier.
///
/// The requests are fanned out with at most [`MAX_CONCURRENT_GATHERS`] in flight, because the
/// sequential form cost one round-trip per pad and a thousand-pad file is the design case. The
/// carrier is taken as an `Arc` so the spawned requests can share it; the manifest order is
/// restored by `datapad::gather`, which sorts by the manifest's `pad_ids`.
pub async fn gather_file_with(
    transport: Arc<dyn Transport>,
    manifest: &FileManifest,
    nodes: &[String],
) -> Result<Vec<u8>, DataPadError> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_GATHERS));
    let mut handles = tokio::task::JoinSet::new();
    for node in nodes {
        let transport = transport.clone();
        let node = node.clone();
        let semaphore = semaphore.clone();
        handles.spawn(async move {
            let _permit = semaphore
                .acquire()
                .await
                .map_err(|e| DataPadError::Transport(e.to_string()))?;
            let response = transport
                .send(&node, &Message::PadRequest(PadRequest { pad_id: String::new() }))
                .await
                .map_err(|e| DataPadError::Transport(e.to_string()))?;
            Ok::<Message, DataPadError>(response)
        });
    }
    let mut pads = Vec::with_capacity(nodes.len());
    while let Some(result) = handles.join_next().await {
        // Two layers: the outer is the task's own join, the inner is the request's result.
        let message = result
            .map_err(|e| DataPadError::Transport(e.to_string()))??;
        match message {
            Message::PadResponse(pad) => pads.push(pad),
            _ => return Err(DataPadError::NotAPad),
        }
    }
    let shards = datapad::gather(manifest, &pads)?;
    datapad::reconstruct(manifest, shards)
}


/// Forwards `request` to `next`, relays the reply to `client`, and tells the engine how the hop
/// went. Returns whether a reply was relayed.
///
/// The outcome is the engine's only learning signal, and the carrier is the only place that knows
/// it: an answered chain strengthens the neighbour that carried it, an unreachable one weakens it,
/// and a forwarded-but-unanswered one is left alone. Without this the pad is a rich-get-richer
/// random walk that locks onto whichever neighbour the early draws favoured -- see `HopOutcome`.
async fn forward_and_observe(
    next: &str,
    request: &Request,
    client: &mut TcpStream,
    engine: &Arc<Mutex<Engine>>,
    timeout: std::time::Duration,
) -> Option<()> {
    let connected = tokio::time::timeout(timeout, TcpStream::connect(next)).await;
    let Ok(Ok(mut upstream)) = connected else {
        engine.lock().await.observe_hop(next, HopOutcome::Failed);
        return None;
    };

    let sent = tokio::time::timeout(
        timeout,
        write_message(&mut upstream, &Message::Request(request.clone())),
    )
    .await;
    if !sent.as_ref().is_ok_and(|r| r.is_ok()) {
        engine.lock().await.observe_hop(next, HopOutcome::Failed);
        return None;
    }

    let replied = tokio::time::timeout(timeout, read_message(&mut upstream)).await;
    let Ok(Ok(Message::Request(response))) = replied else {
        engine.lock().await.observe_hop(next, HopOutcome::Failed);
        return None;
    };

    let outcome = if response.answer.is_some() {
        HopOutcome::Answered
    } else {
        HopOutcome::Forwarded
    };
    engine.lock().await.observe_hop(next, outcome);
    let _ = write_message(client, &Message::Request(response)).await;
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datapad::split_to_pads;
    use crate::engine::{Engine, PAD_DIM, Request};
    use crate::target::{CidFetcher, FnResolver, Target};
    use std::future::Future;
    use std::pin::Pin;
    use tokio::net::{TcpListener, TcpStream, UdpSocket};
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

    /// A peer that completes the handshake and then sends nothing used to hold a spawned task and
    /// a socket for as long as it liked, and enough of those exhaust the process. The serving loop
    /// must close the connection at its inbound timeout instead.
    #[tokio::test]
    async fn a_client_that_sends_nothing_is_dropped_at_the_inbound_timeout() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![],
        )));
        let handle = tokio::spawn(async move {
            serve_on_with_limits(
                engine,
                listener,
                Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| None)),
                Limits {
                    inbound_read: std::time::Duration::from_millis(50),
                    upstream: std::time::Duration::from_millis(50),
                    max_connections: DEFAULT_MAX_CONNECTIONS,
                },
            )
            .await
        });

        // Handshake completes, then the client stalls.
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let closed = tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut buf)).await;
        assert!(closed.is_ok(), "the server must close a client that never sends");
        assert_eq!(closed.unwrap().unwrap(), 0, "a closed connection reads as EOF");

        handle.abort();
    }

    /// Timeouts make each connection finite; this makes their *number* finite. A full server stops
    /// accepting rather than queueing connections nobody is reading, so a second client waits for
    /// the first to finish instead of being served concurrently.
    #[tokio::test]
    async fn a_full_server_does_not_serve_a_second_connection_concurrently() {
        use tokio::io::AsyncWriteExt;
        use std::time::Instant;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![],
        )));
        let handle = tokio::spawn(async move {
            serve_on_with_limits(
                engine,
                listener,
                Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| Some(
                    serde_json::json!("ok"),
                ))),
                Limits {
                    inbound_read: std::time::Duration::from_millis(300),
                    upstream: std::time::Duration::from_millis(300),
                    max_connections: 1,
                },
            )
            .await
        });

        // The first connection takes the only slot and then stalls.
        let mut staller = TcpStream::connect(addr).await.unwrap();
        let started = Instant::now();
        let answered =
            tokio::time::timeout(std::time::Duration::from_secs(5), send(&addr.to_string(), &request("req-2")))
                .await;
        let elapsed = started.elapsed();
        assert!(answered.is_ok(), "the second client must eventually be served");
        assert!(
            elapsed >= std::time::Duration::from_millis(200),
            "the second client must have waited for the stalled one, served in {elapsed:?}"
        );
        let _ = staller.shutdown().await;
        handle.abort();
    }

    #[tokio::test]
    async fn an_unreachable_next_hop_still_gets_a_response_not_a_hang() {
        // A port that was bound and then dropped: connecting is refused immediately, so the
        // server falls through to answering the client itself rather than hanging.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap().to_string();
        drop(dead);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = Arc::new(Mutex::new(Engine::new(
            "node-a".to_string(),
            vec![1.0; PAD_DIM],
            vec![dead_addr.clone()],
        )));
        let handle = tokio::spawn(async move {
            serve_on(
                engine,
                listener,
                Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| None)),
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            send(&addr.to_string(), &request("req-dead")),
        )
        .await
        .expect("an unreachable hop must not hang the client")
        .unwrap();
        // No answer -- the hop never came back -- but the chain still carries this node's own
        // contribution, so the client can see how far the request got.
        assert_eq!(response.answer, None);
        assert_eq!(response.contributions.len(), 1);
        assert_eq!(response.contributions[0].node_id, "node-a");
        handle.abort();
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

    /// The fan-out. A sequential gather costs one round-trip per pad; the bound caps how many are
    /// in flight. A tracking carrier records the peak concurrency, so the test asserts both that
    /// every shard still arrives and that the bound holds.
    #[tokio::test]
    async fn gathering_fans_out_with_a_bounded_concurrency() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct TrackingTransport {
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
            pads: std::collections::HashMap<String, DataPad>,
        }

        impl Transport for TrackingTransport {
            fn send<'a>(
                &'a self,
                address: &'a str,
                _message: &'a Message,
            ) -> Pin<Box<dyn Future<Output = std::io::Result<Message>> + Send + 'a>> {
                let pad = self.pads.get(address).cloned();
                let active = self.active.clone();
                let peak = self.peak.clone();
                Box::pin(async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    match pad {
                        Some(p) => Ok(Message::PadResponse(p)),
                        None => Err(std::io::Error::new(std::io::ErrorKind::NotFound, "no pad")),
                    }
                })
            }
        }

        // Twice the concurrency bound, so a sequential gather would never overlap and an
        // unbounded one would exceed it.
        let count = MAX_CONCURRENT_GATHERS * 2;
        let data: Vec<u8> = (0..100u8).collect();
        let pad_ids: Vec<String> = (0..count).map(|i| format!("node-{i}")).collect();
        let (manifest, shards) = datapad::split_to_pads(&data, &pad_ids).unwrap();

        let mut pads = std::collections::HashMap::new();
        for (id, shard) in pad_ids.iter().zip(shards.iter()) {
            pads.insert(
                id.clone(),
                DataPad { pad_id: id.clone(), values: vec![], shard: Some(shard.clone()) },
            );
        }
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(TrackingTransport {
            active: active.clone(),
            peak: peak.clone(),
            pads,
        });

        let reconstructed = gather_file_with(transport, &manifest, &pad_ids).await.unwrap();
        assert_eq!(reconstructed, data, "the fan-out must still gather every shard");
        assert_eq!(
            peak.load(Ordering::SeqCst),
            MAX_CONCURRENT_GATHERS,
            "the fan-out must be bounded by MAX_CONCURRENT_GATHERS"
        );
    }

    #[tokio::test]
    async fn a_message_is_published_to_ipfs_and_returns_its_cid() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // A minimal IPFS HTTP API: accepts the add and returns a Hash.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf).await;
            let body = "{\"Hash\": \"QmPublished\"}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });

        let transport =
            PublishingIpfsTransport::new(HttpIpfsPublisher::new(format!("http://{addr}")));
        let response = transport
            .send("ignored", &Message::Request(request("req-pub")))
            .await
            .unwrap();
        let Message::Request(response) = response else {
            panic!("expected a request message");
        };
        assert_eq!(response.answer, Some(serde_json::json!({ "cid": "QmPublished" })));
        let _ = handle.await;
    }

    #[tokio::test]
    async fn a_message_travels_over_http() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = http_endpoint(stream).await;
        });

        let transport = HttpTransport::new();
        let response = transport
            .send(&format!("http://{addr}"), &Message::Request(request("req-http")))
            .await
            .unwrap();
        let Message::Request(response) = response else {
            panic!("expected a request message");
        };
        assert_eq!(response.answer, Some(serde_json::json!("over http")));
        let _ = handle.await;
    }

    #[tokio::test]
    async fn a_message_travels_through_an_http_connect_tunnel() {
        // The target answers the message.
        let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        let target_handle = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.unwrap();
            let Message::Request(mut request) = read_message(&mut stream).await.unwrap() else {
                return;
            };
            request.answer = Some(serde_json::json!("via tunnel"));
            write_message(&mut stream, &Message::Request(request)).await.unwrap();
        });

        // The proxy: read CONNECT, connect to the target, establish, forward.
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();
        let proxy_handle = tokio::spawn(async move {
            let (mut client, _) = proxy_listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = client.read(&mut buf).await;
            let mut target = TcpStream::connect(target_addr).await.unwrap();
            let _ = client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await;
            let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
        });

        let transport = ProxiedTransport::new(proxy_addr.to_string());
        let response = transport
            .send(&format!("http://{target_addr}"), &Message::Request(request("req-tunnel")))
            .await
            .unwrap();
        let Message::Request(response) = response else {
            panic!("expected a request message");
        };
        assert_eq!(response.answer, Some(serde_json::json!("via tunnel")));
        let _ = target_handle.await;
        let _ = proxy_handle.await;
    }

    #[tokio::test]
    async fn a_message_is_retrieved_from_ipfs_by_cid() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A minimal IPFS gateway serving the published message at /ipfs/<cid>.
        let published = Message::Request(request("req-ipfs"));
        let body = serde_json::to_vec(&published).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(&body).await;
        });

        #[derive(Debug)]
        struct GatewayFetcher {
            gateway: String,
        }

        impl CidFetcher for GatewayFetcher {
            fn fetch<'a>(
                &'a self,
                cid: &'a str,
            ) -> Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'a>> {
                Box::pin(async move {
                    let url = format!("{}/ipfs/{}", self.gateway, cid);
                    let response = reqwest::get(url).await.ok()?;
                    if !response.status().is_success() {
                        return None;
                    }
                    Some(response.bytes().await.ok()?.to_vec())
                })
            }
        }

        let transport = IpfsTransport::new(GatewayFetcher { gateway: format!("http://{addr}") });
        let fetched = transport.send("QmPublished", &published).await.unwrap();
        let Message::Request(request) = fetched else {
            panic!("expected a request message");
        };
        assert_eq!(request.id, "req-ipfs");
        handle.await.unwrap();
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
