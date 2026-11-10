//! Reticulum carrier for the imagination layer.
//!
//! The engine is transport-agnostic (`oag-imagination`'s `Transport` and `ReticulumCarrier`
//! seams); this crate is the adapter that plugs a Reticulum [`Link`] into it. It is a separate
//! crate rather than a module of `oag-imagination` so that the layer itself keeps no dependency
//! on the `reticulum` crate -- a node that runs over TCP, HTTP, or IPFS never pays for this one.
//!
//! Two halves:
//!
//! - [`ReticulumTransport`] -- the client side. Implements `oag_imagination::ReticulumCarrier`
//!   (and `Transport`), moving one [`Message`] to a destination address and returning the reply.
//! - [`serve`] / [`serve_on`] -- the server side. Announces a destination and answers requests by
//!   running them through an [`Engine`], mirroring `oag-imagination::socket::serve_on`.
//!
//! **What this explicitly is not.** Not a new application protocol: the `Message` type is the
//! engine's own, unchanged. Not a configured transport: the caller supplies the uplink address
//! and the identity seed, exactly as it supplies a TCP address elsewhere.
//!
//! # Identity
//!
//! The base project derives its Reticulum identity from the peer's Ed25519 signing key. This
//! crate has no `oag-crypto` dependency and no peer identity of its own, so it takes a seed
//! *string* instead and derives deterministically via `PrivateIdentity::new_from_name`. The seed
//! must be stable across restarts or the node's address changes and its peers can no longer reach
//! it; the natural seed is the owning `oag` peer's id, passed in by the caller.
//!
//! # Framing
//!
//! A Reticulum `data_packet` carries at most ~2 KiB once encrypted, and an engine response
//! carrying a whole contribution chain can exceed that. Messages are framed with a 4-byte
//! big-endian length prefix over the JSON body, split into [`CHUNK_SIZE`] pieces -- deliberately
//! the same wire format as the base project's own Reticulum transport, so the two could in
//! principle interop, and so a framing bug would show up identically in both.

use std::sync::Arc;
use std::time::Duration;

use oag_imagination::{
    Engine, Message, ReticulumCarrier, Resolver, Transport,
};
use reticulum::destination::link::LinkEvent;
use reticulum::destination::{DestinationDesc, DestinationName, SingleInputDestination};
use reticulum::hash::AddressHash;
use reticulum::identity::PrivateIdentity;
use reticulum::iface::tcp_client::TcpClient;
use reticulum::iface::tcp_server::TcpServer;
use reticulum::transport::{AnnounceEvent, Transport as RnTransport, TransportConfig};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{Mutex, broadcast};

/// Comfortably under a Reticulum packet's plaintext capacity once the link's own encryption
/// overhead is accounted for. Matches the base project's Reticulum transport.
const CHUNK_SIZE: usize = 1400;

/// Bounds how much a single logical message can declare itself to be, so a corrupt or hostile
/// length prefix cannot turn into an unbounded allocation. Same spirit and same value as the base
/// project's Reticulum transport.
const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

const LEN_PREFIX_BYTES: usize = 4;

/// How long a send waits to observe the destination's announce before giving up.
const ANNOUNCE_WAIT: Duration = Duration::from_secs(30);

/// How long a send waits for a freshly established link to activate.
const LINK_ACTIVATE_WAIT: Duration = Duration::from_secs(15);

/// How long a send waits for the reply after the request's last chunk goes out.
const REPLY_WAIT: Duration = Duration::from_secs(15);

/// The Reticulum destination name this crate announces and dials. A node running the imagination
/// engine over Reticulum announces `("oag-imagination", "engine")`.
pub const DESTINATION_APP_NAME: &str = "oag-imagination";
pub const DESTINATION_NAME: &str = "engine";

#[derive(Debug, thiserror::Error)]
pub enum ReticulumError {
    #[error("destination address {0} is not a valid Reticulum address hash")]
    BadAddress(String),
    #[error("no announce for {0} arrived within the timeout")]
    NoAnnounce(String),
    #[error("link to {0} did not activate within the timeout")]
    LinkNotActivated(String),
    #[error("no reply arrived within the timeout")]
    ReplyTimeout,
    #[error("a packet could not be built: {0}")]
    Packet(String),
    #[error("link closed before the reply arrived")]
    LinkClosed,
    #[error("framing error: {0}")]
    Framing(#[from] FramingError),
    #[error("serving error: {0}")]
    Serve(String),
}

#[derive(Debug, thiserror::Error)]
pub enum FramingError {
    #[error("message of {0} bytes exceeds the {MAX_MESSAGE_BYTES}-byte limit")]
    MessageTooLarge(usize),
    #[error("declared length {declared} exceeds the {MAX_MESSAGE_BYTES}-byte limit")]
    DeclaredTooLarge { declared: usize },
    #[error("length prefix is incomplete ({0} of {LEN_PREFIX_BYTES} bytes)")]
    ShortLengthPrefix(usize),
    #[error("payload is shorter than its declared length ({0} of {1})")]
    ShortPayload(usize, usize),
    #[error("a packet could not be built: {0}")]
    Packet(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Serializes `msg` to JSON and prepends a 4-byte big-endian length.
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, FramingError> {
    let json = serde_json::to_vec(msg)?;
    if json.len() > MAX_MESSAGE_BYTES {
        return Err(FramingError::MessageTooLarge(json.len()));
    }
    let mut framed = Vec::with_capacity(LEN_PREFIX_BYTES + json.len());
    framed.extend_from_slice(&(json.len() as u32).to_be_bytes());
    framed.extend_from_slice(&json);
    Ok(framed)
}

/// Parses a framed message back out of the bytes reassembled by [`Reassembler`].
pub fn decode_message<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, FramingError> {
    if bytes.len() < LEN_PREFIX_BYTES {
        return Err(FramingError::ShortLengthPrefix(bytes.len()));
    }
    let declared = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if declared > MAX_MESSAGE_BYTES {
        return Err(FramingError::DeclaredTooLarge { declared });
    }
    if bytes.len() - LEN_PREFIX_BYTES < declared {
        return Err(FramingError::ShortPayload(bytes.len() - LEN_PREFIX_BYTES, declared));
    }
    Ok(serde_json::from_slice(&bytes[LEN_PREFIX_BYTES..LEN_PREFIX_BYTES + declared])?)
}

/// Accumulates chunks received from successive [`LinkEvent::Data`] events for one link into
/// complete framed messages. One reassembler per link; the protocol is strict request/response,
/// so a single pending buffer is sufficient.
#[derive(Default)]
pub struct Reassembler {
    buffer: Vec<u8>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one chunk. Returns `Ok(Some(frame))` once a complete framed message is available.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Option<Vec<u8>>, FramingError> {
        self.buffer.extend_from_slice(chunk);
        if self.buffer.len() < LEN_PREFIX_BYTES {
            return Ok(None);
        }
        let declared = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;
        if declared > MAX_MESSAGE_BYTES {
            return Err(FramingError::DeclaredTooLarge { declared });
        }
        let total = LEN_PREFIX_BYTES + declared;
        if self.buffer.len() < total {
            return Ok(None);
        }
        let frame = self.buffer[..total].to_vec();
        self.buffer.drain(..total);
        Ok(Some(frame))
    }
}

/// The client half: moves a [`Message`] to a destination and returns the reply.
///
/// One instance holds one Reticulum transport and one long-lived announce receiver. Sends are
/// serialized because announce observation, link establishment, and the request/response
/// exchange are all stateful and share the transport's link table.
pub struct ReticulumTransport {
    transport: Arc<RnTransport>,
    /// Held from construction so an announce is never missed by subscribing too late.
    announces: Mutex<broadcast::Receiver<AnnounceEvent>>,
    send_lock: Mutex<()>,
    announce_wait: Duration,
}

impl ReticulumTransport {
    /// Builds a carrier dialing out to `uplink_tcp` (a Reticulum TCP interface address).
    ///
    /// `identity_seed` must be stable across restarts: it derives this node's Reticulum identity,
    /// and therefore its address. See the module docs.
    pub async fn connect(uplink_tcp: &str, identity_seed: &str) -> Result<Self, ReticulumError> {
        let identity = PrivateIdentity::new_from_name(identity_seed);
        let transport = RnTransport::new(TransportConfig::new("oag-imagination", &identity, false));
        transport
            .iface_manager()
            .lock()
            .await
            .spawn(TcpClient::new(uplink_tcp.to_string()), TcpClient::spawn);
        let announces = transport.recv_announces().await;
        Ok(Self {
            transport: Arc::new(transport),
            announces: Mutex::new(announces),
            send_lock: Mutex::new(()),
            announce_wait: ANNOUNCE_WAIT,
        })
    }

    /// Overrides how long a send waits for the destination's announce before giving up.
    ///
    /// A deployment on a fast link, or one that knows its peers announce frequently, may want a
    /// shorter wait than the default; a deployment reaching a node over slow radio may want a
    /// longer one.
    pub fn with_announce_wait(mut self, wait: Duration) -> Self {
        self.announce_wait = wait;
        self
    }

    /// This transport's own Reticulum address, for the destination name [`DESTINATION_NAME`].
    pub fn local_address(&self, identity_seed: &str) -> AddressHash {
        let identity = PrivateIdentity::new_from_name(identity_seed);
        reticulum::destination::SingleInputDestination::new(
            identity,
            DestinationName::new(DESTINATION_APP_NAME, DESTINATION_NAME),
        )
        .desc
        .address_hash
    }

    async fn wait_for_announce(&self, target: AddressHash) -> Result<DestinationDesc, ReticulumError> {
        let mut announces = self.announces.lock().await;
        tokio::time::timeout(self.announce_wait, async {
            loop {
                match announces.recv().await {
                    Ok(announce) => {
                        let desc = announce.destination.lock().await.desc;
                        if desc.address_hash == target {
                            return Some(desc);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .await
        .map_err(|_| ReticulumError::NoAnnounce(target.to_hex_string()))?
        .ok_or_else(|| ReticulumError::NoAnnounce(target.to_hex_string()))
    }

    async fn request(&self, destination: &str, message: &Message) -> Result<Message, ReticulumError> {
        // Serialize the whole exchange: the transport's link table and the announce receiver are
        // shared, and two concurrent sends would interleave their replies on the same link.
        let _guard = self.send_lock.lock().await;

        let target = parse_address(destination)?;
        let desc = self.wait_for_announce(target).await?;
        let link = self.transport.link(desc).await;
        let link_id = *link.lock().await.id();

        let mut events = self.transport.out_link_events();
        tokio::time::timeout(LINK_ACTIVATE_WAIT, async {
            loop {
                match events.recv().await {
                    Ok(event) if event.id == link_id && matches!(event.event, LinkEvent::Activated) => {
                        return true;
                    }
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return false,
                }
            }
        })
        .await
        .map_err(|_| ReticulumError::LinkNotActivated(link_id.to_hex_string()))?
        .then_some(())
        .ok_or_else(|| ReticulumError::LinkNotActivated(link_id.to_hex_string()))?;

        let framed = encode_message(message)?;
        for chunk in framed.chunks(CHUNK_SIZE) {
            let packet = link
                .lock()
                .await
                .data_packet(chunk)
                .map_err(|e| ReticulumError::Framing(FramingError::Packet(format!("{e:?}"))))?;
            self.transport.send_packet(packet).await;
        }

        tokio::time::timeout(REPLY_WAIT, async {
            let mut reassembler = Reassembler::new();
            loop {
                match events.recv().await {
                    Ok(event) if event.id == link_id => match event.event {
                        LinkEvent::Data(payload) => {
                            if let Some(frame) = reassembler.push(payload.as_slice())? {
                                return Ok(decode_message::<Message>(&frame)?);
                            }
                        }
                        LinkEvent::Closed => return Err(ReticulumError::LinkClosed),
                        LinkEvent::Activated => {}
                    },
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Err(ReticulumError::LinkClosed),
                }
            }
        })
        .await
        .map_err(|_| ReticulumError::ReplyTimeout)?
    }
}

impl ReticulumCarrier for ReticulumTransport {
    fn send<'a>(
        &'a self,
        destination: &'a str,
        message: &'a Message,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Message>> + Send + 'a>> {
        Box::pin(async move { self.request(destination, message).await.ok() })
    }
}

/// A carrier is a `Transport` too, so `oag_imagination::send_with` works with it. A failed send
/// (no announce, link not activated, no reply) becomes an `io::Error` rather than a panic -- the
/// engine's own carriers all degrade rather than hang.
impl Transport for ReticulumTransport {
    fn send<'a>(
        &'a self,
        address: &'a str,
        message: &'a Message,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Message>> + Send + 'a>> {
        Box::pin(async move {
            ReticulumCarrier::send(self, address, message)
                .await
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "reticulum send failed"))
        })
    }
}

/// Parses a hex-encoded Reticulum address hash, as printed by [`ReticulumTransport::local_address`].
/// Tolerates the slash-delimited `Display` form as well as plain hex.
pub fn parse_address(hex: &str) -> Result<AddressHash, ReticulumError> {
    AddressHash::new_from_hex_string(hex.trim())
        .map_err(|_| ReticulumError::BadAddress(hex.to_string()))
}

/// Serves the imagination engine over Reticulum, announcing `("oag-imagination", "engine")` on
/// `listen_tcp` and answering every request through `engine`.
///
/// Runs forever, like `oag_imagination::socket::serve_on`. Intended to be spawned alongside a
/// node's other servers.
pub async fn serve(
    engine: Arc<Mutex<Engine>>,
    resolver: Arc<dyn Resolver>,
    identity_seed: &str,
    listen_tcp: impl Into<String>,
) -> Result<(), ReticulumError> {
    let listen_tcp = listen_tcp.into();
    let identity = PrivateIdentity::new_from_name(identity_seed);
    let mut transport = RnTransport::new(TransportConfig::new("oag-imagination", &identity, false));
    transport
        .iface_manager()
        .lock()
        .await
        .spawn(
            TcpServer::new(listen_tcp, transport.iface_manager()),
            TcpServer::spawn,
        );
    let destination = transport
        .add_destination(
            identity,
            DestinationName::new(DESTINATION_APP_NAME, DESTINATION_NAME),
        )
        .await;
    let transport = Arc::new(transport);
    serve_on(engine, resolver, transport, destination).await
}

/// Serves over an already-built transport and destination -- exposed so a test can drive the loop
/// without binding a real interface, and so a caller can share one transport between this and
/// other destinations.
pub async fn serve_on(
    engine: Arc<Mutex<Engine>>,
    resolver: Arc<dyn Resolver>,
    transport: Arc<RnTransport>,
    destination: Arc<Mutex<SingleInputDestination>>,
) -> Result<(), ReticulumError> {
    tracing::info!(
        "oag-imagination-reticulum: serving at {}",
        destination.lock().await.desc.address_hash
    );

    // Announce periodically, like the base project's own Reticulum listener: a client that
    // subscribes between announcements would otherwise wait a full interval to be reachable.
    {
        let transport = transport.clone();
        let destination = destination.clone();
        tokio::spawn(async move {
            loop {
                transport.send_announce(&destination, None).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    let mut buffers: std::collections::HashMap<AddressHash, Reassembler> = std::collections::HashMap::new();
    let mut events = transport.in_link_events();

    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("oag-imagination-reticulum: lagged, dropped {skipped} link events");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return Err(ReticulumError::Serve("link events closed".into())),
        };

        match event.event {
            LinkEvent::Activated => {
                buffers.insert(event.id, Reassembler::new());
            }
            LinkEvent::Closed => {
                buffers.remove(&event.id);
            }
            LinkEvent::Data(payload) => {
                let reassembler = buffers.entry(event.id).or_default();
                let frame = match reassembler.push(payload.as_slice()) {
                    Ok(Some(frame)) => frame,
                    Ok(None) => continue,
                    Err(e) => {
                        tracing::warn!("oag-imagination-reticulum: framing error: {e}");
                        buffers.remove(&event.id);
                        continue;
                    }
                };
                let message: Message = match decode_message(&frame) {
                    Ok(message) => message,
                    Err(e) => {
                        tracing::warn!("oag-imagination-reticulum: malformed message: {e}");
                        continue;
                    }
                };

                // Only requests are answered -- the engine's own protocol is request/response and
                // the other two message kinds are node-internal.
                let reply = match message {
                    Message::Request(mut request) => {
                        let _ = {
                            let mut engine = engine.lock().await;
                            engine.handle(&mut request, resolver.as_ref()).await
                        };
                        Message::Request(request)
                    }
                    _ => continue,
                };
                {
                    match encode_message(&reply) {
                        Ok(framed) => {
                            let Some(link) = transport.find_in_link(&event.id).await else {
                                continue;
                            };
                            for chunk in framed.chunks(CHUNK_SIZE) {
                                match link.lock().await.data_packet(chunk) {
                                    Ok(packet) => transport.send_packet(packet).await,
                                    Err(e) => {
                                        tracing::warn!("oag-imagination-reticulum: packet build failed: {e:?}");
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => tracing::warn!("oag-imagination-reticulum: reply encode failed: {e}"),
                    }
                }
            }
        }
    }
}
