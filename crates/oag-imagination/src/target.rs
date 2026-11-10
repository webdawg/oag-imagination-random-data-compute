//! Request targets and resolvers.
//!
//! A request can be anything: connect to a legacy server (https), a communication request to a
//! node, an API spend, storing information, a CID, a query. The possibilities are infinite, so
//! the target is typed rather than a single hardcoded shape, and the resolver is pluggable --
//! a node answers the targets it knows how to answer and routes the rest.
//!
//! `Target::infer` guesses a kind from a JSON value: an `http://`/`https://` string is a `Url`, a
//! `Qm...`/`bafy...`/`bafk...` string is a `Cid`, an `oagp_...` string is a `Peer`, and anything
//! else is a `Query` or `Other`. The guess is a convenience, not a validation -- a resolver can
//! always treat the target as `Other` and handle it itself.
//!
//! Resolvers are async because a remote target (a URL, a CID, a peer) is fetched, not looked up.

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

/// The kind of thing a request is asking for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Target {
    /// A legacy server URL (`https://...`, `http://...`).
    Url(String),
    /// An IPFS CID.
    Cid(String),
    /// A peer id (`oagp_...`).
    Peer(String),
    /// A query string.
    Query(String),
    /// A key in a store.
    Store(String),
    /// Anything else -- the possibilities are infinite.
    Other(serde_json::Value),
}

impl Target {
    /// Infers a target kind from a JSON value.
    pub fn infer(value: &serde_json::Value) -> Target {
        match value {
            serde_json::Value::String(s) => {
                if s.starts_with("http://") || s.starts_with("https://") {
                    Target::Url(s.clone())
                } else if s.starts_with("Qm") || s.starts_with("bafy") || s.starts_with("bafk") {
                    Target::Cid(s.clone())
                } else if s.starts_with("oagp_") {
                    Target::Peer(s.clone())
                } else {
                    Target::Query(s.clone())
                }
            }
            other => Target::Other(other.clone()),
        }
    }
}

/// Answers a target, or returns `None` to let the engine route it onward.
pub trait Resolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>>;
}

/// A resolver backed by a simple map -- a store of keys to values. Answers `Target::Store` and
/// `Target::Query` when the key is present.
#[derive(Debug, Default)]
pub struct LocalResolver {
    entries: std::collections::HashMap<String, serde_json::Value>,
}

impl LocalResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.entries.insert(key.into(), value);
    }
}

impl Resolver for LocalResolver {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        _payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let key = match target {
                Target::Store(k) | Target::Query(k) => k,
                _ => return None,
            };
            self.entries.get(key).cloned()
        })
    }
}

/// Tries each resolver in order, returning the first answer.
#[derive(Default)]
pub struct CompositeResolver {
    resolvers: Vec<Box<dyn Resolver>>,
}

impl std::fmt::Debug for CompositeResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeResolver").field("resolvers", &self.resolvers.len()).finish()
    }
}

impl CompositeResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, resolver: Box<dyn Resolver>) {
        self.resolvers.push(resolver);
    }
}

impl Resolver for CompositeResolver {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            for resolver in &self.resolvers {
                if let Some(answer) = resolver.resolve(target, payload).await {
                    return Some(answer);
                }
            }
            None
        })
    }
}

/// Answers `Target::Url` by fetching it over HTTP.
pub struct UrlResolver {
    client: reqwest::Client,
}

impl std::fmt::Debug for UrlResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UrlResolver").finish()
    }
}

impl Default for UrlResolver {
    fn default() -> Self {
        Self { client: reqwest::Client::new() }
    }
}

impl UrlResolver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Resolver for UrlResolver {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        _payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let Target::Url(url) = target else {
                return None;
            };
            let response = self.client.get(url).send().await.ok()?;
            let status = response.status();
            if !status.is_success() {
                return Some(serde_json::json!({ "status": status.as_u16(), "body": null }));
            }
            let body = response.text().await.ok()?;
            Some(serde_json::json!({ "status": status.as_u16(), "body": body }))
        })
    }
}

/// Adapts a synchronous closure to the `Resolver` trait -- for tests and simple cases where a
/// full resolver is more than needed.
pub struct FnResolver<F>(pub F);

impl<F> std::fmt::Debug for FnResolver<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnResolver").finish()
    }
}

impl<F> Resolver for FnResolver<F>
where
    F: Fn(&Target, &serde_json::Value) -> Option<serde_json::Value> + Send + Sync,
{
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move { (self.0)(target, payload) })
    }
}

/// A seam for fetching a CID's bytes. The imagination layer does not depend on the IPFS crate;
/// the base project's `PinningProvider` (or any IPFS client) adapts to this trait, and
/// `HttpGatewayFetcher` is the light built-in implementation.
pub trait CidFetcher: Send + Sync {
    fn fetch<'a>(
        &'a self,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'a>>;
}

/// Answers `Target::Cid` by fetching the CID through a `CidFetcher`.
pub struct CidResolver<F: CidFetcher> {
    fetcher: F,
}

impl<F: CidFetcher> CidResolver<F> {
    pub fn new(fetcher: F) -> Self {
        Self { fetcher }
    }
}

impl<F: CidFetcher> std::fmt::Debug for CidResolver<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CidResolver").finish()
    }
}

impl<F: CidFetcher> Resolver for CidResolver<F> {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        _payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let Target::Cid(cid) = target else {
                return None;
            };
            let bytes = self.fetcher.fetch(cid).await?;
            Some(serde_json::json!({
                "cid": cid,
                "length": bytes.len(),
                "body": String::from_utf8_lossy(&bytes),
            }))
        })
    }
}

/// The light built-in `CidFetcher`: fetch a CID from an IPFS HTTP gateway
/// (`<gateway>/ipfs/<cid>`). A deployment that runs its own node can adapt the base project's
/// `PinningProvider` to `CidFetcher` instead.
pub struct HttpGatewayFetcher {
    gateway: String,
    client: reqwest::Client,
}

impl std::fmt::Debug for HttpGatewayFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpGatewayFetcher").field("gateway", &self.gateway).finish()
    }
}

impl HttpGatewayFetcher {
    pub fn new(gateway: impl Into<String>) -> Self {
        Self { gateway: gateway.into(), client: reqwest::Client::new() }
    }
}

impl CidFetcher for HttpGatewayFetcher {
    fn fetch<'a>(
        &'a self,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            let url = format!("{}/ipfs/{}", self.gateway.trim_end_matches('/'), cid);
            let response = self.client.get(url).send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            Some(response.bytes().await.ok()?.to_vec())
        })
    }
}

/// A seam for fetching a CID from a peer. The base project's transfer protocol
/// (`POST /oag/transfer/v1/fetch`) adapts to this trait; `HttpPeerFetcher` is the built-in
/// implementation, which signs the request with the node's own identity.
pub trait PeerFetcher: Send + Sync {
    fn fetch<'a>(
        &'a self,
        peer: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'a>>;
}

/// Answers `Target::Peer` by fetching the request payload's CID from that peer.
pub struct PeerResolver<F: PeerFetcher> {
    fetcher: F,
}

impl<F: PeerFetcher> PeerResolver<F> {
    pub fn new(fetcher: F) -> Self {
        Self { fetcher }
    }
}

impl<F: PeerFetcher> std::fmt::Debug for PeerResolver<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerResolver").finish()
    }
}

impl<F: PeerFetcher> Resolver for PeerResolver<F> {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let Target::Peer(peer) = target else {
                return None;
            };
            let cid = payload.as_str()?;
            let bytes = self.fetcher.fetch(peer, cid).await?;
            Some(serde_json::json!({
                "peer": peer,
                "cid": cid,
                "length": bytes.len(),
                "body": String::from_utf8_lossy(&bytes),
            }))
        })
    }
}

pub const TRANSFER_REQUEST_DOMAIN: &str = "OAG:TRANSFER_REQUEST:v1:";

#[derive(Debug, Serialize)]
struct UnsignedTransferRequest {
    cid: String,
    client_peer_id: String,
    client_public_key: String,
    client_nonce: String,
    requested_at: i64,
    #[serde(default)]
    relay_hops_remaining: u8,
    #[serde(default)]
    relay_path: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TransferRequest {
    #[serde(flatten)]
    unsigned: UnsignedTransferRequest,
    signature: String,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
        as i64
}

/// Builds a signed transfer request, matching the base project's `build_and_sign_request`.
fn build_transfer_request(identity: &oag_crypto::PeerIdentity, cid: &str, nonce: &[u8; 32]) -> TransferRequest {
    let unsigned = UnsignedTransferRequest {
        cid: cid.to_string(),
        client_peer_id: identity.peer_id().to_string(),
        client_public_key: hex::encode(identity.verifying_key().to_bytes()),
        client_nonce: hex::encode(nonce),
        requested_at: now_unix(),
        relay_hops_remaining: 0,
        relay_path: vec![],
    };
    let canonical = oag_core::canonical_json_bytes(&unsigned).expect("UnsignedTransferRequest always serializes");
    let signature =
        oag_crypto::sign_with_domain(identity.signing_key(), TRANSFER_REQUEST_DOMAIN, &canonical);
    TransferRequest { unsigned, signature: hex::encode(signature.to_bytes()) }
}

/// The built-in `PeerFetcher`: `POST /oag/transfer/v1/fetch` with a request signed by this node's
/// identity, returning the content body. The receipt travels in a header; this fetcher returns the
/// bytes and leaves receipt verification to the caller.
pub struct HttpPeerFetcher {
    client: reqwest::Client,
    identity: oag_crypto::PeerIdentity,
}

impl std::fmt::Debug for HttpPeerFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpPeerFetcher").finish()
    }
}

impl HttpPeerFetcher {
    pub fn new(identity: oag_crypto::PeerIdentity) -> Self {
        Self { client: reqwest::Client::new(), identity }
    }
}

impl PeerFetcher for HttpPeerFetcher {
    fn fetch<'a>(
        &'a self,
        peer: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move {
            let nonce = oag_crypto::random_bytes_32();
            let request = build_transfer_request(&self.identity, cid, &nonce);
            let url = format!("{}/oag/transfer/v1/fetch", peer.trim_end_matches('/'));
            let response = self.client.post(&url).json(&request).send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            Some(response.bytes().await.ok()?.to_vec())
        })
    }
}

/// A resolver backed by a shared map, handling both reads and writes for `Target::Store` --
/// the "storing information" case. A `null` payload is a read; any other payload is a write.
#[derive(Debug, Default, Clone)]
pub struct StoreResolver {
    entries: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>,
}

impl StoreResolver {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Resolver for StoreResolver {
    fn resolve<'a>(
        &'a self,
        target: &'a Target,
        payload: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Option<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let Target::Store(key) = target else {
                return None;
            };
            let mut entries = self.entries.lock().await;
            if payload.is_null() {
                entries.get(key).cloned()
            } else {
                entries.insert(key.clone(), payload.clone());
                Some(serde_json::json!({ "stored": key }))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_are_inferred_from_json_values() {
        assert_eq!(Target::infer(&serde_json::json!("https://example.com")), Target::Url("https://example.com".into()));
        assert_eq!(Target::infer(&serde_json::json!("QmXYZ")), Target::Cid("QmXYZ".into()));
        assert_eq!(Target::infer(&serde_json::json!("oagp_abc")), Target::Peer("oagp_abc".into()));
        assert_eq!(Target::infer(&serde_json::json!("find this")), Target::Query("find this".into()));
        assert_eq!(Target::infer(&serde_json::json!({"a": 1})), Target::Other(serde_json::json!({"a": 1})));
    }

    #[tokio::test]
    async fn a_local_resolver_answers_store_and_query_targets() {
        let mut resolver = LocalResolver::new();
        resolver.insert("key", serde_json::json!("value"));
        assert_eq!(
            resolver.resolve(&Target::Store("key".into()), &serde_json::json!({})).await,
            Some(serde_json::json!("value"))
        );
        assert_eq!(resolver.resolve(&Target::Url("https://x".into()), &serde_json::json!({})).await, None);
    }

    #[tokio::test]
    async fn a_composite_resolver_tries_each_in_order() {
        let mut composite = CompositeResolver::new();
        let mut first = LocalResolver::new();
        first.insert("a", serde_json::json!(1));
        let mut second = LocalResolver::new();
        second.insert("b", serde_json::json!(2));
        composite.add(Box::new(first));
        composite.add(Box::new(second));
        assert_eq!(composite.resolve(&Target::Store("b".into()), &serde_json::json!({})).await, Some(serde_json::json!(2)));
    }

    #[tokio::test]
    async fn a_url_resolver_answers_only_url_targets() {
        let resolver = UrlResolver::new();
        assert_eq!(resolver.resolve(&Target::Store("k".into()), &serde_json::json!({})).await, None);
    }

    #[tokio::test]
    async fn a_store_resolver_reads_and_writes() {
        let resolver = StoreResolver::new();
        let target = Target::Store("key".into());
        let write = resolver.resolve(&target, &serde_json::json!("value")).await.unwrap();
        assert_eq!(write, serde_json::json!({ "stored": "key" }));
        let read = resolver
            .resolve(&target, &serde_json::Value::Null)
            .await
            .unwrap();
        assert_eq!(read, serde_json::json!("value"));
    }

    #[tokio::test]
    async fn a_peer_resolver_fetches_a_cid_from_a_peer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A minimal transfer server: reads the request, returns a body.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let body = "peer content";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });

        let identity = oag_crypto::PeerIdentity::generate();
        let resolver = PeerResolver::new(HttpPeerFetcher::new(identity));
        let target = Target::Peer(format!("http://{addr}"));
        let answer = resolver
            .resolve(&target, &serde_json::json!("QmTest"))
            .await
            .unwrap();
        assert_eq!(answer["peer"], format!("http://{addr}"));
        assert_eq!(answer["body"], "peer content");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_cid_resolver_fetches_through_an_http_gateway() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A minimal IPFS gateway serving /ipfs/<cid>.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let body = "the file bytes";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });

        let resolver = CidResolver::new(HttpGatewayFetcher::new(format!("http://{addr}")));
        let target = Target::Cid("QmTest".into());
        let answer = resolver.resolve(&target, &serde_json::json!({})).await.unwrap();
        assert_eq!(answer["cid"], "QmTest");
        assert_eq!(answer["body"], "the file bytes");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_url_resolver_fetches_a_legacy_server() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // A minimal legacy HTTP server.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let body = "hello from the legacy server";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });

        let resolver = UrlResolver::new();
        let target = Target::Url(format!("http://{addr}/"));
        let answer = resolver.resolve(&target, &serde_json::json!({})).await.unwrap();
        assert_eq!(answer["status"], 200);
        assert_eq!(answer["body"], "hello from the legacy server");
        handle.await.unwrap();
    }
}
