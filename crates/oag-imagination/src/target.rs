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
