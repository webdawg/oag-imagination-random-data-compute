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
    fn resolve(&self, target: &Target, payload: &serde_json::Value) -> Option<serde_json::Value>;
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
    fn resolve(&self, target: &Target, _payload: &serde_json::Value) -> Option<serde_json::Value> {
        let key = match target {
            Target::Store(k) | Target::Query(k) => k,
            _ => return None,
        };
        self.entries.get(key).cloned()
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
    fn resolve(&self, target: &Target, payload: &serde_json::Value) -> Option<serde_json::Value> {
        self.resolvers.iter().find_map(|r| r.resolve(target, payload))
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

    #[test]
    fn a_local_resolver_answers_store_and_query_targets() {
        let mut resolver = LocalResolver::new();
        resolver.insert("key", serde_json::json!("value"));
        assert_eq!(
            resolver.resolve(&Target::Store("key".into()), &serde_json::json!({})),
            Some(serde_json::json!("value"))
        );
        assert_eq!(resolver.resolve(&Target::Url("https://x".into()), &serde_json::json!({})), None);
    }

    #[test]
    fn a_composite_resolver_tries_each_in_order() {
        let mut composite = CompositeResolver::new();
        let mut first = LocalResolver::new();
        first.insert("a", serde_json::json!(1));
        let mut second = LocalResolver::new();
        second.insert("b", serde_json::json!(2));
        composite.add(Box::new(first));
        composite.add(Box::new(second));
        assert_eq!(composite.resolve(&Target::Store("b".into()), &serde_json::json!({})), Some(serde_json::json!(2)));
    }
}
