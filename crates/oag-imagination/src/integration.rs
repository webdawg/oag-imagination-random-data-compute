//! End-to-end: a real `oag` database, the adapter, and the engine over TCP.
//!
//! This is the whole stack in one test -- the adapter reads the node's pad and
//! peers from `oag`'s storage, the engine serves over TCP, and a client gets an
//! answer back. It exists because the pieces are individually tested; this
//! checks they fit together.

#[cfg(test)]
mod tests {
    use crate::engine::Request;
    use crate::socket::{send, serve_on};
    use crate::target::{FnResolver, Target};
    use oag_storage::open_pool;
    use oag_storage::repo::{peers, tensor_pads};
    use std::sync::Arc;
    use tokio::net::TcpListener;
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
    async fn the_full_stack_runs_against_a_real_oag_database() {
        let dir = std::env::temp_dir().join(format!("oag-imagination-full-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("oag.sqlite");

        // Seed a real oag database: this node's pad, and one peer with an address.
        let pool = open_pool(&db).await.unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let node_id = [1u8; 32];
        let pad = vec![1.0f32; 32];
        tensor_pads::upsert(&mut conn, &node_id, &pad, 0).await.unwrap();
        let peer_id = [2u8; 32];
        peers::upsert_peer(&mut conn, &peer_id, &[3u8; 32], None, 0).await.unwrap();
        peers::add_address(&mut conn, &peer_id, "127.0.0.1:9000").await.unwrap();
        drop(conn);
        drop(pool);

        // Load the adapter from that database and serve the engine.
        let adapter = crate::NodeAdapter::load(&db, &node_id).await.unwrap();
        assert_eq!(adapter.engine.pad, pad);
        assert_eq!(adapter.engine.neighbors, vec!["127.0.0.1:9000".to_string()]);

        let engine = Arc::new(Mutex::new(adapter.engine));
        let resolver = Arc::new(FnResolver(|_: &Target, _: &serde_json::Value| {
            Some(serde_json::json!("answer"))
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { serve_on(engine, listener, resolver).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let response = send(&addr.to_string(), &request("req-full")).await.unwrap();
        assert_eq!(response.answer, Some(serde_json::json!("answer")));
        assert_eq!(response.contributions[0].node_id, hex::encode(node_id));

        handle.abort();
        std::fs::remove_dir_all(&dir).ok();
    }
}
