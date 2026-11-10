//! Wiring the engine into a real `oag` node.
//!
//! The imagination layer does not keep its own copy of the pad or the peer list. It reads them
//! from `oag`'s storage -- the layer below -- and builds the engine from what it finds. A layer-up
//! protocol connects through the layer below; this is that connection.
//!
//! **What this explicitly is not.** Not a writer. This adapter only reads; it does not update the
//! pad or the peer list. The base project's own writers (`oag-sync`, `oag-brain`) remain the only
//! writers. Not a migration: `open_pool` runs `oag`'s embedded migrations, which are idempotent,
//! but this layer does not add any of its own.

use std::path::Path;

use oag_storage::open_pool;
use oag_storage::repo::{peers, tensor_pads};

use crate::engine::Engine;
use crate::source::SoftwareRandom;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("storage error: {0}")]
    Storage(#[from] oag_storage::StorageError),
    #[error("sqlx error: {0}")]
    Sqlx(String),
}

/// An engine loaded from a real `oag` node's storage.
#[derive(Debug)]
pub struct NodeAdapter {
    pub engine: Engine,
}

impl NodeAdapter {
    /// Loads the engine for the node identified by `node_peer_id` (the raw 32-byte `PeerId`) from
    /// the `oag` database at `database`. The pad comes from the `tensor_pads` table; the neighbors
    /// come from the `peers` and `peer_addresses` tables, using each peer's first address.
    ///
    /// A node with no pad yet gets an empty one (the base project's `TensorPad::default` is
    /// zeroed, and `oag-tensor` treats a missing row the same way).
    pub async fn load(database: &Path, node_peer_id: &[u8; 32]) -> Result<Self, NodeError> {
        let pool = open_pool(database).await?;
        let mut conn = pool.acquire().await.map_err(|e| NodeError::Sqlx(e.to_string()))?;

        let pad = tensor_pads::get(&mut conn, node_peer_id).await?.unwrap_or_default();

        let peer_rows = peers::list_peers(&mut conn).await?;
        let mut neighbors = Vec::new();
        for peer in peer_rows {
            if peer.peer_id.as_slice() == node_peer_id.as_slice() {
                continue;
            }
            if let Some(address) = peers::list_addresses(&mut conn, &peer.peer_id)
                .await?
                .into_iter()
                .next()
            {
                neighbors.push(address);
            }
        }

        let engine = Engine::with_source(
            hex::encode(node_peer_id),
            pad,
            neighbors,
            Box::new(SoftwareRandom),
        );
        Ok(Self { engine })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oag_storage::open_pool;
    use oag_storage::repo::{peers, tensor_pads};

    #[tokio::test]
    async fn the_adapter_loads_the_pad_and_neighbors_from_oag_storage() {
        let dir = std::env::temp_dir().join(format!("oag-imagination-node-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("oag.sqlite");

        // Seed a real `oag` database: this node's pad, and one peer with an address.
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

        let adapter = NodeAdapter::load(&db, &node_id).await.unwrap();
        assert_eq!(adapter.engine.pad, pad);
        assert_eq!(adapter.engine.neighbors, vec!["127.0.0.1:9000".to_string()]);

        std::fs::remove_dir_all(&dir).ok();
    }
}
