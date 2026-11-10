//! Data pads: a file split across many pads, reconstructed by gathering them.
//!
//! A pad is a node's representation of its neighborhood. A pad can also hold a shard of a larger
//! object: a file is split into many shards, each stored in a different node's pad, and a manifest
//! describes which pads hold which shards. To reconstruct the file, gather the pads and
//! concatenate the shards in order -- a thousand pads, each holding a piece, constructing a
//! single file.
//!
//! This is the same "pads can be combined" idea as `engine::combine_pads`, extended from serving
//! a request to serving a file. See `spec/02-tensor-pad-routing.md` and the base project's
//! `spec/23-tensor-pad-and-evolution-layer.md`.
//!
//! **What this explicitly is not.** Not an erasure-coded store: a missing shard is a missing
//! piece, and reconstruction fails loudly rather than guessing. Not a redundancy scheme: it does
//! not replicate shards. Not a consensus mechanism: the manifest is caller-authored, not agreed.
//! The transport that gathers the pads from the nodes is not built here -- this module is the
//! split and the reconstruction; the gathering uses whichever carrier is available.

use serde::{Deserialize, Serialize};

/// The default number of shards a file is split into -- a thousand pads to construct a single
/// file.
pub const DEFAULT_SHARD_COUNT: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub index: usize,
    pub data: Vec<u8>,
}

/// Describes a file split across pads: which pads hold which shards, in shard order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifest {
    pub file_id: String,
    pub shard_count: usize,
    pub shard_size: usize,
    /// The pad id holding each shard, in shard order.
    pub pad_ids: Vec<String>,
}

/// A node's pad with an optional data shard attached.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPad {
    pub pad_id: String,
    /// The routing weights -- the same 32 values `engine` routes with.
    pub values: Vec<f32>,
    pub shard: Option<Shard>,
}

#[derive(Debug, thiserror::Error)]
pub enum DataPadError {
    #[error("shard count must be at least 1")]
    EmptyShardCount,
    #[error("the manifest declares {declared} shards but {actual} were given")]
    CountMismatch { declared: usize, actual: usize },
    #[error("shard {index} is out of order: expected {expected}")]
    OutOfOrder { index: usize, expected: usize },
    #[error("the reconstructed file's hash does not match the manifest's file_id")]
    HashMismatch,
    #[error("the node {0} could not be reached")]
    NodeUnreachable(String),
    #[error("the node did not return a data pad")]
    NotAPad,
    #[error("transport error: {0}")]
    Transport(String),
}

/// Splits `data` into `shard_count` shards, returning the manifest and the shards. The shard size
/// is the ceiling of `len / shard_count`, so the last shard may be short. The manifest's `file_id`
/// is the blake3 hash of the whole file, so a reconstruction can be checked against it.
pub fn split(data: &[u8], shard_count: usize) -> Result<(FileManifest, Vec<Shard>), DataPadError> {
    if shard_count == 0 {
        return Err(DataPadError::EmptyShardCount);
    }
    let shard_size = data.len().div_ceil(shard_count).max(1);
    let mut shards: Vec<Shard> = data
        .chunks(shard_size)
        .enumerate()
        .map(|(index, chunk)| Shard { index, data: chunk.to_vec() })
        .collect();
    // Pad to the requested shard count with empty shards, so a manifest that promises a thousand
    // pads actually names a thousand pads -- a small file leaves the tail of them empty.
    while shards.len() < shard_count {
        shards.push(Shard { index: shards.len(), data: vec![] });
    }
    let manifest = FileManifest {
        file_id: blake3::hash(data).to_hex().to_string(),
        shard_count,
        shard_size,
        pad_ids: (0..shards.len()).map(|i| format!("pad-{i}")).collect(),
    };
    Ok((manifest, shards))
}

/// Splits `data` and assigns the shards to the given pad ids, in order -- the real case, where
/// the manifest names the pads that will actually hold the shards rather than generic ones.
pub fn split_to_pads(
    data: &[u8],
    pad_ids: &[String],
) -> Result<(FileManifest, Vec<Shard>), DataPadError> {
    let (mut manifest, shards) = split(data, pad_ids.len())?;
    manifest.pad_ids = pad_ids.to_vec();
    Ok((manifest, shards))
}

/// Attaches a shard to a pad.
pub fn attach(pad_id: String, values: Vec<f32>, shard: Shard) -> DataPad {
    DataPad { pad_id, values, shard: Some(shard) }
}

/// Reconstructs a file from its shards, verifying the count, the order, and the hash against the
/// manifest. A missing or out-of-order shard is an error, not a guess.
pub fn reconstruct(manifest: &FileManifest, mut shards: Vec<Shard>) -> Result<Vec<u8>, DataPadError> {
    if shards.len() != manifest.shard_count {
        return Err(DataPadError::CountMismatch { declared: manifest.shard_count, actual: shards.len() });
    }
    shards.sort_by_key(|s| s.index);
    let mut out = Vec::new();
    for (expected, shard) in shards.into_iter().enumerate() {
        if shard.index != expected {
            return Err(DataPadError::OutOfOrder { index: shard.index, expected });
        }
        out.extend_from_slice(&shard.data);
    }
    if blake3::hash(&out).to_hex().to_string() != manifest.file_id {
        return Err(DataPadError::HashMismatch);
    }
    Ok(out)
}

/// Collects the shards from a set of data pads, in manifest order. A pad whose shard is missing,
/// or a manifest entry with no pad, is an error.
pub fn gather(manifest: &FileManifest, pads: &[DataPad]) -> Result<Vec<Shard>, DataPadError> {
    let by_id: std::collections::HashMap<&str, &DataPad> =
        pads.iter().map(|p| (p.pad_id.as_str(), p)).collect();
    let mut shards = Vec::with_capacity(manifest.shard_count);
    for (index, pad_id) in manifest.pad_ids.iter().enumerate() {
        let pad = by_id.get(pad_id.as_str()).ok_or(DataPadError::CountMismatch {
            declared: manifest.shard_count,
            actual: index,
        })?;
        let shard = pad.shard.as_ref().ok_or(DataPadError::CountMismatch {
            declared: manifest.shard_count,
            actual: index,
        })?;
        shards.push(shard.clone());
    }
    Ok(shards)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PAD_DIM;

    #[test]
    fn a_file_splits_and_reconstructs_exactly() {
        let data: Vec<u8> = (0..=255u8).cycle().take(5000).collect();
        let (manifest, shards) = split(&data, DEFAULT_SHARD_COUNT).unwrap();
        assert_eq!(shards.len(), manifest.shard_count);
        let reconstructed = reconstruct(&manifest, shards).unwrap();
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn a_short_file_still_fills_the_manifest_with_empty_tail_shards() {
        let data = b"tiny";
        let (manifest, shards) = split(data, 10).unwrap();
        assert_eq!(shards.len(), 10);
        assert_eq!(shards[0].data, b"t");
        assert!(shards[4..].iter().all(|s| s.data.is_empty()));
        assert_eq!(reconstruct(&manifest, shards).unwrap(), data);
    }

    #[test]
    fn a_missing_shard_is_an_error_not_a_guess() {
        let data = b"a thousand pads";
        let (manifest, mut shards) = split(data, 10).unwrap();
        shards.remove(3);
        assert!(matches!(
            reconstruct(&manifest, shards),
            Err(DataPadError::CountMismatch { .. })
        ));
    }

    #[test]
    fn a_shard_with_a_wrong_index_is_an_error() {
        let data = b"order matters";
        let (manifest, mut shards) = split(data, 10).unwrap();
        // Corrupt an index, not just the vec order: reconstruct sorts by index, so a wrong index
        // is the case that must be caught.
        shards[1].index = 5;
        assert!(matches!(reconstruct(&manifest, shards), Err(DataPadError::OutOfOrder { .. })));
    }

    #[test]
    fn a_thousand_pads_construct_a_single_file() {
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 256) as u8).collect();
        let (manifest, shards) = split(&data, DEFAULT_SHARD_COUNT).unwrap();
        assert_eq!(shards.len(), DEFAULT_SHARD_COUNT);

        let pads: Vec<DataPad> = shards
            .into_iter()
            .enumerate()
            .map(|(i, shard)| attach(format!("pad-{i}"), vec![0.0; PAD_DIM], shard))
            .collect();
        assert_eq!(pads.len(), 1000);

        let gathered = gather(&manifest, &pads).unwrap();
        assert_eq!(reconstruct(&manifest, gathered).unwrap(), data);
    }

    #[test]
    fn gathering_pads_in_manifest_order_reconstructs_the_file() {
        let data: Vec<u8> = (0..100u8).collect();
        let (manifest, shards) = split(&data, 10).unwrap();
        let pads: Vec<DataPad> = shards
            .into_iter()
            .enumerate()
            .map(|(i, shard)| attach(format!("pad-{i}"), vec![1.0; 32], shard))
            .collect();
        let gathered = gather(&manifest, &pads).unwrap();
        assert_eq!(reconstruct(&manifest, gathered).unwrap(), data);
    }
}
