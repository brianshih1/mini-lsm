#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Bytes, BytesMut};

use super::{BlockMeta, SsTable};
use crate::{
    block::{self, BlockBuilder},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    data: Vec<u8>,
    max_block_size: usize,
    block_builder: BlockBuilder, // Add other fields you need.
}

const USIZE: usize = std::mem::size_of::<usize>();

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            data: Vec::new(),
            max_block_size: block_size,
            block_builder: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable
    #[allow(unused_must_use)]
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let did_add = self.block_builder.add(key, value);
        // block builder is full
        if !did_add {
            let block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: Bytes::from(value.to_vec()),
            };
            self.meta.push(block_meta);
            // let taken_property = mem::replace(&mut my_struct.property, 0);
            let block_builder = std::mem::replace(
                &mut self.block_builder,
                BlockBuilder::new(self.max_block_size),
            );
            let block = block_builder.build().encode();
            self.data.extend_from_slice(block.as_ref());
            self.block_builder.add(key, value);
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + USIZE * self.meta.len()
    }

    // | data block | data block | meta block | meta block offset (u32) |

    pub fn encode(&self) -> (Vec<u8>, usize) {
        let mut res = Vec::new();

        res.extend_from_slice(&self.data);
        let block_meta_offset = res.len() as usize;
        let block_offset = (res.len() as u32).to_le_bytes();

        let mut buf: Vec<u8> = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        res.extend_from_slice(&buf);
        res.extend_from_slice(&block_offset);
        (res, block_meta_offset)
    }

    // Returns (block_metas, block_meta_offset)
    pub fn decode(bytes: &[u8]) -> (&[u8], usize) {
        let bytes_len = bytes.len();
        let block_meta_offset =
            u32::from_le_bytes(bytes[bytes_len - 4..bytes_len].try_into().unwrap());
        (
            &bytes[block_meta_offset as usize..bytes_len - 4],
            block_meta_offset as usize,
        )
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let (encoded, offset) = self.encode();
        Ok(SsTable {
            file: FileObject::create(Path::new(""), encoded).unwrap(),
            block_metas: self.meta,
            block_meta_offset: offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
