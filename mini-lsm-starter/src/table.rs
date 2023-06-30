#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            // offset (u32) | key_len (u16) | first_key
            let offset_bytes = (meta.offset as u32).to_be_bytes();
            buf.extend_from_slice(&offset_bytes);

            let key_len_bytes = (meta.first_key.len() as u16).to_be_bytes();
            buf.extend_from_slice(&key_len_bytes);

            buf.extend_from_slice(meta.first_key.as_ref());
        }
    }

    /// Decode block meta from a buffer.
    /// Each block meta:  // offset (u32) | key_len (u16) | first_key
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut blocks = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32();
            let key_len = buf.get_u16();
            let key = buf.copy_to_bytes(key_len as usize);
            blocks.push(BlockMeta {
                offset: offset as usize,
                first_key: key,
            });
        }
        blocks
    }
}

/// A file object.
/// Bytes containing whole things, including data, meta, meta_offset
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(FileObject(Bytes::from(data)))
    }

    pub fn open(path: &Path) -> Result<Self> {
        Self::create(path, vec![])
    }
}

pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    // the encoding looks like:
    // data block | data block | meta block | meta block offset (u32)
    // block_meta_offset point to the offset to the start of meta block.
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bytes = &file.0;
        let (block_metas_bytes, block_meta_offset) = SsTableBuilder::decode(bytes);
        let block_metas = BlockMeta::decode_block_meta(block_metas_bytes);
        Ok(SsTable {
            file,
            block_metas,
            block_meta_offset,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_offset = *&self.block_metas[block_idx].offset;
        let end_offset = if self.num_of_blocks() == block_idx + 1 {
            let fo = self.file.size() as usize;
            self.block_meta_offset
        } else {
            self.block_metas[block_idx + 1].offset
        };
        println!("Reading block - start_offset: {start_offset}, end_offset: {end_offset}");
        let block_bytes = self
            .file
            .read(start_offset as u64, (end_offset - start_offset) as u64)
            .unwrap();
        let buf = &self.file.0[start_offset..end_offset];
        Ok(Arc::new(Block::decode(&buf)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`. Returns index of block.
    /// Returns the first block where the first_key <= key_to_find.
    /// In other words, if the key is greater than all elements in all blocks,
    /// then the index of the last block is returned.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        // left is candidate
        let mut left = 0;
        let mut right = self.num_of_blocks() - 1;
        let key_bytes = Bytes::copy_from_slice(key);
        println!("Key is: {key_bytes:?}");
        // Invariant, the [left, right] contains all possible solutions
        while left < right {
            let mid = left + (right - left) / 2;
            println!("Mid is: {mid}");
            let foo = &self.block_metas[mid];
            println!("Mid key: {foo:?}");
            let mid_key: &[u8] = &self.block_metas[mid].first_key;
            if mid_key >= key {
                println!("mid_key is ge");
                right = mid;
            } else if mid_key < key {
                println!("mid_key is less than");
                left = mid + 1;
            }
        }
        left
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
