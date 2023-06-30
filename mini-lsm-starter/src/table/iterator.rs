#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Error, Ok, Result};
use bytes::Bytes;

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    block_it: BlockIterator,
    block_idx: usize,
    table: Arc<SsTable>,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(0)?;
        let block_it = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            block_it,
            block_idx: 0,
            table,
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.advance_to_block(0);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let block = table.read_block(block_idx)?;
        let block_it = BlockIterator::create_and_seek_to_key(block, key);
        if block_it.is_valid() {
            Ok(Self {
                block_it,
                block_idx,
                table: table,
            })
        } else {
            Err(Error::msg("cannot seek to key"))
        }
    }

    pub fn advance_to_block(&mut self, idx: usize) {
        println!("Advancing to block: {idx}");
        let block = self.table.read_block(idx).unwrap();
        self.block_idx = idx;
        self.block_it = BlockIterator::create_and_seek_to_first(block);
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let key_bytes = Bytes::copy_from_slice(&key);
        println!("Seeking key: {key_bytes:?}");

        let block_idx = self.table.find_block_idx(key);
        self.advance_to_block(block_idx);
        self.block_it.seek_to_key(key);
        if !self.block_it.is_valid() {
            self.advance_to_block(block_idx + 1);
            self.block_it.seek_to_key(key);
        }
        let block_it_key = self.block_it.key();
        let key_bytes = Bytes::copy_from_slice(&block_it_key);
        println!("Byte iterator key: {key_bytes:?}");

        if self.is_valid() {
            Ok(())
        } else {
            Err(Error::msg("cannot seek to key"))
        }
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_it.value()
    }

    fn key(&self) -> &[u8] {
        self.block_it.key()
    }

    fn is_valid(&self) -> bool {
        self.block_it.is_valid()
    }

    // After next(), block_it must always be valid unless we finished iterating
    // through all the keys
    fn next(&mut self) -> Result<()> {
        self.block_it.next();
        if self.block_it.is_valid() {
            Ok(())
        } else {
            if self.block_idx + 1 < self.table.num_of_blocks() {
                self.advance_to_block(self.block_idx + 1);
                self.block_it.seek_to_first();
            }
            Ok(())
        }
    }
}
