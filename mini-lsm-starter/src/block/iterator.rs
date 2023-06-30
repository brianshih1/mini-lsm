#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Bytes;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        // | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) |
        let block_data = &block.data;
        let (_, key, _, value) = Self::get_key_and_value(block.clone(), 0);

        let key_bytes = Bytes::copy_from_slice(&key);

        println!("Created iterator with key: {key_bytes:?}");
        BlockIterator {
            block: block.clone(),
            key,
            value,
            idx: 1,
        }
    }

    // Given an offset, returns (key_size, key) OR (value_len, next_offset, value, next_offset)
    fn get_len_and_slice(block: Arc<Block>, offset: u16) -> (u16, Vec<u8>, u16) {
        if offset == 108 {
            let foo = "";
        }
        let offset = offset as usize;
        let block_data = &block.data;

        let len = u16::from_le_bytes([block_data[offset], block_data[offset + 1]]);

        let start = offset + 2 as usize;
        let end = offset + 2 + len as usize;

        (len, block_data[start..end].to_vec(), end as u16)
    }

    // Returns (key_size, key, value_size, value)
    fn get_key_and_value(block: Arc<Block>, offset: u16) -> (u16, Vec<u8>, u16, Vec<u8>) {
        let (key_len, key, next_start) = Self::get_len_and_slice(block.clone(), offset);
        let (value_len, value, _) = Self::get_len_and_slice(block.clone(), next_start);
        (key_len, key, value_len, value)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut it = Self::create_and_seek_to_first(block);
        it.seek_to_key(key);
        it
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let block_data = &self.block.data;
        let (_, key, _, value) = Self::get_key_and_value(self.block.clone(), 0);
        self.idx = 1;
        self.key = key;
        self.value = value;
    }

    pub fn size(&self) -> usize {
        self.block.data.len()
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 < self.block.offsets.len() {
            let offset = self.block.offsets[self.idx];
            let (_, key, _, value) = Self::get_key_and_value(self.block.clone(), offset);
            self.idx += 1;
            self.key = key;
            self.value = value;
        } else {
            self.key.clear();
            self.value.clear();
        }
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        self.seek_to_first();
        while self.is_valid() && self.key.as_slice() < key {
            let bytes = Bytes::copy_from_slice(&self.key);
            self.next();
            let is_valid = self.is_valid();
        }
        if self.key.as_slice() < key {
            self.key.clear();
            self.value.clear();
        }
    }
}
