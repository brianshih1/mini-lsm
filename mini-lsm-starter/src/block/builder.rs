#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    // maximum number of bytes
    max_block_size: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
}

const U16_SIZE: usize = 2;

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        let mut offsets = Vec::with_capacity(block_size);
        offsets.push(0);
        BlockBuilder {
            max_block_size: block_size,
            data: Vec::with_capacity(block_size),
            offsets,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let current_size = self.get_size();
        // extra u16 for offset
        let incoming_size = key.len() + value.len() + U16_SIZE;

        if !self.data.is_empty() && current_size + incoming_size > self.max_block_size {
            return false;
        }
        self.offsets.push(self.data.len().try_into().unwrap());
        self.data.push(key.len().try_into().unwrap());
        self.data.extend_from_slice(key);
        self.data.push(value.len().try_into().unwrap());
        self.data.extend_from_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn get_size(&self) -> usize {
        // the final u16 is for the num_of_elements
        self.data.len() + self.offsets.len() * U16_SIZE + U16_SIZE
    }
}
