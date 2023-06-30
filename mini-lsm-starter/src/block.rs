#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    // |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
    // Example:
    // |offset|offset|num_of_elements|
    // |   0  |  12  |       2       |

    pub fn encode(&self) -> Bytes {
        let mut builder = BytesMut::new();
        builder.extend_from_slice(&self.data);
        builder.extend_from_slice(
            &self
                .offsets
                .iter()
                .flat_map(|offset| offset.to_le_bytes())
                .collect::<Vec<u8>>(),
        );

        builder.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());
        builder.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let length = data.len();
        if length == 0 {
            let foo = "";
        }
        let num_elements = u16::from_le_bytes([data[length - 2], data[length - 1]]);
        let offset_index_start = data.len() - 2 - num_elements as usize * 2;
        let offset_index_end = data.len() - 2;
        let offsets = &data[offset_index_start..offset_index_end];
        let offsets = offsets
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();

        Block {
            data: data[0..offset_index_start].to_vec(),
            offsets,
        }
    }
}

#[cfg(test)]
mod tests;
