#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::sync::Arc;

use anyhow::{Error, Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let entry = self.map.get(key);
        match entry {
            Some(entry) => Some(entry.value().clone()),
            None => None,
        }
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    fn convert_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
        match bound {
            Bound::Included(slice) => Bound::Included(Bytes::copy_from_slice(slice)),
            Bound::Excluded(slice) => Bound::Excluded(Bytes::copy_from_slice(slice)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let iter = self
            .map
            .range((Self::convert_bound(lower), Self::convert_bound(upper)));
        let mut it = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: {
                |map| map.range((Self::convert_bound(lower), Self::convert_bound(upper)))
            },
            item: (
                Bytes::copy_from_slice(&vec![]),
                Bytes::copy_from_slice(&vec![]),
            ),
        }
        .build();
        it.next();
        it
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        unimplemented!()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &[u8] {
        self.with_item(|a| a.1.as_ref())
    }

    fn key(&self) -> &[u8] {
        self.with_item(|a| a.0.as_ref())
    }

    fn is_valid(&self) -> bool {
        self.with_item(|(k, _)| !k.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|s| {
            let next = s.iter.next();
            match next {
                Some(v) => *s.item = (v.key().clone(), v.value().clone()),
                None => {
                    s.item.0.clear();
                    s.item.1.clear();
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests;
