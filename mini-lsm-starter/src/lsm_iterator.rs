#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

type LsmTwoMergeIter =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    upper_bound: Bound<Bytes>,
    storage_it: LsmTwoMergeIter,
}

impl LsmIterator {
    pub fn create(upper_bound: Bound<Bytes>, storage_it: LsmTwoMergeIter) -> Self {
        let mut it = Self {
            upper_bound,
            storage_it,
        };
        it.skip_tombstones();
        it
    }
}

impl LsmIterator {
    fn skip_tombstones(&mut self) {
        while self.is_valid() {
            if self.value().is_empty() {
                self.next().unwrap();
            } else {
                break;
            }
        }
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        if !self.storage_it.is_valid() {
            return false;
        }
        match &self.upper_bound {
            Bound::Included(key) => self.key() <= key,
            Bound::Excluded(key) => self.key() < key,
            Bound::Unbounded => true,
        }
    }

    fn key(&self) -> &[u8] {
        self.storage_it.key()
    }

    fn value(&self) -> &[u8] {
        self.storage_it.value()
    }

    fn next(&mut self) -> Result<()> {
        self.storage_it.next()?;

        // The sequence of key-value pairs produced by TwoMergeIterator may
        // contain empty value, which means that the value is deleted.
        // LsmIterator should filter these empty values
        self.skip_tombstones();

        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.iter.is_valid() {
            self.iter.next()?;
        }
        Ok(())
    }
}
