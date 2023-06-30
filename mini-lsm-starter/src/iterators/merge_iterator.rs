#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};
use bytes::Bytes;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, it) in iters.into_iter().enumerate() {
            if it.is_valid() {
                heap.push(HeapWrapper(idx, it));
            }
        }
        let first = heap.pop();
        Self {
            iters: heap,
            current: first,
        }
    }
}

impl<I: StorageIterator> MergeIterator<I> {}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        unsafe { self.current.as_ref().unwrap_unchecked() }.1.key()
    }

    fn value(&self) -> &[u8] {
        unsafe { self.current.as_ref().unwrap_unchecked() }
            .1
            .value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = &self.current {
            if current.1.is_valid() {
                return true;
            }
        }
        self.iters.len() > 0
    }

    fn next(&mut self) -> Result<()> {
        let current = Bytes::copy_from_slice(self.key());

        unsafe {
            self.current.as_mut().unwrap_unchecked().1.next();
        }

        let is_valid = unsafe { self.current.as_ref().unwrap_unchecked().1.is_valid() };
        if is_valid {
            println!("Peeking");
            if let Some(it) = self.iters.peek() {
                if it > &self.current.as_ref().unwrap() {
                    println!("peeked iterator is greater than");
                    let it = self.iters.pop().unwrap();
                    let original_it = std::mem::replace(&mut self.current, Some(it));
                    self.iters.push(original_it.unwrap());
                } else {
                    println!("Current iterator is greater than");
                }
            } else {
                println!("Nothing to peek");
            }
        } else {
            println!("Current iterator is not valid");
            if !self.iters.is_empty() {
                let popped = self.iters.pop().unwrap();
                std::mem::replace(&mut self.current, Some(popped));
            } else {
                println!("StorageIterator is now invalid!")
            }
        }
        if self.is_valid() && self.key() == current {
            self.next();
        }
        Ok(())
    }
}
