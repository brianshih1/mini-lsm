#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;
use bytes::Bytes;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    is_current_a: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut is_current_a = true;

        match (a.is_valid(), b.is_valid()) {
            (true, true) => {
                if a.key() <= b.key() {
                    is_current_a = true;
                } else {
                    is_current_a = false;
                }
            }
            (true, false) => {
                is_current_a = true;
            }
            (false, true) => {
                is_current_a = false;
            }
            (false, false) => {}
        };
        Ok(TwoMergeIterator { a, b, is_current_a })
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.is_current_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.is_current_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let current_key = Bytes::copy_from_slice(self.key());

        if self.is_current_a {
            self.a.next().unwrap();
        } else {
            self.b.next().unwrap();
        }

        let a_key = if self.a.is_valid() {
            Some(self.a.key())
        } else {
            None
        };
        let b_key = if self.b.is_valid() {
            Some(self.b.key())
        } else {
            None
        };

        match (a_key, b_key) {
            (None, None) => return Ok(()),
            (None, Some(_)) => {
                self.is_current_a = false;
            }
            (Some(_), None) => {
                self.is_current_a = true;
            }
            (Some(a_key), Some(b_key)) => {
                println!(
                    "Both iterators are valid.  A is: {:?}. B is: {:?}",
                    as_bytes(a_key),
                    as_bytes(b_key)
                );
                if a_key <= b_key {
                    println!("b is bigger.");
                    self.is_current_a = true;
                } else if a_key > b_key {
                    println!("a is bigger");
                    self.is_current_a = false;
                }
                // else if a_key == b_key {
                //     self.is_current_a = true;
                // }
            }
        }
        if self.key() == current_key {
            self.next();
        }

        Ok(())
    }
}

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}
