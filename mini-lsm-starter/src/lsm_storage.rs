#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use core::result::Result::Ok;
use std::ops::{Bound, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    sync_mutex: Mutex<()>,
    path: PathBuf,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            sync_mutex: Mutex::new(()),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = self.inner.read().clone(); // drops the read_lock

        let res = inner.memtable.get(key);
        if let Some(res) = res {
            if !res.is_empty() {
                return Ok(Some(res));
            } else {
                return Ok(None);
            }
        }

        for imm_memtable in inner.imm_memtables.iter().rev() {
            let res = imm_memtable.get(key);
            if let Some(res) = res {
                if !res.is_empty() {
                    return Ok(Some(res));
                } else {
                    return Ok(None);
                }
            }
        }

        let mut sstable_iters = Vec::with_capacity(inner.l0_sstables.len());
        for sstable in inner.l0_sstables.iter().rev() {
            let it = SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
            sstable_iters.push(Box::new(it));
        }
        let sstable_it = MergeIterator::create(sstable_iters);
        if sstable_it.is_valid() {
            let value = sstable_it.value();
            if !value.is_empty() {
                return Ok(Some(Bytes::copy_from_slice(value)));
            } else {
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        let writer = self.inner.as_ref().write();
        writer.memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let writer = self.inner.as_ref().write();
        writer.memtable.put(key, &vec![]);
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let lock = self.sync_mutex.lock().unwrap();
        let memtable;
        let sst_id;
        {
            // Moving mutable memtable to imm_memtables.
            // Create a new mutable memtable
            let mut write_guard = self.inner.write();
            let mut copy = write_guard.as_ref().clone();
            memtable = std::mem::replace(&mut copy.memtable, Arc::new(MemTable::create()));
            copy.imm_memtables.push(memtable.clone());
            sst_id = copy.next_sst_id;
            *write_guard = Arc::new(copy);
        }

        // flush the mem-table to disk as an SST file
        let mut builder = SsTableBuilder::new(4096);
        memtable.flush(&mut builder).unwrap();
        let sstable = builder
            .build(sst_id, None, self.compute_path(sst_id))
            .unwrap();

        {
            // remove the mem-table and put the SST into l0_tables
            let mut write_guard = self.inner.write();
            let mut copy = write_guard.as_ref().clone();
            copy.imm_memtables.pop();
            copy.l0_sstables.push(Arc::new(sstable));
            copy.next_sst_id += 1;
            *write_guard = Arc::new(copy);
        };
        Ok(())
    }

    fn compute_path(&self, sst_id: usize) -> PathBuf {
        self.path.join(sst_id.to_string())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let inner = self.inner.read().clone(); // drops the read_lock
        let mem_table_it = inner.memtable.scan(lower, upper);
        let mem_iters = inner
            .imm_memtables
            .iter()
            .rev()
            .map(|memtable| Box::new(memtable.scan(lower, upper)))
            .collect::<Vec<Box<MemTableIterator>>>();
        let mut its = vec![Box::new(mem_table_it)];
        its.extend(mem_iters);

        let merged_it = MergeIterator::create(its);
        let mut sstable_iters = Vec::with_capacity(inner.l0_sstables.len());
        for sstable in inner.l0_sstables.iter().rev() {
            let it = match lower {
                Bound::Included(key) => {
                    let it = SsTableIterator::create_and_seek_to_key(sstable.clone(), key);
                    match it {
                        Ok(it) => Some(it),
                        Err(_) => None,
                    }
                }
                Bound::Excluded(key) => {
                    let it = SsTableIterator::create_and_seek_to_key(sstable.clone(), key);
                    match it {
                        Ok(mut it) => {
                            if it.key() == key {
                                it.next()?;
                            }
                            Some(it)
                        }
                        Err(_) => None,
                    }
                }
                Bound::Unbounded => {
                    Some(SsTableIterator::create_and_seek_to_first(sstable.clone()).unwrap())
                }
            };
            if let Some(it) = it {
                sstable_iters.push(Box::new(it));
            }
        }
        let sstable_it = MergeIterator::create(sstable_iters);

        let merged_it = TwoMergeIterator::create(merged_it, sstable_it).unwrap();
        Ok(FusedIterator::new(LsmIterator::create(
            convert_bound(upper),
            merged_it,
        )))
    }
}

fn convert_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(slice) => Bound::Included(Bytes::copy_from_slice(slice)),
        Bound::Excluded(slice) => Bound::Excluded(Bytes::copy_from_slice(slice)),
        Bound::Unbounded => Bound::Unbounded,
    }
}
