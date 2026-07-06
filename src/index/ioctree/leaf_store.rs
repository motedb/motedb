//! LeafStore: Disk-first leaf data storage with bounded LRU cache
//!
//! Each leaf occupies a fixed-size slot (516 bytes) in a single data file.
//! Points are cached in an LRU cache with configurable capacity (default: 4096 slots ≈ 2MB).

#![allow(dead_code)]

use super::node::IndexedPoint3D;
use crate::{Result, StorageError};
use lru::LruCache;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

const LEAF_MAGIC: u32 = 0x10C7_1F00;
const LEAF_VERSION: u32 = 1;
const FILE_HEADER_SIZE: usize = 16;
pub const MAX_POINTS_PER_SLOT: usize = 32;
const POINT_SIZE: usize = 32; // f64(8) + f64(8) + f64(8) + u64(8)
const SLOT_HEADER_SIZE: usize = 4; // point_count(u16) + reserved(u16)
const SLOT_SIZE: usize = SLOT_HEADER_SIZE + MAX_POINTS_PER_SLOT * POINT_SIZE; // 1028 bytes
const DEFAULT_CACHE_CAPACITY: usize = 4096;

struct LeafEntry {
    points: Vec<IndexedPoint3D>,
}

pub struct LeafStore {
    inner: Mutex<LeafStoreInner>,
    path: PathBuf,
    next_id: AtomicU64,
}

struct LeafStoreInner {
    file: File,
    cache: LruCache<u64, LeafEntry>,
    dirty: HashSet<u64>,
}

impl LeafStore {
    /// Open or create a LeafStore in the given directory
    ///
    /// `cache_capacity` controls the number of leaf slots kept in the LRU cache.
    /// Each slot is ~1028 bytes, so 4096 slots ≈ 4MB.
    pub fn open(dir: &Path, cache_capacity: usize) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("leaf_data.bin");
        let exists = path.exists()
            && std::fs::metadata(&path)
                .map(|m| m.len() > 0)
                .unwrap_or(false);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(!exists)
            .open(&path)
            .map_err(StorageError::Io)?;

        let next_id = if exists {
            let mut f = file.try_clone().map_err(StorageError::Io)?;
            let mut header = [0u8; FILE_HEADER_SIZE];
            if f.read_exact(&mut header).is_ok() {
                let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
                if magic == LEAF_MAGIC {
                    u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as u64
                } else {
                    Self::write_header(&file)?;
                    0
                }
            } else {
                Self::write_header(&file)?;
                0
            }
        } else {
            Self::write_header(&file)?;
            0
        };

        let cap = NonZeroUsize::new(cache_capacity.max(1)).unwrap();
        let cache = LruCache::new(cap);

        Ok(Self {
            inner: Mutex::new(LeafStoreInner {
                file,
                cache,
                dirty: HashSet::new(),
            }),
            path,
            next_id: AtomicU64::new(next_id),
        })
    }

    fn write_header(file: &File) -> Result<()> {
        let mut f = file.try_clone().map_err(StorageError::Io)?;
        f.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
        f.write_all(&LEAF_MAGIC.to_le_bytes())
            .map_err(StorageError::Io)?;
        f.write_all(&LEAF_VERSION.to_le_bytes())
            .map_err(StorageError::Io)?;
        f.write_all(&0u32.to_le_bytes()).map_err(StorageError::Io)?; // slot_count
        f.write_all(&0u32.to_le_bytes()).map_err(StorageError::Io)?; // reserved
        f.flush().map_err(StorageError::Io)?;
        Ok(())
    }

    /// Allocate a new leaf with initial points
    pub fn create_leaf(&self, points: Vec<IndexedPoint3D>) -> Result<u64> {
        let leaf_id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;
        self.evict_if_needed(&mut inner)?;

        // Update slot count in header
        inner
            .file
            .seek(SeekFrom::Start(8))
            .map_err(StorageError::Io)?;
        inner
            .file
            .write_all(&((leaf_id + 1) as u32).to_le_bytes())
            .map_err(StorageError::Io)?;

        Self::write_slot(&mut inner.file, leaf_id, &points)?;
        inner.cache.put(leaf_id, LeafEntry { points });

        Ok(leaf_id)
    }

    /// Get all points for a leaf (from cache or disk)
    pub fn get_points(&self, leaf_id: u64) -> Result<Vec<IndexedPoint3D>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if let Some(entry) = inner.cache.get(&leaf_id) {
            return Ok(entry.points.clone());
        }

        self.evict_if_needed(&mut inner)?;
        let points = Self::read_slot(&mut inner.file, leaf_id)?;
        inner.cache.put(
            leaf_id,
            LeafEntry {
                points: points.clone(),
            },
        );
        Ok(points)
    }

    /// Add a point to a leaf
    pub fn add_point(&self, leaf_id: u64, point: IndexedPoint3D) -> Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if inner.cache.get(&leaf_id).is_none() {
            self.evict_if_needed(&mut inner)?;
            let points = Self::read_slot(&mut inner.file, leaf_id)?;
            inner.cache.put(leaf_id, LeafEntry { points });
        }

        if let Some(entry) = inner.cache.get_mut(&leaf_id) {
            if entry.points.len() >= MAX_POINTS_PER_SLOT {
                return Ok(false); // Leaf is full — caller should split
            }
            entry.points.push(point);
        }
        inner.dirty.insert(leaf_id);
        Ok(true)
    }

    /// Remove a point by row_id, returns true if found
    pub fn remove_point(&self, leaf_id: u64, row_id: u64) -> Result<bool> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if inner.cache.get(&leaf_id).is_none() {
            self.evict_if_needed(&mut inner)?;
            let points = Self::read_slot(&mut inner.file, leaf_id)?;
            inner.cache.put(leaf_id, LeafEntry { points });
        }

        if let Some(entry) = inner.cache.get_mut(&leaf_id) {
            if let Some(pos) = entry.points.iter().position(|p| p.row_id == row_id) {
                entry.points.remove(pos);
                inner.dirty.insert(leaf_id);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Get point count for a leaf (from cache or read header from disk)
    pub fn point_count(&self, leaf_id: u64) -> Result<usize> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if let Some(entry) = inner.cache.get(&leaf_id) {
            return Ok(entry.points.len());
        }

        // Read just the count from disk (first 2 bytes of slot)
        let offset = FILE_HEADER_SIZE as u64 + leaf_id * SLOT_SIZE as u64;
        inner
            .file
            .seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        let mut buf = [0u8; 2];
        inner.file.read_exact(&mut buf).map_err(StorageError::Io)?;
        Ok(u16::from_le_bytes(buf) as usize)
    }

    /// Replace all points in a leaf
    pub fn set_points(&self, leaf_id: u64, points: Vec<IndexedPoint3D>) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if let Some(entry) = inner.cache.get_mut(&leaf_id) {
            entry.points = points;
        } else {
            self.evict_if_needed(&mut inner)?;
            inner.cache.put(leaf_id, LeafEntry { points });
        }
        inner.dirty.insert(leaf_id);
        Ok(())
    }

    /// Filter points in a leaf, keeping only those matching predicate
    pub fn retain_points(
        &self,
        leaf_id: u64,
        f: impl FnMut(&IndexedPoint3D) -> bool,
    ) -> Result<usize> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if inner.cache.get(&leaf_id).is_none() {
            self.evict_if_needed(&mut inner)?;
            let points = Self::read_slot(&mut inner.file, leaf_id)?;
            inner.cache.put(leaf_id, LeafEntry { points });
        }

        if let Some(entry) = inner.cache.get_mut(&leaf_id) {
            let before = entry.points.len();
            entry.points.retain(f);
            let removed = before - entry.points.len();
            if removed > 0 {
                inner.dirty.insert(leaf_id);
            }
            Ok(removed)
        } else {
            Ok(0)
        }
    }

    /// Clear all points in a leaf, returns count of removed
    pub fn clear_leaf(&self, leaf_id: u64) -> Result<usize> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        if let Some(entry) = inner.cache.get_mut(&leaf_id) {
            let count = entry.points.len();
            entry.points.clear();
            inner.dirty.insert(leaf_id);
            return Ok(count);
        }

        // Leaf not in cache — write empty slot to disk to ensure it's cleared
        let disk_points = Self::read_slot(&mut inner.file, leaf_id)?;
        let count = disk_points.len();
        if count > 0 {
            Self::write_slot(&mut inner.file, leaf_id, &[])?;
        }
        Ok(count)
    }

    /// Free a leaf slot
    pub fn free_leaf(&self, leaf_id: u64) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;
        inner.cache.pop(&leaf_id);
        inner.dirty.remove(&leaf_id);
        // Overwrite slot with empty data
        Self::write_slot(&mut inner.file, leaf_id, &[])?;
        Ok(())
    }

    /// Flush all dirty entries to disk
    pub fn flush(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| StorageError::Lock(e.to_string()))?;

        let dirty_ids: Vec<u64> = inner.dirty.iter().copied().collect();
        let dirty_data: Vec<(u64, Vec<IndexedPoint3D>)> = dirty_ids
            .iter()
            .filter_map(|id| inner.cache.get(id).map(|e| (*id, e.points.clone())))
            .collect();

        for (id, points) in &dirty_data {
            Self::write_slot(&mut inner.file, *id, points)?;
        }
        inner.dirty.clear();
        inner.file.flush().map_err(StorageError::Io)?;
        Ok(())
    }

    /// Number of allocated leaf slots
    pub fn slot_count(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    fn evict_if_needed(&self, inner: &mut LeafStoreInner) -> Result<()> {
        let cap = inner.cache.cap().get();
        if inner.cache.len() >= cap {
            if let Some((evicted_id, evicted_entry)) = inner.cache.pop_lru() {
                if inner.dirty.remove(&evicted_id) {
                    Self::write_slot(&mut inner.file, evicted_id, &evicted_entry.points)?;
                }
            }
        }
        Ok(())
    }

    fn slot_offset(leaf_id: u64) -> u64 {
        FILE_HEADER_SIZE as u64 + leaf_id * SLOT_SIZE as u64
    }

    fn read_slot(file: &mut File, leaf_id: u64) -> Result<Vec<IndexedPoint3D>> {
        let offset = Self::slot_offset(leaf_id);
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;

        let mut buf = [0u8; SLOT_SIZE];
        file.read_exact(&mut buf).map_err(StorageError::Io)?;

        let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        let mut points = Vec::with_capacity(count.min(MAX_POINTS_PER_SLOT));
        for i in 0..count.min(MAX_POINTS_PER_SLOT) {
            let base = SLOT_HEADER_SIZE + i * POINT_SIZE;
            points.push(IndexedPoint3D {
                x: f64::from_le_bytes([
                    buf[base],
                    buf[base + 1],
                    buf[base + 2],
                    buf[base + 3],
                    buf[base + 4],
                    buf[base + 5],
                    buf[base + 6],
                    buf[base + 7],
                ]),
                y: f64::from_le_bytes([
                    buf[base + 8],
                    buf[base + 9],
                    buf[base + 10],
                    buf[base + 11],
                    buf[base + 12],
                    buf[base + 13],
                    buf[base + 14],
                    buf[base + 15],
                ]),
                z: f64::from_le_bytes([
                    buf[base + 16],
                    buf[base + 17],
                    buf[base + 18],
                    buf[base + 19],
                    buf[base + 20],
                    buf[base + 21],
                    buf[base + 22],
                    buf[base + 23],
                ]),
                row_id: u64::from_le_bytes([
                    buf[base + 24],
                    buf[base + 25],
                    buf[base + 26],
                    buf[base + 27],
                    buf[base + 28],
                    buf[base + 29],
                    buf[base + 30],
                    buf[base + 31],
                ]),
            });
        }
        Ok(points)
    }

    fn write_slot(file: &mut File, leaf_id: u64, points: &[IndexedPoint3D]) -> Result<()> {
        let offset = Self::slot_offset(leaf_id);
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;

        let mut buf = [0u8; SLOT_SIZE];
        let count = points.len().min(MAX_POINTS_PER_SLOT);
        buf[0..2].copy_from_slice(&(count as u16).to_le_bytes());

        for (i, point) in points.iter().take(MAX_POINTS_PER_SLOT).enumerate() {
            let base = SLOT_HEADER_SIZE + i * POINT_SIZE;
            buf[base..base + 8].copy_from_slice(&point.x.to_le_bytes());
            buf[base + 8..base + 16].copy_from_slice(&point.y.to_le_bytes());
            buf[base + 16..base + 24].copy_from_slice(&point.z.to_le_bytes());
            buf[base + 24..base + 32].copy_from_slice(&point.row_id.to_le_bytes());
        }

        file.write_all(&buf).map_err(StorageError::Io)?;
        Ok(())
    }
}
