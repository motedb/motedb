//! SQ8 compressed vector storage with LRU-bounded memory
//!
//! Storage format:
//! - Data file: vectors_sq8.bin — [count: u64] [entry1] [entry2] ...
//! - Index file: vectors_sq8.idx — [count: u64] [row_id: u64, offset: u64]... (sorted)
//!
//! Memory is bounded: the offset index uses LRU eviction, falling back to
//! binary search on the sidecar index file when entries are evicted.

use super::sq8::{QuantizedVector, SQ8Quantizer};
use crate::types::RowId;
use crate::{Result, StorageError};
use lru::LruCache;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Sidecar header: [count: u64][flushed_upto: u64][entries...]
/// `flushed_upto` = data file length at the last flush. The loader trusts the
/// sidecar only when the data file hasn't grown since (no crash-appended
/// entries); otherwise it rebuilds. Old-format sidecars (8-byte header, no
/// marker) are detected by size and force a one-time rebuild.
const SIDECAR_HEADER_SIZE: u64 = 16;

/// SQ8 compressed vector storage with bounded memory
pub struct SQ8Vectors {
    _data_dir: PathBuf,
    dimension: usize,
    quantizer: Arc<SQ8Quantizer>,

    /// Entry size = 8 (row_id) + 4 (min) + 4 (max) + dimension (codes)
    _entry_size: usize,

    /// mmap of vectors_sq8.bin — zero-syscall quantized vector reads
    data_mmap: Arc<RwLock<Option<Mmap>>>,
    /// mmap of vectors_sq8.idx sidecar — zero-syscall offset lookups
    idx_mmap: Arc<RwLock<Option<Mmap>>>,

    /// Bounded offset index: row_id -> file offset (LRU-capped)
    index: Arc<RwLock<LruCache<RowId, u64>>>,

    /// Sidecar index file handle for binary search on LRU miss
    index_file: Arc<RwLock<File>>,
    /// Total entries in the sidecar index (for binary search bounds)
    index_count: Arc<RwLock<u64>>,
    /// Total entries (tracked incrementally on insert/delete)
    count: Arc<RwLock<u64>>,

    /// LRU cache: row_id -> decompressed f32 vector
    cache: Arc<RwLock<LruCache<RowId, Arc<Vec<f32>>>>>,

    /// Quantized vector cache (for fast distance computation)
    quantized_cache: Arc<RwLock<LruCache<RowId, Arc<QuantizedVector>>>>,

    /// Persistent file handles (avoid open/close per read)
    read_file: Arc<RwLock<File>>,
    write_file: Arc<RwLock<File>>,
    file_path: PathBuf,

    /// Row ids deleted since the last flush. The sidecar/file keep the old
    /// entries (append-only layout), so liveness between flushes rides on
    /// this overlay; flush() filters them out of the rebuilt sidecar.
    tombstones: Arc<Mutex<HashSet<RowId>>>,
}

impl SQ8Vectors {
    /// Create new SQ8 vector storage
    pub fn create(
        data_dir: impl AsRef<Path>,
        quantizer: Arc<SQ8Quantizer>,
        cache_size: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir).map_err(StorageError::Io)?;

        let dimension = quantizer.dimension();
        let entry_size = 8 + 4 + 4 + dimension;
        let file_path = data_dir.join("vectors_sq8.bin");
        let idx_path = data_dir.join("vectors_sq8.idx");

        // Create empty data file with count=0
        let mut file = File::create(&file_path).map_err(StorageError::Io)?;
        file.write_all(&0u64.to_le_bytes())
            .map_err(StorageError::Io)?;

        // Create empty index file with count=0
        let mut idx_file = File::create(&idx_path).map_err(StorageError::Io)?;
        idx_file
            .write_all(&0u64.to_le_bytes())
            .map_err(StorageError::Io)?;

        let read_file = File::open(&file_path).map_err(StorageError::Io)?;
        let write_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;
        let idx_read = File::open(&idx_path).map_err(StorageError::Io)?;

        Ok(Self {
            _data_dir: data_dir,
            dimension,
            quantizer,
            _entry_size: entry_size,
            data_mmap: Arc::new(RwLock::new(None)),
            idx_mmap: Arc::new(RwLock::new(None)),
            index: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_size.max(1)).unwrap(),
            ))),
            index_file: Arc::new(RwLock::new(idx_read)),
            index_count: Arc::new(RwLock::new(0)),
            count: Arc::new(RwLock::new(0)),
            cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_size.max(1)).unwrap(),
            ))),
            quantized_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_size * 2).unwrap(),
            ))),
            read_file: Arc::new(RwLock::new(read_file)),
            write_file: Arc::new(RwLock::new(write_file)),
            file_path,
            tombstones: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Load existing SQ8 vector storage
    pub fn load(
        data_dir: impl AsRef<Path>,
        quantizer: Arc<SQ8Quantizer>,
        cache_size: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let dimension = quantizer.dimension();
        let entry_size = 8 + 4 + 4 + dimension;
        let file_path = data_dir.join("vectors_sq8.bin");
        let idx_path = data_dir.join("vectors_sq8.idx");

        if !file_path.exists() {
            return Err(StorageError::InvalidData(
                "SQ8 vectors file not found".to_string(),
            ));
        }

        let read_file = File::open(&file_path).map_err(StorageError::Io)?;
        let write_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;
        let idx_read = File::open(&idx_path).map_err(StorageError::Io)?;

        let data_len = read_file.metadata().map(|m| m.len()).unwrap_or(0);
        let index_count = if idx_path.exists() {
            let mut idx = File::open(&idx_path).map_err(StorageError::Io)?;
            let mut buf = [0u8; 8];
            idx.read_exact(&mut buf).map_err(StorageError::Io)?;
            let sidecar_count = u64::from_le_bytes(buf);
            let idx_len = idx.metadata().map(|m| m.len()).unwrap_or(0);
            let new_format = idx_len == SIDECAR_HEADER_SIZE + sidecar_count * 16;
            let trusted = if new_format {
                let mut marker = [0u8; 8];
                idx.read_exact(&mut marker).is_ok() && u64::from_le_bytes(marker) == data_len
            } else {
                // Old format (no flushed_upto marker) or torn sidecar
                false
            };
            if trusted {
                sidecar_count
            } else {
                // Sidecar predates unflushed appends (crash / old format):
                // rebuild with last-wins semantics. Tombstones are lost on
                // crash — the DB layer rebuilds the whole index for tables
                // with replayed WAL records, so this stays recovery-grade.
                Self::build_sidecar_index(&file_path, &idx_path, entry_size, None)?
            }
        } else {
            Self::build_sidecar_index(&file_path, &idx_path, entry_size, None)?
        };

        // mmap data and sidecar for zero-syscall reads
        let data_mmap = unsafe { Mmap::map(&read_file).ok() };
        let sidecar_mmap = unsafe { Mmap::map(&idx_read).ok() };

        Ok(Self {
            _data_dir: data_dir,
            dimension,
            quantizer,
            _entry_size: entry_size,
            data_mmap: Arc::new(RwLock::new(data_mmap)),
            idx_mmap: Arc::new(RwLock::new(sidecar_mmap)),
            index: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_size.max(1)).unwrap(),
            ))),
            index_file: Arc::new(RwLock::new(idx_read)),
            index_count: Arc::new(RwLock::new(index_count)),
            count: Arc::new(RwLock::new(index_count)),
            cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_size.max(1)).unwrap(),
            ))),
            quantized_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_size * 2).unwrap(),
            ))),
            read_file: Arc::new(RwLock::new(read_file)),
            write_file: Arc::new(RwLock::new(write_file)),
            file_path,
            tombstones: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Build sidecar index file by scanning the data file.
    ///
    /// The data file is an append-only log: `update()` may leave superseded
    /// entries and `delete()` leaves the old entry behind (removed via the
    /// tombstone set at flush time). The scan therefore covers ALL physical
    /// entries with LAST-WINS semantics per row_id — scanning only the first
    /// `count` records silently reverted updates and resurrected deletes.
    ///
    /// Returns the live entry count written to the sidecar.
    fn build_sidecar_index(
        data_path: &Path,
        idx_path: &Path,
        entry_size: usize,
        tombstones: Option<&HashSet<RowId>>,
    ) -> Result<u64> {
        // 🔑 read+write: the torn-tail repair below truncates via set_len,
        // which fails with EINVAL on a read-only handle.
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_path)
            .map_err(StorageError::Io)?;
        let data_len = data.metadata().map_err(StorageError::Io)?.len();

        // Drop a torn trailing entry (crash mid-append) so the file is a
        // whole number of entries again.
        let physical_entries = data_len.saturating_sub(8) / entry_size as u64;
        let expected_len = 8 + physical_entries * entry_size as u64;
        if data_len > expected_len {
            data.set_len(expected_len).map_err(StorageError::Io)?;
        }

        // Read all (row_id, offset) pairs — full scan, last-wins per row_id
        let mut entries: std::collections::HashMap<RowId, u64> =
            std::collections::HashMap::with_capacity(physical_entries as usize);
        let mut offset = 8u64;
        data.seek(SeekFrom::Start(8)).map_err(StorageError::Io)?;
        for _ in 0..physical_entries {
            let mut row_id_bytes = [0u8; 8];
            data.read_exact(&mut row_id_bytes)
                .map_err(StorageError::Io)?;
            let row_id = u64::from_le_bytes(row_id_bytes);
            if tombstones.is_none_or(|t| !t.contains(&row_id)) {
                entries.insert(row_id, offset);
            }
            offset += entry_size as u64;
            data.seek(SeekFrom::Current((entry_size - 8) as i64))
                .map_err(StorageError::Io)?;
        }

        let mut sorted: Vec<(RowId, u64)> = entries.into_iter().collect();
        sorted.sort_by_key(|(id, _)| *id);
        let count = sorted.len() as u64;

        // Write sidecar index with the flushed_upto marker
        let mut idx_file = File::create(idx_path).map_err(StorageError::Io)?;
        idx_file
            .write_all(&count.to_le_bytes())
            .map_err(StorageError::Io)?;
        idx_file
            .write_all(&expected_len.to_le_bytes())
            .map_err(StorageError::Io)?;
        for (row_id, off) in &sorted {
            idx_file
                .write_all(&row_id.to_le_bytes())
                .map_err(StorageError::Io)?;
            idx_file
                .write_all(&off.to_le_bytes())
                .map_err(StorageError::Io)?;
        }
        idx_file.sync_all().map_err(StorageError::Io)?;

        Ok(count)
    }

    /// Look up file offset for a row_id.
    /// Checks LRU first, then mmap binary search, then sidecar file fallback.
    fn lookup_offset(&self, row_id: RowId) -> Option<u64> {
        // Tombstone overlay: deleted ids stay physically present (and in the
        // sidecar) until the next flush — without this check, re-inserting a
        // deleted id hit "already exists".
        if self.tombstones.lock().contains(&row_id) {
            return None;
        }

        // 1. Check LRU cache
        {
            let mut index = self.index.write();
            if let Some(&offset) = index.get(&row_id) {
                return Some(offset);
            }
        }

        let count = *self.index_count.read();
        if count == 0 {
            return None;
        }

        // 2. mmap binary search (zero syscall)
        {
            let guard = self.idx_mmap.read();
            if let Some(ref mmap) = *guard {
                let entry_size = 16usize;
                let mut lo = 0i64;
                let mut hi = count as i64 - 1;

                while lo <= hi {
                    let mid = lo + (hi - lo) / 2;
                    let off = SIDECAR_HEADER_SIZE as usize + mid as usize * entry_size;
                    if off + 16 > mmap.len() {
                        break;
                    }
                    let mid_id = u64::from_le_bytes(mmap[off..off + 8].try_into().ok()?);
                    let mid_offset = u64::from_le_bytes(mmap[off + 8..off + 16].try_into().ok()?);

                    match mid_id.cmp(&row_id) {
                        std::cmp::Ordering::Equal => {
                            drop(guard);
                            self.index.write().put(row_id, mid_offset);
                            return Some(mid_offset);
                        }
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid - 1,
                    }
                }
                return None;
            }
        }

        // 3. Fallback: binary search on sidecar index file
        let mut file = self.index_file.write();
        let entry_size = 16u64; // row_id (8) + offset (8)
        let mut lo = 0i64;
        let mut hi = count as i64 - 1;

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let file_offset = SIDECAR_HEADER_SIZE + mid as u64 * entry_size;
            file.seek(SeekFrom::Start(file_offset)).ok()?;
            let mut buf = [0u8; 16];
            file.read_exact(&mut buf).ok()?;
            let mid_id = u64::from_le_bytes(buf[..8].try_into().ok()?);
            let mid_offset = u64::from_le_bytes(buf[8..].try_into().ok()?);

            match mid_id.cmp(&row_id) {
                std::cmp::Ordering::Equal => {
                    drop(file);
                    self.index.write().put(row_id, mid_offset);
                    return Some(mid_offset);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid - 1,
            }
        }
        None
    }

    /// Get decompressed vector
    pub fn get(&self, row_id: RowId) -> Option<Arc<Vec<f32>>> {
        // 🔑 PERF: read-lock fast path (peek, no LRU touch).
        {
            let cache = self.cache.read();
            if let Some(vec) = cache.peek(&row_id) {
                return Some(Arc::clone(vec));
            }
        }

        let offset = self.lookup_offset(row_id)?;
        let qvec = self.read_quantized(offset).ok()?;
        let vec = self.quantizer.dequantize(&qvec);

        let arc_vec = Arc::new(vec);
        {
            let mut cache = self.cache.write();
            cache.put(row_id, Arc::clone(&arc_vec));
        }

        Some(arc_vec)
    }

    /// Get quantized vector (no decompression)
    pub fn get_quantized(&self, row_id: RowId) -> Option<Arc<QuantizedVector>> {
        // 🔑 PERF: read-lock fast path (peek, no LRU touch). The greedy_search
        // loop calls this hundreds of times per KNN query — every call taking
        // a write lock serializes all concurrent searches.
        {
            let cache = self.quantized_cache.read();
            if let Some(qvec) = cache.peek(&row_id) {
                return Some(Arc::clone(qvec));
            }
        }

        let offset = self.lookup_offset(row_id)?;
        let qvec = self.read_quantized(offset).ok()?;

        let arc_qvec = Arc::new(qvec);
        {
            let mut cache = self.quantized_cache.write();
            cache.put(row_id, Arc::clone(&arc_qvec));
        }

        Some(arc_qvec)
    }

    /// Insert vector (quantize and write)
    pub fn insert(&self, row_id: RowId, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.dimension {
            return Err(StorageError::InvalidData(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.len()
            )));
        }

        // Check if already exists (LRU lookup + sidecar)
        if self.lookup_offset(row_id).is_some() {
            return Err(StorageError::InvalidData(format!(
                "Vector {} already exists",
                row_id
            )));
        }

        let qvec = self.quantizer.quantize(&vector)?;
        let offset = self.append_quantized(row_id, &qvec)?;

        // Update in-memory LRU index; a re-inserted (previously deleted) id
        // comes back to life
        self.index.write().put(row_id, offset);
        self.tombstones.lock().remove(&row_id);
        *self.count.write() += 1;

        // Cache decompressed vector
        {
            let mut cache = self.cache.write();
            cache.put(row_id, Arc::new(vector));
        }

        Ok(())
    }

    /// Batch insert
    pub fn batch_insert(&self, batch: Vec<(RowId, Vec<f32>)>) -> Result<usize> {
        let mut inserted = 0;
        let mut dup = 0usize;
        let mut dim_err = 0usize;
        let mut other_err = 0usize;
        let mut first_other: Option<String> = None;
        for (row_id, vector) in batch {
            match self.insert(row_id, vector) {
                Ok(()) => inserted += 1,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("already exists") {
                        dup += 1;
                    } else if msg.contains("dimension") {
                        dim_err += 1;
                    } else {
                        other_err += 1;
                        if first_other.is_none() {
                            first_other = Some(msg);
                        }
                    }
                }
            }
        }
        if dup + dim_err + other_err > 0 {
            debug_log!(
                "[SQ8Vectors::batch_insert] inserted={}, dup={}, dim_mismatch={}, other={} ({:?})",
                inserted,
                dup,
                dim_err,
                other_err,
                first_other
            );
        }
        Ok(inserted)
    }

    /// Update vector (quantize, persist to disk, update caches)
    ///
    /// Entries are fixed-size, so the new quantized vector OVERWRITES the old
    /// entry in place. The previous append-based implementation broke the
    /// flush path: the sidecar rebuild only covered the first `count` records,
    /// so every updated vector silently reverted to its old value after
    /// flush + reload.
    pub fn update(&self, row_id: RowId, vector: Vec<f32>) -> Result<bool> {
        let offset = match self.lookup_offset(row_id) {
            Some(off) => off,
            None => return Ok(false),
        };

        let qvec = self.quantizer.quantize(&vector)?;
        self.write_quantized_at(offset, row_id, &qvec)?;

        // Update raw vector cache
        {
            let mut cache = self.cache.write();
            cache.put(row_id, Arc::new(vector));
        }
        // Invalidate stale quantized cache entry (next read re-reads disk)
        {
            let mut qcache = self.quantized_cache.write();
            qcache.pop(&row_id);
        }

        // Invalidate mmap — bytes changed under it
        *self.data_mmap.write() = None;

        Ok(true)
    }

    /// Delete vector
    pub fn delete(&self, row_id: RowId) -> Result<bool> {
        // Liveness = LRU/sidecar says present AND not tombstoned. Checking
        // only the LRU made delete a silent no-op after a fresh load (LRU
        // empty, entry only in the sidecar).
        if self.lookup_offset(row_id).is_none() {
            return Ok(false);
        }

        self.index.write().pop(&row_id);
        self.tombstones.lock().insert(row_id);
        *self.count.write() -= 1;
        self.cache.write().pop(&row_id);
        self.quantized_cache.write().pop(&row_id);

        Ok(true)
    }

    /// Flush: update data file header and rebuild sidecar index
    pub fn flush(&self) -> Result<()> {
        let count = *self.count.read();

        // Update data file header with current count
        {
            let mut file = OpenOptions::new()
                .write(true)
                .open(&self.file_path)
                .map_err(StorageError::Io)?;
            file.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
            file.write_all(&count.to_le_bytes())
                .map_err(StorageError::Io)?;
            file.sync_all().map_err(StorageError::Io)?;
        }

        // Always rebuild the sidecar — including when count == 0 (delete-all
        // previously left the old sidecar in place and resurrected every
        // vector on the next load). The rebuild drops tombstoned ids.
        let idx_path = self.file_path.with_extension("idx");
        let live = Self::build_sidecar_index(
            &self.file_path,
            &idx_path,
            self._entry_size,
            Some(&self.tombstones.lock()),
        )?;
        *self.index_count.write() = live;
        self.tombstones.lock().clear();
        let idx_read = File::open(&idx_path).map_err(StorageError::Io)?;
        *self.index_file.write() = idx_read;

        // Remap after flush
        self.remap();

        Ok(())
    }

    /// Get all vector IDs (reads from sidecar index)
    pub fn ids(&self) -> Vec<RowId> {
        let count = *self.index_count.read();
        if count == 0 {
            // Fall back to LRU entries if sidecar is empty (during initial inserts)
            return self.index.read().iter().map(|(&id, _)| id).collect();
        }

        let mut file = self.index_file.write();
        let mut ids = Vec::with_capacity(count as usize);
        let _ = file.seek(SeekFrom::Start(SIDECAR_HEADER_SIZE));
        for _ in 0..count {
            let mut buf = [0u8; 16];
            if file.read_exact(&mut buf).is_ok() {
                ids.push(u64::from_le_bytes(buf[..8].try_into().unwrap()));
            }
        }
        ids
    }

    pub fn len(&self) -> usize {
        *self.count.read() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn memory_usage(&self) -> usize {
        let index_size = self.index.read().len() * 16;
        let cache_size = self.cache.read().len() * (8 + self.dimension * 4);
        index_size + cache_size
    }

    pub fn disk_usage(&self) -> usize {
        std::fs::metadata(&self.file_path)
            .map(|m| m.len() as usize)
            .unwrap_or(0)
    }

    // ==================== Private Helpers ====================

    fn read_quantized(&self, offset: u64) -> Result<QuantizedVector> {
        // Try mmap path (zero syscall)
        {
            let guard = self.data_mmap.read();
            if let Some(ref mmap) = *guard {
                // Entry layout: [row_id: 8] [min: 4] [max: 4] [codes: dimension]
                let off = offset as usize + 8; // skip row_id
                let end = off + 8 + self.dimension;
                if end <= mmap.len() {
                    let min = f32::from_le_bytes(mmap[off..off + 4].try_into().unwrap());
                    let max = f32::from_le_bytes(mmap[off + 4..off + 8].try_into().unwrap());
                    let codes = mmap[off + 8..off + 8 + self.dimension].to_vec();
                    return Ok(QuantizedVector { codes, min, max });
                }
                // mmap out of bounds — stale mmap after append, fall through to seek+read
            }
        }

        // Fallback: seek+read
        let mut file = self.read_file.write();
        file.seek(SeekFrom::Start(offset + 8))
            .map_err(StorageError::Io)?;

        let mut min_bytes = [0u8; 4];
        let mut max_bytes = [0u8; 4];
        file.read_exact(&mut min_bytes).map_err(StorageError::Io)?;
        file.read_exact(&mut max_bytes).map_err(StorageError::Io)?;

        let min = f32::from_le_bytes(min_bytes);
        let max = f32::from_le_bytes(max_bytes);

        let mut codes = vec![0u8; self.dimension];
        file.read_exact(&mut codes).map_err(StorageError::Io)?;

        Ok(QuantizedVector { codes, min, max })
    }

    fn append_quantized(&self, row_id: RowId, qvec: &QuantizedVector) -> Result<u64> {
        let mut file = self.write_file.write();
        let offset = file.seek(SeekFrom::End(0)).map_err(StorageError::Io)?;

        file.write_all(&row_id.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&qvec.min.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&qvec.max.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&qvec.codes).map_err(StorageError::Io)?;

        Ok(offset)
    }

    /// Overwrite a fixed-size entry in place (used by update)
    fn write_quantized_at(&self, offset: u64, row_id: RowId, qvec: &QuantizedVector) -> Result<()> {
        let mut file = self.write_file.write();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        file.write_all(&row_id.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&qvec.min.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&qvec.max.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&qvec.codes).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Remap data and sidecar files after flush
    fn remap(&self) {
        {
            let file = self.read_file.read();
            *self.data_mmap.write() = unsafe { Mmap::map(&*file).ok() };
        }
        {
            let idx = self.index_file.read();
            *self.idx_mmap.write() = unsafe { Mmap::map(&*idx).ok() };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sq8_vectors_basic() {
        let temp_dir = std::env::temp_dir().join("sq8_vectors_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let quantizer = Arc::new(SQ8Quantizer::new(4));
        let storage = SQ8Vectors::create(&temp_dir, quantizer.clone(), 10).unwrap();

        storage.insert(1, vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        storage.insert(2, vec![5.0, 6.0, 7.0, 8.0]).unwrap();

        let v1 = storage.get(1).unwrap();
        assert_eq!(v1.len(), 4);

        let expected = [1.0, 2.0, 3.0, 4.0];
        for (a, &b) in v1.iter().zip(expected.iter()) {
            assert!((a - b).abs() < 0.1);
        }

        storage.flush().unwrap();
        let loaded = SQ8Vectors::load(&temp_dir, quantizer, 10).unwrap();

        assert_eq!(loaded.len(), 2);
        let v1_loaded = loaded.get(1).unwrap();
        assert_eq!(v1_loaded.len(), 4);

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_sq8_vectors_lru_eviction() {
        let temp_dir = std::env::temp_dir().join("sq8_vectors_lru_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let quantizer = Arc::new(SQ8Quantizer::new(4));
        let storage = SQ8Vectors::create(&temp_dir, quantizer.clone(), 2).unwrap(); // tiny LRU

        // Insert 10 vectors (LRU can only hold 2)
        for i in 0..10u64 {
            storage.insert(i, vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
        }
        storage.flush().unwrap();

        // All should still be accessible via sidecar fallback
        for i in 0..10u64 {
            let v = storage.get(i).unwrap();
            assert!((v[0] - i as f32).abs() < 0.1, "Failed for row_id={}", i);
        }

        std::fs::remove_dir_all(&temp_dir).ok();
    }
}
