//! Disk-based graph storage with bounded memory for DiskANN
//!
//! Stores Vamana graph adjacency list on disk with LRU cache.
//! Offset index is LRU-bounded, falling back to binary search on a
//! sidecar index file (graph.idx).

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

const MAGIC: u32 = 0x4752_5048; // "GRPH"
const VERSION: u32 = 1;
const HEADER_SIZE: u64 = 16;

/// Sidecar header: [count: u64][flushed_upto: u64][entries...]
/// `flushed_upto` = graph.bin length at the last flush; the loader trusts the
/// sidecar only while the file hasn't grown past it. Old-format sidecars
/// (8-byte header) are detected by size and force a one-time rebuild.
const SIDECAR_HEADER_SIZE: u64 = 16;

/// Disk-based graph with bounded memory
pub struct DiskGraph {
    max_degree: usize,
    file: Arc<RwLock<File>>,

    /// mmap of graph.bin — zero-syscall neighbor reads
    mmap: Arc<RwLock<Option<Mmap>>>,
    /// mmap of graph.idx sidecar — zero-syscall offset lookups
    idx_mmap: Arc<RwLock<Option<Mmap>>>,

    /// Bounded offset index: row_id → file offset (LRU-capped)
    index: Arc<RwLock<LruCache<RowId, u64>>>,

    /// Sidecar index file handle (fallback when mmap unavailable)
    index_file: Arc<RwLock<File>>,
    index_count: Arc<RwLock<u64>>,
    /// Tracked count of nodes (incremental on set/remove)
    count: Arc<RwLock<u64>>,

    /// LRU cache for adjacency lists
    cache: Arc<Mutex<LruCache<RowId, Arc<Vec<RowId>>>>>,

    /// Pinned hot nodes (bounded)
    hot_nodes: Arc<RwLock<HashSet<RowId>>>,
    hot_cache: Arc<RwLock<LruCache<RowId, Arc<Vec<RowId>>>>>,
    max_hot_nodes: usize,

    /// Next write offset
    next_offset: Arc<Mutex<u64>>,

    /// Dirty flag
    dirty: Arc<RwLock<bool>>,

    /// Serializes flush with set_neighbors to prevent stale node_count
    /// in sidecar index (flush acquires exclusively, set_neighbors shared)
    flush_lock: Arc<Mutex<()>>,

    /// Nodes deleted since the last flush. graph.bin is append-only, so the
    /// deleted node's record (and its sidecar entry) physically remain until
    /// the next flush filters them out.
    tombstones: Arc<Mutex<HashSet<RowId>>>,

    file_path: PathBuf,
}

impl DiskGraph {
    /// Create new disk graph
    pub fn create(
        data_dir: impl AsRef<Path>,
        max_degree: usize,
        cache_capacity: usize,
    ) -> Result<Self> {
        Self::create_with_hot_limit(data_dir, max_degree, cache_capacity, 100)
    }

    /// Create with explicit hot node limit
    pub fn create_with_hot_limit(
        data_dir: impl AsRef<Path>,
        max_degree: usize,
        cache_capacity: usize,
        max_hot_nodes: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir).map_err(StorageError::Io)?;

        let file_path = data_dir.join("graph.bin");
        let idx_path = data_dir.join("graph.idx");

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;

        Self::write_header(&mut file, max_degree, 0)?;

        // Create empty sidecar index
        let mut idx = File::create(&idx_path).map_err(StorageError::Io)?;
        idx.write_all(&0u64.to_le_bytes())
            .map_err(StorageError::Io)?;

        Ok(Self {
            max_degree,
            file: Arc::new(RwLock::new(file)),
            mmap: Arc::new(RwLock::new(None)),
            idx_mmap: Arc::new(RwLock::new(None)),
            index: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_capacity.max(1)).unwrap(),
            ))),
            index_file: Arc::new(RwLock::new(
                File::open(&idx_path).map_err(StorageError::Io)?,
            )),
            index_count: Arc::new(RwLock::new(0)),
            count: Arc::new(RwLock::new(0)),
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_capacity.max(1)).unwrap(),
            ))),
            hot_nodes: Arc::new(RwLock::new(HashSet::new())),
            hot_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(max_hot_nodes.max(1)).unwrap(),
            ))),
            max_hot_nodes,
            next_offset: Arc::new(Mutex::new(HEADER_SIZE)),
            dirty: Arc::new(RwLock::new(false)),
            flush_lock: Arc::new(Mutex::new(())),
            tombstones: Arc::new(Mutex::new(HashSet::new())),
            file_path,
        })
    }

    /// Load existing disk graph
    pub fn load(data_dir: impl AsRef<Path>, cache_capacity: usize) -> Result<Self> {
        Self::load_with_hot_limit(data_dir, cache_capacity, 100)
    }

    /// Load with explicit hot node limit
    pub fn load_with_hot_limit(
        data_dir: impl AsRef<Path>,
        cache_capacity: usize,
        max_hot_nodes: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let file_path = data_dir.join("graph.bin");
        let idx_path = data_dir.join("graph.idx");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;

        let (max_degree, _node_count) = Self::read_header(&mut file)?;
        let graph_len = file.metadata().map_err(StorageError::Io)?.len();

        // Full scan of every record in the file: the file is an append-only
        // log (updates append a newer record for the same node), so the true
        // append point is EOF, not "after the first `count` records" — that
        // stale assumption made post-reload set_neighbors overwrite live
        // records mid-file.
        let (scan_eof, truncated) = Self::scan_eof(&mut file)?;
        let next_off = if truncated {
            // Torn trailing record (crash mid-append): drop it so later
            // appends and future scans stay well-formed.
            file.set_len(scan_eof).map_err(StorageError::Io)?;
            scan_eof
        } else {
            graph_len.min(scan_eof)
        };
        let effective_len = next_off;

        // Sidecar: trust it only in the new format AND only while graph.bin
        // hasn't grown past the flush marker. Otherwise rebuild with
        // last-wins semantics (tombstones are unknown on this path — this is
        // the crash-recovery path; the DB layer rebuilds the whole index for
        // tables with replayed WAL records).
        let index_count = if idx_path.exists() {
            let mut idx = File::open(&idx_path).map_err(StorageError::Io)?;
            let mut buf = [0u8; 8];
            idx.read_exact(&mut buf).map_err(StorageError::Io)?;
            let sidecar_count = u64::from_le_bytes(buf);
            let idx_len = idx.metadata().map(|m| m.len()).unwrap_or(0);
            let new_format = idx_len == SIDECAR_HEADER_SIZE + sidecar_count * 16;
            let trusted = if new_format {
                let mut marker = [0u8; 8];
                idx.read_exact(&mut marker).is_ok() && u64::from_le_bytes(marker) == effective_len
            } else {
                false
            };
            if trusted {
                sidecar_count
            } else {
                Self::build_sidecar_index(&file_path, &idx_path, None)?
            }
        } else {
            Self::build_sidecar_index(&file_path, &idx_path, None)?
        };

        let idx_read = File::open(&idx_path).map_err(StorageError::Io)?;

        let rw_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;

        // mmap data file and sidecar for zero-syscall reads
        let data_mmap = unsafe { Mmap::map(&rw_file).ok() };
        let idx_mmap = unsafe { Mmap::map(&idx_read).ok() };

        Ok(Self {
            max_degree,
            file: Arc::new(RwLock::new(rw_file)),
            mmap: Arc::new(RwLock::new(data_mmap)),
            idx_mmap: Arc::new(RwLock::new(idx_mmap)),
            index: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(cache_capacity.max(1)).unwrap(),
            ))),
            index_file: Arc::new(RwLock::new(idx_read)),
            index_count: Arc::new(RwLock::new(index_count)),
            count: Arc::new(RwLock::new(index_count)),
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_capacity.max(1)).unwrap(),
            ))),
            hot_nodes: Arc::new(RwLock::new(HashSet::new())),
            hot_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(max_hot_nodes.max(1)).unwrap(),
            ))),
            max_hot_nodes,
            next_offset: Arc::new(Mutex::new(next_off)),
            dirty: Arc::new(RwLock::new(false)),
            flush_lock: Arc::new(Mutex::new(())),
            tombstones: Arc::new(Mutex::new(HashSet::new())),
            file_path,
        })
    }

    /// Walk every record in graph.bin from the header to the first parse
    /// failure. Returns (offset after the last well-formed record, torn?).
    /// `torn == true` means the file contains bytes past `eof` (a partially
    /// written record).
    fn scan_eof(file: &mut File) -> Result<(u64, bool)> {
        let file_len = file.metadata().map_err(StorageError::Io)?.len();
        file.seek(SeekFrom::Start(HEADER_SIZE))
            .map_err(StorageError::Io)?;
        let mut offset = HEADER_SIZE;
        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];

        loop {
            if file.read_exact(&mut buf8).is_err() {
                break;
            }
            if file.read_exact(&mut buf4).is_err() {
                break;
            }
            let ncount = u32::from_le_bytes(buf4) as usize;
            let record_size = 8 + 4 + (ncount * 8);
            if offset + record_size as u64 > file_len {
                // Neighbor bytes run past EOF: torn record
                return Ok((offset, true));
            }
            if file.seek(SeekFrom::Current((ncount * 8) as i64)).is_err() {
                break;
            }
            offset += record_size as u64;
        }

        let torn = offset != file_len;
        Ok((offset, torn))
    }

    /// Rebuild the sidecar index by scanning graph.bin.
    ///
    /// graph.bin is an append-only log: `set_neighbors` on an existing node
    /// APPENDS a newer record, and deletes leave the old record behind. The
    /// scan therefore covers every record with LAST-WINS semantics per
    /// node_id (minus tombstones when known). The previous implementation
    /// scanned only the first `node_count` records, which reverted every
    /// post-build edge update, resurrected deleted nodes, and dropped the
    /// newest nodes from the sidecar after any delete.
    fn build_sidecar_index(
        data_path: &Path,
        idx_path: &Path,
        tombstones: Option<&HashSet<RowId>>,
    ) -> Result<u64> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_path)
            .map_err(StorageError::Io)?;
        let file_len = file.metadata().map_err(StorageError::Io)?.len();
        file.seek(SeekFrom::Start(HEADER_SIZE))
            .map_err(StorageError::Io)?;

        let mut entries: std::collections::HashMap<RowId, u64> = std::collections::HashMap::new();
        let mut buf8 = [0u8; 8];
        let mut buf4 = [0u8; 4];
        let mut offset = HEADER_SIZE;
        let mut well_formed_len = HEADER_SIZE;

        loop {
            if file.read_exact(&mut buf8).is_err() {
                break;
            }
            if file.read_exact(&mut buf4).is_err() {
                break;
            }
            let node_id = u64::from_le_bytes(buf8);
            let ncount = u32::from_le_bytes(buf4) as usize;
            let record_size = 8 + 4 + (ncount * 8);
            if offset + record_size as u64 > file_len {
                break; // torn trailing record
            }
            if file.seek(SeekFrom::Current((ncount * 8) as i64)).is_err() {
                break;
            }

            if tombstones.is_none_or(|t| !t.contains(&node_id)) {
                // later record wins for duplicate node ids
                entries.insert(node_id, offset);
            }
            offset += record_size as u64;
            well_formed_len = offset;
        }

        // Drop a torn tail so the file stays a whole number of records
        if well_formed_len < file_len {
            file.set_len(well_formed_len).map_err(StorageError::Io)?;
        }

        let mut sorted: Vec<(RowId, u64)> = entries.into_iter().collect();
        sorted.sort_by_key(|(id, _)| *id);
        let count = sorted.len() as u64;

        let mut idx_file = File::create(idx_path).map_err(StorageError::Io)?;
        idx_file
            .write_all(&count.to_le_bytes())
            .map_err(StorageError::Io)?;
        idx_file
            .write_all(&well_formed_len.to_le_bytes())
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

    /// Look up file offset: tombstone check → LRU → mmap binary search →
    /// sidecar file fallback
    fn lookup_offset(&self, node_id: RowId) -> Option<u64> {
        if self.tombstones.lock().contains(&node_id) {
            return None;
        }
        {
            let mut index = self.index.write();
            if let Some(&offset) = index.get(&node_id) {
                return Some(offset);
            }
        }

        let count = *self.index_count.read();
        if count == 0 {
            return None;
        }

        // Try mmap path first (zero syscall)
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

                    match mid_id.cmp(&node_id) {
                        std::cmp::Ordering::Equal => {
                            drop(guard);
                            self.index.write().put(node_id, mid_offset);
                            return Some(mid_offset);
                        }
                        std::cmp::Ordering::Less => lo = mid + 1,
                        std::cmp::Ordering::Greater => hi = mid - 1,
                    }
                }
                return None;
            }
        }

        // Fallback: seek+read on sidecar file
        let mut file = self.index_file.write();
        let entry_size = 16u64;
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

            match mid_id.cmp(&node_id) {
                std::cmp::Ordering::Equal => {
                    drop(file);
                    self.index.write().put(node_id, mid_offset);
                    return Some(mid_offset);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid - 1,
            }
        }
        None
    }

    pub fn max_degree(&self) -> usize {
        self.max_degree
    }

    pub fn node_count(&self) -> usize {
        *self.count.read() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.node_count() == 0
    }

    /// Pin hot node (bounded — evicts oldest when at capacity)
    pub fn pin_hot_node(&self, node_id: RowId) {
        if self.hot_nodes.read().contains(&node_id) {
            return;
        }
        if let Some(neighbors) = self.get_from_cache_or_disk(node_id) {
            // Evict from hot_cache if at capacity
            if self.hot_nodes.read().len() >= self.max_hot_nodes {
                // Find oldest hot node (first entry in LRU)
                let _to_evict = {
                    let mut hc = self.hot_cache.write();
                    if let Some((evict_id, _)) = hc.pop_lru() {
                        self.hot_nodes.write().remove(&evict_id);
                        drop(hc);
                        Some(evict_id)
                    } else {
                        None
                    }
                };
            }
            self.hot_cache.write().put(node_id, neighbors);
            self.hot_nodes.write().insert(node_id);
        }
    }

    /// Batch pin high-degree nodes
    pub fn pin_high_degree_nodes(&self, top_k: usize) {
        // Sample a subset of IDs to avoid loading all
        let ids: Vec<RowId> = {
            let index = self.index.read();
            index.iter().map(|(&id, _)| id).take(top_k * 10).collect()
        };

        let mut degrees: Vec<(RowId, usize)> = ids
            .iter()
            .map(|&id| (id, self.neighbors(id).len()))
            .collect();
        degrees.sort_by(|a, b| b.1.cmp(&a.1));

        for (id, _) in degrees.into_iter().take(top_k) {
            self.pin_hot_node(id);
        }
    }

    pub fn node_ids(&self) -> Vec<RowId> {
        let count = *self.index_count.read();
        if count > 0 {
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
        } else {
            self.index.read().iter().map(|(&id, _)| id).collect()
        }
    }

    /// Add node (without neighbors)
    pub fn add_node(&self, node_id: RowId) {
        if self.lookup_offset(node_id).is_some() {
            return;
        }
        *self.dirty.write() = true;
    }

    /// Get neighbors with tiered caching: hot → LRU → disk.
    /// Uses `peek` (no LRU promotion) to avoid write lock contention
    /// during parallel graph construction.
    pub fn neighbors(&self, node_id: RowId) -> Arc<Vec<RowId>> {
        // 1. Hot cache (peek = no LRU promotion, only needs &self)
        {
            let hot = self.hot_cache.read();
            if let Some(n) = hot.peek(&node_id) {
                return Arc::clone(n);
            }
        }
        // 2. LRU cache (peek = no promotion)
        {
            let cache = self.cache.lock();
            if let Some(n) = cache.peek(&node_id) {
                return Arc::clone(n);
            }
        }
        // 3. Disk (populates cache for future lookups)
        match self.get_from_cache_or_disk(node_id) {
            Some(n) => n,
            None => Arc::new(Vec::new()),
        }
    }

    fn get_from_cache_or_disk(&self, node_id: RowId) -> Option<Arc<Vec<RowId>>> {
        let offset = self.lookup_offset(node_id)?;
        match self.read_neighbors_at(offset) {
            Ok(neighbors) => {
                let arc = Arc::new(neighbors);
                self.cache.lock().put(node_id, Arc::clone(&arc));
                Some(arc)
            }
            Err(_) => None,
        }
    }

    /// Set neighbors (replaces existing)
    pub fn set_neighbors(&self, node_id: RowId, mut neighbors: Vec<RowId>) -> Result<()> {
        // Block during flush to prevent sidecar from being built with stale node_count
        let _flush_guard = self.flush_lock.lock();
        neighbors.retain(|&id| id != node_id);
        neighbors.sort_unstable();
        neighbors.dedup();
        if neighbors.len() > self.max_degree {
            neighbors.truncate(self.max_degree);
        }

        let offset = {
            let mut next_offset = self.next_offset.lock();
            let offset = *next_offset;
            self.write_neighbors_at(node_id, &neighbors, offset)?;
            let record_size = 8 + 4 + (neighbors.len() * 8);
            *next_offset += record_size as u64;
            offset
        };

        let is_new = {
            let mut idx = self.index.write();
            let was_present = idx.get(&node_id).is_some();
            idx.put(node_id, offset);
            !was_present
        };
        // A re-inserted node comes back to life
        self.tombstones.lock().remove(&node_id);
        if is_new {
            *self.count.write() += 1;
        }

        let arc = Arc::new(neighbors);
        if self.hot_nodes.read().contains(&node_id) {
            self.hot_cache.write().put(node_id, arc);
        } else {
            self.cache.lock().put(node_id, arc);
        }

        *self.dirty.write() = true;

        // Invalidate mmap — file has grown but mmap still maps the old range.
        // Subsequent reads will use seek+read fallback until next flush() remaps.
        *self.mmap.write() = None;

        Ok(())
    }

    /// Remove node
    pub fn remove_node(&self, node_id: RowId) -> Arc<Vec<RowId>> {
        // Presence via the same tombstone-aware lookup readers use: after a
        // fresh load the LRU is empty and the sidecar is the only source of
        // truth, so an LRU-only check made delete-then-delete over-decrement
        // the count and made delete-after-load a silent no-op.
        let was_present = self.lookup_offset(node_id).is_some();
        if !was_present {
            return Arc::new(Vec::new());
        }
        let neighbors = self.neighbors(node_id);
        self.index.write().pop(&node_id);
        self.cache.lock().pop(&node_id);
        self.hot_nodes.write().remove(&node_id);
        self.hot_cache.write().pop(&node_id);
        self.tombstones.lock().insert(node_id);
        // 🔑 Split read/write into two statements: in the one-liner
        // `*self.count.write() = self.count.read().saturating_sub(1)`
        // the RHS read guard is a temporary that lives until the END of
        // the statement, so evaluating the LHS write blocks on a reader
        // held by this very thread — a self-deadlock that hung every
        // UPDATE/DELETE on a vector-indexed column.
        let c = *self.count.read();
        *self.count.write() = c.saturating_sub(1);
        *self.dirty.write() = true;
        neighbors
    }

    /// Flush to disk — blocks concurrent set_neighbors to prevent
    /// the sidecar index from being built with stale node_count.
    pub fn flush(&self) -> Result<()> {
        if !*self.dirty.read() {
            return Ok(());
        }

        // Prevent concurrent writes during flush so node_count and
        // sidecar index are consistent. Flush is infrequent enough
        // that the serialization cost is negligible.
        let _flush_guard = self.flush_lock.lock();
        // Re-check dirty after acquiring lock (could have been flushed
        // by another thread while we waited).
        if !*self.dirty.read() {
            return Ok(());
        }

        // Rebuild sidecar with last-wins semantics minus tombstones. This
        // runs even when the graph is empty: delete-all previously skipped
        // the rebuild, leaving the old sidecar in place and resurrecting
        // every node on the next load.
        let idx_path = self.file_path.with_extension("idx");
        let live =
            Self::build_sidecar_index(&self.file_path, &idx_path, Some(&self.tombstones.lock()))?;
        *self.index_count.write() = live;
        *self.count.write() = live;
        self.tombstones.lock().clear();
        let idx_read = File::open(&idx_path).map_err(StorageError::Io)?;
        *self.index_file.write() = idx_read;

        {
            let mut file = self.file.write();
            Self::write_header(&mut file, self.max_degree, live as usize)?;
            file.sync_all().map_err(StorageError::Io)?;
        }

        // Remap after flush
        self.remap();

        *self.dirty.write() = false;
        Ok(())
    }

    /// Compact graph file (full rewrite)
    pub fn compact(&self) -> Result<()> {
        let _guard = self.flush_lock.lock(); // Prevent concurrent set_neighbors during compact
        let temp_path = self.file_path.with_extension("tmp");
        let idx_path = self.file_path.with_extension("idx");

        // Get all node IDs from sidecar (the live set)
        let ids = self.node_ids();
        let final_offset;
        {
            let mut temp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .map_err(StorageError::Io)?;

            Self::write_header(&mut temp_file, self.max_degree, ids.len())?;

            let mut new_entries: Vec<(RowId, u64)> = Vec::with_capacity(ids.len());
            let mut offset = HEADER_SIZE;

            for &node_id in &ids {
                let neighbors = self.neighbors(node_id);
                temp_file
                    .write_all(&node_id.to_le_bytes())
                    .map_err(StorageError::Io)?;
                temp_file
                    .write_all(&(neighbors.len() as u32).to_le_bytes())
                    .map_err(StorageError::Io)?;
                for &neighbor in neighbors.iter() {
                    temp_file
                        .write_all(&neighbor.to_le_bytes())
                        .map_err(StorageError::Io)?;
                }
                new_entries.push((node_id, offset));
                let record_size = 8 + 4 + (neighbors.len() * 8);
                offset += record_size as u64;
            }

            temp_file.sync_all().map_err(StorageError::Io)?;

            // Write new sidecar (with the flushed_upto marker)
            new_entries.sort_by_key(|(id, _)| *id);
            let mut idx_file = File::create(&idx_path).map_err(StorageError::Io)?;
            let count = new_entries.len() as u64;
            idx_file
                .write_all(&count.to_le_bytes())
                .map_err(StorageError::Io)?;
            idx_file
                .write_all(&offset.to_le_bytes())
                .map_err(StorageError::Io)?;
            for (row_id, off) in &new_entries {
                idx_file
                    .write_all(&row_id.to_le_bytes())
                    .map_err(StorageError::Io)?;
                idx_file
                    .write_all(&off.to_le_bytes())
                    .map_err(StorageError::Io)?;
            }
            idx_file.sync_all().map_err(StorageError::Io)?;

            final_offset = offset;
        }

        std::fs::rename(&temp_path, &self.file_path).map_err(StorageError::Io)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)
            .map_err(StorageError::Io)?;
        *self.file.write() = file;

        let idx_read = File::open(&idx_path).map_err(StorageError::Io)?;
        *self.index_file.write() = idx_read;
        // Clear LRU index (offsets changed) and neighbor caches (stale data)
        self.index.write().clear();
        self.cache.lock().clear();
        self.hot_cache.write().clear();
        // The rewrite covers exactly the live set from the old sidecar —
        // any pending tombstones are now physically gone
        self.tombstones.lock().clear();
        *self.index_count.write() = ids.len() as u64;
        *self.count.write() = ids.len() as u64;
        *self.next_offset.lock() = final_offset;

        // Remap after compact
        self.remap();

        *self.dirty.write() = false;
        Ok(())
    }

    pub fn clear(&self) {
        self.index.write().clear();
        self.cache.lock().clear();
        self.hot_nodes.write().clear();
        self.hot_cache.write().clear();
        self.tombstones.lock().clear();
        // Reset the on-disk state too: flush() rebuilds the sidecar from
        // graph.bin, so leaving the records in place resurrected everything.
        {
            let mut file = self.file.write();
            let _ = Self::write_header(&mut file, self.max_degree, 0);
            let _ = file.set_len(HEADER_SIZE);
        }
        let idx_path = self.file_path.with_extension("idx");
        if let Ok(mut idx_file) = File::create(&idx_path) {
            let _ = idx_file.write_all(&0u64.to_le_bytes());
            let _ = idx_file.write_all(&HEADER_SIZE.to_le_bytes());
        }
        *self.index_count.write() = 0;
        *self.count.write() = 0;
        *self.next_offset.lock() = HEADER_SIZE;
        *self.dirty.write() = true;
    }

    pub fn memory_usage(&self) -> usize {
        let cache_size = self.cache.lock().len() * (8 + 4 + 32 * 8);
        let hot_size = self.hot_cache.read().len() * (8 + 4 + 32 * 8);
        cache_size + hot_size
    }

    pub fn disk_usage(&self) -> usize {
        let count = self.node_count();
        count * (8 + 4 + 32 * 8)
    }

    // --- Private helpers ---

    /// Remap data and sidecar files after flush/compact
    fn remap(&self) {
        {
            let file = self.file.read();
            *self.mmap.write() = unsafe { Mmap::map(&*file).ok() };
        }
        {
            let idx = self.index_file.read();
            *self.idx_mmap.write() = unsafe { Mmap::map(&*idx).ok() };
        }
    }

    fn write_header(file: &mut File, max_degree: usize, node_count: usize) -> Result<()> {
        file.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
        file.write_all(&MAGIC.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&VERSION.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&(max_degree as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&(node_count as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        Ok(())
    }

    fn read_header(file: &mut File) -> Result<(usize, usize)> {
        file.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
        let mut buf = [0u8; 4];

        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let magic = u32::from_le_bytes(buf);
        if magic != MAGIC {
            return Err(StorageError::InvalidData("Invalid graph file".to_string()));
        }

        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let _version = u32::from_le_bytes(buf);
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let max_degree = u32::from_le_bytes(buf) as usize;
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let node_count = u32::from_le_bytes(buf) as usize;

        Ok((max_degree, node_count))
    }

    fn write_neighbors_at(&self, node_id: RowId, neighbors: &[RowId], offset: u64) -> Result<()> {
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        file.write_all(&node_id.to_le_bytes())
            .map_err(StorageError::Io)?;
        file.write_all(&(neighbors.len() as u32).to_le_bytes())
            .map_err(StorageError::Io)?;
        for &neighbor in neighbors {
            file.write_all(&neighbor.to_le_bytes())
                .map_err(StorageError::Io)?;
        }
        Ok(())
    }

    fn read_neighbors_at(&self, offset: u64) -> Result<Vec<RowId>> {
        // Try mmap path (zero syscall)
        {
            let guard = self.mmap.read();
            if let Some(ref mmap) = *guard {
                let off = offset as usize;
                if off + 12 <= mmap.len() {
                    let _node_id = u64::from_le_bytes(mmap[off..off + 8].try_into().unwrap());
                    let count =
                        u32::from_le_bytes(mmap[off + 8..off + 12].try_into().unwrap()) as usize;
                    let neighbors_start = off + 12;
                    let neighbors_end = neighbors_start + count * 8;
                    if neighbors_end <= mmap.len() {
                        let mut neighbors = Vec::with_capacity(count);
                        for i in 0..count {
                            let n_off = neighbors_start + i * 8;
                            neighbors.push(u64::from_le_bytes(
                                mmap[n_off..n_off + 8].try_into().unwrap(),
                            ));
                        }
                        return Ok(neighbors);
                    }
                }
                // mmap out of bounds — stale mmap after write, fall through to seek+read
            }
        }

        // Fallback: seek+read
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8).map_err(StorageError::Io)?;
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4).map_err(StorageError::Io)?;
        let count = u32::from_le_bytes(buf4) as usize;

        let mut neighbors = Vec::with_capacity(count);
        for _ in 0..count {
            file.read_exact(&mut buf8).map_err(StorageError::Io)?;
            neighbors.push(u64::from_le_bytes(buf8));
        }
        Ok(neighbors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_disk_graph_create() {
        let temp_dir = TempDir::new().unwrap();
        let graph = DiskGraph::create(temp_dir.path(), 32, 1000).unwrap();
        assert_eq!(graph.max_degree(), 32);
        assert!(graph.is_empty());
    }

    #[test]
    fn test_disk_graph_neighbors() {
        let temp_dir = TempDir::new().unwrap();
        let graph = DiskGraph::create(temp_dir.path(), 32, 1000).unwrap();
        graph.set_neighbors(1, vec![2, 3, 4]).unwrap();
        let neighbors = graph.neighbors(1);
        assert_eq!(neighbors.len(), 3);
        assert!(neighbors.contains(&2));
    }

    #[test]
    fn test_disk_graph_persistence() {
        let temp_dir = TempDir::new().unwrap();

        {
            let graph = DiskGraph::create(temp_dir.path(), 32, 1000).unwrap();
            graph.set_neighbors(1, vec![2, 3]).unwrap();
            graph.set_neighbors(2, vec![1, 3]).unwrap();
            graph.flush().unwrap();
        }

        {
            let graph = DiskGraph::load(temp_dir.path(), 1000).unwrap();
            assert_eq!(graph.node_count(), 2);
            let n1 = graph.neighbors(1);
            assert!(n1.contains(&2) && n1.contains(&3));
        }
    }

    #[test]
    fn test_disk_graph_lru_eviction() {
        let temp_dir = TempDir::new().unwrap();
        let graph = DiskGraph::create_with_hot_limit(temp_dir.path(), 32, 2, 5).unwrap();

        // Insert 10 nodes (LRU only holds 2)
        for i in 0..10u64 {
            graph.set_neighbors(i, vec![(i + 1) % 10]).unwrap();
        }
        graph.flush().unwrap();

        // All should be accessible via sidecar fallback
        for i in 0..10u64 {
            let n = graph.neighbors(i);
            assert_eq!(n.len(), 1, "node {} should have 1 neighbor", i);
        }
    }
}
