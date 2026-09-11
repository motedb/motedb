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
use std::collections::{HashMap, HashSet};
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
/// Appended records are mirrored in memory (`Tail`) and written out in one
/// go once this many bytes have accumulated; the mmap is refreshed at the
/// same time, so neighbor reads never need a syscall.
const TAIL_FLUSH_BYTES: usize = 256 << 10;

/// Records appended since the last tail flush. graph.bin `[0, start)` is on
/// disk (and mmap'd); `[start, start + buf.len())` lives only here until
/// `flush_tail` writes it. Bounded by TAIL_FLUSH_BYTES.
struct Tail {
    start: u64,
    buf: Vec<u8>,
}

/// Disk-based graph with bounded memory
pub struct DiskGraph {
    max_degree: usize,
    file: Arc<RwLock<File>>,

    /// mmap of graph.bin — zero-syscall neighbor reads
    mmap: Arc<RwLock<Option<Mmap>>>,

    /// 🔑 Inbound edge counts: node_id → number of edges pointing at it
    /// across all edge lists. Evictions may only drop a node whose inbound
    /// count stays ≥1 — otherwise the node becomes unreachable from the
    /// medoid (the graph has no other reachability oracle).
    inbound: Arc<RwLock<HashMap<RowId, u32>>>,

    /// 🔑 Authoritative COMPLETE offset map: node_id → file offset.
    /// The old bounded LRU lost nodes beyond cache capacity between
    /// flushes — lookups failed and builds/rebuilds stranded every
    /// non-resident node (measured: 1020/2100 reachable after a 1999-node
    /// rebuild). The sidecar file stays the durable form; this map is the
    /// in-memory truth.
    index: Arc<RwLock<HashMap<RowId, u64>>>,

    /// Tracked count of nodes (incremental on set/remove)
    count: Arc<RwLock<u64>>,

    /// LRU cache for adjacency lists
    cache: Arc<Mutex<LruCache<RowId, Arc<Vec<RowId>>>>>,

    /// Pinned hot nodes (bounded)
    hot_nodes: Arc<RwLock<HashSet<RowId>>>,
    hot_cache: Arc<RwLock<LruCache<RowId, Arc<Vec<RowId>>>>>,
    max_hot_nodes: usize,

    /// Next write offset (== tail.start + tail.buf.len())
    next_offset: Arc<Mutex<u64>>,

    /// In-memory mirror of the not-yet-written file tail (see `Tail`).
    tail: Arc<Mutex<Tail>>,

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
            inbound: Arc::new(RwLock::new(HashMap::new())),
            index: Arc::new(RwLock::new(HashMap::new())),
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
            tail: Arc::new(Mutex::new(Tail {
                start: HEADER_SIZE,
                buf: Vec::new(),
            })),
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

        let rw_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;

        // mmap data file for zero-syscall reads
        let data_mmap = unsafe { Mmap::map(&rw_file).ok() };

        // 🔑 Load the COMPLETE node_id → offset map from the sidecar (the
        // sidecar covers every physical record with last-wins semantics:
        // trusted-marker form is post-flush complete; the untrusted form was
        // just rebuilt by build_sidecar_index above).
        let index_map = {
            let mut map: HashMap<RowId, u64> = HashMap::new();
            let mut idx = File::open(&idx_path).map_err(StorageError::Io)?;
            idx.seek(SeekFrom::Start(SIDECAR_HEADER_SIZE))
                .map_err(StorageError::Io)?;
            for _ in 0..index_count {
                let mut buf = [0u8; 16];
                if idx.read_exact(&mut buf).is_err() {
                    break;
                }
                let id = u64::from_le_bytes(buf[..8].try_into().unwrap());
                let off = u64::from_le_bytes(buf[8..16].try_into().unwrap());
                map.insert(id, off);
            }
            map
        };

        // 🔑 Rebuild inbound counts by reading every edge list once.
        let inbound = {
            let mut map: HashMap<RowId, u32> = HashMap::new();
            let mut rf = rw_file.try_clone().map_err(StorageError::Io)?;
            for &off in index_map.values() {
                if let Ok(ns) = read_neighbors_from(&mut rf, off) {
                    for n in ns {
                        *map.entry(n).or_insert(0) += 1;
                    }
                }
            }
            map
        };

        Ok(Self {
            max_degree,
            file: Arc::new(RwLock::new(rw_file)),
            mmap: Arc::new(RwLock::new(data_mmap)),
            inbound: Arc::new(RwLock::new(inbound)),
            index: Arc::new(RwLock::new(index_map)),
            count: Arc::new(RwLock::new(index_count as u64)),
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_capacity.max(1)).unwrap(),
            ))),
            hot_nodes: Arc::new(RwLock::new(HashSet::new())),
            hot_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(max_hot_nodes.max(1)).unwrap(),
            ))),
            max_hot_nodes,
            next_offset: Arc::new(Mutex::new(next_off)),
            tail: Arc::new(Mutex::new(Tail {
                start: next_off,
                buf: Vec::new(),
            })),
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
    /// Look up a node's file offset — O(1) from the complete authoritative
    /// map (inserts add, removes delete, load() populates from sidecar).
    fn lookup_offset(&self, node_id: RowId) -> Option<u64> {
        if self.tombstones.lock().contains(&node_id) {
            return None;
        }
        self.index.read().get(&node_id).copied()
    }

    pub fn max_degree(&self) -> usize {
        self.max_degree
    }

    /// 🔑 Eviction guard: true when `id` has more than one inbound edge, so
    /// removing ONE edge (e.g. from the caller's list) cannot strand it.
    /// Counting is conservative: `false` means "unknown or ≤1" — callers
    /// must treat it as NOT evictable.
    pub fn evictable(&self, id: RowId) -> bool {
        self.inbound.read().get(&id).is_some_and(|c| *c > 1)
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
            let mut v: Vec<RowId> = index.keys().copied().collect();
            v.sort_unstable();
            v.into_iter().take(top_k * 10).collect()
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
        let mut ids: Vec<RowId> = self.index.read().keys().copied().collect();
        ids.sort_unstable();
        ids
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
        // 🔑 Inbound delta bookkeeping: a node's edge list swap changes the
        // inbound counts of every id added/removed. Eviction sites rely on
        // these counts to never strand a node (drop its last inbound edge).
        // Hot/LRU cache first; only an uncached node costs a disk read.
        let old_list: Arc<Vec<RowId>> = self.neighbors(node_id);
        neighbors.retain(|&id| id != node_id);
        neighbors.sort_unstable();
        neighbors.dedup();
        if neighbors.len() > self.max_degree {
            neighbors.truncate(self.max_degree);
        }
        {
            let mut inbound = self.inbound.write();
            for id in old_list.iter().filter(|id| !neighbors.contains(id)) {
                if let Some(c) = inbound.get_mut(id) {
                    *c = c.saturating_sub(1);
                }
            }
            for id in neighbors.iter().filter(|id| !old_list.contains(id)) {
                *inbound.entry(*id).or_insert(0) += 1;
            }
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
            let was_present = idx.insert(node_id, offset).is_none();
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
        {
            let mut inbound = self.inbound.write();
            for n in neighbors.iter() {
                if let Some(c) = inbound.get_mut(n) {
                    *c = c.saturating_sub(1);
                }
            }
            inbound.remove(&node_id);
        }
        self.index.write().remove(&node_id);
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

        // The sidecar is built from the file, so the tail must be on disk.
        self.flush_tail()?;

        // Rebuild sidecar with last-wins semantics minus tombstones. This
        // runs even when the graph is empty: delete-all previously skipped
        // the rebuild, leaving the old sidecar in place and resurrecting
        // every node on the next load.
        let idx_path = self.file_path.with_extension("idx");
        let live =
            Self::build_sidecar_index(&self.file_path, &idx_path, Some(&self.tombstones.lock()))?;
        *self.count.write() = live;
        self.tombstones.lock().clear();

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
        let mut new_entries: Vec<(RowId, u64)> = Vec::with_capacity(ids.len());
        let mut compact_edges: Vec<(RowId, Vec<RowId>)> = Vec::with_capacity(ids.len());
        {
            let mut temp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .map_err(StorageError::Io)?;

            Self::write_header(&mut temp_file, self.max_degree, ids.len())?;
            let mut offset = HEADER_SIZE;

            for &node_id in &ids {
                let neighbors = self.neighbors(node_id);
                compact_edges.push((node_id, neighbors.to_vec()));
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

        // 🔑 Rebuild the authoritative offset map AND inbound counts from
        // the rewrite's (id, offset) pairs — offsets changed, so the old
        // maps are stale.
        {
            let mut idx = self.index.write();
            idx.clear();
            idx.extend(new_entries.iter().copied());
        }
        {
            let mut inbound = self.inbound.write();
            inbound.clear();
            for (_, neighbors) in compact_edges.iter() {
                for n in neighbors {
                    *inbound.entry(*n).or_insert(0) += 1;
                }
            }
        }
        self.cache.lock().clear();
        self.hot_cache.write().clear();
        // The rewrite covers exactly the live set from the old sidecar —
        // any pending tombstones are now physically gone
        self.tombstones.lock().clear();
        *self.count.write() = ids.len() as u64;
        *self.next_offset.lock() = final_offset;
        {
            let mut tail = self.tail.lock();
            tail.start = final_offset;
            tail.buf.clear();
        }

        // Remap after compact
        self.remap();

        *self.dirty.write() = false;
        Ok(())
    }

    pub fn clear(&self) {
        self.index.write().clear();
        self.inbound.write().clear();
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
        *self.count.write() = 0;
        *self.next_offset.lock() = HEADER_SIZE;
        {
            let mut tail = self.tail.lock();
            tail.start = HEADER_SIZE;
            tail.buf.clear();
        }
        // The file was truncated under the map; a stale map past the new EOF
        // would SIGBUS on access.
        self.remap();
        *self.dirty.write() = true;
    }

    pub fn memory_usage(&self) -> usize {
        let cache_size = self.cache.lock().len() * (8 + 4 + 32 * 8);
        let hot_size = self.hot_cache.read().len() * (8 + 4 + 32 * 8);
        let map_size = self.index.read().len() * 24;
        cache_size + hot_size + map_size
    }

    pub fn disk_usage(&self) -> usize {
        let count = self.node_count();
        count * (8 + 4 + 32 * 8)
    }

    // --- Private helpers ---

    /// Remap the data file after flush/compact
    fn remap(&self) {
        let file = self.file.read();
        *self.mmap.write() = unsafe { Mmap::map(&*file).ok() };
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

    /// Append a record at `offset` (always the current end of file). The
    /// bytes go to the in-memory tail; `flush_tail` writes them out in bulk.
    /// The old per-record path did 2 + degree write syscalls, then dropped
    /// the mmap so every uncached read for the rest of a build went through
    /// seek + one read per neighbor.
    fn write_neighbors_at(&self, node_id: RowId, neighbors: &[RowId], offset: u64) -> Result<()> {
        let mut tail = self.tail.lock();
        debug_assert_eq!(offset, tail.start + tail.buf.len() as u64);
        let _ = offset;
        tail.buf.reserve(12 + neighbors.len() * 8);
        tail.buf.extend_from_slice(&node_id.to_le_bytes());
        tail.buf
            .extend_from_slice(&(neighbors.len() as u32).to_le_bytes());
        for &neighbor in neighbors {
            tail.buf.extend_from_slice(&neighbor.to_le_bytes());
        }
        if tail.buf.len() >= TAIL_FLUSH_BYTES {
            self.flush_tail_locked(&mut tail)?;
        }
        Ok(())
    }

    /// Write the in-memory tail to graph.bin and refresh the mmap so those
    /// records are served from the map from now on.
    fn flush_tail(&self) -> Result<()> {
        let mut tail = self.tail.lock();
        self.flush_tail_locked(&mut tail)
    }

    fn flush_tail_locked(&self, tail: &mut Tail) -> Result<()> {
        if tail.buf.is_empty() {
            return Ok(());
        }
        {
            let mut file = self.file.write();
            file.seek(SeekFrom::Start(tail.start))
                .map_err(StorageError::Io)?;
            file.write_all(&tail.buf).map_err(StorageError::Io)?;
        }
        tail.start += tail.buf.len() as u64;
        tail.buf.clear();
        self.remap();
        Ok(())
    }

    fn read_neighbors_at(&self, offset: u64) -> Result<Vec<RowId>> {
        // Not yet written: serve from the in-memory tail.
        {
            let tail = self.tail.lock();
            if offset >= tail.start {
                let rel = (offset - tail.start) as usize;
                let buf = &tail.buf;
                if rel + 12 > buf.len() {
                    return Err(StorageError::InvalidData(format!(
                        "graph record at {offset} beyond tail"
                    )));
                }
                let count = u32::from_le_bytes(buf[rel + 8..rel + 12].try_into().unwrap()) as usize;
                let start = rel + 12;
                let end = start + count * 8;
                if end > buf.len() {
                    return Err(StorageError::InvalidData(format!(
                        "graph record at {offset} truncated in tail"
                    )));
                }
                return Ok(buf[start..end]
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                    .collect());
            }
        }
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

        // Fallback (unmapped tail): header, then the whole edge list in one
        // read — was one read per neighbor.
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        let mut head = [0u8; 12];
        file.read_exact(&mut head).map_err(StorageError::Io)?;
        let count = u32::from_le_bytes(head[8..12].try_into().unwrap()) as usize;
        if count > self.max_degree * 4 + 16 {
            return Err(StorageError::InvalidData(format!(
                "graph record at {offset} claims {count} neighbors"
            )));
        }
        let mut body = vec![0u8; count * 8];
        file.read_exact(&mut body).map_err(StorageError::Io)?;
        Ok(body
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
}

impl Drop for DiskGraph {
    fn drop(&mut self) {
        // Best effort: a shutdown that skipped flush() must not lose the
        // buffered tail (bounded by TAIL_FLUSH_BYTES).
        let _ = self.flush_tail();
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

/// Read one neighbor record at `offset` from an open graph file (used by the
/// load-time inbound-count rebuild before Self exists).
fn read_neighbors_from(file: &mut File, offset: u64) -> Result<Vec<RowId>> {
    use std::io::{Read as _, Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))
        .map_err(StorageError::Io)?;
    let mut buf8 = [0u8; 8];
    let mut buf4 = [0u8; 4];
    if file.read_exact(&mut buf8).is_err() {
        return Ok(Vec::new());
    }
    if file.read_exact(&mut buf4).is_err() {
        return Ok(Vec::new());
    }
    let count = u32::from_le_bytes(buf4) as usize;
    let mut neighbors = Vec::with_capacity(count);
    for _ in 0..count {
        if file.read_exact(&mut buf8).is_err() {
            break;
        }
        neighbors.push(u64::from_le_bytes(buf8));
    }
    Ok(neighbors)
}
