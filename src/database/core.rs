//! Database Core - MoteDB Structure and Initialization
//!
//! Extracted from database_legacy.rs (4,798 lines) as part of modularization
//! This module contains:
//! - MoteDB struct definition
//! - create() / create_with_config()
//! - open() with WAL recovery
//! - Index loading helpers

use crate::cache::RowCache;
use crate::catalog::TableRegistry;
use crate::config::DBConfig;
use crate::index::btree::{BTree, BTreeConfig};
use crate::index::column_value::ColumnValueIndex;
use crate::index::ioctree::IOctreeIndex;
use crate::index::text_fts::TextFTSIndex;
use crate::index::vamana::{DiskANNIndex, VamanaConfig};
use crate::storage::LSMEngine;
use crate::txn::coordinator::TransactionCoordinator;
use crate::txn::version_store::VersionStore;
use crate::txn::wal::{WALManager, WALRecord};
use crate::types::RowId;
use crate::{MoteDBError, Result, StorageError};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::sync::{Arc, Mutex};

/// Vector index statistics

/// MoteDB instance
pub struct MoteDB {
    /// Database file path
    pub(crate) path: PathBuf,

    /// WAL manager
    pub(crate) wal: Arc<WALManager>,

    /// LSM-Tree storage engine (main data storage)
    pub(crate) lsm_engine: Arc<LSMEngine>,

    /// Timestamp index (using BTree for persistent storage)
    pub(crate) timestamp_index: Arc<RwLock<BTree>>,

    /// Next row ID (lock-free atomic counter)
    pub(crate) next_row_id: Arc<AtomicU64>,

    /// Global write LSN — monotonically increasing counter used as the unified
    /// timestamp for all LSM Value writes. Initialized to max(current_time_micros,
    /// max_row_id + 1) on open, guaranteeing it exceeds any pre-existing timestamp.
    pub(crate) write_lsn: Arc<AtomicU64>,

    /// 🚀 Phase 4: Per-table AUTO_INCREMENT counters
    /// Format: table_name → next_id
    /// 🚀 Optimized: DashMap for lock-free reads after first insert per table.
    ///    First insert acquires shard lock, subsequent inserts are lock-free (AtomicI64 only).
    pub(crate) table_auto_increment: Arc<DashMap<String, Arc<AtomicI64>>>,

    /// Number of partitions
    pub(crate) num_partitions: u8,

    /// Transaction coordinator
    pub(crate) txn_coordinator: Arc<TransactionCoordinator>,

    /// Version store for MVCC
    pub(crate) version_store: Arc<VersionStore>,

    /// Pending index updates counter (for triggering background flush)
    /// 🚀 P0 CRITICAL FIX: 使用 AtomicUsize 避免锁竞争，解决 CPU 飙升问题
    pub(crate) pending_updates: Arc<std::sync::atomic::AtomicUsize>,

    /// 🚀 Vector indexes (DiskANN) - 使用 DashMap 提升并发性能
    pub(crate) vector_indexes: Arc<DashMap<String, Arc<RwLock<DiskANNIndex>>>>,

    /// i-Octree indexes (3D point cloud) for embodied intelligence
    pub(crate) ioctree_indexes: Arc<DashMap<String, Arc<RwLock<IOctreeIndex>>>>,

    /// 🚀 Text indexes (FTS with single-file B-Tree) - 使用 DashMap 提升并发性能
    pub(crate) text_indexes: Arc<DashMap<String, Arc<RwLock<TextFTSIndex>>>>,

    /// 🚀 Column value indexes (for WHERE optimization) - 使用 DashMap 提升并发性能
    pub(crate) column_indexes: Arc<DashMap<String, Arc<ColumnValueIndex>>>,

    /// Columnar segment store for TimeSeries tables (Gorilla-compressed immutable segments)
    pub(crate) columnar_store: Arc<crate::storage::ColumnarStore>,

    /// Columnar SSTable for this table (if compaction has produced one).
    /// Stored per table: table_name → ColumnarSSTable
    pub(crate) columnar_sstables:
        Arc<DashMap<String, Arc<crate::storage::lsm::columnar::ColumnarSSTable>>>,

    /// Columnar write buffer — accumulates INSERT rows across batches.
    /// Zero encoding overhead: Values pushed directly to per-column arrays.
    /// Finalized during flush/vacuum to a columnar SSTable.
    pub(crate) columnar_write_bufs: Arc<
        DashMap<
            String,
            Arc<parking_lot::Mutex<crate::storage::lsm::columnar::ColumnarSSTableBuilder>>,
        >,
    >,

    /// 🆕 Multi-segment columnar store (append-only + compaction).
    /// Replaces the single-SSTable full-rewrite path. Each table that opts in
    /// gets a ColSegmentStore; writes append delta segments (O(1)), reads
    /// multi-way merge. Coexists with the legacy fields during migration.
    pub(crate) col_segment_stores:
        Arc<DashMap<String, Arc<crate::storage::col_segment::ColSegmentStore>>>,

    /// 🚀 In-memory PK lookup: table_name → (PK_value_key → RowId)
    /// Bypasses disk-based column index for O(1) PK → row_id resolution.
    /// Only populated for non-AUTO_INCREMENT primary keys.
    /// Bounded by LRU eviction — falls back to disk index on cache miss.
    pub(crate) pk_lookup: Arc<DashMap<String, Arc<crate::database::pk_cache::PkLookupCache>>>,

    /// Per-table live row count (for COUNT(*) fast path).
    /// Incremented on INSERT, decremented on DELETE.
    pub(crate) table_row_count: Arc<DashMap<String, Arc<AtomicU64>>>,

    /// Table registry (catalog)
    pub(crate) table_registry: Arc<TableRegistry>,

    /// 🆕 Index metadata registry
    pub(crate) index_registry: Arc<crate::database::index_metadata::IndexRegistry>,

    /// 🚀 P1: Row cache (hot data cache)
    pub(crate) row_cache: Arc<RowCache>,

    /// 🚀 Phase 3+: Index update strategy
    pub(crate) index_update_strategy: crate::config::IndexUpdateStrategy,

    /// 🚀 P0: Query timeout (seconds)
    pub(crate) query_timeout_secs: Option<u64>,

    /// Maximum rows a single SELECT may return (prevents OOM).
    pub(crate) max_result_rows: Option<usize>,

    /// PK lookup cache capacity per table (LRU eviction)
    pub(crate) pk_lookup_capacity: usize,

    /// Column index in-memory write buffer size (bytes)
    pub(crate) column_index_buffer_size: usize,

    /// 🆕 防止递归 flush 的标志
    pub(crate) is_flushing: Arc<AtomicBool>,

    /// 🔒 Checkpoint mutex: prevents concurrent checkpoints (auto + manual)
    /// which can cause deadlock via timestamp_index write lock contention
    pub(crate) checkpoint_mutex: Arc<Mutex<()>>,

    /// 🛡️ Database closed flag — all operations check this and return error if true
    pub(crate) is_closed: Arc<AtomicBool>,

    /// Auto-checkpoint thread (if enabled)
    auto_checkpoint_thread: Option<AutoCheckpointThread>,

    /// Shared flag: true when the async index-build pipeline is running.
    /// Shared across all clones so `is_async_index_pipeline_active()` works
    /// correctly even for cloned MoteDB instances (auto-checkpoint thread).
    is_pipeline_active: Arc<AtomicBool>,

    /// Async index build pipeline: sender (None if pipeline disabled)
    index_build_tx: Option<std::sync::mpsc::Sender<IndexBuildBatch>>,

    /// Number of index build batches sent but not yet processed by the background thread.
    /// Used by `wait_for_indexes_ready()` to know when indexes are caught up.
    pending_index_batches: Arc<std::sync::atomic::AtomicUsize>,

    /// Counter for index build errors (incremented by background thread, readable by user)
    pub index_build_errors: Arc<std::sync::atomic::AtomicUsize>,

    /// Counter for flush errors (incremented by background auto-flush thread)
    pub flush_errors: Arc<std::sync::atomic::AtomicUsize>,

    /// Background index builder thread
    index_builder_thread: Option<IndexBuilderThread>,

    /// Auto-flush background thread: single dedicated thread replaces per-batch spawns
    auto_flush_thread: Option<AutoFlushThread>,

    /// File lock to prevent concurrent database opens on the same directory.
    /// Holds an exclusive flock on `.lock` file. Released on Drop OR explicitly
    /// via release_lock() so that close() allows a subsequent open() on the
    /// same directory (the test/multi-handle scenario). Wrapped in a Mutex
    /// because close() runs on a &self MoteDB.
    _lock_file: std::sync::Mutex<Option<std::fs::File>>,

    /// True for clone_for_callback() instances — skip Drop checkpoint.
    _is_clone: bool,
}

/// Auto-checkpoint background thread
struct AutoCheckpointThread {
    /// Thread handle
    handle: Option<std::thread::JoinHandle<()>>,

    /// Stop signal
    should_stop: Arc<AtomicBool>,
}

/// Index build job sent through the async pipeline
struct IndexBuildBatch {
    /// Raw row bytes grouped by table_name — decoded lazily in the builder thread
    tables_data: std::collections::HashMap<String, Vec<(RowId, Vec<u8>)>>,
}

/// Background index builder thread
struct IndexBuilderThread {
    /// Thread handle
    handle: Option<std::thread::JoinHandle<()>>,

    /// Stop signal
    should_stop: Arc<AtomicBool>,
}

/// Auto-flush background thread: single thread handles all auto-flush requests
struct AutoFlushThread {
    /// Channel to signal flush requests
    flush_tx: std::sync::mpsc::Sender<()>,

    /// Thread handle
    handle: Option<std::thread::JoinHandle<()>>,

    /// Stop signal
    should_stop: Arc<AtomicBool>,
}

impl MoteDB {
    /// Create a new database
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_config(path, DBConfig::default())
    }

    /// Create a new database with custom configuration
    pub fn create_with_config<P: AsRef<Path>>(path: P, config: DBConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let db_path = path.with_extension("mote");

        // 🎯 统一目录结构：所有文件放在 {name}.mote/ 目录下
        if db_path.exists() && db_path.join("lsm").exists() {
            return Err(StorageError::InvalidData(format!(
                "Database already exists at {:?}; use open() instead of create()",
                db_path
            )));
        }
        std::fs::create_dir_all(&db_path)?;

        // 🔒 Acquire exclusive file lock to prevent concurrent opens
        let lock_file = Self::acquire_lock(&db_path)?;

        let wal_path = db_path.join("wal");
        let lsm_dir = db_path.join("lsm");
        let indexes_dir = db_path.join("indexes");

        let num_partitions = config.num_partitions;

        // Create WAL directory with config
        std::fs::create_dir_all(&wal_path)?;
        let wal_config = crate::txn::wal::WALConfig::from(config.wal_config);
        let wal = Arc::new(WALManager::create_with_config(
            &wal_path,
            num_partitions,
            wal_config,
        )?);

        // Create timestamp index with BTree storage (放在 indexes/ 目录)
        std::fs::create_dir_all(&indexes_dir)?;
        let timestamp_storage = indexes_dir.join("timestamp.idx");
        let btree_config = BTreeConfig {
            unique_keys: false, // Allow duplicate timestamps
            allow_updates: true,
            ..Default::default()
        };
        let timestamp_index = Arc::new(RwLock::new(BTree::with_config(
            timestamp_storage,
            btree_config,
        )?));

        // Create LSM-Tree storage engine
        std::fs::create_dir_all(&lsm_dir)?;
        // Use edge-optimized LSM config if memtable_size_limit differs from default
        let lsm_config = crate::storage::lsm::LSMConfig::from_db_config(&config.lsm_config);
        let lsm_engine = Arc::new(LSMEngine::new(lsm_dir, lsm_config)?);

        // Create version store and transaction coordinator
        let version_store = Arc::new(VersionStore::new());
        let txn_coordinator = Arc::new(TransactionCoordinator::new(version_store.clone()));

        // Create table registry (catalog)
        let table_registry = Arc::new(TableRegistry::new(&db_path)?);

        // 🆕 Create index metadata registry
        let index_registry = Arc::new(crate::database::index_metadata::IndexRegistry::new(
            &db_path,
        ));

        // 🚀 P1: Create row cache (default 10000 rows ≈ 10MB)
        let row_cache = Arc::new(RowCache::new(config.row_cache_size.unwrap_or(10000)));

        // Ensure "_default" table has a stable table_id (= 0)
        table_registry.ensure_default_table_id()?;

        // Shared row ID counter
        let next_row_id = Arc::new(AtomicU64::new(0));

        // Unified write LSN — start at current time micros to be higher than any row_id
        let init_lsn = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(1);
        let write_lsn = Arc::new(AtomicU64::new(init_lsn));

        // Persist initial LSN so restart after clock change doesn't regress
        Self::persist_lsn_counter(&db_path, init_lsn);

        // Create columnar store for TimeSeries tables (shares next_row_id and table_registry)
        let columnar_dir = db_path.join("columnar");
        let columnar_store = Arc::new(crate::storage::ColumnarStore::create(
            &columnar_dir,
            config.columnar_config.clone(),
            next_row_id.clone(),
            table_registry.clone(),
        )?);
        // Set WAL on columnar store for crash recovery
        columnar_store.set_wal(wal.clone());

        // 🚀 Recover columnar SSTables from disk (zero-encode INSERTs skip WAL)
        let columnar_sstables = Arc::new(DashMap::new());
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|e| e == "sst") {
                        if let Some(name) = path.file_stem().and_then(|n| n.to_str()) {
                            if name.ends_with("_col") {
                                let table_name = name.trim_end_matches("_col").to_string();
                                if let Ok(col_sst) =
                                    crate::storage::lsm::columnar::ColumnarSSTable::open(&path)
                                {
                                    let rows = col_sst.num_rows;
                                    columnar_sstables.insert(table_name.clone(), Arc::new(col_sst));
                                    debug_log!(
                                        "[Recovery] Loaded columnar SSTable for '{}' ({} rows)",
                                        table_name,
                                        rows
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // 🚀 Ensure columnar SSTable exists for all tables.
        //    After WAL recovery, LSM has data but columnar may not. Build it.
        //    This guarantees all reads can use the columnar fast path.
        for table_name in table_registry.list_tables()? {
            if !columnar_sstables.contains_key(&table_name) {
                if let Ok(schema) = table_registry.get_table(&table_name) {
                    let col_types = schema.col_types();
                    match lsm_engine.compact_to_columnar(col_types) {
                        Ok((col_sst, _paths)) => {
                            columnar_sstables.insert(table_name.clone(), Arc::new(col_sst));
                            debug_log!("[Recovery] Built columnar SSTable for '{}'", table_name);
                        }
                        Err(_) => { /* LSM may be empty — skip */ }
                    }
                }
            }
        }

        let mut db = Self {
            path: db_path,
            wal,
            lsm_engine: lsm_engine.clone(),
            timestamp_index,
            next_row_id: next_row_id.clone(),
            write_lsn: write_lsn.clone(),
            table_auto_increment: Arc::new(DashMap::new()),
            num_partitions,
            txn_coordinator,
            version_store,
            pending_updates: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            vector_indexes: Arc::new(DashMap::new()),
            ioctree_indexes: Arc::new(DashMap::new()),
            text_indexes: Arc::new(DashMap::new()),
            column_indexes: Arc::new(DashMap::new()),
            columnar_store,
            columnar_sstables,
            columnar_write_bufs: Arc::new(DashMap::new()),
            col_segment_stores: Arc::new(DashMap::new()),
            pk_lookup: Arc::new(DashMap::new()),
            table_row_count: Arc::new(DashMap::new()),
            table_registry,
            index_registry,
            row_cache,
            index_update_strategy: config.index_update_strategy.clone(),
            query_timeout_secs: config.query_timeout_secs,
            pk_lookup_capacity: config.pk_lookup_capacity,
            column_index_buffer_size: config.column_index_buffer_size,
            max_result_rows: config.max_result_rows,
            is_flushing: Arc::new(AtomicBool::new(false)),
            is_pipeline_active: Arc::new(AtomicBool::new(false)),
            pending_index_batches: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            index_build_errors: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            flush_errors: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            checkpoint_mutex: Arc::new(Mutex::new(())),
            is_closed: Arc::new(AtomicBool::new(false)),
            auto_checkpoint_thread: None,
            index_build_tx: None,
            index_builder_thread: None,
            auto_flush_thread: None,
            _lock_file: std::sync::Mutex::new(Some(lock_file)),
            _is_clone: false,
        };

        // 🚀 P1: Async Index Build Pipeline
        // Extract rows from memtable in the flush callback, send through a bounded channel.
        // A dedicated index builder thread receives and builds indexes asynchronously.
        // This eliminates deadlock: the flush thread never blocks on index locks.
        let (index_build_tx, index_builder_thread) =
            Self::start_index_builder_pipeline(db.clone_for_callback());
        db.index_build_tx = Some(index_build_tx);
        db.index_builder_thread = Some(index_builder_thread);
        db.is_pipeline_active
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Set flush callback
        {
            let tx = db.index_build_tx.clone().ok_or_else(|| {
                StorageError::Index("Index builder pipeline not initialized".into())
            })?;
            let registry = db.table_registry.clone();
            let pending = db.pending_index_batches.clone();
            let lsm = db.lsm_engine.clone();
            db.lsm_engine.set_flush_callback(move |memtable| {
                let result = Self::extract_and_send_index_batch(memtable, &tx, &registry, &lsm);
                if result.is_ok() && !memtable.is_empty() {
                    pending.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                result
            })?;
        }

        // 🚀 Start auto-checkpoint thread if enabled
        let auto_checkpoint_thread = config.auto_checkpoint.map(|auto_config| {
            Self::start_auto_checkpoint_thread(db.clone_for_callback(), auto_config)
        });

        // Update db with the thread handle
        let mut db = db;
        db.auto_checkpoint_thread = auto_checkpoint_thread;

        // Start auto-flush background thread (single thread for all auto-flush requests)
        let auto_flush = Self::start_auto_flush_thread(db.clone_for_callback());
        db.auto_flush_thread = Some(auto_flush);

        Ok(db)
    }

    /// Check whether the async index-build pipeline is active.
    /// When active, `flush_impl` skips vector/text index flushing to avoid
    /// write-lock contention with the builder thread.
    pub(crate) fn is_async_index_pipeline_active(&self) -> bool {
        self.is_pipeline_active
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Wait until all pending index build batches have been processed.
    ///
    /// Call this after `flush()` to ensure column/vector/text indexes are
    /// fully built before running queries that depend on them.
    ///
    /// Returns `true` if all batches completed, `false` on timeout or if the
    /// builder thread has exited.
    pub fn wait_for_indexes_ready(&self) -> bool {
        self.wait_for_indexes_ready_timeout(std::time::Duration::from_secs(10))
    }

    /// Wait for index readiness with a custom timeout.
    pub fn wait_for_indexes_ready_timeout(&self, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            if self
                .pending_index_batches
                .load(std::sync::atomic::Ordering::Relaxed)
                == 0
            {
                return true;
            }
            // Check if index builder thread has crashed (thread handle finished but work remains)
            if let Some(ref thread) = self.index_builder_thread {
                if let Some(ref handle) = thread.handle {
                    if handle.is_finished() {
                        let pending = self
                            .pending_index_batches
                            .load(std::sync::atomic::Ordering::Relaxed);
                        warn_log!("[wait_for_indexes_ready] Index builder thread exited with {} batches pending", pending);
                        return false;
                    }
                }
            }
            if start.elapsed() > timeout {
                warn_log!(
                    "[wait_for_indexes_ready] Timed out after {:?}, pending={}",
                    timeout,
                    self.pending_index_batches
                        .load(std::sync::atomic::Ordering::Relaxed)
                );
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Signal all background threads (index builder, auto-flush, auto-checkpoint)
    /// to stop. Used by `close()` so that `checkpoint_full()` can acquire all
    /// index write locks without contention.
    pub(crate) fn signal_background_threads_stop(&self) {
        if let Some(ref thread) = self.index_builder_thread {
            thread
                .should_stop
                .store(true, std::sync::atomic::Ordering::Release);
        }
        if let Some(ref thread) = self.auto_flush_thread {
            thread
                .should_stop
                .store(true, std::sync::atomic::Ordering::Release);
            // Wake the auto-flush thread from its recv_timeout
            let _ = thread.flush_tx.send(());
        }
        if let Some(ref thread) = self.auto_checkpoint_thread {
            thread
                .should_stop
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Wait for background threads to actually finish after signaling stop.
    /// Returns true if all threads stopped within the timeout.
    pub(crate) fn wait_for_background_threads_stop(&self, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            let mut all_done = true;

            if let Some(ref thread) = self.index_builder_thread {
                if let Some(ref handle) = thread.handle {
                    if !handle.is_finished() {
                        all_done = false;
                    }
                }
            }

            if let Some(ref thread) = self.auto_flush_thread {
                if let Some(ref handle) = thread.handle {
                    if !handle.is_finished() {
                        all_done = false;
                    }
                }
            }

            if let Some(ref thread) = self.auto_checkpoint_thread {
                if let Some(ref handle) = thread.handle {
                    if !handle.is_finished() {
                        all_done = false;
                    }
                }
            }

            if all_done {
                return true;
            }

            if start.elapsed() > timeout {
                warn_log!(
                    "[wait_for_background_threads_stop] Timed out after {:?}",
                    timeout
                );
                return false;
            }

            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Clone self for callback (only what's needed)
    pub(crate) fn clone_for_callback(&self) -> Self {
        Self {
            path: self.path.clone(),
            wal: self.wal.clone(),
            lsm_engine: self.lsm_engine.clone(),
            timestamp_index: self.timestamp_index.clone(),
            next_row_id: self.next_row_id.clone(),
            write_lsn: self.write_lsn.clone(),
            table_auto_increment: self.table_auto_increment.clone(), // 🚀 Phase 4
            num_partitions: self.num_partitions,
            txn_coordinator: self.txn_coordinator.clone(),
            version_store: self.version_store.clone(),
            pending_updates: self.pending_updates.clone(),
            index_build_errors: self.index_build_errors.clone(),
            flush_errors: self.flush_errors.clone(),
            vector_indexes: self.vector_indexes.clone(),
            ioctree_indexes: self.ioctree_indexes.clone(),
            text_indexes: self.text_indexes.clone(),
            column_indexes: self.column_indexes.clone(),
            columnar_store: self.columnar_store.clone(),
            columnar_sstables: self.columnar_sstables.clone(),
            columnar_write_bufs: self.columnar_write_bufs.clone(),
            col_segment_stores: self.col_segment_stores.clone(),
            pk_lookup: self.pk_lookup.clone(),
            table_row_count: self.table_row_count.clone(),
            table_registry: self.table_registry.clone(),
            index_registry: self.index_registry.clone(), // 🆕
            row_cache: self.row_cache.clone(),
            index_update_strategy: self.index_update_strategy.clone(),
            query_timeout_secs: self.query_timeout_secs, // 🚀 P0
            pk_lookup_capacity: self.pk_lookup_capacity,
            column_index_buffer_size: self.column_index_buffer_size,
            max_result_rows: self.max_result_rows,
            is_flushing: self.is_flushing.clone(),
            is_pipeline_active: self.is_pipeline_active.clone(), // shared — clones see true when pipeline runs
            pending_index_batches: self.pending_index_batches.clone(),
            checkpoint_mutex: self.checkpoint_mutex.clone(),
            is_closed: self.is_closed.clone(),
            auto_checkpoint_thread: None, // Don't clone thread (only owned by original)
            index_build_tx: None,         // Don't clone sender (only owned by original)
            index_builder_thread: None,   // Don't clone thread (only owned by original)
            auto_flush_thread: None,      // Don't clone thread (only owned by original)
            _lock_file: std::sync::Mutex::new(None), // Don't clone lock (only owned by original)
            _is_clone: true,              // Skip Drop checkpoint for clones
        }
    }

    /// Open an existing database
    /// Open an existing database with default configuration
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, DBConfig::default())
    }

    /// Open an existing database with custom configuration
    ///
    /// Use this to apply edge-optimized settings when reopening:
    /// ```ignore
    /// let config = DBConfig::for_edge();
    /// let db = MoteDB::open_with_config("data.mote", config)?;
    /// ```
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: DBConfig) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let db_path = path.with_extension("mote");

        // 🔒 Acquire exclusive file lock to prevent concurrent opens
        let lock_file = Self::acquire_lock(&db_path)?;

        // 🎯 统一目录结构：从 {name}.mote/ 目录读取
        let wal_path = db_path.join("wal");
        let lsm_dir = db_path.join("lsm");
        let indexes_dir = db_path.join("indexes");

        // Use config instead of hardcoded default
        let num_partitions = config.num_partitions;

        // Open or create WAL (pass user config — fixes config loss on reopen)
        let wal_config = crate::txn::wal::WALConfig::from(config.wal_config.clone());
        let wal = if wal_path.exists() {
            Arc::new(WALManager::open_with_config(
                &wal_path,
                num_partitions,
                wal_config,
            )?)
        } else {
            std::fs::create_dir_all(&wal_path)?;
            Arc::new(WALManager::create_with_config(
                &wal_path,
                num_partitions,
                wal_config,
            )?)
        };

        // Replay WAL records into LSM Engine.
        // Safety: In MoteDB's embedded single-process model, WAL records from committed
        // transactions are written atomically via batch_append(). Uncommitted records
        // (crash mid-batch) are detected by checksum verification and skipped.
        // TimeSeries data is replayed separately into the columnar store below.
        let recovered_records = wal.recover()?;

        // Open timestamp index with BTree storage (从 indexes/ 目录)
        std::fs::create_dir_all(&indexes_dir)?;
        let timestamp_storage = indexes_dir.join("timestamp.idx");
        let btree_config = BTreeConfig {
            unique_keys: false,
            allow_updates: true,
            ..Default::default()
        };
        let mut timestamp_idx = BTree::with_config(timestamp_storage, btree_config)?;

        // Get total entries from timestamp index (already persisted data)
        let persisted_count = timestamp_idx.len();

        let mut max_row_id = if persisted_count > 0 {
            // Estimate max_row_id from persisted count
            // Since row_ids are sequential starting from 0, max is count-1
            (persisted_count - 1) as u64
        } else {
            0
        };

        // Open LSM-Tree storage engine
        std::fs::create_dir_all(&lsm_dir)?;
        // Use edge-optimized LSM config if memtable_size_limit differs from default
        let lsm_config = crate::storage::lsm::LSMConfig::from_db_config(&config.lsm_config);
        let lsm_engine = Arc::new(LSMEngine::new(lsm_dir, lsm_config)?);

        // Load table registry BEFORE WAL replay so we can resolve table_name → table_id
        // for correct composite key construction.
        let table_registry = Arc::new(TableRegistry::new(&db_path)?);
        table_registry.ensure_default_table_id()?;

        // Replay WAL records into LSM Engine using stable table_id
        debug_log!("[database] 恢复 WAL 记录到 LSM Engine...");
        let mut _recovered_count = 0;

        // Phase 1: Analysis — determine which transactions committed
        let mut committed_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut active_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for records in recovered_records.values() {
            for record in records {
                match record {
                    WALRecord::Begin { txn_id, .. } => {
                        active_txns.insert(*txn_id);
                    }
                    WALRecord::Commit { txn_id, .. } => {
                        active_txns.remove(txn_id);
                        committed_txns.insert(*txn_id);
                    }
                    WALRecord::Rollback { txn_id } => {
                        active_txns.remove(txn_id);
                    }
                    _ => {}
                }
            }
        }

        // Update timestamp index — only for committed/auto-commit records
        for records in recovered_records.values() {
            for record in records {
                match record {
                    WALRecord::Insert {
                        row_id,
                        data,
                        txn_id,
                        ..
                    } => {
                        max_row_id = max_row_id.max(*row_id);
                        if *txn_id == 0 || committed_txns.contains(txn_id) {
                            if let Some(crate::types::Value::Timestamp(ts)) = data.first() {
                                if let Err(e) = timestamp_idx.insert(ts.as_micros_u64(), *row_id) {
                                    warn_log!(
                                        "[Recovery] Timestamp index insert failed for row {}: {}",
                                        row_id,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    WALRecord::InsertRaw {
                        row_id,
                        raw_data,
                        txn_id,
                        ..
                    } => {
                        max_row_id = max_row_id.max(*row_id);
                        if *txn_id == 0 || committed_txns.contains(txn_id) {
                            // Extract timestamp from raw data for index
                            if let Ok(row) = crate::storage::row_format::decode_any(raw_data) {
                                if let Some(crate::types::Value::Timestamp(ts)) = row.first() {
                                    if let Err(e) =
                                        timestamp_idx.insert(ts.as_micros_u64(), *row_id)
                                    {
                                        warn_log!("[Recovery] Timestamp index insert failed for row {}: {}", row_id, e);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let timestamp_index = Arc::new(RwLock::new(timestamp_idx));

        // Initialize write_lsn early for WAL recovery replay.
        // Must exceed any pre-existing timestamp AND the persisted LSN from
        // the last checkpoint (to survive clock regression across restarts).
        let current_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(1);
        let persisted_lsn = Self::load_lsn_counter(&db_path);
        let init_lsn = current_micros
            .max(max_row_id + 1)
            .max(persisted_lsn)
            .saturating_add(1_000_000);
        let recovery_lsn = Arc::new(AtomicU64::new(init_lsn));

        // Phase 2: Redo — replay into LSM + columnar buffers simultaneously.
        // Columnar data is finalized below; LSM provides backward compat.
        // Prepare columnar builders for all tables (pre-allocate before replay)
        let col_builders: Arc<
            DashMap<
                String,
                Arc<parking_lot::Mutex<crate::storage::lsm::columnar::ColumnarSSTableBuilder>>,
            >,
        > = Arc::new(DashMap::new());
        for table_name in table_registry.list_tables()? {
            if let Ok(schema) = table_registry.get_table(&table_name) {
                let indexes_dir = db_path.join("indexes");
                std::fs::create_dir_all(&indexes_dir).ok();
                let col_path = indexes_dir.join(format!("{}_col.sst", &table_name));
                let builder = crate::storage::lsm::columnar::ColumnarSSTableBuilder::new(
                    col_path,
                    schema.col_types().to_vec(),
                );
                col_builders.insert(
                    table_name.clone(),
                    Arc::new(parking_lot::Mutex::new(builder)),
                );
            }
        }

        for records in recovered_records.values() {
            for record in records {
                match record {
                    WALRecord::InsertRaw {
                        table_name,
                        row_id,
                        raw_data,
                        txn_id,
                        ..
                    } => {
                        if *txn_id != 0 && !committed_txns.contains(txn_id) {
                            continue;
                        }
                        let table_id = table_registry.get_table_id(table_name).unwrap_or(0);
                        let composite_key = ((table_id as u64) << 32) | (*row_id & 0xFFFFFFFF);
                        let ts = recovery_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let value = crate::storage::lsm::Value::new(raw_data.clone(), ts);
                        lsm_engine.put(composite_key, value)?;
                        // Also write to columnar buffer
                        if let Some(builder_arc) = col_builders.get(table_name) {
                            if let Ok(row) = crate::storage::row_format::decode_any(raw_data) {
                                let key = (table_id as u64) << 32 | (*row_id & 0xFFFFFFFF);
                                let _ = builder_arc.value().lock().add_values(key, ts, false, &row);
                            }
                        }
                        _recovered_count += 1;
                    }
                    WALRecord::Insert {
                        table_name,
                        row_id,
                        data,
                        txn_id,
                        ..
                    } => {
                        if *txn_id != 0 && !committed_txns.contains(txn_id) {
                            continue;
                        }
                        let table_id = table_registry.get_table_id(table_name).unwrap_or(0);
                        let composite_key = ((table_id as u64) << 32) | (*row_id & 0xFFFFFFFF);
                        let row_data = bincode::serialize(data)?;
                        let ts = recovery_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let value = crate::storage::lsm::Value::new(row_data, ts);
                        lsm_engine.put(composite_key, value)?;
                        if let Some(builder_arc) = col_builders.get(table_name) {
                            let key = (table_id as u64) << 32 | (*row_id & 0xFFFFFFFF);
                            let _ = builder_arc.value().lock().add_values(key, ts, false, data);
                        }
                        _recovered_count += 1;
                    }
                    WALRecord::UpdateRaw {
                        table_name,
                        row_id,
                        raw_new,
                        txn_id,
                        ..
                    } => {
                        if *txn_id != 0 && !committed_txns.contains(txn_id) {
                            continue;
                        }
                        let table_id = table_registry.get_table_id(table_name).unwrap_or(0);
                        let composite_key = ((table_id as u64) << 32) | (*row_id & 0xFFFFFFFF);
                        let ts = recovery_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let value = crate::storage::lsm::Value::new(raw_new.clone(), ts);
                        lsm_engine.put(composite_key, value)?;
                        if let Some(builder_arc) = col_builders.get(table_name) {
                            if let Ok(row) = crate::storage::row_format::decode_any(raw_new) {
                                let key = (table_id as u64) << 32 | (*row_id & 0xFFFFFFFF);
                                let _ = builder_arc.value().lock().add_values(key, ts, false, &row);
                            }
                        }
                        _recovered_count += 1;
                    }
                    WALRecord::Update {
                        table_name,
                        row_id,
                        new_data,
                        txn_id,
                        ..
                    } => {
                        if *txn_id != 0 && !committed_txns.contains(txn_id) {
                            continue;
                        }
                        let table_id = table_registry.get_table_id(table_name).unwrap_or(0);
                        let composite_key = ((table_id as u64) << 32) | (*row_id & 0xFFFFFFFF);
                        let row_data = bincode::serialize(new_data)?;
                        let ts = recovery_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let value = crate::storage::lsm::Value::new(row_data, ts);
                        lsm_engine.put(composite_key, value)?;
                        if let Some(builder_arc) = col_builders.get(table_name) {
                            let key = (table_id as u64) << 32 | (*row_id & 0xFFFFFFFF);
                            let _ = builder_arc
                                .value()
                                .lock()
                                .add_values(key, ts, false, new_data);
                        }
                        _recovered_count += 1;
                    }
                    WALRecord::DeleteRaw {
                        table_name,
                        row_id,
                        txn_id,
                        ..
                    } => {
                        if *txn_id != 0 && !committed_txns.contains(txn_id) {
                            continue;
                        }
                        let table_id = table_registry.get_table_id(table_name).unwrap_or(0);
                        let composite_key = ((table_id as u64) << 32) | (*row_id & 0xFFFFFFFF);
                        let ts = recovery_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        lsm_engine.delete(composite_key, ts)?;
                        _recovered_count += 1;
                    }
                    WALRecord::Delete {
                        table_name,
                        row_id,
                        txn_id,
                        ..
                    } => {
                        if *txn_id != 0 && !committed_txns.contains(txn_id) {
                            continue;
                        }
                        let table_id = table_registry.get_table_id(table_name).unwrap_or(0);
                        let composite_key = ((table_id as u64) << 32) | (*row_id & 0xFFFFFFFF);

                        let ts = recovery_lsn.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        lsm_engine.delete(composite_key, ts)?;
                        _recovered_count += 1;
                    }
                    _ => {}
                }
            }
        }
        debug_log!(
            "[database] WAL 恢复完成，恢复了 {} 条记录",
            _recovered_count
        );

        // Create version store and transaction coordinator
        let version_store = Arc::new(VersionStore::new());
        let txn_coordinator = Arc::new(TransactionCoordinator::new(version_store.clone()));

        // 🆕 Load index metadata registry first (needed for metric info)
        let index_registry = Arc::new(crate::database::index_metadata::IndexRegistry::new(
            &db_path,
        ));
        if let Err(e) = index_registry.load() {
            debug_log!(
                "[database] ⚠️ Failed to load index_metadata: {:?}. Indexes will need rebuild.",
                e
            );
            // Not fatal — indexes can be rebuilt, but user should be warned
        }

        // Load existing vector indexes (using metric from registry)
        let vector_indexes = Self::load_vector_indexes(&db_path, &index_registry)?;

        // Load existing text indexes
        let text_indexes = Self::load_text_indexes(&db_path)?;

        // Load existing i-Octree indexes
        let ioctree_indexes = Self::load_ioctree_indexes(&db_path)?;

        // Load existing column indexes
        let column_indexes = Self::load_column_indexes(&db_path, &index_registry)?;

        // 🚀 P1: Create row cache (use config or default 10000)
        let row_cache = Arc::new(RowCache::new(config.row_cache_size.unwrap_or(10000)));

        // Shared row ID counter (initialized from WAL replay)
        let next_row_id = Arc::new(AtomicU64::new(max_row_id + 1));

        // Reuse the recovery_lsn as the database's write_lsn (it's already initialized high enough)
        let write_lsn = recovery_lsn;

        // Create columnar store for TimeSeries tables
        let columnar_dir = db_path.join("columnar");

        // Clean up leftover .mcdb.tmp files from interrupted columnar segment writes.
        // These are safe to delete because they were never registered with a SegmentManager.
        if columnar_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&columnar_dir) {
                for entry in entries.flatten() {
                    let sub_dir = entry.path();
                    if sub_dir.is_dir() {
                        if let Ok(sub_entries) = std::fs::read_dir(&sub_dir) {
                            for sub_entry in sub_entries.flatten() {
                                let path = sub_entry.path();
                                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                    if name.ends_with(".mcdb.tmp") {
                                        debug_log!(
                                            "[database] Cleaning up temp columnar segment: {:?}",
                                            path
                                        );
                                        let _ = std::fs::remove_file(&path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let columnar_store = Arc::new(crate::storage::ColumnarStore::create(
            &columnar_dir,
            config.columnar_config.clone(),
            next_row_id.clone(),
            table_registry.clone(),
        )?);

        // Register existing TimeSeries tables with columnar store
        for table_name in table_registry.list_tables()? {
            if let Ok(schema) = table_registry.get_table(&table_name) {
                if schema.table_type == crate::types::TableType::TimeSeries {
                    if let Ok(table_id) = table_registry.get_table_id(&table_name) {
                        if let Err(e) = columnar_store.register_table(table_id, &schema) {
                            debug_log!(
                                "[database] ⚠️ Failed to register columnar table '{}': {:?}",
                                table_name,
                                e
                            );
                        }
                    }
                }
            }
        }

        // Set WAL on columnar store for crash recovery
        columnar_store.set_wal(wal.clone());

        // Replay WAL records for TimeSeries tables into columnar store
        // (These records were already replayed into LSM above, but TimeSeries data
        //  belongs in the columnar store for proper querying)
        {
            let mut columnar_replay_count = 0u64;
            for records in recovered_records.values() {
                for record in records {
                    let (table_name, row_id, txn_id, row_data) = match record {
                        WALRecord::Insert {
                            table_name,
                            row_id,
                            data,
                            txn_id,
                            ..
                        } => (table_name.clone(), *row_id, *txn_id, data.clone()),
                        WALRecord::InsertRaw {
                            table_name,
                            row_id,
                            raw_data,
                            txn_id,
                            ..
                        } => {
                            let row = match crate::storage::row_format::decode_any(raw_data) {
                                Ok(r) => r,
                                Err(_) => continue,
                            };
                            (table_name.clone(), *row_id, *txn_id, row)
                        }
                        _ => continue,
                    };
                    if txn_id != 0 && !committed_txns.contains(&txn_id) {
                        continue;
                    }
                    if let Ok(schema) = table_registry.get_table(&table_name) {
                        if schema.table_type == crate::types::TableType::TimeSeries {
                            if let Err(e) = columnar_store.replay_row(&table_name, row_id, row_data)
                            {
                                debug_log!(
                                    "[database] ⚠️ Failed to replay columnar row for '{}': {:?}",
                                    table_name,
                                    e
                                );
                            }
                            columnar_replay_count += 1;
                        }
                    }
                }
            }
            if columnar_replay_count > 0 {
                debug_log!(
                    "[database] Replayed {} columnar rows from WAL",
                    columnar_replay_count
                );
            }
        }

        let mut db = Self {
            path: db_path,
            wal,
            lsm_engine: lsm_engine.clone(),
            timestamp_index,
            next_row_id,
            write_lsn,
            table_auto_increment: Arc::new(DashMap::new()),
            num_partitions,
            txn_coordinator,
            version_store,
            pending_updates: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            vector_indexes: Arc::new(Self::hashmap_to_dashmap(vector_indexes)),
            ioctree_indexes: Arc::new(Self::hashmap_to_dashmap(ioctree_indexes)),
            text_indexes: Arc::new(Self::hashmap_to_dashmap(text_indexes)),
            column_indexes: Arc::new(Self::hashmap_to_dashmap(column_indexes)),
            columnar_store,
            columnar_sstables: Arc::new(DashMap::new()),
            columnar_write_bufs: Arc::new(DashMap::new()),
            col_segment_stores: Arc::new(DashMap::new()),
            pk_lookup: Arc::new(DashMap::new()),
            table_row_count: Arc::new(DashMap::new()),
            table_registry,
            index_registry,
            row_cache,
            index_update_strategy: config.index_update_strategy.clone(),
            query_timeout_secs: config.query_timeout_secs,
            pk_lookup_capacity: config.pk_lookup_capacity,
            column_index_buffer_size: config.column_index_buffer_size,
            max_result_rows: config.max_result_rows,
            is_flushing: Arc::new(AtomicBool::new(false)),
            is_pipeline_active: Arc::new(AtomicBool::new(false)),
            pending_index_batches: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            index_build_errors: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            flush_errors: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            checkpoint_mutex: Arc::new(Mutex::new(())),
            is_closed: Arc::new(AtomicBool::new(false)),
            auto_checkpoint_thread: None,
            index_build_tx: None,
            index_builder_thread: None,
            auto_flush_thread: None,
            _lock_file: std::sync::Mutex::new(Some(lock_file)),
            _is_clone: false,
        };

        // Recover ColSegmentStore: scan columnar_ms/ for table dirs, replay
        // MANIFEST, load segments. Ensures data survives restart (ACID).
        let ms_dir = db.path.join("columnar_ms");
        if ms_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&ms_dir) {
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata() {
                        if meta.is_dir() {
                            let table_name = entry.file_name().to_string_lossy().to_string();
                            if let Ok(schema) = db.table_registry.get_table(&table_name) {
                                let col_types = schema.col_types().to_vec();
                                if let Ok(store) =
                                    crate::storage::col_segment::ColSegmentStore::create(
                                        &db.path,
                                        &table_name,
                                        col_types,
                                    )
                                {
                                    store.recover_from_disk();
                                    // Pre-compact to single segment so queries
                                    // use fast SelectColumnar path (zero-copy).
                                    while store.segment_count() >= 2 {
                                        let _ = store.force_compact_all();
                                    }
                                    db.col_segment_stores.insert(table_name, store);
                                }
                            }
                        }
                    }
                }
            }
            // 🔑 Recover next_row_id from ColSegmentStore segments so that
            // post-reopen INSERTs don't reuse a row_id from a previous session.
            // Without this, the non-AUTO_INCREMENT row_id counter resets to 0/1
            // after reopen, causing key collisions and data loss (the 3-cycle
            // count=2 bug — cycle 3 reused row_id 1 instead of allocating 2).
            let max_seg_row_id = db
                .col_segment_stores
                .iter()
                .map(|e| e.value().max_row_id())
                .max()
                .unwrap_or(0);
            let current = db.next_row_id.load(std::sync::atomic::Ordering::Relaxed);
            if max_seg_row_id + 1 > current {
                db.next_row_id
                    .store(max_seg_row_id + 1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // 🚀 P1: Async Index Build Pipeline (same as create_with_config)
        let (index_build_tx, index_builder_thread) =
            Self::start_index_builder_pipeline(db.clone_for_callback());
        db.index_build_tx = Some(index_build_tx);
        db.index_builder_thread = Some(index_builder_thread);
        db.is_pipeline_active
            .store(true, std::sync::atomic::Ordering::Relaxed);

        {
            let tx = db.index_build_tx.clone().unwrap();
            let registry = db.table_registry.clone();
            let pending = db.pending_index_batches.clone();
            let lsm = db.lsm_engine.clone();
            db.lsm_engine.set_flush_callback(move |memtable| {
                let result = Self::extract_and_send_index_batch(memtable, &tx, &registry, &lsm);
                if result.is_ok() && !memtable.is_empty() {
                    pending.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                result
            })?;
        }

        // 🚀 Start auto-checkpoint thread (only if config provided, matching create behavior)
        let auto_checkpoint_thread = config
            .auto_checkpoint
            .map(|cfg| Self::start_auto_checkpoint_thread(db.clone_for_callback(), cfg));

        db.auto_checkpoint_thread = auto_checkpoint_thread;

        // Start auto-flush background thread
        let auto_flush = Self::start_auto_flush_thread(db.clone_for_callback());
        db.auto_flush_thread = Some(auto_flush);

        // 🚀 Phase 5: Recover AUTO_INCREMENT counters (B3: Crash Recovery)
        // For each table with AUTO_INCREMENT, find max ID from LSM and initialize counter
        for table_name in db.table_registry.list_tables()? {
            let schema = db.table_registry.get_table(&table_name)?;
            if schema.is_primary_key_auto_increment() {
                let max_id = db.recover_auto_increment_counter(&table_name, &schema)?;
                debug_log!(
                    "[database] 🔄 Recovered AUTO_INCREMENT counter for '{}': next_id = {}",
                    table_name,
                    max_id + 1
                );

                db.table_auto_increment
                    .insert(table_name.clone(), Arc::new(AtomicI64::new(max_id + 1)));

                // Initialize row count counter (will count via streaming scan)
                let row_counter = Arc::new(AtomicU64::new(0));
                let table_prefix = db.compute_table_prefix(&table_name);
                let start_key = table_prefix << 32;
                let end_key = (table_prefix + 1) << 32;
                if let Ok(stream) = db.lsm_engine.scan_range_streaming(start_key, end_key) {
                    let mut cnt = 0u64;
                    for (_, value) in stream.flatten() {
                        if !value.deleted {
                            cnt += 1;
                        }
                    }
                    row_counter.store(cnt, std::sync::atomic::Ordering::Relaxed);
                }
                // 🔑 ColSegmentStore tables: data lives in segment files, not LSM.
                // Recover row count from ColSegmentStore if LSM count is 0.
                if row_counter.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                    if let Some(store) = db.col_segment_stores.get(&table_name) {
                        let cnt = store.count_live_rows() as u64;
                        row_counter.store(cnt, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                db.table_row_count.insert(table_name.clone(), row_counter);
            } else if let Some(pk_col) = schema.primary_key() {
                // Pre-warm PK lookup cache from SSTable data
                db.warm_pk_cache(&table_name, &schema, pk_col);
            }
        }

        Ok(db)
    }

    /// Pre-warm PK lookup cache by scanning SSTable data for a table.
    /// This avoids cold-start misses where every PK SELECT requires a full SSTable scan.
    fn warm_pk_cache(&self, table_name: &str, schema: &crate::types::TableSchema, pk_col: &str) {
        let pk_position = match schema.columns.iter().find(|c| c.name == pk_col) {
            Some(col) => col.position,
            None => return,
        };

        // Create the PK lookup cache for this table
        let pk_cache = Arc::new(crate::database::pk_cache::PkLookupCache::new(
            self.pk_lookup_capacity,
        ));
        self.pk_lookup
            .insert(table_name.to_string(), pk_cache.clone());

        // Initialize row count counter
        self.table_row_count
            .insert(table_name.to_string(), Arc::new(AtomicU64::new(0)));

        // Scan LSM for this table's data
        let table_prefix = self.compute_table_prefix(table_name);
        let start_key = table_prefix << 32;
        let end_key = (table_prefix + 1) << 32;

        let col_types = schema.col_types();
        let mut count = 0;

        if let Ok(stream) = self.lsm_engine.scan_range_streaming(start_key, end_key) {
            for result in stream {
                let (composite_key, value) = match result {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                if value.deleted {
                    continue;
                }
                let row_id = (composite_key & 0xFFFFFFFF) as RowId;

                let data_bytes: Vec<u8> = match &value.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.to_vec(),
                    crate::storage::lsm::ValueData::Blob(blob_ref) => {
                        match self.lsm_engine.resolve_blob(blob_ref) {
                            Ok(data) => data,
                            Err(_) => continue,
                        }
                    }
                };

                if let Ok(pk_value) =
                    crate::storage::row_format::get_column(&data_bytes, col_types, pk_position)
                {
                    pk_cache.insert(
                        crate::database::pk_cache::PkKey::from_value(&pk_value),
                        row_id,
                    );
                    count += 1;
                }
            }
        }

        if count > 0 {
            debug_log!(
                "[warm_pk_cache] ✅ Pre-warmed PK cache for '{}': {} entries",
                table_name,
                count
            );
            // Set row count from recovered data
            if let Some(counter) = self.table_row_count.get(table_name) {
                counter.store(count as u64, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // 🔑 ColSegmentStore tables: recover row count from segments (LSM is empty).
        if count == 0 {
            if let Some(store) = self.col_segment_stores.get(table_name) {
                let cnt = store.count_live_rows() as u64;
                if let Some(counter) = self.table_row_count.get(table_name) {
                    counter.store(cnt, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    /// 🚀 Helper: Convert HashMap to DashMap
    fn hashmap_to_dashmap<K: std::hash::Hash + Eq, V>(map: HashMap<K, V>) -> DashMap<K, V> {
        let dashmap = DashMap::new();
        for (k, v) in map {
            dashmap.insert(k, v);
        }
        dashmap
    }

    /// 🆕 Set AUTO_INCREMENT value for a table
    ///
    /// # Arguments
    /// * `table_name` - Table name
    /// * `new_value` - New AUTO_INCREMENT starting value
    ///
    /// # Errors
    /// Returns error if table doesn't exist or doesn't have AUTO_INCREMENT
    pub fn set_auto_increment_value(&self, table_name: &str, new_value: i64) -> Result<()> {
        // Verify table has AUTO_INCREMENT
        if let Some(counter_ref) = self.table_auto_increment.get(table_name) {
            counter_ref.store(new_value, std::sync::atomic::Ordering::SeqCst);
            debug_log!(
                "[database] ✓ Set AUTO_INCREMENT for '{}' to {}",
                table_name,
                new_value
            );
            Ok(())
        } else {
            Err(MoteDBError::InvalidArgument(format!(
                "Table {} does not have AUTO_INCREMENT",
                table_name
            )))
        }
    }

    /// Load existing vector indexes from disk
    fn load_vector_indexes(
        db_path: &Path,
        index_registry: &crate::database::index_metadata::IndexRegistry,
    ) -> Result<HashMap<String, Arc<RwLock<DiskANNIndex>>>> {
        let mut indexes = HashMap::new();

        // 🎯 从统一目录加载：{db}.mote/indexes/vector_*/
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    if let Ok(name) = entry.file_name().into_string() {
                        if name.starts_with("vector_") {
                            let index_name = match name.strip_prefix("vector_") {
                                Some(n) => n,
                                None => continue,
                            };
                            let index_path = entry.path();

                            // Resolve metric from metadata registry
                            let distance_kind = index_registry
                                .get(index_name)
                                .and_then(|meta| meta.metric.clone())
                                .map(|m| match m.as_str() {
                                    "cosine" => crate::distance::DistanceKind::Cosine,
                                    _ => crate::distance::DistanceKind::Euclidean,
                                })
                                .unwrap_or(crate::distance::DistanceKind::Euclidean);

                            let config = VamanaConfig::default().with_metric(distance_kind);
                            if let Ok(index) = DiskANNIndex::load(&index_path, config) {
                                indexes
                                    .insert(index_name.to_string(), Arc::new(RwLock::new(index)));
                                debug_log!(
                                    "[MoteDB] Loaded vector index: {} (metric={:?})",
                                    index_name,
                                    distance_kind
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(indexes)
    }

    /// Load existing text indexes from disk
    fn load_text_indexes(db_path: &Path) -> Result<HashMap<String, Arc<RwLock<TextFTSIndex>>>> {
        let mut indexes = HashMap::new();

        // 🧹 Clean up legacy text_indexes_metadata.bin (no longer used)
        let legacy_metadata_path = db_path.join("text_indexes_metadata.bin");
        if legacy_metadata_path.exists() {
            if let Err(e) = std::fs::remove_file(&legacy_metadata_path) {
                debug_log!(
                    "⚠️ Failed to remove legacy text_indexes_metadata.bin: {}",
                    e
                );
            } else {
                debug_log!("[MoteDB] 🧹 Removed legacy text_indexes_metadata.bin (replaced by index_metadata.bin)");
            }
        }

        // 🎯 从统一目录加载：{db}.mote/indexes/text_*/
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    if let Ok(name) = entry.file_name().into_string() {
                        if name.starts_with("text_") {
                            let index_name = match name.strip_prefix("text_") {
                                Some(n) => n,
                                None => continue,
                            };
                            let index_path = entry.path();

                            // Try to load the index
                            if let Ok(index) = TextFTSIndex::new(index_path) {
                                indexes
                                    .insert(index_name.to_string(), Arc::new(RwLock::new(index)));
                                debug_log!("[MoteDB] Loaded text index: {}", index_name);
                            }
                        }
                    }
                }
            }
        }

        Ok(indexes)
    }

    /// Load existing i-Octree indexes from disk
    fn load_ioctree_indexes(db_path: &Path) -> Result<HashMap<String, Arc<RwLock<IOctreeIndex>>>> {
        let mut indexes = HashMap::new();

        // Load from {db}.mote/indexes/ioctree_*/
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    if let Ok(name) = entry.file_name().into_string() {
                        if name.starts_with("ioctree_") {
                            let index_name = match name.strip_prefix("ioctree_") {
                                Some(n) => n,
                                None => continue,
                            };
                            let index_file = entry.path().join("ioctree.bin");

                            if index_file.exists() {
                                if let Ok(index) = IOctreeIndex::load_from_path(&index_file) {
                                    indexes.insert(
                                        index_name.to_string(),
                                        Arc::new(RwLock::new(index)),
                                    );
                                    debug_log!("[MoteDB] Loaded ioctree index: {}", index_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(indexes)
    }

    /// Load existing column value indexes from disk.
    /// Scans {db}.mote/indexes/ for column_*.idx files and reopens the BTree.
    fn load_column_indexes(
        db_path: &Path,
        index_registry: &crate::database::index_metadata::IndexRegistry,
    ) -> Result<HashMap<String, Arc<ColumnValueIndex>>> {
        let mut indexes = HashMap::new();
        let indexes_dir = db_path.join("indexes");
        if !indexes_dir.exists() {
            return Ok(indexes);
        }
        let entries = match std::fs::read_dir(&indexes_dir) {
            Ok(e) => e,
            Err(_) => return Ok(indexes),
        };
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            // Expected pattern: column_{index_name}.idx
            if !name.starts_with("column_") || !name.ends_with(".idx") {
                continue;
            }
            let stem = &name["column_".len()..name.len() - ".idx".len()];
            let index_name = stem.to_string();

            // Resolve table/column name from registry
            let (table_name, column_name) = match index_registry.resolve_index_name(&index_name) {
                Some((t, c)) => (t, c),
                None => {
                    // Fallback: parse "table.column" format
                    let parts: Vec<&str> = stem.splitn(2, '.').collect();
                    if parts.len() == 2 {
                        (parts[0].to_string(), parts[1].to_string())
                    } else {
                        debug_log!(
                            "[load_column_indexes] Skipping {}: cannot resolve table/column",
                            name
                        );
                        continue;
                    }
                }
            };

            let config = crate::index::column_value::ColumnValueIndexConfig::default();
            match ColumnValueIndex::open(entry.path(), table_name, column_name, config) {
                Ok(index) => {
                    debug_log!("[MoteDB] Loaded column index: {}", index_name);
                    indexes.insert(index_name, Arc::new(index));
                }
                Err(e) => {
                    debug_log!(
                        "[MoteDB] Failed to load column index {}: {:?}",
                        index_name,
                        e
                    );
                }
            }
        }
        Ok(indexes)
    }

    // ==================== P1: Async Index Build Pipeline ====================

    /// Start the async index builder pipeline.
    ///
    /// Returns (sender, thread handle). The sender is given to the LSM flush callback.
    /// The builder thread receives batches and builds indexes in the background.
    fn start_index_builder_pipeline(
        db: Self,
    ) -> (std::sync::mpsc::Sender<IndexBuildBatch>, IndexBuilderThread) {
        let (tx, rx) = std::sync::mpsc::channel::<IndexBuildBatch>();
        let should_stop = Arc::new(AtomicBool::new(false));
        let should_stop_clone = should_stop.clone();

        let handle = std::thread::Builder::new()
            .name("index-builder".into())
            .spawn(move || {
                debug_log!("[IndexBuilder] Background thread started");
                while !should_stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                            Ok(batch) => {
                                // Drop guard ensures pending_index_batches is ALWAYS decremented,
                                // even if batch_build_table_indexes_raw panics.
                                struct BatchGuard {
                                    pending: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                                }
                                impl Drop for BatchGuard {
                                    fn drop(&mut self) {
                                        self.pending
                                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                }
                                let _guard = BatchGuard {
                                    pending: db.pending_index_batches.clone(),
                                };

                                for (table_name, raw_rows) in &batch.tables_data {
                                    if let Err(e) =
                                        db.batch_build_table_indexes_raw(table_name, raw_rows)
                                    {
                                        warn_log!(
                                            "[IndexBuilder] Index build failed for '{}': {:?}",
                                            table_name,
                                            e
                                        );
                                        db.index_build_errors
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                }
                                debug_log!(
                                    "[IndexBuilder] Processed batch ({} tables)",
                                    batch.tables_data.len()
                                );
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                debug_log!("[IndexBuilder] Channel disconnected, exiting");
                                return false; // signal to exit loop
                            }
                        }
                        true // continue
                    }));
                    match result {
                        Ok(true) => {}
                        Ok(false) | Err(_) => {
                            // Panic or disconnected: continue (don't let thread die)
                            if result.is_err() {
                                error_log!("[MoteDB] Index builder thread panicked, restarting...");
                                std::thread::sleep(std::time::Duration::from_millis(100));
                            } else {
                                break;
                            }
                        }
                    }
                }
                debug_log!("[IndexBuilder] Background thread stopped");
            })
            .expect("Failed to spawn index-builder thread");

        (
            tx,
            IndexBuilderThread {
                handle: Some(handle),
                should_stop,
            },
        )
    }

    /// Extract rows from a flushed memtable and send through the channel.
    ///
    /// This is the LSM flush callback. It only extracts and sends —
    /// the flush thread never blocks on index locks.
    fn extract_and_send_index_batch(
        memtable: &crate::storage::lsm::UnifiedMemTable,
        tx: &std::sync::mpsc::Sender<IndexBuildBatch>,
        registry: &crate::catalog::TableRegistry,
        lsm_engine: &crate::storage::lsm::LSMEngine,
    ) -> crate::Result<()> {
        let memtable_len = memtable.len();
        if memtable_len == 0 {
            return Ok(());
        }

        let mut tables_data: std::collections::HashMap<String, Vec<(RowId, Vec<u8>)>> =
            std::collections::HashMap::new();

        // Cache table_id → table_name to avoid repeated lookups
        let mut name_cache: std::collections::HashMap<u32, String> =
            std::collections::HashMap::new();

        for (composite_key, entry) in memtable.iter() {
            if entry.deleted {
                continue;
            }
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            let table_id = (composite_key >> 32) as u32;

            let row_bytes: Vec<u8> = match &entry.data {
                crate::storage::lsm::ValueData::Inline(bytes) => bytes.to_vec(),
                crate::storage::lsm::ValueData::Blob(blob_ref) => {
                    match lsm_engine.resolve_blob(blob_ref) {
                        Ok(data) => data,
                        Err(_) => continue,
                    }
                }
            };

            // Resolve table_id → table_name (cached, no decode needed)
            if let std::collections::hash_map::Entry::Vacant(e) = name_cache.entry(table_id) {
                let name = if table_id == 0 {
                    "_default".to_string()
                } else {
                    match registry.get_table_name_by_id(table_id) {
                        Ok(n) => n,
                        Err(_) => continue,
                    }
                };
                e.insert(name);
            }

            let table_name = match name_cache.get(&table_id) {
                Some(name) => name,
                None => continue, // Unknown table — skip
            };
            tables_data
                .entry(table_name.to_string())
                .or_default()
                .push((row_id, row_bytes));
        }

        if !tables_data.is_empty() {
            if let Err(e) = tx.send(IndexBuildBatch { tables_data }) {
                debug_log!("[FlushCallback] ⚠️ Failed to send index batch: {:?}", e);
            }
        }

        Ok(())
    }

    /// Start auto-checkpoint background thread
    ///
    /// 🚀 Optimized for embedded environments:
    /// 1. Lazy-checking: Only checks WAL size when interval reached (no unnecessary fs calls)
    /// 2. Start a single background thread for auto-flush requests.
    ///    Replaces the old pattern of spawning a new thread per 2000 writes.
    fn start_auto_flush_thread(db: Self) -> AutoFlushThread {
        let (flush_tx, flush_rx) = std::sync::mpsc::channel::<()>();
        let should_stop = Arc::new(AtomicBool::new(false));
        let should_stop_clone = should_stop.clone();

        let handle = std::thread::Builder::new()
            .name("motedb-auto-flush".into())
            .spawn(move || {
                while !should_stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        match flush_rx.recv_timeout(std::time::Duration::from_millis(200)) {
                            Ok(()) => {
                                if should_stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                                    return false;
                                }
                                while flush_rx.try_recv().is_ok() {}
                                if let Err(e) = db.flush() {
                                    warn_log!("[AutoFlush] Flush failed: {}", e);
                                    db.flush_errors
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                true
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => true,
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => false,
                        }
                    }));
                    match result {
                        Ok(true) => {}
                        Ok(false) | Err(_) => {
                            if result.is_ok() {
                                break;
                            } // disconnected
                            error_log!("[MoteDB] Auto-flush thread panicked, restarting...");
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                    }
                }
                debug_log!("[AutoFlush] Background thread stopped");
            })
            .expect("Failed to spawn auto-flush thread");

        AutoFlushThread {
            flush_tx,
            handle: Some(handle),
            should_stop,
        }
    }

    /// Request an auto-flush via the background thread (non-blocking).
    /// Returns false if the channel is disconnected (thread died).
    pub(crate) fn request_auto_flush(&self) -> bool {
        if let Some(ref t) = self.auto_flush_thread {
            t.flush_tx.send(()).is_ok()
        } else {
            false
        }
    }

    /// 2. Adaptive sleep: Longer intervals in low-activity periods
    /// 3. Zero allocation in hot path
    /// 4. Minimal CPU usage: < 0.1% CPU overhead
    fn start_auto_checkpoint_thread(
        db: Self,
        config: crate::config::AutoCheckpointConfig,
    ) -> AutoCheckpointThread {
        use std::time::{Duration, Instant};

        let should_stop = Arc::new(AtomicBool::new(false));
        let should_stop_clone = should_stop.clone();

        let handle = std::thread::spawn(move || {
            let mut last_checkpoint = Instant::now();

            // 🚀 Adaptive check interval:
            // - Start with min_interval (avoid too-frequent checks)
            // - Only check WAL size when interval reached
            let check_interval = Duration::from_secs(config.min_interval_secs.max(10));

            debug_log!("[AutoCheckpoint] 🚀 Background thread started (embedded-optimized)");
            debug_log!(
                "[AutoCheckpoint] Config: max_wal={}MB, interval={}s, check_every={}s",
                config.max_wal_size_bytes / 1024 / 1024,
                config.min_interval_secs,
                check_interval.as_secs()
            );

            while !should_stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                // 🚀 **CRITICAL FIX**: Use interruptible sleep (check every 1s)
                // This allows fast shutdown when Drop is called
                let mut remaining = check_interval;
                while remaining > Duration::ZERO {
                    if should_stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                        debug_log!("[AutoCheckpoint] 🛑 Shutdown signal received during sleep");
                        break;
                    }

                    // Use 100ms chunks so shutdown is responsive (was 1s)
                    let sleep_chunk = Duration::from_millis(100).min(remaining);
                    std::thread::sleep(sleep_chunk);
                    remaining = remaining.saturating_sub(sleep_chunk);
                }

                // Check if stop signal was set during sleep
                if should_stop_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                // 🚀 Only check WAL size when enough time has passed
                // (avoids unnecessary filesystem calls)
                let elapsed = last_checkpoint.elapsed();
                if elapsed.as_secs() < config.min_interval_secs {
                    continue;
                }

                // 🚀 Lazy WAL size check - only when needed
                let wal_dir = db.path.join("wal");
                match super::helpers::dir_size(&wal_dir) {
                    Ok(wal_size) if wal_size >= config.max_wal_size_bytes => {
                        debug_log!(
                            "[AutoCheckpoint] 🔔 Trigger: WAL {}MB >= {}MB",
                            wal_size / 1024 / 1024,
                            config.max_wal_size_bytes / 1024 / 1024
                        );

                        // Trigger checkpoint
                        if let Err(e) = db.checkpoint() {
                            debug_log!("[AutoCheckpoint] ⚠️  Checkpoint failed: {:?}", e);
                        } else {
                            debug_log!("[AutoCheckpoint] ✅ Checkpoint complete");
                            last_checkpoint = Instant::now();
                        }
                    }
                    Ok(_) => {
                        // WAL size below threshold, skip checkpoint
                    }
                    Err(_e) => {
                        debug_log!("[AutoCheckpoint] ⚠️  Failed to check WAL size: {:?}", _e);
                    }
                }
            }

            debug_log!("[AutoCheckpoint] 👋 Background thread stopped");
        });

        AutoCheckpointThread {
            handle: Some(handle),
            should_stop,
        }
    }

    /// 🚀 Phase 5: Recover AUTO_INCREMENT counter (B3: Crash Recovery)
    ///
    /// Fast path: Read persisted counter from catalog.bin (O(1)).
    /// Slow path: Full table scan (fallback if counter not persisted).
    fn recover_auto_increment_counter(
        &self,
        table_name: &str,
        schema: &crate::types::TableSchema,
    ) -> Result<i64> {
        // Fast path: use persisted counter from catalog.bin
        if let Some(persisted_max) = self.table_registry.get_auto_increment_counter(table_name) {
            debug_log!(
                "[database] ⚡ Recovered AUTO_INCREMENT for '{}' from catalog: {}",
                table_name,
                persisted_max
            );
            return Ok(persisted_max);
        }

        // Slow path: scan all rows to find max ID
        use crate::types::Value;

        let pk_col_name = schema.primary_key().ok_or_else(|| {
            StorageError::InvalidData(format!("Table '{}' has no primary key", table_name))
        })?;
        let pk_col = schema
            .get_column(pk_col_name)
            .ok_or_else(|| StorageError::ColumnNotFound(pk_col_name.to_string()))?;

        let mut max_id = schema.get_auto_increment_start() - 1;

        match self.scan_table_rows_streaming(table_name) {
            Ok(iter) => {
                for result in iter {
                    match result {
                        Ok((_row_id, row)) => {
                            if let Some(Value::Integer(id)) = row.get(pk_col.position) {
                                max_id = max_id.max(*id);
                            }
                        }
                        Err(_e) => {
                            debug_log!(
                                "[database] Warning: Error during AUTO_INCREMENT scan: {:?}",
                                _e
                            );
                            break;
                        }
                    }
                }
            }
            Err(_e) => {
                debug_log!("[database] Warning: Failed to scan table '{}' for AUTO_INCREMENT recovery: {:?}",
                    table_name, _e);
            }
        }

        Ok(max_id)
    }

    /// Explicitly release the exclusive flock. Called by close() so a
    /// subsequent open() on the same directory succeeds (otherwise the lock
    /// is only freed when the MoteDB is dropped, which may be much later if
    /// the caller keeps the handle alive).
    pub fn release_lock(&self) {
        if let Ok(mut guard) = self._lock_file.lock() {
            // Dropping the Option's File releases the OS flock.
            guard.take();
        }
    }

    /// Acquire an exclusive file lock on the database directory.
    ///
    /// Creates a `.lock` file and acquires an exclusive `flock`.
    /// Prevents two processes from opening the same database simultaneously.
    /// The lock is automatically released when the File is dropped (on Drop).
    fn acquire_lock(db_path: &Path) -> Result<std::fs::File> {
        let lock_path = db_path.join(".lock");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        // Try exclusive, non-blocking lock
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
            if result != 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.raw_os_error() == Some(libc::EWOULDBLOCK)
                {
                    return Err(StorageError::InvalidData(
                        "Database is already open by another process".into(),
                    ));
                }
                return Err(StorageError::Io(err));
            }
        }

        // Non-unix: just proceed without file locking
        #[cfg(not(unix))]
        {
            // File locking not supported on this platform
        }

        Ok(file)
    }

    /// Join a background thread with a timeout. If the thread doesn't exit
    /// within `timeout`, it is detached (the Weak references it holds will
    /// naturally become invalid, causing it to exit on its next iteration).
    fn join_with_timeout(
        name: &'static str,
        handle: std::thread::JoinHandle<()>,
        timeout: std::time::Duration,
    ) {
        // 🔑 True timeout join: spawn a helper thread that calls join(),
        // and race it against a sleep. If the thread doesn't exit within
        // timeout, we detach it (it holds Weak<MoteDB> so it exits naturally).
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = handle.join();
            let _ = tx.send(());
        });
        match rx.recv_timeout(timeout) {
            Ok(()) => {
                debug_log!("[MoteDB::Drop] ✅ Thread '{}' joined", name);
            }
            Err(_) => {
                warn_log!(
                    "[MoteDB::Drop] ⚠️ Thread '{}' did not exit within {:?}, detaching",
                    name,
                    timeout
                );
            }
        }
    }

    /// Persist the current write_lsn to a counter file.
    /// Survives clock regression across restarts.
    pub(crate) fn persist_lsn_counter(db_path: &Path, lsn: u64) {
        let path = db_path.join("lsn_counter");
        match std::fs::File::create(&path) {
            Ok(mut file) => {
                if let Err(e) = file.write_all(&lsn.to_le_bytes()) {
                    warn_log!(
                        "[persist_lsn_counter] write failed {}: {}",
                        path.display(),
                        e
                    );
                    return;
                }
                if let Err(e) = file.sync_all() {
                    warn_log!(
                        "[persist_lsn_counter] fsync failed {}: {}",
                        path.display(),
                        e
                    );
                }
            }
            Err(e) => {
                warn_log!(
                    "[persist_lsn_counter] create failed {}: {}",
                    path.display(),
                    e
                );
            }
        }
    }

    /// Load the persisted LSN counter. Returns 0 if the file doesn't exist.
    fn load_lsn_counter(db_path: &Path) -> u64 {
        let path = db_path.join("lsn_counter");
        match std::fs::read(&path) {
            Ok(data) if data.len() >= 8 => {
                u64::from_le_bytes(data[..8].try_into().unwrap_or([0u8; 8]))
            }
            _ => 0,
        }
    }
}

/// Automatic cleanup when database is dropped
///
/// This ensures proper shutdown:
/// 1. Flush all in-memory data (MemTable → SSTable)
/// 2. Persist all indexes
/// 3. Checkpoint WAL (truncate log files)
///
/// This prevents WAL files from accumulating indefinitely and ensures
/// clean shutdown even if user forgets to call checkpoint().
impl Drop for MoteDB {
    fn drop(&mut self) {
        // Clones (used by callback threads) must NOT checkpoint on drop.
        // Only the original MoteDB owns threads and is responsible for final cleanup.
        if self._is_clone {
            return;
        }

        // 🛑 Step 1: Stop index builder thread (drop sender to signal end, then join)
        if let Some(mut thread) = self.index_builder_thread.take() {
            debug_log!("[MoteDB::Drop] 🛑 Stopping index builder thread...");
            self.index_build_tx = None;
            self.is_pipeline_active
                .store(false, std::sync::atomic::Ordering::Relaxed);
            thread
                .should_stop
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(handle) = thread.handle.take() {
                Self::join_with_timeout("index-builder", handle, std::time::Duration::from_secs(5));
            }
        }
        self.index_build_tx = None;
        self.is_pipeline_active
            .store(false, std::sync::atomic::Ordering::Release);

        // 🛑 Step 2: Stop auto-checkpoint thread
        if let Some(mut thread) = self.auto_checkpoint_thread.take() {
            debug_log!("[MoteDB::Drop] 🛑 Stopping auto-checkpoint thread...");
            thread
                .should_stop
                .store(true, std::sync::atomic::Ordering::Release);
            if let Some(handle) = thread.handle.take() {
                Self::join_with_timeout(
                    "auto-checkpoint",
                    handle,
                    std::time::Duration::from_secs(5),
                );
            }
            debug_log!("[MoteDB::Drop] ✅ Auto-checkpoint thread stopped");
        }

        // 🛑 Step 2.5: Stop auto-flush thread
        if let Some(mut thread) = self.auto_flush_thread.take() {
            debug_log!("[MoteDB::Drop] 🛑 Stopping auto-flush thread...");
            thread
                .should_stop
                .store(true, std::sync::atomic::Ordering::Release);
            // Drop sender to unblock recv
            drop(thread.flush_tx);
            if let Some(handle) = thread.handle.take() {
                Self::join_with_timeout("auto-flush", handle, std::time::Duration::from_secs(5));
            }
            debug_log!("[MoteDB::Drop] ✅ Auto-flush thread stopped");
        }

        // Skip columnar store flush on Drop — it can trigger LSM operations
        // that enter backpressure (dead flush thread). Data is already in WAL.
        // if let Err(e) = self.columnar_store.flush_all() {
        //     warn_log!("[Drop] Columnar store flush failed: {:?}", e);
        // }

        if let Err(e) = self.checkpoint_on_drop() {
            warn_log!("[Drop] Final checkpoint failed: {:?}", e);
            warn_log!("[Drop] WAL files may not be cleaned up");
        } else {
            debug_log!("[MoteDB::Drop] ✅ Final checkpoint complete, WAL cleaned");
        }

        debug_log!("[MoteDB::Drop] 👋 Database closed cleanly");
    }
}
