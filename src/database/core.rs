//! Database Core - MoteDB Structure and Initialization
//!
//! Extracted from database_legacy.rs (4,798 lines) as part of modularization
//! This module contains:
//! - MoteDB struct definition
//! - create() / create_with_config()
//! - open() with WAL recovery
//! - Index loading helpers

use crate::config::DBConfig;
use crate::index::btree::{BTree, BTreeConfig};
use crate::index::vamana::{DiskANNIndex, VamanaConfig};
use crate::index::{SpatialHybridIndex, SpatialHybridConfig, BoundingBoxF32};
use crate::index::text_fts::TextFTSIndex;
use crate::index::column_value::ColumnValueIndex;
use crate::storage::{LSMEngine, LSMConfig};
use crate::txn::coordinator::TransactionCoordinator;
use crate::txn::version_store::VersionStore;
use crate::txn::wal::{WALManager, WALRecord};
use crate::types::{Row, RowId, BoundingBox};
use crate::catalog::TableRegistry;
use crate::cache::RowCache;
use crate::{Result, StorageError};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// Database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub total_rows: RowId,
    pub num_partitions: u8,
}

/// Vector index statistics
#[derive(Debug, Clone)]
pub struct VectorIndexStats {
    pub total_vectors: usize,
    pub dimension: usize,
    pub cache_hit_rate: f32,
    pub memory_usage: usize,
    pub disk_usage: usize,
}

/// Spatial index statistics
#[derive(Debug, Clone)]
pub struct SpatialIndexStats {
    pub total_entries: usize,
    pub memory_usage: usize,
    pub bytes_per_entry: usize,
}

/// MoteDB instance
pub struct MoteDB {
    /// Database file path
    pub(crate) path: PathBuf,

    /// WAL manager
    pub(crate) wal: Arc<WALManager>,
    
    /// LSM-Tree storage engine (main data storage)
    pub(crate) lsm_engine: Arc<LSMEngine>,

    /// Primary key index (DEPRECATED: redundant row_id → row_id mapping)
    /// Kept for backward compatibility, no longer used
    #[deprecated(note = "Primary key index is redundant and no longer used")]
    pub(crate) primary_key: Arc<RwLock<HashMap<RowId, RowId>>>,

    /// Timestamp index (using BTree for persistent storage)
    pub(crate) timestamp_index: Arc<RwLock<BTree>>,

    /// Next row ID
    pub(crate) next_row_id: Arc<RwLock<RowId>>,

    /// Number of partitions
    pub(crate) num_partitions: u8,

    /// Transaction coordinator
    pub(crate) txn_coordinator: Arc<TransactionCoordinator>,

    /// Version store for MVCC
    pub(crate) version_store: Arc<VersionStore>,
    
    /// Pending index updates counter (for triggering background flush)
    pub(crate) pending_updates: Arc<RwLock<usize>>,
    
    /// Pending spatial index updates counter
    pub(crate) pending_spatial_updates: Arc<RwLock<usize>>,
    
    /// 🚀 Vector indexes (DiskANN) - 使用 DashMap 提升并发性能
    pub(crate) vector_indexes: Arc<DashMap<String, Arc<RwLock<DiskANNIndex>>>>,
    
    /// 🚀 Spatial indexes (Hybrid Grid+RTree) - 使用 DashMap 提升并发性能
    pub(crate) spatial_indexes: Arc<DashMap<String, Arc<RwLock<SpatialHybridIndex>>>>,
    
    /// 🚀 Text indexes (FTS with single-file B-Tree) - 使用 DashMap 提升并发性能
    pub text_indexes: Arc<DashMap<String, Arc<RwLock<TextFTSIndex>>>>,
    
    /// 🚀 Column value indexes (for WHERE optimization) - 使用 DashMap 提升并发性能
    pub column_indexes: Arc<DashMap<String, Arc<RwLock<ColumnValueIndex>>>>,
    
    /// Table registry (catalog)
    pub(crate) table_registry: Arc<TableRegistry>,
    
    /// 🆕 Index metadata registry
    pub(crate) index_registry: Arc<crate::database::index_metadata::IndexRegistry>,
    
    /// 🚀 P1: Row cache (hot data cache)
    pub(crate) row_cache: Arc<RowCache>,
    
    /// 🚀 P1: Table name → hash cache (避免重复计算 hash) - 使用 DashMap 提升并发性能
    /// Format: table_name → table_hash (u64)
    pub(crate) table_hash_cache: Arc<DashMap<String, u64>>,
    
    /// 🚀 Phase 3+: Index update strategy
    pub(crate) index_update_strategy: crate::config::IndexUpdateStrategy,
    
    /// 🆕 防止递归 flush 的标志
    pub(crate) is_flushing: Arc<AtomicBool>,
}

impl MoteDB {
    /// Create a new database
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_config(path, DBConfig::default())
    }
    
    /// Create a new database with custom configuration
    pub fn create_with_config<P: AsRef<Path>>(path: P, config: DBConfig) -> Result<Self> {
        let path = path.as_ref();
        let db_path = path.with_extension("mote");
        
        // 🎯 统一目录结构：所有文件放在 {name}.mote/ 目录下
        std::fs::create_dir_all(&db_path)?;
        
        let wal_path = db_path.join("wal");
        let lsm_dir = db_path.join("lsm");
        let indexes_dir = db_path.join("indexes");

        let num_partitions = config.num_partitions;

        // Create WAL directory with config
        std::fs::create_dir_all(&wal_path)?;
        let wal_config = crate::txn::wal::WALConfig::from(config.wal_config);
        let wal = Arc::new(WALManager::create_with_config(&wal_path, num_partitions, wal_config)?);

        // Create primary key index (in-memory)
        let primary_key = Arc::new(RwLock::new(HashMap::new()));

        // Create timestamp index with BTree storage (放在 indexes/ 目录)
        std::fs::create_dir_all(&indexes_dir)?;
        let timestamp_storage = indexes_dir.join("timestamp.idx");
        let btree_config = BTreeConfig {
            unique_keys: false,  // Allow duplicate timestamps
            allow_updates: true,
            ..Default::default()
        };
        let timestamp_index = Arc::new(RwLock::new(BTree::with_config(timestamp_storage, btree_config)?));
        
        // Create LSM-Tree storage engine
        std::fs::create_dir_all(&lsm_dir)?;
        let lsm_engine = Arc::new(LSMEngine::new(lsm_dir, LSMConfig::default())?);

        // Create version store and transaction coordinator
        let version_store = Arc::new(VersionStore::new());
        let txn_coordinator = Arc::new(TransactionCoordinator::new(version_store.clone()));
        
        // Create table registry (catalog)
        let table_registry = Arc::new(TableRegistry::new(&db_path)?);
        
        // 🆕 Create index metadata registry
        let index_registry = Arc::new(crate::database::index_metadata::IndexRegistry::new(&db_path));
        
        // 🚀 P1: Create row cache (default 10000 rows ≈ 10MB)
        let row_cache = Arc::new(RowCache::new(config.row_cache_size.unwrap_or(10000)));
        
        // 🚀 P1: Create table hash cache and register "_default" table
        let table_hash_cache = Arc::new(DashMap::new());
        {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            "_default".hash(&mut hasher);
            let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;  // Take lower 32 bits
            table_hash_cache.insert("_default".to_string(), table_hash);
        }

        let db = Self {
            path: db_path,
            wal,
            lsm_engine: lsm_engine.clone(),
            primary_key,
            timestamp_index,
            next_row_id: Arc::new(RwLock::new(0)),
            num_partitions,
            txn_coordinator,
            version_store,
            pending_updates: Arc::new(RwLock::new(0)),
            pending_spatial_updates: Arc::new(RwLock::new(0)),
            vector_indexes: Arc::new(DashMap::new()),
            spatial_indexes: Arc::new(DashMap::new()),
            text_indexes: Arc::new(DashMap::new()),
            column_indexes: Arc::new(DashMap::new()),
            table_registry,
            index_registry,  // 🆕
            row_cache,
            table_hash_cache,
            index_update_strategy: config.index_update_strategy.clone(),  // 🚀 Phase 3+
            is_flushing: Arc::new(AtomicBool::new(false)),  // 🆕 防止递归
        };
        
        // 🚀 Unified Flush Callback: 统一入口（手动+后台Flush）
        // 传入 MemTable 引用，零拷贝批量构建所有索引
        let db_clone = db.clone_for_callback();
        lsm_engine.set_flush_callback(move |memtable| {
            db_clone.batch_build_indexes_from_flush(memtable)
        })?;
        
        Ok(db)
    }
    
    /// Clone self for callback (only what's needed)
    pub(crate) fn clone_for_callback(&self) -> Self {
        Self {
            path: self.path.clone(),
            wal: self.wal.clone(),
            lsm_engine: self.lsm_engine.clone(),
            primary_key: self.primary_key.clone(),
            timestamp_index: self.timestamp_index.clone(),
            next_row_id: self.next_row_id.clone(),
            num_partitions: self.num_partitions,
            txn_coordinator: self.txn_coordinator.clone(),
            version_store: self.version_store.clone(),
            pending_updates: self.pending_updates.clone(),
            pending_spatial_updates: self.pending_spatial_updates.clone(),
            vector_indexes: self.vector_indexes.clone(),
            spatial_indexes: self.spatial_indexes.clone(),
            text_indexes: self.text_indexes.clone(),
            column_indexes: self.column_indexes.clone(),
            table_registry: self.table_registry.clone(),
            index_registry: self.index_registry.clone(),  // 🆕
            row_cache: self.row_cache.clone(),
            table_hash_cache: self.table_hash_cache.clone(),  // 🚀 P1
            index_update_strategy: self.index_update_strategy.clone(),  // 🚀 Phase 3+
            is_flushing: self.is_flushing.clone(),  // 🆕 共享 flush 标志
        }
    }

    /// Open an existing database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let db_path = path.with_extension("mote");
        
        // 🎯 统一目录结构：从 {name}.mote/ 目录读取
        let wal_path = db_path.join("wal");
        let lsm_dir = db_path.join("lsm");
        let indexes_dir = db_path.join("indexes");

        // Default number of partitions
        let num_partitions = 4;

        // Open or create WAL
        let wal = if wal_path.exists() {
            Arc::new(WALManager::open(&wal_path, num_partitions)?)
        } else {
            std::fs::create_dir_all(&wal_path)?;
            Arc::new(WALManager::create(&wal_path, num_partitions)?)
        };

        // Recover from WAL
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
        
        // Primary key index has been removed (redundant: row_id → row_id)
        // We keep the field for now but don't populate it
        let primary_key_map = HashMap::new();
        let mut max_row_id = if persisted_count > 0 {
            // Estimate max_row_id from persisted count
            // Since row_ids are sequential starting from 0, max is count-1
            (persisted_count - 1) as u64
        } else {
            0
        };

        // Replay WAL records (if any uncommitted changes after last checkpoint)
        for (_partition, records) in &recovered_records {
            for record in records {
                if let WALRecord::Insert { row_id, data, .. } = record {
                    // 🔧 Primary Key 已移除（冗余）
                    // primary_key_map.insert(*row_id, *row_id);
                    max_row_id = max_row_id.max(*row_id);
                    
                    // Also insert into timestamp index
                    if let Some(crate::types::Value::Timestamp(ts)) = data.first() {
                        let _ = timestamp_idx.insert(ts.as_micros() as u64, *row_id);
                    }
                }
            }
        }

        let primary_key = Arc::new(RwLock::new(primary_key_map));
        let timestamp_index = Arc::new(RwLock::new(timestamp_idx));
        
        // Open LSM-Tree storage engine (从统一目录)
        std::fs::create_dir_all(&lsm_dir)?;
        let lsm_engine = Arc::new(LSMEngine::new(lsm_dir, LSMConfig::default())?);

        // ⭐ 关键修复：将 WAL 恢复的数据写回 LSM Engine
        // 现在 WAL 记录了 table_name，可以正确构建 composite_key
        println!("[database] 恢复 WAL 记录到 LSM Engine...");
        let mut recovered_count = 0;
        for (_partition, records) in &recovered_records {
            for record in records {
                use std::hash::{Hash, Hasher};
                match record {
                    WALRecord::Insert { table_name, row_id, data, .. } => {
                        // 构建 composite_key = hash(table_name) << 32 | row_id
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        table_name.hash(&mut hasher);
                        let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;  // Take lower 32 bits
                        let composite_key = (table_hash << 32) | (*row_id & 0xFFFFFFFF);
                        
                        // 将数据恢复到 LSM Engine
                        let row_data = bincode::serialize(data)?;
                        let value = crate::storage::lsm::Value::new(row_data, composite_key);
                        lsm_engine.put(composite_key, value)?;
                        recovered_count += 1;
                    }
                    WALRecord::Update { table_name, row_id, new_data, .. } => {
                        // 更新操作：构建 composite_key 并更新
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        table_name.hash(&mut hasher);
                        let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;  // Take lower 32 bits
                        let composite_key = (table_hash << 32) | (*row_id & 0xFFFFFFFF);
                        
                        let row_data = bincode::serialize(new_data)?;
                        let value = crate::storage::lsm::Value::new(row_data, composite_key);
                        lsm_engine.put(composite_key, value)?;
                        recovered_count += 1;
                    }
                    WALRecord::Delete { table_name, row_id, .. } => {
                        // 删除操作：构建 composite_key 并删除
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        table_name.hash(&mut hasher);
                        let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;  // Take lower 32 bits
                        let composite_key = (table_hash << 32) | (*row_id & 0xFFFFFFFF);
                        
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(|e| StorageError::InvalidData(e.to_string()))?
                            .as_micros() as u64;
                        lsm_engine.delete(composite_key, timestamp)?;
                        recovered_count += 1;
                    }
                    _ => {}
                }
            }
        }
        println!("[database] WAL 恢复完成，恢复了 {} 条记录", recovered_count);

        // Create version store and transaction coordinator
        let version_store = Arc::new(VersionStore::new());
        let txn_coordinator = Arc::new(TransactionCoordinator::new(version_store.clone()));

        // Load existing vector indexes
        let vector_indexes = Self::load_vector_indexes(&db_path)?;
        
        // Load existing spatial indexes
        let spatial_indexes = Self::load_spatial_indexes(&db_path)?;
        
        // Load existing text indexes
        let text_indexes = Self::load_text_indexes(&db_path)?;
        
        // Load table registry (catalog)
        let table_registry = Arc::new(TableRegistry::new(&db_path)?);
        
        // 🆕 Load index metadata registry
        let index_registry = Arc::new(crate::database::index_metadata::IndexRegistry::new(&db_path));
        let _ = index_registry.load();  // Ignore error if file doesn't exist
        
        // 🚀 P1: Create row cache (default 10000 rows)
        let row_cache = Arc::new(RowCache::new(10000));
        
        // 🚀 P1: Create table hash cache and populate from registry + "_default"
        let table_hash_cache = Arc::new(DashMap::new());
        {
            use std::hash::{Hash, Hasher};
            // Always register "_default" table
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            "_default".hash(&mut hasher);
            let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;  // Take lower 32 bits
            table_hash_cache.insert("_default".to_string(), table_hash);
            
            // Then populate from registry
            for table_name in table_registry.list_tables()? {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                table_name.hash(&mut hasher);
                let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;  // Take lower 32 bits
                table_hash_cache.insert(table_name, table_hash);
            }
        }

        let db = Self {
            path: db_path,
            wal,
            lsm_engine: lsm_engine.clone(),
            primary_key,
            timestamp_index,
            next_row_id: Arc::new(RwLock::new(max_row_id + 1)),
            num_partitions,
            txn_coordinator,
            version_store,
            pending_updates: Arc::new(RwLock::new(0)),
            pending_spatial_updates: Arc::new(RwLock::new(0)),
            vector_indexes: Arc::new(Self::hashmap_to_dashmap(vector_indexes)),
            spatial_indexes: Arc::new(Self::hashmap_to_dashmap(spatial_indexes)),
            text_indexes: Arc::new(Self::hashmap_to_dashmap(text_indexes)),
            column_indexes: Arc::new(DashMap::new()),  // Empty for now, will be loaded on-demand
            table_registry,
            index_registry,  // 🆕
            row_cache,
            table_hash_cache,  // 🚀 P1
            index_update_strategy: crate::config::IndexUpdateStrategy::default(),  // 🚀 Phase 3+ (默认 BatchOnly)
            is_flushing: Arc::new(AtomicBool::new(false)),  // 🆕 防止递归
        };
        
        // 🚀 Unified Flush Callback: 统一入口（手动+后台Flush）
        // 传入 MemTable 引用，零拷贝批量构建所有索引
        let db_clone = db.clone_for_callback();
        lsm_engine.set_flush_callback(move |memtable| {
            db_clone.batch_build_indexes_from_flush(memtable)
        })?;
        
        Ok(db)
    }
    
    /// 🚀 Helper: Convert HashMap to DashMap
    fn hashmap_to_dashmap<K: std::hash::Hash + Eq, V>(map: HashMap<K, V>) -> DashMap<K, V> {
        let dashmap = DashMap::new();
        for (k, v) in map {
            dashmap.insert(k, v);
        }
        dashmap
    }
    
    /// Load existing vector indexes from disk
    fn load_vector_indexes(db_path: &Path) -> Result<HashMap<String, Arc<RwLock<DiskANNIndex>>>> {
        let mut indexes = HashMap::new();
        
        // 🎯 从统一目录加载：{db}.mote/indexes/vector_*/
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    if let Ok(name) = entry.file_name().into_string() {
                        if name.starts_with("vector_") {
                            let index_name = name.strip_prefix("vector_").unwrap();
                            let index_path = entry.path();
                            
                            // Try to load the index
                            let config = VamanaConfig::default();
                            if let Ok(index) = DiskANNIndex::load(&index_path, config) {
                                indexes.insert(
                                    index_name.to_string(), 
                                    Arc::new(RwLock::new(index))
                                );
                                println!("[MoteDB] Loaded vector index: {}", index_name);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(indexes)
    }
    
    /// Load existing spatial indexes from disk
    fn load_spatial_indexes(db_path: &Path) -> Result<HashMap<String, Arc<RwLock<SpatialHybridIndex>>>> {
        let mut indexes = HashMap::new();
        
        // 🎯 从统一目录加载：{db}.mote/indexes/spatial_*/
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    if let Ok(name) = entry.file_name().into_string() {
                        if name.starts_with("spatial_") {
                            let index_name = name.strip_prefix("spatial_").unwrap();
                            let index_path = entry.path();
                            
                            // Try to load with default config (will use saved config from metadata)
                            let default_config = SpatialHybridConfig::new(
                                BoundingBoxF32::new(0.0, 0.0, 1000.0, 1000.0)
                            ).with_mmap(true, Some(index_path.clone()));
                            
                            if let Ok(index) = SpatialHybridIndex::load(&index_path, default_config) {
                                indexes.insert(
                                    index_name.to_string(),
                                    Arc::new(RwLock::new(index))
                                );
                                println!("[MoteDB] Loaded spatial index: {}", index_name);
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
        
        // 🎯 从统一目录加载：{db}.mote/indexes/text_*/
        let indexes_dir = db_path.join("indexes");
        if indexes_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&indexes_dir) {
                for entry in entries.flatten() {
                    if let Ok(name) = entry.file_name().into_string() {
                        if name.starts_with("text_") {
                            let index_name = name.strip_prefix("text_").unwrap();
                            let index_path = entry.path();
                            
                            // Try to load the index
                            if let Ok(index) = TextFTSIndex::new(index_path) {
                                indexes.insert(
                                    index_name.to_string(),
                                    Arc::new(RwLock::new(index))
                                );
                                println!("[MoteDB] Loaded text index: {}", index_name);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(indexes)
    }
}
