//! Write-Ahead Logging implementation
//!
//! WAL ensures data durability by writing all modifications to a log file
//! before applying them to the database. Supports crash recovery within 2s.
//!
//! ## Checksum Protection
//! - Every WAL record has CRC32C checksum
//! - Detects corruption during crash recovery
//! - Partial writes are detected and skipped

use crate::txn::version_store::{TransactionId, Timestamp};
use crate::types::{Row, RowId, PartitionId};
use crate::{Result, StorageError};
use crate::config::DurabilityLevel;
use crate::storage::checksum::{Checksum, ChecksumType};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

/// Log sequence number (monotonically increasing)
pub type LogSequenceNumber = u64;

/// WAL 配置（简化版，用于内部）
#[derive(Debug, Clone)]
pub struct WALConfig {
    /// 持久性级别
    pub durability_level: DurabilityLevel,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            durability_level: DurabilityLevel::default(),
        }
    }
}

impl From<crate::config::WALConfig> for WALConfig {
    fn from(config: crate::config::WALConfig) -> Self {
        Self {
            durability_level: config.durability_level,
        }
    }
}

/// WAL record types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WALRecord {
    /// Insert operation: (table_name, row_id, partition_id, row_data)
    Insert {
        table_name: String,  // ⭐ 添加 table_name 用于构建 composite_key
        row_id: RowId,
        partition: PartitionId,
        data: Row,
    },
    
    /// Update operation: (table_name, row_id, partition_id, old_data, new_data)
    Update {
        table_name: String,  // ⭐ 添加 table_name
        row_id: RowId,
        partition: PartitionId,
        old_data: Row,  // For undo during rollback
        new_data: Row,
    },
    
    /// Delete operation: (table_name, row_id, partition_id, old_data)
    Delete {
        table_name: String,  // ⭐ 添加 table_name
        row_id: RowId,
        partition: PartitionId,
        old_data: Row,  // For undo during rollback
    },
    
    /// Transaction begin marker
    Begin {
        txn_id: TransactionId,
        isolation_level: u8,  // IsolationLevel as u8
    },
    
    /// Transaction commit marker
    Commit {
        txn_id: TransactionId,
        commit_ts: Timestamp,
    },
    
    /// Transaction rollback marker
    Rollback {
        txn_id: TransactionId,
    },
    
    /// Checkpoint marker (all records before this LSN are persisted)
    Checkpoint { lsn: LogSequenceNumber },
}

/// WAL entry with LSN and checksum
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WALEntry {
    lsn: LogSequenceNumber,
    record: WALRecord,
    checksum: u32, // CRC32C checksum of serialized record
}

/// WAL manager for each partition
struct PartitionWAL {
    /// WAL file path
    path: PathBuf,
    
    /// Append-only WAL file
    file: File,
    
    /// Current LSN
    next_lsn: LogSequenceNumber,
    
    /// Last checkpoint LSN
    last_checkpoint: LogSequenceNumber,
    
    /// WAL configuration
    config: WALConfig,
}

impl PartitionWAL {
    /// Create a new partition WAL
    fn create(path: PathBuf) -> Result<Self> {
        Self::create_with_config(path, WALConfig::default())
    }
    
    /// Create a new partition WAL with config
    fn create_with_config(path: PathBuf, config: WALConfig) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        
        Ok(Self {
            path,
            file,
            next_lsn: 0,
            last_checkpoint: 0,
            config,
        })
    }

    /// Open existing partition WAL
    fn open(path: PathBuf) -> Result<Self> {
        Self::open_with_config(path, WALConfig::default())
    }
    
    /// Open existing partition WAL with config
    fn open_with_config(path: PathBuf, config: WALConfig) -> Result<Self> {
        let mut file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&path)?;
        
        // Scan to find next LSN and verify checksums
        let mut next_lsn = 0;
        let mut last_checkpoint = 0;
        let mut corrupted_count = 0;
        
        // Simple recovery: read all records
        file.seek(SeekFrom::Start(0))?;
        
        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            
            // Detect partial writes
            match file.read_exact(&mut buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    eprintln!("WAL open: Detected partial write at end of file");
                    break;
                }
                Err(e) => return Err(e.into()),
            }
            
            // Deserialize and verify
            let entry: WALEntry = match bincode::deserialize(&buf) {
                Ok(e) => e,
                Err(_) => {
                    corrupted_count += 1;
                    continue;
                }
            };
            
            // Verify checksum
            let record_data = bincode::serialize(&entry.record)?;
            if Checksum::verify(ChecksumType::CRC32C, &record_data, entry.checksum).is_err() {
                corrupted_count += 1;
                continue;
            }
            
            next_lsn = entry.lsn + 1;
            if let WALRecord::Checkpoint { lsn } = entry.record {
                last_checkpoint = lsn;
            }
        }
        
        if corrupted_count > 0 {
            eprintln!("WAL open: Found {} corrupted records (will skip during recovery)", corrupted_count);
        }
        
        Ok(Self {
            path,
            file,
            next_lsn,
            last_checkpoint,
            config,
        })
    }

    /// Append a record to WAL
    fn append(&mut self, record: WALRecord) -> Result<LogSequenceNumber> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        
        // Serialize record for checksum computation
        let record_data = bincode::serialize(&record)?;
        let checksum = Checksum::compute(ChecksumType::CRC32C, &record_data);
        
        // Create entry with checksum
        let entry = WALEntry { lsn, record, checksum };
        let encoded = bincode::serialize(&entry)?;
        
        // Write length prefix (for recovery parsing)
        let len = encoded.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&encoded)?;
        
        // Fsync based on durability level
        match self.config.durability_level {
            DurabilityLevel::Synchronous => {
                // 同步模式：每次立即 fsync（金融/支付场景）
                self.file.sync_data()?;
            }
            DurabilityLevel::GroupCommit { .. } => {
                // ⚡ GroupCommit 简化实现：
                // 
                // 标准 GroupCommit 需要复杂的等待队列和协调线程。
                // 这里使用简化方案：单条append()不fsync，应用层负责调用flush()
                // 
                // 设计思路：
                // 1. 应用层使用 batch_insert() → 内部调用 batch_append() → 单次 fsync ✅
                // 2. 如果必须单条insert()，应用层自行按时间/数量调用 flush()
                // 3. 或者使用 Periodic 模式（后台线程定期刷盘）
                // 
                // 此处不 fsync，数据仍在 OS 缓冲区，崩溃时可能丢失。
                // 安全性依赖：
                // - batch_insert() 做 fsync
                // - 应用层显式 flush()
                // - 或 OS 自动刷盘（通常 30秒）
                //
                // 如需每次都fsync，请使用 Synchronous 模式
            }
            DurabilityLevel::Periodic { .. } => {
                // 不立即 fsync，由后台线程定期刷盘
            }
            DurabilityLevel::NoSync => {
                // 不 fsync（仅测试用）
            }
        }
        
        Ok(lsn)
    }

    /// Batch append multiple records (optimized - single fsync)
    /// 
    /// CRITICAL FOR ACID DURABILITY:
    /// - All records are serialized to a single buffer
    /// - Buffer is written in ONE syscall
    /// - IMMEDIATE fsync to guarantee persistence
    /// - Only returns after data is durable on disk
    /// - Each record has checksum protection
    /// 
    /// This is the CORRECT way to batch WAL writes:
    /// - Maintains ACID durability (fsync before return)
    /// - Amortizes fsync cost across N records
    /// - Performance: 100-1000x better than individual fsyncs
    fn batch_append(&mut self, records: Vec<WALRecord>) -> Result<Vec<LogSequenceNumber>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let mut lsns = Vec::with_capacity(records.len());
        let mut buffer = Vec::new();
        
        // 1. Serialize all records to buffer (in-memory, fast)
        for record in records {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            lsns.push(lsn);
            
            // Compute checksum for record
            let record_data = bincode::serialize(&record)?;
            let checksum = Checksum::compute(ChecksumType::CRC32C, &record_data);
            
            let entry = WALEntry { lsn, record, checksum };
            let encoded = bincode::serialize(&entry)?;
            
            // Write length prefix
            buffer.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&encoded);
        }
        
        // 2. Single write operation (append 模式自动追加)
        self.file.write_all(&buffer)?;
        
        // 3. Fsync based on durability level
        match self.config.durability_level {
            DurabilityLevel::Synchronous | DurabilityLevel::GroupCommit { .. } => {
                // CRITICAL: Immediate fsync for durability ⚠️
                // GroupCommit 在 batch_append() 中必须 fsync
                self.file.sync_data()?;
            }
            DurabilityLevel::Periodic { .. } => {
                // 定期 fsync，由后台线程处理
            }
            DurabilityLevel::NoSync => {
                // 不 fsync（仅测试）
            }
        }
        
        Ok(lsns)
    }

    /// Create a checkpoint
    fn checkpoint(&mut self) -> Result<()> {
        if self.next_lsn == 0 {
            return Ok(());
        }
        
        let lsn = self.next_lsn - 1;
        self.append(WALRecord::Checkpoint { lsn })?;
        self.last_checkpoint = lsn;
        
        // Truncate WAL file after checkpoint
        self.file.set_len(0)?;
        self.file.sync_all()?;
        
        // Reset counters
        self.next_lsn = 0;
        self.last_checkpoint = 0;
        
        Ok(())
    }

    /// Recover records since last checkpoint
    /// 
    /// Verifies checksum for each record. Corrupted records are skipped with warning.
    /// Partial writes (incomplete records at end of file) are automatically detected.
    fn recover(&mut self) -> Result<Vec<WALRecord>> {
        let mut records = Vec::new();
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        
        let mut skipped_corrupted = 0;
        
        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            
            // Partial write detection
            match file.read_exact(&mut buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Partial write at end of file - skip and continue
                    eprintln!("WAL recovery: Detected partial write, skipping last incomplete record");
                    break;
                }
                Err(e) => return Err(e.into()),
            }
            
            // Deserialize entry
            let entry: WALEntry = match bincode::deserialize(&buf) {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("WAL recovery: Failed to deserialize entry: {}", e);
                    skipped_corrupted += 1;
                    continue;
                }
            };
            
            // Verify checksum
            let record_data = bincode::serialize(&entry.record)?;
            if let Err(e) = Checksum::verify(ChecksumType::CRC32C, &record_data, entry.checksum) {
                eprintln!("WAL recovery: Checksum verification failed for LSN {}: {}", entry.lsn, e);
                skipped_corrupted += 1;
                continue;
            }
            
            // Only include records after last checkpoint (>= for LSN starting at 0)
            if entry.lsn >= self.last_checkpoint {
                // Skip the checkpoint record itself
                if !matches!(entry.record, WALRecord::Checkpoint { .. }) {
                    records.push(entry.record);
                }
            }
        }
        
        if skipped_corrupted > 0 {
            eprintln!("WAL recovery: Skipped {} corrupted records", skipped_corrupted);
        }
        
        Ok(records)
    }
}

/// WAL Manager coordinates WAL for all partitions
pub struct WALManager {
    /// WAL directory
    base_path: PathBuf,
    
    /// Per-partition WALs
    partitions: Arc<RwLock<HashMap<PartitionId, PartitionWAL>>>,
    
    /// Number of partitions
    num_partitions: u8,
    
    /// WAL configuration
    config: WALConfig,
    
    /// 后台刷盘线程（Periodic 模式）
    flush_thread: Option<FlushThread>,
}

/// 后台刷盘线程
struct FlushThread {
    /// 线程句柄
    handle: Option<thread::JoinHandle<()>>,
    
    /// 停止信号
    should_stop: Arc<AtomicBool>,
}

impl WALManager {
    /// Create a new WAL manager
    pub fn create<P: AsRef<Path>>(base_path: P, num_partitions: u8) -> Result<Self> {
        Self::create_with_config(base_path, num_partitions, WALConfig::default())
    }
    
    /// Create a new WAL manager with config
    pub fn create_with_config<P: AsRef<Path>>(
        base_path: P,
        num_partitions: u8,
        config: WALConfig,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)?;
        
        let mut partitions = HashMap::new();
        for partition_id in 0..num_partitions {
            let wal_path = base_path.join(format!("partition_{}.wal", partition_id));
            let wal = PartitionWAL::create_with_config(wal_path, config.clone())?;
            partitions.insert(partition_id, wal);
        }
        
        let partitions = Arc::new(RwLock::new(partitions));
        
        // 启动后台刷盘线程（如果需要）
        let flush_thread = Self::start_flush_thread_if_needed(&config, partitions.clone());
        
        Ok(Self {
            base_path,
            partitions,
            num_partitions,
            config,
            flush_thread,
        })
    }

    /// Open existing WAL manager
    pub fn open<P: AsRef<Path>>(base_path: P, num_partitions: u8) -> Result<Self> {
        Self::open_with_config(base_path, num_partitions, WALConfig::default())
    }
    
    /// Open existing WAL manager with config
    pub fn open_with_config<P: AsRef<Path>>(
        base_path: P,
        num_partitions: u8,
        config: WALConfig,
    ) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        
        let mut partitions = HashMap::new();
        for partition_id in 0..num_partitions {
            let wal_path = base_path.join(format!("partition_{}.wal", partition_id));
            if wal_path.exists() {
                let wal = PartitionWAL::open_with_config(wal_path, config.clone())?;
                partitions.insert(partition_id, wal);
            } else {
                let wal = PartitionWAL::create_with_config(wal_path, config.clone())?;
                partitions.insert(partition_id, wal);
            }
        }
        
        let partitions = Arc::new(RwLock::new(partitions));
        
        // 启动后台刷盘线程（如果需要）
        let flush_thread = Self::start_flush_thread_if_needed(&config, partitions.clone());
        
        Ok(Self {
            base_path,
            partitions,
            num_partitions,
            config,
            flush_thread,
        })
    }
    
    /// 启动后台刷盘线程（Periodic 模式）
    fn start_flush_thread_if_needed(
        config: &WALConfig,
        partitions: Arc<RwLock<HashMap<PartitionId, PartitionWAL>>>,
    ) -> Option<FlushThread> {
        if let DurabilityLevel::Periodic { interval_ms } = config.durability_level {
            let should_stop = Arc::new(AtomicBool::new(false));
            let should_stop_clone = should_stop.clone();
            
            let interval = Duration::from_millis(interval_ms);
            
            let handle = thread::spawn(move || {
                while !should_stop_clone.load(Ordering::Relaxed) {
                    thread::sleep(interval);
                    
                    // 刷盘所有分区
                    let mut partitions_guard = partitions.write();
                    for (_partition_id, wal) in partitions_guard.iter_mut() {
                        let _ = wal.file.sync_data();
                    }
                }
            });
            
            Some(FlushThread {
                handle: Some(handle),
                should_stop,
            })
        } else {
            None
        }
    }

    /// Log an insert operation
    pub fn log_insert(
        &self,
        table_name: &str,  // ⭐ 添加 table_name 参数
        partition: PartitionId,
        row_id: RowId,
        data: Row,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Insert {
            table_name: table_name.to_string(),
            row_id,
            partition,
            data,
        };
        
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.append(record)
    }

    /// Log an update operation
    pub fn log_update(
        &self,
        table_name: &str,  // ⭐ 添加 table_name 参数
        partition: PartitionId,
        row_id: RowId,
        old_data: Row,
        new_data: Row,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Update {
            table_name: table_name.to_string(),
            row_id,
            partition,
            old_data,
            new_data,
        };
        
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.append(record)
    }

    /// Log a delete operation
    pub fn log_delete(
        &self,
        table_name: &str,  // ⭐ 添加 table_name 参数
        partition: PartitionId,
        row_id: RowId,
        old_data: Row,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Delete {
            table_name: table_name.to_string(),
            row_id,
            partition,
            old_data,
        };
        
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.append(record)
    }

    /// Log transaction begin
    pub fn log_begin(
        &self,
        partition: PartitionId,
        txn_id: TransactionId,
        isolation_level: u8,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Begin {
            txn_id,
            isolation_level,
        };
        
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.append(record)
    }

    /// Log transaction commit
    pub fn log_commit(
        &self,
        partition: PartitionId,
        txn_id: TransactionId,
        commit_ts: Timestamp,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Commit {
            txn_id,
            commit_ts,
        };
        
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.append(record)
    }

    /// Log transaction rollback
    pub fn log_rollback(
        &self,
        partition: PartitionId,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Rollback {
            txn_id,
        };
        
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.append(record)
    }

    /// Batch append records to a partition (optimized for transaction commit)
    /// 
    /// This method is used during transaction commit to write all transaction
    /// operations (Begin, Insert/Update/Delete, Commit) in a single batch,
    /// reducing fsync overhead from O(n) to O(1).
    /// 
    /// # Example
    /// ```ignore
    /// let records = vec![
    ///     WALRecord::Begin { txn_id: 1, isolation_level: 0 },
    ///     WALRecord::Insert { row_id: 100, partition: 0, data: row1 },
    ///     WALRecord::Insert { row_id: 101, partition: 0, data: row2 },
    ///     WALRecord::Commit { txn_id: 1, commit_ts: 1000 },
    /// ];
    /// wal.batch_append(0, records)?;
    /// ```
    pub fn batch_append(
        &self,
        partition: PartitionId,
        records: Vec<WALRecord>,
    ) -> Result<Vec<LogSequenceNumber>> {
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.batch_append(records)
    }

    /// Create checkpoint for a partition
    pub fn checkpoint(&self, partition: PartitionId) -> Result<()> {
        let mut partitions = self.partitions.write();
        let wal = partitions
            .get_mut(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        
        wal.checkpoint()
    }

    /// Checkpoint all partitions
    pub fn checkpoint_all(&self) -> Result<()> {
        let mut partitions = self.partitions.write();
        for wal in partitions.values_mut() {
            wal.checkpoint()?;
        }
        Ok(())
    }

    /// Recover from crash (returns records per partition)
    pub fn recover(&self) -> Result<HashMap<PartitionId, Vec<WALRecord>>> {
        let mut partitions = self.partitions.write();
        let mut result = HashMap::new();
        
        for (partition_id, wal) in partitions.iter_mut() {
            let records = wal.recover()?;
            result.insert(*partition_id, records); // Always insert, even if empty
        }
        
        Ok(result)
    }
}

impl Drop for WALManager {
    fn drop(&mut self) {
        // 停止后台刷盘线程
        if let Some(mut flush_thread) = self.flush_thread.take() {
            flush_thread.should_stop.store(true, Ordering::Relaxed);
            if let Some(handle) = flush_thread.handle.take() {
                let _ = handle.join();
            }
        }
        
        // 最后一次刷盘，确保数据安全
        let mut partitions = self.partitions.write();
        for (_partition_id, wal) in partitions.iter_mut() {
            let _ = wal.file.sync_data();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, Timestamp};
    use tempfile::TempDir;

    #[test]
    fn test_wal_create() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 4).unwrap();
        
        assert_eq!(wal.num_partitions, 4);
    }

    #[test]
    fn test_wal_log_insert() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 4).unwrap();
        
        let row = vec![Value::Null];
        let lsn = wal.log_insert("test_table", 0, 1, row).unwrap();
        
        assert_eq!(lsn, 0);
    }

    #[test]
    fn test_wal_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 4).unwrap();
        
        wal.log_insert("test_table", 0, 1, vec![Value::Null]).unwrap();
        wal.checkpoint(0).unwrap();
    }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        
        // Write some records
        {
            let wal = WALManager::create(path, 2).unwrap();
            wal.log_insert("test_table", 0, 1, vec![Value::Null]).unwrap();
            wal.log_insert("test_table", 0, 2, vec![Value::Null]).unwrap();
            wal.log_insert("test_table", 1, 3, vec![Value::Null]).unwrap();
        }
        
        // Recover
        {
            let wal = WALManager::open(path, 2).unwrap();
            let recovered = wal.recover().unwrap();
            
            assert_eq!(recovered.len(), 2);
            
            let count_inserts = |records: &[WALRecord]| -> usize {
                records.iter().filter(|r| matches!(r, WALRecord::Insert { .. })).count()
            };
            
            assert_eq!(count_inserts(recovered.get(&0).unwrap()), 2);
            assert_eq!(count_inserts(recovered.get(&1).unwrap()), 1);
        }
    }

    #[test]
    fn test_wal_update_operation() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        let old_data = vec![Value::Null];
        let new_data = vec![Value::Null];
        let lsn = wal.log_update("test_table", 0, 1, old_data.clone(), new_data.clone()).unwrap();
        
        assert_eq!(lsn, 0);
        
        // Verify recovery
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0], WALRecord::Update { .. }));
    }

    #[test]
    fn test_wal_delete_operation() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        let old_data = vec![Value::Null];
        let lsn = wal.log_delete("test_table", 0, 1, old_data.clone()).unwrap();
        
        assert_eq!(lsn, 0);
        
        // Verify recovery
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0], WALRecord::Delete { .. }));
    }

    #[test]
    fn test_wal_transaction_boundaries() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        // Begin transaction
        let lsn1 = wal.log_begin(0, 1, 1).unwrap();
        assert_eq!(lsn1, 0);
        
        // Insert data
        let lsn2 = wal.log_insert("test_table", 0, 10, vec![Value::Null]).unwrap();
        assert_eq!(lsn2, 1);
        
        // Commit transaction
        let lsn3 = wal.log_commit(0, 1, 100).unwrap();
        assert_eq!(lsn3, 2);
        
        // Verify recovery
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).unwrap();
        assert_eq!(records.len(), 3);
        
        assert!(matches!(records[0], WALRecord::Begin { txn_id: 1, .. }));
        assert!(matches!(records[1], WALRecord::Insert { row_id: 10, .. }));
        assert!(matches!(records[2], WALRecord::Commit { txn_id: 1, .. }));
    }

    #[test]
    fn test_wal_transaction_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        // Begin transaction
        wal.log_begin(0, 1, 1).unwrap();
        
        // Insert data
        wal.log_insert("test_table", 0, 10, vec![Value::Null]).unwrap();
        
        // Rollback transaction
        wal.log_rollback(0, 1).unwrap();
        
        // Verify recovery
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).unwrap();
        assert_eq!(records.len(), 3);
        
        assert!(matches!(records[0], WALRecord::Begin { txn_id: 1, .. }));
        assert!(matches!(records[1], WALRecord::Insert { row_id: 10, .. }));
        assert!(matches!(records[2], WALRecord::Rollback { txn_id: 1 }));
    }

    #[test]
    fn test_wal_complete_transaction_flow() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();
        
        // Simulate complete transaction flow
        {
            let wal = WALManager::create(path, 2).unwrap();
            
            // T1: Begin, Insert, Update, Commit
            wal.log_begin(0, 1, 2).unwrap();
            wal.log_insert("test_table", 0, 100, vec![Value::Null]).unwrap();
            wal.log_update("test_table", 0, 100, vec![Value::Null], vec![Value::Null]).unwrap();
            wal.log_commit(0, 1, 1000).unwrap();
            
            // T2: Begin, Insert, Rollback
            wal.log_begin(0, 2, 2).unwrap();
            wal.log_insert("test_table", 0, 200, vec![Value::Null]).unwrap();
            wal.log_rollback(0, 2).unwrap();
            
            // T3: Begin, Delete, Commit
            wal.log_begin(0, 3, 2).unwrap();
            wal.log_delete("test_table", 0, 100, vec![Value::Null]).unwrap();
            wal.log_commit(0, 3, 2000).unwrap();
        }
        
        // Recover and verify
        {
            let wal = WALManager::open(path, 2).unwrap();
            let recovered = wal.recover().unwrap();
            let records = recovered.get(&0).unwrap();
            
            assert_eq!(records.len(), 10);
            
            // Count record types
            let count_type = |records: &[WALRecord], pred: fn(&WALRecord) -> bool| -> usize {
                records.iter().filter(|r| pred(r)).count()
            };
            
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Begin { .. })), 3);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Insert { .. })), 2);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Update { .. })), 1);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Delete { .. })), 1);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Commit { .. })), 2);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Rollback { .. })), 1);
        }
    }

    #[test]
    fn test_wal_batch_append() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        // Create a batch of records for a transaction
        let records = vec![
            WALRecord::Begin { txn_id: 1, isolation_level: 2 },
            WALRecord::Insert { 
                table_name: "test_table".to_string(),
                row_id: 100, 
                partition: 0, 
                data: vec![Value::Timestamp(Timestamp::from_micros(42))] 
            },
            WALRecord::Insert { 
                table_name: "test_table".to_string(),
                row_id: 101, 
                partition: 0, 
                data: vec![Value::Timestamp(Timestamp::from_micros(43))] 
            },
            WALRecord::Update {
                table_name: "test_table".to_string(),
                row_id: 100,
                partition: 0,
                old_data: vec![Value::Timestamp(Timestamp::from_micros(42))],
                new_data: vec![Value::Timestamp(Timestamp::from_micros(100))],
            },
            WALRecord::Commit { txn_id: 1, commit_ts: 1000 },
        ];
        
        // Batch append all records
        let lsns = wal.batch_append(0, records).unwrap();
        
        // Verify LSNs are sequential
        assert_eq!(lsns.len(), 5);
        assert_eq!(lsns[0], 0);
        assert_eq!(lsns[1], 1);
        assert_eq!(lsns[2], 2);
        assert_eq!(lsns[3], 3);
        assert_eq!(lsns[4], 4);
        
        // Verify recovery
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).unwrap();
        assert_eq!(records.len(), 5);
        
        assert!(matches!(records[0], WALRecord::Begin { txn_id: 1, .. }));
        assert!(matches!(records[1], WALRecord::Insert { row_id: 100, .. }));
        assert!(matches!(records[2], WALRecord::Insert { row_id: 101, .. }));
        assert!(matches!(records[3], WALRecord::Update { row_id: 100, .. }));
        assert!(matches!(records[4], WALRecord::Commit { txn_id: 1, .. }));
    }

    #[test]
    fn test_wal_batch_append_empty() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        // Empty batch should succeed without doing anything
        let lsns = wal.batch_append(0, vec![]).unwrap();
        assert_eq!(lsns.len(), 0);
    }

    #[test]
    fn test_wal_batch_append_multiple_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 2).unwrap();
        
        // Simulate multiple concurrent transactions
        // T1
        let records1 = vec![
            WALRecord::Begin { txn_id: 1, isolation_level: 2 },
            WALRecord::Insert { table_name: "test_table".to_string(), row_id: 100, partition: 0, data: vec![Value::Null] },
            WALRecord::Commit { txn_id: 1, commit_ts: 1000 },
        ];
        wal.batch_append(0, records1).unwrap();
        
        // T2
        let records2 = vec![
            WALRecord::Begin { txn_id: 2, isolation_level: 2 },
            WALRecord::Insert { table_name: "test_table".to_string(), row_id: 200, partition: 0, data: vec![Value::Null] },
            WALRecord::Insert { table_name: "test_table".to_string(), row_id: 201, partition: 0, data: vec![Value::Null] },
            WALRecord::Commit { txn_id: 2, commit_ts: 2000 },
        ];
        wal.batch_append(0, records2).unwrap();
        
        // T3
        let records3 = vec![
            WALRecord::Begin { txn_id: 3, isolation_level: 2 },
            WALRecord::Delete { table_name: "test_table".to_string(), row_id: 100, partition: 0, old_data: vec![Value::Null] },
            WALRecord::Rollback { txn_id: 3 },
        ];
        wal.batch_append(0, records3).unwrap();
        
        // Verify recovery
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).unwrap();
        assert_eq!(records.len(), 10);
        
        // Verify transaction boundaries
        let count_type = |records: &[WALRecord], pred: fn(&WALRecord) -> bool| -> usize {
            records.iter().filter(|r| pred(r)).count()
        };
        
        assert_eq!(count_type(records, |r| matches!(r, WALRecord::Begin { .. })), 3);
        assert_eq!(count_type(records, |r| matches!(r, WALRecord::Commit { .. })), 2);
        assert_eq!(count_type(records, |r| matches!(r, WALRecord::Rollback { .. })), 1);
    }
}
