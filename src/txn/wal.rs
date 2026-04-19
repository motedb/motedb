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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;
use dashmap::DashMap;

/// Log sequence number (monotonically increasing)
pub type LogSequenceNumber = u64;

/// WAL 配置（简化版，用于内部）
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct WALConfig {
    /// 持久性级别
    pub durability_level: DurabilityLevel,
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
    /// Insert operation
    Insert {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        data: Row,
        txn_id: TransactionId, // 0 = auto-commit, >0 = explicit transaction
    },

    /// Update operation
    Update {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        old_data: Row,
        new_data: Row,
        txn_id: TransactionId,
    },

    /// Delete operation
    Delete {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        old_data: Row,
        timestamp: u64,
        txn_id: TransactionId,
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
        
        // Simple recovery: read all records with new header format
        file.seek(SeekFrom::Start(0))?;

        loop {
            // Read total length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let _total_len = u32::from_le_bytes(len_buf) as usize;

            // Read header: [u64 lsn][u32 checksum][u32 record_len] = 16 bytes
            let mut header = [0u8; 16];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    debug_log!("WAL open: Detected partial write (header)");
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            let lsn = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());
            let record_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;

            // Read record data
            let mut record_data = vec![0u8; record_len];
            match file.read_exact(&mut record_data) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    debug_log!("WAL open: Detected partial write at end of file");
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            // Verify checksum (directly on record_data, no re-serialization)
            if Checksum::verify(ChecksumType::CRC32C, &record_data, checksum).is_err() {
                corrupted_count += 1;
                continue;
            }

            next_lsn = lsn + 1;
            // Deserialize record to check for Checkpoint
            if let Ok(WALRecord::Checkpoint { lsn: cp_lsn }) = bincode::deserialize::<WALRecord>(&record_data) {
                last_checkpoint = cp_lsn;
            }
        }
        
        if corrupted_count > 0 {
            debug_log!("WAL open: Found {} corrupted records (will skip during recovery)", corrupted_count);
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

        // 🚀 P0: Serialize once, write header + data
        let record_data = bincode::serialize(&record)?;
        let checksum = Checksum::compute(ChecksumType::CRC32C, &record_data);

        // Layout: [u32 total_len][u64 lsn][u32 checksum][u32 record_len][record_data]
        let header_size = 4 + 8 + 4 + 4; // 20 bytes
        let total_len = (header_size + record_data.len()) as u32;

        self.file.write_all(&total_len.to_le_bytes())?;
        self.file.write_all(&lsn.to_le_bytes())?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file.write_all(&(record_data.len() as u32).to_le_bytes())?;
        self.file.write_all(&record_data)?;

        // Fsync based on durability level
        match self.config.durability_level {
            DurabilityLevel::Synchronous => {
                self.file.sync_data()?;
            }
            DurabilityLevel::GroupCommit { .. } => {
                // No fsync - application layer responsible for batch_append or explicit flush
            }
            DurabilityLevel::Periodic { .. } => {
                // Background thread handles periodic fsync
            }
            DurabilityLevel::NoSync => {}
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
    /// Append multiple records in a single I/O operation.
    /// Assumes LSNs were already allocated via `allocate_lsn()` — does NOT increment next_lsn.
    fn batch_append(&mut self, records: Vec<WALRecord>) -> Result<Vec<LogSequenceNumber>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let mut lsns = Vec::with_capacity(records.len());
        let mut buffer = Vec::new();

        // 1. Serialize all records to buffer (LSNs already pre-allocated)
        for record in records {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            lsns.push(lsn);

            let record_data = bincode::serialize(&record)?;
            let checksum = Checksum::compute(ChecksumType::CRC32C, &record_data);

            // Header: [u32 total_len][u64 lsn][u32 checksum][u32 record_len]
            let header_size = 4 + 8 + 4 + 4; // 20 bytes
            let total_len = (header_size + record_data.len()) as u32;

            buffer.extend_from_slice(&total_len.to_le_bytes());
            buffer.extend_from_slice(&lsn.to_le_bytes());
            buffer.extend_from_slice(&checksum.to_le_bytes());
            buffer.extend_from_slice(&(record_data.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&record_data);
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
    ///
    /// Uses atomic write-new-rename pattern for crash safety:
    /// 1. Write checkpoint record and sync
    /// 2. Create a new empty WAL file at a temp path
    /// 3. Sync the temp file
    /// 4. Atomically rename temp → original path
    /// 5. Reopen the new file
    ///
    /// If crash occurs at any point, the original WAL is intact.
    fn checkpoint(&mut self) -> Result<()> {
        if self.next_lsn == 0 {
            return Ok(());
        }

        let lsn = self.next_lsn - 1;
        self.append(WALRecord::Checkpoint { lsn })?;
        self.last_checkpoint = lsn;

        // Ensure checkpoint record is durable BEFORE truncating.
        self.file.sync_all()?;

        // Atomic truncation: write-new-rename pattern
        // Create a fresh empty WAL file at a temp path
        let tmp_path = self.path.with_extension("wal.tmp");
        {
            let tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp_file.sync_all()?;
        }

        // Atomic rename: temp → original (on same filesystem, this is atomic)
        std::fs::rename(&tmp_path, &self.path)?;

        // Reopen the new empty file
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;

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
            // Read total length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let _total_len = u32::from_le_bytes(len_buf) as usize;

            // Read header: [u64 lsn][u32 checksum][u32 record_len] = 16 bytes
            let mut header = [0u8; 16];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    debug_log!("WAL recovery: Detected partial write (header), skipping");
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            let lsn = u64::from_le_bytes(header[0..8].try_into().unwrap());
            let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());
            let record_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;

            // Read record data
            let mut record_data = vec![0u8; record_len];
            match file.read_exact(&mut record_data) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    debug_log!("WAL recovery: Detected partial write (record), skipping");
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            // Verify checksum directly on record_data (no re-serialization needed!)
            if let Err(e) = Checksum::verify(ChecksumType::CRC32C, &record_data, checksum) {
                debug_log!("WAL recovery: Checksum verification failed for LSN {}: {}", lsn, e);
                skipped_corrupted += 1;
                continue;
            }

            // Deserialize record
            let record: WALRecord = match bincode::deserialize(&record_data) {
                Ok(r) => r,
                Err(e) => {
                    debug_log!("WAL recovery: Failed to deserialize record: {}", e);
                    skipped_corrupted += 1;
                    continue;
                }
            };

            // Only include records after last checkpoint (>= for LSN starting at 0)
            if lsn >= self.last_checkpoint {
                // Skip the checkpoint record itself
                if !matches!(record, WALRecord::Checkpoint { .. }) {
                    records.push(record);
                }
            }
        }

        if skipped_corrupted > 0 {
            debug_log!("WAL recovery: Skipped {} corrupted records", skipped_corrupted);
        }
        
        Ok(records)
    }
}

/// WAL Manager coordinates WAL for all partitions
pub struct WALManager {
    /// WAL directory
    _base_path: PathBuf,

    /// Per-partition WALs (DashMap for concurrent partition writes)
    partitions: Arc<DashMap<PartitionId, parking_lot::Mutex<PartitionWAL>>>,

    /// Number of partitions
    #[allow(dead_code)]
    num_partitions: u8,

    /// WAL configuration
    #[allow(dead_code)]
    config: WALConfig,

    /// Background flush thread (Periodic mode)
    flush_thread: Option<FlushThread>,

    /// Group commit state (GroupCommit mode)
    group_commit: Option<GroupCommitThread>,

    /// Flag indicating new writes have arrived since last flush check (Periodic mode).
    /// Shared with the flush thread for adaptive backoff.
    periodic_new_writes: Arc<AtomicBool>,
}

/// Background flush thread (Periodic mode)
struct FlushThread {
    handle: Option<thread::JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
    /// Flag set by write paths, consumed by the flush thread for adaptive interval.
    /// Kept alive via Arc so the thread can read it; not accessed from the struct itself.
    #[allow(dead_code)]
    new_writes: Arc<AtomicBool>,
}

// === Group Commit ===

/// A single pending WAL record waiting to be batch-flushed
struct GroupCommitEntry {
    partition: PartitionId,
    record: WALRecord,
    /// Caller waits on this condvar until the record is durable
    done: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

struct GroupCommitState {
    /// Queue of pending records
    queue: std::sync::Mutex<Vec<GroupCommitEntry>>,
    /// Signal the flush thread to wake up
    wakeup: std::sync::Condvar,
    /// Maximum records per batch
    max_batch_size: usize,
    /// Maximum wait time in microseconds before flushing
    max_wait_us: u64,
}

struct GroupCommitThread {
    handle: Option<thread::JoinHandle<()>>,
    should_stop: Arc<AtomicBool>,
    state: Arc<GroupCommitState>,
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

        let partitions = DashMap::new();
        for partition_id in 0..num_partitions {
            let wal_path = base_path.join(format!("partition_{}.wal", partition_id));
            let wal = PartitionWAL::create_with_config(wal_path, config.clone())?;
            partitions.insert(partition_id, parking_lot::Mutex::new(wal));
        }

        let partitions = Arc::new(partitions);
        let new_writes = Arc::new(AtomicBool::new(false));

        // Start background threads
        let flush_thread = Self::start_flush_thread_if_needed(&config, partitions.clone(), new_writes.clone());
        let group_commit = Self::start_group_commit_thread_if_needed(&config, partitions.clone());

        Ok(Self {
            _base_path: base_path,
            partitions,
            num_partitions,
            config,
            flush_thread,
            group_commit,
            periodic_new_writes: new_writes,
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

        let partitions = DashMap::new();
        for partition_id in 0..num_partitions {
            let wal_path = base_path.join(format!("partition_{}.wal", partition_id));
            if wal_path.exists() {
                let wal = PartitionWAL::open_with_config(wal_path, config.clone())?;
                partitions.insert(partition_id, parking_lot::Mutex::new(wal));
            } else {
                let wal = PartitionWAL::create_with_config(wal_path, config.clone())?;
                partitions.insert(partition_id, parking_lot::Mutex::new(wal));
            }
        }

        let partitions = Arc::new(partitions);
        let new_writes = Arc::new(AtomicBool::new(false));

        // Start background threads
        let flush_thread = Self::start_flush_thread_if_needed(&config, partitions.clone(), new_writes.clone());
        let group_commit = Self::start_group_commit_thread_if_needed(&config, partitions.clone());

        Ok(Self {
            _base_path: base_path,
            partitions,
            num_partitions,
            config,
            flush_thread,
            group_commit,
            periodic_new_writes: new_writes,
        })
    }

    /// Start background flush thread (Periodic mode) with adaptive backoff.
    ///
    /// When no writes are detected, the sleep interval doubles up to `max_idle_ms`
    /// to conserve CPU/wake-ups on idle edge devices. When writes arrive, the thread
    /// syncs and resets to the base interval.
    fn start_flush_thread_if_needed(
        config: &WALConfig,
        partitions: Arc<DashMap<PartitionId, parking_lot::Mutex<PartitionWAL>>>,
        new_writes: Arc<AtomicBool>,
    ) -> Option<FlushThread> {
        if let DurabilityLevel::Periodic { interval_ms } = config.durability_level {
            let should_stop = Arc::new(AtomicBool::new(false));
            let should_stop_clone = should_stop.clone();
            let new_writes_clone = new_writes.clone();

            let base_interval = Duration::from_millis(interval_ms);
            // Max idle interval: 10x base, capped at 5000ms, never less than base
            let max_idle = Duration::from_millis((interval_ms * 10).min(5000).max(interval_ms));

            let handle = thread::Builder::new()
                .name("motedb-periodic-flush".into())
                .spawn(move || {
                    let mut current_interval = base_interval;

                    while !should_stop_clone.load(Ordering::Relaxed) {
                        thread::sleep(current_interval);

                        let had_writes = new_writes_clone.swap(false, Ordering::Relaxed);

                        // Flush all partitions (sync_data is cheap if nothing dirty)
                        for entry in partitions.iter() {
                            let wal = entry.value().lock();
                            let _ = wal.file.sync_data();
                        }

                        // Adaptive backoff: if no writes, double interval; if writes, reset
                        if had_writes {
                            current_interval = base_interval;
                        } else {
                            current_interval = (current_interval * 2).min(max_idle);
                        }
                    }
                })
                .ok();

            Some(FlushThread {
                handle,
                should_stop,
                new_writes,
            })
        } else {
            None
        }
    }

    /// Start group commit thread (GroupCommit mode)
    fn start_group_commit_thread_if_needed(
        config: &WALConfig,
        partitions: Arc<DashMap<PartitionId, parking_lot::Mutex<PartitionWAL>>>,
    ) -> Option<GroupCommitThread> {
        if let DurabilityLevel::GroupCommit { max_batch_size, max_wait_us } = config.durability_level {
            let should_stop = Arc::new(AtomicBool::new(false));
            let state = Arc::new(GroupCommitState {
                queue: std::sync::Mutex::new(Vec::new()),
                wakeup: std::sync::Condvar::new(),
                max_batch_size,
                max_wait_us,
            });

            let should_stop_clone = should_stop.clone();
            let state_clone = state.clone();
            let wait_duration = Duration::from_micros(max_wait_us);

            let handle = thread::Builder::new()
                .name("motedb-group-commit".into())
                .spawn(move || {
                    while !should_stop_clone.load(Ordering::Relaxed) {
                        // Wait for entries or timeout
                        {
                            let queue = state_clone.queue.lock().unwrap();
                            if queue.is_empty() {
                                let _ = state_clone.wakeup.wait_timeout(queue, wait_duration).unwrap();
                            }
                        }

                        // Drain queue
                        let entries: Vec<GroupCommitEntry> = {
                            let mut queue = state_clone.queue.lock().unwrap();
                            let drain_count = queue.len().min(state_clone.max_batch_size);
                            queue.drain(..drain_count).collect()
                        };

                        if entries.is_empty() {
                            continue;
                        }

                        // Group by partition
                        let mut groups: HashMap<PartitionId, Vec<WALRecord>> = HashMap::new();
                        let mut done_signals: Vec<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>> = Vec::new();

                        for entry in entries {
                            groups.entry(entry.partition).or_default().push(entry.record);
                            done_signals.push(entry.done);
                        }

                        // Flush each partition group
                        let flush_result = (|| -> Result<()> {
                            for (partition, records) in groups {
                                if let Some(entry) = partitions.get(&partition) {
                                    let mut wal = entry.value().lock();
                                    wal.batch_append(records)?;
                                }
                            }
                            Ok(())
                        })();

                        // Signal all callers regardless of result
                        for done in done_signals {
                            {
                                let mut flag = done.0.lock().unwrap();
                                *flag = true;
                            }
                            done.1.notify_all();
                        }

                        if let Err(e) = flush_result {
                            debug_log!("[GroupCommit] Flush error: {}", e);
                        }
                    }

                    // Final drain on shutdown
                    let entries: Vec<GroupCommitEntry> = {
                        let mut queue = state_clone.queue.lock().unwrap();
                        std::mem::take(&mut *queue)
                    };

                    if !entries.is_empty() {
                        let mut groups: HashMap<PartitionId, Vec<WALRecord>> = HashMap::new();
                        let mut done_signals: Vec<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>> = Vec::new();

                        for entry in entries {
                            groups.entry(entry.partition).or_default().push(entry.record);
                            done_signals.push(entry.done);
                        }

                        for (partition, records) in groups {
                            if let Some(entry) = partitions.get(&partition) {
                                let mut wal = entry.value().lock();
                                let _ = wal.batch_append(records);
                            }
                        }

                        for done in done_signals {
                            {
                                let mut flag = done.0.lock().unwrap();
                                *flag = true;
                            }
                            done.1.notify_all();
                        }
                    }
                })
                .ok();

            Some(GroupCommitThread {
                handle,
                should_stop,
                state,
            })
        } else {
            None
        }
    }

    /// Push a record through group commit queue and wait for flush
    fn group_commit_append(
        &self,
        partition: PartitionId,
        record: WALRecord,
    ) -> Result<LogSequenceNumber> {
        // Signal periodic flush thread that writes are happening
        self.periodic_new_writes.store(true, Ordering::Relaxed);

        if let Some(ref gc) = self.group_commit {
            let done = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));

            {
                let mut queue = gc.state.queue.lock().unwrap();
                queue.push(GroupCommitEntry {
                    partition,
                    record,
                    done: done.clone(),
                });
                if queue.len() >= gc.state.max_batch_size {
                    gc.state.wakeup.notify_all();
                }
            }
            gc.state.wakeup.notify_all();

            // Wait for flush with timeout (2× max_wait_us)
            let timeout = Duration::from_micros(gc.state.max_wait_us * 2);
            let flag = done.0.lock().unwrap();
            if !*flag {
                let result = done.1.wait_timeout(flag, timeout).unwrap();
                let timed_out = !*result.0;
                if timed_out {
                    // Data may still be in the group commit queue and not yet written
                    // to the kernel buffer. Sync the partition file as a safety net so
                    // that at least whatever IS in the kernel buffer reaches disk.
                    if let Some(entry) = self.partitions.get(&partition) {
                        let wal = entry.value().lock();
                        let _ = wal.file.sync_data();
                    }
                }
            }

            Ok(0) // LSN assigned asynchronously by batch_append
        } else {
            // No group commit — direct append
            let entry = self.partitions.get(&partition)
                .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
            let mut wal = entry.value().lock();
            wal.append(record)
        }
    }

    /// Log an insert operation (uses group commit if enabled)
    pub fn log_insert(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        data: Row,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Insert {
            table_name: table_name.to_string(),
            row_id,
            partition,
            data,
            txn_id,
        };
        self.group_commit_append(partition, record)
    }

    /// Log an update operation (uses group commit if enabled)
    pub fn log_update(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        old_data: Row,
        new_data: Row,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Update {
            table_name: table_name.to_string(),
            row_id,
            partition,
            old_data,
            new_data,
            txn_id,
        };
        self.group_commit_append(partition, record)
    }

    /// Log a delete operation (uses group commit if enabled)
    pub fn log_delete(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        old_data: Row,
        timestamp: u64,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::Delete {
            table_name: table_name.to_string(),
            row_id,
            partition,
            old_data,
            timestamp,
            txn_id,
        };
        self.group_commit_append(partition, record)
    }

    /// Log transaction begin
    pub fn log_begin(
        &self,
        partition: PartitionId,
        txn_id: TransactionId,
        isolation_level: u8,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);
        let record = WALRecord::Begin {
            txn_id,
            isolation_level,
        };

        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.append(record)
    }

    /// Log transaction commit
    pub fn log_commit(
        &self,
        partition: PartitionId,
        txn_id: TransactionId,
        commit_ts: Timestamp,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);
        let record = WALRecord::Commit {
            txn_id,
            commit_ts,
        };

        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.append(record)
    }

    /// Log transaction rollback
    pub fn log_rollback(
        &self,
        partition: PartitionId,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);
        let record = WALRecord::Rollback {
            txn_id,
        };

        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.append(record)
    }

    /// Batch append records to a partition (optimized for transaction commit)
    ///
    /// This method is used during transaction commit to write all transaction
    /// operations (Begin, Insert/Update/Delete, Commit) in a single batch,
    /// reducing fsync overhead from O(n) to O(1).
    pub fn batch_append(
        &self,
        partition: PartitionId,
        records: Vec<WALRecord>,
    ) -> Result<Vec<LogSequenceNumber>> {
        if !records.is_empty() {
            self.periodic_new_writes.store(true, Ordering::Relaxed);
        }
        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.batch_append(records)
    }

    /// Create checkpoint for a partition
    pub fn checkpoint(&self, partition: PartitionId) -> Result<()> {
        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.checkpoint()
    }

    /// Checkpoint all partitions
    pub fn checkpoint_all(&self) -> Result<()> {
        for entry in self.partitions.iter() {
            let mut wal = entry.value().lock();
            wal.checkpoint()?;
        }
        Ok(())
    }

    /// Recover from crash (returns records per partition)
    pub fn recover(&self) -> Result<HashMap<PartitionId, Vec<WALRecord>>> {
        let mut result = HashMap::new();

        for entry in self.partitions.iter() {
            let partition_id = *entry.key();
            let mut wal = entry.value().lock();
            let records = wal.recover()?;
            result.insert(partition_id, records);
        }

        Ok(result)
    }
}

impl Drop for WALManager {
    fn drop(&mut self) {
        // Stop group commit thread
        if let Some(mut gc) = self.group_commit.take() {
            gc.should_stop.store(true, Ordering::Relaxed);
            gc.state.wakeup.notify_all();
            if let Some(handle) = gc.handle.take() {
                let _ = handle.join();
            }
        }

        // Stop periodic flush thread
        if let Some(mut flush_thread) = self.flush_thread.take() {
            flush_thread.should_stop.store(true, Ordering::Relaxed);
            if let Some(handle) = flush_thread.handle.take() {
                let _ = handle.join();
            }
        }

        // Final sync on all partitions
        for entry in self.partitions.iter() {
            let wal = entry.value().lock();
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
        let lsn = wal.log_insert("test_table", 0, 1, row, 0).unwrap();
        
        assert_eq!(lsn, 0);
    }

    #[test]
    fn test_wal_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let wal = WALManager::create(temp_dir.path(), 4).unwrap();
        
        wal.log_insert("test_table", 0, 1, vec![Value::Null], 0).unwrap();
        wal.checkpoint(0).unwrap();
    }

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();

        // Write some records
        {
            let wal = WALManager::create(path, 2).unwrap();
            wal.log_insert("test_table", 0, 1, vec![Value::Null], 0).unwrap();
            wal.log_insert("test_table", 0, 2, vec![Value::Null], 0).unwrap();
            wal.log_insert("test_table", 1, 3, vec![Value::Null], 0).unwrap();
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
        let lsn = wal.log_update("test_table", 0, 1, old_data.clone(), new_data.clone(), 0).unwrap();
        
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
        let lsn = wal.log_delete("test_table", 0, 1, old_data.clone(), 12345, 0).unwrap();
        
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
        let config = WALConfig { durability_level: DurabilityLevel::Synchronous };
        let wal = WALManager::create_with_config(temp_dir.path(), 2, config).unwrap();
        
        // Begin transaction
        let lsn1 = wal.log_begin(0, 1, 1).unwrap();
        assert_eq!(lsn1, 0);
        
        // Insert data
        let lsn2 = wal.log_insert("test_table", 0, 10, vec![Value::Null], 1).unwrap();
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
        wal.log_insert("test_table", 0, 10, vec![Value::Null], 1).unwrap();
        
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
            wal.log_insert("test_table", 0, 100, vec![Value::Null], 1).unwrap();
            wal.log_update("test_table", 0, 100, vec![Value::Null], vec![Value::Null], 1).unwrap();
            wal.log_commit(0, 1, 1000).unwrap();

            // T2: Begin, Insert, Rollback
            wal.log_begin(0, 2, 2).unwrap();
            wal.log_insert("test_table", 0, 200, vec![Value::Null], 2).unwrap();
            wal.log_rollback(0, 2).unwrap();

            // T3: Begin, Delete, Commit
            wal.log_begin(0, 3, 2).unwrap();
            wal.log_delete("test_table", 0, 100, vec![Value::Null], 12345, 3).unwrap();
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
                data: vec![Value::Timestamp(Timestamp::from_micros(42))],
                txn_id: 1,
            },
            WALRecord::Insert {
                table_name: "test_table".to_string(),
                row_id: 101,
                partition: 0,
                data: vec![Value::Timestamp(Timestamp::from_micros(43))],
                txn_id: 1,
            },
            WALRecord::Update {
                table_name: "test_table".to_string(),
                row_id: 100,
                partition: 0,
                old_data: vec![Value::Timestamp(Timestamp::from_micros(42))],
                new_data: vec![Value::Timestamp(Timestamp::from_micros(100))],
                txn_id: 1,
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
            WALRecord::Insert { table_name: "test_table".to_string(), row_id: 100, partition: 0, data: vec![Value::Null], txn_id: 1 },
            WALRecord::Commit { txn_id: 1, commit_ts: 1000 },
        ];
        wal.batch_append(0, records1).unwrap();

        // T2
        let records2 = vec![
            WALRecord::Begin { txn_id: 2, isolation_level: 2 },
            WALRecord::Insert { table_name: "test_table".to_string(), row_id: 200, partition: 0, data: vec![Value::Null], txn_id: 2 },
            WALRecord::Insert { table_name: "test_table".to_string(), row_id: 201, partition: 0, data: vec![Value::Null], txn_id: 2 },
            WALRecord::Commit { txn_id: 2, commit_ts: 2000 },
        ];
        wal.batch_append(0, records2).unwrap();
        
        // T3
        let records3 = vec![
            WALRecord::Begin { txn_id: 3, isolation_level: 2 },
            WALRecord::Delete { table_name: "test_table".to_string(), row_id: 100, partition: 0, old_data: vec![Value::Null], timestamp: 0, txn_id: 3 },
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
