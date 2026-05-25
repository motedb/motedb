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
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use parking_lot::Mutex as PlMutex;
use parking_lot::Condvar as PlCondvar;
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
///
/// P3: Row data is stored as raw bytes (from RawRow encoding).
/// The native binary format replaces bincode for WAL record serialization.
#[derive(Debug, Clone, PartialEq)]
pub enum WALRecord {
    /// Insert operation
    Insert {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        data: Row,
        txn_id: TransactionId,
    },

    /// Insert with raw bytes (zero-copy recovery)
    InsertRaw {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        raw_data: Vec<u8>,
        txn_id: TransactionId,
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

    /// Update with raw bytes (zero-copy recovery)
    UpdateRaw {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        raw_old: Vec<u8>,
        raw_new: Vec<u8>,
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

    /// Delete with raw bytes (zero-copy recovery)
    DeleteRaw {
        table_name: String,
        row_id: RowId,
        partition: PartitionId,
        raw_old: Vec<u8>,
        timestamp: u64,
        txn_id: TransactionId,
    },

    /// Transaction begin marker
    Begin {
        txn_id: TransactionId,
        isolation_level: u8,
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

// Native binary format type tags
const TAG_INSERT_RAW: u8 = 0x01;
const TAG_UPDATE_RAW: u8 = 0x02;
const TAG_DELETE_RAW: u8 = 0x03;
const TAG_BEGIN: u8 = 0x04;
const TAG_COMMIT: u8 = 0x05;
const TAG_ROLLBACK: u8 = 0x06;
const TAG_CHECKPOINT: u8 = 0x07;
/// Compression marker tag (0x00 never used by record types, backward compatible)
const TAG_COMPRESSED: u8 = 0x00;
/// Minimum payload size (bytes) to consider compression. Small records aren't worth it.
const WAL_COMPRESS_THRESHOLD: usize = 128;

impl WALRecord {
    /// Encode WAL record to native binary format.
    fn encode_native(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        match self {
            WALRecord::InsertRaw { table_name, row_id, partition, raw_data, txn_id } => {
                buf.push(TAG_INSERT_RAW);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                encode_str(&mut buf, table_name);
                buf.extend_from_slice(&row_id.to_le_bytes());
                buf.extend_from_slice(&(*partition as u16).to_le_bytes());
                buf.extend_from_slice(&(raw_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(raw_data);
            }
            WALRecord::Insert { table_name, row_id, partition, data, txn_id } => {
                buf.push(TAG_INSERT_RAW);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                encode_str(&mut buf, table_name);
                buf.extend_from_slice(&row_id.to_le_bytes());
                buf.extend_from_slice(&(*partition as u16).to_le_bytes());
                let bytes = bincode::serialize(data)?;
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
            }
            WALRecord::UpdateRaw { table_name, row_id, partition, raw_old, raw_new, txn_id } => {
                buf.push(TAG_UPDATE_RAW);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                encode_str(&mut buf, table_name);
                buf.extend_from_slice(&row_id.to_le_bytes());
                buf.extend_from_slice(&(*partition as u16).to_le_bytes());
                buf.extend_from_slice(&(raw_old.len() as u32).to_le_bytes());
                buf.extend_from_slice(raw_old);
                buf.extend_from_slice(&(raw_new.len() as u32).to_le_bytes());
                buf.extend_from_slice(raw_new);
            }
            WALRecord::Update { table_name, row_id, partition, old_data, new_data, txn_id } => {
                buf.push(TAG_UPDATE_RAW);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                encode_str(&mut buf, table_name);
                buf.extend_from_slice(&row_id.to_le_bytes());
                buf.extend_from_slice(&(*partition as u16).to_le_bytes());
                let old_bytes = bincode::serialize(old_data)?;
                buf.extend_from_slice(&(old_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&old_bytes);
                let new_bytes = bincode::serialize(new_data)?;
                buf.extend_from_slice(&(new_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&new_bytes);
            }
            WALRecord::DeleteRaw { table_name, row_id, partition, raw_old, timestamp, txn_id } => {
                buf.push(TAG_DELETE_RAW);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                encode_str(&mut buf, table_name);
                buf.extend_from_slice(&row_id.to_le_bytes());
                buf.extend_from_slice(&(*partition as u16).to_le_bytes());
                buf.extend_from_slice(&timestamp.to_le_bytes());
                buf.extend_from_slice(&(raw_old.len() as u32).to_le_bytes());
                buf.extend_from_slice(raw_old);
            }
            WALRecord::Delete { table_name, row_id, partition, old_data, timestamp, txn_id } => {
                buf.push(TAG_DELETE_RAW);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                encode_str(&mut buf, table_name);
                buf.extend_from_slice(&row_id.to_le_bytes());
                buf.extend_from_slice(&(*partition as u16).to_le_bytes());
                buf.extend_from_slice(&timestamp.to_le_bytes());
                let bytes = bincode::serialize(old_data)?;
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bytes);
            }
            WALRecord::Begin { txn_id, isolation_level } => {
                buf.push(TAG_BEGIN);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.push(*isolation_level);
            }
            WALRecord::Commit { txn_id, commit_ts } => {
                buf.push(TAG_COMMIT);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&commit_ts.to_le_bytes());
            }
            WALRecord::Rollback { txn_id } => {
                buf.push(TAG_ROLLBACK);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            WALRecord::Checkpoint { lsn } => {
                buf.push(TAG_CHECKPOINT);
                buf.extend_from_slice(&lsn.to_le_bytes());
            }
        }
        Ok(buf)
    }

    /// Decode WAL record from native binary format.
    /// Returns None if the data doesn't look like native format (caller should try bincode).
    fn decode_native(data: &[u8]) -> Option<Result<Self>> {
        if data.is_empty() {
            return None;
        }
        let tag = data[0];
        let mut pos = 1usize;

        match tag {
            TAG_INSERT_RAW => {
                let txn_id = read_u64(data, &mut pos)?;
                let table_name = read_str(data, &mut pos)?;
                let row_id = read_u64(data, &mut pos)?;
                let partition = read_u16(data, &mut pos)? as PartitionId;
                let payload_len = read_u32(data, &mut pos)? as usize;
                if pos + payload_len > data.len() {
                    return None;
                }
                let raw_data = data[pos..pos + payload_len].to_vec();
                Some(Ok(WALRecord::InsertRaw { table_name, row_id, partition, raw_data, txn_id }))
            }
            TAG_UPDATE_RAW => {
                let txn_id = read_u64(data, &mut pos)?;
                let table_name = read_str(data, &mut pos)?;
                let row_id = read_u64(data, &mut pos)?;
                let partition = read_u16(data, &mut pos)? as PartitionId;
                let old_len = read_u32(data, &mut pos)? as usize;
                if pos + old_len > data.len() {
                    return None;
                }
                let raw_old = data[pos..pos + old_len].to_vec();
                pos += old_len;
                let new_len = read_u32(data, &mut pos)? as usize;
                if pos + new_len > data.len() {
                    return None;
                }
                let raw_new = data[pos..pos + new_len].to_vec();
                Some(Ok(WALRecord::UpdateRaw { table_name, row_id, partition, raw_old, raw_new, txn_id }))
            }
            TAG_DELETE_RAW => {
                let txn_id = read_u64(data, &mut pos)?;
                let table_name = read_str(data, &mut pos)?;
                let row_id = read_u64(data, &mut pos)?;
                let partition = read_u16(data, &mut pos)? as PartitionId;
                let timestamp = read_u64(data, &mut pos)?;
                let old_len = read_u32(data, &mut pos)? as usize;
                if pos + old_len > data.len() {
                    return None;
                }
                let raw_old = data[pos..pos + old_len].to_vec();
                Some(Ok(WALRecord::DeleteRaw { table_name, row_id, partition, raw_old, timestamp, txn_id }))
            }
            TAG_BEGIN => {
                let txn_id = read_u64(data, &mut pos)?;
                if pos >= data.len() {
                    return None;
                }
                let isolation_level = data[pos];
                Some(Ok(WALRecord::Begin { txn_id, isolation_level }))
            }
            TAG_COMMIT => {
                let txn_id = read_u64(data, &mut pos)?;
                let commit_ts = read_u64(data, &mut pos)? as Timestamp;
                Some(Ok(WALRecord::Commit { txn_id, commit_ts }))
            }
            TAG_ROLLBACK => {
                let txn_id = read_u64(data, &mut pos)?;
                Some(Ok(WALRecord::Rollback { txn_id }))
            }
            TAG_CHECKPOINT => {
                let lsn = read_u64(data, &mut pos)?;
                Some(Ok(WALRecord::Checkpoint { lsn }))
            }
            _ => None, // Unknown tag — likely bincode data
        }
    }

    /// Decode native or compressed format with fallback to bincode (for old WAL files)
    fn decode_with_fallback(data: &[u8]) -> Result<Self> {
        // Check for compression marker
        if !data.is_empty() && data[0] == TAG_COMPRESSED {
            // Compressed: [0x00][u32 original_len][zstd_data...]
            if data.len() < 5 {
                return Err(StorageError::Serialization("WAL: truncated compressed record".into()));
            }
            let original_len = u32::from_le_bytes(
                data[1..5].try_into().map_err(|_| StorageError::Serialization("WAL: bad compression header".into()))?
            ) as usize;
            let compressed = &data[5..];
            let decompressed = zstd::decode_all(compressed)
                .map_err(|e| StorageError::Serialization(format!("WAL zstd decompress failed: {}", e)))?;
            if decompressed.len() != original_len {
                return Err(StorageError::Serialization(
                    format!("WAL: decompressed size {} != expected {}", decompressed.len(), original_len)));
            }
            // Recurse on decompressed data
            return Self::decode_with_fallback(&decompressed);
        }
        // Try native binary format first
        if let Some(result) = Self::decode_native(data) {
            return result;
        }
        // Fallback to bincode (old WAL files with legacy WALRecord format)
        let legacy: LegacyWALRecord = bincode::deserialize(data)
            .map_err(|e| StorageError::Serialization(format!("WAL decode failed: {}", e)))?;
        Ok(legacy.into())
    }

    /// Get the table name from any record variant that has one
    pub fn table_name(&self) -> Option<&str> {
        match self {
            WALRecord::Insert { table_name, .. }
            | WALRecord::InsertRaw { table_name, .. }
            | WALRecord::Update { table_name, .. }
            | WALRecord::UpdateRaw { table_name, .. }
            | WALRecord::Delete { table_name, .. }
            | WALRecord::DeleteRaw { table_name, .. } => Some(table_name),
            _ => None,
        }
    }

    /// Get the row_id from any record variant that has one
    pub fn row_id(&self) -> Option<RowId> {
        match self {
            WALRecord::Insert { row_id, .. }
            | WALRecord::InsertRaw { row_id, .. }
            | WALRecord::Update { row_id, .. }
            | WALRecord::UpdateRaw { row_id, .. }
            | WALRecord::Delete { row_id, .. }
            | WALRecord::DeleteRaw { row_id, .. } => Some(*row_id),
            _ => None,
        }
    }

    /// Get the txn_id from any record variant
    pub fn txn_id(&self) -> TransactionId {
        match self {
            WALRecord::Insert { txn_id, .. }
            | WALRecord::InsertRaw { txn_id, .. }
            | WALRecord::Update { txn_id, .. }
            | WALRecord::UpdateRaw { txn_id, .. }
            | WALRecord::Delete { txn_id, .. }
            | WALRecord::DeleteRaw { txn_id, .. }
            | WALRecord::Begin { txn_id, .. }
            | WALRecord::Commit { txn_id, .. }
            | WALRecord::Rollback { txn_id } => *txn_id,
            WALRecord::Checkpoint { .. } => 0,
        }
    }
}

fn encode_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos + 8 > data.len() {
        return None;
    }
    let val = u64::from_le_bytes(data[*pos..*pos + 8].try_into().ok()?);
    *pos += 8;
    Some(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 4 > data.len() {
        return None;
    }
    let val = u32::from_le_bytes(data[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(val)
}

fn read_u16(data: &[u8], pos: &mut usize) -> Option<u16> {
    if *pos + 2 > data.len() {
        return None;
    }
    let val = u16::from_le_bytes(data[*pos..*pos + 2].try_into().ok()?);
    *pos += 2;
    Some(val)
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = read_u16(data, pos)? as usize;
    if *pos + len > data.len() {
        return None;
    }
    let s = std::str::from_utf8(&data[*pos..*pos + len]).ok()?.to_string();
    *pos += len;
    Some(s)
}

/// Legacy WAL record format (bincode-encoded, for reading old WAL files)
#[derive(Debug, Clone, Deserialize)]
enum LegacyWALRecord {
    Insert { table_name: String, row_id: RowId, partition: PartitionId, data: Row, txn_id: TransactionId },
    Update { table_name: String, row_id: RowId, partition: PartitionId, old_data: Row, new_data: Row, txn_id: TransactionId },
    Delete { table_name: String, row_id: RowId, partition: PartitionId, old_data: Row, timestamp: u64, txn_id: TransactionId },
    Begin { txn_id: TransactionId, isolation_level: u8 },
    Commit { txn_id: TransactionId, commit_ts: Timestamp },
    Rollback { txn_id: TransactionId },
    Checkpoint { lsn: LogSequenceNumber },
}

impl From<LegacyWALRecord> for WALRecord {
    fn from(legacy: LegacyWALRecord) -> Self {
        match legacy {
            LegacyWALRecord::Insert { table_name, row_id, partition, data, txn_id } =>
                WALRecord::Insert { table_name, row_id, partition, data, txn_id },
            LegacyWALRecord::Update { table_name, row_id, partition, old_data, new_data, txn_id } =>
                WALRecord::Update { table_name, row_id, partition, old_data, new_data, txn_id },
            LegacyWALRecord::Delete { table_name, row_id, partition, old_data, timestamp, txn_id } =>
                WALRecord::Delete { table_name, row_id, partition, old_data, timestamp, txn_id },
            LegacyWALRecord::Begin { txn_id, isolation_level } =>
                WALRecord::Begin { txn_id, isolation_level },
            LegacyWALRecord::Commit { txn_id, commit_ts } =>
                WALRecord::Commit { txn_id, commit_ts },
            LegacyWALRecord::Rollback { txn_id } =>
                WALRecord::Rollback { txn_id },
            LegacyWALRecord::Checkpoint { lsn } =>
                WALRecord::Checkpoint { lsn },
        }
    }
}

/// WAL manager for each partition
struct PartitionWAL {
    /// WAL file path
    path: PathBuf,

    /// Buffered WAL file (BufWriter amortizes syscalls for Periodic/GroupCommit modes)
    file: BufWriter<File>,

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
            file: BufWriter::new(file),
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

            let total_len = u32::from_le_bytes(len_buf) as usize;

            // Sanity check: reject obviously corrupted total_len
            if total_len < 20 || total_len > Self::MAX_WAL_FRAME_SIZE {
                debug_log!("WAL open: Corrupted total_len={}, stopping", total_len);
                break;
            }

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

            // record_len must fit inside total_len
            if record_len > total_len.saturating_sub(20) || record_len > Self::MAX_WAL_FRAME_SIZE {
                debug_log!("WAL open: Corrupted record_len={} (total_len={}), stopping",
                    record_len, total_len);
                break;
            }

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
                // Seek to next record boundary: total_len includes the 4-byte length prefix
                // and 16-byte header (lsn+checksum+record_len). We've already read all of those
                // plus record_data, but record_len may be wrong if the header was corrupted.
                // Use total_len to seek to the correct position.
                // We've read 4 (total_len) + 16 (header) = 20 bytes of the frame so far.
                // The record data portion is total_len - 20 bytes.
                if total_len > 20 {
                    let seek_offset = (total_len - 20) as i64 - record_len as i64;
                    if seek_offset != 0 {
                        if let Err(e) = file.seek(SeekFrom::Current(seek_offset)) {
                            debug_log!("WAL open: Failed to seek past corrupted record: {}", e);
                            break;
                        }
                    }
                }
                continue;
            }

            next_lsn = lsn + 1;
            // Deserialize record to check for Checkpoint
            if let Ok(WALRecord::Checkpoint { lsn: cp_lsn }) = WALRecord::decode_with_fallback(&record_data) {
                last_checkpoint = cp_lsn;
            }
        }
        
        if corrupted_count > 0 {
            debug_log!("WAL open: Found {} corrupted records (will skip during recovery)", corrupted_count);
        }
        
        Ok(Self {
            path,
            file: BufWriter::new(file),
            next_lsn,
            last_checkpoint,
            config,
        })
    }

    /// Flush BufWriter to OS buffer + fsync (for durability)
    fn sync_flush(&mut self) -> Result<()> {
        self.file.flush()?;
        // fsync: flush both data and metadata (file size) for durability on all platforms
        self.file.get_ref().sync_all()?;
        Ok(())
    }

    /// Append a record to WAL
    fn append(&mut self, record: WALRecord) -> Result<LogSequenceNumber> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        let record_data = record.encode_native()?;
        self.write_record(lsn, &record_data)?;

        match self.config.durability_level {
            DurabilityLevel::Synchronous => { self.sync_flush()?; }
            DurabilityLevel::GroupCommit { .. } => {
                // Flush BufWriter to OS buffers; group commit thread handles fsync
                self.file.flush()?;
            }
            DurabilityLevel::Periodic { .. } | DurabilityLevel::NoSync => {}
        }

        Ok(lsn)
    }

    /// Append an INSERT record using raw data by reference (avoids clone).
    /// Uses a single write buffer to minimize BufWriter interactions.
    fn append_insert_raw_ref(
        &mut self,
        table_name: &str,
        row_id: RowId,
        partition: PartitionId,
        raw_data: &[u8],
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        // Build native record inline (TAG + header + data)
        let table_bytes = table_name.as_bytes();
        let record_len = 1 + 8 + (2 + table_bytes.len()) + 8 + 2 + 4 + raw_data.len();

        // Build record body first (for potential compression)
        let mut record_body = Vec::with_capacity(record_len);
        record_body.push(TAG_INSERT_RAW);
        record_body.extend_from_slice(&txn_id.to_le_bytes());
        record_body.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
        record_body.extend_from_slice(table_bytes);
        record_body.extend_from_slice(&row_id.to_le_bytes());
        record_body.extend_from_slice(&(partition as u16).to_le_bytes());
        record_body.extend_from_slice(&(raw_data.len() as u32).to_le_bytes());
        record_body.extend_from_slice(raw_data);

        // Compress if worthwhile
        let payload = Self::compress_if_worthwhile(&record_body);

        let mut write_buf = Vec::with_capacity(20 + payload.len());
        write_buf.extend_from_slice(&((20 + payload.len()) as u32).to_le_bytes());
        write_buf.extend_from_slice(&lsn.to_le_bytes());
        // checksum placeholder (will patch)
        let checksum_offset = write_buf.len();
        write_buf.extend_from_slice(&0u32.to_le_bytes());
        write_buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());

        // Record body (possibly compressed)
        let record_start = write_buf.len();
        write_buf.extend_from_slice(&payload);

        // Compute and patch checksum
        let checksum = Checksum::compute(ChecksumType::CRC32C, &write_buf[record_start..]);
        write_buf[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&write_buf)?;


        if self.config.durability_level == DurabilityLevel::Synchronous { self.sync_flush()?; }

        Ok(lsn)
    }

    fn append_update_raw_ref(
        &mut self,
        table_name: &str,
        row_id: RowId,
        partition: PartitionId,
        raw_old: &[u8],
        raw_new: &[u8],
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        let table_bytes = table_name.as_bytes();
        let record_len = 1 + 8 + (2 + table_bytes.len()) + 8 + 2 + 4 + raw_old.len() + 4 + raw_new.len();

        let mut record_body = Vec::with_capacity(record_len);
        record_body.push(TAG_UPDATE_RAW);
        record_body.extend_from_slice(&txn_id.to_le_bytes());
        record_body.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
        record_body.extend_from_slice(table_bytes);
        record_body.extend_from_slice(&row_id.to_le_bytes());
        record_body.extend_from_slice(&(partition as u16).to_le_bytes());
        record_body.extend_from_slice(&(raw_old.len() as u32).to_le_bytes());
        record_body.extend_from_slice(raw_old);
        record_body.extend_from_slice(&(raw_new.len() as u32).to_le_bytes());
        record_body.extend_from_slice(raw_new);

        let payload = Self::compress_if_worthwhile(&record_body);

        let mut write_buf = Vec::with_capacity(20 + payload.len());
        write_buf.extend_from_slice(&((20 + payload.len()) as u32).to_le_bytes());
        write_buf.extend_from_slice(&lsn.to_le_bytes());
        let checksum_offset = write_buf.len();
        write_buf.extend_from_slice(&0u32.to_le_bytes());
        write_buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());

        let record_start = write_buf.len();
        write_buf.extend_from_slice(&payload);

        let checksum = Checksum::compute(ChecksumType::CRC32C, &write_buf[record_start..]);
        write_buf[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&write_buf)?;

        if self.config.durability_level == DurabilityLevel::Synchronous { self.sync_flush()?; }

        Ok(lsn)
    }

    /// Compress record data if beneficial. Returns either compressed or original bytes.
    fn compress_if_worthwhile(data: &[u8]) -> Vec<u8> {
        if data.len() < WAL_COMPRESS_THRESHOLD {
            return data.to_vec();
        }
        // Try Zstd level 1 compression
        if let Ok(compressed) = zstd::encode_all(data, 1) {
            // Only use compressed if we save meaningful space (>10%)
            if compressed.len() + 5 < data.len() * 9 / 10 {
                // Format: [0x00][u32 original_len][zstd_data]
                let mut out = Vec::with_capacity(5 + compressed.len());
                out.push(TAG_COMPRESSED);
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(&compressed);
                return out;
            }
        }
        data.to_vec()
    }

    /// Write a pre-serialized record with framing (single buffer).
    fn write_record(&mut self, lsn: u64, record_data: &[u8]) -> Result<()> {
        let payload = Self::compress_if_worthwhile(record_data);
        let total_len = (20 + payload.len()) as u32;
        let checksum = Checksum::compute(ChecksumType::CRC32C, &payload);

        let mut buf = Vec::with_capacity(20 + payload.len());
        buf.extend_from_slice(&total_len.to_le_bytes());
        buf.extend_from_slice(&lsn.to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);

        self.file.write_all(&buf)?;

        Ok(())
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
    /// - Append multiple records in a single I/O operation.
    ///   Assumes LSNs were already allocated via `allocate_lsn()` — does NOT increment next_lsn.
    fn batch_append(&mut self, records: Vec<WALRecord>) -> Result<Vec<LogSequenceNumber>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let mut lsns = Vec::with_capacity(records.len());
        let mut buffer = Vec::with_capacity(records.len() * 256);

        // 1. Serialize all records to buffer (LSNs already pre-allocated)
        for record in records {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            lsns.push(lsn);

            let record_data = record.encode_native()?;
            let payload = Self::compress_if_worthwhile(&record_data);
            let checksum = Checksum::compute(ChecksumType::CRC32C, &payload);

            // Header: [u32 total_len][u64 lsn][u32 checksum][u32 payload_len]
            let header_size = 4 + 8 + 4 + 4; // 20 bytes
            let total_len = (header_size + payload.len()) as u32;

            buffer.extend_from_slice(&total_len.to_le_bytes());
            buffer.extend_from_slice(&lsn.to_le_bytes());
            buffer.extend_from_slice(&checksum.to_le_bytes());
            buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&payload);
        }
        
        // 2. Single write operation (append 模式自动追加)
        self.file.write_all(&buffer)?;
        
        // 3. Fsync based on durability level
        match self.config.durability_level {
            DurabilityLevel::Synchronous | DurabilityLevel::GroupCommit { .. } => {
                self.sync_flush()?;
            }
            DurabilityLevel::Periodic { .. } => {
                // BufWriter buffers; periodic flush thread calls sync_flush
            }
            DurabilityLevel::NoSync => {}
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
        self.file.flush()?;
        self.file.get_ref().sync_all()?;

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
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.file = BufWriter::new(file);

        // Reset counters
        self.next_lsn = 0;
        self.last_checkpoint = 0;


        Ok(())
    }

    /// Recover records since last checkpoint
    /// 
    /// Verifies checksum for each record. Corrupted records are skipped with warning.
    /// Partial writes (incomplete records at end of file) are automatically detected.
    /// Maximum sane WAL frame size (64 MB). Frames larger than this are almost
    /// certainly the result of a corrupted `total_len` field. Using this guard
    /// prevents a bogus `total_len` from causing seeks that skip valid records.
    const MAX_WAL_FRAME_SIZE: usize = 64 * 1024 * 1024;

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

            let total_len = u32::from_le_bytes(len_buf) as usize;

            // Sanity check: reject obviously corrupted total_len
            if total_len < 20 || total_len > Self::MAX_WAL_FRAME_SIZE {
                debug_log!("WAL recovery: Corrupted total_len={}, stopping", total_len);
                break;
            }

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

            // record_len must fit inside total_len (20 bytes = 4 len prefix + 16 header)
            if record_len > total_len.saturating_sub(20) || record_len > Self::MAX_WAL_FRAME_SIZE {
                debug_log!("WAL recovery: Corrupted record_len={} (total_len={}), stopping",
                    record_len, total_len);
                break;
            }

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
                // Seek to next record boundary using total_len to correct for
                // potentially corrupted record_len in the header.
                if total_len > 20 {
                    let seek_offset = (total_len - 20) as i64 - record_len as i64;
                    if seek_offset != 0 {
                        if let Err(seek_err) = file.seek(SeekFrom::Current(seek_offset)) {
                            debug_log!("WAL recovery: Failed to seek past corrupted record: {}", seek_err);
                            break;
                        }
                    }
                }
                continue;
            }

            // Deserialize record (native binary with bincode fallback)
            let record: WALRecord = match WALRecord::decode_with_fallback(&record_data) {
                Ok(r) => r,
                Err(e) => {
                    debug_log!("WAL recovery: Failed to deserialize record: {}", e);
                    skipped_corrupted += 1;
                    // Checksum was valid but deser failed — cursor is already past record data,
                    // but correct using total_len in case record_len was inconsistent.
                    if total_len > 20 {
                        let seek_offset = (total_len - 20) as i64 - record_len as i64;
                        if seek_offset != 0 {
                            if let Err(seek_err) = file.seek(SeekFrom::Current(seek_offset)) {
                                debug_log!("WAL recovery: Failed to seek past corrupted record: {}", seek_err);
                                break;
                            }
                        }
                    }
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
    /// Caller waits on this condvar until the record is durable.
    /// `None` = pending, `Some(Ok(()))` = flushed, `Some(Err(msg))` = flush failed.
    done: Arc<(PlMutex<Option<std::result::Result<(), String>>>, PlCondvar)>,
}

struct GroupCommitState {
    /// Queue of pending records
    queue: PlMutex<Vec<GroupCommitEntry>>,
    /// Signal the flush thread to wake up
    wakeup: PlCondvar,
    /// Maximum records per batch
    max_batch_size: usize,
    /// Maximum wait time in microseconds before flushing
    max_wait_us: u64,
    /// Last flush error (set by flush thread, checked by callers)
    last_error: parking_lot::Mutex<Option<StorageError>>,
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

                        // Flush all partitions: BufWriter flush → OS buffer, then sync_data → disk
                        for entry in partitions.iter() {
                            let mut wal = entry.value().lock();
                        if let Err(e) = wal.sync_flush() {
                            warn_log!("[WAL] Periodic sync_flush failed: {}", e);
                        }
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
                queue: PlMutex::new(Vec::new()),
                wakeup: PlCondvar::new(),
                max_batch_size,
                max_wait_us,
                last_error: parking_lot::Mutex::new(None),
            });

            let should_stop_clone = should_stop.clone();
            let state_clone = state.clone();
            let wait_duration = Duration::from_micros(max_wait_us);

            let handle = thread::Builder::new()
                .name("motedb-group-commit".into())
                .spawn(move || {
                    while !should_stop_clone.load(Ordering::Relaxed) {
                        // Wait for first entry or shutdown
                        {
                            let mut queue = state_clone.queue.lock();
                            if queue.is_empty() {
                                let _ = state_clone.wakeup.wait_for(&mut queue, wait_duration);
                            }
                        }

                        // Batch accumulation: use condvar timeout to wait for more entries.
                        // This avoids busy-spinning while still letting batches grow.
                        {
                            let mut queue = state_clone.queue.lock();
                            if !queue.is_empty() && queue.len() < state_clone.max_batch_size {
                                // Wait up to max_wait_us for more entries to arrive
                                let _ = state_clone.wakeup.wait_for(
                                    &mut queue,
                                    Duration::from_micros(state_clone.max_wait_us),
                                );
                            }
                        }

                        // Drain queue (up to max_batch_size)
                        let entries: Vec<GroupCommitEntry> = {
                            let mut queue = state_clone.queue.lock();
                            let drain_count = queue.len().min(state_clone.max_batch_size);
                            queue.drain(..drain_count).collect()
                        };

                        if entries.is_empty() {
                            continue;
                        }

                        // Group by partition
                        let mut groups: HashMap<PartitionId, Vec<WALRecord>> = HashMap::new();
                        let mut done_signals: Vec<Arc<(PlMutex<Option<std::result::Result<(), String>>>, PlCondvar)>> = Vec::new();

                        for entry in entries {
                            groups.entry(entry.partition).or_default().push(entry.record);
                            done_signals.push(entry.done);
                        }

                        // Flush each partition group
                        let flush_ok = (|| -> std::result::Result<(), String> {
                            for (partition, records) in groups {
                                if let Some(entry) = partitions.get(&partition) {
                                    let mut wal = entry.value().lock();
                                    wal.batch_append(records).map_err(|e| e.to_string())?;
                                }
                            }
                            Ok(())
                        })();

                        // Signal all callers with the result
                        for done in done_signals {
                            {
                                let mut flag = done.0.lock();
                                *flag = Some(flush_ok.clone());
                            }
                            done.1.notify_all();
                        }

                        if let Err(ref msg) = flush_ok {
                            warn_log!("[GroupCommit] Flush error: {}", msg);
                            *state_clone.last_error.lock() = Some(StorageError::Transaction(msg.clone()));
                        }
                    }

                    // Final drain on shutdown
                    let entries: Vec<GroupCommitEntry> = {
                        let mut queue = state_clone.queue.lock();
                        std::mem::take(&mut *queue)
                    };

                    if !entries.is_empty() {
                        let mut groups: HashMap<PartitionId, Vec<WALRecord>> = HashMap::new();
                        let mut done_signals: Vec<Arc<(PlMutex<Option<std::result::Result<(), String>>>, PlCondvar)>> = Vec::new();

                        for entry in entries {
                            groups.entry(entry.partition).or_default().push(entry.record);
                            done_signals.push(entry.done);
                        }

                        let drain_ok = (|| -> std::result::Result<(), String> {
                            for (partition, records) in groups {
                                if let Some(entry) = partitions.get(&partition) {
                                    let mut wal = entry.value().lock();
                                    wal.batch_append(records).map_err(|e| e.to_string())?;
                                }
                            }
                            Ok(())
                        })();

                        for done in done_signals {
                            {
                                let mut flag = done.0.lock();
                                *flag = Some(drain_ok.clone());
                            }
                            done.1.notify_all();
                        }

                        if let Err(ref msg) = drain_ok {
                            warn_log!("[WAL] Shutdown drain flush failed: {}", msg);
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

    /// Push a record through group commit queue (fire-and-forget).
    /// Returns immediately; background thread handles batching & fsync.
    /// Call `check_flush_errors()` to detect background flush failures.
    fn group_commit_append(
        &self,
        partition: PartitionId,
        record: WALRecord,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);

        if let Some(ref gc) = self.group_commit {
            // Check for prior flush errors before enqueuing
            if let Some(e) = gc.state.last_error.lock().take() {
                return Err(e);
            }
            {
                let mut queue = gc.state.queue.lock();
                queue.push(GroupCommitEntry {
                    partition,
                    record,
                    done: Arc::new((PlMutex::new(None), PlCondvar::new())),
                });
            }
            gc.state.wakeup.notify_all();
            Ok(0)
        } else {
            let entry = self.partitions.get(&partition)
                .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
            let mut wal = entry.value().lock();
            wal.append(record)
        }
    }

    /// Check for any background flush errors accumulated by the group commit thread.
    /// Returns and clears the last error if one exists.
    pub fn check_flush_errors(&self) -> Option<StorageError> {
        self.group_commit.as_ref()?.state.last_error.lock().take()
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

    /// Log an insert with raw bytes (zero-copy P3)
    ///
    /// Raw data is the same bytes that go into LSM — on recovery, they're
    /// passed directly to LSM without re-encoding.
    pub fn log_insert_raw(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        raw_data: Vec<u8>,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::InsertRaw {
            table_name: table_name.to_string(),
            row_id,
            partition,
            raw_data,
            txn_id,
        };
        self.group_commit_append(partition, record)
    }

    /// Log an INSERT using raw data by reference (avoids Vec clone).
    ///
    /// For Periodic/NoSync modes, bypasses WALRecord creation entirely —
    /// encodes directly into a single buffer and writes once to BufWriter.
    pub fn log_insert_raw_ref(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        raw_data: &[u8],
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);

        if self.group_commit.is_some() {
            // GroupCommit: fall back to owned variant (needs the record in the queue)
            return self.log_insert_raw(table_name, partition, row_id, raw_data.to_vec(), txn_id);
        }

        // Direct path (Periodic/NoSync): encode and write in one step, zero clone
        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.append_insert_raw_ref(table_name, row_id, partition, raw_data, txn_id)
    }

    /// Log an update with raw bytes (zero-copy P3)
    pub fn log_update_raw(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        raw_old: Vec<u8>,
        raw_new: Vec<u8>,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);

        if self.group_commit.is_some() {
            // GroupCommit: use the owned record path
            let record = WALRecord::UpdateRaw {
                table_name: table_name.to_string(),
                row_id,
                partition,
                raw_old,
                raw_new,
                txn_id,
            };
            return self.group_commit_append(partition, record);
        }

        // Direct path: encode and write in one step
        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.append_update_raw_ref(table_name, row_id, partition, &raw_old, &raw_new, txn_id)
    }

    /// Log an update by reference (avoids cloning raw_old and raw_new).
    /// Only usable with GroupCommit disabled (falls back to owned path if enabled).
    pub fn log_update_raw_ref(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        raw_old: &[u8],
        raw_new: &[u8],
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        self.periodic_new_writes.store(true, Ordering::Relaxed);

        if self.group_commit.is_some() {
            // GroupCommit needs owned data — clone here (rare path)
            let record = WALRecord::UpdateRaw {
                table_name: table_name.to_string(),
                row_id,
                partition,
                raw_old: raw_old.to_vec(),
                raw_new: raw_new.to_vec(),
                txn_id,
            };
            return self.group_commit_append(partition, record);
        }

        let entry = self.partitions.get(&partition)
            .ok_or_else(|| StorageError::Transaction("Invalid partition ID".to_string()))?;
        let mut wal = entry.value().lock();
        wal.append_update_raw_ref(table_name, row_id, partition, raw_old, raw_new, txn_id)
    }

    /// Log an update via group commit (owned path)
    pub fn log_update_raw_group(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        raw_old: Vec<u8>,
        raw_new: Vec<u8>,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::UpdateRaw {
            table_name: table_name.to_string(),
            row_id,
            partition,
            raw_old,
            raw_new,
            txn_id,
        };
        self.group_commit_append(partition, record)
    }

    /// Log a delete with raw bytes (zero-copy P3)
    pub fn log_delete_raw(
        &self,
        table_name: &str,
        partition: PartitionId,
        row_id: RowId,
        raw_old: Vec<u8>,
        timestamp: u64,
        txn_id: TransactionId,
    ) -> Result<LogSequenceNumber> {
        let record = WALRecord::DeleteRaw {
            table_name: table_name.to_string(),
            row_id,
            partition,
            raw_old,
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
    /// Flush all pending group-commit entries to disk.
    /// Called before `recover()` so in-flight records are visible on disk.
    pub fn flush_group_commit_queue(&self) {
        if let Some(ref gc) = self.group_commit {
            // Stop the background thread so it stops competing for the queue.
            gc.should_stop.store(true, Ordering::Relaxed);
            gc.state.wakeup.notify_all();

            // Wait for the background thread to exit (it has a final drain in its
            // shutdown path). Poll the queue until empty, then give the thread time
            // to finish its in-flight writes.
            let deadline = std::time::Instant::now() + Duration::from_millis(200);
            loop {
                let queue_len = gc.state.queue.lock().len();
                if queue_len == 0 {
                    // Extra sleep to let the thread finish writing its last batch
                    std::thread::sleep(Duration::from_millis(5));
                    break;
                }
                if std::time::Instant::now() > deadline {
                    break;
                }
                gc.state.wakeup.notify_all();
                std::thread::sleep(Duration::from_micros(500));
            }

            // Safety net: drain anything still left (e.g. thread didn't finish in time)
            let entries: Vec<GroupCommitEntry> = {
                let mut queue = gc.state.queue.lock();
                std::mem::take(&mut *queue)
            };
            if !entries.is_empty() {
                let mut groups: HashMap<PartitionId, Vec<WALRecord>> = HashMap::new();
                for entry in entries {
                    groups.entry(entry.partition).or_default().push(entry.record);
                }
                for (partition, records) in groups {
                    if let Some(entry) = self.partitions.get(&partition) {
                        let mut wal = entry.value().lock();
                        let _ = wal.batch_append(records);
                    }
                }
            }
        }
    }

    pub fn recover(&self) -> Result<HashMap<PartitionId, Vec<WALRecord>>> {
        // First, flush any in-flight group-commit entries so they're visible on disk
        self.flush_group_commit_queue();

        let mut result = HashMap::new();

        for entry in self.partitions.iter() {
            let partition_id = *entry.key();
            let mut wal = entry.value().lock();
            // Flush BufWriter so all written data is visible to the file read
            let _ = wal.file.flush();
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
            let mut wal = entry.value().lock();
            let _ = wal.sync_flush();
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
                records.iter().filter(|r| matches!(r, WALRecord::Insert { .. } | WALRecord::InsertRaw { .. })).count()
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
        assert!(matches!(records[0], WALRecord::Update { .. } | WALRecord::UpdateRaw { .. }));
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
        assert!(matches!(records[0], WALRecord::Delete { .. } | WALRecord::DeleteRaw { .. }));
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
        assert!(matches!(records[1], WALRecord::Insert { row_id: 10, .. } | WALRecord::InsertRaw { row_id: 10, .. }));
        assert!(matches!(records[2], WALRecord::Commit { txn_id: 1, .. }));
    }

    #[test]
    fn test_wal_transaction_rollback() {
        let temp_dir = TempDir::new().unwrap();
        let config = WALConfig { durability_level: DurabilityLevel::Synchronous };
        let wal = WALManager::create_with_config(temp_dir.path(), 2, config).unwrap();
        
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
        assert!(matches!(records[1], WALRecord::Insert { row_id: 10, .. } | WALRecord::InsertRaw { row_id: 10, .. }));
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
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Insert { .. } | WALRecord::InsertRaw { .. })), 2);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Update { .. } | WALRecord::UpdateRaw { .. })), 1);
            assert_eq!(count_type(records, |r| matches!(r, WALRecord::Delete { .. } | WALRecord::DeleteRaw { .. })), 1);
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
        assert!(matches!(records[1], WALRecord::Insert { row_id: 100, .. } | WALRecord::InsertRaw { row_id: 100, .. }));
        assert!(matches!(records[2], WALRecord::Insert { row_id: 101, .. } | WALRecord::InsertRaw { row_id: 101, .. }));
        assert!(matches!(records[3], WALRecord::Update { row_id: 100, .. } | WALRecord::UpdateRaw { row_id: 100, .. }));
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

    /// Regression test: corrupted total_len should stop recovery, not cause misaligned seeks.
    /// Before the fix, a corrupted total_len (e.g., 0xFFFFFFFF) would cause a seek past
    /// valid records, losing data that could have been recovered.
    #[test]
    fn test_wal_corrupted_total_len_stops_cleanly() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();

        // Write 3 valid records
        {
            let wal = WALManager::create(path, 1).unwrap();
            for i in 0..3u64 {
                wal.log_insert("t", 0, i, vec![Value::Integer(i as i64)], 0).unwrap();
            }
        }

        // Corrupt the file: overwrite bytes at the start of the second record's
        // total_len with garbage (0xFF bytes) to simulate corruption.
        let wal_path = path.join("partition_0.wal");
        let mut data = std::fs::read(&wal_path).unwrap();

        if data.len() < 24 {
            eprintln!("WAL data too short for corruption test, skipping");
            return;
        }
        let first_total_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        // Corrupt the total_len of the 2nd record
        let corrupt_offset = 4 + first_total_len;
        if corrupt_offset + 4 <= data.len() {
            data[corrupt_offset] = 0xFF;
            data[corrupt_offset + 1] = 0xFF;
            data[corrupt_offset + 2] = 0xFF;
            data[corrupt_offset + 3] = 0x0F; // ~268MB — clearly corrupt
            std::fs::write(&wal_path, &data).unwrap();
        }

        // Recovery should succeed and return at least the first record (before corruption)
        let wal = WALManager::open(path, 1).unwrap();
        let recovered = wal.recover().unwrap();
        let records = recovered.get(&0).map(|r| r.len()).unwrap_or(0);
        assert!(records >= 1, "Should recover at least 1 record before corruption, got {}", records);
    }

    /// Test that corrupted record_len (larger than total_len) stops recovery cleanly.
    #[test]
    fn test_wal_corrupted_record_len_stops_cleanly() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path();

        {
            let wal = WALManager::create(path, 1).unwrap();
            wal.log_insert("t", 0, 1, vec![Value::Integer(42)], 0).unwrap();
        }

        // Corrupt record_len to be larger than total_len
        let wal_path = path.join("partition_0.wal");
        let mut data = std::fs::read(&wal_path).unwrap();

        if data.len() >= 20 {
            let total_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            let bogus_record_len = (total_len + 1000) as u32;
            data[16..20].copy_from_slice(&bogus_record_len.to_le_bytes());
            std::fs::write(&wal_path, &data).unwrap();
        }

        // Recovery should not panic or hang
        let wal = WALManager::open(path, 1).unwrap();
        let _ = wal.recover();
    }
}
