//! Timestamp Index Operations
//!
//! Extracted from database_legacy.rs
//! Provides timestamp range query and index rebuild functionality

use crate::types::{Row, RowId};
use crate::{Result, StorageError};

use crate::database::core::MoteDB;

/// Query performance profile
#[derive(Debug, Clone)]
pub struct QueryProfile {
    pub index_time_us: u64,
    pub memtable_time_us: u64,
    pub memtable_load_us: u64,
    pub memtable_scan_us: u64,
    pub memtable_size: usize,
    pub merge_time_us: u64,
    pub total_time_us: u64,
    pub index_results: usize,
    pub memtable_results: usize,
    pub total_results: usize,
}

/// MemTable scan profile
#[derive(Debug, Clone)]
pub struct MemTableScanProfile {
    pub load_time_us: u64,
    pub scan_time_us: u64,
    pub total_time_us: u64,
    pub memtable_size: usize,
    pub matched_results: usize,
}

impl MoteDB {
    /// Rebuild timestamp index from LSM storage
    /// 
    /// Scans all rows and rebuilds the timestamp index incrementally
    pub fn rebuild_timestamp_index(&self) -> Result<()> {
        // Get max indexed timestamp to avoid reprocessing
        let max_indexed_ts = self.timestamp_index.read().max_key()?.unwrap_or(0);
        
        // Scan LSM for all entries
        let mut entries_to_index = Vec::new();
        
        let table_name = "_default";
        let composite_key_base = self.make_composite_key(table_name, 0);
        let table_hash = composite_key_base >> 32;
        
        let max_row_id = *self.next_row_id.read();
        for row_id in 0..max_row_id {
            let composite_key = (table_hash << 32) | row_id;
            if let Some(value) = self.lsm_engine.get(composite_key)? {
                let data_bytes = match &value.data {
                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                    crate::storage::lsm::ValueData::Blob(_) => continue,
                };
                
                if let Ok(row) = bincode::deserialize::<Row>(data_bytes) {
                    if let Some(crate::types::Value::Timestamp(ts)) = row.first() {
                        let ts_micros = ts.as_micros() as u64;
                        
                        if ts_micros > max_indexed_ts {
                            entries_to_index.push((ts_micros, row_id));
                        }
                    }
                }
            }
        }
        
        // Batch insert into index
        if !entries_to_index.is_empty() {
            let mut ts_index = self.timestamp_index.write();
            for (timestamp, row_id) in entries_to_index {
                ts_index.insert(timestamp, row_id)?;
            }
        }
        
        Ok(())
    }
    
    /// Query by timestamp range
    /// 
    /// # Example
    /// ```ignore
    /// let row_ids = db.query_timestamp_range(1000, 2000)?;
    /// ```
    pub fn query_timestamp_range(&self, start: i64, end: i64) -> Result<Vec<RowId>> {
        self.query_timestamp_range_with_profile(start, end).map(|(ids, _)| ids)
    }
    
    /// Query by timestamp range with performance profiling
    pub fn query_timestamp_range_with_profile(&self, start: i64, end: i64) -> Result<(Vec<RowId>, QueryProfile)> {
        let total_start = std::time::Instant::now();
        
        // 1. Query from persisted index (flushed data)
        let index_start = std::time::Instant::now();
        let start_u64 = start as u64;
        let end_u64 = end as u64;
        let index_results = self.timestamp_index.read().range(&start_u64, &end_u64)?;
        let mut result_ids: Vec<RowId> = index_results.into_iter().map(|(_, row_id)| row_id).collect();
        let index_duration = index_start.elapsed();
        
        // 2. Query from LSM MemTable (unflushed data)
        let memtable_start = std::time::Instant::now();
        let (lsm_row_ids, memtable_profile) = self.scan_memtable_by_timestamp_with_profile(start, end)?;
        let memtable_duration = memtable_start.elapsed();
        
        // 3. Merge and deduplicate
        let merge_start = std::time::Instant::now();
        
        let index_count = result_ids.len();
        let memtable_count = lsm_row_ids.len();
        
        result_ids.extend(lsm_row_ids);
        result_ids.sort_unstable();
        result_ids.dedup();
        
        let merge_duration = merge_start.elapsed();
        let total_duration = total_start.elapsed();
        
        let profile = QueryProfile {
            index_time_us: index_duration.as_micros() as u64,
            memtable_time_us: memtable_duration.as_micros() as u64,
            memtable_load_us: memtable_profile.load_time_us,
            memtable_scan_us: memtable_profile.scan_time_us,
            memtable_size: memtable_profile.memtable_size,
            merge_time_us: merge_duration.as_micros() as u64,
            total_time_us: total_duration.as_micros() as u64,
            index_results: index_count,
            memtable_results: memtable_count,
            total_results: result_ids.len(),
        };
        
        Ok((result_ids, profile))
    }
    
    /// Scan LSM MemTable for timestamp range
    fn scan_memtable_by_timestamp(&self, start: i64, end: i64) -> Result<Vec<RowId>> {
        self.scan_memtable_by_timestamp_with_profile(start, end).map(|(ids, _)| ids)
    }
    
    /// Scan LSM MemTable with profiling
    fn scan_memtable_by_timestamp_with_profile(&self, start: i64, end: i64) -> Result<(Vec<RowId>, MemTableScanProfile)> {
        let total_start = std::time::Instant::now();
        
        // 1. Load incremental MemTable data
        let load_start = std::time::Instant::now();
        let memtable_entries = self.lsm_engine.scan_memtable_incremental()?;
        let load_duration = load_start.elapsed();
        let memtable_size = memtable_entries.len();
        
        // 2. Scan and filter
        let scan_start = std::time::Instant::now();
        let mut row_ids = Vec::with_capacity(memtable_entries.len() / 10);
        
        for (composite_key, value_bytes) in memtable_entries {
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            
            // Zero-copy optimization: extract timestamp directly from bincode bytes
            if value_bytes.len() >= 20 {
                let ts_bytes: [u8; 8] = [
                    value_bytes[12], value_bytes[13], value_bytes[14], value_bytes[15],
                    value_bytes[16], value_bytes[17], value_bytes[18], value_bytes[19],
                ];
                let timestamp = i64::from_le_bytes(ts_bytes);
                
                if timestamp >= start && timestamp <= end {
                    row_ids.push(row_id);
                }
            }
        }
        let scan_duration = scan_start.elapsed();
        
        let total_duration = total_start.elapsed();
        
        let profile = MemTableScanProfile {
            load_time_us: load_duration.as_micros() as u64,
            scan_time_us: scan_duration.as_micros() as u64,
            total_time_us: total_duration.as_micros() as u64,
            memtable_size,
            matched_results: row_ids.len(),
        };
        
        Ok((row_ids, profile))
    }
    
    /// Get timestamp index statistics
    pub fn timestamp_index_stats(&self) -> crate::index::btree::BTreeStats {
        self.timestamp_index.read().stats()
    }
}
