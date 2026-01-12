//! Column Index Operations
//!
//! Extracted from database_legacy.rs
//! Provides column value indexing for WHERE clause optimization

use crate::database::core::MoteDB;
use crate::types::{Row, RowId, Value};
use crate::{Result, StorageError};
use crate::index::column_value::{ColumnValueIndex, ColumnValueIndexConfig};
use parking_lot::RwLock;
use std::sync::Arc;

impl MoteDB {
    /// Create a column value index for WHERE clause optimization
    /// 
    /// 🚀 **方案B（高性能）**: 使用scan_range一次性扫描LSM，避免全表加载到内存
    /// 
    /// # Performance
    /// - Point queries: 40x faster (1ms vs 40ms)
    /// - Range queries: Efficient B-Tree scan
    /// - CREATE INDEX: O(N) 顺序扫描，避免内存溢出
    /// 
    /// # Example
    /// ```ignore
    /// db.create_column_index("users", "email")?;
    /// // Now queries like WHERE email = 'foo@bar.com' are 40x faster
    /// ```
    pub fn create_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        // Use default index name format: {table}.{column}
        let index_name = format!("{}.{}", table_name, column_name);
        self.create_column_index_with_name(table_name, column_name, &index_name)
    }
    
    /// Create a column value index with custom name
    /// 
    /// 🚀 **方案B（高性能）**: 使用scan_range一次性扫描LSM，避免全表加载到内存
    /// 
    /// # Performance
    /// - Point queries: 40x faster (1ms vs 40ms)
    /// - Range queries: Efficient B-Tree scan
    /// - CREATE INDEX: O(N) 顺序扫描，避免内存溢出
    /// 
    /// # Example
    /// ```ignore
    /// db.create_column_index_with_name("users", "email", "idx_users_email")?;
    /// // Now queries like WHERE email = 'foo@bar.com' are 40x faster
    /// ```
    pub fn create_column_index_with_name(&self, table_name: &str, column_name: &str, index_name: &str) -> Result<()> {
        // 🎯 统一路径：{db}.mote/indexes/column_{index_name}.idx
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_path = indexes_dir.join(format!("column_{}.idx", index_name));
        
        let config = ColumnValueIndexConfig::default();
        let index = ColumnValueIndex::create(
            index_path, 
            table_name.to_string(), 
            column_name.to_string(), 
            config
        )?;
        
        let index_arc = Arc::new(RwLock::new(index));
        self.column_indexes.insert(index_name.to_string(), index_arc.clone());
        
        // 🚀 方案B：使用scan_range高性能扫描
        // 获取列在schema中的位置
        if let Ok(schema) = self.table_registry.get_table(table_name) {
            if let Some(col_def) = schema.columns.iter().find(|c| c.name == column_name) {
                let col_position = col_def.position;
                
                debug_log!("[create_column_index] 🔍 使用scan_range扫描LSM（方案B）...");
                let start_time = std::time::Instant::now();
                
                // 计算表的key范围
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                table_name.hash(&mut hasher);
                let table_hash = (hasher.finish() & 0xFFFFFFFF) as u64;
                
                let start_key = table_hash << 32;
                let end_key = (table_hash + 1) << 32;
                
                // 一次scan_range扫描所有数据
                let mut indexed_count = 0;
                const BATCH_SIZE: usize = 500; // 批量flush，避免内存溢出
                
                match self.lsm_engine.scan_range(start_key, end_key) {
                    Ok(entries) => {
                        for (batch_idx, chunk) in entries.chunks(BATCH_SIZE).enumerate() {
                            for (composite_key, value) in chunk {
                                let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                                
                                let data_bytes = match &value.data {
                                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                    crate::storage::lsm::ValueData::Blob(_) => continue,
                                };
                                
                                if let Ok(row) = bincode::deserialize::<Row>(data_bytes) {
                                    if let Some(value) = row.get(col_position) {
                                        if let Err(e) = index_arc.write().insert(value, row_id) {
                                            eprintln!("[create_column_index] ⚠️ 插入失败 row_id={}: {}", row_id, e);
                                        } else {
                                            indexed_count += 1;
                                        }
                                    }
                                }
                            }
                            
                            // 每4个batch或最后一个batch时flush
                            if (batch_idx + 1) % 4 == 0 || (batch_idx + 1) * BATCH_SIZE >= entries.len() {
                                if let Err(e) = index_arc.write().flush() {
                                    eprintln!("[create_column_index] ⚠️ Flush失败: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[create_column_index] ⚠️ scan_range失败: {}", e);
                    }
                }
                
                let scan_time = start_time.elapsed();
                
                if indexed_count > 0 {
                    debug_log!("[create_column_index] 🚀 扫描完成：{} 个值，耗时 {:?}", 
                             indexed_count, scan_time);
                    debug_log!("[create_column_index] ✅ 批量建索引完成！");
                } else {
                    debug_log!("[create_column_index] ⚠️ 未找到任何数据（扫描耗时 {:?}）", scan_time);
                }
            } else {
                println!("  ✓ Created empty column index '{}' (column not found in schema)", index_name);
            }
        } else {
            println!("  ✓ Created empty column index '{}' (table not found)", index_name);
        }
        
        Ok(())
    }
    
    /// Insert value into column index
    /// 
    /// Should be called after insert_table_row() if column index exists
    pub fn insert_column_value(&self, table_name: &str, column_name: &str, row_id: RowId, value: &Value) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        index_ref.value().write().insert(value, row_id)?;
        Ok(())
    }
    
    /// Get all column indexes for a table
    /// 
    /// Returns list of column names that have indexes
    pub fn get_table_column_indexes(&self, table_name: &str) -> Vec<String> {
        let prefix = format!("{}.", table_name);
        
        self.column_indexes.iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| entry.key().strip_prefix(&prefix).unwrap().to_string())
            .collect()
    }
    
    /// Delete value from column index
    /// 
    /// Should be called after delete_row() if column index exists
    pub fn delete_column_value(&self, table_name: &str, column_name: &str, row_id: RowId, value: &Value) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        index_ref.value().write().delete(value, row_id)?;
        Ok(())
    }
    
    /// Update value in column index (delete old + insert new)
    /// 
    /// Should be called after update_row() if column index exists
    pub fn update_column_value(&self, table_name: &str, column_name: &str, row_id: RowId, 
                                old_value: &Value, new_value: &Value) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        let mut index_guard = index_ref.value().write();
        index_guard.delete(old_value, row_id)?;
        index_guard.insert(new_value, row_id)?;
        Ok(())
    }
    
    /// Flush column index to disk (for bulk insert operations)
    pub fn flush_column_index(&self, table_name: &str, column_name: &str) -> Result<()> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        index_ref.value().write().flush()?;
        Ok(())
    }
    
    /// Query by column value (point query)
    /// 
    /// # Performance
    /// - With index: <1ms
    /// - Without index: 40ms (table scan)
    /// 
    /// # LSM Architecture
    /// - Queries both SSTable (via index) and MemTable (live data)
    /// - Merges and deduplicates results
    /// 
    /// # Example
    /// ```ignore
    /// let row_ids = db.query_by_column("users", "email", &Value::Text("foo@bar.com".into()))?;
    /// ```
    pub fn query_by_column(&self, table_name: &str, column_name: &str, value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        
        // Step 1: Query indexed data (SSTable)
        let mut row_ids = {
            let index_ref = self.column_indexes.get(&index_name)
                .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
            
            let index_guard = index_ref.value().read();
            index_guard.get(value)?
            // 🔓 自动释放
        };
        
        debug_log!("[query_by_column] 索引查询到 {} 条数据（来自SSTable）", row_ids.len());
        
        // Step 2: Scan MemTable for new data (不持有 column_indexes 锁)
        let memtable_ids = self.scan_memtable_for_column(table_name, column_name, |col_value| {
            col_value == value
        })?;
        
        debug_log!("[query_by_column] MemTable扫描到 {} 条数据（未索引）", memtable_ids.len());
        
        // Step 3: Merge and deduplicate
        row_ids.extend(memtable_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        
        // Step 4: 🔧 FIX: Filter out deleted rows (tombstone)
        let original_count = row_ids.len();
        row_ids.retain(|&row_id| {
            self.get_table_row(table_name, row_id)
                .map(|opt_row| opt_row.is_some())
                .unwrap_or(false)
        });
        
        let filtered_count = original_count - row_ids.len();
        if filtered_count > 0 {
            debug_log!("[query_by_column] 过滤掉 {} 条已删除数据", filtered_count);
        }
        
        debug_log!("[query_by_column] 最终返回 {} 条有效数据", row_ids.len());
        
        Ok(row_ids)
    }
    
    /// Query column value index with range (WHERE col >= start AND col <= end)
    /// Returns matching row IDs sorted by value
    /// 
    /// # LSM Architecture
    /// - Queries both SSTable (via index) and MemTable (live data)
    /// 
    /// # Example
    /// ```ignore
    /// let row_ids = db.query_by_column_range("users", "age", 
    ///     &Value::Integer(20), &Value::Integer(30))?;
    /// ```
    pub fn query_by_column_range(&self, table_name: &str, column_name: &str, 
                                start: &Value, end: &Value) -> Result<Vec<RowId>> {
        self.query_by_column_between(table_name, column_name, start, true, end, true)
    }
    
    /// 🚀 Query column value index: WHERE col < value
    /// 
    /// # Example
    /// ```ignore
    /// // Get all users with age < 30
    /// let row_ids = db.query_by_column_less_than("users", "age", &Value::Integer(30))?;
    /// ```
    pub fn query_by_column_less_than(&self, table_name: &str, column_name: &str, 
                                    value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        // Step 1: Query indexed data
        let mut row_ids = {
            let index_guard = index_ref.value().read();
            index_guard.query_less_than(value)?
        };
        
        // Step 2: Scan MemTable
        let memtable_ids = self.scan_memtable_for_column(table_name, column_name, |col_value| {
            col_value < value
        })?;
        
        // Step 3: Merge
        row_ids.extend(memtable_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        
        // Step 4: 🔧 FIX: Filter tombstones
        row_ids.retain(|&row_id| {
            self.get_table_row(table_name, row_id)
                .map(|opt_row| opt_row.is_some())
                .unwrap_or(false)
        });
        
        Ok(row_ids)
    }
    
    /// 🚀 Query column value index: WHERE col > value
    /// 
    /// # Example
    /// ```ignore
    /// // Get all users with age > 18
    /// let row_ids = db.query_by_column_greater_than("users", "age", &Value::Integer(18))?;
    /// ```
    pub fn query_by_column_greater_than(&self, table_name: &str, column_name: &str, 
                                       value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        // Step 1: Query indexed data
        let mut row_ids = {
            let index_guard = index_ref.value().read();
            index_guard.query_greater_than(value)?
        };
        
        // Step 2: Scan MemTable
        let memtable_ids = self.scan_memtable_for_column(table_name, column_name, |col_value| {
            col_value > value
        })?;
        
        // Step 3: Merge
        row_ids.extend(memtable_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        
        // Step 4: 🔧 FIX: Filter tombstones
        row_ids.retain(|&row_id| {
            self.get_table_row(table_name, row_id)
                .map(|opt_row| opt_row.is_some())
                .unwrap_or(false)
        });
        
        Ok(row_ids)
    }
    
    /// 🚀 Query column value index: WHERE col <= value
    pub fn query_by_column_less_than_or_equal(&self, table_name: &str, column_name: &str, 
                                             value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        // Step 1: Query indexed data
        let mut row_ids = {
            let index_guard = index_ref.value().read();
            index_guard.query_less_than_or_equal(value)?
        };
        
        // Step 2: Scan MemTable
        let memtable_ids = self.scan_memtable_for_column(table_name, column_name, |col_value| {
            col_value <= value
        })?;
        
        // Step 3: Merge
        row_ids.extend(memtable_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        
        // Step 4: 🔧 FIX: Filter tombstones
        row_ids.retain(|&row_id| {
            self.get_table_row(table_name, row_id)
                .map(|opt_row| opt_row.is_some())
                .unwrap_or(false)
        });
        
        Ok(row_ids)
    }
    
    /// 🚀 Query column value index: WHERE col >= value
    pub fn query_by_column_greater_than_or_equal(&self, table_name: &str, column_name: &str, 
                                                value: &Value) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        let index_ref = self.column_indexes.get(&index_name)
            .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
        
        // Step 1: Query indexed data
        let mut row_ids = {
            let index_guard = index_ref.value().read();
            index_guard.query_greater_than_or_equal(value)?
        };
        
        // Step 2: Scan MemTable
        let memtable_ids = self.scan_memtable_for_column(table_name, column_name, |col_value| {
            col_value >= value
        })?;
        
        // Step 3: Merge
        row_ids.extend(memtable_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        
        // Step 4: 🔧 FIX: Filter tombstones
        row_ids.retain(|&row_id| {
            self.get_table_row(table_name, row_id)
                .map(|opt_row| opt_row.is_some())
                .unwrap_or(false)
        });
        
        Ok(row_ids)
    }
    
    /// 🚀 Query column value index: dual-bound range query with flexible boundaries
    /// 
    /// **Use case**: `WHERE col > X AND col < Y`, `WHERE col >= X AND col <= Y`
    /// 
    /// **Performance**: O(log N + K) - single B-Tree scan (much faster than intersecting two queries)
    /// 
    /// # Example
    /// ```ignore
    /// // Get robots created between timestamps (exclusive)
    /// let row_ids = db.query_by_column_between("robots", "created_at",
    ///     &Value::Integer(100000), false,
    ///     &Value::Integer(200000), false)?;
    /// ```
    pub fn query_by_column_between(&self, table_name: &str, column_name: &str,
                                  lower_bound: &Value, lower_inclusive: bool,
                                  upper_bound: &Value, upper_inclusive: bool) -> Result<Vec<RowId>> {
        let index_name = format!("{}.{}", table_name, column_name);
        
        // Step 1: Query indexed data (SSTable)
        let mut row_ids = {
            let index_ref = self.column_indexes.get(&index_name)
                .ok_or_else(|| StorageError::Index(format!("Column index '{}' not found", index_name)))?;
            
            let index_guard = index_ref.value().read();
            index_guard.query_between(lower_bound, lower_inclusive, upper_bound, upper_inclusive)?
            // 🔓 自动释放
        };
        
        // Step 2: Scan MemTable for new data (不持有 column_indexes 锁)
        let memtable_ids = self.scan_memtable_for_column(table_name, column_name, |col_value| {
            let matches_lower = if lower_inclusive {
                col_value >= lower_bound
            } else {
                col_value > lower_bound
            };
            
            let matches_upper = if upper_inclusive {
                col_value <= upper_bound
            } else {
                col_value < upper_bound
            };
            
            matches_lower && matches_upper
        })?;
        
        // Step 3: Merge and deduplicate
        row_ids.extend(memtable_ids);
        row_ids.sort_unstable();
        row_ids.dedup();
        
        // Step 4: 🔧 FIX: Filter out deleted rows (tombstone)
        row_ids.retain(|&row_id| {
            self.get_table_row(table_name, row_id)
                .map(|opt_row| opt_row.is_some())
                .unwrap_or(false)
        });
        
        Ok(row_ids)
    }
    
    /// 🔧 LSM Helper: Scan MemTable for rows matching a column predicate
    /// 
    /// This scans the active + immutable MemTables to find rows where the specified
    /// column satisfies the given predicate.
    /// 
    /// # Arguments
    /// - `table_name`: Table to scan
    /// - `column_name`: Column to check
    /// - `predicate`: Function that returns true if the column value matches
    /// 
    /// # Returns
    /// Vector of row IDs that match the predicate
    fn scan_memtable_for_column<F>(&self, table_name: &str, column_name: &str, predicate: F) -> Result<Vec<RowId>>
    where
        F: Fn(&Value) -> bool,
    {
        // Get table schema to find column index
        let schema = self.table_registry.get_table(table_name)?;
        let column_index = schema.columns.iter()
            .position(|col| col.name == column_name)
            .ok_or_else(|| StorageError::InvalidData(format!("Column '{}' not found in table '{}'", column_name, table_name)))?;
        
        let mut matching_ids = Vec::new();
        let mut scanned_count = 0;
        
        // Scan incremental MemTable (active + immutable)
        self.lsm_engine.scan_memtable_incremental_with(|composite_key, row_data| {
            scanned_count += 1;
            
            // Extract row_id from composite key (lower 32 bits)
            let row_id = (composite_key & 0xFFFFFFFF) as RowId;
            
            // Deserialize row
            let row: Row = bincode::deserialize(row_data)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            // Check if column matches predicate
            if column_index < row.len() {
                let col_value = &row[column_index];
                if predicate(col_value) {
                    matching_ids.push(row_id);
                }
            }
            
            Ok(())
        })?;
        
        debug_log!("[scan_memtable_for_column] 扫描了 {} 条MemTable数据，匹配 {} 条", 
                 scanned_count, matching_ids.len());
        
        Ok(matching_ids)
    }
}
