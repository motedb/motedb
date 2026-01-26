//! Text Index Operations (Full-Text Search)
//!
//! Extracted from database_legacy.rs
//! Provides full-text search with BM25 ranking

use crate::database::core::MoteDB;
use crate::types::{Row, RowId};
use crate::{Result, StorageError};
use crate::index::text_fts::{TextFTSIndex, TextFTSStats};
use parking_lot::RwLock;
use std::sync::Arc;

impl MoteDB {
    /// Create a text index for full-text search
    /// 
    /// # Example
    /// ```ignore
    /// db.create_text_index("articles_content")?;
    /// ```
    pub fn create_text_index(&self, name: &str) -> Result<()> {
        // 🎯 统一路径：{db}.mote/indexes/text_{name}/
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_path = indexes_dir.join(format!("text_{}", name));
        
        let index = TextFTSIndex::new(index_path)?;
        let index_arc = Arc::new(RwLock::new(index));
        self.text_indexes.insert(name.to_string(), index_arc.clone());
        
        // 🚀 方案B：使用scan_range高性能扫描
        // name格式: "table_column"
        let parts: Vec<&str> = name.split('_').collect();
        if parts.len() >= 2 {
            let table_name = parts[0];
            let column_name = parts[1..].join("_");
            
            if let Ok(schema) = self.table_registry.get_table(table_name) {
                if let Some(col_def) = schema.columns.iter().find(|c| c.name == column_name) {
                    let col_position = col_def.position;
                    
                    println!("[create_text_index] 🔍 使用scan_range扫描LSM（方案B）...");
                    let start_time = std::time::Instant::now();
                    
                    // 计算表的key范围
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    table_name.hash(&mut hasher);
                    let table_hash = hasher.finish() & 0xFFFFFFFF;
                    
                    let start_key = table_hash << 32;
                    let end_key = (table_hash + 1) << 32;
                    
                    // 一次scan_range扫描所有数据
                    let mut texts_to_index = Vec::new();
                    match self.lsm_engine.scan_range(start_key, end_key) {
                        Ok(entries) => {
                            for (composite_key, value) in entries {
                                let row_id = (composite_key & 0xFFFFFFFF) as RowId;
                                
                                let data_bytes = match &value.data {
                                    crate::storage::lsm::ValueData::Inline(bytes) => bytes.as_slice(),
                                    crate::storage::lsm::ValueData::Blob(_) => continue,
                                };
                                
                                if let Ok(row) = bincode::deserialize::<Row>(data_bytes) {
                                    if let Some(crate::types::Value::Text(text)) = row.get(col_position) {
                                        texts_to_index.push((row_id, text.clone()));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[create_text_index] ⚠️ scan_range失败: {}", e);
                        }
                    }
                    
                    let scan_time = start_time.elapsed();
                    
                    if !texts_to_index.is_empty() {
                        println!("[create_text_index] 🚀 扫描完成：{} 条文本，耗时 {:?}", 
                                 texts_to_index.len(), scan_time);
                        
                        let build_time = std::time::Instant::now();
                        for (row_id, text) in texts_to_index {
                            if let Err(e) = index_arc.write().insert(row_id, &text) {
                                eprintln!("[create_text_index] ⚠️ 插入失败 row_id={}: {}", row_id, e);
                            }
                        }
                        println!("[create_text_index] ✅ 批量建索引完成！耗时 {:?}", build_time.elapsed());
                    } else {
                        println!("[create_text_index] ⚠️ 未找到任何文本数据（扫描耗时 {:?}）", scan_time);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Insert text for a row into text index
    /// 
    /// # Example
    /// ```ignore
    /// db.insert_text(row_id, "articles_content", "The quick brown fox...")?;
    /// ```
    pub fn insert_text(&self, row_id: RowId, index_name: &str, text: &str) -> Result<()> {
        let index_ref = self.text_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        
        index_ref.value().write().insert(row_id, text)?;
        Ok(())
    }
    
    /// Delete text for a row from text index
    /// 
    /// # Example
    /// ```ignore
    /// db.delete_text(row_id, "articles_content", "The quick brown fox...")?;
    /// ```
    pub fn delete_text(&self, row_id: RowId, index_name: &str, text: &str) -> Result<()> {
        let index_ref = self.text_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        
        index_ref.value().write().delete(row_id, text)?;
        Ok(())
    }
    
    /// Update text for a row in text index
    /// 
    /// # Example
    /// ```ignore
    /// db.update_text(row_id, "articles_content", "old text", "new text")?;
    /// ```
    pub fn update_text(&self, row_id: RowId, index_name: &str, old_text: &str, new_text: &str) -> Result<()> {
        let index_ref = self.text_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        
        index_ref.value().write().update(row_id, old_text, new_text)?;
        Ok(())
    }
    
    /// Batch insert texts for multiple rows (10-100x faster than individual inserts)
    ///
    /// # Performance Optimization
    /// - Avoids repeated lock acquisition
    /// - Builds all inverted lists at once
    /// - Zero-copy: passes &str references instead of String copies
    ///
    /// # Example
    /// ```ignore
    /// let texts: Vec<(u64, &str)> = vec![
    ///     (1, "The quick brown fox"),
    ///     (2, "jumps over the lazy dog"),
    ///     (3, "The lazy cat"),
    /// ];
    /// db.batch_insert_texts("description", &texts)?;
    /// ```
    pub fn batch_insert_texts(&self, index_name: &str, texts: &[(RowId, &str)]) -> Result<usize> {
        if texts.is_empty() {
            return Ok(0);
        }
        
        let index_ref = self.text_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        
        let count = texts.len();
        index_ref.value().write().batch_insert(texts)?;
        
        Ok(count)
    }
    
    /// Search for documents containing query terms (boolean AND)
    /// 
    /// # Example
    /// ```ignore
    /// let doc_ids = db.text_search("articles_content", "rust database")?;
    /// ```
    pub fn text_search(&self, index_name: &str, query: &str) -> Result<Vec<RowId>> {
        let index_ref = self.text_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        
        let results = index_ref.value().read().search(query)?;
        Ok(results)
    }
    
    /// Search with BM25 ranking (returns top-k results sorted by relevance)
    /// 
    /// # Example
    /// ```ignore
    /// // Get top 10 most relevant documents
    /// let results = db.text_search_ranked("articles_content", "rust database", 10)?;
    /// for (row_id, score) in results {
    ///     println!("Document {}: score {:.3}", row_id, score);
    /// }
    /// ```
    pub fn text_search_ranked(&self, index_name: &str, query: &str, top_k: usize) -> Result<Vec<(RowId, f32)>> {
        let index_ref = self.text_indexes.get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        
        let results = index_ref.value().read().search_ranked(query, top_k)?;
        Ok(results)
    }
    
    /// Get text index statistics
    /// 
    /// # Example
    /// ```ignore
    /// let stats = db.text_index_stats("articles_content")?;
    /// println!("Total documents: {}", stats.total_documents);
    /// println!("Unique terms: {}", stats.unique_terms);
    /// ```
    pub fn text_index_stats(&self, name: &str) -> Result<TextFTSStats> {
        let index_ref = self.text_indexes.get(name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", name)))?;
        
        let index_guard = index_ref.value().read();
        Ok(index_guard.stats())
    }
    
    /// Flush text indexes to disk
    /// 
    /// Persists all in-memory inverted lists and metadata to disk
    pub fn flush_text_indexes(&self) -> Result<()> {
        // 🚀 DashMap: 收集索引名称并保存 metadata
        let index_names: Vec<String> = self.text_indexes.iter()
            .map(|entry| entry.key().clone())
            .collect();
        
        if !index_names.is_empty() {
            // ⭐ 修复路径：应该是 {db}.mote/text_indexes_metadata.bin
            let metadata_path = self.path.join("text_indexes_metadata.bin");
            
            let data = bincode::serialize(&index_names)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            std::fs::write(&metadata_path, data)
                .map_err(StorageError::Io)?;
        }
        
        // 🚀 DashMap: 直接遍历并 flush
        for entry in self.text_indexes.iter() {
            entry.value().write().flush()?;
        }
        
        Ok(())
    }
}
