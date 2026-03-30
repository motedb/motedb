//! Text Index Operations (Full-Text Search)
//!
//! Extracted from database_legacy.rs
//! Provides full-text search with BM25 ranking

use crate::database::core::MoteDB;
use crate::types::RowId;
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
        
        // ✅ P0 FIX: 只创建空索引，不在这里回填数据
        // 原因：
        // 1. 避免双重扫描（create_text_index + executor各扫一次）
        // 2. 避免内存爆炸（全量加载到Vec）
        // 3. 避免锁风暴（100万次写锁）
        // 回填工作由 executor.rs 负责（使用批量流式处理）
        
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
    /// Persists all in-memory inverted lists to disk.
    /// Note: Index metadata is managed by IndexRegistry (index_metadata.bin),
    /// no need to save text_indexes_metadata.bin separately.
    pub fn flush_text_indexes(&self) -> Result<()> {
        // 🚀 DashMap: 直接遍历并 flush
        for entry in self.text_indexes.iter() {
            entry.value().write().flush()?;
        }
        
        Ok(())
    }
}
