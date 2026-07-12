//! Text Index Operations (Full-Text Search)
//!
//! Extracted from database_legacy.rs
//! Provides full-text search with BM25 ranking

use crate::database::core::MoteDB;
use crate::index::text_fts::{TextFTSIndex, TextFTSStats};
use crate::types::RowId;
use crate::{Result, StorageError};
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
        ensure_open!(self);
        // 🎯 统一路径：{db}.mote/indexes/text_{name}/
        let indexes_dir = self.path.join("indexes");
        std::fs::create_dir_all(&indexes_dir)?;
        let index_path = indexes_dir.join(format!("text_{}", name));

        let index = TextFTSIndex::new(index_path)?;
        let index_arc = Arc::new(RwLock::new(index));
        self.text_indexes
            .insert(name.to_string(), index_arc.clone());

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
        let index_ref = self
            .text_indexes
            .get(index_name)
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
        let index_ref = self
            .text_indexes
            .get(index_name)
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
    pub fn update_text(
        &self,
        row_id: RowId,
        index_name: &str,
        old_text: &str,
        new_text: &str,
    ) -> Result<()> {
        let index_ref = self
            .text_indexes
            .get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;

        index_ref
            .value()
            .write()
            .update(row_id, old_text, new_text)?;
        Ok(())
    }

    /// 🚀 Build text index from columnar SSTable data — O(N) scan of TextSegment.
    /// Restores FTS capability after zero-encode INSERT (which skips per-row indexing).
    pub fn build_text_index_from_columnar(
        &self,
        index_name: &str,
        table_name: &str,
        col_position: usize,
    ) -> Result<usize> {
        // 🚀 ColSegmentStore path (the active storage engine).
        if self.has_col_segment_store(table_name) {
            return self.build_text_index_from_col_segment(index_name, table_name, col_position);
        }
        // Legacy columnar_sstables path.
        let col_sst = match self.columnar_sstables.get(table_name) {
            Some(sst) => sst.clone(),
            None => return Ok(0),
        };
        let text_seg = match col_sst.read_text(col_position) {
            Ok(seg) => seg,
            Err(_) => return Ok(0),
        };

        let mut batch: Vec<(RowId, String)> = Vec::with_capacity(10000);
        let mut total = 0usize;
        let _ = col_sst.load_full_keys();
        for i in 0..col_sst.num_rows {
            if col_sst.row_map.is_deleted(i) {
                continue;
            }
            if let Some(s) = text_seg.get_str(i) {
                let row_id = (col_sst.row_map.key(i) & 0xFFFFFFFF) as RowId;
                batch.push((row_id, s.to_string()));
                if batch.len() >= 10000 {
                    let refs: Vec<(RowId, &str)> =
                        batch.iter().map(|(id, s)| (*id, s.as_str())).collect();
                    total += self.batch_insert_texts(index_name, &refs)?;
                    batch.clear();
                }
            }
        }
        if !batch.is_empty() {
            let refs: Vec<(RowId, &str)> = batch.iter().map(|(id, s)| (*id, s.as_str())).collect();
            total += self.batch_insert_texts(index_name, &refs)?;
        }
        Ok(total)
    }

    /// 🚀 Build text index from ColSegmentStore (the active storage engine).
    /// Reads TextSegment from each segment and batch-inserts into the FTS index.
    pub fn build_text_index_from_col_segment(
        &self,
        index_name: &str,
        table_name: &str,
        col_position: usize,
    ) -> Result<usize> {
        let store = match self.col_segment_stores.get(table_name) {
            Some(s) => s.clone(),
            None => return Ok(0),
        };
        let _ = store.flush_buffer();

        let segs = store.segments_snapshot();
        let mut batch: Vec<(RowId, String)> = Vec::with_capacity(10000);
        let mut total = 0usize;

        for seg in segs.iter() {
            let n = seg.sst.num_rows;
            let has_deletions = seg.sst.row_map.has_any_deleted();
            let _ = seg.sst.load_full_keys();
            match seg.sst.read_text(col_position) {
                Ok(tseg) => {
                    let has_nulls = tseg.has_any_null();
                    for i in 0..n {
                        if has_deletions && seg.sst.row_map.is_deleted(i) {
                            continue;
                        }
                        let s = if has_nulls {
                            tseg.get_str(i)
                        } else {
                            Some(tseg.get_str_fast(i))
                        };
                        if let Some(s) = s {
                            let row_id = (seg.sst.row_map.key(i) & 0xFFFFFFFF) as RowId;
                            batch.push((row_id, s.to_string()));
                            if batch.len() >= 10000 {
                                let refs: Vec<(RowId, &str)> =
                                    batch.iter().map(|(id, s)| (*id, s.as_str())).collect();
                                total += self.batch_insert_texts(index_name, &refs)?;
                                batch.clear();
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }
        if !batch.is_empty() {
            let refs: Vec<(RowId, &str)> = batch.iter().map(|(id, s)| (*id, s.as_str())).collect();
            total += self.batch_insert_texts(index_name, &refs)?;
        }
        Ok(total)
    }

    /// Batch insert texts for multiple rows (10-100x faster than individual inserts)
    pub fn batch_insert_texts(&self, index_name: &str, texts: &[(RowId, &str)]) -> Result<usize> {
        if texts.is_empty() {
            return Ok(0);
        }

        let index_ref = self
            .text_indexes
            .get(index_name)
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
        ensure_open!(self);
        let index_ref = self
            .text_indexes
            .get(index_name)
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
    pub fn text_search_ranked(
        &self,
        index_name: &str,
        query: &str,
        top_k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        let index_ref = self
            .text_indexes
            .get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;

        let results = index_ref.value().read().search_ranked(query, top_k)?;
        Ok(results)
    }

    /// Search for documents containing an exact phrase
    pub fn text_search_phrase(&self, index_name: &str, phrase: &str) -> Result<Vec<RowId>> {
        let index_ref = self
            .text_indexes
            .get(index_name)
            .ok_or_else(|| StorageError::Index(format!("Text index '{}' not found", index_name)))?;
        let guard = index_ref.value().read();
        guard.search_phrase(phrase)
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
        let index_ref = self
            .text_indexes
            .get(name)
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
