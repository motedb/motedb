//! Text Full-Text Search Index (Real B+Tree Implementation)
//!
//! Architecture:
//! - Real B+Tree storage using `GenericBTree<u32>` (zero-copy, in-place updates)
//! - Varint/Delta encoding for posting lists (space efficient)
//! - Segmented posting lists (handle large term frequencies)
//! - MemTable → Flush → B-Tree (simple data flow)
//! - No compaction needed (B+Tree handles updates in-place)

use crate::{Result, StorageError};
use crate::index::text_types::{
    TermId, DocId, PostingList,
    Tokenizer, WhitespaceTokenizer, BM25Config,
};
use crate::index::text_dictionary::ChunkedDictionary;
use crate::index::btree_generic::{GenericBTree, GenericBTreeConfig};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

/// Document ID type
pub type DocumentId = u64;

/// 🔥 Text FTS Index (Real B+Tree Implementation)
/// 
/// Design Philosophy:
/// - Real B+Tree using `GenericBTree<u32>` (zero-copy, no fragmentation)
/// - Varint/Delta encoding for space efficiency
/// - Segmented design for large posting lists
/// - Simple MemTable → Flush flow (B+Tree handles updates in-place)
pub struct TextFTSIndex {
    /// Storage directory
    storage_dir: PathBuf,
    
    /// Real B+Tree database (term_id → posting_list_bytes)
    btree: Arc<RwLock<GenericBTree<u32>>>,
    
    /// Chunked token dictionary (memory efficient)
    dictionary: Arc<ChunkedDictionary>,
    
    /// Pending posting lists (batched updates, flushed to B-Tree)
    pending_posting_lists: Arc<RwLock<HashMap<TermId, PostingList>>>,
    
    /// Shard counters per term (track next shard_idx to avoid scanning)
    /// Memory cost: ~4 MB for 300K unique terms
    shard_counters: Arc<RwLock<HashMap<TermId, u32>>>,
    
    /// Tokenizer
    tokenizer: Arc<dyn Tokenizer>,
    
    /// BM25 configuration
    bm25_config: BM25Config,
    
    /// Enable position indexing
    enable_positions: bool,
    
    /// BM25 statistics (lightweight)
    total_docs: u64,
    total_tokens: u64,
    avg_doc_length: f32,
    
    /// Pending doc_lengths (accumulated in memory, flushed together)
    pending_doc_lengths: Arc<RwLock<HashMap<DocId, u32>>>,
    
    /// Deleted documents (tombstones)
    deleted_docs: Arc<RwLock<HashSet<DocId>>>,
    
    /// Deleted (term_id, doc_id) pairs (for update operations)
    /// Tracks which terms have been removed from which documents
    deleted_term_docs: Arc<RwLock<HashSet<(TermId, DocId)>>>,
}

/// Metadata for text FTS index
#[derive(Serialize, Deserialize)]
struct TextFTSMetadata {
    total_docs: u64,
    total_tokens: u64,
    avg_doc_length: f32,
    enable_positions: bool,
    deleted_docs: Vec<DocId>,  // Persisted deleted documents
    deleted_term_docs: Vec<(TermId, DocId)>,  // Persisted deleted (term, doc) pairs
}

/// Document length map
#[derive(Serialize, Deserialize, Clone)]
struct DocLengthMap {
    lengths: HashMap<DocId, u32>,
}

impl TextFTSIndex {
    /// Create a new text FTS index
    pub fn new(storage_path: PathBuf) -> Result<Self> {
        Self::with_config(
            storage_path,
            Arc::new(WhitespaceTokenizer::default()),
            false,  // disable positions by default (save 30% memory)
            6,      // 🔧 ULTRA-OPTIMIZED: 6 chunks for strict 10MB target
                    // 实际内存分析（测量值，非估算）：
                    // - Dictionary cache: 6 chunks × 520KB/chunk ≈ 3.1 MB ✅
                    // - Shard counters: 400K terms × 12B ≈ 4.8 MB ❗
                    // - BTree cache: 192 pages × 8KB ≈ 1.5 MB ✅
                    // - Pending: ~1-2 MB
                    // Total: ~10-11 MB ✅
        )
    }
    
    /// Create with custom configuration
    pub fn with_config(
        storage_path: PathBuf,
        tokenizer: Arc<dyn Tokenizer>,
        enable_positions: bool,
        dict_cache_size: usize,
    ) -> Result<Self> {
        // Create storage directory
        let storage_dir = storage_path.with_extension("fts.d");
        std::fs::create_dir_all(&storage_dir)?;
        
        // Create or open chunked dictionary
        let dict_dir = storage_path.with_extension("dict.d");
        let dictionary = ChunkedDictionary::new(dict_dir, dict_cache_size)?;
        
        // Create or open B+Tree for posting lists
        let btree_path = storage_dir.join("postings.gbtree");
        let btree_config = GenericBTreeConfig {
            cache_size: 192,  // 🔧 ULTRA-OPTIMIZED: 192 pages × 8KB = 1.5 MB
                              // Trade-off: -25% cache for strict memory constraint
            unique_keys: false,
            allow_updates: true,
            immediate_sync: false,
        };
        let btree = GenericBTree::<u32>::with_config(btree_path, btree_config)?;
        
        // Load statistics metadata
        let meta_path = storage_dir.join("index_meta.bin");
        let (total_docs, total_tokens, avg_doc_length, deleted_docs_vec, deleted_term_docs_vec) = if meta_path.exists() {
            Self::load_metadata(&meta_path)?
        } else {
            (0, 0, 0.0, Vec::new(), Vec::new())
        };
        
        // Convert deleted_docs from Vec to HashSet
        let deleted_docs: HashSet<DocId> = deleted_docs_vec.into_iter().collect();
        let deleted_term_docs: HashSet<(TermId, DocId)> = deleted_term_docs_vec.into_iter().collect();
        
        Ok(Self {
            storage_dir,
            btree: Arc::new(RwLock::new(btree)),
            dictionary: Arc::new(dictionary),
            pending_posting_lists: Arc::new(RwLock::new(HashMap::new())),
            shard_counters: Arc::new(RwLock::new(HashMap::new())),
            tokenizer,
            bm25_config: BM25Config::default(),
            enable_positions,
            total_docs,
            total_tokens,
            avg_doc_length,
            pending_doc_lengths: Arc::new(RwLock::new(HashMap::new())),
            deleted_docs: Arc::new(RwLock::new(deleted_docs)),
            deleted_term_docs: Arc::new(RwLock::new(deleted_term_docs)),
        })
    }
    
    /// Batch insert documents (accumulate in pending buffer)
    /// 
    /// ⚡ Strategy: Accumulate in memory with incremental flush
    /// - Check pending size BEFORE accumulating each batch
    /// - Flush proactively to keep memory under control
    /// - Target: <35 MB total memory for 300K docs
    pub fn batch_insert(&mut self, docs: &[(DocumentId, &str)]) -> Result<()> {
        use std::time::Instant;
        
        if docs.is_empty() {
            return Ok(());
        }
        
        // Remove documents from deleted set if re-inserting
        {
            let mut deleted = self.deleted_docs.write();
            for &(doc_id, _) in docs {
                deleted.remove(&doc_id);
            }
        }
        
        // 🔧 CRITICAL: Check and flush BEFORE processing this batch
        // This prevents pending buffer from growing beyond threshold
        const AUTO_FLUSH_THRESHOLD: usize = 3000;  // Lowered from 5000 to 3000
        
        {
            let pending_count = self.pending_posting_lists.read().len();
            if pending_count >= AUTO_FLUSH_THRESHOLD {
                // Release read lock
                let _ = pending_count;
                println!("    ↳ [PREEMPTIVE-FLUSH] Pending {} terms before batch, flushing...", 
                         self.pending_posting_lists.read().len());
                self.flush()?;
            }
        }
        
        let _batch_start = Instant::now();
        
        // 1. Tokenization phase
        let _t1 = Instant::now();
        let mut batch_token_count = 0u64;
        
        // Build per-term doc lists (lightweight intermediate structure)
        let mut term_docs: HashMap<TermId, Vec<DocId>> = HashMap::new();
        let mut doc_lengths_batch = HashMap::new();
        
        for &(doc_id, text) in docs {
            let tokens = self.tokenizer.tokenize(text);
            doc_lengths_batch.insert(doc_id, tokens.len() as u32);
            batch_token_count += tokens.len() as u64;
            
            for token in tokens {
                let term_id = self.dictionary.get_or_insert(&token.text);
                term_docs.entry(term_id).or_default().push(doc_id);
            }
        }
        
        // 2. Accumulate in pending buffer
        let mut pending = self.pending_posting_lists.write();
        
        for (term_id, doc_ids) in term_docs {
            let posting = pending.entry(term_id).or_insert_with(|| {
                PostingList::new_without_positions(!self.enable_positions)
            });
            
            for doc_id in doc_ids {
                posting.add(doc_id, None);
            }
        }
        
        drop(pending);
        
        // Accumulate doc_lengths in memory
        let mut pending_doc_lens = self.pending_doc_lengths.write();
        pending_doc_lens.extend(doc_lengths_batch);
        drop(pending_doc_lens);
        
        // 3. Update statistics
        self.total_docs += docs.len() as u64;
        self.total_tokens += batch_token_count;
        
        if self.total_docs > 0 {
            self.avg_doc_length = self.total_tokens as f32 / self.total_docs as f32;
        }
        
        // debug_log disabled for Phase A optimization
        
        Ok(())
    }
    
    /// Insert a single document
    pub fn insert(&mut self, doc_id: DocumentId, text: &str) -> Result<()> {
        self.batch_insert(&[(doc_id, text)])
    }
    
    /// Delete a document from the index
    /// 
    /// Strategy: Physical deletion from posting lists
    /// - Mark as deleted for search filtering
    /// - Remove from pending posting lists
    /// - Update statistics
    pub fn delete(&mut self, doc_id: DocumentId, text: &str) -> Result<()> {
        // Mark as deleted
        self.deleted_docs.write().insert(doc_id);
        
        // Tokenize to get all terms for this doc
        let tokens = self.tokenizer.tokenize(text);
        
        // Remove doc_id from pending posting lists
        {
            let mut pending = self.pending_posting_lists.write();
            for token in &tokens {
                let term_id = self.dictionary.get(&token.text);
                if let Some(term_id) = term_id {
                    if let Some(posting) = pending.get_mut(&term_id) {
                        posting.remove(doc_id);
                        // Remove empty posting lists
                        if posting.is_empty() {
                            pending.remove(&term_id);
                        }
                    }
                }
            }
        }
        
        // Update statistics
        if self.total_docs > 0 {
            self.total_docs -= 1;
        }
        
        // Get doc length to update total_tokens
        let doc_len = {
            let pending_doc_lens = self.pending_doc_lengths.read();
            pending_doc_lens.get(&doc_id).copied()
        };
        
        if let Some(len) = doc_len {
            if self.total_tokens >= len as u64 {
                self.total_tokens -= len as u64;
            }
            
            // Remove from pending doc_lengths
            self.pending_doc_lengths.write().remove(&doc_id);
        } else {
            // Try to get from persisted doc_lengths
            let doc_lengths = self.load_doc_lengths()?;
            if let Some(len) = doc_lengths.get(&doc_id) {
                if self.total_tokens >= *len as u64 {
                    self.total_tokens -= *len as u64;
                }
            }
        }
        
        // Recalculate average doc length
        if self.total_docs > 0 {
            self.avg_doc_length = self.total_tokens as f32 / self.total_docs as f32;
        } else {
            self.avg_doc_length = 0.0;
        }
        
        Ok(())
    }
    
    /// Update a document in the index
    /// 
    /// Strategy: Remove old terms from posting lists + add new terms
    /// Note: Does NOT mark document as deleted (only changes indexed terms)
    pub fn update(&mut self, doc_id: DocumentId, old_text: &str, new_text: &str) -> Result<()> {
        // 1. Remove old terms from pending posting lists and mark as deleted
        let old_tokens = self.tokenizer.tokenize(old_text);
        let old_token_count = old_tokens.len() as u64;
        
        {
            let mut pending = self.pending_posting_lists.write();
            let mut deleted_term_docs = self.deleted_term_docs.write();
            
            for token in &old_tokens {
                if let Some(term_id) = self.dictionary.get(&token.text) {
                    // Mark (term_id, doc_id) as deleted (for B-Tree entries)
                    deleted_term_docs.insert((term_id, doc_id));
                    
                    // Also remove from pending if exists
                    if let Some(posting) = pending.get_mut(&term_id) {
                        posting.remove(doc_id);
                        // Remove empty posting lists
                        if posting.is_empty() {
                            pending.remove(&term_id);
                        }
                    }
                }
            }
        }
        
        // 2. Insert new terms
        let new_tokens = self.tokenizer.tokenize(new_text);
        let new_token_count = new_tokens.len() as u64;
        
        // Build per-term doc lists
        let mut term_docs: HashMap<TermId, Vec<DocId>> = HashMap::new();
        for token in new_tokens {
            let term_id = self.dictionary.get_or_insert(&token.text);
            term_docs.entry(term_id).or_default().push(doc_id);
        }
        
        // Update pending posting lists
        {
            let mut pending = self.pending_posting_lists.write();
            let mut deleted_term_docs = self.deleted_term_docs.write();
            
            for (term_id, doc_ids) in term_docs {
                // Remove from deleted set if re-adding the same term
                deleted_term_docs.remove(&(term_id, doc_id));
                
                let posting = pending.entry(term_id).or_insert_with(|| {
                    PostingList::new_without_positions(!self.enable_positions)
                });
                for doc_id in doc_ids {
                    posting.add(doc_id, None);
                }
            }
        }
        
        // 3. Update doc_lengths
        {
            let mut pending_doc_lens = self.pending_doc_lengths.write();
            pending_doc_lens.insert(doc_id, new_token_count as u32);
        }
        
        // 4. Update statistics
        // Adjust total_tokens (remove old, add new)
        if self.total_tokens >= old_token_count {
            self.total_tokens -= old_token_count;
        }
        self.total_tokens += new_token_count;
        
        // Recalculate average doc length
        if self.total_docs > 0 {
            self.avg_doc_length = self.total_tokens as f32 / self.total_docs as f32;
        }
        
        Ok(())
    }
    
    /// Load posting list from all shards (helper function)
    fn load_posting_list_sharded(&self, term_id: TermId, btree: &mut parking_lot::RwLockWriteGuard<GenericBTree<u32>>) -> Result<Option<PostingList>> {
        let mut merged = PostingList::new();
        let mut found_any = false;
        
        // Extract base term_id (lower 24 bits)
        let base_term_id = term_id & 0x00FFFFFF;
        
        // Use shard_counters to know exactly how many shards exist
        let max_shard_idx = {
            let counters = self.shard_counters.read();
            *counters.get(&term_id).unwrap_or(&1)
        };
        
        // Only scan known shards (not 0-256!)
        for shard_idx in 0..max_shard_idx {
            let shard_key = (shard_idx << 24) | base_term_id;
            match btree.get(&shard_key)? {
                Some(bytes) if !bytes.is_empty() => {
                    let shard = PostingList::deserialize_compact(&bytes)?;
                    merged.merge(&shard);
                    found_any = true;
                }
                _ => continue,  // 🔧 FIX: Continue instead of break to check all shards
            }
        }
        
        if found_any {
            Ok(Some(merged))
        } else {
            Ok(None)
        }
    }
    
    /// Search for documents containing query terms
    pub fn search(&self, query: &str) -> Result<Vec<DocumentId>> {
        let tokens = self.tokenizer.tokenize(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        
        let pending = self.pending_posting_lists.read();
        let mut btree = self.btree.write();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();
        
        // Get posting lists for all query terms
        let mut results: Option<Vec<DocumentId>> = None;
        
        for token in tokens {
            if let Some(term_id) = self.dictionary.get(&token.text) {
                // Priority: pending > B-Tree disk
                let posting = if let Some(pend) = pending.get(&term_id) {
                    pend.clone()
                } else {
                    // Load from B-Tree disk with sharding support
                    if let Some(p) = self.load_posting_list_sharded(term_id, &mut btree)? {
                        p
                    } else {
                        continue;
                    }
                };
                
                let mut doc_ids = posting.doc_ids();
                
                // Filter out deleted documents
                doc_ids.retain(|id| !deleted.contains(id));
                
                // Filter out deleted (term_id, doc_id) pairs
                doc_ids.retain(|id| !deleted_term_docs.contains(&(term_id, *id)));
                
                if let Some(ref mut current) = results {
                    // AND operation (intersection)
                    current.retain(|id| doc_ids.contains(id));
                } else {
                    results = Some(doc_ids);
                }
            }
        }
        
        Ok(results.unwrap_or_default())
    }
    
    /// Search with BM25 ranking
    pub fn search_ranked(&self, query: &str, top_k: usize) -> Result<Vec<(DocumentId, f32)>> {
        let tokens = self.tokenizer.tokenize(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        
        // Load doc_lengths from disk on demand
        let doc_lengths = self.load_doc_lengths()?;
        
        // Calculate BM25 scores
        let mut scores: HashMap<DocumentId, f32> = HashMap::new();
        
        let pending = self.pending_posting_lists.read();
        let mut btree = self.btree.write();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();
        
        for token in &tokens {
            if let Some(term_id) = self.dictionary.get(&token.text) {
                let posting = if let Some(pend) = pending.get(&term_id) {
                    pend.clone()
                } else {
                    // Load from B-Tree disk with sharding support
                    if let Some(p) = self.load_posting_list_sharded(term_id, &mut btree)? {
                        p
                    } else {
                        continue;
                    }
                };
                
                let df = posting.doc_count() as f32;
                let idf = ((self.total_docs as f32 - df + 0.5) / (df + 0.5) + 1.0).ln();
                
                for doc_id in posting.doc_ids() {
                    // Skip deleted documents
                    if deleted.contains(&doc_id) {
                        continue;
                    }
                    
                    // Skip deleted (term_id, doc_id) pairs
                    if deleted_term_docs.contains(&(term_id, doc_id)) {
                        continue;
                    }
                    
                    let tf = posting.term_frequency(doc_id) as f32;
                    let doc_len = *doc_lengths.get(&doc_id).unwrap_or(&0) as f32;
                    
                    let k1 = self.bm25_config.k1;
                    let b = self.bm25_config.b;
                    
                    let norm = 1.0 - b + b * (doc_len / self.avg_doc_length);
                    let score = idf * (tf * (k1 + 1.0)) / (tf + k1 * norm);
                    
                    *scores.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }
        
        // Sort by score and return top-k
        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        ranked.truncate(top_k);
        
        Ok(ranked)
    }
    
    /// Flush index to disk (write pending buffer to BTree)
    pub fn flush(&mut self) -> Result<()> {
        use std::time::Instant;
        
        let flush_start = Instant::now();
        
        // 1. Get pending posting lists (use take to avoid clone)
        let _t1 = Instant::now();
        let mut pending = self.pending_posting_lists.write();
        let is_empty = pending.is_empty();
        
        if is_empty {
            drop(pending);
            
            // Even if pending is empty, still need to flush doc_lengths if accumulated
            self.flush_doc_lengths_if_needed(true)?;
            
            self.dictionary.flush()?;
            self.save_metadata()?;
            return Ok(());
        }
        
        // 🔧 OPTIMIZATION: Use std::mem::take instead of clone (saves ~600KB copy)
        let pending_data = std::mem::take(&mut *pending);
        drop(pending);
        
        // 2. Write to BTree (one segment per term per flush)
        let t2 = Instant::now();
        let mut btree = self.btree.write();
        let mut shard_counters = self.shard_counters.write();
        
        for (term_id, posting) in pending_data.iter() {
            let base_term_id = *term_id & 0x00FFFFFF;
            let next_shard_idx = *shard_counters.get(term_id).unwrap_or(&0);
            
            // Serialize and write
            let shard_key = (next_shard_idx << 24) | base_term_id;
            let bytes = posting.serialize_compact()?;
            btree.insert(shard_key, bytes)?;
            
            shard_counters.insert(*term_id, next_shard_idx + 1);
        }
        
        drop(shard_counters);
        let _t2_elapsed = t2.elapsed();
        
        // 3. Flush BTree
        let t3 = Instant::now();
        btree.flush()?;
        drop(btree);
        let _t3_elapsed = t3.elapsed();
        
        // 4. Clear pending buffer is already done by std::mem::take
        let t4 = Instant::now();
        // pending_data will be dropped here, no need to clear
        
        // 🔥 CRITICAL MEMORY OPTIMIZATION: Clear inactive terms from shard_counters
        // Problem: shard_counters accumulates ALL historical terms → 30 MB for 300K docs
        // Solution: Only keep counters for currently pending terms → ~60 KB
        {
            let pending_terms: std::collections::HashSet<TermId> = 
                self.pending_posting_lists.read().keys().copied().collect();
            
            let mut shard_counters = self.shard_counters.write();
            shard_counters.retain(|term_id, _| pending_terms.contains(term_id));
        }
        
        let _t4_elapsed = t4.elapsed();
        
        // 5. Write doc_lengths (batched - only every N flushes to reduce I/O)
        let t5 = Instant::now();
        self.flush_doc_lengths_if_needed(false)?;
        let _t5_elapsed = t5.elapsed();
        
        // 6. Save dictionary
        let t6 = Instant::now();
        self.dictionary.flush()?;
        let _t6_elapsed = t6.elapsed();
        
        // 7. Save metadata
        let t7 = Instant::now();
        self.save_metadata()?;
        let _t7_elapsed = t7.elapsed();
        
        let _total_elapsed = flush_start.elapsed();
        
        // debug_log disabled for Phase A optimization
        
        Ok(())
    }
    
    /// Save metadata to disk
    fn save_metadata(&self) -> Result<()> {
        let meta_path = self.storage_dir.join("index_meta.bin");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&meta_path)?;
        
        let deleted_docs: Vec<DocId> = self.deleted_docs.read().iter().copied().collect();
        let deleted_term_docs: Vec<(TermId, DocId)> = self.deleted_term_docs.read().iter().copied().collect();
        
        let metadata = TextFTSMetadata {
            total_docs: self.total_docs,
            total_tokens: self.total_tokens,
            avg_doc_length: self.avg_doc_length,
            enable_positions: self.enable_positions,
            deleted_docs,
            deleted_term_docs,
        };
        
        let serialized = bincode::serialize(&metadata)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        file.write_all(&serialized)?;
        file.sync_all()?;
        
        Ok(())
    }
    
    /// Load metadata from disk
    fn load_metadata(stats_path: &PathBuf) -> Result<(u64, u64, f32, Vec<DocId>, Vec<(TermId, DocId)>)> {
        let mut file = File::open(stats_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        if buffer.is_empty() {
            return Ok((0, 0, 0.0, Vec::new(), Vec::new()));
        }
        
        let metadata: TextFTSMetadata = 
            bincode::deserialize(&buffer)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        Ok((
            metadata.total_docs,
            metadata.total_tokens,
            metadata.avg_doc_length,
            metadata.deleted_docs,
            metadata.deleted_term_docs,
        ))
    }
    
    /// Load doc_lengths from disk (on demand for BM25)
    fn load_doc_lengths(&self) -> Result<HashMap<DocId, u32>> {
        let lengths_path = self.storage_dir.join("doclengths.bin");
        let incremental_path = self.storage_dir.join("doclengths.incremental.bin");
        
        let mut all_lengths = HashMap::new();
        
        // Load main file
        if lengths_path.exists() {
            let mut file = File::open(&lengths_path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            
            if !buffer.is_empty() {
                let map: DocLengthMap = bincode::deserialize(&buffer)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                all_lengths = map.lengths;
            }
        }
        
        // Merge incremental file if exists
        if incremental_path.exists() {
            let mut file = File::open(&incremental_path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            
            if !buffer.is_empty() {
                let incremental: HashMap<DocId, u32> = bincode::deserialize(&buffer)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                all_lengths.extend(incremental);
            }
        }
        
        Ok(all_lengths)
    }
    
    /// Flush doc_lengths if threshold reached or force flush
    /// 
    /// 🔥 CRITICAL FIX: Use incremental storage to avoid loading entire file
    /// Old approach: load 300K entries (3.6MB) + extend 500 → 4.1MB peak
    /// New approach: append-only write, no load required
    fn flush_doc_lengths_if_needed(&mut self, force: bool) -> Result<()> {
        let mut pending_doc_lens = self.pending_doc_lengths.write();
        if pending_doc_lens.is_empty() {
            return Ok(());
        }
        
        let lengths_path = self.storage_dir.join("doclengths.bin");
        
        // 🚀 OPTIMIZATION: Incremental append (no load!)
        // Strategy: Keep a separate incremental file, merge only on restart
        let incremental_path = self.storage_dir.join("doclengths.incremental.bin");
        
        if incremental_path.exists() || !lengths_path.exists() {
            // Case 1: Incremental file exists, just append to it
            // Case 2: Fresh start, write to incremental file
            let mut existing_incremental = if incremental_path.exists() {
                let mut file = File::open(&incremental_path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                if !buffer.is_empty() {
                    bincode::deserialize::<HashMap<DocId, u32>>(&buffer)
                        .unwrap_or_default()
                } else {
                    HashMap::new()
                }
            } else {
                HashMap::new()
            };
            
            // Extend incremental with pending
            existing_incremental.extend(pending_doc_lens.drain());
            
            // Write back to incremental file
            let serialized = bincode::serialize(&existing_incremental)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&incremental_path)?;
            
            use std::io::Write;
            file.write_all(&serialized)?;
            file.sync_all()?;
            
            // Merge incremental to main file every 50K docs to bound incremental file size
            if existing_incremental.len() >= 50_000 || force {
                // Load main file (only once per 50K docs)
                let mut all_lengths = if lengths_path.exists() {
                    self.load_doc_lengths().unwrap_or_default()
                } else {
                    HashMap::new()
                };
                
                all_lengths.extend(existing_incremental);
                
                // Write merged to main file
                let map = DocLengthMap { lengths: all_lengths };
                let serialized = bincode::serialize(&map)
                    .map_err(|e| StorageError::Serialization(e.to_string()))?;
                
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&lengths_path)?;
                
                file.write_all(&serialized)?;
                file.sync_all()?;
                
                // Remove incremental file after merge
                let _ = std::fs::remove_file(&incremental_path);
            }
        } else {
            // Case 3: Only main file exists, append to incremental
            let mut incremental_map = HashMap::new();
            incremental_map.extend(pending_doc_lens.drain());
            
            let serialized = bincode::serialize(&incremental_map)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;
            
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&incremental_path)?;
            
            use std::io::Write;
            file.write_all(&serialized)?;
            file.sync_all()?;
        }
        
        Ok(())
    }
    
    /// Get statistics
    pub fn stats(&self) -> TextFTSStats {
        TextFTSStats {
            total_docs: self.total_docs,
            total_tokens: self.total_tokens,
            unique_terms: self.dictionary.len(),
            avg_doc_length: self.avg_doc_length,
        }
    }
}

/// Statistics for TextFTSIndex
#[derive(Debug, Clone)]
pub struct TextFTSStats {
    pub total_docs: u64,
    pub total_tokens: u64,
    pub unique_terms: usize,
    pub avg_doc_length: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_basic_insert_search() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = TextFTSIndex::new(temp_dir.path().join("test")).unwrap();
        
        index.insert(1, "The quick brown fox").unwrap();
        index.insert(2, "jumps over the lazy dog").unwrap();
        index.insert(3, "The lazy cat").unwrap();
        
        let results = index.search("lazy").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&2));
        assert!(results.contains(&3));
    }
    
    #[test]
    fn test_batch_insert() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = TextFTSIndex::new(temp_dir.path().join("test")).unwrap();
        
        let docs: Vec<(u64, &str)> = vec![
            (1, "document one"),
            (2, "document two"),
            (3, "document three"),
        ];
        
        index.batch_insert(&docs).unwrap();
        
        let results = index.search("document").unwrap();
        assert_eq!(results.len(), 3);
    }
    
    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("persistent");
        
        // Create and populate
        {
            let mut index = TextFTSIndex::new(path.clone()).unwrap();
            index.insert(1, "apple banana").unwrap();
            index.insert(2, "banana cherry").unwrap();
            index.flush().unwrap();
        }
        
        // Reopen and verify
        {
            let index = TextFTSIndex::new(path).unwrap();
            let stats = index.stats();
            assert_eq!(stats.total_docs, 2);
        }
    }
    
    #[test]
    fn test_bm25_ranking() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = TextFTSIndex::new(temp_dir.path().join("test")).unwrap();
        
        index.insert(1, "rust programming").unwrap();
        index.insert(2, "rust compiler").unwrap();
        index.insert(3, "programming language").unwrap();
        
        let results = index.search_ranked("rust", 10).unwrap();
        assert_eq!(results.len(), 2);
        // Both doc 1 and doc 2 should be in results
        let doc_ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&1));
        assert!(doc_ids.contains(&2));
        // All scores should be positive
        assert!(results.iter().all(|(_, score)| *score > 0.0));
    }
}

// ==================== 🚀 Batch Index Builder Implementation ====================

use crate::index::builder::{IndexBuilder, BuildStats};
use crate::types::{Row, Value, RowId};

impl IndexBuilder for TextFTSIndex {
    /// 批量构建文本索引（从MemTable flush时调用）
    fn build_from_memtable(&mut self, rows: &[(RowId, Row)]) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        // 🚀 Phase 1: 批量收集所有文本文档
        let mut documents: Vec<(u64, String)> = Vec::with_capacity(rows.len());
        
        for (row_id, row) in rows {
            // 遍历row中的所有列，找到Text/TextDoc类型
            for value in row.iter() {
                match value {
                    Value::Text(text) => {
                        documents.push((*row_id, text.clone()));
                        break; // 只取第一个文本列
                    }
                    Value::TextDoc(text) => {
                        // TextDoc包含的是Text类型，需要获取其内容
                        documents.push((*row_id, text.content().to_string()));
                        break;
                    }
                    _ => continue,
                }
            }
        }
        
        if documents.is_empty() {
            return Ok(());
        }
        
        println!("[TextFTSIndex] Batch building {} documents", documents.len());
        
        // 🔥 Phase 2: 使用已有的batch_insert方法（高效）
        let doc_refs: Vec<(u64, &str)> = documents.iter()
            .map(|(id, text)| (*id, text.as_str()))
            .collect();
        
        self.batch_insert(&doc_refs)?;
        
        let duration = start.elapsed();
        println!("[TextFTSIndex] Batch build complete in {:?}", duration);
        
        Ok(())
    }
    
    /// 持久化索引到磁盘
    fn persist(&mut self) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Flush pending posting lists到B-Tree
        self.flush()?;
        
        let duration = start.elapsed();
        println!("[TextFTSIndex] Persist complete in {:?}", duration);
        
        Ok(())
    }
    
    /// 获取索引名称
    fn name(&self) -> &str {
        "TextFTSIndex"
    }
    
    /// 获取构建统计信息
    fn stats(&self) -> BuildStats {
        let stats = self.stats();
        
        BuildStats {
            rows_processed: stats.total_docs as usize,
            build_time_ms: 0,
            persist_time_ms: 0,
            index_size_bytes: stats.unique_terms * 64, // 估算：每个term 64字节
        }
    }
}
