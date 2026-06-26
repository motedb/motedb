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
    TermId, DocId, PostingList, PostingListFormat, Position,
    Tokenizer, WhitespaceTokenizer, BM25Config, FieldNormTable,
};
use crate::index::text_dictionary::ChunkedDictionary;
use crate::index::btree_generic::{GenericBTree, GenericBTreeConfig};
use lru::LruCache;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

/// Document ID type
pub type DocumentId = u64;

/// Type alias for the cached doc_lengths map
type DocLengthCache = Arc<RwLock<Option<Arc<HashMap<DocId, u8>>>>>;

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
    /// Bounded by LRU capacity to cap memory usage
    shard_counters: Arc<RwLock<LruCache<TermId, u32>>>,
    
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

    /// Cached doc_lengths (fieldnorm encoded, lazy loaded).
    /// Invalidated on insert/update/delete, rebuilt on next search.
    doc_length_cache: DocLengthCache,

    /// Deleted documents (tombstones)
    deleted_docs: Arc<RwLock<HashSet<DocId>>>,

    /// Deleted (term_id, doc_id) pairs (for update operations)
    /// Tracks which terms have been removed from which documents
    deleted_term_docs: Arc<RwLock<HashSet<(TermId, DocId)>>>,

    /// 🚀 Posting list cache: avoids re-reading posting lists from disk on
    /// every search. Bounded LRU to cap memory (256 entries × ~2KB = ~512KB).
    posting_cache: Arc<RwLock<LruCache<TermId, PostingList>>>,

    /// 🚀 Top-K results cache: (token_string) → Vec<(doc_id, score)>.
    /// Avoids re-scoring the entire posting list on repeated queries.
    /// Bounded LRU (128 entries × ~80 bytes = ~10KB).
    topk_cache: Arc<RwLock<LruCache<String, Vec<(DocumentId, f32)>>>>,
}

/// Metadata for text FTS index
#[derive(Serialize, Deserialize)]
struct TextFTSMetadata {
    total_docs: u64,
    total_tokens: u64,
    avg_doc_length: f32,
    enable_positions: bool,
    deleted_docs: Vec<DocId>,
    deleted_term_docs: Vec<(TermId, DocId)>,
}

/// Document length map
#[derive(Serialize, Deserialize, Clone)]
struct DocLengthMap {
    lengths: HashMap<DocId, u32>,
}

/// Internal cursor for WAND query processing.
struct TermCursorData {
    idf: f32,
    upper_bound: f32,
    /// Sorted (doc_id, tf) entries
    entries: Vec<(u32, u16)>,
    /// Current position within entries
    pos: usize,
}

impl TermCursorData {
    fn current_doc(&self) -> u32 {
        if self.pos >= self.entries.len() {
            u32::MAX // exhausted cursors sort to end
        } else {
            self.entries[self.pos].0
        }
    }

    fn current_tf(&self) -> u16 {
        if self.pos >= self.entries.len() { 0 } else { self.entries[self.pos].1 }
    }

    fn is_exhausted(&self) -> bool {
        self.pos >= self.entries.len()
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn seek(&mut self, target: u32) {
        // Binary search for first entry >= target
        if self.is_exhausted() { return; }
        if self.entries[self.pos].0 >= target { return; }
        match self.entries[self.pos..].binary_search_by_key(&target, |&(d, _)| d) {
            Ok(idx) => self.pos += idx,
            Err(idx) => self.pos += idx,
        }
    }
}

impl TextFTSIndex {
    /// Create a new text FTS index
    pub fn new(storage_path: PathBuf) -> Result<Self> {
        Self::with_config(
            storage_path,
            Arc::new(WhitespaceTokenizer::default()),
            true,   // enable positions for phrase query support
            4,
        )
    }
    
    /// Create with custom configuration
    pub fn with_config(
        storage_path: PathBuf,
        tokenizer: Arc<dyn Tokenizer>,
        enable_positions: bool,
        dict_cache_size: usize,
    ) -> Result<Self> {
        Self::with_config_and_lru_capacity(
            storage_path,
            tokenizer,
            enable_positions,
            dict_cache_size,
            NonZeroUsize::new(50_000).unwrap(),
        )
    }

    /// Create with custom configuration and explicit LRU capacity for shard counters
    pub fn with_config_and_lru_capacity(
        storage_path: PathBuf,
        tokenizer: Arc<dyn Tokenizer>,
        enable_positions: bool,
        dict_cache_size: usize,
        shard_lru_capacity: NonZeroUsize,
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
            cache_size: 128,  // 🚀 P0: 降低到128 pages (1MB)，原192页(1.5MB)
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
            shard_counters: Arc::new(RwLock::new(LruCache::new(shard_lru_capacity))),
            tokenizer,
            bm25_config: BM25Config::default(),
            enable_positions,
            total_docs,
            total_tokens,
            avg_doc_length,
            pending_doc_lengths: Arc::new(RwLock::new(HashMap::new())),
            doc_length_cache: Arc::new(RwLock::new(None)),
            deleted_docs: Arc::new(RwLock::new(deleted_docs)),
            deleted_term_docs: Arc::new(RwLock::new(deleted_term_docs)),
            posting_cache: Arc::new(RwLock::new(LruCache::new(std::num::NonZeroUsize::new(256).unwrap()))),
            topk_cache: Arc::new(RwLock::new(LruCache::new(std::num::NonZeroUsize::new(128).unwrap()))),
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

        const AUTO_FLUSH_THRESHOLD_TERMS: usize = 200;
        const AUTO_FLUSH_THRESHOLD_DOCS: usize = 2000;

        {
            let pending_terms = self.pending_posting_lists.read().len();
            let pending_docs = self.pending_doc_lengths.read().len();

            if pending_terms >= AUTO_FLUSH_THRESHOLD_TERMS || pending_docs >= AUTO_FLUSH_THRESHOLD_DOCS {
                self.flush()?;
                self.cleanup_shard_counters();
            }
        }
        
        let _batch_start = Instant::now();

        // 1. Tokenization phase
        let _t1 = Instant::now();
        let mut batch_token_count = 0u64;
        
        // Build per-term doc lists (lightweight intermediate structure)
        let mut term_docs: HashMap<TermId, Vec<(DocId, Option<Position>)>> = HashMap::new();
        let mut doc_lengths_batch = HashMap::new();

        for &(doc_id, text) in docs {
            let tokens = self.tokenizer.tokenize(text);
            doc_lengths_batch.insert(doc_id, tokens.len() as u32);
            batch_token_count += tokens.len() as u64;

            for token in tokens {
                let term_id = self.dictionary.get_or_insert(&token.text);
                let pos = if self.enable_positions { Some(token.position) } else { None };
                term_docs.entry(term_id).or_default().push((doc_id, pos));
            }
        }

        // 2. Accumulate in pending buffer
        let mut pending = self.pending_posting_lists.write();

        for (term_id, doc_entries) in term_docs {
            let posting = pending.entry(term_id).or_insert_with(|| {
                PostingList::new_without_positions(!self.enable_positions)
            });

            for (doc_id, pos) in doc_entries {
                posting.add(doc_id, pos);
            }
        }
        
        // 🚀 P0 CRITICAL FIX: 检查是否需要自动flush（防止内存无限增长）
        let should_auto_flush = pending.len() >= 5000;
        drop(pending);
        
        // Accumulate doc_lengths in memory
        let mut pending_doc_lens = self.pending_doc_lengths.write();
        pending_doc_lens.extend(doc_lengths_batch);
        drop(pending_doc_lens);

        // 3. Update statistics — each document in batch is a new document.
        // No need to track known_docs: callers guarantee insert is for new rows,
        // updates go through the update() API, not insert().
        self.total_docs += docs.len() as u64;
        self.total_tokens += batch_token_count;

        if self.total_docs > 0 {
            self.avg_doc_length = self.total_tokens as f32 / self.total_docs as f32;
        }

        // Invalidate doc length cache
        *self.doc_length_cache.write() = None;
        self.topk_cache.write().clear();
        
        // ✅ 自动flush（每5000个term触发一次）
        if should_auto_flush {
            self.flush()?;
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
        // Mark as deleted (skip stats if already deleted)
        let already_deleted = self.deleted_docs.read().contains(&doc_id);
        self.deleted_docs.write().insert(doc_id);

        if already_deleted {
            return Ok(());
        }

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

        // Invalidate doc length cache
        *self.doc_length_cache.write() = None;
        self.topk_cache.write().clear();

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

        // Invalidate doc length cache
        *self.doc_length_cache.write() = None;
        self.topk_cache.write().clear();

        Ok(())
    }

    /// Load posting list from all shards (helper function)
    fn load_posting_list_sharded(&self, term_id: TermId, btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>) -> Result<Option<PostingList>> {
        let mut merged = PostingList::new_without_positions(true);
        let mut found_any = false;

        // Extract base term_id (lower 24 bits)
        let base_term_id = term_id & 0x00FFFFFF;

        // Use shard_counters LRU to know exactly how many shards exist;
        // on cache miss, probe the BTree to discover the shard count.
        let max_shard_idx = {
            let counters = self.shard_counters.read();
            if let Some(&count) = counters.peek(&term_id) {
                count
            } else {
                drop(counters);
                let count = self.discover_shard_count(term_id, &*btree)?;
                let mut counters = self.shard_counters.write();
                counters.put(term_id, count);
                count
            }
        };

        // Only scan known shards (not 0-256!)
        for shard_idx in 0..max_shard_idx {
            let shard_key = (shard_idx << 24) | base_term_id;
            match btree.get(&shard_key)? {
                Some(bytes) if !bytes.is_empty() => {
                    // Detect format: block (0x42, 0x50) or legacy RoaringBitmap
                    match PostingListFormat::deserialize(&bytes) {
                        Ok(PostingListFormat::Block(block)) => {
                            // Debug: verify block decode
                            // Convert block format to legacy PostingList for merging
                            let mut cursor = block.cursor();
                            while cursor.is_valid() {
                                let doc_id = cursor.current_doc() as u64;
                                let tf = cursor.current_tf();
                                merged.add_with_freq(doc_id, None, tf);
                                cursor.advance();
                            }
                        }
                        Ok(PostingListFormat::Legacy(shard)) => {
                            merged.merge(&shard);
                        }
                        Err(_) => continue,
                    }
                    found_any = true;
                }
                _ => continue,
            }
        }

        if found_any {
            Ok(Some(merged))
        } else {
            Ok(None)
        }
    }

    /// Load posting list with positions (for phrase queries).
    /// Loads the regular posting list then fetches positions from shard 0xFE.
    fn load_posting_list_with_positions(&self, term_id: TermId, btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>) -> Result<Option<PostingList>> {
        let mut posting = match self.load_posting_list_sharded(term_id, btree)? {
            Some(p) => p,
            None => return Ok(None),
        };
        let base_term_id = term_id & 0x00FFFFFF;
        let pos_key = (0xFEu32 << 24) | base_term_id;
        if let Some(bytes) = btree.get(&pos_key)? {
            posting.load_positions(&bytes);
        }
        Ok(Some(posting))
    }

    /// Discover shard count for a term by probing the BTree.
    ///
    /// Scans keys in range [base_term_id, (0xFE << 24) | base_term_id] and
    /// counts how many distinct shard indices exist (shard 0..0xFE, excluding
    /// the position key at shard 0xFE).
    fn discover_shard_count(
        &self,
        term_id: TermId,
        btree: &GenericBTree<u32>,
    ) -> Result<u32> {
        let base_term_id = term_id & 0x00FFFFFF;
        let range_start = base_term_id; // shard 0 key
        let range_end = (0xFEu32 << 24) | base_term_id; // position shard key (inclusive bound)

        let entries = btree.range(&range_start, &range_end)?;

        let mut max_shard_idx: u32 = 0;
        for (key, _) in &entries {
            let shard_idx = *key >> 24;
            // Only count data shards (0..0xFE), skip position shard (0xFE)
            if shard_idx < 0xFE && shard_idx + 1 > max_shard_idx {
                max_shard_idx = shard_idx + 1;
            }
        }

        Ok(max_shard_idx)
    }
    
    /// Search for documents containing query terms
    pub fn search(&self, query: &str) -> Result<Vec<DocumentId>> {
        let tokens = self.tokenizer.tokenize(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        
        let pending = self.pending_posting_lists.read();
        let btree = self.btree.read();
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
                    if let Some(p) = self.load_posting_list_sharded(term_id, &btree)? {
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
    
    /// Search for documents containing an exact phrase (consecutive token positions).
    ///
    /// E.g., search_phrase("machine learning") returns only docs where "machine"
    /// appears at position N and "learning" at position N+1.
    pub fn search_phrase(&self, phrase: &str) -> Result<Vec<DocumentId>> {
        let tokens = self.tokenizer.tokenize(phrase);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let pending = self.pending_posting_lists.read();
        let btree = self.btree.read();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();

        // Load posting lists for all phrase tokens (with positions for phrase matching)
        let mut postings: Vec<(TermId, PostingList)> = Vec::new();
        for token in &tokens {
            if let Some(term_id) = self.dictionary.get(&token.text) {
                let posting = if let Some(pend) = pending.get(&term_id) {
                    pend.clone()
                } else if let Some(p) = self.load_posting_list_with_positions(term_id, &btree)? {
                    p
                } else {
                    return Ok(Vec::new()); // Term not found → phrase cannot match
                };
                postings.push((term_id, posting));
            } else {
                return Ok(Vec::new());
            }
        }

        // Single-token phrase: just return docs containing it
        if postings.len() == 1 {
            let term_id = postings[0].0;
            let mut doc_ids = postings.into_iter().next().unwrap().1.doc_ids();
            doc_ids.retain(|id| {
                let did = *id as DocId;
                !deleted.contains(&did) && !deleted_term_docs.contains(&(term_id, did))
            });
            return Ok(doc_ids);
        }

        postings.sort_by_key(|(_, p)| p.doc_count());

        let candidates = &postings[0].1;
        let mut result = Vec::new();

        'outer: for doc_id in candidates.doc_ids() {
            let doc_id = doc_id as DocId;
            if deleted.contains(&doc_id) {
                continue;
            }
            // Check if any term's association with this doc was deleted
            if postings.iter().any(|(tid, _)| deleted_term_docs.contains(&(*tid, doc_id))) {
                continue;
            }

            // Get positions of the first token in this doc
            let first_positions = match postings[0].1.get_positions(doc_id) {
                Some(positions) => positions,
                None => continue, // No position data → cannot verify phrase
            };

            'pos: for &start_pos in first_positions.iter() {
                // Check if every subsequent token appears at start_pos + offset
                for (offset, (_, posting)) in postings.iter().enumerate().skip(1) {
                    let expected_pos = start_pos + offset as u32;
                    match posting.get_positions(doc_id) {
                        Some(positions) => {
                            if !positions.contains(&expected_pos) {
                                continue 'pos;
                            }
                        }
                        None => continue 'outer,
                    }
                }
                // All tokens matched at consecutive positions
                result.push(doc_id);
                continue 'outer;
            }
        }

        Ok(result)
    }

    /// 🚀 Fast single-term search: score all docs for one term, return top-K.
    /// O(N) but with minimal overhead — no WAND, no cursor merge, no Vec cloning.
    /// ~10x faster than WAND for single-term queries (the common case).
    fn search_single_term(&self, token: &str, top_k: usize) -> Result<Vec<(DocumentId, f32)>> {
        // 🚀 Top-K result cache: return cached results for repeated queries.
        // Cache key includes top_k to handle different LIMIT values.
        let cache_key = format!("{}:{}", token, top_k);
        {
            let mut tc = self.topk_cache.write();
            if let Some(cached) = tc.get(&cache_key).cloned() {
                return Ok(cached);
            }
        }

        let term_id = match self.dictionary.get(token) {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let doc_lengths = self.get_doc_lengths_cached()?;
        let avg_dl = if self.avg_doc_length > 0.0 { self.avg_doc_length } else { 1.0 };
        let k1 = self.bm25_config.k1;
        let b = self.bm25_config.b;
        let total_docs = self.total_docs as f32;

        // Load posting list (cache > pending > disk).
        // For cached posting lists, use the cached decoded pairs directly.
        let pairs: Vec<(u32, u16)>;
        let df: u64;
        {
            let pending = self.pending_posting_lists.read();
            if let Some(pend) = pending.get(&term_id) {
                pairs = pend.iter_doc_tf();
                df = pend.doc_count();
            } else {
                drop(pending);
                // Check decoded-pair cache first (avoids posting list clone).
                let pair_cache_key = term_id;
                let mut pc = self.posting_cache.write();
                if let Some(cached_pl) = pc.get(&pair_cache_key) {
                    pairs = cached_pl.iter_doc_tf_cached_ref();
                    df = cached_pl.doc_count();
                } else {
                    drop(pc);
                    let btree = self.btree.read();
                    if let Some(p) = self.load_posting_list_sharded(term_id, &btree)? {
                        let pl_clone = p.clone();
                        self.posting_cache.write().put(term_id, p);
                        pairs = pl_clone.iter_doc_tf_cached_ref();
                        df = pl_clone.doc_count();
                    } else {
                        return Ok(Vec::new());
                    }
                }
            }
        }

        if df == 0 { return Ok(Vec::new()); }

        let idf = ((total_docs - df as f32 + 0.5) / (df as f32 + 0.5) + 1.0).ln();

        // Score all docs, maintain a bounded min-heap of top-K.
        let deleted = self.deleted_docs.read();
        let deleted_td = self.deleted_term_docs.read();
        let deleted_empty = deleted.is_empty() && deleted_td.is_empty();

        // Use a simple Vec + partial sort for small top_k (faster than BinaryHeap).
        let mut scored: Vec<(DocumentId, f32)> = Vec::with_capacity(pairs.len());

        for (doc_id_u32, tf) in pairs {
            if tf == 0 { continue; }
            let doc_id = doc_id_u32 as DocumentId;
            if !deleted_empty {
                if deleted.contains(&doc_id) { continue; }
                if deleted_td.contains(&(term_id, doc_id)) { continue; }
            }
            let dl = doc_lengths.get(&doc_id).copied().unwrap_or(1) as f32;
            let norm = 1.0 - b + b * (dl / avg_dl);
            let score = idf * (tf as f32 * (k1 + 1.0)) / (tf as f32 + k1 * norm);
            scored.push((doc_id, score));
        }

        // Partial sort: get top-K by score descending.
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);

        // Cache for repeated queries.
        self.topk_cache.write().put(cache_key, scored.clone());

        Ok(scored)
    }

    /// Search with BM25 ranking using WAND (Weak AND) for top-K early termination.
    ///
    /// WAND skips documents that cannot make it into the top-K results by
    /// maintaining a score threshold and using per-term upper bounds.
    pub fn search_ranked(&self, query: &str, top_k: usize) -> Result<Vec<(DocumentId, f32)>> {
        let tokens = self.tokenizer.tokenize(query);
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let mut unique_tokens: Vec<_> = tokens.iter().map(|t| t.text.clone()).collect();
        unique_tokens.sort();
        unique_tokens.dedup();

        // 🚀 Fast path for single-term queries (most common case).
        // Skip WAND overhead — just score all docs for this term and return top-K.
        if unique_tokens.len() == 1 {
            return self.search_single_term(&unique_tokens[0], top_k);
        }

        let doc_lengths = self.get_doc_lengths_cached()?;
        let avg_dl = if self.avg_doc_length > 0.0 { self.avg_doc_length } else { 1.0 };

        let pending = self.pending_posting_lists.read();
        let btree = self.btree.read();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();

        let k1 = self.bm25_config.k1;
        let b = self.bm25_config.b;
        let total_docs = self.total_docs as f32;

        // Build term cursors: for each token, load posting list and compute IDF + upper bound
        let mut cursors: Vec<TermCursorData> = Vec::new();
        for token_text in &unique_tokens {
            let term_id = match self.dictionary.get(token_text) {
                Some(id) => id,
                None => {
                    continue;
                }
            };

            // Load posting list (cache > pending > disk)
            let posting = if let Some(pend) = pending.get(&term_id) {
                pend.clone()
            } else if let Some(cached) = self.posting_cache.write().get(&term_id).cloned() {
                // 🚀 Cache hit — skip disk I/O entirely.
                cached
            } else if let Some(p) = self.load_posting_list_sharded(term_id, &btree)? {
                // Cache miss — load from disk, then cache for future queries.
                self.posting_cache.write().put(term_id, p.clone());
                p
            } else {
                continue;
            };

            let df = posting.doc_count() as f32;
            if df == 0.0 { continue; }
            // BM25 IDF: non-negative variant (Lucene-compatible). The +1 ensures
            // terms appearing in every document still get a small positive weight.
            let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();

            // Compute max possible BM25 score for this term (upper bound)
            let max_tf = posting.max_tf();
            let upper_bound = {
                let tf = max_tf as f32;
                // Use dl=1 as the minimum possible doc length for a tighter bound
                let min_norm = 1.0 - b + b / self.avg_doc_length.max(1.0);
                idf * (tf * (k1 + 1.0)) / (tf + k1 * min_norm)
            };

            // Collect non-deleted doc_ids with their TFs (using cached iterator).
            // 🚀 Fast path: skip all deletion checks when sets are empty.
            let pairs = posting.iter_doc_tf_cached_ref();
            let deleted_empty = deleted.is_empty();
            let deleted_td_empty = deleted_term_docs.is_empty();
            let mut entries: Vec<(u32, u16)> = if deleted_empty && deleted_td_empty {
                // No deletions at all — skip all checks (100x faster).
                pairs.into_iter().filter(|&(_, tf)| tf > 0).collect()
            } else {
                let mut e: Vec<(u32, u16)> = Vec::with_capacity(pairs.len());
                for (doc_id_u32, tf) in pairs {
                    let doc_id = doc_id_u32 as DocId;
                    if deleted.contains(&doc_id) { continue; }
                    if deleted_term_docs.contains(&(term_id, doc_id)) { continue; }
                    if tf > 0 { e.push((doc_id_u32, tf)); }
                }
                e
            };

            if entries.is_empty() {
                continue;
            }
            // Posting lists are already sorted by doc_id (RoaringBitmap iterates
            // in order). Skip re-sorting — saves O(N log N) per term.
            // entries.sort_by_key(|&(d, _)| d);

            cursors.push(TermCursorData {
                idf,
                upper_bound,
                entries,
                pos: 0,
            });
        }

        if cursors.is_empty() {
            return Ok(Vec::new());
        }
        // Sort cursors by upper bound descending (for better pruning)
        cursors.sort_by(|a, b| b.upper_bound.partial_cmp(&a.upper_bound).unwrap());

        // WAND execution
        // Use ordered floats for the heap (Reverse<(OrderedFloat, u32)>)
        #[derive(Clone, PartialEq)]
        struct OrdF32(f32);
        impl Eq for OrdF32 {}
        impl PartialOrd for OrdF32 {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
        }
        impl Ord for OrdF32 {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
            }
        }

        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(OrdF32, u32)>> =
            std::collections::BinaryHeap::with_capacity(top_k + 1);
        let mut threshold = 0.0f32;

        // Calculate sum of all upper bounds
        let total_upper: f32 = cursors.iter().map(|c| c.upper_bound).sum();
        if total_upper < threshold {
            return Ok(Vec::new());
        }

        // Iterate using WAND pivot selection
        loop {
            // Sort cursors by current doc_id
            cursors.sort_by_key(|c| c.current_doc());

            // Find pivot: smallest p where sum(upper_bound[0..p+1]) >= threshold
            let mut prefix_sum = 0.0f32;
            let mut pivot_idx = None;
            for (i, cursor) in cursors.iter().enumerate() {
                if cursor.is_exhausted() { continue; }
                prefix_sum += cursor.upper_bound;
                if prefix_sum >= threshold {
                    pivot_idx = Some(i);
                    break;
                }
            }

            let pivot_idx = match pivot_idx {
                Some(idx) => idx,
                None => break, // No doc can exceed threshold
            };

            let pivot_doc = cursors[pivot_idx].current_doc();

            // Check if all cursors up to pivot are at pivot_doc
            let mut all_at_pivot = true;
            for cursor in &mut cursors[..=pivot_idx] {
                if cursor.is_exhausted() { continue; }
                if cursor.current_doc() < pivot_doc {
                    cursor.seek(pivot_doc);
                }
                if cursor.current_doc() != pivot_doc {
                    all_at_pivot = false;
                }
            }

            if !all_at_pivot {
                continue; // Re-sort and retry
            }

            // Score pivot document
            let doc_id = pivot_doc as DocId;
            let doc_len = doc_lengths.get(&doc_id).copied().unwrap_or(0);
            let dl_approx = FieldNormTable::decode(doc_len, avg_dl);
            let norm = 1.0 - b + b * (dl_approx / avg_dl);

            let mut score = 0.0f32;
            for cursor in &cursors {
                if !cursor.is_exhausted() && cursor.current_doc() == pivot_doc {
                    let tf = cursor.current_tf() as f32;
                    score += cursor.idf * (tf * (k1 + 1.0)) / (tf + k1 * norm);
                }
            }

            // Update heap
            if heap.len() < top_k {
                heap.push(std::cmp::Reverse((OrdF32(score), pivot_doc)));
                if heap.len() == top_k {
                    threshold = heap.peek().map(|h| h.0.0.0).unwrap_or(0.0);
                }
            } else if score > threshold {
                heap.pop();
                heap.push(std::cmp::Reverse((OrdF32(score), pivot_doc)));
                threshold = heap.peek().map(|h| h.0.0.0).unwrap_or(0.0);
            }

            // Advance all cursors at pivot_doc
            for cursor in &mut cursors {
                if !cursor.is_exhausted() && cursor.current_doc() == pivot_doc {
                    cursor.advance();
                }
            }

            // Check if any active cursors remain
            if cursors.iter().all(|c| c.is_exhausted()) {
                break;
            }
        }

        // Extract results
        let mut results: Vec<(DocumentId, f32)> = heap.into_iter()
            .map(|std::cmp::Reverse((OrdF32(score), doc))| (doc as DocId, score))
            .collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        Ok(results)
    }

    /// Get doc_lengths with caching (fieldnorm encoded).
    fn get_doc_lengths_cached(&self) -> Result<Arc<HashMap<DocId, u8>>> {
        {
            let cache = self.doc_length_cache.read();
            if let Some(ref cached) = *cache {
                return Ok(cached.clone());
            }
        }

        // Cache miss: load from disk + merge pending
        let mut doc_lengths = self.load_doc_lengths()?;
        {
            let pending_dl = self.pending_doc_lengths.read();
            doc_lengths.extend(pending_dl.iter().map(|(&k, &v)| (k, v)));
        }

        let avg_dl = if self.avg_doc_length > 0.0 { self.avg_doc_length } else { 1.0 };
        let encoded: HashMap<DocId, u8> = doc_lengths.into_iter()
            .map(|(k, v)| (k, FieldNormTable::encode(v, avg_dl)))
            .collect();

        let arc = Arc::new(encoded);
        *self.doc_length_cache.write() = Some(arc.clone());
        Ok(arc)
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
        
        // 2. Write to BTree using block format
        let t2 = Instant::now();
        let mut btree = self.btree.write();
        let mut shard_counters = self.shard_counters.write();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();

        for (term_id, posting) in pending_data.iter() {
            let base_term_id = *term_id & 0x00FFFFFF;
            // If the shard counter was evicted from LRU, discover the actual
            // count from the BTree to avoid overwriting consolidated shard 0.
            let next_shard_idx = match shard_counters.get(term_id) {
                Some(idx) => *idx,
                None => {
                    let discovered = self.discover_shard_count(
                        *term_id, &*btree).unwrap_or(0);
                    shard_counters.put(*term_id, discovered);
                    discovered
                }
            };

            // Remove tombstoned docs from the new posting only
            let doc_ids = posting.doc_ids();
            let mut clean_ids: Vec<u32> = Vec::new();
            let mut clean_tfs: Vec<u16> = Vec::new();
            for &doc_id_u64 in &doc_ids {
                let doc_id = doc_id_u64 as DocId;
                if deleted.contains(&doc_id) { continue; }
                if deleted_term_docs.contains(&(*term_id, doc_id)) { continue; }
                clean_ids.push(doc_id_u64 as u32);
                clean_tfs.push(posting.term_frequency(doc_id));
            }

            if clean_ids.is_empty() { continue; }

            // Encode as block format
            let block_list = super::text_types::BlockPostingList::from_sorted_pairs(&clean_ids, &clean_tfs);
            let bytes = block_list.as_bytes();

            // Append-only: write as a new shard (no merge with existing shards)
            let shard_key = (next_shard_idx << 24) | base_term_id;
            btree.insert(shard_key, bytes.to_vec())?;

            shard_counters.put(*term_id, next_shard_idx + 1);

            // Write positions to separate key (shard 0xFE) for phrase query support
            let pos_key = (0xFEu32 << 24) | base_term_id;
            if let Some(pos_bytes) = posting.serialize_positions_for(&clean_ids) {
                btree.insert(pos_key, pos_bytes)?;
            } else {
                let _ = btree.delete(&pos_key);
            }

            // Lazy consolidation: merge shards when count exceeds threshold
            if next_shard_idx + 1 >= 5 {
                if let Err(e) = self.consolidate_shards_for_term(&mut btree, &mut shard_counters, *term_id) {
                    debug_log!("[FTS] Shard consolidation failed for term {}: {}", term_id, e);
                }
            }
        }

        drop(deleted);
        drop(deleted_term_docs);
        
        drop(shard_counters);
        let _t2_elapsed = t2.elapsed();
        
        // 3. Flush BTree
        let t3 = Instant::now();
        btree.flush()?;
        drop(btree);
        let _t3_elapsed = t3.elapsed();
        
        // 4. ✅ P0 CRITICAL FIX: 完全清空所有内存buffer（释放capacity）
        let t4 = Instant::now();
        
        // pending_data will be dropped here
        drop(pending_data);
        
        // 🔥 P0 FIX: 强制清空所有HashMap，释放capacity
        {
            let mut pending = self.pending_posting_lists.write();
            *pending = HashMap::new();  // 完全替换（capacity归零）
        }
        
        {
            // NOTE: Do NOT clear shard_counters! They are needed for
            // load_posting_list_sharded() to know how many shards exist per term.
            // Clearing would cause data loss (only shard 0 would be read after flush).
            // LRU automatically evicts cold entries when capacity is exceeded.
            let _counters = self.shard_counters.write();
        }
        
        {
            let mut doc_lens = self.pending_doc_lengths.write();
            doc_lens.clear();
            doc_lens.shrink_to_fit();  // 释放capacity
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
        
        let _total_elapsed = flush_start.elapsed();// debug_log disabled for Phase A optimization
        
        Ok(())
    }
    
    /// No-op now that shard_counters is bounded by LRU capacity.
    /// Kept as a stub for call-site compatibility.
    fn cleanup_shard_counters(&self) {
        // LRU handles eviction automatically; no manual cleanup needed.
    }
    
    /// Save metadata to disk (prunes deleted_term_docs for fully-deleted docs)
    fn save_metadata(&self) -> Result<()> {
        // Prune: remove deleted_term_docs entries whose doc is already in deleted_docs
        {
            let deleted_docs = self.deleted_docs.read();
            if !deleted_docs.is_empty() {
                let mut dtd = self.deleted_term_docs.write();
                let before = dtd.len();
                dtd.retain(|(_, doc_id)| !deleted_docs.contains(doc_id));
                let pruned = before - dtd.len();
                if pruned > 100 {
                    debug_log!("[FTS] Pruned {} stale deleted_term_docs entries", pruned);
                }
            }
        }

        let meta_path = self.storage_dir.join("index_meta.bin");
        let tmp_path = self.storage_dir.join("index_meta.bin.tmp");

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        
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
        drop(file);

        // Atomic rename for crash safety
        std::fs::rename(&tmp_path, &meta_path).map_err(StorageError::Io)?;

        Ok(())
    }
    
    /// Load metadata from disk
    #[allow(clippy::type_complexity)]
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
        // Format: repeated [len:u32 LE][bincode(HashMap<DocId, u32>)]
        if incremental_path.exists() {
            let mut file = File::open(&incremental_path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            if !buffer.is_empty() {
                let mut cursor = std::io::Cursor::new(&buffer);
                while cursor.position() < buffer.len() as u64 {
                    let mut len_bytes = [0u8; 4];
                    if cursor.read_exact(&mut len_bytes).is_err() {
                        break;
                    }
                    let block_len = u32::from_le_bytes(len_bytes) as usize;
                    let start = cursor.position() as usize;
                    let end = start + block_len;
                    if end > buffer.len() {
                        break;
                    }
                    if let Ok(block) = bincode::deserialize::<HashMap<DocId, u32>>(&buffer[start..end]) {
                        all_lengths.extend(block);
                    }
                    cursor.set_position(end as u64);
                }
            }
        }
        
        Ok(all_lengths)
    }
    
    /// Flush doc_lengths if threshold reached or force flush
    /// 
    /// 🔥 P0 CRITICAL FIX: Use append-only incremental writes to avoid memory explosion
    /// Old approach: load ALL 680K entries (8+ MB) on every flush → OOM
    /// New approach: append new entries to incremental file → O(pending size) memory
    /// Merge all shards for a term into a single shard (lazy consolidation)
    fn consolidate_shards_for_term(
        &self,
        btree: &mut parking_lot::RwLockWriteGuard<'_, crate::index::btree_generic::GenericBTree<u32>>,
        shard_counters: &mut parking_lot::RwLockWriteGuard<'_, LruCache<TermId, u32>>,
        term_id: TermId,
    ) -> Result<()> {
        let base_term_id = term_id & 0x00FFFFFF;

        let shard_count = {
            if let Some(&count) = shard_counters.peek(&term_id) {
                count
            } else {
                let range_start = base_term_id;
                let range_end = (0xFEu32 << 24) | base_term_id;
                let entries = btree.range(&range_start, &range_end)?;
                let mut max_shard: u32 = 0;
                for (key, _) in &entries {
                    let shard_idx = *key >> 24;
                    if shard_idx < 0xFE && shard_idx + 1 > max_shard {
                        max_shard = shard_idx + 1;
                    }
                }
                shard_counters.put(term_id, max_shard);
                max_shard
            }
        };

        if shard_count <= 1 {
            return Ok(());
        }

        // Read and merge all shards
        let mut merged = PostingList::new();
        for shard_idx in 0..shard_count {
            let shard_key = (shard_idx << 24) | base_term_id;
            if let Ok(Some(bytes)) = btree.get(&shard_key) {
                if !bytes.is_empty() {
                    if super::text_types::BlockPostingList::is_block_format(&bytes) {
                        if let Ok(block_list) = super::text_types::BlockPostingList::deserialize(&bytes) {
                            let mut cursor = block_list.cursor();
                            while cursor.is_valid() {
                                merged.add_with_freq(cursor.current_doc() as u64, None, cursor.current_tf());
                                cursor.advance();
                            }
                        }
                    } else if let Ok(shard) = PostingList::deserialize_compact(&bytes) {
                        merged.merge(&shard);
                    }
                }
            }
        }

        // Filter deleted docs
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();
        let doc_ids = merged.doc_ids();
        let mut clean_ids: Vec<u32> = Vec::new();
        let mut clean_tfs: Vec<u16> = Vec::new();
        for &doc_id_u64 in &doc_ids {
            let doc_id = doc_id_u64 as DocId;
            if deleted.contains(&doc_id) { continue; }
            if deleted_term_docs.contains(&(term_id, doc_id)) { continue; }
            clean_ids.push(doc_id_u64 as u32);
            clean_tfs.push(merged.term_frequency(doc_id));
        }
        drop(deleted);
        drop(deleted_term_docs);

        if clean_ids.is_empty() {
            // Delete all shards
            for shard_idx in 0..shard_count {
                let _ = btree.delete(&(shard_idx << 24 | base_term_id));
            }
            shard_counters.put(term_id, 0);
            return Ok(());
        }

        // Write consolidated as shard 0
        let block_list = super::text_types::BlockPostingList::from_sorted_pairs(&clean_ids, &clean_tfs);
        let bytes = block_list.as_bytes();
        btree.insert(base_term_id, bytes.to_vec())?;

        // Delete old shards
        for shard_idx in 1..shard_count {
            let _ = btree.delete(&(shard_idx << 24 | base_term_id));
        }

        // Reset counter to 1
        shard_counters.put(term_id, 1);

        Ok(())
    }

    fn flush_doc_lengths_if_needed(&mut self, _force: bool) -> Result<()> {
        let mut pending_doc_lens = self.pending_doc_lengths.write();
        if pending_doc_lens.is_empty() {
            return Ok(());
        }
        
        // 🚀 P0 NEW: Append to incremental file instead of rewriting main file
        let incremental_path = self.storage_dir.join("doclengths.incremental.bin");
        
        // Serialize only the pending entries
        let pending_map = DocLengthMap { lengths: pending_doc_lens.drain().collect() };
        let serialized = bincode::serialize(&pending_map)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        // Append to incremental file
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incremental_path)?;
        
        use std::io::Write;
        // Write length prefix + data
        let len_bytes = (serialized.len() as u32).to_le_bytes();
        file.write_all(&len_bytes)?;
        file.write_all(&serialized)?;
        file.sync_all()?;
        
        // 🚀 P0 FIX: 释放HashMap capacity
        if pending_doc_lens.capacity() > 1024 {
            pending_doc_lens.shrink_to_fit();
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
#[allow(clippy::items_after_test_module)]
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

    #[test]
    fn test_total_docs_double_delete_no_underflow() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = TextFTSIndex::new(temp_dir.path().join("test")).unwrap();

        index.insert(1, "apple").unwrap();
        index.insert(2, "banana").unwrap();
        assert_eq!(index.stats().total_docs, 2);

        // First delete: decrements
        index.delete(1, "apple").unwrap();
        assert_eq!(index.stats().total_docs, 1);

        // Double-delete: should NOT decrement again
        index.delete(1, "apple").unwrap();
        assert_eq!(index.stats().total_docs, 1, "double-delete should not underflow total_docs");
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
                        documents.push((*row_id, text.to_string()));
                        break; // 只取第一个文本列
                    }
                    Value::TextDoc(text) => {
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
        
        debug_log!("[TextFTSIndex] Batch building {} documents", documents.len());
        
        // 🔥 Phase 2: 使用已有的batch_insert方法（高效）
        let doc_refs: Vec<(u64, &str)> = documents.iter()
            .map(|(id, text)| (*id, text.as_str()))
            .collect();
        
        self.batch_insert(&doc_refs)?;
        
        let duration = start.elapsed();
        debug_log!("[TextFTSIndex] Batch build complete in {:?}", duration);
        
        Ok(())
    }
    
    /// 持久化索引到磁盘
    fn persist(&mut self) -> Result<()> {
        use std::time::Instant;
        let start = Instant::now();
        
        // Flush pending posting lists到B-Tree
        self.flush()?;
        
        let duration = start.elapsed();
        debug_log!("[TextFTSIndex] Persist complete in {:?}", duration);
        
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
