//! Text Full-Text Search Index (Real B+Tree Implementation)
//!
//! Architecture:
//! - Real B+Tree storage using `GenericBTree<u32>` (zero-copy, in-place updates)
//! - Varint/Delta encoding for posting lists (space efficient)
//! - Segmented posting lists (handle large term frequencies)
//! - MemTable → Flush → B-Tree (simple data flow)
//! - No compaction needed (B+Tree handles updates in-place)

use crate::index::btree_generic::{GenericBTree, GenericBTreeConfig};
use crate::index::text_dictionary::ChunkedDictionary;
use crate::index::text_types::{
    BM25Config, DocId, FieldNormTable, Position, PostingList, PostingListFormat, TermId, Tokenizer,
    WhitespaceTokenizer,
};
use crate::{Result, StorageError};
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

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

    /// 🚀 Posting shard cache: per-term disk sources (block bytes shared via
    /// Arc, ~3 bits/doc compressed). Avoids re-reading + re-decoding posting
    /// shards from the B+Tree on every search. Invalidated on flush (a flush
    /// appends new shards / consolidates old ones).
    posting_cache: Arc<RwLock<LruCache<TermId, DiskPostings>>>,

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

/// One disk source for a term's postings — one B+Tree shard. Block-format
/// shards stay in compressed form (decoded 128 docs at a time by the
/// stream); legacy shards materialize to sorted pairs once.
#[derive(Clone)]
enum DiskSource {
    Block(Arc<super::text_types::BlockPostingList>),
    Pairs(Arc<[(u32, u16)]>),
}

/// Cached per-term disk sources (all shards).
type DiskPostings = Vec<DiskSource>;

/// Streaming merged, sorted (doc_id, tf) cursor over one term: the pending
/// posting (small, bounded by the flush threshold) plus the cached disk
/// shards. B1: replaces full posting materialization — the old path decoded
/// every doc into a PostingList via per-doc `add_with_freq`, then HashMap-
/// merged + re-sorted per query; this decodes one block at a time and only
/// the blocks actually visited. Doc dedup across sources keeps the FIRST
/// source's tf (pending wins — same precedence as the old merge).
struct TermStream {
    /// Ascending pair sources (pending first, then legacy shards).
    pairs: Vec<Arc<[(u32, u16)]>>,
    pair_pos: Vec<usize>,
    /// Block-format shard streams.
    blocks: Vec<super::text_types::BlockStream>,
    /// Total entries across sources (headers only — no decode needed).
    df: u64,
    /// Max tf across sources (pending scan + block skip tables).
    max_tf: u16,
}

impl TermStream {
    fn empty() -> Self {
        Self {
            pairs: Vec::new(),
            pair_pos: Vec::new(),
            blocks: Vec::new(),
            df: 0,
            max_tf: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.df == 0
    }

    /// Current (doc, tf) — the minimum head across sources; on ties the
    /// earliest source (pending) wins.
    fn current(&self) -> Option<(u32, u16)> {
        let mut best: Option<(u32, u16)> = None;
        for (i, src) in self.pairs.iter().enumerate() {
            if let Some(&(d, t)) = src.get(self.pair_pos[i]) {
                if best.is_none_or(|(bd, _)| d < bd) {
                    best = Some((d, t));
                }
            }
        }
        for b in &self.blocks {
            if let Some((d, t)) = b.current() {
                if best.is_none_or(|(bd, _)| d < bd) {
                    best = Some((d, t));
                }
            }
        }
        best
    }

    /// Advance past the current merged doc (single step).
    fn advance(&mut self) {
        if let Some((d, _)) = self.current() {
            self.advance_past(d);
        }
    }

    /// First merged doc >= target (monotonic only).
    fn seek(&mut self, target: u32) -> Option<(u32, u16)> {
        for (i, src) in self.pairs.iter().enumerate() {
            if self.pair_pos[i] < src.len() && src[self.pair_pos[i]].0 < target {
                self.pair_pos[i] = src.partition_point(|&(d, _)| d < target);
            }
        }
        for b in &mut self.blocks {
            if b.current().is_none_or(|(d, _)| d < target) {
                b.seek(target);
            }
        }
        self.current()
    }

    /// Move every source strictly past `doc` (monotonic only).
    fn advance_past(&mut self, doc: u32) {
        for (i, src) in self.pairs.iter().enumerate() {
            if self.pair_pos[i] < src.len() && src[self.pair_pos[i]].0 <= doc {
                self.pair_pos[i] = src.partition_point(|&(d, _)| d <= doc);
            }
        }
        for b in &mut self.blocks {
            if b.current().is_some_and(|(d, _)| d <= doc) {
                b.seek(doc + 1);
            }
        }
    }
}

/// Zig-zag AND intersection across term streams (FTS5/Lucene strategy):
/// the SMALLEST-df stream drives; every other stream gallops to the driver's
/// doc via monotonic seeks (block-granular, amortized one decode per block
/// across the whole query). Docs where all heads agree are the
/// intersection. O(min_df seeks + blocks touched), independent of the
/// largest posting's length. Deleted docs and (term, doc) tombstones are
/// skipped. `streams` and `term_ids` are reordered together (driver first).
fn intersect_streams(
    streams: &mut [TermStream],
    term_ids: &mut [TermId],
    deleted: &HashSet<DocId>,
    deleted_term_docs: &HashSet<(TermId, DocId)>,
    out: &mut Vec<u32>,
) {
    // Drive by the shortest posting.
    let mut order: Vec<usize> = (0..streams.len()).collect();
    order.sort_by_key(|&i| streams[i].df);
    let mut s2: Vec<TermStream> = Vec::with_capacity(streams.len());
    let mut t2: Vec<TermId> = Vec::with_capacity(term_ids.len());
    for i in order {
        s2.push(std::mem::replace(&mut streams[i], TermStream::empty()));
        t2.push(term_ids[i]);
    }
    for (i, s) in s2.into_iter().enumerate() {
        streams[i] = s;
    }
    for (i, t) in t2.into_iter().enumerate() {
        term_ids[i] = t;
    }

    'outer: loop {
        // Reach consensus: every stream at the same doc, or a strictly
        // increasing target until someone exhausts.
        let (mut target, _) = match streams[0].current() {
            Some(p) => p,
            None => break,
        };
        loop {
            let mut stable = true;
            for s in streams[1..].iter_mut() {
                if s.current().is_none_or(|(d, _)| d < target) {
                    s.seek(target);
                }
                match s.current() {
                    Some((d, _)) if d == target => {}
                    Some((d, _)) => {
                        target = d;
                        stable = false;
                    }
                    None => break 'outer,
                }
            }
            if stable {
                break;
            }
            // A follower jumped past target — pull the driver up and retry.
            if streams[0].current().is_none_or(|(d, _)| d < target) {
                streams[0].seek(target);
            }
            match streams[0].current() {
                Some((d, _)) if d == target => {}
                Some((d, _)) => target = d,
                None => break 'outer,
            }
        }

        let doc_id = target as DocId;
        let alive = !deleted.contains(&doc_id)
            && term_ids
                .iter()
                .all(|tid| !deleted_term_docs.contains(&(*tid, doc_id)));
        if alive {
            out.push(target);
        }
        streams[0].advance_past(target);
    }
}

impl TextFTSIndex {
    /// Create a new text FTS index
    pub fn new(storage_path: PathBuf) -> Result<Self> {
        Self::with_config(
            storage_path,
            Arc::new(WhitespaceTokenizer::default()),
            true, // enable positions for phrase query support
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
            cache_size: 128, // 🚀 P0: 降低到128 pages (1MB)，原192页(1.5MB)
            // Trade-off: -25% cache for strict memory constraint
            unique_keys: false,
            allow_updates: true,
            immediate_sync: false,
        };
        let btree = GenericBTree::<u32>::with_config(btree_path, btree_config)?;

        // Load statistics metadata
        let meta_path = storage_dir.join("index_meta.bin");
        let (total_docs, total_tokens, avg_doc_length, deleted_docs_vec, deleted_term_docs_vec) =
            if meta_path.exists() {
                Self::load_metadata(&meta_path)?
            } else {
                (0, 0, 0.0, Vec::new(), Vec::new())
            };

        // Convert deleted_docs from Vec to HashSet
        let deleted_docs: HashSet<DocId> = deleted_docs_vec.into_iter().collect();
        let deleted_term_docs: HashSet<(TermId, DocId)> =
            deleted_term_docs_vec.into_iter().collect();

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
            posting_cache: Arc::new(RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ))),
            topk_cache: Arc::new(RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(128).unwrap(),
            ))),
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

            if pending_terms >= AUTO_FLUSH_THRESHOLD_TERMS
                || pending_docs >= AUTO_FLUSH_THRESHOLD_DOCS
            {
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
                let pos = if self.enable_positions {
                    Some(token.position)
                } else {
                    None
                };
                term_docs.entry(term_id).or_default().push((doc_id, pos));
            }
        }

        // 2. Accumulate in pending buffer
        let mut pending = self.pending_posting_lists.write();

        for (term_id, doc_entries) in term_docs {
            let posting = pending
                .entry(term_id)
                .or_insert_with(|| PostingList::new_without_positions(!self.enable_positions));

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
        // 🔑 Carry token POSITIONS (mirrors batch_insert). The old
        // `add(doc_id, None)` relied on doc_freqs, but with positions
        // enabled iter_doc_tf()/term_frequency() derive TF from the
        // positions map — a doc without positions scored TF=0 and was
        // skipped by every search: after UPDATE, the row went permanently
        // invisible for all its new terms.
        let new_tokens = self.tokenizer.tokenize(new_text);
        let new_token_count = new_tokens.len() as u64;

        // Build per-term doc lists
        let mut term_docs: HashMap<TermId, Vec<(DocId, Option<Position>)>> = HashMap::new();
        for token in new_tokens {
            let term_id = self.dictionary.get_or_insert(&token.text);
            let pos = if self.enable_positions {
                Some(token.position)
            } else {
                None
            };
            term_docs.entry(term_id).or_default().push((doc_id, pos));
        }

        // Update pending posting lists
        {
            let mut pending = self.pending_posting_lists.write();
            let mut deleted_term_docs = self.deleted_term_docs.write();

            for (term_id, doc_entries) in term_docs {
                // Remove from deleted set if re-adding the same term
                deleted_term_docs.remove(&(term_id, doc_id));

                let posting = pending
                    .entry(term_id)
                    .or_insert_with(|| PostingList::new_without_positions(!self.enable_positions));
                for (doc_id, pos) in doc_entries {
                    posting.add(doc_id, pos);
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
    fn load_posting_list_sharded(
        &self,
        term_id: TermId,
        btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>,
    ) -> Result<Option<PostingList>> {
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
                let count = self.discover_shard_count(term_id, btree)?;
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
    fn load_posting_list_with_positions(
        &self,
        term_id: TermId,
        btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>,
    ) -> Result<Option<PostingList>> {
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
    /// counts how many distinct shard indices exist (shard 0..0xFE,
    /// excluding the position key at shard 0xFE).
    ///
    /// 🔑 The range spans OTHER terms' shard keys too — `(s<<24)|t` layouts
    /// interleave every term's shards across the whole key space — so every
    /// key must be filtered to this term's base. The unfiltered version
    /// returned the MAX shard index across all higher-base terms: a term
    /// flushing after a high-shard neighbor got `next_shard_idx` pushed
    /// arbitrarily high, scattering its shards (measured: alpha@shard2 with
    /// shard 0/1 missing after a mid-backfill auto-checkpoint; reopen then
    /// lost the term on any contiguity-assuming reader).
    fn discover_shard_count(&self, term_id: TermId, btree: &GenericBTree<u32>) -> Result<u32> {
        let base_term_id = term_id & 0x00FFFFFF;
        let range_start = base_term_id; // shard 0 key
        let range_end = (0xFEu32 << 24) | base_term_id; // position shard key (inclusive bound)

        let entries = btree.range(&range_start, &range_end)?;

        let mut max_shard_idx: u32 = 0;
        for (key, _) in &entries {
            if *key & 0x00FF_FFFF != base_term_id {
                continue; // another term's shard living inside this range
            }
            let shard_idx = *key >> 24;
            // Only count data shards (0..0xFE), skip position shard (0xFE)
            if shard_idx < 0xFE && shard_idx + 1 > max_shard_idx {
                max_shard_idx = shard_idx + 1;
            }
        }

        Ok(max_shard_idx)
    }

    /// Gather EVERY shard of a term (any layout, including legacy scattered
    /// keys) via a base-filtered range scan. Fallback for
    /// `probe_shard_count` == 0 — a term whose shard 0 is missing but higher
    /// shards exist (the old cross-term discovery bug could scatter writes
    /// like that).
    fn scattered_shard_keys(
        &self,
        term_id: TermId,
        btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>,
    ) -> Result<Vec<u32>> {
        let base_term_id = term_id & 0x00FFFFFF;
        let entries = btree.range(&base_term_id, &(0xFEu32 << 24 | base_term_id))?;
        Ok(entries
            .iter()
            .map(|(k, _)| *k)
            .filter(|k| {
                let shard = k >> 24;
                shard < 0xFE && k & 0x00FF_FFFF == base_term_id
            })
            .collect())
    }

    /// Shard count by sequential point probes: shards for a term are
    /// CONTIGUOUS (0..k-1) by construction — flush appends at the discovered
    /// next index, and consolidation rewrites everything as shard 0 — so the
    /// first missing key ends the run. This replaces the old full-range
    /// discovery on the search path: `(s<<24)|t` keys interleave every
    /// term's shards across the whole key space, so a range scan walks (and
    /// materializes values for) a huge slice of the tree — measured 10ms per
    /// fresh rare term on a 100K-unique-term vocabulary.
    fn probe_shard_count(
        &self,
        term_id: TermId,
        btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>,
    ) -> Result<u32> {
        let base_term_id = term_id & 0x00FFFFFF;
        let mut count = 0u32;
        while count < 0xFE {
            let shard_key = (count << 24) | base_term_id;
            match btree.get(&shard_key)? {
                Some(bytes) if !bytes.is_empty() => count += 1,
                _ => break,
            }
        }
        Ok(count)
    }

    /// Load (and cache) a term's DISK sources — one per shard, block format
    /// kept compressed. `None` = no shard on disk at all.
    fn load_disk_sources(
        &self,
        term_id: TermId,
        btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>,
    ) -> Result<Option<DiskPostings>> {
        if let Some(cached) = self.posting_cache.write().get(&term_id) {
            return Ok(Some(cached.clone()));
        }
        let base_term_id = term_id & 0x00FFFFFF;
        // Probe WITHOUT caching into shard_counters: the cached value is
        // flush's "next write index" and must equal max+1, which a
        // contiguous probe UNDERCOUNTS on legacy scattered trees. The probe
        // is 1-3 point gets — cheap enough uncached.
        let max_shard_idx = self.probe_shard_count(term_id, btree)?;

        // Shard keys to load: the contiguous run 0..count (native layout),
        // or — when the probe found nothing — every same-base key via a
        // base-filtered range scan (legacy trees scattered by the old
        // cross-term discovery bug).
        let mut shard_keys: Vec<u32> = (0..max_shard_idx)
            .map(|s| (s << 24) | base_term_id)
            .collect();
        if shard_keys.is_empty() {
            shard_keys = self.scattered_shard_keys(term_id, btree)?;
        }

        let mut sources = Vec::new();
        let mut found_any = false;
        for shard_key in shard_keys {
            let Some(bytes) = btree.get(&shard_key)? else {
                continue;
            };
            if bytes.is_empty() {
                continue;
            }
            found_any = true;
            if super::text_types::BlockPostingList::is_block_format(&bytes) {
                if let Ok(block) = super::text_types::BlockPostingList::deserialize(&bytes) {
                    sources.push(DiskSource::Block(Arc::new(block)));
                    continue;
                }
            }
            // Legacy shard: materialize to sorted pairs once.
            if let Ok(shard) = PostingList::deserialize_compact(&bytes) {
                let pairs: Vec<(u32, u16)> = shard.iter_doc_tf();
                sources.push(DiskSource::Pairs(Arc::from(pairs.into_boxed_slice())));
            }
        }
        if !found_any {
            return Ok(None);
        }
        self.posting_cache.write().put(term_id, sources.clone());
        Ok(Some(sources))
    }

    /// Build the streaming cursor for one term: pending posting first (its
    /// tf wins on cross-source duplicates), then the cached disk sources.
    /// Returns None when the term has no postings anywhere.
    #[allow(clippy::type_complexity)]
    fn term_stream(
        &self,
        term_id: TermId,
        pending: &HashMap<TermId, PostingList>,
        btree: &parking_lot::RwLockReadGuard<GenericBTree<u32>>,
    ) -> Result<Option<TermStream>> {
        let mut stream = TermStream::empty();
        if let Some(pend) = pending.get(&term_id) {
            let pairs = pend.iter_doc_tf(); // ascending (Roaring iteration)
            stream.df += pairs.len() as u64;
            stream.max_tf = stream
                .max_tf
                .max(pairs.iter().map(|&(_, t)| t).max().unwrap_or(0));
            stream.pair_pos.push(0);
            stream.pairs.push(Arc::from(pairs.into_boxed_slice()));
        }
        match self.load_disk_sources(term_id, btree)? {
            Some(sources) => {
                for src in sources {
                    match src {
                        DiskSource::Block(b) => {
                            stream.df += b.num_docs() as u64;
                            stream.max_tf = stream.max_tf.max(b.skip_max_tf());
                            stream.blocks.push(b.stream());
                        }
                        DiskSource::Pairs(p) => {
                            stream.df += p.len() as u64;
                            stream.max_tf = stream
                                .max_tf
                                .max(p.iter().map(|&(_, t)| t).max().unwrap_or(0));
                            stream.pair_pos.push(0);
                            stream.pairs.push(p);
                        }
                    }
                }
            }
            None => {
                if stream.pairs.is_empty() {
                    return Ok(None);
                }
            }
        }
        if stream.df == 0 {
            if std::env::var_os("MOTE_TRACE_FTS").is_some() {
                let shard_count = self.probe_shard_count(term_id, btree).unwrap_or(u32::MAX);
                eprintln!(
                    "[fts-stream] EMPTY term_id={term_id} base={:#x} probe_shards={shard_count}",
                    term_id & 0x00FFFFFF
                );
            }
            return Ok(None);
        }
        Ok(Some(stream))
    }

    /// Search for documents containing query terms
    /// Unranked match-set search.
    ///
    /// B0 semantics — OR-of-AND-groups (FTS5-compatible): a document matches
    /// when it contains every term of at least one group. Default
    /// conjunction between words is AND; `a OR b` unions the groups.
    /// (Pre-0.12 every multi-term query was an OR union over tokens.)
    ///
    /// B1: groups resolve via streaming lockstep intersection over block
    /// cursors — no posting materialization, and blocks are decoded only
    /// where the intersection actually walks.
    pub fn search(&self, query: &str) -> Result<Vec<DocumentId>> {
        let groups = self.parse_query_expanded(query);
        if groups.is_empty() {
            return Ok(Vec::new());
        }

        let pending = self.pending_posting_lists.read();
        let btree = self.btree.read();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();

        let mut results: Vec<DocumentId> = Vec::new();
        for group in &groups {
            // A fresh stream per term per group (a term may appear in two
            // groups; streams are consumed by the intersection). A term
            // with no posting empties the whole group (AND).
            let mut group_streams: Vec<TermStream> = Vec::with_capacity(group.len());
            let mut group_tids: Vec<TermId> = Vec::with_capacity(group.len());
            let mut complete = true;
            for term in group {
                let term_id = match self.dictionary.get(term) {
                    Some(id) => id,
                    None => {
                        complete = false;
                        break;
                    }
                };
                match self.term_stream(term_id, &pending, &btree)? {
                    Some(s) => {
                        group_tids.push(term_id);
                        group_streams.push(s);
                    }
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            if complete && !group_streams.is_empty() {
                let mut docs: Vec<u32> = Vec::new();
                intersect_streams(
                    &mut group_streams,
                    &mut group_tids,
                    &deleted,
                    &deleted_term_docs,
                    &mut docs,
                );
                results.extend(docs.into_iter().map(|d| d as DocumentId));
            }
        }

        if groups.len() > 1 {
            results.sort_unstable();
            results.dedup();
        }
        Ok(results)
    }

    /// Parse a MATCH query into OR-of-AND groups of TOKENIZED terms (each
    /// raw word expanded through this index's tokenizer, so ngram/length
    /// filters apply exactly as at index time).
    fn parse_query_expanded(&self, query: &str) -> Vec<Vec<String>> {
        crate::index::text_query::expand_groups(
            crate::index::text_query::parse_query_groups(query),
            |w| {
                self.tokenizer
                    .tokenize(w)
                    .into_iter()
                    .map(|t| t.text)
                    .collect()
            },
        )
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

        // 🔑 Enumerate candidates from the RAREST token (smallest posting)
        // but verify positions in PHRASE ORDER. The old code sorted `postings`
        // by doc_count and then treated index+1 as "next token in the
        // phrase" — after the sort, offsets followed RARITY order, not phrase
        // order, so 2-token phrases matched in the REVERSE direction
        // ('alpha delta' matched documents containing "delta alpha").
        let anchor_idx = postings
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, p))| p.doc_count())
            .map(|(i, _)| i)
            .unwrap_or(0);

        let mut result = Vec::new();

        'outer: for doc_id in postings[anchor_idx].1.doc_ids() {
            let doc_id = doc_id as DocId;
            if deleted.contains(&doc_id) {
                continue;
            }
            // Check if any term's association with this doc was deleted
            if postings
                .iter()
                .any(|(tid, _)| deleted_term_docs.contains(&(*tid, doc_id)))
            {
                continue;
            }

            // Positions of the anchor (rarest) token in this doc.
            let anchor_positions = match postings[anchor_idx].1.get_positions(doc_id) {
                Some(positions) => positions,
                None => continue, // No position data → cannot verify phrase
            };

            'pos: for &anchor_pos in anchor_positions.iter() {
                // Token at phrase index i must appear at anchor_pos + (i - anchor_idx).
                for (i, (_, posting)) in postings.iter().enumerate() {
                    if i == anchor_idx {
                        continue;
                    }
                    let expected_pos = (anchor_pos as i64 + (i as i64 - anchor_idx as i64)) as u32;
                    match posting.get_positions(doc_id) {
                        Some(positions) => {
                            if !positions.contains(&expected_pos) {
                                continue 'pos;
                            }
                        }
                        None => continue 'outer,
                    }
                }
                // All tokens matched at consecutive positions in phrase order
                result.push(doc_id);
                continue 'outer;
            }
        }

        Ok(result)
    }

    /// 🚀 Fast single-term search: score all docs for one term, return top-K.
    /// B1: streams the posting (one 128-doc block decoded at a time) — no
    /// materialization, no per-query HashMap merge/sort.
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
            None => {
                return Ok(Vec::new());
            }
        };

        let mut stream = {
            let pending = self.pending_posting_lists.read();
            let btree = self.btree.read();
            match self.term_stream(term_id, &pending, &btree)? {
                Some(s) => s,
                None => return Ok(Vec::new()),
            }
        };
        let df = stream.df;
        let idf = ((self.total_docs as f32 - df as f32 + 0.5) / (df as f32 + 0.5) + 1.0).ln();

        let doc_lengths = self.get_doc_lengths_cached()?;
        let avg_dl = if self.avg_doc_length > 0.0 {
            self.avg_doc_length
        } else {
            1.0
        };
        let k1 = self.bm25_config.k1;
        let b = self.bm25_config.b;

        let deleted = self.deleted_docs.read();
        let deleted_td = self.deleted_term_docs.read();
        let deleted_empty = deleted.is_empty() && deleted_td.is_empty();

        let mut scored: Vec<(DocumentId, f32)> = Vec::with_capacity(df.min(4096) as usize);
        while let Some((doc_id_u32, tf)) = stream.current() {
            stream.advance();
            if tf == 0 {
                continue;
            }
            let doc_id = doc_id_u32 as DocumentId;
            if !deleted_empty {
                if deleted.contains(&doc_id) {
                    continue;
                }
                if deleted_td.contains(&(term_id, doc_id)) {
                    continue;
                }
            }
            // doc_lengths holds ENCODED fieldnorm bytes (FieldNormTable),
            // not raw lengths — decode before the BM25 norm.
            let fieldnorm = doc_lengths.get(&doc_id).copied().unwrap_or(0);
            let dl = FieldNormTable::decode(fieldnorm, avg_dl).max(1.0);
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

    /// Ranked BM25 search with top-K early termination.
    ///
    /// B0 semantics — OR-of-AND-groups (FTS5-compatible): a document is a
    /// candidate only when it contains every term of at least one group
    /// (default conjunction AND; `a OR b` unions groups). Candidates come
    /// from each group's lockstep intersection — the old WAND loop admitted
    /// every doc holding ANY term, so a multi-term query scanned the union.
    ///
    /// B1: intersections stream over block cursors (no posting
    /// materialization); df/max_tf for IDF and upper bounds come from block
    /// headers and skip tables without decoding. Candidates merge into one
    /// ascending list; a single monotonic scoring pass probes each term's
    /// stream once — the per-doc sum of matched bounds gates the heap.
    pub fn search_ranked(&self, query: &str, top_k: usize) -> Result<Vec<(DocumentId, f32)>> {
        let groups = self.parse_query_expanded(query);
        if groups.is_empty() {
            return Ok(Vec::new());
        }

        // 🚀 Fast path for single-term queries (most common case).
        // Skip the cursor machinery — score one posting, partial-sort, done.
        if groups.len() == 1 && groups[0].len() == 1 {
            return self.search_single_term(&groups[0][0], top_k);
        }

        // 🚀 Multi-term top-K cache: repeated queries with the same GROUP
        // STRUCTURE hit the cache and skip scoring entirely. The key
        // serializes groups, not just the token set — `a b` and `a OR b`
        // share tokens but not semantics.
        let cache_key = format!(
            "{}|{}",
            groups
                .iter()
                .map(|g| g.join("\x1f"))
                .collect::<Vec<_>>()
                .join("\x1e"),
            top_k
        );
        {
            let mut cache = self.topk_cache.write();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        let doc_lengths = self.get_doc_lengths_cached()?;
        let avg_dl = if self.avg_doc_length > 0.0 {
            self.avg_doc_length
        } else {
            1.0
        };

        let pending = self.pending_posting_lists.read();
        let btree = self.btree.read();
        let deleted = self.deleted_docs.read();
        let deleted_term_docs = self.deleted_term_docs.read();

        let k1 = self.bm25_config.k1;
        let b = self.bm25_config.b;
        let total_docs = self.total_docs as f32;

        // Distinct terms across all groups; one SCORING stream each (df and
        // max_tf read from headers/skip tables — no block decode).
        let mut term_order: Vec<String> = groups.iter().flatten().cloned().collect();
        term_order.sort();
        term_order.dedup();

        let mut term_idx: HashMap<&str, usize> = HashMap::new();
        let mut streams: Vec<TermStream> = Vec::new();
        let mut term_ids: Vec<TermId> = Vec::new();
        let mut idfs: Vec<f32> = Vec::new();
        let mut bounds: Vec<f32> = Vec::new();
        for token_text in &term_order {
            let term_id = match self.dictionary.get(token_text) {
                Some(id) => id,
                None => continue,
            };
            let stream = match self.term_stream(term_id, &pending, &btree)? {
                Some(s) => s,
                None => continue,
            };
            let df = stream.df as f32;
            if df == 0.0 {
                continue;
            }
            let max_tf = stream.max_tf;
            // BM25 IDF: non-negative variant (Lucene-compatible). The +1
            // ensures terms in every document keep a small positive weight.
            let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
            let upper_bound = {
                let tf = max_tf as f32;
                // dl=1 as the minimum possible doc length (tighter bound)
                let min_norm = 1.0 - b + b / self.avg_doc_length.max(1.0);
                idf * (tf * (k1 + 1.0)) / (tf + k1 * min_norm)
            };
            term_idx.insert(token_text, streams.len());
            term_ids.push(term_id);
            idfs.push(idf);
            bounds.push(upper_bound);
            streams.push(stream);
        }

        if streams.is_empty() {
            return Ok(Vec::new());
        }

        // Per-group candidate sets: fresh streams (the scoring streams above
        // stay untouched for the scoring pass), lockstep intersection. A
        // term with no stream empties its group (AND semantics).
        let mut group_sets: Vec<Vec<u32>> = Vec::with_capacity(groups.len());
        for group in &groups {
            let mut group_streams: Vec<TermStream> = Vec::with_capacity(group.len());
            let mut group_tids: Vec<TermId> = Vec::with_capacity(group.len());
            let mut complete = true;
            for t in group {
                match term_idx.get(t.as_str()) {
                    Some(&i) => {
                        // Rebuild from cached sources — cheap (Arc clones +
                        // one block decode), and stateful streams can't be
                        // shared with the scoring pass.
                        match self.term_stream(term_ids[i], &pending, &btree)? {
                            Some(s) => {
                                group_tids.push(term_ids[i]);
                                group_streams.push(s);
                            }
                            None => {
                                complete = false;
                                break;
                            }
                        }
                    }
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            let mut docs = Vec::new();
            if complete && !group_streams.is_empty() {
                intersect_streams(
                    &mut group_streams,
                    &mut group_tids,
                    &deleted,
                    &deleted_term_docs,
                    &mut docs,
                );
            }
            group_sets.push(docs);
        }

        // Merge the per-group sets into one ascending candidate list.
        let mut candidates: Vec<u32> = Vec::new();
        if group_sets.len() == 1 {
            candidates = group_sets.into_iter().next().unwrap();
        } else {
            let mut heads: Vec<usize> = vec![0; group_sets.len()];
            loop {
                let mut mn = u32::MAX;
                for (gi, docs) in group_sets.iter().enumerate() {
                    if heads[gi] < docs.len() {
                        mn = mn.min(docs[heads[gi]]);
                    }
                }
                if mn == u32::MAX {
                    break;
                }
                candidates.push(mn);
                for (gi, docs) in group_sets.iter().enumerate() {
                    if heads[gi] < docs.len() && docs[heads[gi]] == mn {
                        heads[gi] += 1;
                    }
                }
            }
        }

        // Ordered floats for the heap (Reverse<(OrderedFloat, u32)>)
        #[derive(Clone, PartialEq)]
        struct OrdF32(f32);
        impl Eq for OrdF32 {}
        impl PartialOrd for OrdF32 {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for OrdF32 {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0
                    .partial_cmp(&other.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        }

        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(OrdF32, u32)>> =
            std::collections::BinaryHeap::with_capacity(top_k + 1);
        let mut threshold = 0.0f32;

        // One monotonic scoring pass: candidates ascend, so each stream only
        // ever seeks forward. A doc's score is the BM25 sum over the terms
        // whose stream lands exactly on it (dedup across groups is free —
        // each doc appears once in the merged candidate list).
        for &doc in &candidates {
            let doc_id = doc as DocId;
            if !deleted.is_empty() && deleted.contains(&doc_id) {
                continue;
            }
            let mut tfs: Vec<(usize, u16)> = Vec::with_capacity(streams.len());
            let mut bound = 0.0f32;
            for (i, s) in streams.iter_mut().enumerate() {
                if s.current().is_none_or(|(d, _)| d < doc) {
                    s.seek(doc);
                }
                if s.current().is_some_and(|(d, _)| d == doc)
                    && !(!deleted_term_docs.is_empty()
                        && deleted_term_docs.contains(&(term_ids[i], doc_id)))
                {
                    bound += bounds[i];
                    tfs.push((i, s.current().unwrap().1));
                }
            }
            if tfs.is_empty() {
                continue;
            }
            if heap.len() >= top_k && bound <= threshold {
                continue;
            }

            let doc_len = doc_lengths.get(&doc_id).copied().unwrap_or(0);
            let dl_approx = FieldNormTable::decode(doc_len, avg_dl);
            let norm = 1.0 - b + b * (dl_approx / avg_dl);
            let mut score = 0.0f32;
            for (i, tf) in tfs {
                let tf = tf as f32;
                score += idfs[i] * (tf * (k1 + 1.0)) / (tf + k1 * norm);
            }

            if heap.len() < top_k {
                heap.push(std::cmp::Reverse((OrdF32(score), doc)));
                if heap.len() == top_k {
                    threshold = heap.peek().map(|h| h.0 .0 .0).unwrap_or(0.0);
                }
            } else if score > threshold {
                heap.pop();
                heap.push(std::cmp::Reverse((OrdF32(score), doc)));
                threshold = heap.peek().map(|h| h.0 .0 .0).unwrap_or(0.0);
            }
        }

        // Extract results
        let mut results: Vec<(DocumentId, f32)> = heap
            .into_iter()
            .map(|std::cmp::Reverse((OrdF32(score), doc))| (doc as DocId, score))
            .collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        // 🚀 Populate multi-term top-K cache for future repeat queries.
        if !results.is_empty() {
            self.topk_cache.write().put(cache_key, results.clone());
        }

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

        let avg_dl = if self.avg_doc_length > 0.0 {
            self.avg_doc_length
        } else {
            1.0
        };
        let encoded: HashMap<DocId, u8> = doc_lengths
            .into_iter()
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

        // A flush appends new shards (and may consolidate old ones), so the
        // cached per-term disk sources and every top-K result are stale — a
        // term searched before the flush must see the new shard's docs.
        self.posting_cache.write().clear();
        self.topk_cache.write().clear();

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

        // 🔥 Bulk shard discovery for large batches (CREATE INDEX / bulk load):
        // the per-term fallback below pays a full root-to-leaf BTree descent
        // PER TERM (~5 page deserializes each) — 10K terms ≈ 50K page reads,
        // and the interleaved inserts keep evicting the page cache so it never
        // amortizes (profiled: 85% of CREATE TEXT INDEX time). One sequential
        // keys-only scan reads every page exactly once instead. Threshold
        // 1024: below it, per-term point lookups are cheaper than a full scan.
        if pending_data.len() >= 1024 {
            let mut max_by_base: std::collections::HashMap<u32, u32> =
                std::collections::HashMap::with_capacity(pending_data.len());
            if let Ok(keys) = btree.range_keys(&0u32, &u32::MAX) {
                for key in keys {
                    let shard_idx = key >> 24;
                    if shard_idx < 0xFE {
                        let base = key & 0x00FF_FFFF;
                        let count = shard_idx + 1;
                        let e = max_by_base.entry(base).or_insert(0);
                        if count > *e {
                            *e = count;
                        }
                    }
                }
            }
            for term_id in pending_data.keys() {
                if !shard_counters.contains(term_id) {
                    // Absent from disk ⇒ 0 shards — same value discover_shard_count
                    // would return, without the per-term BTree descent.
                    let count = max_by_base
                        .get(&(*term_id & 0x00FF_FFFF))
                        .copied()
                        .unwrap_or(0);
                    shard_counters.put(*term_id, count);
                }
            }
        }

        for (term_id, posting) in pending_data.iter() {
            let base_term_id = *term_id & 0x00FFFFFF;
            // If the shard counter was evicted from LRU, discover the actual
            // count from the BTree to avoid overwriting consolidated shard 0.
            let next_shard_idx = match shard_counters.get(term_id) {
                Some(idx) => *idx,
                None => {
                    let discovered = self.discover_shard_count(*term_id, &btree).unwrap_or(0);
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
                if deleted.contains(&doc_id) {
                    continue;
                }
                if deleted_term_docs.contains(&(*term_id, doc_id)) {
                    continue;
                }
                clean_ids.push(doc_id_u64 as u32);
                clean_tfs.push(posting.term_frequency(doc_id));
            }

            if clean_ids.is_empty() {
                continue;
            }

            // Encode as block format
            let block_list =
                super::text_types::BlockPostingList::from_sorted_pairs(&clean_ids, &clean_tfs);
            let bytes = block_list.as_bytes();

            // Append-only: write as a new shard (no merge with existing shards)
            let shard_key = (next_shard_idx << 24) | base_term_id;
            if std::env::var_os("MOTE_TRACE_FTS").is_some() {
                eprintln!(
                    "[fts-flush] term={term_id} base={base_term_id} next_shard={next_shard_idx} docs={}",
                    clean_ids.len()
                );
            }
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
                if let Err(e) =
                    self.consolidate_shards_for_term(&mut btree, &mut shard_counters, *term_id)
                {
                    debug_log!(
                        "[FTS] Shard consolidation failed for term {}: {}",
                        term_id,
                        e
                    );
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
            *pending = HashMap::new(); // 完全替换（capacity归零）
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
        }

        let _t4_elapsed = t4.elapsed();

        // 5. Write doc_lengths — flush_doc_lengths_if_needed DRAINS the
        // pending map into the incremental file. An earlier `clear()` here
        // ran before it, so every auto-flush permanently destroyed the
        // accumulated lengths: after the first mid-backfill flush, BM25
        // scored every document with the same constant length (dl=1) and
        // ranking degraded to length-blind.
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

        let _total_elapsed = flush_start.elapsed(); // debug_log disabled for Phase A optimization

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
        let deleted_term_docs: Vec<(TermId, DocId)> =
            self.deleted_term_docs.read().iter().copied().collect();

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
    fn load_metadata(
        stats_path: &PathBuf,
    ) -> Result<(u64, u64, f32, Vec<DocId>, Vec<(TermId, DocId)>)> {
        let mut file = File::open(stats_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok((0, 0, 0.0, Vec::new(), Vec::new()));
        }

        let metadata: TextFTSMetadata = bincode::deserialize(&buffer)
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
                    // The writer serialized a DocLengthMap struct (same as
                    // the main file), not a bare HashMap — deserializing the
                    // wrong type failed on EVERY block and was silently
                    // swallowed, losing all flushed doc lengths (BM25 then
                    // scored every doc with the same constant length).
                    if let Ok(map) = bincode::deserialize::<DocLengthMap>(&buffer[start..end]) {
                        all_lengths.extend(map.lengths);
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
        btree: &mut parking_lot::RwLockWriteGuard<
            '_,
            crate::index::btree_generic::GenericBTree<u32>,
        >,
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
                        if let Ok(block_list) =
                            super::text_types::BlockPostingList::deserialize(&bytes)
                        {
                            let mut cursor = block_list.cursor();
                            while cursor.is_valid() {
                                merged.add_with_freq(
                                    cursor.current_doc() as u64,
                                    None,
                                    cursor.current_tf(),
                                );
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
            if deleted.contains(&doc_id) {
                continue;
            }
            if deleted_term_docs.contains(&(term_id, doc_id)) {
                continue;
            }
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
        let block_list =
            super::text_types::BlockPostingList::from_sorted_pairs(&clean_ids, &clean_tfs);
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
        let pending_map = DocLengthMap {
            lengths: pending_doc_lens.drain().collect(),
        };
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

    /// 🔒 Cross-term shard discovery: `(s<<24)|t` keys interleave every
    /// term's shards, so an unfiltered range "discover" returns the max
    /// shard across ALL higher-base terms — a term flushing after a
    /// high-shard neighbor got its next_shard_idx pushed arbitrarily high,
    /// scattering its shards (alpha@shard2, shard 0/1 missing) and losing
    /// the term on reopen. Staggered multi-round flushes must keep every
    /// term's shards contiguous AND fully findable after reopen.
    #[test]
    fn shard_discovery_not_cross_term_contaminated() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("t");
        {
            let mut index = TextFTSIndex::new(path.clone()).unwrap();
            // Round 1: a wide term vocabulary (drives many distinct bases).
            for i in 0..40u64 {
                index
                    .insert(i, &format!("shared alpha{i} beta{i} gamma common{}", i % 7))
                    .unwrap();
            }
            index.flush().unwrap();
            // Round 2: RE-TOUCH terms (pending again) so the next flush
            // assigns next_shard_idx per term under contention — the
            // contamination window.
            for i in 0..40u64 {
                index
                    .insert(
                        100 + i,
                        &format!("shared alpha{i} beta{i} gamma common{}", i % 7),
                    )
                    .unwrap();
            }
            index.flush().unwrap();
            // Round 3 once more for good measure.
            for i in 0..40u64 {
                index
                    .insert(
                        200 + i,
                        &format!("shared alpha{i} beta{i} gamma common{}", i % 7),
                    )
                    .unwrap();
            }
            index.flush().unwrap();
        }
        // Reopen: EVERY term must see its full doc set (3 docs each for the
        // alphaX/betaX terms, 120 for the dense shared terms).
        let index = TextFTSIndex::new(path).unwrap();
        for i in 0..40u64 {
            for term in [format!("alpha{i}"), format!("beta{i}")] {
                let r = index.search(&term).unwrap();
                assert_eq!(r.len(), 3, "term {term} lost docs after reopen");
            }
        }
        assert_eq!(index.search("shared").unwrap().len(), 120);
        assert_eq!(index.search("gamma").unwrap().len(), 120);
        // And the layout is contiguous: the contiguous probe must agree
        // with the base-filtered range count (no scattered writes).
        let btree = index.btree.read();
        for term in ["shared", "gamma", "alpha0", "beta39"] {
            let tid = index.dictionary.get(term).expect(term);
            let count = index.probe_shard_count(tid, &btree).unwrap();
            assert!(count >= 1, "{term}: no shards");
            let scattered = index.scattered_shard_keys(tid, &btree).unwrap().len() as u32;
            assert_eq!(
                count, scattered,
                "{term}: shards scattered (probe {count} vs range {scattered})"
            );
        }
    }

    /// 🔒 Legacy-scattered layout (shard 0 missing, data at a higher shard —
    /// what the old cross-term discovery bug wrote) must still load via the
    /// base-filtered range fallback.
    #[test]
    fn scattered_shard_layout_still_searchable() {
        let temp_dir = TempDir::new().unwrap();
        {
            let mut index = TextFTSIndex::new(temp_dir.path().join("t")).unwrap();
            for i in 1..=5u64 {
                index.insert(i, "legacy shard survivor").unwrap();
            }
            index.flush().unwrap();
        }

        // Rewrite the term's single shard at index 3 (simulate scatter) by
        // moving the key inside the B+Tree, then reopen.
        {
            let index = TextFTSIndex::new(temp_dir.path().join("t")).unwrap();
            let term_id = index.dictionary.get("legacy").unwrap();
            let base = term_id & 0x00FF_FFFF;
            let mut btree = index.btree.write();
            let bytes = btree.get(&base).unwrap().unwrap();
            btree.insert((3u32 << 24) | base, bytes).unwrap();
            let _ = btree.delete(&base);
            btree.flush().unwrap();
        }

        let index = TextFTSIndex::new(temp_dir.path().join("t")).unwrap();
        let r = index.search("legacy").unwrap();
        assert_eq!(
            r.len(),
            5,
            "scattered layout (shard 0 missing) must load via range fallback"
        );
    }

    /// 🔒 B1: a term searched BEFORE a flush must see the docs the flush
    /// added afterwards — the posting cache used to keep serving the stale
    /// pre-flush shard snapshot forever (no invalidation on flush).
    #[test]
    fn test_posting_cache_invalidated_on_flush() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = TextFTSIndex::new(temp_dir.path().join("test")).unwrap();

        index.insert(1, "cache me apple").unwrap();
        index.insert(2, "cache me apple").unwrap();
        // Populate the posting cache with the pre-flush (pending) posting.
        assert_eq!(index.search("apple").unwrap().len(), 2);

        // These land in pending; flush() turns them into a NEW shard while
        // the old "apple" entry sits in the posting cache.
        index.insert(3, "cache me apple").unwrap();
        index.flush().unwrap();

        let mut r = index.search("apple").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![1, 2, 3], "flush must invalidate the posting cache");
        let ranked = index.search_ranked("apple", 10).unwrap();
        assert_eq!(ranked.len(), 3, "ranked path too");
    }

    /// B0: multi-term default conjunction is AND (FTS5-compatible); explicit
    /// uppercase `OR` unions groups; lowercase `or` is an ordinary term.
    #[test]
    fn test_match_query_and_or_groups() {
        let temp_dir = TempDir::new().unwrap();
        let mut index = TextFTSIndex::new(temp_dir.path().join("test")).unwrap();

        index.insert(1, "apple pie").unwrap();
        index.insert(2, "banana bread").unwrap();
        index.insert(3, "apple banana bread").unwrap();
        index.insert(4, "either or both").unwrap();
        index.insert(5, "banana tart").unwrap();
        index.insert(6, "banana").unwrap();

        // Default AND: only docs holding EVERY term.
        let mut r = index.search("apple banana").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![3], "default conjunction must be AND");
        let ranked = index.search_ranked("apple banana", 10).unwrap();
        assert_eq!(ranked.len(), 1, "ranked path must agree with AND");
        assert_eq!(ranked[0].0, 3);

        // Explicit OR: union of groups.
        let mut r = index.search("apple OR banana").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![1, 2, 3, 5, 6]);
        let ranked = index.search_ranked("apple OR banana", 10).unwrap();
        let mut ids: Vec<u64> = ranked.iter().map(|(d, _)| *d).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 5, 6]);

        // Implicit AND binds tighter than OR: (apple) OR (banana AND bread).
        // Docs 5/6 hold `banana` but NOT the group — a flat OR would admit them.
        let mut r = index.search("apple OR banana bread").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![1, 2, 3]);

        // Uppercase AND is an explicit no-op separator.
        let mut r = index.search("apple AND banana").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![3]);

        // An unknown term ANDs the group to nothing.
        assert!(index.search("apple zzzqqq").unwrap().is_empty());
        let ranked = index.search_ranked("apple zzzqqq", 10).unwrap();
        assert!(ranked.is_empty());
        // …but ORs as an escape hatch.
        let mut r = index.search("apple OR zzzqqq").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![1, 3]);

        // Lowercase `or` is a term, not an operator.
        let mut r = index.search("either or").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![4]);

        // Single term keeps the fast path (cache + single-term scoring).
        let mut r = index.search("bread").unwrap();
        r.sort_unstable();
        assert_eq!(r, vec![2, 3]);
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
        assert_eq!(
            index.stats().total_docs,
            1,
            "double-delete should not underflow total_docs"
        );
    }
}

// ==================== 🚀 Batch Index Builder Implementation ====================

use crate::index::builder::{BuildStats, IndexBuilder};
use crate::types::{Row, RowId, Value};

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

        debug_log!(
            "[TextFTSIndex] Batch building {} documents",
            documents.len()
        );

        // 🔥 Phase 2: 使用已有的batch_insert方法（高效）
        let doc_refs: Vec<(u64, &str)> = documents
            .iter()
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
