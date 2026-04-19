//! Index layer implementation
//!
//! Provides indexes for multi-modal data types

mod manager;
pub mod builder;  // 🚀 新增：批量索引构建接口
pub mod text_types;
pub mod text_fts;
pub mod text_encoding;
pub mod text_dictionary;
pub mod tokenizers;  // 🔌 新增：分词器插件系统
pub mod vamana;
pub mod fresh_graph;
pub mod btree;
pub mod btree_generic;
pub mod primary_key;
pub mod column_value;
pub mod cached_index; // 🚀 P1: 索引缓存层
pub mod ioctree;     // i-Octree 3D spatial index for embodied intelligence

pub use manager::{IndexManager, IndexType, IndexUpdate};
pub use builder::{IndexBuilder, BuildStats};  // 🚀 导出批量构建接口
pub use text_fts::{TextFTSIndex, TextFTSStats};
pub use text_types::{Tokenizer, WhitespaceTokenizer, NgramTokenizer, Token};
pub use text_dictionary::ChunkedDictionary;
pub use btree::{BTree, BTreeConfig, BTreeStats, RangeQueryProfile};
pub use btree_generic::{GenericBTree, GenericBTreeConfig, BTreeKey};
pub use primary_key::PrimaryKeyIndex;
pub use vamana::DiskANNIndex;
pub use column_value::{ColumnValueIndex, ColumnValueIndexConfig, IndexStats as ColumnIndexStats};
pub use cached_index::{CachedIndex, CacheStats};

use crate::types::Value;
use crate::Result;

/// Common index trait for all index types
pub trait Index: Send + Sync {
    /// Insert a single value
    fn insert(&mut self, row_id: u64, value: &Value) -> Result<()>;

    /// Batch insert multiple values
    fn batch_insert(&mut self, items: Vec<(u64, Value)>) -> Result<()>;

    /// Query index with a predicate
    fn query(&self, predicate: &Predicate) -> Result<Vec<u64>>;
}

/// Query predicate for index search
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Vector KNN search: (query_vector, k)
    VectorKnn(Vec<f32>, usize),
    
    /// Text search: query string
    TextSearch(String),
    
    /// Timestamp range: (start, end)
    TimestampRange(i64, i64),
}
