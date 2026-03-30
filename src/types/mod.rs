//! Multi-modal data types for MoteDB

mod tensor;
mod spatial;
mod text;
mod timestamp;
mod table;

pub use tensor::Tensor;
pub use spatial::{Geometry, Point, BoundingBox};
pub use text::{Text, TextDoc};
pub use timestamp::Timestamp;
pub use table::{TableSchema, ColumnDef, ColumnType, IndexDef, IndexType, Column};

use serde::{Deserialize, Serialize, Deserializer, Serializer};
use std::sync::Arc;

/// Wrapper for Arc<Vec<f32>> with custom serde implementation
#[derive(Debug, Clone, PartialEq)]
pub struct ArcVec(pub Arc<Vec<f32>>);

impl ArcVec {
    pub fn new(vec: Vec<f32>) -> Self {
        ArcVec(Arc::new(vec))
    }
    
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    
    pub fn iter(&self) -> std::slice::Iter<'_, f32> {
        self.0.iter()
    }
    
    pub fn as_slice(&self) -> &[f32] {
        self.0.as_ref()
    }
    
    /// Get a cloned Vec<f32> (for APIs that need owned Vec)
    pub fn to_vec(&self) -> Vec<f32> {
        (*self.0).clone()
    }
}

impl Serialize for ArcVec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArcVec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<f32>::deserialize(deserializer)?;
        Ok(ArcVec(Arc::new(vec)))
    }
}

/// Unified value type supporting all data modalities
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    /// Integer value
    Integer(i64),
    
    /// Floating point value
    Float(f64),
    
    /// Boolean value
    Bool(bool),
    
    /// Text string
    Text(String),
    
    /// Vector data (for embeddings)
    /// 🚀 优化：使用 Arc 避免深拷贝（clone只拷贝指针，不拷贝数据）
    /// - 原来：每次clone复制整个Vec（128个f32 = 512 bytes）
    /// - 现在：每次clone复制Arc指针（8 bytes）
    /// - 性能提升：物化10,000行向量从 ~5MB拷贝降至 ~80KB
    Vector(ArcVec),
    
    /// Vector/Tensor data (stored as Float16) - legacy
    Tensor(Tensor),
    
    /// Spatial geometry data
    Spatial(Geometry),
    
    /// Text document (for full-text search) - legacy
    TextDoc(Text),
    
    /// Timestamp data
    Timestamp(Timestamp),
    
    /// Null value
    Null,
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
}

/// A row contains multiple values (for storage engine)
pub type Row = Vec<Value>;

/// A SQL row contains named values (for SQL engine)
pub type SqlRow = std::collections::HashMap<String, Value>;

/// Row identifier (unique across the database)
pub type RowId = u64;

/// Partition identifier for parallel writes
pub type PartitionId = u8;
