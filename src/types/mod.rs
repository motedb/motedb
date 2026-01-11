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

use serde::{Deserialize, Serialize};

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
    Vector(Vec<f32>),
    
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
        use std::cmp::Ordering;
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
