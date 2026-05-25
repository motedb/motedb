//! Multi-modal data types for MoteDB

mod tensor;
mod spatial;
mod text;
mod timestamp;
mod table;

pub use tensor::Tensor;
pub use spatial::{Geometry, Point, Point3D, BoundingBox, BoundingBox3D};
pub use text::{Text, TextDoc};
pub use timestamp::Timestamp;
pub use table::{TableSchema, ColumnDef, ColumnType, IndexDef, IndexType, TableType, TTLDuration};

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

    pub fn to_vec(&self) -> Vec<f32> {
        (*self.0).clone()
    }

    pub fn as_slice(&self) -> &[f32] {
        self.0.as_ref()
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

/// Arc-wrapped String for cheap cloning in Value::Text.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ArcString(pub Arc<String>);

impl ArcString {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for ArcString {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for ArcString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArcString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(ArcString(Arc::new(s)))
    }
}

impl std::fmt::Display for ArcString {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialOrd for ArcString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl From<String> for ArcString {
    fn from(s: String) -> Self {
        ArcString(Arc::new(s))
    }
}

impl From<&str> for ArcString {
    fn from(s: &str) -> Self {
        ArcString(Arc::new(s.to_string()))
    }
}

impl PartialEq<String> for ArcString {
    fn eq(&self, other: &String) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for ArcString {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_str() == *other
    }
}

/// Unified value type supporting all data modalities
///
/// Size optimization: large variants (Text, Tensor, Spatial, TextDoc) are
/// boxed to keep the enum at 16 bytes instead of 40 bytes. This reduces
/// memory amplification for scalar-heavy rows from 8.6x to ~3.5x.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Integer value
    Integer(i64),

    /// Floating point value
    Float(f64),

    /// Boolean value
    Bool(bool),

    /// Text string (Arc for cheap cloning — just atomic increment)
    Text(ArcString),

    /// Vector data (for embeddings)
    Vector(ArcVec),

    /// Tensor data (boxed to reduce enum size)
    Tensor(Box<Tensor>),

    /// Spatial geometry data (boxed to reduce enum size)
    Spatial(Box<Geometry>),

    /// Text document for full-text search (boxed to reduce enum size)
    TextDoc(Box<Text>),

    /// Timestamp data
    Timestamp(Timestamp),

    /// Null value
    Null,
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            (Value::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Value::Null) => Some(std::cmp::Ordering::Greater),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            // Timestamp vs Integer: compare timestamp micros to integer value
            (Value::Timestamp(a), Value::Integer(b)) => a.as_micros().partial_cmp(b),
            (Value::Integer(a), Value::Timestamp(b)) => a.partial_cmp(&b.as_micros()),
            // Timestamp vs Float: compare timestamp micros to float value
            (Value::Timestamp(a), Value::Float(b)) => (a.as_micros() as f64).partial_cmp(b),
            (Value::Float(a), Value::Timestamp(b)) => a.partial_cmp(&(b.as_micros() as f64)),
            _ => None,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            // Cross-type numeric equality: Integer(1) == Float(1.0)
            (Value::Integer(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Integer(b)) => *a == (*b as f64),
            // Cross-type Timestamp equality (must match partial_cmp semantics)
            (Value::Timestamp(a), Value::Integer(b)) => a.as_micros() == *b,
            (Value::Integer(a), Value::Timestamp(b)) => *a == b.as_micros(),
            (Value::Timestamp(a), Value::Float(b)) => (a.as_micros() as f64) == *b,
            (Value::Float(a), Value::Timestamp(b)) => *a == (b.as_micros() as f64),
            _ => false,
        }
    }
}
impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // For Integer/Float: hash the normalized f64 representation so that
        // equal cross-type values (e.g., Integer(1) == Float(1.0)) produce the same hash.
        match self {
            Value::Integer(i) => {
                // Hash using the same canonical representation as Float would
                let f = *i as f64;
                state.write_u8(0); // numeric discriminant
                f.to_bits().hash(state);
            }
            Value::Float(f) => {
                let canonical = if *f == 0.0 { 0.0f64 } else { *f };
                state.write_u8(0); // numeric discriminant
                canonical.to_bits().hash(state);
            }
            Value::Text(s) => {
                state.write_u8(1);
                s.hash(state);
            }
            Value::Bool(b) => {
                state.write_u8(2);
                b.hash(state);
            }
            Value::Null => {
                state.write_u8(3);
            }
            Value::Timestamp(t) => {
                // Timestamp is cross-type equal to Integer/Float with same micros value,
                // so must hash to the same discriminant + representation.
                let micros = t.as_micros();
                let f = micros as f64;
                state.write_u8(0); // numeric discriminant (same as Integer/Float)
                f.to_bits().hash(state);
            }
            other => {
                state.write_u8(5);
                format!("{:?}", other).hash(state);
            }
        }
    }
}

impl Value {
    /// Create a Text value (boxes the string to keep enum compact)
    pub fn text(s: String) -> Self {
        Value::Text(ArcString(Arc::new(s)))
    }

    /// Create a Tensor value
    pub fn tensor(t: Tensor) -> Self {
        Value::Tensor(Box::new(t))
    }

    /// Create a Spatial value
    pub fn spatial(g: Geometry) -> Self {
        Value::Spatial(Box::new(g))
    }

    /// Create a TextDoc value
    pub fn textdoc(t: Text) -> Self {
        Value::TextDoc(Box::new(t))
    }

    /// Convert to a hashable string key for use in HashMap/DashMap lookups.
    /// Handles f64 by converting to bits (lossless).
    pub fn to_hash_key(&self) -> String {
        match self {
            Value::Integer(i) => format!("i:{}", i),
            Value::Float(f) => format!("f:{}", f.to_bits()),
            Value::Text(s) => format!("t:{}", s),
            Value::Bool(b) => format!("b:{}", b),
            Value::Timestamp(t) => format!("ts:{}", t.as_micros()),
            _ => format!("{:?}", self),
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
