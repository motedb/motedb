//! Error types for MoteDB storage engine

use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Fragment error: {0}")]
    Fragment(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),
    
    #[error("Data corruption: {0}")]
    Corruption(String),
    
    #[error("Lock error: {0}")]
    Lock(String),
    
    #[error("File not found: {0}")]
    FileNotFound(std::path::PathBuf),
    
    #[error("Corrupted file: {0}")]
    CorruptedFile(std::path::PathBuf),
    
    // SQL-related errors
    #[error("Parse error: {0}")]
    ParseError(String),
    
    #[error("Type error: {0}")]
    TypeError(String),
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),
    
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    
    #[error("Unknown function: {0}")]
    UnknownFunction(String),
    
    #[error("Division by zero")]
    DivisionByZero,
    
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    
    /// 🚀 Phase 5: AUTO_INCREMENT overflow error
    #[error("AUTO_INCREMENT overflow for table '{0}': counter has reached i64::MAX")]
    AutoIncrementOverflow(String),

    /// Columnar segment store error
    #[error("Columnar store error: {0}")]
    Columnar(String),

    /// Segment file corrupted
    #[error("Segment file corrupted: {0}")]
    SegmentCorrupted(std::path::PathBuf),
}

// Alias for compatibility
pub type MoteDBError = StorageError;

impl From<bincode::Error> for StorageError {
    fn from(err: bincode::Error) -> Self {
        StorageError::Serialization(err.to_string())
    }
}
