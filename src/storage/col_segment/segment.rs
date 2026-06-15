use crate::storage::lsm::columnar::ColumnarSSTable;
use std::sync::Arc;
use std::time::Instant;

/// Immutable columnar segment = a `ColumnarSSTable` plus bookkeeping metadata.
/// File format is unchanged — this is a thin wrapper for the multi-segment layer.
pub struct Segment {
    /// Shared so merge cursors can hold a ref without cloning the SSTable.
    pub sst: Arc<ColumnarSSTable>,
    pub id: u64,
    pub row_count: usize,
    pub created_at: Instant,
}

impl Segment {
    pub fn open(path: &std::path::Path, id: u64) -> crate::Result<Self> {
        let sst = ColumnarSSTable::open(path)?;
        let row_count = sst.num_rows;
        Ok(Self { sst: Arc::new(sst), id, row_count, created_at: Instant::now() })
    }
}
