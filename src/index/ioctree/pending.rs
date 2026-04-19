//! Pending Buffer: bounded in-memory buffer for batching inserts/deletes
//!
//! Holds up to PENDING_CAPACITY points before flushing to LeafStore.

#![allow(dead_code)]

use super::node::IndexedPoint3D;
use std::collections::HashSet;

const PENDING_CAPACITY: usize = 2048;

pub struct PendingBuffer {
    inserts: Vec<IndexedPoint3D>,
    deletes: HashSet<u64>,
}

impl PendingBuffer {
    pub fn new() -> Self {
        Self {
            inserts: Vec::with_capacity(PENDING_CAPACITY),
            deletes: HashSet::new(),
        }
    }

    /// Add an insert. Returns true if buffer is now full.
    pub fn push_insert(&mut self, point: IndexedPoint3D) -> bool {
        // Remove from pending deletes if it was previously marked for deletion
        self.deletes.remove(&point.row_id);
        self.inserts.push(point);
        self.inserts.len() >= PENDING_CAPACITY
    }

    /// Add a delete by row_id.
    pub fn push_delete(&mut self, row_id: u64) {
        // Remove from pending inserts if present (cancel out)
        if let Some(pos) = self.inserts.iter().position(|p| p.row_id == row_id) {
            self.inserts.swap_remove(pos);
        } else {
            self.deletes.insert(row_id);
        }
    }

    pub fn is_full(&self) -> bool {
        self.inserts.len() >= PENDING_CAPACITY
    }

    /// Drain all pending inserts and deletes.
    pub fn drain(&mut self) -> (Vec<IndexedPoint3D>, HashSet<u64>) {
        let inserts = std::mem::take(&mut self.inserts);
        let deletes = std::mem::take(&mut self.deletes);
        (inserts, deletes)
    }

    pub fn len(&self) -> usize {
        self.inserts.len() + self.deletes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty() && self.deletes.is_empty()
    }

    /// Get pending inserts (for query merging)
    pub fn inserts(&self) -> &[IndexedPoint3D] {
        &self.inserts
    }

    /// Check if a row_id is pending deletion
    pub fn is_pending_delete(&self, row_id: u64) -> bool {
        self.deletes.contains(&row_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_buffer_basic() {
        let mut buf = PendingBuffer::new();
        assert!(buf.is_empty());

        buf.push_insert(IndexedPoint3D { x: 1.0, y: 2.0, z: 3.0, row_id: 1 });
        assert_eq!(buf.len(), 1);
        assert!(!buf.is_full());

        buf.push_delete(99);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_cancel_out() {
        let mut buf = PendingBuffer::new();
        buf.push_insert(IndexedPoint3D { x: 1.0, y: 2.0, z: 3.0, row_id: 42 });
        buf.push_delete(42);
        assert!(buf.is_empty()); // Insert was cancelled by delete
    }
}
