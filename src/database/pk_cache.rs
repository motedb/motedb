//! Bounded PK Lookup Cache
//!
//! LRU-bounded cache mapping PK values to RowIds.
//! Uses compact PkKey enum instead of String to reduce memory:
//! - Integer PK: 16 bytes (vs ~80 bytes with String)
//! - 50K entries ≈ 800KB (vs 4MB with String keys)

use crate::types::RowId;
use parking_lot::Mutex;
use std::num::NonZeroUsize;

/// Compact PK key — avoids String heap allocation
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PkKey {
    Int(i64),
    Float(u64),
    Text(Box<str>),
    Bool(bool),
    Null,
}

impl PkKey {
    pub fn from_value(value: &crate::types::Value) -> Self {
        match value {
            crate::types::Value::Integer(i) => PkKey::Int(*i),
            crate::types::Value::Float(f) => PkKey::Float(f.to_bits()),
            crate::types::Value::Text(s) => PkKey::Text(s.clone().into_boxed_str()),
            crate::types::Value::Bool(b) => PkKey::Bool(*b),
            crate::types::Value::Null => PkKey::Null,
            _ => PkKey::Null,
        }
    }

    /// Parse from legacy hash key format ("i:42", "f:1234", "t:hello")
    pub fn from_hash_key(s: &str) -> Self {
        if let Some(rest) = s.strip_prefix("i:") {
            if let Ok(i) = rest.parse::<i64>() {
                return PkKey::Int(i);
            }
        } else if let Some(rest) = s.strip_prefix("f:") {
            if let Ok(bits) = rest.parse::<u64>() {
                return PkKey::Float(bits);
            }
        } else if let Some(rest) = s.strip_prefix("t:") {
            return PkKey::Text(rest.into());
        } else if let Some(rest) = s.strip_prefix("b:") {
            if let Ok(b) = rest.parse::<bool>() {
                return PkKey::Bool(b);
            }
        }
        PkKey::Null
    }
}

/// Thread-safe LRU-bounded PK → RowId cache.
///
/// Memory: ~40 bytes/entry × capacity.
/// - 50K entries ≈ 2MB (default)
/// - 10K entries ≈ 400KB (edge/embedded)
pub struct PkLookupCache {
    cache: Mutex<lru::LruCache<PkKey, RowId>>,
}

impl PkLookupCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(capacity.max(1)).unwrap(),
            )),
        }
    }

    /// Insert a PK value → RowId mapping.
    pub fn insert(&self, key: PkKey, row_id: RowId) {
        let mut cache = self.cache.lock();
        cache.put(key, row_id);
    }

    /// Look up a PK value by hash key string (legacy compat).
    pub fn get(&self, key: &str) -> Option<RowId> {
        let mut cache = self.cache.lock();
        let pk_key = PkKey::from_hash_key(key);
        cache.get(&pk_key).copied()
    }

    /// Look up by compact PkKey (zero-allocation).
    pub fn get_pk(&self, key: &PkKey) -> Option<RowId> {
        let mut cache = self.cache.lock();
        cache.get(key).copied()
    }

    /// Remove a PK value (used during DELETE).
    pub fn remove(&self, key: &str) {
        let mut cache = self.cache.lock();
        let pk_key = PkKey::from_hash_key(key);
        cache.pop(&pk_key);
    }

    /// Remove by compact PkKey.
    pub fn remove_pk(&self, key: &PkKey) {
        let mut cache = self.cache.lock();
        cache.pop(key);
    }
}
