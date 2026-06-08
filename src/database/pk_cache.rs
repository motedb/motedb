//! Bounded PK Lookup Cache
//!
//! LRU-bounded cache mapping PK values to RowIds.
//! Uses compact PkKey enum instead of String to reduce memory:
//! - Integer PK: 16 bytes (vs ~80 bytes with String)
//! - 50K entries ≈ 800KB (vs 4MB with String keys)

use crate::types::RowId;
use parking_lot::RwLock;
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
            crate::types::Value::Text(s) => PkKey::Text(s.as_str().to_string().into_boxed_str()),
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
    cache: RwLock<lru::LruCache<PkKey, RowId>>,
}

impl PkLookupCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: RwLock::new(lru::LruCache::new(
                NonZeroUsize::new(capacity.max(1)).unwrap(),
            )),
        }
    }

    /// Insert a PK value → RowId mapping.
    pub fn insert(&self, key: PkKey, row_id: RowId) {
        let mut cache = self.cache.write();
        cache.put(key, row_id);
    }

    /// Look up a PK value by hash key string (legacy compat).
    pub fn get(&self, key: &str) -> Option<RowId> {
        let mut cache = self.cache.write(); // LRU touch requires write
        let pk_key = PkKey::from_hash_key(key);
        cache.get(&pk_key).copied()
    }

    /// Look up by compact PkKey (zero-allocation).
    pub fn get_pk(&self, key: &PkKey) -> Option<RowId> {
        let mut cache = self.cache.write(); // LRU touch requires write
        cache.get(key).copied()
    }

    /// Remove a PK value (used during DELETE).
    pub fn remove(&self, key: &str) {
        let mut cache = self.cache.write();
        let pk_key = PkKey::from_hash_key(key);
        cache.pop(&pk_key);
    }

    /// Remove by compact PkKey.
    pub fn remove_pk(&self, key: &PkKey) {
        let mut cache = self.cache.write();
        cache.pop(key);
    }

    /// Atomically check-and-insert: returns `Err(row_id)` if key already exists,
    /// or `Ok(())` after inserting the new mapping.
    pub fn insert_if_absent(&self, key: PkKey, row_id: RowId) -> Result<(), RowId> {
        let mut cache = self.cache.write();
        if let Some(&existing) = cache.get(&key) {
            return Err(existing);
        }
        cache.put(key, row_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pk_key_from_integer() {
        let key = PkKey::from_value(&crate::types::Value::Integer(42));
        assert_eq!(key, PkKey::Int(42));
    }

    #[test]
    fn test_pk_key_from_text() {
        let key = PkKey::from_value(&crate::types::Value::Text("hello".into()));
        match key {
            PkKey::Text(s) => assert_eq!(&*s, "hello"),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn test_pk_key_from_null() {
        let key = PkKey::from_value(&crate::types::Value::Null);
        assert_eq!(key, PkKey::Null);
    }

    #[test]
    fn test_insert_and_get() {
        let cache = PkLookupCache::new(100);
        cache.insert(PkKey::Int(1), 100);
        cache.insert(PkKey::Int(2), 200);

        assert_eq!(cache.get_pk(&PkKey::Int(1)), Some(100));
        assert_eq!(cache.get_pk(&PkKey::Int(2)), Some(200));
        assert_eq!(cache.get_pk(&PkKey::Int(99)), None);
    }

    #[test]
    fn test_remove() {
        let cache = PkLookupCache::new(100);
        cache.insert(PkKey::Int(1), 100);
        cache.remove_pk(&PkKey::Int(1));
        assert_eq!(cache.get_pk(&PkKey::Int(1)), None);
    }

    #[test]
    fn test_overwrite() {
        let cache = PkLookupCache::new(100);
        cache.insert(PkKey::Int(1), 100);
        cache.insert(PkKey::Int(1), 999);
        assert_eq!(cache.get_pk(&PkKey::Int(1)), Some(999));
    }

    #[test]
    fn test_lru_eviction() {
        let cache = PkLookupCache::new(10);
        // Insert 20 entries — first 10 should be evicted
        for i in 0..20i64 {
            cache.insert(PkKey::Int(i), i as RowId);
        }
        // First entries should be gone
        assert_eq!(cache.get_pk(&PkKey::Int(0)), None);
        // Last entry should be present
        assert!(cache.get_pk(&PkKey::Int(19)).is_some());
    }
}
