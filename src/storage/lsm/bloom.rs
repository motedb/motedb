//! Bloom Filter for fast negative lookups
//!
//! ## Performance
//! - False positive rate: 1% (10 bits/key)
//! - Lookup: O(k) where k=7 hash functions
//! - Memory: 10 bits per key (~1.25 bytes/key)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Bloom filter for SSTable
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u8>,
    
    /// Number of hash functions
    num_hashes: u32,
    
    /// Number of bits
    num_bits: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter
    ///
    /// # Parameters
    /// - `num_keys`: Expected number of keys
    /// - `bits_per_key`: Bits allocated per key (typically 10 for 1% FPR)
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let num_bits = num_keys * bits_per_key;
        let num_bytes = (num_bits + 7) / 8;
        
        // Optimal number of hash functions: k = (m/n) * ln(2)
        // Where m = total bits, n = number of keys
        let num_hashes = ((bits_per_key as f64) * 0.693).ceil() as u32;
        let num_hashes = num_hashes.max(1).min(30); // Clamp to reasonable range
        
        Self {
            bits: vec![0u8; num_bytes],
            num_hashes,
            num_bits,
        }
    }
    
    /// Create from existing data
    pub fn from_bytes(bits: Vec<u8>, num_hashes: u32) -> Self {
        let num_bits = bits.len() * 8;
        Self {
            bits,
            num_hashes,
            num_bits,
        }
    }
    
    /// Insert a key
    pub fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i);
            let bit_pos = (hash as usize) % self.num_bits;
            self.set_bit(bit_pos);
        }
    }
    
    /// Check if key might exist (may have false positives)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i);
            let bit_pos = (hash as usize) % self.num_bits;
            if !self.get_bit(bit_pos) {
                return false; // Definitely not in set
            }
        }
        true // Might be in set (or false positive)
    }
    
    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.num_hashes as u32).to_le_bytes());
        buf.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }
    
    /// Deserialize from bytes
    pub fn from_bytes_full(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        
        let num_hashes = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let num_bits = u64::from_le_bytes([
            data[4], data[5], data[6], data[7],
            data[8], data[9], data[10], data[11]
        ]) as usize;
        
        let bits = data[12..].to_vec();
        
        Some(Self {
            bits,
            num_hashes,
            num_bits,
        })
    }
    
    /// Get byte size
    pub fn byte_size(&self) -> usize {
        12 + self.bits.len() // header + data
    }
    
    // Internal helpers
    
    fn hash(&self, key: &[u8], seed: u32) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    fn set_bit(&mut self, pos: usize) {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
    }
    
    fn get_bit(&self, pos: usize) -> bool {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        (self.bits[byte_idx] & (1 << bit_idx)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_operations() {
        let mut bloom = BloomFilter::new(100, 10);
        
        // Insert keys
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        bloom.insert(b"key3");
        
        // Should find inserted keys
        assert!(bloom.may_contain(b"key1"));
        assert!(bloom.may_contain(b"key2"));
        assert!(bloom.may_contain(b"key3"));
        
        // Should not find non-existent keys (with high probability)
        assert!(!bloom.may_contain(b"key4"));
        assert!(!bloom.may_contain(b"key5"));
    }
    
    #[test]
    fn test_false_positive_rate() {
        let num_keys = 1000;
        let bits_per_key = 10;
        let mut bloom = BloomFilter::new(num_keys, bits_per_key);
        
        // Insert keys
        for i in 0..num_keys {
            let key = format!("key_{}", i);
            bloom.insert(key.as_bytes());
        }
        
        // Test for false positives
        let mut false_positives = 0;
        let test_count = 10000;
        
        for i in num_keys..(num_keys + test_count) {
            let key = format!("key_{}", i);
            if bloom.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }
        
        let fpr = false_positives as f64 / test_count as f64;
        println!("False positive rate: {:.2}%", fpr * 100.0);
        
        // Should be around 1% (allow up to 3% for small sample)
        assert!(fpr < 0.03, "FPR too high: {:.2}%", fpr * 100.0);
    }
    
    #[test]
    fn test_serialization() {
        let mut bloom = BloomFilter::new(100, 10);
        bloom.insert(b"key1");
        bloom.insert(b"key2");
        
        // Serialize
        let bytes = bloom.to_bytes();
        
        // Deserialize
        let bloom2 = BloomFilter::from_bytes_full(&bytes).unwrap();
        
        // Verify
        assert!(bloom2.may_contain(b"key1"));
        assert!(bloom2.may_contain(b"key2"));
        assert!(!bloom2.may_contain(b"nonexistent"));
    }
    
    #[test]
    fn test_empty_filter() {
        let bloom = BloomFilter::new(100, 10); // Create with capacity but no inserts
        assert!(!bloom.may_contain(b"any_key"));
    }
}
