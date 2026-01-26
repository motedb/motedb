//! Disk-based graph storage for DiskANN
//!
//! Stores Vamana graph adjacency list on disk with LRU cache.
//! File: graph.bin
//!
//! Format:
//! [Header - 16 bytes]
//!   magic: u32 (0x4752_5048 = "GRPH")
//!   version: u32
//!   max_degree: u32
//!   node_count: u32
//!
//! [Node Records]
//! Each record:
//!   row_id: u64
//!   neighbor_count: u32
//!   neighbors: [u64; neighbor_count]

use crate::types::RowId;
use crate::{Result, StorageError};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: u32 = 0x4752_5048; // "GRPH"
const VERSION: u32 = 1;
const HEADER_SIZE: u64 = 16;

/// Disk-based graph with LRU cache + 🚀 **分层存储（热/冷数据分离）**
pub struct DiskGraph {
    max_degree: usize,
    file: Arc<RwLock<File>>,
    
    /// Index: row_id → file offset
    index: Arc<RwLock<HashMap<RowId, u64>>>,
    
    /// 🔥 **热数据层：LRU缓存（内存）**
    /// - 常访问的节点邻接表
    /// - 自动淘汰冷数据到磁盘
    /// 
    /// ✅ P1: Arc-wrapped values to avoid cloning large neighbor lists
    /// - Old: Clone Vec<RowId> (avg 64 * 8 = 512 bytes)
    /// - New: Clone Arc (8 bytes) - **98.4% memory saving**
    cache: Arc<Mutex<LruCache<RowId, Arc<Vec<RowId>>>>>,
    
    /// 🧊 **冷数据层：Pin到内存的热节点集合**
    /// - 永远不会被LRU淘汰
    /// - 用于medoid和高度数节点
    /// 
    /// ✅ P1: Arc-wrapped values for hot nodes too
    hot_nodes: Arc<RwLock<HashSet<RowId>>>,
    hot_cache: Arc<RwLock<HashMap<RowId, Arc<Vec<RowId>>>>>,
    
    /// Next write offset
    next_offset: Arc<Mutex<u64>>,
    
    /// Dirty flag
    dirty: Arc<RwLock<bool>>,
    
    file_path: PathBuf,
}

impl DiskGraph {
    /// Create new disk graph
    pub fn create(
        data_dir: impl AsRef<Path>,
        max_degree: usize,
        cache_capacity: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir).map_err(StorageError::Io)?;
        
        let file_path = data_dir.join("graph.bin");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;
        
        Self::write_header(&mut file, max_degree, 0)?;
        
        Ok(Self {
            max_degree,
            file: Arc::new(RwLock::new(file)),
            index: Arc::new(RwLock::new(HashMap::new())),
            cache: Arc::new(Mutex::new(
                LruCache::new(NonZeroUsize::new(cache_capacity).unwrap())
            )),
            hot_nodes: Arc::new(RwLock::new(HashSet::new())),
            hot_cache: Arc::new(RwLock::new(HashMap::new())),
            next_offset: Arc::new(Mutex::new(HEADER_SIZE)),
            dirty: Arc::new(RwLock::new(false)),
            file_path,
        })
    }
    
    /// Load existing disk graph
    pub fn load(
        data_dir: impl AsRef<Path>,
        cache_capacity: usize,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let file_path = data_dir.join("graph.bin");
        
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(StorageError::Io)?;
        
        let (max_degree, node_count) = Self::read_header(&mut file)?;
        let (index, next_offset) = Self::build_index(&mut file, node_count)?;
        
        Ok(Self {
            max_degree,
            file: Arc::new(RwLock::new(file)),
            index: Arc::new(RwLock::new(index)),
            cache: Arc::new(Mutex::new(
                LruCache::new(NonZeroUsize::new(cache_capacity).unwrap())
            )),
            hot_nodes: Arc::new(RwLock::new(HashSet::new())),
            hot_cache: Arc::new(RwLock::new(HashMap::new())),
            next_offset: Arc::new(Mutex::new(next_offset)),
            dirty: Arc::new(RwLock::new(false)),
            file_path,
        })
    }
    
    pub fn max_degree(&self) -> usize {
        self.max_degree
    }
    
    pub fn node_count(&self) -> usize {
        self.index.read().len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.index.read().is_empty()
    }
    
    /// 🔥 **Pin节点到热数据层（永远在内存中）**
    /// 
    /// **适用场景：**
    /// - Medoid节点（每次查询的起点）
    /// - 高度数节点（hub nodes）
    /// - 频繁访问的节点
    pub fn pin_hot_node(&self, node_id: RowId) {
        if !self.hot_nodes.read().contains(&node_id) {
            // 加载到热缓存
            if let Some(neighbors) = self.get_from_cache_or_disk(node_id) {
                self.hot_cache.write().insert(node_id, neighbors);
                self.hot_nodes.write().insert(node_id);
            }
        }
    }
    
    /// 🧊 **Unpin节点（允许LRU淘汰）**
    pub fn unpin_hot_node(&self, node_id: RowId) {
        self.hot_nodes.write().remove(&node_id);
        self.hot_cache.write().remove(&node_id);
    }
    
    /// 获取热节点数量
    pub fn hot_node_count(&self) -> usize {
        self.hot_nodes.read().len()
    }
    
    /// 🚀 **批量Pin高度数节点（自动识别hub nodes）**
    pub fn pin_high_degree_nodes(&self, top_k: usize) {
        let index = self.index.read();
        let mut degrees: Vec<(RowId, usize)> = index
            .keys()
            .filter_map(|&id| {
                let degree = self.neighbors(id).len();
                Some((id, degree))
            })
            .collect();
        
        degrees.sort_by(|a, b| b.1.cmp(&a.1));
        
        for (id, _) in degrees.into_iter().take(top_k) {
            self.pin_hot_node(id);
        }
    }
    
    /// Check if node exists in graph
    pub fn has_node(&self, node_id: RowId) -> bool {
        self.index.read().contains_key(&node_id)
    }
    
    pub fn node_ids(&self) -> Vec<RowId> {
        self.index.read().keys().copied().collect()
    }
    
    /// Add node (without neighbors)
    pub fn add_node(&self, node_id: RowId) {
        if self.index.read().contains_key(&node_id) {
            return;
        }
        *self.dirty.write() = true;
    }
    
    /// Get neighbors (with tiered caching: hot → LRU → disk)
    /// 
    /// ✅ P1: Returns Arc-wrapped Vec to avoid cloning in high-QPS scenarios
    pub fn neighbors(&self, node_id: RowId) -> Arc<Vec<RowId>> {
        // 1. 🔥 Check hot cache first (pinned nodes)
        {
            let hot_cache = self.hot_cache.read();
            if let Some(neighbors) = hot_cache.get(&node_id) {
                return Arc::clone(neighbors);  // ✅ P1: Clone Arc (8 bytes) instead of Vec (512 bytes)
            }
        }
        
        // 2. 🌡️ Check LRU cache
        {
            let mut cache = self.cache.lock();
            if let Some(neighbors) = cache.get(&node_id) {
                return Arc::clone(neighbors);  // ✅ P1: Clone Arc (8 bytes)
            }
        }
        
        // 3. 🧊 Read from disk (cold data)
        match self.get_from_cache_or_disk(node_id) {
            Some(neighbors) => neighbors,
            None => Arc::new(Vec::new()),
        }
    }
    
    /// Internal helper: get from cache or disk
    fn get_from_cache_or_disk(&self, node_id: RowId) -> Option<Arc<Vec<RowId>>> {
        // Get offset
        let offset = match self.index.read().get(&node_id) {
            Some(&off) => off,
            None => return None,
        };
        
        // Read from disk
        match self.read_neighbors_at(offset) {
            Ok(neighbors) => {
                // Add to LRU cache (Arc-wrapped)
                let arc_neighbors = Arc::new(neighbors);
                self.cache.lock().put(node_id, Arc::clone(&arc_neighbors));
                Some(arc_neighbors)
            }
            Err(_) => None,
        }
    }
    
    /// Set neighbors (replaces existing) with tiered caching
    pub fn set_neighbors(&self, node_id: RowId, mut neighbors: Vec<RowId>) -> Result<()> {
        // Remove duplicates and self-loops
        neighbors.retain(|&id| id != node_id);
        neighbors.sort_unstable();
        neighbors.dedup();
        
        // Enforce degree limit
        if neighbors.len() > self.max_degree {
            neighbors.truncate(self.max_degree);
        }
        
        let offset = {
            let mut next_offset = self.next_offset.lock();
            let offset = *next_offset;
            
            self.write_neighbors_at(node_id, &neighbors, offset)?;
            
            let record_size = 8 + 4 + (neighbors.len() * 8);
            *next_offset += record_size as u64;
            
            offset
        };
        
        self.index.write().insert(node_id, offset);
        
        // 🔥 Update tiered cache (Arc-wrapped)
        let arc_neighbors = Arc::new(neighbors);
        if self.hot_nodes.read().contains(&node_id) {
            // Hot node: update hot cache
            self.hot_cache.write().insert(node_id, Arc::clone(&arc_neighbors));
        } else {
            // Normal node: update LRU cache
            self.cache.lock().put(node_id, arc_neighbors);
        }
        
        *self.dirty.write() = true;
        
        Ok(())
    }
    
    /// Add edge
    pub fn add_edge(&self, from: RowId, to: RowId) -> Result<()> {
        if from == to {
            return Ok(());
        }
        
        let neighbors_arc = self.neighbors(from);
        
        if neighbors_arc.contains(&to) {
            return Ok(());
        }
        
        if neighbors_arc.len() >= self.max_degree {
            return Err(StorageError::InvalidData("Max degree exceeded".to_string()));
        }
        
        // Clone Arc's inner Vec to modify
        let mut neighbors = (*neighbors_arc).clone();
        neighbors.push(to);
        self.set_neighbors(from, neighbors)
    }
    
    /// Check if edge exists
    pub fn has_edge(&self, from: RowId, to: RowId) -> bool {
        self.neighbors(from).contains(&to)
    }
    
    /// Get degree
    pub fn degree(&self, node_id: RowId) -> usize {
        self.neighbors(node_id).len()
    }
    
    /// Remove node
    pub fn remove_node(&self, node_id: RowId) -> Arc<Vec<RowId>> {
        let neighbors = self.neighbors(node_id);
        
        self.index.write().remove(&node_id);
        // 🚀 P2: Only invalidate this node (not full clear)
        self.cache.lock().pop(&node_id);
        *self.dirty.write() = true;
        
        neighbors
    }
    
    /// 🚀 P2: Batch remove nodes with smart cache invalidation
    /// 
    /// More efficient than calling `remove_node()` multiple times
    /// as it only locks once.
    pub fn batch_remove_nodes(&self, node_ids: &[RowId]) -> HashMap<RowId, Vec<RowId>> {
        let mut all_neighbors = HashMap::new();
        
        {
            let mut index = self.index.write();
            let mut cache = self.cache.lock();
            
            for &node_id in node_ids {
                // Get neighbors before removal
                if let Some(&offset) = index.get(&node_id) {
                    let neighbors = self.read_neighbors_at(offset).unwrap_or_default();
                    all_neighbors.insert(node_id, neighbors);
                }
                
                // Remove from index and cache
                index.remove(&node_id);
                cache.pop(&node_id);
            }
        }
        
        *self.dirty.write() = true;
        all_neighbors
    }
    
    /// Flush to disk (OPTIMIZED: incremental write, skip full rewrite during batch insert)
    pub fn flush(&self) -> Result<()> {
        if !*self.dirty.read() {
            return Ok(());
        }
        
        // Update header with current node count
        let node_count = self.index.read().len();
        {
            let mut file = self.file.write();
            Self::write_header(&mut file, self.max_degree, node_count)?;
            file.sync_all().map_err(StorageError::Io)?;
        }
        
        *self.dirty.write() = false;
        Ok(())
    }
    
    /// Compact graph file (full rewrite for defragmentation)
    /// Call this periodically, not on every flush
    pub fn compact(&self) -> Result<()> {
        // Rewrite file
        let temp_path = self.file_path.with_extension("tmp");
        
        {
            let mut temp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&temp_path)
                .map_err(StorageError::Io)?;
            
            let index = self.index.read();
            Self::write_header(&mut temp_file, self.max_degree, index.len())?;
            
            let mut new_index = HashMap::new();
            let mut offset = HEADER_SIZE;
            
            for (&node_id, _) in index.iter() {
                let neighbors = self.neighbors(node_id);
                
                // Write to temp file
                temp_file.write_all(&node_id.to_le_bytes())
                    .map_err(StorageError::Io)?;
                temp_file.write_all(&(neighbors.len() as u32).to_le_bytes())
                    .map_err(StorageError::Io)?;
                
                for &neighbor in neighbors.iter() {  // ✅ P1: Arc deref via iter()
                    temp_file.write_all(&neighbor.to_le_bytes())
                        .map_err(StorageError::Io)?;
                }
                
                new_index.insert(node_id, offset);
                
                let record_size = 8 + 4 + (neighbors.len() * 8);
                offset += record_size as u64;
            }
            
            temp_file.sync_all().map_err(StorageError::Io)?;
            
            drop(index);
            *self.index.write() = new_index;
            *self.next_offset.lock() = offset;
        }
        
        // Replace file
        std::fs::rename(&temp_path, &self.file_path)
            .map_err(StorageError::Io)?;
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)
            .map_err(StorageError::Io)?;
        
        *self.file.write() = file;
        *self.dirty.write() = false;
        
        Ok(())
    }
    
    pub fn clear(&self) {
        self.index.write().clear();
        self.cache.lock().clear();
        *self.next_offset.lock() = HEADER_SIZE;
        *self.dirty.write() = true;
    }
    
    /// Get memory usage (cache only)
    pub fn memory_usage(&self) -> usize {
        let cache_size = self.cache.lock().len();
        // Approximate: row_id (8) + degree (4) + neighbors (avg 32 * 8)
        cache_size * (8 + 4 + 32 * 8)
    }
    
    /// Get disk usage (approximate)
    pub fn disk_usage(&self) -> usize {
        let count = self.index.read().len();
        // Approximate: row_id (8) + degree (4) + neighbors (avg 32 * 8)
        count * (8 + 4 + 32 * 8)
    }
    
    // --- Private helpers ---
    
    fn write_header(file: &mut File, max_degree: usize, node_count: usize) -> Result<()> {
        file.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
        
        file.write_all(&MAGIC.to_le_bytes()).map_err(StorageError::Io)?;
        file.write_all(&VERSION.to_le_bytes()).map_err(StorageError::Io)?;
        file.write_all(&(max_degree as u32).to_le_bytes()).map_err(StorageError::Io)?;
        file.write_all(&(node_count as u32).to_le_bytes()).map_err(StorageError::Io)?;
        
        Ok(())
    }
    
    fn read_header(file: &mut File) -> Result<(usize, usize)> {
        file.seek(SeekFrom::Start(0)).map_err(StorageError::Io)?;
        
        let mut buf = [0u8; 4];
        
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let magic = u32::from_le_bytes(buf);
        if magic != MAGIC {
            return Err(StorageError::InvalidData("Invalid graph file".to_string()));
        }
        
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let _version = u32::from_le_bytes(buf);
        
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let max_degree = u32::from_le_bytes(buf) as usize;
        
        file.read_exact(&mut buf).map_err(StorageError::Io)?;
        let node_count = u32::from_le_bytes(buf) as usize;
        
        Ok((max_degree, node_count))
    }
    
    fn write_neighbors_at(&self, node_id: RowId, neighbors: &[RowId], offset: u64) -> Result<()> {
        let mut file = self.file.write();
        
        file.seek(SeekFrom::Start(offset)).map_err(StorageError::Io)?;
        
        file.write_all(&node_id.to_le_bytes()).map_err(StorageError::Io)?;
        file.write_all(&(neighbors.len() as u32).to_le_bytes()).map_err(StorageError::Io)?;
        
        for &neighbor in neighbors {
            file.write_all(&neighbor.to_le_bytes()).map_err(StorageError::Io)?;
        }
        
        Ok(())
    }
    
    fn read_neighbors_at(&self, offset: u64) -> Result<Vec<RowId>> {
        let mut file = self.file.write();
        
        file.seek(SeekFrom::Start(offset)).map_err(StorageError::Io)?;
        
        // Read node_id
        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8).map_err(StorageError::Io)?;
        
        // Read neighbor count
        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4).map_err(StorageError::Io)?;
        let count = u32::from_le_bytes(buf4) as usize;
        
        // Read neighbors
        let mut neighbors = Vec::with_capacity(count);
        for _ in 0..count {
            file.read_exact(&mut buf8).map_err(StorageError::Io)?;
            neighbors.push(u64::from_le_bytes(buf8));
        }
        
        Ok(neighbors)
    }
    
    fn build_index(file: &mut File, node_count: usize) -> Result<(HashMap<RowId, u64>, u64)> {
        let mut index = HashMap::with_capacity(node_count);
        let mut offset = HEADER_SIZE;
        
        file.seek(SeekFrom::Start(offset)).map_err(StorageError::Io)?;
        
        for _ in 0..node_count {
            let mut buf8 = [0u8; 8];
            file.read_exact(&mut buf8).map_err(StorageError::Io)?;
            let node_id = u64::from_le_bytes(buf8);
            
            let mut buf4 = [0u8; 4];
            file.read_exact(&mut buf4).map_err(StorageError::Io)?;
            let neighbor_count = u32::from_le_bytes(buf4) as usize;
            
            index.insert(node_id, offset);
            
            // Skip neighbors
            file.seek(SeekFrom::Current((neighbor_count * 8) as i64))
                .map_err(StorageError::Io)?;
            
            let record_size = 8 + 4 + (neighbor_count * 8);
            offset += record_size as u64;
        }
        
        Ok((index, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_disk_graph_create() {
        let temp_dir = TempDir::new().unwrap();
        let graph = DiskGraph::create(temp_dir.path(), 32, 1000).unwrap();
        
        assert_eq!(graph.max_degree(), 32);
        assert!(graph.is_empty());
    }
    
    #[test]
    fn test_disk_graph_neighbors() {
        let temp_dir = TempDir::new().unwrap();
        let graph = DiskGraph::create(temp_dir.path(), 32, 1000).unwrap();
        
        graph.set_neighbors(1, vec![2, 3, 4]).unwrap();
        
        let neighbors = graph.neighbors(1);
        assert_eq!(neighbors.len(), 3);
        assert!(neighbors.contains(&2));
    }
    
    #[test]
    fn test_disk_graph_persistence() {
        let temp_dir = TempDir::new().unwrap();
        
        {
            let graph = DiskGraph::create(temp_dir.path(), 32, 1000).unwrap();
            graph.set_neighbors(1, vec![2, 3]).unwrap();
            graph.set_neighbors(2, vec![1, 3]).unwrap();
            graph.flush().unwrap();
        }
        
        // Reload
        {
            let graph = DiskGraph::load(temp_dir.path(), 1000).unwrap();
            assert_eq!(graph.node_count(), 2);
            
            let n1 = graph.neighbors(1);
            assert!(n1.contains(&2) && n1.contains(&3));
        }
    }
}
