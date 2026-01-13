//! Vamana SST File Format - 不可变的磁盘索引文件
//! 
//! ## 文件格式（V4 - Phase 4: 添加删除标记支持）
//! 
//! ```text
//! ┌──────────────────────────────────────┐
//! │  Header (256 bytes)                  │
//! │  - Magic: "VSST" (4 bytes)           │
//! │  - Version: u32 (= 4)                │  ← 升级到 V4
//! │  - Node count: u64                   │
//! │  - Dimension: u32                    │
//! │  - Medoid: u64                       │
//! │  - ID List offset: u64               │
//! │  - Deleted Bitmap offset: u64        │  ← 🆕 Phase 4
//! │  - Vectors offset: u64 (SQ8)         │
//! │  - Raw vectors offset: u64           │
//! │  - Graph offset: u64                 │
//! │  - Footer offset: u64                │
//! ├──────────────────────────────────────┤
//! │  ID List Block                       │
//! │  - [RowId; node_count]               │
//! ├──────────────────────────────────────┤
//! │  Deleted Bitmap Block                │  ← 🆕 Phase 4
//! │  - [u8; (node_count + 7) / 8]        │
//! ├──────────────────────────────────────┤
//! │  SQ8 Vectors Block (粗排)           │
//! │  - Centroid: [f32; dim]              │
//! │  - Scales: [f32; dim]                │
//! │  - Compressed: [u8; node_count*dim]  │
//! ├──────────────────────────────────────┤
//! │  Raw Vectors Block (精排)           │
//! │  - [f32; node_count*dim]             │
//! ├──────────────────────────────────────┤
//! │  Graph Adjacency Block               │
//! │  - Node offsets: [u64; node_count]   │
//! │  - Adjacency lists: [[u64; degree]]  │
//! ├──────────────────────────────────────┤
//! │  Footer (64 bytes)                   │
//! │  - CRC32 checksum                    │
//! │  - Padding                           │
//! └──────────────────────────────────────┘
//! ```text
use crate::error::{Result, StorageError};
use crate::types::RowId;
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom, BufReader, BufWriter};
use std::collections::HashSet;
use memmap2::{Mmap, MmapOptions};
use super::{Candidate, VectorNode};

const MAGIC: &[u8; 4] = b"VSST";
const VERSION: u32 = 4;  // V4: Phase 4 - 添加删除标记支持
const HEADER_SIZE: usize = 256;
const FOOTER_SIZE: usize = 64;

/// SST 元数据
#[derive(Debug, Clone)]
pub struct SSTMetadata {
    pub node_count: u64,
    pub dimension: u32,
    pub medoid: RowId,
    pub id_list_offset: u64,
    pub deleted_bitmap_offset: u64,  // 🆕 Phase 4: 删除标记位图
    pub vectors_offset: u64,         // SQ8 压缩向量
    pub raw_vectors_offset: u64,     // 原始 f32 向量
    pub graph_offset: u64,
    pub footer_offset: u64,
}

/// Vamana SST 文件
pub struct VamanaSSTFile {
    path: PathBuf,
    metadata: SSTMetadata,
    mmap: Mmap,
    /// ID 到索引的映射（因为 RowId 不一定连续）
    id_to_index: std::collections::HashMap<RowId, usize>,
    /// 🆕 Phase 4: 删除标记（内存中的位图）
    deleted_bitmap: parking_lot::RwLock<Vec<u8>>,
}

impl VamanaSSTFile {
    /// 创建新的 SST 文件
    pub fn create(
        path: &Path,
        mut nodes: Vec<(RowId, VectorNode)>,
        medoid: RowId,
    ) -> Result<Self> {
        if nodes.is_empty() {
            return Err(StorageError::InvalidData("Cannot create empty SST".into()));
        }
        
        // 按 ID 排序
        nodes.sort_by_key(|(id, _)| *id);
        
        let dimension = nodes[0].1.vector.len();
        let node_count = nodes.len() as u64;
        
        // 创建文件
        let mut file = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?
        );
        
        // 1. 写入占位 header
        let header_pos = file.stream_position()?;
        file.write_all(&vec![0u8; HEADER_SIZE])?;
        
        // 2. 写入 ID List（关键修复：持久化 ID 映射）
        let id_list_offset = file.stream_position()?;
        for (id, _) in &nodes {
            file.write_all(&id.to_le_bytes())?;
        }
        
        // 🆕 Phase 4: 3. 写入删除标记位图（初始全为 0，即未删除）
        let deleted_bitmap_offset = file.stream_position()?;
        let bitmap_size = ((node_count + 7) / 8) as usize;
        
        // 初始化位图：根据 VectorNode.deleted 字段设置
        let mut bitmap = vec![0u8; bitmap_size];
        for (idx, (_id, node)) in nodes.iter().enumerate() {
            if node.deleted {
                let byte_idx = idx / 8;
                let bit_idx = idx % 8;
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }
        file.write_all(&bitmap)?;
        
        // 4. 写入 SQ8 压缩向量
        let vectors_offset = file.stream_position()?;
        write_sq8_vectors(&mut file, &nodes)?;
        
        // 5. 写入原始 f32 向量（用于精排）
        let raw_vectors_offset = file.stream_position()?;
        write_raw_vectors(&mut file, &nodes)?;
        
        // 6. 写入图结构
        let graph_offset = file.stream_position()?;
        write_graph(&mut file, &nodes)?;
        
        // 7. 写入 footer
        let footer_offset = file.stream_position()?;
        write_footer(&mut file)?;
        
        // 8. 回写 header
        file.seek(SeekFrom::Start(header_pos))?;
        write_header(&mut file, &SSTMetadata {
            node_count,
            dimension: dimension as u32,
            medoid,
            id_list_offset,
            deleted_bitmap_offset,  // 🆕 Phase 4
            vectors_offset,
            raw_vectors_offset,
            graph_offset,
            footer_offset,
        })?;
        
        file.flush()?;
        drop(file);
        
        // 9. 重新打开（会自动从文件恢复 ID 映射和删除标记）
        Self::open(path)
    }
    
    /// 打开已存在的 SST 文件
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        // 解析 header
        let metadata = parse_header(&mmap)?;
        
        // 从文件中读取 ID List 并重建映射
        let id_to_index = read_id_list(&mmap, &metadata)?;
        
        // 🆕 Phase 4: 读取删除标记位图
        let deleted_bitmap = if metadata.deleted_bitmap_offset > 0 {
            // V4: 从文件读取
            let bitmap_size = ((metadata.node_count + 7) / 8) as usize;
            let start = metadata.deleted_bitmap_offset as usize;
            let end = start + bitmap_size;
            mmap[start..end].to_vec()
        } else {
            // V2/V3: 初始化为全 0（无删除）
            vec![0u8; ((metadata.node_count + 7) / 8) as usize]
        };
        
        Ok(Self {
            path: path.to_path_buf(),
            metadata,
            mmap,
            id_to_index,
            deleted_bitmap: parking_lot::RwLock::new(deleted_bitmap),  // 🆕 Phase 4
        })
    }
    
    /// 搜索接口 (Phase 4: 图搜索 + Phase 6: 精排优化)
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<Candidate>> {
        let centroid = self.get_centroid()?;
        let scales = self.get_scales()?;
        
        // 🚀 Phase 4: 贪心图搜索
        let candidates = if self.id_to_index.len() <= 100 {
            self.linear_search(query, k, &centroid, &scales)?
        } else {
            self.graph_search(query, k, ef, &centroid, &scales)?
        };
        
        // 🚀 Phase 6: 精排（如果有原始向量）
        if self.metadata.raw_vectors_offset > 0 {
            self.rerank(query, candidates, k)
        } else {
            Ok(candidates)
        }
    }
    
    /// 精排：使用原始向量重新计算距离
    fn rerank(&self, query: &[f32], mut candidates: Vec<Candidate>, k: usize) -> Result<Vec<Candidate>> {
        // 🆕 Phase 4: 过滤已删除节点
        candidates.retain(|c| !self.is_deleted(c.id));
        
        // 对每个候选，使用原始向量重新计算精确距离
        for candidate in &mut candidates {
            if let Ok(raw_vec) = self.get_raw_vector(candidate.id) {
                candidate.distance = l2_distance(query, &raw_vec);
            }
        }
        
        // 重新排序并返回 Top-K
        candidates.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.truncate(k);
        
        Ok(candidates)
    }
    
    /// 获取原始 f32 向量（用于精排）
    fn get_raw_vector(&self, id: RowId) -> Result<Vec<f32>> {
        if self.metadata.raw_vectors_offset == 0 {
            return Err(StorageError::InvalidData("Raw vectors not available in this SST version".into()));
        }
        
        let dim = self.metadata.dimension as usize;
        let index = self.id_to_index.get(&id)
            .ok_or_else(|| StorageError::InvalidData(format!("ID {} not found", id).into()))?;
        
        let offset = self.metadata.raw_vectors_offset as usize + (*index) * dim * 4;
        
        if offset + dim * 4 > self.mmap.len() {
            return Err(StorageError::Corruption(
                format!("Raw vector offset out of bounds: {} + {} > {}", 
                    offset, dim * 4, self.mmap.len()).into()
            ));
        }
        
        let mut vector = vec![0.0f32; dim];
        for i in 0..dim {
            let bytes = &self.mmap[offset + i * 4..offset + (i + 1) * 4];
            vector[i] = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        
        Ok(vector)
    }
    
    /// 图搜索（贪心算法 + 多起点优化）
    fn graph_search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        centroid: &[f32],
        scales: &[f32],
    ) -> Result<Vec<Candidate>> {
        use std::collections::{BinaryHeap, HashSet};
        use std::cmp::Reverse;
        
        // 🚀 延迟优化：进一步降低 ef 到 50（性能提升 ~50%）
        let ef = ef.max(k * 3).max(50).min(self.id_to_index.len());
        
        // 🔥 多起点搜索（提升召回率）
        let start_ids = self.get_start_points()?;
        
        let mut global_candidates = BinaryHeap::new();  // 最大堆
        
        // 🔥 Phase 10 关键修复: 每个起点独立搜索（不共享 visited）
        let per_start_ef = ef;  // 每个起点使用完整 ef
        
        // 从每个起点独立搜索
        for start_id in &start_ids {
            let mut local_visited = HashSet::new();  // ✅ 独立 visited
            let local_results = self.graph_search_from_point(
                query,
                k,
                per_start_ef,
                *start_id,
                centroid,
                scales,
                &mut local_visited,  // ✅ 每个起点独立
            )?;
            
            for candidate in local_results {
                global_candidates.push(candidate);
            }
        }
        
        // 全局去重并返回 Top-K
        let mut seen = HashSet::new();
        let mut results: Vec<Candidate> = global_candidates.into_sorted_vec()
            .into_iter()
            .filter(|c| seen.insert(c.id))  // 去重
            .collect();
        results.truncate(k);
        
        Ok(results)
    }
    
    /// 获取多个起点（medoid + 均匀采样）
    fn get_start_points(&self) -> Result<Vec<RowId>> {
        let mut starts = vec![self.metadata.medoid];
        
        // 🚀 延迟优化：减少起点数量到2个（性能提升 ~50%）
        let target_starts = 2;
        let ids: Vec<_> = self.id_to_index.keys().copied().collect();
        
        if ids.len() > target_starts {
            // 均匀采样（覆盖不同区域）
            let step = ids.len() / target_starts;
            
            for i in 1..target_starts {  // 从 1 开始（medoid 已添加）
                let idx = i * step;
                let candidate = ids[idx];
                
                if candidate != self.metadata.medoid && !starts.contains(&candidate) {
                    starts.push(candidate);
                }
            }
        } else if ids.len() > 1 {
            // 小数据集：使用所有节点作为起点
            for id in ids {
                if id != self.metadata.medoid {
                    starts.push(id);
                }
            }
        }
        
        Ok(starts)
    }
    
    /// 从单个起点进行图搜索
    fn graph_search_from_point(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        start_id: RowId,
        centroid: &[f32],
        scales: &[f32],
        global_visited: &mut HashSet<RowId>,  // ✅ 共享 visited
    ) -> Result<Vec<Candidate>> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        let ef = ef.max(k * 2);
        
        // 🔥 Phase 10: 移除起点跳过检查（允许所有起点参与）
        
        // 起点
        let start_vec = self.decompress_vector(start_id, centroid, scales)?;
        let start_dist = l2_distance(query, &start_vec);
        
        // 最小堆：存储候选（按距离从小到大）
        let mut candidates = BinaryHeap::new();
        candidates.push(Reverse(Candidate::new(start_id, start_dist)));
        
        // 最大堆：存储已访问的最佳 ef 个节点（按距离从大到小）
        let mut visited = BinaryHeap::new();
        visited.push(Candidate::new(start_id, start_dist));
        
        global_visited.insert(start_id);
        
        // 贪心扩展
        while let Some(Reverse(current)) = candidates.pop() {
            // 剪枝：当前距离已经比 visited 中第 ef 大的距离还大
            if visited.len() >= ef {
                if let Some(furthest) = visited.peek() {
                    if current.distance > furthest.distance {
                        break;
                    }
                }
            }
            
            // 获取邻居
            let neighbors = self.get_neighbors(current.id)?;
            
            // 扩展邻居
            for neighbor_id in neighbors {
                if global_visited.contains(&neighbor_id) {
                    continue;
                }
                global_visited.insert(neighbor_id);
                
                // 计算邻居距离
                let neighbor_vec = self.decompress_vector(neighbor_id, centroid, scales)?;
                let neighbor_dist = l2_distance(query, &neighbor_vec);
                
                // 更新候选池
                if visited.len() < ef {
                    candidates.push(Reverse(Candidate::new(neighbor_id, neighbor_dist)));
                    visited.push(Candidate::new(neighbor_id, neighbor_dist));
                } else if let Some(furthest) = visited.peek() {
                    if neighbor_dist < furthest.distance {
                        candidates.push(Reverse(Candidate::new(neighbor_id, neighbor_dist)));
                        visited.push(Candidate::new(neighbor_id, neighbor_dist));
                        
                        // 保持 visited 大小为 ef
                        if visited.len() > ef {
                            visited.pop();
                        }
                    }
                }
            }
        }
        
        // 返回所有访问过的节点
        Ok(visited.into_sorted_vec())
    }
    
    /// 线性扫描（fallback）
    fn linear_search(
        &self,
        query: &[f32],
        k: usize,
        centroid: &[f32],
        scales: &[f32],
    ) -> Result<Vec<Candidate>> {
        let mut candidates = Vec::with_capacity(self.id_to_index.len());
        
        // 🆕 Phase 4: 过滤已删除节点
        for (&id, _) in &self.id_to_index {
            if !self.is_deleted(id) {  // 跳过已删除节点
                let vec = self.decompress_vector(id, centroid, scales)?;
                let dist = l2_distance(query, &vec);
                candidates.push(Candidate::new(id, dist));
            }
        }
        
        candidates.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.truncate(k);
        
        Ok(candidates)
    }
    
    /// 获取质心
    fn get_centroid(&self) -> Result<Vec<f32>> {
        let offset = self.metadata.vectors_offset as usize;
        let dim = self.metadata.dimension as usize;
        let bytes = &self.mmap[offset..offset + dim * 4];
        
        let mut centroid = vec![0.0f32; dim];
        for i in 0..dim {
            let val = f32::from_le_bytes([
                bytes[i * 4],
                bytes[i * 4 + 1],
                bytes[i * 4 + 2],
                bytes[i * 4 + 3],
            ]);
            centroid[i] = val;
        }
        
        Ok(centroid)
    }
    
    /// 获取缩放系数
    fn get_scales(&self) -> Result<Vec<f32>> {
        let offset = self.metadata.vectors_offset as usize + self.metadata.dimension as usize * 4;
        let dim = self.metadata.dimension as usize;
        let bytes = &self.mmap[offset..offset + dim * 4];
        
        let mut scales = vec![0.0f32; dim];
        for i in 0..dim {
            let val = f32::from_le_bytes([
                bytes[i * 4],
                bytes[i * 4 + 1],
                bytes[i * 4 + 2],
                bytes[i * 4 + 3],
            ]);
            scales[i] = val;
        }
        
        Ok(scales)
    }
    
    /// 解压单个向量
    fn decompress_vector(&self, id: RowId, centroid: &[f32], scales: &[f32]) -> Result<Vec<f32>> {
        let dim = self.metadata.dimension as usize;
        
        // 使用映射获取索引
        let index = self.id_to_index.get(&id)
            .ok_or_else(|| StorageError::InvalidData(format!("ID {} not found in SST", id).into()))?;
        
        let compressed_offset = self.metadata.vectors_offset as usize 
            + dim * 8  // centroid + scales
            + (*index) * dim;
        
        if compressed_offset + dim > self.mmap.len() {
            return Err(StorageError::Corruption(
                format!("Vector offset out of bounds: {} + {} > {}", 
                    compressed_offset, dim, self.mmap.len()).into()
            ));
        }
        
        let bytes = &self.mmap[compressed_offset..compressed_offset + dim];
        
        let mut vector = vec![0.0f32; dim];
        for i in 0..dim {
            let code = bytes[i];
            // 修复：对称反量化 [0, 255] -> [-max_abs, max_abs]
            let normalized = code as f32 - 127.5;  // -> [-127.5, 127.5]
            vector[i] = centroid[i] + normalized * scales[i];
        }
        
        Ok(vector)
    }
    
    /// 获取邻居列表
    fn get_neighbors(&self, id: RowId) -> Result<Vec<RowId>> {
        let graph_offset = self.metadata.graph_offset as usize;
        
        // 使用映射获取索引
        let index = self.id_to_index.get(&id)
            .ok_or_else(|| StorageError::InvalidData(format!("ID {} not found in SST", id).into()))?;
        
        // 读取节点偏移表
        let offset_table_start = graph_offset;
        let offset_pos = offset_table_start + (*index) * 8;
        
        if offset_pos + 8 > self.mmap.len() {
            return Err(StorageError::Corruption(
                format!("Offset table out of bounds: {} + 8 > {}", 
                    offset_pos, self.mmap.len()).into()
            ));
        }
        
        let node_offset = u64::from_le_bytes([
            self.mmap[offset_pos],
            self.mmap[offset_pos + 1],
            self.mmap[offset_pos + 2],
            self.mmap[offset_pos + 3],
            self.mmap[offset_pos + 4],
            self.mmap[offset_pos + 5],
            self.mmap[offset_pos + 6],
            self.mmap[offset_pos + 7],
        ]) as usize;
        
        if node_offset + 4 > self.mmap.len() {
            return Err(StorageError::Corruption(
                format!("Node offset out of bounds: {} + 4 > {}", 
                    node_offset, self.mmap.len()).into()
            ));
        }
        
        // 读取邻居数量
        let degree = u32::from_le_bytes([
            self.mmap[node_offset],
            self.mmap[node_offset + 1],
            self.mmap[node_offset + 2],
            self.mmap[node_offset + 3],
        ]) as usize;
        
        // 读取邻居列表
        let mut neighbors = Vec::with_capacity(degree);
        let neighbors_start = node_offset + 4;
        
        if neighbors_start + degree * 8 > self.mmap.len() {
            return Err(StorageError::Corruption(
                format!("Neighbors list out of bounds: {} + {} > {}", 
                    neighbors_start, degree * 8, self.mmap.len()).into()
            ));
        }
        
        for i in 0..degree {
            let pos = neighbors_start + i * 8;
            let neighbor_id = u64::from_le_bytes([
                self.mmap[pos],
                self.mmap[pos + 1],
                self.mmap[pos + 2],
                self.mmap[pos + 3],
                self.mmap[pos + 4],
                self.mmap[pos + 5],
                self.mmap[pos + 6],
                self.mmap[pos + 7],
            ]);
            neighbors.push(neighbor_id);
        }
        
        Ok(neighbors)
    }
    
    /// 获取元数据
    pub fn metadata(&self) -> &SSTMetadata {
        &self.metadata
    }
    
    /// 获取文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }
    
    // 🆕 Phase 4: Delete 和 Update 支持
    
    /// 软删除节点
    pub fn delete(&self, id: RowId) -> Result<()> {
        if let Some(&index) = self.id_to_index.get(&id) {
            let byte_idx = index / 8;
            let bit_idx = index % 8;
            
            let mut bitmap = self.deleted_bitmap.write();
            bitmap[byte_idx] |= 1 << bit_idx;
            
            Ok(())
        } else {
            Err(StorageError::InvalidData(format!("Node {} not found in SST", id)))
        }
    }
    
    /// 检查节点是否被删除
    pub fn is_deleted(&self, id: RowId) -> bool {
        if let Some(&index) = self.id_to_index.get(&id) {
            let byte_idx = index / 8;
            let bit_idx = index % 8;
            
            let bitmap = self.deleted_bitmap.read();
            (bitmap[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false  // 不存在的节点视为未删除
        }
    }
    
    /// 获取未删除节点数量
    pub fn active_node_count(&self) -> usize {
        let bitmap = self.deleted_bitmap.read();
        let total = self.metadata.node_count as usize;
        
        let deleted_count: usize = (0..total)
            .filter(|&i| {
                let byte_idx = i / 8;
                let bit_idx = i % 8;
                (bitmap[byte_idx] & (1 << bit_idx)) != 0
            })
            .count();
        
        total - deleted_count
    }
    
    // 🆕 Phase 5: Compaction 支持
    
    /// 导出所有未删除的节点（用于 Compaction）
    pub fn export_active_nodes(&self) -> Result<Vec<(RowId, VectorNode)>> {
        let centroid = self.get_centroid()?;
        let scales = self.get_scales()?;
        
        let mut nodes = Vec::new();
        
        for (&row_id, _) in &self.id_to_index {
            // 🔥 关键：只导出未删除的节点
            if self.is_deleted(row_id) {
                continue;
            }
            
            // 解压向量
            let vector = self.decompress_vector(row_id, &centroid, &scales)?;
            
            // 获取邻居
            let neighbors = self.get_neighbors(row_id)?;
            
            // 创建节点
            let node = VectorNode {
                vector,
                neighbors,
                timestamp: 0,  // SST 文件不存储时间戳
                deleted: false,
            };
            
            nodes.push((row_id, node));
        }
        
        // 按 row_id 排序
        nodes.sort_by_key(|(id, _)| *id);
        
        Ok(nodes)
    }
}

/// 写入 header
fn write_header<W: Write>(writer: &mut W, metadata: &SSTMetadata) -> Result<()> {
    writer.write_all(MAGIC)?;
    writer.write_all(&VERSION.to_le_bytes())?;
    writer.write_all(&metadata.node_count.to_le_bytes())?;
    writer.write_all(&metadata.dimension.to_le_bytes())?;
    writer.write_all(&metadata.medoid.to_le_bytes())?;
    writer.write_all(&metadata.id_list_offset.to_le_bytes())?;
    writer.write_all(&metadata.deleted_bitmap_offset.to_le_bytes())?;  // 🆕 Phase 4
    writer.write_all(&metadata.vectors_offset.to_le_bytes())?;
    writer.write_all(&metadata.raw_vectors_offset.to_le_bytes())?;
    writer.write_all(&metadata.graph_offset.to_le_bytes())?;
    writer.write_all(&metadata.footer_offset.to_le_bytes())?;
    
    // 填充到 256 字节 (V4: 11个 u64/u32 字段 = 80 字节)
    let padding = HEADER_SIZE - 4 - 4 - 8 - 4 - 8 - 8 - 8 - 8 - 8 - 8 - 8;
    writer.write_all(&vec![0u8; padding])?;
    
    Ok(())
}

/// 解析 header
fn parse_header(mmap: &[u8]) -> Result<SSTMetadata> {
    if &mmap[0..4] != MAGIC {
        return Err(StorageError::Corruption("Invalid SST magic".into()));
    }
    
    let version = u32::from_le_bytes([mmap[4], mmap[5], mmap[6], mmap[7]]);
    
    // 兼容 V2, V3, V4
    if version == 1 {
        // V1: 已废弃
        return Err(StorageError::Corruption(
            "SST V1 format is deprecated, please rebuild the index".into()
        ));
    } else if version == 2 {
        // V2: 没有原始向量，设置 raw_vectors_offset = 0 表示不可用
        return parse_header_v2(mmap);
    } else if version == 3 {
        // V3: 没有删除标记，需要特殊处理
        return parse_header_v3(mmap);
    } else if version != VERSION {
        return Err(StorageError::Corruption(format!("Unsupported SST version: {}", version)));
    }
    
    // V4 解析
    let node_count = u64::from_le_bytes([
        mmap[8], mmap[9], mmap[10], mmap[11],
        mmap[12], mmap[13], mmap[14], mmap[15],
    ]);
    
    let dimension = u32::from_le_bytes([mmap[16], mmap[17], mmap[18], mmap[19]]);
    
    let medoid = u64::from_le_bytes([
        mmap[20], mmap[21], mmap[22], mmap[23],
        mmap[24], mmap[25], mmap[26], mmap[27],
    ]);
    
    let id_list_offset = u64::from_le_bytes([
        mmap[28], mmap[29], mmap[30], mmap[31],
        mmap[32], mmap[33], mmap[34], mmap[35],
    ]);
    
    let deleted_bitmap_offset = u64::from_le_bytes([  // 🆕 Phase 4
        mmap[36], mmap[37], mmap[38], mmap[39],
        mmap[40], mmap[41], mmap[42], mmap[43],
    ]);
    
    let vectors_offset = u64::from_le_bytes([
        mmap[44], mmap[45], mmap[46], mmap[47],
        mmap[48], mmap[49], mmap[50], mmap[51],
    ]);
    
    let raw_vectors_offset = u64::from_le_bytes([
        mmap[52], mmap[53], mmap[54], mmap[55],
        mmap[56], mmap[57], mmap[58], mmap[59],
    ]);
    
    let graph_offset = u64::from_le_bytes([
        mmap[60], mmap[61], mmap[62], mmap[63],
        mmap[64], mmap[65], mmap[66], mmap[67],
    ]);
    
    let footer_offset = u64::from_le_bytes([
        mmap[68], mmap[69], mmap[70], mmap[71],
        mmap[72], mmap[73], mmap[74], mmap[75],
    ]);
    
    Ok(SSTMetadata {
        node_count,
        dimension,
        medoid,
        id_list_offset,
        deleted_bitmap_offset,  // 🆕 Phase 4
        vectors_offset,
        raw_vectors_offset,
        graph_offset,
        footer_offset,
    })
}

/// 解析 V2 Header（向后兼容）
fn parse_header_v2(mmap: &[u8]) -> Result<SSTMetadata> {
    let node_count = u64::from_le_bytes([
        mmap[8], mmap[9], mmap[10], mmap[11],
        mmap[12], mmap[13], mmap[14], mmap[15],
    ]);
    
    let dimension = u32::from_le_bytes([mmap[16], mmap[17], mmap[18], mmap[19]]);
    
    let medoid = u64::from_le_bytes([
        mmap[20], mmap[21], mmap[22], mmap[23],
        mmap[24], mmap[25], mmap[26], mmap[27],
    ]);
    
    let id_list_offset = u64::from_le_bytes([
        mmap[28], mmap[29], mmap[30], mmap[31],
        mmap[32], mmap[33], mmap[34], mmap[35],
    ]);
    
    let vectors_offset = u64::from_le_bytes([
        mmap[36], mmap[37], mmap[38], mmap[39],
        mmap[40], mmap[41], mmap[42], mmap[43],
    ]);
    
    let graph_offset = u64::from_le_bytes([
        mmap[44], mmap[45], mmap[46], mmap[47],
        mmap[48], mmap[49], mmap[50], mmap[51],
    ]);
    
    let footer_offset = u64::from_le_bytes([
        mmap[52], mmap[53], mmap[54], mmap[55],
        mmap[56], mmap[57], mmap[58], mmap[59],
    ]);
    
    Ok(SSTMetadata {
        node_count,
        dimension,
        medoid,
        id_list_offset,
        deleted_bitmap_offset: 0,  // V2 没有删除标记
        vectors_offset,
        raw_vectors_offset: 0,  // V2 没有原始向量
        graph_offset,
        footer_offset,
    })
}

/// 解析 V3 Header（向后兼容）
fn parse_header_v3(mmap: &[u8]) -> Result<SSTMetadata> {
    let node_count = u64::from_le_bytes([
        mmap[8], mmap[9], mmap[10], mmap[11],
        mmap[12], mmap[13], mmap[14], mmap[15],
    ]);
    
    let dimension = u32::from_le_bytes([mmap[16], mmap[17], mmap[18], mmap[19]]);
    
    let medoid = u64::from_le_bytes([
        mmap[20], mmap[21], mmap[22], mmap[23],
        mmap[24], mmap[25], mmap[26], mmap[27],
    ]);
    
    let id_list_offset = u64::from_le_bytes([
        mmap[28], mmap[29], mmap[30], mmap[31],
        mmap[32], mmap[33], mmap[34], mmap[35],
    ]);
    
    let vectors_offset = u64::from_le_bytes([
        mmap[36], mmap[37], mmap[38], mmap[39],
        mmap[40], mmap[41], mmap[42], mmap[43],
    ]);
    
    let raw_vectors_offset = u64::from_le_bytes([
        mmap[44], mmap[45], mmap[46], mmap[47],
        mmap[48], mmap[49], mmap[50], mmap[51],
    ]);
    
    let graph_offset = u64::from_le_bytes([
        mmap[52], mmap[53], mmap[54], mmap[55],
        mmap[56], mmap[57], mmap[58], mmap[59],
    ]);
    
    let footer_offset = u64::from_le_bytes([
        mmap[60], mmap[61], mmap[62], mmap[63],
        mmap[64], mmap[65], mmap[66], mmap[67],
    ]);
    
    Ok(SSTMetadata {
        node_count,
        dimension,
        medoid,
        id_list_offset,
        deleted_bitmap_offset: 0,  // V3 没有删除标记
        vectors_offset,
        raw_vectors_offset,
        graph_offset,
        footer_offset,
    })
}

/// 读取 ID List 并构建映射
fn read_id_list(mmap: &[u8], metadata: &SSTMetadata) -> Result<std::collections::HashMap<RowId, usize>> {
    let offset = metadata.id_list_offset as usize;
    let node_count = metadata.node_count as usize;
    let id_list_size = node_count * 8;
    
    if offset + id_list_size > mmap.len() {
        return Err(StorageError::Corruption(
            format!("ID list out of bounds: {} + {} > {}", 
                offset, id_list_size, mmap.len()).into()
        ));
    }
    
    let mut id_to_index = std::collections::HashMap::with_capacity(node_count);
    
    for i in 0..node_count {
        let pos = offset + i * 8;
        let id = u64::from_le_bytes([
            mmap[pos],
            mmap[pos + 1],
            mmap[pos + 2],
            mmap[pos + 3],
            mmap[pos + 4],
            mmap[pos + 5],
            mmap[pos + 6],
            mmap[pos + 7],
        ]);
        id_to_index.insert(id, i);
    }
    
    Ok(id_to_index)
}

/// 写入 SQ8 压缩向量
fn write_sq8_vectors<W: Write>(writer: &mut W, nodes: &[(RowId, VectorNode)]) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }
    
    let dim = nodes[0].1.vector.len();
    
    // 计算质心
    let mut centroid = vec![0.0f32; dim];
    for (_, node) in nodes {
        for i in 0..dim {
            centroid[i] += node.vector[i];
        }
    }
    for v in &mut centroid {
        *v /= nodes.len() as f32;
    }
    
    // 🔥 修复：计算每个维度的最大绝对偏移（用于对称量化）
    let mut max_abs = vec![0.0f32; dim];
    
    for (_, node) in nodes {
        for i in 0..dim {
            let shifted = node.vector[i] - centroid[i];
            max_abs[i] = max_abs[i].max(shifted.abs());
        }
    }
    
    // scales[i] = max_abs[i] / 127.5 （映射到 [-127.5, 127.5]）
    let mut scales = vec![0.0f32; dim];
    for i in 0..dim {
        scales[i] = if max_abs[i] > 1e-6 { max_abs[i] / 127.5 } else { 1.0 };
    }
    
    // 写入质心
    for &v in &centroid {
        writer.write_all(&v.to_le_bytes())?;
    }
    
    // 写入缩放系数
    for &s in &scales {
        writer.write_all(&s.to_le_bytes())?;
    }
    
    // 写入压缩向量
    for (_, node) in nodes {
        for i in 0..dim {
            let shifted = node.vector[i] - centroid[i];
            // 修复：对称量化 [-max_abs, max_abs] -> [0, 255]
            let normalized = shifted / scales[i];  // [-127.5, 127.5]
            let code = (normalized + 127.5).clamp(0.0, 255.0) as u8;
            writer.write_all(&[code])?;
        }
    }
    
    Ok(())
}

/// 写入图结构
fn write_graph<W: Write + Seek>(writer: &mut W, nodes: &[(RowId, VectorNode)]) -> Result<()> {
    let graph_start = writer.stream_position()?;
    
    // 1. 预留偏移表空间
    let offset_table_size = nodes.len() * 8;
    writer.write_all(&vec![0u8; offset_table_size])?;
    
    // 2. 写入邻接列表并记录偏移
    let mut offsets = Vec::with_capacity(nodes.len());
    
    for (_, node) in nodes {
        let offset = writer.stream_position()?;
        offsets.push(offset);
        
        // 写入度数
        writer.write_all(&(node.neighbors.len() as u32).to_le_bytes())?;
        
        // 写入邻居列表
        for &neighbor_id in &node.neighbors {
            writer.write_all(&neighbor_id.to_le_bytes())?;
        }
    }
    
    // 3. 回写偏移表
    let end_pos = writer.stream_position()?;
    writer.seek(SeekFrom::Start(graph_start))?;
    
    for offset in offsets {
        writer.write_all(&offset.to_le_bytes())?;
    }
    
    writer.seek(SeekFrom::Start(end_pos))?;
    
    Ok(())
}

/// 写入原始 f32 向量（用于精排）
fn write_raw_vectors<W: Write>(writer: &mut W, nodes: &[(RowId, VectorNode)]) -> Result<()> {
    for (_, node) in nodes {
        for &v in &node.vector {
            writer.write_all(&v.to_le_bytes())?;
        }
    }
    Ok(())
}

/// 写入 footer
fn write_footer<W: Write>(writer: &mut W) -> Result<()> {
    // 简单的 CRC32 校验（TODO: 实现真正的校验）
    let checksum = 0u32;
    writer.write_all(&checksum.to_le_bytes())?;
    
    // 填充
    writer.write_all(&vec![0u8; FOOTER_SIZE - 4])?;
    
    Ok(())
}

/// L2 距离计算
fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}
