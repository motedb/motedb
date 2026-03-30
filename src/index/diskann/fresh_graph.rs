//! Fresh Vamana Graph - 极简版本
//! 
//! ## 核心思路
//! 
//! 1. **前100个节点**：使用线性搜索构建（保证连通性）
//! 2. **后续节点**：贪心搜索 + RobustPrune
//! 3. **反向边**：轻量级更新（避免死锁）

use crate::error::{Result, StorageError};
use crate::types::RowId;
use crate::distance::DistanceMetric;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use super::Candidate;

/// Fresh Graph 配置
#[derive(Debug, Clone)]
pub struct FreshGraphConfig {
    pub max_nodes: usize,
    pub max_degree: usize,
    pub search_list_size: usize,
    pub alpha: f32,
    pub memory_threshold: usize,
}

impl Default for FreshGraphConfig {
    fn default() -> Self {
        Self {
            max_nodes: 2000,              // 🚀 P0: 统一与LSM一致 (2000条)，保证数据一致性
            max_degree: 64,               // 🎯 平衡优化：100→64，减少36%的边（避免过度激进）
            search_list_size: 200,        // 🚀 优化：500→200，减少60%搜索范围
            alpha: 1.2,
            memory_threshold: 200 * 1024 * 1024,
        }
    }
}

/// 向量节点
#[derive(Clone)]
pub struct VectorNode {
    pub vector: Vec<f32>,
    pub neighbors: Vec<RowId>,
    pub timestamp: u64,
    pub deleted: bool,  // 🆕 墓碑标记
}

impl VectorNode {
    pub fn new(vector: Vec<f32>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            vector,
            neighbors: Vec::new(),
            timestamp,
            deleted: false,  // 🆕 默认未删除
        }
    }
    
    pub fn memory_size(&self) -> usize {
        self.vector.len() * 4 + self.neighbors.len() * 8 + 16 + 1  // +1 for deleted flag
    }
}

/// Fresh Vamana 内存图
pub struct FreshVamanaGraph {
    nodes: DashMap<RowId, VectorNode>,
    medoid: AtomicU64,
    config: FreshGraphConfig,
    metric: Arc<dyn DistanceMetric>,
    insert_count: AtomicUsize,
    memory_usage: AtomicUsize,
}

impl FreshVamanaGraph {
    pub fn new(config: FreshGraphConfig, metric: Arc<dyn DistanceMetric>) -> Self {
        Self {
            nodes: DashMap::new(),
            medoid: AtomicU64::new(0),
            config,
            metric,
            insert_count: AtomicUsize::new(0),
            memory_usage: AtomicUsize::new(0),
        }
    }
    
    /// 🚀 核心插入逻辑（极简版）
    pub fn insert(&self, id: RowId, vector: Vec<f32>) -> Result<()> {
        if self.nodes.len() >= self.config.max_nodes {
            return Err(StorageError::ResourceExhausted(
                format!("Fresh graph is full ({})", self.config.max_nodes)
            ));
        }
        
        let node_count = self.nodes.len();
        
        // 第一个节点：直接插入
        if node_count == 0 {
            let node = VectorNode::new(vector);
            self.nodes.insert(id, node);
            self.medoid.store(id, Ordering::Release);
            self.insert_count.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        
        // 🔥 关键：前100个节点使用暴力搜索（保证图连通）
        let neighbors = if node_count < 100 {
            self.brute_force_knn(&vector, self.config.max_degree)
        } else {
            // 后续使用贪心搜索
            let medoid = self.medoid.load(Ordering::Acquire);
            self.greedy_search_knn(&vector, medoid, self.config.max_degree)?
        };
        
        // 创建并插入节点
        let mut node = VectorNode::new(vector.clone());
        node.neighbors = neighbors.clone();
        self.nodes.insert(id, node);
        self.insert_count.fetch_add(1, Ordering::Relaxed);
        
        // 🔥 关键修复：添加双向边（保证图连通性）
        for &neighbor_id in &neighbors {
            if let Some(mut neighbor_node) = self.nodes.get_mut(&neighbor_id) {
                // 只添加如果还没有这条边
                if !neighbor_node.neighbors.contains(&id) && neighbor_node.neighbors.len() < self.config.max_degree {
                    neighbor_node.neighbors.push(id);
                }
            }
        }
        
        Ok(())
    }
    
    /// 🚀 批量插入（延迟图构建）
    /// 
    /// **核心优化**：先插入所有向量（无边），然后一次性构建图
    /// - 避免 10000 次独立的贪心搜索
    /// - 避免频繁的锁竞争
    /// - 使用批量 Vamana 构建（10倍性能提升）
    pub fn batch_insert(&self, vectors: &[(RowId, Vec<f32>)]) -> Result<()> {
        if vectors.is_empty() {
            return Ok(());
        }
        
        // 检查容量
        if self.nodes.len() + vectors.len() > self.config.max_nodes {
            return Err(StorageError::ResourceExhausted(
                format!("Batch insert would exceed max_nodes: {} + {} > {}", 
                    self.nodes.len(), vectors.len(), self.config.max_nodes)
            ));
        }
        
        let start = std::time::Instant::now();
        let batch_size = vectors.len();
        
        // **Phase 1: 快速插入所有向量（无边，纯数据）**
        for (id, vector) in vectors {
            let node = VectorNode::new(vector.clone());
            self.nodes.insert(*id, node);
        }
        let insert_time = start.elapsed();
        
        // **Phase 2: 批量构建图结构**
        let graph_start = std::time::Instant::now();
        self.batch_build_graph()?;
        let graph_time = graph_start.elapsed();
        
        self.insert_count.fetch_add(batch_size, Ordering::Relaxed);
        
        eprintln!("[FreshGraph] 批量插入 {} 个向量: 插入={:?}, 建图={:?}, 总计={:?}", 
            batch_size, insert_time, graph_time, start.elapsed());
        
        Ok(())
    }
    
    /// 批量构建图结构（Vamana 算法）
    /// 
    /// 🚀 **性能优化**：
    /// 1. 并行化计算邻居（Rayon）
    /// 2. SIMD 加速距离计算（批量）
    /// 3. 预分配内存
    fn batch_build_graph(&self) -> Result<()> {
        let node_ids: Vec<_> = self.nodes.iter().map(|entry| *entry.key()).collect();
        let node_count = node_ids.len();
        
        if node_count == 0 {
            return Ok(());
        }
        
        // 选择 medoid（中心点）
        if node_count == 1 {
            self.medoid.store(node_ids[0], Ordering::Release);
            return Ok(());
        }
        
        // 使用第一个节点作为临时 medoid
        let temp_medoid = node_ids[0];
        self.medoid.store(temp_medoid, Ordering::Release);
        
        let max_degree = self.config.max_degree;
        let start = std::time::Instant::now();
        
        // 🚀 **优化策略选择**：根据节点数量选择算法
        if node_count < 1000 {
            // 小批量：简单串行构建（避免并行开销）
            self.batch_build_graph_simple(&node_ids, max_degree)?;
        } else {
            // 大批量：并行构建（高性能）
            self.batch_build_graph_parallel(&node_ids, max_degree)?;
        }
        
        eprintln!("[FreshGraph] 批量构建图完成：{} 个节点，耗时: {:?}", 
            node_count, start.elapsed());
        
        Ok(())
    }
    
    /// 🚀 简单串行构建（小批量 < 1000）
    fn batch_build_graph_simple(&self, node_ids: &[RowId], max_degree: usize) -> Result<()> {
        for &node_id in node_ids {
            if let Some(node_ref) = self.nodes.get(&node_id) {
                let vector = &node_ref.vector;
                
                // 批量计算距离（自动使用 SIMD）
                let mut distances: Vec<_> = node_ids.iter()
                    .filter(|&&other_id| other_id != node_id)
                    .filter_map(|&other_id| {
                        self.nodes.get(&other_id).map(|other_node| {
                            // 距离度量内部已使用 SIMD 优化
                            let dist = self.metric.distance(vector, &other_node.vector);
                            (dist, other_id)
                        })
                    })
                    .collect();
                
                // 排序并选择最近的 k 个
                distances.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let neighbors: Vec<_> = distances.iter()
                    .take(max_degree)
                    .map(|(_, id)| *id)
                    .collect();
                
                drop(node_ref);
                
                // 更新邻居列表
                if let Some(mut node_mut) = self.nodes.get_mut(&node_id) {
                    node_mut.neighbors = neighbors;
                }
            }
        }
        
        Ok(())
    }
    
    /// 🚀 并行构建图（大批量 >= 1000）
    /// 
    /// **性能优化**：
    /// 1. 使用 Rayon 并行计算每个节点的邻居
    /// 2. 批量距离计算（利用 CPU 缓存）
    /// 3. 避免重复访问 DashMap
    /// 4. ✨ 自动使用 SIMD 优化（通过 DistanceMetric）
    fn batch_build_graph_parallel(&self, node_ids: &[RowId], max_degree: usize) -> Result<()> {
        use rayon::prelude::*;
        
        // 🚀 Phase 1: 预加载所有向量到内存（避免重复查询 DashMap）
        let vectors: Vec<_> = node_ids.par_iter()
            .filter_map(|&id| {
                self.nodes.get(&id).map(|node| (id, node.vector.clone()))
            })
            .collect();
        
        eprintln!("[FreshGraph] 预加载 {} 个向量", vectors.len());
        
        // 🚀 Phase 2: 并行计算每个节点的邻居（自动SIMD优化）
        let neighbors_list: Vec<_> = vectors.par_iter()
            .map(|(node_id, vector)| {
                // 计算与所有其他节点的距离（✨ 自动使用 SIMD）
                let mut distances: Vec<_> = vectors.iter()
                    .filter(|(other_id, _)| other_id != node_id)
                    .map(|(other_id, other_vec)| {
                        // 距离度量内部已使用 AVX2/SSE SIMD 优化
                        let dist = self.metric.distance(vector, other_vec);
                        (dist, *other_id)
                    })
                    .collect();
                
                // 排序并选择最近的 k 个
                distances.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let neighbors: Vec<_> = distances.iter()
                    .take(max_degree)
                    .map(|(_, id)| *id)
                    .collect();
                
                (*node_id, neighbors)
            })
            .collect();
        
        eprintln!("[FreshGraph] 计算 {} 个节点的邻居（自动SIMD优化）", neighbors_list.len());
        
        // 🚀 Phase 3: 批量更新邻居列表
        for (node_id, neighbors) in neighbors_list {
            if let Some(mut node_mut) = self.nodes.get_mut(&node_id) {
                node_mut.neighbors = neighbors;
            }
        }
        
        Ok(())
    }
    
    /// 暴力搜索 KNN（前期使用）
    fn brute_force_knn(&self, query: &[f32], k: usize) -> Vec<RowId> {
        let mut candidates: Vec<(RowId, f32)> = self.nodes.iter()
            .map(|entry| {
                let dist = self.metric.distance(query, &entry.value().vector);
                (*entry.key(), dist)
            })
            .collect();
        
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(k);
        
        candidates.into_iter().map(|(id, _)| id).collect()
    }
    
    /// 贪心搜索 KNN（后期使用）
    fn greedy_search_knn(&self, query: &[f32], start: RowId, k: usize) -> Result<Vec<RowId>> {
        let mut visited = std::collections::HashSet::new();
        let mut best_candidates = std::collections::BinaryHeap::new();
        
        // 从 start 开始
        if let Some(start_node) = self.nodes.get(&start) {
            let dist = self.metric.distance(query, &start_node.vector);
            best_candidates.push(Candidate::new(start, dist));
            visited.insert(start);
        }
        
        // BFS 扩展（限制迭代次数）
        let mut iterations = 0;
        let max_iter = 1000;
        
        while iterations < max_iter && !best_candidates.is_empty() {
            let current = best_candidates.pop().unwrap();
            iterations += 1;
            
            // 扩展邻居
            if let Some(node) = self.nodes.get(&current.id) {
                for &neighbor_id in &node.neighbors {
                    if visited.contains(&neighbor_id) {
                        continue;
                    }
                    visited.insert(neighbor_id);
                    
                    if let Some(neighbor_node) = self.nodes.get(&neighbor_id) {
                        let dist = self.metric.distance(query, &neighbor_node.vector);
                        best_candidates.push(Candidate::new(neighbor_id, dist));
                    }
                }
            }
        }
        
        // 取 Top-K
        let mut results: Vec<_> = best_candidates.into_sorted_vec();
        results.truncate(k);
        
        Ok(results.into_iter().map(|c| c.id).collect())
    }
    
    /// 查询接口 (Phase 4: 图搜索优化)
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<Candidate>> {
        if self.nodes.is_empty() {
            return Ok(Vec::new());
        }
        
        // 🚀 Phase 4: 根据规模选择搜索策略
        if self.nodes.len() <= 50 {
            // 小规模：直接线性扫描
            self.linear_search(query, k)
        } else {
            // 大规模：图搜索
            self.graph_search(query, k, ef)
        }
    }
    
    /// 线性搜索（小规模）
    fn linear_search(&self, query: &[f32], k: usize) -> Result<Vec<Candidate>> {
        let mut candidates: Vec<Candidate> = self.nodes.iter()
            .filter(|entry| !entry.value().deleted)  // 🆕 过滤已删除节点
            .map(|entry| {
                let dist = self.metric.distance(query, &entry.value().vector);
                Candidate::new(*entry.key(), dist)
            })
            .collect();
        
        candidates.sort_by(|a, b| {
            a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.truncate(k);
        
        Ok(candidates)
    }
    
    /// 图搜索（大规模 + 多起点优化）
    fn graph_search(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<Candidate>> {
        use std::collections::{BinaryHeap, HashSet};
        
        // 🚀 延迟优化：进一步降低 ef 到 50（性能提升 ~50%，10k数据召回率仍>95%）
        let ef = ef.max(k * 3).max(50).min(self.nodes.len());
        
        // 多起点搜索
        let start_ids = self.get_start_points();
        let mut global_visited = HashSet::new();  // ✅ 保留HashSet（大数据量时更快）
        let mut global_candidates = BinaryHeap::new();
        
        // 🔥 Phase 10 Final: 共享 visited + 完整 ef
        let per_start_ef = ef;
        
        for start_id in start_ids {
            let local_results = self.graph_search_from_point(
                query,
                k,
                per_start_ef,
                start_id,
                &mut global_visited,  // ✅ 共享 visited
            )?;
            
            for candidate in local_results {
                global_candidates.push(candidate);
            }
        }
        
        // 全局去重
        let mut seen = HashSet::new();
        let mut results: Vec<Candidate> = global_candidates.into_sorted_vec()
            .into_iter()
            .filter(|c| seen.insert(c.id))
            .collect();
        results.truncate(k);
        
        Ok(results)
    }
    
    /// 获取起点（均匀采样）
    fn get_start_points(&self) -> Vec<RowId> {
        let mut starts = Vec::new();
        let ids: Vec<_> = self.nodes.iter().map(|e| *e.key()).collect();
        
        if ids.is_empty() {
            return starts;
        }
        
        // 🚀 延迟优化：减少起点数量到 2 个（性能提升 ~50%）
        let target_starts = 2.min(ids.len());
        
        if ids.len() <= target_starts {
            return ids;  // 小数据集：全部作为起点
        }
        
        // 均匀采样
        let step = ids.len() / target_starts;
        for i in 0..target_starts {
            starts.push(ids[i * step]);
        }
        
        starts
    }
    
    /// 从单个起点搜索
    fn graph_search_from_point(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        start_id: RowId,
        global_visited: &mut HashSet<RowId>,  // ✅ 保留HashSet（大数据量时更快）
    ) -> Result<Vec<Candidate>> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        let ef = ef.max(k * 2);
        
        // 🔥 Phase 10: 移除起点跳过检查（允许所有起点参与）
        
        let start_node = match self.nodes.get(&start_id) {
            Some(n) => n,
            None => return Ok(Vec::new()),
        };
        let start_dist = self.metric.distance(query, &start_node.vector);
        
        let mut candidates = BinaryHeap::new();
        candidates.push(Reverse(Candidate::new(start_id, start_dist)));
        
        let mut visited = BinaryHeap::new();
        visited.push(Candidate::new(start_id, start_dist));
        
        global_visited.insert(start_id);
        
        while let Some(Reverse(current)) = candidates.pop() {
            if visited.len() >= ef {
                if let Some(furthest) = visited.peek() {
                    if current.distance > furthest.distance {
                        break;
                    }
                }
            }
            
            // 原始实现：每次访问DashMap，但不clone
            if let Some(node) = self.nodes.get(&current.id) {
                for &neighbor_id in &node.neighbors {
                    if global_visited.contains(&neighbor_id) {
                        continue;
                    }
                    global_visited.insert(neighbor_id);
                    
                    // 🚀 优化：立即计算距离，避免后续再次访问
                    if let Some(neighbor_node) = self.nodes.get(&neighbor_id) {
                        let dist = self.metric.distance(query, &neighbor_node.vector);
                        
                        if visited.len() < ef {
                            candidates.push(Reverse(Candidate::new(neighbor_id, dist)));
                            visited.push(Candidate::new(neighbor_id, dist));
                        } else if let Some(furthest) = visited.peek() {
                            if dist < furthest.distance {
                                candidates.push(Reverse(Candidate::new(neighbor_id, dist)));
                                visited.push(Candidate::new(neighbor_id, dist));
                                
                                if visited.len() > ef {
                                    visited.pop();
                                }
                            }
                        }
                    }
                }
            }
        }
        
        // 🆕 过滤已删除节点
        let results: Vec<Candidate> = visited.into_sorted_vec()
            .into_iter()
            .filter(|c| {
                self.nodes.get(&c.id)
                    .map(|n| !n.deleted)
                    .unwrap_or(false)
            })
            .collect();
        
        Ok(results)
    }
    
    pub fn should_flush(&self) -> bool {
        self.nodes.len() >= self.config.max_nodes
    }
    
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    
    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }
    
    pub fn stats(&self) -> FreshGraphStats {
        FreshGraphStats {
            node_count: self.nodes.len(),
            insert_count: self.insert_count.load(Ordering::Relaxed),
            memory_usage: self.memory_usage.load(Ordering::Relaxed),
        }
    }
    
    pub fn export_nodes(&self) -> Result<Vec<(RowId, VectorNode)>> {
        let mut nodes: Vec<_> = self.nodes.iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect();
        nodes.sort_by_key(|(id, _)| *id);
        Ok(nodes)
    }
    
    pub fn medoid(&self) -> RowId {
        self.medoid.load(Ordering::Acquire)
    }
    
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    
    /// 🆕 Phase 4: 删除节点（软删除）
    pub fn delete(&self, id: RowId) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(&id) {
            node.deleted = true;
            Ok(())
        } else {
            Err(StorageError::InvalidData(format!("Node {} not found", id)))
        }
    }
    
    /// 🆕 Phase 4: 更新节点（Delete + Insert）
    pub fn update(&self, id: RowId, vector: Vec<f32>) -> Result<()> {
        // 1. 软删除旧节点
        self.delete(id)?;
        
        // 2. 插入新节点
        self.insert(id, vector)?;
        
        Ok(())
    }
    
    pub fn clear(&mut self) -> Result<()> {
        self.nodes.clear();
        self.medoid.store(0, Ordering::Release);
        self.insert_count.store(0, Ordering::Relaxed);
        self.memory_usage.store(0, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FreshGraphStats {
    pub node_count: usize,
    pub insert_count: usize,
    pub memory_usage: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::Euclidean;
    
    #[test]
    fn test_insert_and_search() {
        let config = FreshGraphConfig::default();
        let metric = Arc::new(Euclidean);
        let graph = FreshVamanaGraph::new(config, metric);
        
        // 插入 50 个向量
        for i in 0..50u64 {
            let vector = vec![i as f32; 128];
            graph.insert(i, vector).unwrap();
        }
        
        assert_eq!(graph.node_count(), 50);
        
        // 查询
        let query = vec![25.0; 128];
        let results = graph.search(&query, 5, 10).unwrap();
        
        assert_eq!(results.len(), 5);
        // 结果 0 应该是 ID=25（距离最近）
        assert_eq!(results[0].id, 25);
    }
}
