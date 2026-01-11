//! 多层查询合并

use crate::error::Result;
use super::Candidate;
use std::collections::BinaryHeap;

/// 多层搜索合并器
pub struct MultiLevelSearch;

impl MultiLevelSearch {
    pub fn new() -> Self {
        Self
    }
    
    /// 合并多层的查询结果
    /// 
    /// 策略：
    /// 1. 合并所有层的候选
    /// 2. 按距离排序
    /// 3. 去重（保留距离最小的）
    /// 4. 返回 Top-K
    pub fn merge(
        &self,
        fresh: Vec<Candidate>,
        l1: Vec<Candidate>,
        l2: Vec<Candidate>,
        k: usize,
    ) -> Result<Vec<Candidate>> {
        // 使用优先队列合并（最小堆）
        let mut heap = BinaryHeap::new();
        
        // 添加所有候选
        for c in fresh {
            heap.push(std::cmp::Reverse(c));
        }
        for c in l1 {
            heap.push(std::cmp::Reverse(c));
        }
        for c in l2 {
            heap.push(std::cmp::Reverse(c));
        }
        
        // 去重并取 Top-K
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();
        
        while let Some(std::cmp::Reverse(candidate)) = heap.pop() {
            if seen.contains(&candidate.id) {
                continue;
            }
            
            seen.insert(candidate.id);
            result.push(candidate);
            
            if result.len() >= k {
                break;
            }
        }
        
        Ok(result)
    }
}

impl Default for MultiLevelSearch {
    fn default() -> Self {
        Self::new()
    }
}
