//! 🚀 流式合并迭代器 - 真正的 O(1) 内存占用
//!
//! 实现类似 RocksDB 的 MergingIterator，使用多路归并算法逐个返回 key-value，
//! 而不是预先合并所有数据到内存。
//!
//! ## 核心思路
//! - 使用 BinaryHeap（最小堆）管理多个数据源
//! - 每次 `next()` 返回当前最小的 key
//! - 自动处理 MVCC 多版本（选择最新版本）
//! - 自动过滤 tombstone（删除标记）
//!
//! ## 内存占用
//! - 传统方案：30万条 × 1.4 KB = 420 MB 🔴
//! - 流式合并：13 个迭代器 × 1.5 KB = 20 KB ✅
//! - **节省 99.995% 内存**

use super::{Key, Value};
use crate::Result;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

// Type alias for KV iterator
type KVIterator = Box<dyn Iterator<Item = Result<(Key, Value)>> + Send>;

/// 迭代器项（用于堆排序）
#[derive(Debug, Clone)]
struct HeapItem {
    key: Key,
    value: Value,
    source_id: usize,  // 数据源 ID（用于去重后重新填充）
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value.timestamp == other.value.timestamp
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. 按 key 升序（小的优先）
        // 2. 相同 key 按 timestamp 降序（新的优先）
        // 3. 相同 key + timestamp 按 source_id 升序（MemTable 优先）
        self.key.cmp(&other.key)
            .then(other.value.timestamp.cmp(&self.value.timestamp))  // 注意：反向比较
            .then(self.source_id.cmp(&other.source_id))
    }
}

/// 🚀 流式合并迭代器
///
/// 使用多路归并算法，从多个数据源（MemTable, Immutable, SSTables）
/// 逐个返回 key-value，内存占用 O(N) 其中 N = 数据源数量。
///
/// ## 示例
/// ```ignore
/// let iter = lsm_engine.scan_range_streaming(start, end)?;
/// for result in iter {
///     let (key, value) = result?;
///     // 🚀 每次只在内存中保留一条记录！
/// }
/// ```
pub struct MergingIterator {
    /// 最小堆（存储每个数据源的当前最小元素）
    heap: BinaryHeap<Reverse<HeapItem>>,

    /// 各个数据源的迭代器
    sources: Vec<KVIterator>,

    /// 上一次返回的 key（用于去重）
    last_key: Option<Key>,

    /// 是否已结束
    finished: bool,

    /// First error from any source (propagated when heap drains)
    first_error: Option<crate::StorageError>,

    /// 🚀 Single-source fast path: skip heap entirely when only 1 source
    single_source: bool,
}

impl MergingIterator {
    /// Create a new merging iterator
    pub fn new(sources: Vec<KVIterator>) -> Self {
        let single = sources.len() == 1;
        let mut iter = Self {
            heap: BinaryHeap::new(),
            sources,
            last_key: None,
            finished: false,
            first_error: None,
            single_source: single,
        };

        if !single {
            iter.fill_heap();
        }

        iter
    }

    fn fill_heap(&mut self) {
        for (source_id, source) in self.sources.iter_mut().enumerate() {
            match source.next() {
                Some(Ok((key, value))) => {
                    self.heap.push(Reverse(HeapItem { key, value, source_id }));
                }
                Some(Err(e)) => {
                    if self.first_error.is_none() {
                        self.first_error = Some(e);
                    }
                }
                None => {}
            }
        }
    }

    fn refill_from_source(&mut self, source_id: usize) {
        if let Some(source) = self.sources.get_mut(source_id) {
            match source.next() {
                Some(Ok((key, value))) => {
                    self.heap.push(Reverse(HeapItem { key, value, source_id }));
                }
                Some(Err(e)) => {
                    if self.first_error.is_none() {
                        self.first_error = Some(e);
                    }
                }
                None => {}
            }
        }
    }
}

impl Iterator for MergingIterator {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // 🚀 Single-source fast path: skip heap entirely
        if self.single_source {
            let source = match self.sources.get_mut(0) {
                Some(s) => s,
                None => { self.finished = true; return None; }
            };
            loop {
                match source.next() {
                    Some(Ok((key, value))) => {
                        // Dedup check
                        if let Some(last_key) = self.last_key {
                            if key == last_key { continue; }
                        }
                        // Tombstone filter
                        if value.deleted {
                            self.last_key = Some(key);
                            continue;
                        }
                        self.last_key = Some(key);
                        return Some(Ok((key, value)));
                    }
                    Some(Err(e)) => {
                        self.finished = true;
                        return Some(Err(e));
                    }
                    None => {
                        self.finished = true;
                        return None;
                    }
                }
            }
        }

        loop {
            // 1. 从堆顶取出最小的元素
            let Reverse(item) = match self.heap.pop() {
                Some(item) => item,
                None => {
                    self.finished = true;
                    // Propagate the first source error if any
                    if let Some(e) = self.first_error.take() {
                        return Some(Err(e));
                    }
                    return None;
                }
            };

            // 2. 从对应的数据源读取下一个元素，放回堆
            self.refill_from_source(item.source_id);

            // 3. 处理 MVCC 去重：相同 key 只返回最新版本
            if let Some(last_key) = self.last_key {
                if item.key == last_key {
                    // 跳过旧版本
                    continue;
                }
            }

            // 4. 过滤 tombstone（删除标记）
            if item.value.deleted {
                self.last_key = Some(item.key);
                continue;
            }

            // 5. 返回当前 key-value
            self.last_key = Some(item.key);
            return Some(Ok((item.key, item.value)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::lsm::ValueData;

    /// Type alias to reduce complexity of boxed iterator sources in tests.
    type BoxedIter = Box<dyn Iterator<Item = Result<(Key, Value)>> + Send>;
    
    #[test]
    fn test_merging_iterator_basic() {
        // 创建两个数据源
        let source1: Vec<Result<(Key, Value)>> = vec![
            Ok((1, Value::new(vec![1], 100))),
            Ok((3, Value::new(vec![3], 100))),
            Ok((5, Value::new(vec![5], 100))),
        ];
        
        let source2: Vec<Result<(Key, Value)>> = vec![
            Ok((2, Value::new(vec![2], 100))),
            Ok((4, Value::new(vec![4], 100))),
            Ok((6, Value::new(vec![6], 100))),
        ];
        
        let sources: Vec<BoxedIter> = vec![
            Box::new(source1.into_iter()),
            Box::new(source2.into_iter()),
        ];
        
        let iter = MergingIterator::new(sources);
        let keys: Vec<Key> = iter.map(|r| r.unwrap().0).collect();
        
        assert_eq!(keys, vec![1, 2, 3, 4, 5, 6]);
    }
    
    #[test]
    fn test_merging_iterator_mvcc() {
        // 相同 key 在不同数据源（模拟多版本）
        let source1: Vec<Result<(Key, Value)>> = vec![
            Ok((1, Value { 
                data: ValueData::Inline(vec![1, 0, 0]),  // v3 (newest)
                timestamp: 300,
                deleted: false,
            })),
        ];
        
        let source2: Vec<Result<(Key, Value)>> = vec![
            Ok((1, Value { 
                data: ValueData::Inline(vec![1, 0]),  // v2
                timestamp: 200,
                deleted: false,
            })),
        ];
        
        let source3: Vec<Result<(Key, Value)>> = vec![
            Ok((1, Value { 
                data: ValueData::Inline(vec![1]),  // v1 (oldest)
                timestamp: 100,
                deleted: false,
            })),
        ];
        
        let sources: Vec<BoxedIter> = vec![
            Box::new(source1.into_iter()),
            Box::new(source2.into_iter()),
            Box::new(source3.into_iter()),
        ];
        
        let iter = MergingIterator::new(sources);
        let results: Vec<(Key, Vec<u8>)> = iter.map(|r| {
            let (k, v) = r.unwrap();
            match v.data {
                ValueData::Inline(data) => (k, data),
                _ => panic!("Expected inline data"),
            }
        }).collect();
        
        // 应该只返回最新版本
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1, vec![1, 0, 0]);  // v3
    }
    
    #[test]
    fn test_merging_iterator_tombstone() {
        // 测试 tombstone 过滤
        let source1: Vec<Result<(Key, Value)>> = vec![
            Ok((1, Value::new(vec![1], 100))),
            Ok((2, Value { 
                data: ValueData::Inline(vec![]),
                timestamp: 200,
                deleted: true,  // tombstone
            })),
            Ok((3, Value::new(vec![3], 100))),
        ];
        
        let sources: Vec<BoxedIter> = vec![
            Box::new(source1.into_iter()),
        ];
        
        let iter = MergingIterator::new(sources);
        let keys: Vec<Key> = iter.map(|r| r.unwrap().0).collect();
        
        // key=2 应该被过滤掉
        assert_eq!(keys, vec![1, 3]);
    }
}
