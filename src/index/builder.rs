/// 统一的批量索引构建接口
/// 
/// 所有索引类型都实现此trait，从而统一flush时的批量构建流程
use crate::types::{RowId, Row};
use crate::error::Result;

/// 批量索引构建器
pub trait IndexBuilder: Send + Sync {
    /// 从MemTable批量构建索引
    /// 
    /// 在LSM flush时调用，一次性处理一批数据
    /// 
    /// # Arguments
    /// * `rows` - 刚flush的所有行数据 (row_id, row)
    /// 
    /// # Returns
    /// * `Ok(())` - 构建成功
    /// * `Err(e)` - 构建失败
    fn build_from_memtable(&mut self, rows: &[(RowId, Row)]) -> Result<()>;
    
    /// 持久化索引到磁盘
    /// 
    /// 在build_from_memtable之后调用
    fn persist(&mut self) -> Result<()>;
    
    /// 获取索引名称
    fn name(&self) -> &str;
    
    /// 获取构建统计信息（可选）
    fn stats(&self) -> BuildStats {
        BuildStats::default()
    }
}

/// 索引构建统计信息
#[derive(Debug, Default, Clone)]
pub struct BuildStats {
    /// 处理的行数
    pub rows_processed: usize,
    
    /// 构建耗时（毫秒）
    pub build_time_ms: u64,
    
    /// 持久化耗时（毫秒）
    pub persist_time_ms: u64,
    
    /// 索引大小（字节）
    pub index_size_bytes: usize,
}

impl BuildStats {
    pub fn new(rows: usize, build_ms: u64, persist_ms: u64, size: usize) -> Self {
        Self {
            rows_processed: rows,
            build_time_ms: build_ms,
            persist_time_ms: persist_ms,
            index_size_bytes: size,
        }
    }
}
