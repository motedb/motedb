# 安装与配置

详细的安装说明和数据库配置指南。

## 系统要求

- **Rust**: 1.70 或更高版本
- **操作系统**: Linux, macOS, Windows
- **内存**: 推荐 512MB 以上
- **磁盘**: 取决于数据量

## 安装方式

### 方式 1: 使用 Cargo（推荐）

在项目的 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
motedb = "0.1"
```

### 方式 2: 从源码构建

```bash
# 克隆仓库
git clone https://github.com/yourusername/motedb.git
cd motedb

# 构建项目
cargo build --release

# 运行测试
cargo test

# 运行示例
cargo run --example quick_start --release
```

## 基础配置

### 默认配置

使用默认配置创建数据库：

```rust
use motedb::Database;

let db = Database::open("myapp.mote")?;
```

### 自定义配置

使用 `DBConfig` 自定义数据库配置：

```rust
use motedb::{Database, DBConfig};

let config = DBConfig {
    // Memtable 大小（MB）
    memtable_size_mb: 16,
    
    // 行缓存大小（条数）
    row_cache_size: 10000,
    
    // LSM 层级数
    lsm_max_levels: 4,
    
    // 压缩策略
    compression: true,
    
    // WAL 启用
    enable_wal: true,
    
    // 自动 flush 间隔（秒）
    auto_flush_interval: 60,
    
    // 默认持久化级别
    durability_level: motedb::DurabilityLevel::Full,
};

let db = Database::create_with_config("myapp.mote", config)?;
```

## 配置参数详解

### 内存配置

| 参数 | 默认值 | 说明 | 推荐值 |
|-----|-------|-----|-------|
| `memtable_size_mb` | 8 | 内存表大小 | 8-32 MB |
| `row_cache_size` | 10000 | 行缓存容量 | 1000-50000 |

#### 示例：低内存环境

```rust
let config = DBConfig {
    memtable_size_mb: 4,
    row_cache_size: 1000,
    ..Default::default()
};
```

#### 示例：高性能环境

```rust
let config = DBConfig {
    memtable_size_mb: 32,
    row_cache_size: 50000,
    ..Default::default()
};
```

### 持久化配置

#### DurabilityLevel

控制数据持久化保证级别：

```rust
use motedb::DurabilityLevel;

pub enum DurabilityLevel {
    /// 无持久化保证（最快，数据可能丢失）
    None,
    
    /// 仅内存刷新（较快，进程崩溃可能丢失数据）
    Memory,
    
    /// 完整持久化（最安全，性能稍低）
    Full,
}
```

#### 配置示例

```rust
// 高性能模式（可能丢失数据）
let config = DBConfig {
    durability_level: DurabilityLevel::Memory,
    enable_wal: false,
    ..Default::default()
};

// 安全模式（推荐生产环境）
let config = DBConfig {
    durability_level: DurabilityLevel::Full,
    enable_wal: true,
    auto_flush_interval: 30,
    ..Default::default()
};
```

### LSM-Tree 配置

```rust
let config = DBConfig {
    // LSM 最大层级
    lsm_max_levels: 4,
    
    // 启用压缩
    compression: true,
    
    // Bloom Filter（减少磁盘读取）
    bloom_filter_bits: 10,
    
    ..Default::default()
};
```

### 索引配置

#### 向量索引

```rust
// 在创建向量索引时配置
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// 高召回率配置（通过 API）
db.create_vector_index("docs_embedding", 128)?;
// 默认配置: R=32, L=50, alpha=1.2
```

#### 空间索引

```rust
use motedb::BoundingBox;

let bounds = BoundingBox {
    min_x: -180.0,
    min_y: -90.0,
    max_x: 180.0,
    max_y: 90.0,
};

db.create_spatial_index("locations_coords", bounds)?;
```

## 性能调优

### 场景 1: 批量写入优先

```rust
let config = DBConfig {
    memtable_size_mb: 32,       // 增大内存表
    row_cache_size: 1000,       // 减小缓存
    enable_wal: false,          // 关闭 WAL（提升写入速度）
    durability_level: DurabilityLevel::Memory,
    auto_flush_interval: 120,   // 延长 flush 间隔
    ..Default::default()
};
```

### 场景 2: 查询优先

```rust
let config = DBConfig {
    memtable_size_mb: 8,        // 标准内存表
    row_cache_size: 50000,      // 增大缓存
    enable_wal: true,
    durability_level: DurabilityLevel::Full,
    bloom_filter_bits: 12,      // 更大的 Bloom Filter
    ..Default::default()
};
```

### 场景 3: 平衡模式（推荐）

```rust
let config = DBConfig {
    memtable_size_mb: 16,
    row_cache_size: 10000,
    enable_wal: true,
    durability_level: DurabilityLevel::Full,
    auto_flush_interval: 60,
    compression: true,
    ..Default::default()
};
```

## 文件结构

MoteDB 在数据目录下创建以下文件：

```
myapp.mote/
├── manifest.json          # 元数据清单
├── wal/                   # Write-Ahead Log
│   └── 000001.wal
├── tables/                # 表数据
│   └── users/
│       ├── data.sst       # SSTable 数据
│       └── data.idx       # 索引文件
├── indexes/               # 索引数据
│   ├── users_email.idx
│   ├── docs_embedding.diskann
│   └── locations_coords.rtree
└── checkpoints/           # 检查点
    └── checkpoint_001.dat
```

## 环境变量

可选的环境变量配置：

```bash
# 日志级别
export MOTEDB_LOG_LEVEL=debug

# 数据目录
export MOTEDB_DATA_DIR=/var/lib/motedb

# 最大并发连接数
export MOTEDB_MAX_CONNECTIONS=100
```

## 常见配置问题

### Q1: 如何减少内存占用？

```rust
let config = DBConfig {
    memtable_size_mb: 4,
    row_cache_size: 1000,
    bloom_filter_bits: 8,
    ..Default::default()
};
```

### Q2: 如何提高写入性能？

```rust
let config = DBConfig {
    memtable_size_mb: 32,
    enable_wal: false,
    durability_level: DurabilityLevel::Memory,
    ..Default::default()
};

// 使用批量插入
db.batch_insert_map("users", rows)?;

// 定期手动 flush
db.flush()?;
```

### Q3: 如何保证数据安全？

```rust
let config = DBConfig {
    enable_wal: true,
    durability_level: DurabilityLevel::Full,
    auto_flush_interval: 30,
    ..Default::default()
};

// 关键操作后手动 flush
db.execute("INSERT INTO critical_data VALUES (...)")?;
db.flush()?;
```

## 验证安装

运行以下代码验证安装：

```rust
use motedb::{Database, Result};

fn main() -> Result<()> {
    let db = Database::open("test.mote")?;
    db.execute("CREATE TABLE test (id INT, name TEXT)")?;
    db.execute("INSERT INTO test VALUES (1, 'Hello MoteDB')")?;
    let results = db.query("SELECT * FROM test")?;
    
    assert_eq!(results.row_count(), 1);
    println!("MoteDB 安装成功！");
    
    Ok(())
}
```

## 下一步

- [快速开始](./01-quick-start.md) - 学习基础用法
- [SQL 操作](./03-sql-operations.md) - 了解 SQL 语法
- [性能优化](./12-performance.md) - 深入性能调优

---

**上一篇**: [文档首页](./README.md)  
**下一篇**: [快速开始](./01-quick-start.md)
