# MoteDB

全球首款面向具身智能场景的 AI 原生嵌入式数据库。

是专为**家庭机器人、AR 眼镜、工业机械臂**等边缘设备设计的嵌入式数据库，原生支持向量、文本、时序、空间坐标的统一存储与查询。MoteDB 将多模态数据类型作为第一类公民，提供低延迟的在线检索、强一致的数据语义，以及面向具身智能感知与决策的查询扩展。

## 📖 文档目录

### 🚀 入门指南
- **[快速开始](./01-quick-start.md)** - 5 分钟快速上手
- **[安装配置](./02-installation.md)** - 详细安装步骤

### 📘 核心功能
- **[基础操作](./03-basic-operations.md)** - 增删改查（CRUD）
- **[SQL 语法](./04-sql-reference.md)** - 完整 SQL 语法参考
- **[数据类型](./05-data-types.md)** - 支持的数据类型

### 🔍 高级特性
- **[索引系统](./06-index-guide.md)** - 多种索引类型使用指南
- **[向量运算符](./07-vector-operators.md)** - 向量距离运算符详解（<->, <=>, <#>）⭐ NEW
- **[全文搜索](./07-full-text-search.md)** - FTS 和 BM25 搜索
- **[向量检索](./08-vector-search.md)** - 向量相似度搜索
- **[空间索引](./09-spatial-index.md)** - 地理位置查询
- **[时间序列](./10-time-series.md)** - 时间序列数据处理

### ⚡ 性能优化
- **[性能优化指南](./11-performance-tuning.md)** - 最佳实践
- **[批量操作](./12-batch-operations.md)** - 高效批量处理
- **[事务管理](./13-transactions.md)** - ACID 事务

### 🛠️ 实战案例
- **[电商应用](./examples/01-ecommerce.md)** - 商品管理系统
- **[日志分析](./examples/02-log-analysis.md)** - 日志查询分析
- **[推荐系统](./examples/03-recommendation.md)** - 向量相似度推荐

## 🎯 快速导航

### 我想要...

| 需求 | 推荐阅读 |
|------|----------|
| 快速上手 | [快速开始](./01-quick-start.md) |
| 创建表和索引 | [基础操作](./03-basic-operations.md) |
| 提升查询速度 | [索引系统](./06-index-guide.md) + [性能优化](./11-performance-tuning.md) |
| 搜索文本内容 | [全文搜索](./07-full-text-search.md) |
| 相似度推荐 | [向量检索](./08-vector-search.md) |
| 地理位置查询 | [空间索引](./09-spatial-index.md) |
| 时间范围查询 | [时间序列](./10-time-series.md) |
| 批量插入数据 | [批量操作](./12-batch-operations.md) |

## ✨ 核心特性

- 🎯 **AI 原生类型**: `TENSOR` / `SPATIAL` / `TIMESTAMP` / `ACTION` 为一等公民
- 🚀 **超高性能**: PRIMARY KEY 等值查询 P99 0.005ms，复杂查询 P99 0.29ms，写入吞吐 1004 rows/sec
- 💾 **超轻量级**: 二进制 1.2MB，内存占用 18MB
- 🔧 **嵌入式优先**: 单文件存储，无外部依赖，无后台进程
- 📊 **SQL 兼容**: 标准 SQL + 多模态扩展（E-SQL）
- 🔒 **内存安全**: 100% Rust 实现，零 GC
- � **数据持久化**: 完整的 ACID 事务保证，断电不丢失
- ⚡ **多线程支持**: `Arc<Mutex<WAL>>` 架构，支持高并发

<!-- 继续保留更详细的能力与索引说明，见下方文档章节 -->

## 🆘 获取帮助

- **问题反馈**: GitHub Issues
- **功能建议**: GitHub Discussions
- **示例代码**: `examples/` 目录

## 📝 版本信息

- **当前版本**: 0.1.0
- **Rust 最低版本**: 1.70+
- **更新日期**: 2026-01-11

---

**下一步**: 阅读 [快速开始](./01-quick-start.md) 开始你的第一个 MoteDB 应用！
