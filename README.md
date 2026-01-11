# MoteDB

全球首款面向具身智能场景的 AI 原生多模态数据库

欢迎使用 MoteDB —— 一个为具身智能（embodied intelligence）场景设计的 AI 原生多模态数据库。MoteDB 原生支持文本、图像、向量、空间与时间等多种模态数据，提供低延迟的向量检索、强一致性的事务语义、以及面向机器人、AR/VR、边缘设备等具身智能应用的优化查询路径。

核心卖点：原生多模态（multimodal）数据模型、向量与传统索引无缝融合、面向具身智能的检索与感知工作流。

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

## 📦 特性概览

### ✅ 核心能力
- ✅ 完整的 SQL 支持（SELECT, INSERT, UPDATE, DELETE）
- ✅ B-Tree 和 LSM-Tree 双引擎
- ✅ ACID 事务保证
- ✅ WAL 日志和崩溃恢复

### 🔍 索引类型
- ✅ B-Tree 索引（范围查询）
- ✅ LSM-Tree 索引（高并发写入）
- ✅ 全文搜索索引（文本检索 + BM25）
- ✅ 向量索引（DiskANN，相似度搜索）
- ✅ 空间索引（R-Tree，地理查询）
- ✅ 时间序列索引（时间范围查询）

### 🚀 性能特点
- 🚀 百万级数据插入优化
- 🚀 SIMD 加速聚合运算
- 🚀 智能查询优化器
- 🚀 多种索引类型组合优化

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
