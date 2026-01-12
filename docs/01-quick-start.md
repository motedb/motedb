# 快速开始

5 分钟快速上手 MoteDB，了解核心功能和使用方法。

## 安装

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
motedb = "0.1"
```

## 基础用法

### 1. 创建数据库

```rust
use motedb::{Database, Result};

fn main() -> Result<()> {
    // 创建或打开数据库
    let db = Database::open("myapp.mote")?;
    
    Ok(())
}
```

### 2. 创建表

```rust
// 创建用户表
db.execute("CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT
)")?;

// 创建文档表（包含向量字段）
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(128)
)")?;
```

### 3. 插入数据

#### 方式 1: SQL 插入（推荐）

```rust
// 单行插入
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25)")?;

// 批量插入（SQL）
db.execute("INSERT INTO users VALUES 
    (2, 'Bob', 'bob@example.com', 30),
    (3, 'Carol', 'carol@example.com', 28)")?;
```

#### 方式 2: API 批量插入（高性能）

```rust
use motedb::types::{Value, SqlRow};
use std::collections::HashMap;

// 批量插入（比 SQL 快 10-20 倍）
let mut rows = Vec::new();
for i in 0..1000 {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(i));
    row.insert("name".to_string(), Value::Text(format!("User{}", i)));
    row.insert("email".to_string(), Value::Text(format!("user{}@example.com", i)));
    row.insert("age".to_string(), Value::Integer(20 + (i % 40)));
    rows.push(row);
}

let row_ids = db.batch_insert_map("users", rows)?;
println!("Inserted {} rows", row_ids.len());
```

### 4. 查询数据

```rust
// 简单查询
let results = db.query("SELECT * FROM users WHERE age > 25")?;
println!("Found {} users", results.row_count());

// 遍历结果
for row_map in results.rows_as_maps() {
    if let Some(name) = row_map.get("name") {
        println!("Name: {:?}", name);
    }
}

// 聚合查询
let avg_result = db.query("SELECT AVG(age) as avg_age FROM users")?;
```

### 5. 更新和删除

```rust
// 更新数据
db.execute("UPDATE users SET age = 26 WHERE name = 'Alice'")?;

// 删除数据
db.execute("DELETE FROM users WHERE age < 20")?;
```

## 创建索引

索引可以大幅提升查询性能。

### 列索引（等值/范围查询）

```rust
// 创建列索引（性能提升 40 倍）
db.execute("CREATE INDEX users_email ON users(email)")?;

// 查询会自动使用索引
let results = db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
```

### 向量索引（相似度搜索）

```rust
// 创建向量索引
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// 向量 KNN 搜索
let query = "SELECT * FROM documents 
             ORDER BY embedding <-> [0.1, 0.2, ..., 0.5] 
             LIMIT 10";
let results = db.query(query)?;
```

### 全文索引（文本搜索）

```rust
// 创建全文索引
db.execute("CREATE TEXT INDEX articles_content ON articles(content)")?;

// BM25 全文搜索
let results = db.query(
    "SELECT * FROM articles WHERE MATCH(content, 'rust database')"
)?;
```

## 事务支持

```rust
// 开始事务
let tx_id = db.begin_transaction()?;

db.execute("INSERT INTO users VALUES (100, 'Dave', 'dave@example.com', 35)")?;
db.execute("INSERT INTO users VALUES (101, 'Eve', 'eve@example.com', 32)")?;

// 提交事务
db.commit_transaction(tx_id)?;

// 或者回滚
// db.rollback_transaction(tx_id)?;
```

### Savepoint（事务内检查点）

```rust
let tx_id = db.begin_transaction()?;

db.execute("INSERT INTO users VALUES (200, 'Frank', 'frank@example.com', 40)")?;
db.savepoint(tx_id, "sp1")?;

db.execute("INSERT INTO users VALUES (201, 'Grace', 'grace@example.com', 38)")?;
db.rollback_to_savepoint(tx_id, "sp1")?; // 只回滚 Grace

db.commit_transaction(tx_id)?; // Frank 会被保留
```

## 持久化

```rust
// 手动刷新到磁盘
db.flush()?;

// 关闭数据库（自动刷新）
db.close()?;
```

## 完整示例

```rust
use motedb::{Database, Result};
use motedb::types::{Value, SqlRow};
use std::collections::HashMap;

fn main() -> Result<()> {
    // 1. 打开数据库
    let db = Database::open("demo.mote")?;
    
    // 2. 创建表
    db.execute("CREATE TABLE products (
        id INT,
        name TEXT,
        price FLOAT,
        category TEXT
    )")?;
    
    // 3. 批量插入数据
    let mut rows = Vec::new();
    for i in 0..100 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(i));
        row.insert("name".to_string(), Value::Text(format!("Product{}", i)));
        row.insert("price".to_string(), Value::Float((i as f64) * 9.99));
        row.insert("category".to_string(), Value::Text(
            if i % 2 == 0 { "Electronics" } else { "Books" }.to_string()
        ));
        rows.push(row);
    }
    db.batch_insert_map("products", rows)?;
    
    // 4. 创建索引
    db.execute("CREATE INDEX products_category ON products(category)")?;
    
    // 5. 查询数据
    let results = db.query("SELECT * FROM products WHERE category = 'Electronics' AND price < 200")?;
    println!("Found {} products", results.row_count());
    
    // 6. 聚合查询
    let avg_result = db.query("SELECT category, AVG(price) as avg_price FROM products GROUP BY category")?;
    for row_map in avg_result.rows_as_maps() {
        println!("Category: {:?}, Avg Price: {:?}", 
            row_map.get("category"), 
            row_map.get("avg_price"));
    }
    
    // 7. 持久化
    db.flush()?;
    
    Ok(())
}
```

## 性能提示

1. **批量操作优先**: 使用 `batch_insert_map()` 而非逐行插入（快 10-20 倍）
2. **合理使用索引**: 对频繁查询的列创建索引（性能提升 40 倍）
3. **使用事务**: 多个操作放在一个事务中提高性能
4. **定期 flush**: 确保数据持久化

## 下一步

- [SQL 操作详解](./03-sql-operations.md) - 学习完整 SQL 语法
- [批量操作指南](./04-batch-operations.md) - 高性能批量插入技巧
- [索引系统](./06-indexes-overview.md) - 深入了解五大索引类型
- [API 参考](./14-api-reference.md) - 完整 API 文档

## 常见问题

**Q: 如何选择使用 SQL 还是 API？**  
A: 优先使用 SQL API，除非需要极致性能的批量插入，才使用 `batch_insert_map()`。

**Q: 索引什么时候创建？**  
A: 数据插入后创建索引，或者插入前创建（会自动增量更新）。

**Q: 如何保证数据不丢失？**  
A: 使用事务 + 定期 `flush()`，或在关键操作后调用 `flush()`。

---

**上一篇**: [安装配置](./02-installation.md)  
**下一篇**: [SQL 操作](./03-sql-operations.md)
