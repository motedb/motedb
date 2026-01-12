# SQL 操作

MoteDB 完整 SQL 支持，包含 DDL、DML、查询、聚合、JOIN 等功能。

## 核心 API

```rust
// 执行 SQL（INSERT/UPDATE/DELETE/CREATE/DROP）
db.execute(sql: &str) -> Result<QueryResult>

// 查询 SQL（SELECT）
db.query(sql: &str) -> Result<QueryResult>
```

两者实际上是同一个函数的别名，都可以执行任何 SQL 语句。

## DDL（数据定义语言）

### CREATE TABLE

```rust
// 基础表
db.execute("CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT,
    salary FLOAT,
    is_active BOOL
)")?;

// 带向量字段
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(128),
    created_at TIMESTAMP
)")?;

// 带空间字段
db.execute("CREATE TABLE locations (
    id INT,
    name TEXT,
    coords VECTOR(2),
    category TEXT
)")?;
```

### DROP TABLE

```rust
db.execute("DROP TABLE users")?;
```

### 支持的数据类型

| 类型 | 说明 | 示例 |
|-----|-----|-----|
| `INT` | 64位整数 | `42` |
| `FLOAT` | 64位浮点数 | `3.14` |
| `TEXT` | 字符串 | `'Hello'` |
| `BOOL` | 布尔值 | `TRUE`, `FALSE` |
| `VECTOR(n)` | n维向量 | `[0.1, 0.2, 0.3]` |
| `TIMESTAMP` | Unix时间戳 | `1609459200` |

## DML（数据操作语言）

### INSERT

#### 单行插入

```rust
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25, 50000.0, TRUE)")?;
```

#### 多行插入

```rust
db.execute("INSERT INTO users VALUES 
    (2, 'Bob', 'bob@example.com', 30, 60000.0, TRUE),
    (3, 'Carol', 'carol@example.com', 28, 55000.0, FALSE)")?;
```

#### 指定列插入

```rust
db.execute("INSERT INTO users (id, name, email) VALUES (4, 'Dave', 'dave@example.com')")?;
```

#### 插入向量数据

```rust
// 使用数组语法
db.execute("INSERT INTO documents VALUES (
    1, 
    'Rust Tutorial', 
    'Learn Rust programming...',
    [0.1, 0.2, 0.3, ..., 0.5],
    1609459200
)")?;
```

### UPDATE

```rust
// 更新单个字段
db.execute("UPDATE users SET age = 26 WHERE name = 'Alice'")?;

// 更新多个字段
db.execute("UPDATE users SET age = 31, salary = 65000.0 WHERE id = 2")?;

// 条件更新
db.execute("UPDATE users SET is_active = FALSE WHERE age < 20")?;
```

### DELETE

```rust
// 删除特定行
db.execute("DELETE FROM users WHERE id = 3")?;

// 条件删除
db.execute("DELETE FROM users WHERE age < 18 OR is_active = FALSE")?;

// 删除所有行
db.execute("DELETE FROM users")?;
```

## SELECT 查询

### 基础查询

```rust
// 查询所有列
let results = db.query("SELECT * FROM users")?;

// 查询特定列
let results = db.query("SELECT name, email FROM users")?;

// 带条件查询
let results = db.query("SELECT * FROM users WHERE age > 25")?;
```

### WHERE 条件

支持的运算符：

| 运算符 | 说明 | 示例 |
|-------|-----|-----|
| `=` | 等于 | `WHERE age = 25` |
| `!=`, `<>` | 不等于 | `WHERE age != 30` |
| `>`, `>=` | 大于（等于） | `WHERE age > 18` |
| `<`, `<=` | 小于（等于） | `WHERE salary <= 50000` |
| `AND` | 逻辑与 | `WHERE age > 18 AND is_active = TRUE` |
| `OR` | 逻辑或 | `WHERE age < 20 OR age > 60` |
| `IN` | 范围匹配 | `WHERE id IN (1, 2, 3)` |
| `LIKE` | 模式匹配 | `WHERE name LIKE '%Alice%'` |

#### 示例

```rust
// 复合条件
let results = db.query("
    SELECT * FROM users 
    WHERE age >= 25 AND age <= 35 
    AND is_active = TRUE
")?;

// IN 查询
let results = db.query("
    SELECT * FROM users 
    WHERE id IN (1, 2, 3, 5, 8)
")?;

// LIKE 模糊查询
let results = db.query("
    SELECT * FROM users 
    WHERE email LIKE '%@example.com'
")?;
```

### ORDER BY

```rust
// 升序
let results = db.query("SELECT * FROM users ORDER BY age ASC")?;

// 降序
let results = db.query("SELECT * FROM users ORDER BY salary DESC")?;

// 多列排序
let results = db.query("
    SELECT * FROM users 
    ORDER BY age DESC, name ASC
")?;
```

### LIMIT 和 OFFSET

```rust
// 限制返回行数
let results = db.query("SELECT * FROM users LIMIT 10")?;

// 分页查询
let results = db.query("SELECT * FROM users LIMIT 10 OFFSET 20")?;

// 组合使用
let results = db.query("
    SELECT * FROM users 
    WHERE age > 18 
    ORDER BY created_at DESC 
    LIMIT 50
")?;
```

## 聚合函数

### 支持的聚合函数

| 函数 | 说明 | 示例 |
|-----|-----|-----|
| `COUNT(*)` | 计数 | `SELECT COUNT(*) FROM users` |
| `SUM(col)` | 求和 | `SELECT SUM(salary) FROM users` |
| `AVG(col)` | 平均值 | `SELECT AVG(age) FROM users` |
| `MIN(col)` | 最小值 | `SELECT MIN(age) FROM users` |
| `MAX(col)` | 最大值 | `SELECT MAX(salary) FROM users` |

### 示例

```rust
// 总数
let result = db.query("SELECT COUNT(*) as total FROM users")?;

// 平均值
let result = db.query("SELECT AVG(age) as avg_age FROM users")?;

// 多个聚合
let result = db.query("
    SELECT 
        COUNT(*) as total,
        AVG(age) as avg_age,
        MIN(age) as min_age,
        MAX(age) as max_age,
        SUM(salary) as total_salary
    FROM users
")?;
```

### GROUP BY

```rust
// 按单列分组
let result = db.query("
    SELECT category, COUNT(*) as count
    FROM products
    GROUP BY category
")?;

// 按多列分组
let result = db.query("
    SELECT category, is_active, AVG(price) as avg_price
    FROM products
    GROUP BY category, is_active
")?;

// 带 HAVING 过滤
let result = db.query("
    SELECT category, COUNT(*) as count
    FROM products
    GROUP BY category
    HAVING count > 10
")?;
```

## JOIN 查询

### INNER JOIN

```rust
let result = db.query("
    SELECT users.name, orders.order_id, orders.amount
    FROM users
    INNER JOIN orders ON users.id = orders.user_id
")?;
```

### LEFT JOIN

```rust
let result = db.query("
    SELECT users.name, orders.order_id
    FROM users
    LEFT JOIN orders ON users.id = orders.user_id
")?;
```

### 多表 JOIN

```rust
let result = db.query("
    SELECT 
        users.name,
        orders.order_id,
        products.product_name
    FROM users
    INNER JOIN orders ON users.id = orders.user_id
    INNER JOIN products ON orders.product_id = products.id
    WHERE users.is_active = TRUE
")?;
```

## 子查询

### WHERE 子查询

```rust
let result = db.query("
    SELECT * FROM users
    WHERE age > (SELECT AVG(age) FROM users)
")?;

// IN 子查询
let result = db.query("
    SELECT * FROM products
    WHERE category_id IN (
        SELECT id FROM categories WHERE is_active = TRUE
    )
")?;
```

### FROM 子查询

```rust
let result = db.query("
    SELECT avg_age, COUNT(*) as count
    FROM (
        SELECT age, AVG(salary) as avg_age
        FROM users
        GROUP BY age
    ) subquery
    WHERE avg_age > 50000
    GROUP BY avg_age
")?;
```

## 索引操作

### 创建索引

```rust
// 列索引
db.execute("CREATE INDEX users_email ON users(email)")?;

// 向量索引
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// 全文索引
db.execute("CREATE TEXT INDEX articles_content ON articles(content)")?;

// 空间索引
db.execute("CREATE SPATIAL INDEX locations_coords ON locations(coords)")?;
```

### 删除索引

```rust
db.execute("DROP INDEX users_email ON users")?;
```

### 查看索引

```rust
let result = db.query("SHOW INDEXES FROM users")?;
```

## 特殊操作

### 向量搜索

```rust
// 使用 <-> 运算符（L2距离）
let result = db.query("
    SELECT * FROM documents
    ORDER BY embedding <-> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;

// 使用 <#> 运算符（内积）
let result = db.query("
    SELECT * FROM documents
    ORDER BY embedding <#> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;

// 使用 <=> 运算符（余弦距离）
let result = db.query("
    SELECT * FROM documents
    ORDER BY embedding <=> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;
```

### 全文搜索

```rust
// MATCH 函数
let result = db.query("
    SELECT * FROM articles
    WHERE MATCH(content, 'rust database')
")?;

// 带 BM25 分数
let result = db.query("
    SELECT *, BM25_SCORE(content, 'rust database') as score
    FROM articles
    WHERE MATCH(content, 'rust database')
    ORDER BY score DESC
    LIMIT 20
")?;
```

### 空间查询

```rust
// ST_WITHIN 函数
let result = db.query("
    SELECT * FROM locations
    WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)
")?;

// ST_DISTANCE 函数
let result = db.query("
    SELECT *, ST_DISTANCE(coords, 116.4, 39.9) as distance
    FROM locations
    ORDER BY distance ASC
    LIMIT 10
")?;
```

## 解析查询结果

```rust
let results = db.query("SELECT * FROM users WHERE age > 25")?;

// 获取列信息
println!("Columns: {:?}", results.columns);

// 遍历行
for row_map in results.rows_as_maps() {
    // 获取字段值
    if let Some(name) = row_map.get("name") {
        println!("Name: {:?}", name);
    }
    
    if let Some(age) = row_map.get("age") {
        if let motedb::Value::Integer(age_val) = age {
            println!("Age: {}", age_val);
        }
    }
}

// 获取行数
println!("Total rows: {}", results.row_count());
```

## 最佳实践

1. **使用参数化查询**（避免 SQL 注入）
2. **创建合适的索引**（提升查询性能）
3. **使用 LIMIT**（限制返回数据量）
4. **避免 SELECT ***（明确指定需要的列）
5. **使用事务**（保证数据一致性）

## 下一步

- [批量操作](./04-batch-operations.md) - 高性能批量插入
- [索引系统](./06-indexes-overview.md) - 深入了解索引
- [API 参考](./14-api-reference.md) - 完整 API 文档

---

**上一篇**: [快速开始](./01-quick-start.md)  
**下一篇**: [批量操作](./04-batch-operations.md)
