# SQL Operations

MoteDB provides full SQL support, including DDL, DML, queries, aggregation, JOINs, and more.

## Core API

```rust
// Execute SQL (INSERT/UPDATE/DELETE/CREATE/DROP)
db.execute(sql: &str) -> Result<QueryResult>

// Query SQL (SELECT)
db.query(sql: &str) -> Result<QueryResult>
```

Both are aliases for the same function and can execute any SQL statement.

## DDL (Data Definition Language)

### CREATE TABLE

```rust
// Basic table
db.execute("CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT,
    salary FLOAT,
    is_active BOOL
)")?;

// With vector field
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(128),
    created_at TIMESTAMP
)")?;

// With spatial field
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

### Supported Data Types

| Type | Description | Example |
|-----|-----|-----|
| `INT` | 64-bit integer | `42` |
| `FLOAT` | 64-bit floating point | `3.14` |
| `TEXT` | String | `'Hello'` |
| `BOOL` | Boolean | `TRUE`, `FALSE` |
| `VECTOR(n)` | n-dimensional vector | `[0.1, 0.2, 0.3]` |
| `TIMESTAMP` | Unix timestamp | `1609459200` |

## DML (Data Manipulation Language)

### INSERT

#### Single Row Insert

```rust
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25, 50000.0, TRUE)")?;
```

#### Multi-Row Insert

```rust
db.execute("INSERT INTO users VALUES
    (2, 'Bob', 'bob@example.com', 30, 60000.0, TRUE),
    (3, 'Carol', 'carol@example.com', 28, 55000.0, FALSE)")?;
```

#### Insert with Specified Columns

```rust
db.execute("INSERT INTO users (id, name, email) VALUES (4, 'Dave', 'dave@example.com')")?;
```

#### Insert Vector Data

```rust
// Using array syntax
db.execute("INSERT INTO documents VALUES (
    1,
    'Rust Tutorial',
    'Learn Rust programming...',
    [0.1, 0.2, 0.3, ..., 0.5],
    1609459200
)")?;
```

### UPSERT (INSERT ... ON CONFLICT / OR REPLACE)

Sensor-style "insert or accumulate" writes without a separate read-modify-write:

```rust
// SQL-standard form: ON CONFLICT (pk) DO UPDATE
db.execute("INSERT INTO sensors VALUES (7, 'temp', 1.0)
    ON CONFLICT (id) DO UPDATE SET reading = reading + excluded.reading")?;

// Bare forms
db.execute("INSERT INTO sensors VALUES (7, 'temp', 1.0) ON CONFLICT DO NOTHING")?;
db.execute("INSERT OR IGNORE INTO sensors VALUES (7, 'temp', 1.0)")?; // skip duplicates
db.execute("INSERT OR REPLACE INTO sensors VALUES (7, 'temp', 1.0)")?; // full-row replace
```

Semantics:

- In `DO UPDATE SET` expressions, an unqualified column refers to the
  **existing** row; `excluded.col` refers to the **proposed** (new) row —
  same as PostgreSQL/SQLite.
- The conflict target must be the table's primary key (`ON CONFLICT (id)`).
  There are no secondary unique constraints to conflict on yet.
- Works inside explicit transactions, including against rows inserted
  earlier in the same transaction.
- `affected_rows` counts inserted + updated rows; skipped rows count 0.
- Not yet supported on `TIMESERIES` tables.

### TimeSeries TTL

`TIMESERIES(ts) TTL <seconds>` attaches a retention policy to a TimeSeries
table. Expired data is purged at database open and at every checkpoint.

```rust
// Keep the last 24 hours of sensor readings.
db.execute("CREATE TABLE readings (ts TIMESTAMP, v FLOAT) TIMESERIES(ts) TTL 86400")?;
```

Granularity is per columnar **segment** (standard time-series practice): a
segment is dropped once its newest row passes the cutoff. Because time-series
data arrives roughly in time order, flush generations cluster by time and
expired data lands in fully-expired segments. Manual per-range purging is
available via `DELETE FROM t WHERE ts < value` (same granularity).

### EXPLAIN

`EXPLAIN <SELECT>` reports the plan the executor's fast paths would choose,
without executing the query:

```rust
let plan = db.query("EXPLAIN SELECT * FROM readings WHERE id = 42")?;
// step | operator  | detail
//    1 | scan      | table 'readings': strategy=pk_point_lookup(id), rows=1
//    2 | note      | heuristic plan (v1): ...

let plan = db.query("EXPLAIN SELECT * FROM readings ORDER BY val LIMIT 10")?;
//    1 | scan      | table 'readings': strategy=top_k_bounded_heap(LIMIT 10), ...
```

v1 is a heuristic report of the same signals the runtime fast paths key on
(PK equality, column-index equality, ORDER BY + LIMIT top-K); JOIN/subquery
plans are not yet modeled.

### UPDATE

```rust
// Update a single field
db.execute("UPDATE users SET age = 26 WHERE name = 'Alice'")?;

// Update multiple fields
db.execute("UPDATE users SET age = 31, salary = 65000.0 WHERE id = 2")?;

// Conditional update
db.execute("UPDATE users SET is_active = FALSE WHERE age < 20")?;
```

### DELETE

```rust
// Delete specific rows
db.execute("DELETE FROM users WHERE id = 3")?;

// Conditional delete
db.execute("DELETE FROM users WHERE age < 18 OR is_active = FALSE")?;

// Delete all rows
db.execute("DELETE FROM users")?;
```

## SELECT Queries

### Basic Queries

```rust
// Query all columns
let results = db.query("SELECT * FROM users")?;

// Query specific columns
let results = db.query("SELECT name, email FROM users")?;

// Query with conditions
let results = db.query("SELECT * FROM users WHERE age > 25")?;
```

### Column Aliases

`AS` 可省略 —— 隐式别名与显式别名等价（round-20 修复：此前无 `AS` 的别名
会报误导性的 `Multiple statements are not supported` 解析错误）：

```sql
SELECT name n FROM users;                  -- 列引用隐式别名
SELECT score * 2 AS final FROM users;      -- 表达式显式别名
SELECT dept d, COUNT(*) headcount
  FROM users GROUP BY d HAVING headcount > 2;   -- 聚合隐式别名
```

别名可在 `ORDER BY` / `GROUP BY` / `HAVING` 中引用（如上 `GROUP BY d`、
`HAVING headcount`）；`WHERE` 中引用别名会报 `ColumnNotFound`（与标准 SQL
一致 —— `WHERE` 先于投影求值）。

### WHERE Conditions

Supported operators:

| Operator | Description | Example |
|-------|-----|-----|
| `=` | Equal to | `WHERE age = 25` |
| `!=`, `<>` | Not equal to | `WHERE age != 30` |
| `>`, `>=` | Greater than (or equal to) | `WHERE age > 18` |
| `<`, `<=` | Less than (or equal to) | `WHERE salary <= 50000` |
| `AND` | Logical AND | `WHERE age > 18 AND is_active = TRUE` |
| `OR` | Logical OR | `WHERE age < 20 OR age > 60` |
| `IN` | Range matching | `WHERE id IN (1, 2, 3)` |
| `LIKE` | Pattern matching | `WHERE name LIKE '%Alice%'` |

> 🔴 **round-22 修复与语义钉住**：
> * `LIKE` 与 `=` 一致为**大小写敏感**（PostgreSQL 语义；SQLite/MySQL 会在
>   LIKE 中折叠 ASCII 大小写，MoteDB 不会）。
> * 表名**大小写敏感**（存储键精确匹配），列名大小写不敏感——引用时请使
>   用建表时的表名拼写。
> * `SHOW TABLES` / `DESCRIBE` 此前在流式路径上把结果行丢弃、只返回
>   "Tables shown" 消息——现已返回真实行。
> * `ORDER BY ... NULLS FIRST/LAST` 此前在多条排序路径上被静默忽略——
>   现已全路径生效（默认：ASC 时 NULL 最前，DESC 时 NULL 最后）。
> * `ts BETWEEN '字面量' AND '字面量'` 此前恒为空（向量化谓词未做
>   Timestamp-vs-Text 预解析）——现已与 `>= AND <=` 形式一致。
> * DDL 非事务：事务内 `CREATE TABLE` 在 ROLLBACK 后表保留（其事务内
>   插入的行正确回滚）；事务内 `DROP TABLE` 被拒绝执行。

> 🔴 **除零语义（round-24 钉住）**：`x / 0` 对整数**与浮点**除数一律报
> `DivisionByZero` 错误（PostgreSQL 语义；PG 对 `1.0/0.0` 同样报错。
> SQLite 返回 NULL、IEEE 给 ±inf —— MoteDB 均不采用）。`%` 同理。
> ⚠️ 已知不对称（限制）：该错误出现在 **SELECT 投影**时立即报错，但出现在
> **WHERE 过滤**里时被过滤回退路径吞掉、静默返回 0 行 —— 修复需重构过滤
> 错误传播（见 round-20 WHERE 校证的同类权衡），v0.16 候选。

> 🔴 **列名校验（round-20）**：单表 SELECT / UPDATE / DELETE 的 WHERE 中引用
> 不存在的列会立即报 `ColumnNotFound`。此前过滤路径把求值错误静默当作
> "不匹配" —— 拼错列名（`WHERE usr_id = 5`）返回 0 行且无任何错误提示，
> 属于静默错误结果。JOIN 查询与相关子查询不受此校验影响（相关子查询引用
> 外层列是合法的）。WHERE 中引用 SELECT 列表的别名同样报错（与标准 SQL
> 一致：别名在 WHERE 之后绑定）。

#### Examples

```rust
// Compound conditions
let results = db.query("
    SELECT * FROM users
    WHERE age >= 25 AND age <= 35
    AND is_active = TRUE
")?;

// IN query
let results = db.query("
    SELECT * FROM users
    WHERE id IN (1, 2, 3, 5, 8)
")?;

// LIKE fuzzy match
let results = db.query("
    SELECT * FROM users
    WHERE email LIKE '%@example.com'
")?;
```

#### LIKE … ESCAPE — 字面匹配 % / _ / 转义符 (2026-10-24+)

```sql
-- '$' 后的字符按字面匹配: 只命中文本恰为 "100% sure" 的行
SELECT * FROM s WHERE t LIKE '100$% sure' ESCAPE '$';
-- 双写转义符 = 字面转义符: 命中 "a$b"
SELECT * FROM s WHERE t LIKE 'a$$b' ESCAPE '$';
-- ⚠️ LIKE 是全串匹配 — 字面前缀过滤需尾随未转义通配:
SELECT * FROM s WHERE t LIKE 'item_9$%%' ESCAPE '$';  -- 命中 "item_9%…"
```

规则: ESCAPE 取单字符字面量 (多字符/非字面量报解析错误); 转义符悬空
落在模式末尾时按字面转义符匹配 (宽松处理, SQLite 在此报错)。
回归: `tests/test_like_escape.rs` (含 NOT LIKE 组合/语法错误/reopen+索引路径)。

#### CROSS JOIN 与逗号连接 (2026-10-28+)

```sql
-- 笛卡尔积: 两种标准写法等价 (round-19 前均不支持 — CROSS 报
-- "Expected On", 逗号报 "Multiple statements")
SELECT COUNT(*) FROM a CROSS JOIN b;
SELECT a.x, b.y FROM a, b WHERE a.x = 1;   -- 隐式交叉积 + WHERE 过滤
```

回归: `tests/test_bug_hunt_v108.rs::probe_cross_join`。
⚠️ 已知缺口: 列级 `UNIQUE` 约束语法 (`k INT UNIQUE`) 尚不支持
(解析报错), 唯一性请用 PRIMARY KEY 或应用层校验。

#### SUBSTRING 标准 FROM/FOR 形式 (2026-10-26+)

```sql
-- 标准 SQL 形式与逗号形式等价 (round-12 前标准形式解析失败):
SELECT SUBSTRING('abcdef' FROM 2 FOR 3);  -- 'bcd'
SELECT SUBSTRING('abcdef' FROM 2);        -- 'bcdef' (省略 FOR = 到结尾)
SELECT SUBSTRING('abcdef', 2, 3);         -- 'bcd'   (逗号形式, 兼容不变)
SELECT SUBSTR('abcdef', 2, 3);            -- 'bcd'   (别名)
```

语义: 位置 1 起算 (0 视为 1); 负位置从尾部倒数 (SQLite 兼容)。
回归: `tests/test_bug_hunt_v102.rs::probe_string_functions`。

NULL 分工 (自洽设计, 此处明确): `CONCAT(...)` 跳过 NULL 参数
(PostgreSQL `concat` 语义); `||` 运算符传播 NULL (SQL 标准)。

#### 相关子查询 — 外引用绑定 (2026-10-24 修复)

```sql
-- 相关 EXISTS: 内表 b 的行按外行 a.k 逐行判断 — 只返回在 b 中有同 k 值的 a 行
SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k);
-- 相关标量子查询 (SELECT 列表或 WHERE): 每外行执行一次
SELECT id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a;
```

规则: 表限定列按前缀区分内外 — 前缀等于子查询 FROM 表/别名的列是
内引用, 其余前缀 (可在外层解析) 是外引用按外行逐行绑定; 无限定名列
取**最内层**作用域 (`b.k = k` 中 k 即 b.k)。
历史 bug (已修, 回归 `tests/test_correlated_subquery.rs`): 内外表裸列
同名时两侧都被绑到外行 → `b.k = a.k` 恒真 → EXISTS 匹配全部行、
标量计数恒为内表行数; 以及 WHERE 快路径 eager 执行未绑定相关子查询。

### ORDER BY

```rust
// Ascending order
let results = db.query("SELECT * FROM users ORDER BY age ASC")?;

// Descending order
let results = db.query("SELECT * FROM users ORDER BY salary DESC")?;

// Multi-column sort
let results = db.query("
    SELECT * FROM users
    ORDER BY age DESC, name ASC
")?;
```

### LIMIT and OFFSET

```rust
// Limit the number of returned rows
let results = db.query("SELECT * FROM users LIMIT 10")?;

// Paginated query
let results = db.query("SELECT * FROM users LIMIT 10 OFFSET 20")?;

// Combined usage
let results = db.query("
    SELECT * FROM users
    WHERE age > 18
    ORDER BY created_at DESC
    LIMIT 50
")?;
```

## Aggregate Functions

### Supported Aggregate Functions

| Function | Description | Example |
|-----|-----|-----|
| `COUNT(*)` | Count rows | `SELECT COUNT(*) FROM users` |
| `SUM(col)` | Sum values | `SELECT SUM(salary) FROM users` |
| `AVG(col)` | Average value | `SELECT AVG(age) FROM users` |
| `MIN(col)` | Minimum value | `SELECT MIN(age) FROM users` |
| `MAX(col)` | Maximum value | `SELECT MAX(salary) FROM users` |

### Examples

```rust
// Total count
let result = db.query("SELECT COUNT(*) as total FROM users")?;

// Average value
let result = db.query("SELECT AVG(age) as avg_age FROM users")?;

// Multiple aggregates
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
// Group by a single column
let result = db.query("
    SELECT category, COUNT(*) as count
    FROM products
    GROUP BY category
")?;

// Group by multiple columns
let result = db.query("
    SELECT category, is_active, AVG(price) as avg_price
    FROM products
    GROUP BY category, is_active
")?;

// With HAVING filter
let result = db.query("
    SELECT category, COUNT(*) as count
    FROM products
    GROUP BY category
    HAVING count > 10
")?;
```

## JOIN Queries

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

### Multi-Table JOIN

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

## Subqueries

### WHERE Subquery

```rust
let result = db.query("
    SELECT * FROM users
    WHERE age > (SELECT AVG(age) FROM users)
")?;

// IN subquery
let result = db.query("
    SELECT * FROM products
    WHERE category_id IN (
        SELECT id FROM categories WHERE is_active = TRUE
    )
")?;
```

### FROM Subquery

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

## Index Operations

### Creating Indexes

```rust
// Column index
db.execute("CREATE INDEX users_email ON users(email)")?;

// Vector index
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// Full-text index
db.execute("CREATE TEXT INDEX articles_content ON articles(content)")?;

// Spatial index
db.execute("CREATE SPATIAL INDEX locations_coords ON locations(coords)")?;
```

### Dropping Indexes

```rust
db.execute("DROP INDEX users_email ON users")?;
```

### Viewing Indexes

```rust
let result = db.query("SHOW INDEXES FROM users")?;
```

## Special Operations

### Vector Search

```rust
// Using the <-> operator (L2 distance)
let result = db.query("
    SELECT * FROM documents
    ORDER BY embedding <-> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;

// Using the <#> operator (inner product)
let result = db.query("
    SELECT * FROM documents
    ORDER BY embedding <#> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;

// Using the <=> operator (cosine distance)
let result = db.query("
    SELECT * FROM documents
    ORDER BY embedding <=> [0.1, 0.2, ..., 0.5]
    LIMIT 10
")?;
```

### Full-Text Search

```rust
// MATCH function
let result = db.query("
    SELECT * FROM articles
    WHERE MATCH(content, 'rust database')
")?;

// With BM25 score
let result = db.query("
    SELECT *, BM25_SCORE(content, 'rust database') as score
    FROM articles
    WHERE MATCH(content, 'rust database')
    ORDER BY score DESC
    LIMIT 20
")?;
```

### Spatial Queries

```rust
// ST_WITHIN function
let result = db.query("
    SELECT * FROM locations
    WHERE ST_WITHIN(coords, 116.0, 39.0, 117.0, 40.0)
")?;

// ST_DISTANCE function
let result = db.query("
    SELECT *, ST_DISTANCE(coords, 116.4, 39.9) as distance
    FROM locations
    ORDER BY distance ASC
    LIMIT 10
")?;
```

## Parsing Query Results

```rust
let results = db.query("SELECT * FROM users WHERE age > 25")?;

// Get column information
println!("Columns: {:?}", results.columns);

// Iterate over rows
for row_map in results.rows_as_maps() {
    // Get field values
    if let Some(name) = row_map.get("name") {
        println!("Name: {:?}", name);
    }

    if let Some(age) = row_map.get("age") {
        if let motedb::Value::Integer(age_val) = age {
            println!("Age: {}", age_val);
        }
    }
}

// Get row count
println!("Total rows: {}", results.row_count());
```

## Best Practices

1. **Use parameterized queries** (prevent SQL injection)
2. **Create appropriate indexes** (improve query performance)
3. **Use LIMIT** (restrict the amount of returned data)
4. **Avoid SELECT *** (explicitly specify the columns you need)
5. **Use transactions** (ensure data consistency)

## Limits

- **Expression nesting**: at most 64 levels of nested parentheses / unary
  operators / vector literals `[...]` / `CASE` expressions. Deeper input
  returns a syntax error (`Expression nesting too deep`) instead of
  overflowing the stack — any realistic SQL stays far below the limit.

## Next Steps

- [Batch Operations](./04-batch-operations.md) - High-performance batch inserts
- [Index System](./06-indexes-overview.md) - Deep dive into indexes
- [API Reference](./14-api-reference.md) - Complete API documentation

---

**Previous**: [Quick Start](./01-quick-start.md)
**Next**: [Batch Operations](./04-batch-operations.md)


## 参数化 LIMIT / OFFSET（0.12 新增）

```rust,ignore
db.execute_prepared("SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?",
                    vec![Value::Integer(10), Value::Integer(20)])?;
```

Python: `db.query("SELECT ... LIMIT ? OFFSET ?", params=[10, 20])`。

限制：UNION/EXCEPT/INTERSECT 结果集不支持参数化 LIMIT/OFFSET（普通
SELECT 全支持）；参数必须是非负整数。

## Python 混合检索 / Arrow（0.12 新增）

见 [17-hybrid-arrow.md](17-hybrid-arrow.md)。
