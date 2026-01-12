# 数据类型参考

MoteDB 使用 `Value` 枚举统一管理所有存储层类型，SQL 层通过 `SqlRow = HashMap<String, Value>` 传递数据。本文档列出可用类型及其对应的 Rust API。

## Value 枚举

```rust
pub enum Value {
    Integer(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Vector(Vec<f32>),
    Tensor(Tensor),
    Spatial(Geometry),
    TextDoc(Text),
    Timestamp(Timestamp),
    Null,
}
```

### 整数 / 浮点 / 布尔

- SQL: `INT`, `FLOAT`, `BOOL`
- Rust: `Value::Integer`, `Value::Float`, `Value::Bool`
- 自动支持比较、范围查询、聚合

### 文本

- SQL: `TEXT`
- Rust: `Value::Text("alice".into())`
- 可参与 LIKE、全文检索（配合 `TextDoc`）

### 向量

- SQL: `VECTOR(n)`
- Rust: `Value::Vector(vec![0.1; 128])`
- 与向量索引、空间索引兼容（空间场景推荐 2 维）

### Tensor（FP16）

- legacy 类型，用于兼容历史 FP16 张量
- Rust: `Value::Tensor(Tensor::from_vec_f16(...))`

### Spatial

```rust
use motedb::types::{Geometry, Point, BoundingBox};

let point = Geometry::Point(Point::new(116.4, 39.9));
let value = Value::Spatial(point);
```

- 支持 `Point`, `LineString`, `Polygon`
- `BoundingBox` 用于创建空间索引与范围查询

### TextDoc

- 全文索引 legacy 类型（多字段混合时使用）
- 建议直接使用 `TEXT` + `CREATE TEXT INDEX`

### Timestamp

```rust
use motedb::types::Timestamp;
let ts = Timestamp::from_secs(1_700_000_000);
let value = Value::Timestamp(ts);
```

- 内部以微秒存储，支持范围查询、排序

## Row vs SqlRow

| 类型 | 定义 | 用途 |
|------|------|------|
| `Row` | `Vec<Value>` | 存储引擎原生行（内部使用） |
| `SqlRow` | `HashMap<String, Value>` | API/SQL 层（推荐） |

API `*_map` 方法会自动在两者之间转换，例如：

```rust
let row_id = db.insert_row_map("users", sql_row)?;
let maybe_row = db.get_row_map("users", row_id)?;
```

## Schema 定义

通过 SQL DDL 或 `TableSchema` 指定类型：

```rust
use motedb::types::{TableSchema, ColumnDef, ColumnType};

let schema = TableSchema::new("users")
    .with_column(ColumnDef::new("id", ColumnType::Integer))
    .with_column(ColumnDef::new("embedding", ColumnType::Vector(128)));
```

> 常见 `ColumnType`: `Integer`, `Float`, `Bool`, `Text`, `Vector(usize)`, `Timestamp`, `Spatial`。

## 类型转换与比较

- `Value::Integer` 与 `Value::Float` 可互相比较
- 其他类型需严格匹配（如 `Text` vs `Text`）
- `Value::Null` 仅用于占位（建议在应用层处理）

## 常见问题

| 问题 | 说明 |
|------|------|
| SQL 写入 `NULL` | 对应 `Value::Null`，索引会跳过该条目 |
| 向量维度不符 | `CREATE VECTOR INDEX` 的 dimension 必须与 `Value::Vector` 长度一致 |
| 时间戳单位混乱 | API 使用秒/毫秒创建，内部自动转微秒；查询时保持一致 |

---

- 相关文档：[07~11 索引专题](./07-column-index.md)、[05 事务管理](./05-transactions.md)
