# Data Types Reference

MoteDB uses the `Value` enum to unify all storage-layer types. The SQL layer passes data through `SqlRow = HashMap<String, Value>`. This document lists the available types and their corresponding Rust APIs.

## Value Enum

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

### Integer / Float / Bool

- SQL: `INT`, `FLOAT`, `BOOL`
- Rust: `Value::Integer`, `Value::Float`, `Value::Bool`
- Automatically supports comparison, range queries, and aggregation
- 🔖 FLOAT 列接受引号数字拼写 (2026-10-27+)：`'3.5'` 以及 IEEE 特殊值
  `'NaN'` / `'inf'` / `'Infinity'`（这些值没有 SQL 字面量形式，裸 `NaN`
  会按列名解析）。与 DATE/VECTOR 接受各自字符串拼写的约定对齐；
  不可解析文本仍报清晰的类型错误。回归:
  `tests/test_bug_hunt_v105.rs::probe_nan_inf_roundtrip`。

### Text

- SQL: `TEXT`
- Rust: `Value::Text("alice".into())`
- Compatible with LIKE and full-text search (when combined with `TextDoc`)

### Vector

- SQL: `VECTOR(n)`
- Rust: `Value::Vector(vec![0.1; 128])`
- Compatible with vector indexes and spatial indexes (2D recommended for spatial use cases)

### Tensor (FP16)

- Legacy type, used for backward compatibility with historical FP16 tensors
- Rust: `Value::Tensor(Tensor::from_vec_f16(...))`

### Spatial

```rust
use motedb::types::{Geometry, Point, BoundingBox};

let point = Geometry::Point(Point::new(116.4, 39.9));
let value = Value::Spatial(point);
```

- Supports `Point`, `LineString`, `Polygon`
- `BoundingBox` is used for creating spatial indexes and range queries

### TextDoc

- Legacy type for full-text indexing (used when mixing multiple fields)
- Recommended approach: use `TEXT` + `CREATE TEXT INDEX` directly

### Timestamp

```rust
use motedb::types::Timestamp;
let ts = Timestamp::from_secs(1_700_000_000);
let value = Value::Timestamp(ts);
```

- Stored internally as microseconds; supports range queries and sorting

## Row vs SqlRow

| Type | Definition | Purpose |
|------|------------|---------|
| `Row` | `Vec<Value>` | Storage engine native row (internal use) |
| `SqlRow` | `HashMap<String, Value>` | API/SQL layer (recommended) |

The `*_map` API methods automatically convert between the two, for example:

```rust
let row_id = db.insert_row_map("users", sql_row)?;
let maybe_row = db.get_row_map("users", row_id)?;
```

## Schema Definition

Specify types via SQL DDL or `TableSchema`:

```rust
use motedb::types::{TableSchema, ColumnDef, ColumnType};

let schema = TableSchema::new("users")
    .with_column(ColumnDef::new("id", ColumnType::Integer))
    .with_column(ColumnDef::new("embedding", ColumnType::Vector(128)));
```

> Common `ColumnType` values: `Integer`, `Float`, `Bool`, `Text`, `Vector(usize)`, `Timestamp`, `Spatial`.

## Type Conversion and Comparison

- `Value::Integer` and `Value::Float` can be compared with each other
- Other types require strict matching (e.g., `Text` vs `Text`)
- `Value::Null` is only used as a placeholder (recommended to handle at the application layer)

## Common Issues

| Issue | Description |
|-------|-------------|
| SQL writes `NULL` | Maps to `Value::Null`; indexes will skip this entry |
| Vector dimension mismatch | The `dimension` in `CREATE VECTOR INDEX` must match the length of `Value::Vector` |
| Timestamp unit confusion | The API accepts seconds/milliseconds for creation and internally converts to microseconds; keep units consistent when querying |

---

- Related docs: [07~11 Index Topics](./07-column-index.md), [05 Transaction Management](./05-transactions.md)


## BLOB (v0.13)

Binary large object — raw bytes for sensor payloads, serialized messages
(MCAP `data` column) and image frames.

```sql
CREATE TABLE frames (id INTEGER PRIMARY KEY, ts TIMESTAMP, payload BLOB);
INSERT INTO frames VALUES (1, 1700000000000000, X'00fffe0a0b');
SELECT COUNT(*) FROM frames WHERE payload = X'00fffe0a0b';  -- 字节等值
```

- Python: `params=[..., b"\x00\xff"]` / 取回为 `bytes`;`bytearray` 亦可
- `LENGTH(blob)` 返回**字节数**(SQLite 语义)
- 上限 16 MiB/值(防止误将整文件写入行缓冲)
- 已知 v0.13 缺口 **已清零**(2026-10-22): BLOB 列上的标量函数与谓词
  (`WHERE LENGTH(payload) > 2`、`IS NOT NULL`)在 segment 合并扫描路径
  被解码成 NULL 而静默丢行 —— 根因是 MergeCursor 缺 Blob tag 臂 + Text
  无条件 UTF-8 校验(该游标同时驱动 compaction 重写,不修则压实会把
  BLOB 永久写成 NULL),及 row_format 部分解码缺 Blob 臂;均已修复并有
  回归测试(tests/test_blob_where.rs,含 compaction 存活性)。不可索引
