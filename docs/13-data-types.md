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
