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
- FLOAT columns accept quoted numeric spellings: `'3.5'` and the IEEE
  specials `'NaN'` / `'inf'` / `'Infinity'` (these have no bare SQL
  literal form — a bare `NaN` parses as a column name). Mirrors the
  convention where DATE/VECTOR columns accept their string spellings;
  unparseable text still fails with a clear type error. Regression:
  `tests/test_bug_hunt_v105.rs::probe_nan_inf_roundtrip`.

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

#### Date/time functions

| Function | Behavior |
|------|------|
| `YEAR/MONTH/DAY_OF_MONTH(x)` | Accept **both TIMESTAMP and DATE** columns |
| `HOUR/MINUTE/SECOND(ts)` | TIMESTAMP columns; pre-epoch times use `rem_euclid` so hours stay 0-23 |
| `DATEDIFF(a, b)` | Signed difference **a − b in seconds** (engine convention; not MySQL's days). Accepts TIMESTAMP / DATE / INTEGER(µs) / ISO string literals |
| `CAST(x AS TEXT)` | DATE → `YYYY-MM-DD`; TIMESTAMP → `YYYY-MM-DD HH:MM:SS[.ffffff]`; BLOB → interpreted as UTF-8; VECTOR → `[1, 2, 3]` |
| `CONCAT(a, b, ...)` | **Skips** NULL operands (PostgreSQL / SQLite≥3.44 semantics); the `\|\|` operator propagates NULL per the SQL standard |

- ISO string parsing (INSERT / literals) preserves **fractional seconds**
  to microsecond precision: `'2024-06-01 12:00:00.5'`.

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
SELECT COUNT(*) FROM frames WHERE payload = X'00fffe0a0b';  -- byte equality
```

- Python: `params=[..., b"\x00\xff"]`; retrieved as `bytes` (`bytearray`
  also accepted on input)
- `LENGTH(blob)` returns the **byte count** (SQLite semantics)
- 16 MiB per value cap (prevents accidentally buffering whole files)
- BLOB predicates and scalar functions work on every scan path, including
  segment-merge rewrites (`WHERE LENGTH(payload) > 2`, `IS NOT NULL`)
  — regression: `tests/test_blob_where.rs` (incl. compaction survival).

