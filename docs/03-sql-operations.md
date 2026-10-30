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

`AS` is optional — implicit aliases are equivalent to explicit ones:

```sql
SELECT name n FROM users;                  -- implicit column alias
SELECT score * 2 AS final FROM users;      -- explicit expression alias
SELECT dept d, COUNT(*) headcount
  FROM users GROUP BY d HAVING headcount > 2;   -- aggregate alias
```

Aliases can be referenced in `ORDER BY` / `GROUP BY` / `HAVING` (as with
`GROUP BY d` and `HAVING headcount` above). Referencing an alias in
`WHERE` raises `ColumnNotFound` — standard SQL semantics (WHERE is
evaluated before the projection).

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

> **Semantics notes**:
> * `LIKE` is **case-sensitive**, same as `=` (PostgreSQL semantics;
>   SQLite/MySQL fold ASCII case in LIKE — MoteDB does not).
> * Table names are **case-sensitive** (exact storage keys); column names
>   are case-insensitive. Quote table names exactly as created.
> * `ORDER BY ... NULLS FIRST/LAST` is honored on every sort path
>   (default: NULLs first on ASC, last on DESC).
> * Division by zero raises `DivisionByZero` for integer **and** float
>   divisors (PostgreSQL semantics — not SQLite's NULL, not IEEE ±inf).
>   `%` behaves the same. Known asymmetry: the error surfaces from
>   SELECT projections, but inside a WHERE filter it is currently
>   swallowed as "no match" (0 rows) — on the roadmap.
> * Unknown columns in a single-table WHERE raise `ColumnNotFound`
>   immediately. JOINs and correlated subqueries (outer references) are
>   unaffected; referencing a SELECT alias in WHERE also errors (aliases
>   bind after WHERE, as in standard SQL).
> * DDL is non-transactional: `CREATE TABLE` inside a rolled-back
>   transaction survives (its transactional inserts are rolled back
>   correctly); `DROP TABLE` inside a transaction is rejected.

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

#### LIKE ... ESCAPE — literal % / _ / escape char

```sql
-- The character after '$' matches literally: only "100% sure" hits
SELECT * FROM s WHERE t LIKE '100$% sure' ESCAPE '$';
-- Doubled escape char = literal escape char: matches "a$b"
SELECT * FROM s WHERE t LIKE 'a$$b' ESCAPE '$';
-- LIKE matches the WHOLE string — a literal-prefix filter needs a
-- trailing unescaped wildcard:
SELECT * FROM s WHERE t LIKE 'item_9$%%' ESCAPE '$';  -- matches "item_9%..."
```

Rules: `ESCAPE` takes a single-character literal (multi-char or
non-literal is a parse error); a dangling escape at the very end of the
pattern matches a literal escape char (lenient — SQLite errors here).
Regressions: `tests/test_like_escape.rs`.

#### CROSS JOIN and comma joins

```sql
-- Cartesian product: both standard spellings are equivalent
SELECT COUNT(*) FROM a CROSS JOIN b;
SELECT a.x, b.y FROM a, b WHERE a.x = 1;   -- implicit cross join + WHERE
```

Regressions: `tests/test_bug_hunt_v108.rs::probe_cross_join`.
Known gap: column-level `UNIQUE` (`k INT UNIQUE`) is not supported yet —
use PRIMARY KEY or application-side checks for uniqueness.

#### SUBSTRING — standard FROM/FOR form

```sql
SELECT SUBSTRING('abcdef' FROM 2);        -- 'bcdef' (FOR omitted = to end)
SELECT SUBSTRING('abcdef', 2, 3);         -- 'bcd'   (comma form)
SELECT SUBSTR('abcdef', 2, 3);            -- 'bcd'   (alias)
```

Semantics: positions are 1-based (0 is treated as 1); negative positions
count from the end (SQLite-compatible).
Regressions: `tests/test_bug_hunt_v102.rs::probe_string_functions`.

NULL handling: `CONCAT(...)` skips NULL arguments (PostgreSQL `concat`
semantics); the `||` operator propagates NULL (SQL standard).

#### Correlated subqueries — outer-reference binding

```sql
-- Correlated EXISTS: rows of inner table b are judged per outer row of a
SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k);
-- Correlated scalar subquery (projection or WHERE): runs per outer row
SELECT id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a;
```

Binding rule: table-qualified columns resolve by prefix — a prefix equal
to the subquery's own FROM table/alias is an inner reference; any other
prefix (resolvable in the outer scope) binds to the outer row, per row.
Unqualified columns take the **innermost** scope (in `b.k = k`, `k` is
`b.k`). Regression: `tests/test_correlated_subquery.rs`.

---

**Previous**: [Quick Start](./01-quick-start.md)
**Next**: [Batch Operations](./04-batch-operations.md)


## Parameterized LIMIT / OFFSET

```rust,ignore
db.execute_prepared("SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?",
                    vec![Value::Integer(10), Value::Integer(20)])?;
```

Python: `db.query("SELECT ... LIMIT ? OFFSET ?", params=[10, 20])`.

Limits: set-operation results (UNION/EXCEPT/INTERSECT) do not support
parameterized LIMIT/OFFSET (plain SELECTs fully do); parameters must be
non-negative integers.

## Python Hybrid Retrieval / Arrow

See [17-hybrid-arrow.md](17-hybrid-arrow.md).
