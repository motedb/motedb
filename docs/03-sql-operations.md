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

## Next Steps

- [Batch Operations](./04-batch-operations.md) - High-performance batch inserts
- [Index System](./06-indexes-overview.md) - Deep dive into indexes
- [API Reference](./14-api-reference.md) - Complete API documentation

---

**Previous**: [Quick Start](./01-quick-start.md)
**Next**: [Batch Operations](./04-batch-operations.md)
