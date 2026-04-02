# Quick Start

Get started with MoteDB in 5 minutes and learn about its core features and usage.

## Installation

Add the dependency in your `Cargo.toml`:

```toml
[dependencies]
motedb = "0.1"
```

## Basic Usage

### 1. Create a Database

```rust
use motedb::{Database, Result};

fn main() -> Result<()> {
    // Create or open a database
    let db = Database::open("myapp.mote")?;

    Ok(())
}
```

### 2. Create Tables

```rust
// Create a users table
db.execute("CREATE TABLE users (
    id INT,
    name TEXT,
    email TEXT,
    age INT
)")?;

// Create a documents table (with a vector field)
db.execute("CREATE TABLE documents (
    id INT,
    title TEXT,
    content TEXT,
    embedding VECTOR(128)
)")?;
```

### 3. Insert Data

#### Method 1: SQL Insert (Recommended)

```rust
// Single row insert
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 25)")?;

// Batch insert (SQL)
db.execute("INSERT INTO users VALUES
    (2, 'Bob', 'bob@example.com', 30),
    (3, 'Carol', 'carol@example.com', 28)")?;
```

#### Method 2: API Batch Insert (High Performance)

```rust
use motedb::types::{Value, SqlRow};
use std::collections::HashMap;

// Batch insert (10-20x faster than SQL)
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

### 4. Query Data

```rust
// Simple query
let results = db.query("SELECT * FROM users WHERE age > 25")?;
println!("Found {} users", results.row_count());

// Iterate over results
for row_map in results.rows_as_maps() {
    if let Some(name) = row_map.get("name") {
        println!("Name: {:?}", name);
    }
}

// Aggregate query
let avg_result = db.query("SELECT AVG(age) as avg_age FROM users")?;
```

### 5. Update and Delete

```rust
// Update data
db.execute("UPDATE users SET age = 26 WHERE name = 'Alice'")?;

// Delete data
db.execute("DELETE FROM users WHERE age < 20")?;
```

## Creating Indexes

Indexes can significantly improve query performance.

### Column Index (Equality/Range Queries)

```rust
// Create a column index (40x performance improvement)
db.execute("CREATE INDEX users_email ON users(email)")?;

// Queries will automatically use the index
let results = db.query("SELECT * FROM users WHERE email = 'alice@example.com'")?;
```

### Vector Index (Similarity Search)

```rust
// Create a vector index
db.execute("CREATE VECTOR INDEX docs_embedding ON documents(embedding)")?;

// Vector KNN search
let query = "SELECT * FROM documents
             ORDER BY embedding <-> [0.1, 0.2, ..., 0.5]
             LIMIT 10";
let results = db.query(query)?;
```

### Full-Text Index (Text Search)

```rust
// Create a full-text index
db.execute("CREATE TEXT INDEX articles_content ON articles(content)")?;

// BM25 full-text search
let results = db.query(
    "SELECT * FROM articles WHERE MATCH(content, 'rust database')"
)?;
```

## Transaction Support

```rust
// Begin a transaction
let tx_id = db.begin_transaction()?;

db.execute("INSERT INTO users VALUES (100, 'Dave', 'dave@example.com', 35)")?;
db.execute("INSERT INTO users VALUES (101, 'Eve', 'eve@example.com', 32)")?;

// Commit the transaction
db.commit_transaction(tx_id)?;

// Or rollback
// db.rollback_transaction(tx_id)?;
```

### Savepoints (Transaction Checkpoints)

```rust
let tx_id = db.begin_transaction()?;

db.execute("INSERT INTO users VALUES (200, 'Frank', 'frank@example.com', 40)")?;
db.savepoint(tx_id, "sp1")?;

db.execute("INSERT INTO users VALUES (201, 'Grace', 'grace@example.com', 38)")?;
db.rollback_to_savepoint(tx_id, "sp1")?; // Only rolls back Grace

db.commit_transaction(tx_id)?; // Frank will be preserved
```

## Persistence

```rust
// Manually flush to disk
db.flush()?;

// Close the database (auto-flushes)
db.close()?;
```

## Complete Example

```rust
use motedb::{Database, Result};
use motedb::types::{Value, SqlRow};
use std::collections::HashMap;

fn main() -> Result<()> {
    // 1. Open the database
    let db = Database::open("demo.mote")?;

    // 2. Create a table
    db.execute("CREATE TABLE products (
        id INT,
        name TEXT,
        price FLOAT,
        category TEXT
    )")?;

    // 3. Batch insert data
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

    // 4. Create an index
    db.execute("CREATE INDEX products_category ON products(category)")?;

    // 5. Query data
    let results = db.query("SELECT * FROM products WHERE category = 'Electronics' AND price < 200")?;
    println!("Found {} products", results.row_count());

    // 6. Aggregate query
    let avg_result = db.query("SELECT category, AVG(price) as avg_price FROM products GROUP BY category")?;
    for row_map in avg_result.rows_as_maps() {
        println!("Category: {:?}, Avg Price: {:?}",
            row_map.get("category"),
            row_map.get("avg_price"));
    }

    // 7. Persist data
    db.flush()?;

    Ok(())
}
```

## Performance Tips

1. **Prefer batch operations**: Use `batch_insert_map()` instead of row-by-row inserts (10-20x faster)
2. **Use indexes wisely**: Create indexes on frequently queried columns (40x performance improvement)
3. **Use transactions**: Group multiple operations in a single transaction for better performance
4. **Flush periodically**: Ensure data persistence

## Next Steps

- [SQL Operations Guide](./03-sql-operations.md) - Learn the complete SQL syntax
- [Batch Operations Guide](./04-batch-operations.md) - High-performance batch insert techniques
- [Index System](./06-indexes-overview.md) - Deep dive into the five index types
- [API Reference](./14-api-reference.md) - Complete API documentation

## FAQ

**Q: Should I use SQL or the API?**
A: Prefer the SQL API, unless you need maximum-performance batch inserts, in which case use `batch_insert_map()`.

**Q: When should I create indexes?**
A: You can create indexes after data insertion, or create them before insertion (they will be incrementally updated automatically).

**Q: How do I ensure no data is lost?**
A: Use transactions combined with periodic `flush()` calls, or call `flush()` after critical operations.

---

**Previous**: [Installation & Configuration](./02-installation.md)
**Next**: [SQL Operations](./03-sql-operations.md)
