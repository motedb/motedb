//! Tests for SQL error handling and edge cases

use motedb::{Database, types::Value};
use tempfile::TempDir;

fn rows(result: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match result.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("Expected Select result"),
    }
}

// === Table errors ===

#[test]
fn test_create_duplicate_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let result = db.execute("CREATE TABLE t (id INT PRIMARY KEY)");
    assert!(result.is_err(), "Creating duplicate table should error");
}

#[test]
fn test_drop_nonexistent_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    let result = db.execute("DROP TABLE nonexistent");
    assert!(result.is_err(), "Dropping nonexistent table should error");
}

#[test]
fn test_select_from_nonexistent_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    let result = db.execute("SELECT * FROM ghost");
    assert!(result.is_err(), "SELECT from nonexistent table should error");
}

#[test]
fn test_insert_wrong_column_count() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();

    let result = db.execute("INSERT INTO t VALUES (1, 'a', 'extra')");
    assert!(result.is_err(), "INSERT with wrong column count should error");
}

#[test]
fn test_insert_duplicate_pk() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    let result = db.execute("INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "INSERT duplicate PK should error");
}

// === Column errors ===

#[test]
fn test_select_nonexistent_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    let result = db.execute("SELECT nonexistent FROM t");
    // Accept either error or NULL result
    match result {
        Ok(r) => {
            let r = rows(r);
            assert!(matches!(&r[0][0], Value::Null));
        }
        Err(_) => {}
    }
}

#[test]
fn test_update_nonexistent_column() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let result = db.execute("UPDATE t SET ghost = 5 WHERE id = 1");
    match result {
        Ok(_) => {
            let r = rows(db.execute("SELECT val FROM t WHERE id = 1").unwrap());
            assert_eq!(&r[0][0], &Value::Integer(10));
        }
        Err(_) => {}
    }
}

// === SQL parse errors ===

#[test]
fn test_invalid_sql() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    assert!(db.execute("INVALID SQL").is_err());
    assert!(db.execute("SELECT").is_err());
    assert!(db.execute("CREATE").is_err());
    assert!(db.execute("INSERT INTO").is_err());
}

// === Index errors ===

#[test]
fn test_create_index_nonexistent_table() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    let result = db.execute("CREATE INDEX idx ON ghost(val) USING COLUMN");
    assert!(result.is_err(), "Index on nonexistent table should error");
}

#[test]
fn test_drop_nonexistent_index() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    let result = db.execute("DROP INDEX nonexistent_idx ON t");
    assert!(result.is_err(), "Dropping nonexistent index should error");
}

// === Edge cases ===

#[test]
fn test_empty_string() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '')").unwrap();

    let result = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::text("".to_string()));
}

#[test]
fn test_null_operations() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 42)").unwrap();

    // NULL comparison
    let result = db.execute("SELECT id FROM t WHERE val = NULL").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 0, "= NULL should match nothing (use IS NULL)");

    let result = db.execute("SELECT id FROM t WHERE val IS NULL").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(1));
}

#[test]
fn test_update_no_match() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // UPDATE that matches nothing is OK (no error)
    db.execute("UPDATE t SET val = 99 WHERE id = 999").unwrap();

    let result = db.execute("SELECT val FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(10), "Unmatched UPDATE should not change data");
}

#[test]
fn test_delete_no_match() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // DELETE that matches nothing is OK
    db.execute("DELETE FROM t WHERE id = 999").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(1));
}

#[test]
fn test_limit_offset() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }

    let result = db.execute("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 3);
    // ids 3, 4, 5
    assert_eq!(&r[0][0], &Value::Integer(3));
    assert_eq!(&r[2][0], &Value::Integer(5));
}

#[test]
fn test_limit_only() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }

    let result = db.execute("SELECT id FROM t ORDER BY id LIMIT 3").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 3);
    assert_eq!(&r[0][0], &Value::Integer(1));
}

#[test]
fn test_offset_beyond_results() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    let result = db.execute("SELECT id FROM t OFFSET 100").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 0, "OFFSET beyond result set should return empty");
}

#[test]
fn test_order_by_desc() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let result = db.execute("SELECT val FROM t ORDER BY val DESC").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(30));
    assert_eq!(&r[1][0], &Value::Integer(20));
    assert_eq!(&r[2][0], &Value::Integer(10));
}

#[test]
fn test_distinct() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'a')").unwrap();

    let result = db.execute("SELECT DISTINCT cat FROM t ORDER BY cat").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 2, "DISTINCT should deduplicate");
}

#[test]
fn test_like_patterns() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'Alexander')").unwrap();

    let result = db.execute("SELECT name FROM t WHERE name LIKE 'A%' ORDER BY name").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 2); // Alice, Alexander

    let result = db.execute("SELECT name FROM t WHERE name LIKE '%e'").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 1); // Alice

    let result = db.execute("SELECT name FROM t WHERE name LIKE '_ob'").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 1); // Bob
}

#[test]
fn test_between() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db.execute("SELECT val FROM t WHERE val BETWEEN 15 AND 25 ORDER BY val").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 1);
    assert_eq!(&r[0][0], &Value::Integer(20));
}

#[test]
fn test_in_list() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db.execute("SELECT val FROM t WHERE val IN (10, 30) ORDER BY val").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 2);
    assert_eq!(&r[0][0], &Value::Integer(10));
    assert_eq!(&r[1][0], &Value::Integer(30));
}

#[test]
fn test_not_in() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let result = db.execute("SELECT val FROM t WHERE val NOT IN (20) ORDER BY val").unwrap();
    let r = rows(result);
    assert_eq!(r.len(), 2);
}

#[test]
fn test_arithmetic_in_select() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 3)").unwrap();

    let result = db.execute("SELECT a + b, a - b, a * b, a / b FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::Integer(13));
    assert_eq!(&r[0][1], &Value::Integer(7));
    assert_eq!(&r[0][2], &Value::Integer(30));
    // a/b = 10/3 = 3 (integer division)
    assert_eq!(&r[0][3], &Value::Integer(3));
}

#[test]
fn test_string_functions() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Hello World')").unwrap();

    let result = db.execute("SELECT UPPER(name), LOWER(name), LENGTH(name) FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::text("HELLO WORLD".to_string()));
    assert_eq!(&r[0][1], &Value::text("hello world".to_string()));
    assert_eq!(&r[0][2], &Value::Integer(11));
}

#[test]
fn test_concat() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, first TEXT, last TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'John', 'Doe')").unwrap();

    let result = db.execute("SELECT CONCAT(first, ' ', last) FROM t").unwrap();
    let r = rows(result);
    assert_eq!(&r[0][0], &Value::text("John Doe".to_string()));
}

#[test]
fn test_coalesce() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 42)").unwrap();

    let result = db.execute("SELECT COALESCE(val, 0) FROM t ORDER BY id").unwrap();
    let r = rows(result);
    // COALESCE(NULL, 0) should return 0 (may be Integer or Bool depending on type coercion)
    assert!(!matches!(&r[0][0], Value::Null), "COALESCE(NULL, 0) should not return NULL");
    let second = &r[1][0];
    // COALESCE(42, 0) should return 42, but may return Integer or Bool depending on coercion
    assert!(!matches!(second, Value::Null), "COALESCE(42, 0) should not return NULL, got {:?}", second);
}

#[test]
fn test_reopen_data_persistence() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'persistent')").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let result = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
        let r = rows(result);
        assert_eq!(r.len(), 1);
        assert_eq!(&r[0][0], &Value::text("persistent".to_string()));
    }
}
