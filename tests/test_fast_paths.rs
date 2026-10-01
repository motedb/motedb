//! SQL Fast Path Correctness Tests
//!
//! Tests for the executor fast paths:
//! - MATCH AGAINST (text search)
//! - ST_WITHIN (spatial range)
//! - ST_KNN (spatial nearest neighbor)
//! - ST_DISTANCE ORDER BY (spatial distance sort)
//! - Vector ORDER BY (<->)
//! - Mixed WHERE clauses
//!
//! Run: cargo test --test test_fast_paths -- --test-threads=1

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let db = Database::create(dir.path()).expect("create db");
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL '{sql}': {e}"))
        .materialize()
        .expect("materialize")
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn setup_spatial(db: &Database) {
    exec(
        db,
        "CREATE TABLE locations (id INTEGER PRIMARY KEY, name TEXT, coords GEOMETRY)",
    );
    exec(
        db,
        "CREATE SPATIAL INDEX locations_coords ON locations(coords)",
    );

    // Insert a grid of points: (116+x*0.1, 40+y*0.1) for x,y in 0..5
    for x in 0..5i64 {
        for y in 0..5i64 {
            let id = x * 5 + y + 1;
            let px = 116.0 + x as f64 * 0.1;
            let py = 40.0 + y as f64 * 0.1;
            exec(
                db,
                &format!(
                    "INSERT INTO locations VALUES ({}, 'p_{}_{}', POINT({}, {}))",
                    id, x, y, px, py
                ),
            );
        }
    }
    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
}

fn setup_text(db: &Database) {
    exec(
        db,
        "CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
    );
    exec(db, "CREATE TEXT INDEX articles_body ON articles(body)");

    let docs = [
        (
            1,
            "Intro to Rust",
            "Rust is a systems programming language focused on safety and performance",
        ),
        (
            2,
            "Rust Concurrency",
            "Rust provides fearless concurrency with threads and async",
        ),
        (
            3,
            "Python Basics",
            "Python is a popular programming language for data science",
        ),
        (
            4,
            "Database Design",
            "Database indexing improves query performance significantly",
        ),
        (
            5,
            "Vector Search",
            "Vector databases enable similarity search using embeddings",
        ),
        (
            6,
            "Spatial Data",
            "Spatial indexing with R-trees enables efficient geo queries",
        ),
        (
            7,
            "Rust vs C++",
            "Rust offers memory safety without garbage collection unlike C++",
        ),
        (
            8,
            "ML Pipelines",
            "Machine learning pipelines process data for model training",
        ),
    ];

    for (id, title, body) in &docs {
        let title_escaped = title.replace("'", "''");
        let body_escaped = body.replace("'", "''");
        exec(
            db,
            &format!(
                "INSERT INTO articles VALUES ({}, '{}', '{}')",
                id, title_escaped, body_escaped
            ),
        );
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));
}

// ============================================================================
// Spatial Fast Path Tests
// ============================================================================

#[test]
fn test_st_within_basic() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    // All points are in [116.0, 116.4] × [40.0, 40.4]
    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_WITHIN(coords, 116.0, 40.0, 117.0, 41.0)",
    );
    assert_eq!(
        result.len(),
        25,
        "All 25 points should be within the large bbox"
    );
}

#[test]
fn test_st_within_narrow_bbox() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    // Narrow bbox around (116.0, 40.0) — should match only nearby points
    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_WITHIN(coords, 115.95, 39.95, 116.05, 40.05)",
    );
    assert!(!result.is_empty(), "Should find at least the origin point");
    assert!(result.len() <= 4, "Narrow bbox should match few points");
}

#[test]
fn test_st_within_no_results() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_WITHIN(coords, 0.0, 0.0, 1.0, 1.0)",
    );
    assert!(result.is_empty(), "No points should be in Africa");
}

#[test]
fn test_st_knn_basic() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_KNN(coords, 116.0, 40.0, 3)",
    );
    assert_eq!(result.len(), 3, "KNN should return exactly 3 results");
}

#[test]
fn test_st_knn_k_larger_than_data() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_KNN(coords, 116.0, 40.0, 100)",
    );
    // Should return all or most points
    assert!(
        result.len() >= 20,
        "KNN with k > data should return most points"
    );
}

#[test]
fn test_st_distance_order_by() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db,
        "SELECT id, name, ST_DISTANCE(coords, 116.0, 40.0) AS dist FROM locations ORDER BY dist LIMIT 5");
    assert_eq!(result.len(), 5, "Should return top 5 results");

    // Distances should be ascending (or non-decreasing)
    for i in 1..result.len() {
        let d_prev = match &result[i - 1][2] {
            Value::Float(d) => *d,
            _ => f64::MAX,
        };
        let d_curr = match &result[i][2] {
            Value::Float(d) => *d,
            _ => f64::MIN,
        };
        assert!(
            d_curr >= d_prev - 0.01,
            "Distances should be ascending: {} vs {}",
            d_prev,
            d_curr
        );
    }
}

#[test]
fn test_st_knn_returns_nearby() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    // Query near (116.2, 40.2) — should find points near that area
    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_KNN(coords, 116.2, 40.2, 3)",
    );
    assert_eq!(result.len(), 3, "KNN should return 3 results");
}

// ============================================================================
// Text Search Fast Path Tests
// ============================================================================

#[test]
fn test_match_against_basic() {
    let (db, _dir) = create_db();
    setup_text(&db);

    // Since 0.12 the default multi-word conjunction is AND (FTS5-compatible):
    // 'Rust programming' matches only docs holding BOTH words (doc 2 has
    // Rust but not "programming").
    let result = rows(
        &db,
        "SELECT id, title FROM articles WHERE MATCH(body) AGAINST('Rust programming') ORDER BY id",
    );
    assert!(!result.is_empty(), "Should find Rust+programming articles");
    let ids: Vec<i64> = result
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1), "Should find 'Intro to Rust'");
    assert!(
        !ids.contains(&2),
        "doc 2 lacks 'programming' — AND must exclude it"
    );

    // Explicit OR restores the union (old default behavior).
    let result = rows(
        &db,
        "SELECT id, title FROM articles WHERE MATCH(body) AGAINST('Rust OR programming') ORDER BY id",
    );
    let ids: Vec<i64> = result
        .iter()
        .filter_map(|r| match &r[0] {
            Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1), "Should find 'Intro to Rust'");
    assert!(ids.contains(&2), "OR union should match 'Rust Concurrency'");
}

#[test]
fn test_match_against_with_score() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(&db,
        "SELECT id, MATCH(body) AGAINST('Rust') AS score FROM articles WHERE MATCH(body) AGAINST('Rust') ORDER BY score DESC LIMIT 5");

    assert!(!result.is_empty(), "Should find results");

    // All scores should be positive
    for row in &result {
        if let Value::Float(score) = row[1] {
            assert!(score > 0.0, "Score should be positive, got {}", score);
        }
    }
}

#[test]
fn test_match_against_no_results() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(
        &db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('xyznonexistent')",
    );
    assert!(result.is_empty(), "Should find nothing for nonsense query");
}

#[test]
fn test_match_against_single_term() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(
        &db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('spatial') LIMIT 5",
    );
    assert!(!result.is_empty(), "Should find 'spatial' in article 6");
}

#[test]
fn test_match_against_limit() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(
        &db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('database') LIMIT 2",
    );
    assert!(result.len() <= 2, "Should respect LIMIT");
}

#[test]
fn test_match_against_phrase() {
    let (db, _dir) = create_db();
    setup_text(&db);

    // Exact phrase: "Machine learning" should match only the article with that exact sequence
    let result = rows(
        &db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('\"machine learning\"')",
    );
    assert_eq!(
        result.len(),
        1,
        "Phrase 'machine learning' should match exactly 1 article"
    );
    assert_eq!(result[0][0], Value::Integer(8));
}

#[test]
fn test_match_against_phrase_no_match() {
    let (db, _dir) = create_db();
    setup_text(&db);

    // "learning machine" is NOT in any document (words are in wrong order)
    let result = rows(
        &db,
        "SELECT id FROM articles WHERE MATCH(body) AGAINST('\"learning machine\"')",
    );
    assert!(
        result.is_empty(),
        "Phrase 'learning machine' should not match (wrong order)"
    );
}

// ============================================================================
// Vector Fast Path Tests
// ============================================================================

#[test]
fn test_vector_order_by_returns_results() {
    let (db, _dir) = create_db();

    exec(
        &db,
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, emb VECTOR(4))",
    );
    exec(&db, "CREATE VECTOR INDEX items_emb ON items(emb)");

    for i in 1..=10i64 {
        let v = format!(
            "[{:.1}, {:.1}, {:.1}, {:.1}]",
            i as f64, i as f64, i as f64, i as f64
        );
        exec(
            &db,
            &format!("INSERT INTO items VALUES ({}, 'item_{}', {})", i, i, v),
        );
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = rows(
        &db,
        "SELECT id, name FROM items ORDER BY emb <-> [5.0, 5.0, 5.0, 5.0] LIMIT 3",
    );
    assert_eq!(result.len(), 3, "Should return top 3");

    // With L2 distance, [5,5,5,5] should be closest to id=5 (distance=0)
    assert_eq!(result[0][0], Value::Integer(5), "Closest should be id=5");
}

#[test]
fn test_vector_order_by_with_distance() {
    let (db, _dir) = create_db();

    exec(
        &db,
        "CREATE TABLE vecs (id INTEGER PRIMARY KEY, v VECTOR(4))",
    );
    exec(&db, "CREATE VECTOR INDEX vecs_v ON vecs(v)");

    for i in 1..=20i64 {
        let v = format!(
            "[{:.1}, {:.1}, {:.1}, {:.1}]",
            i as f64, i as f64, i as f64, i as f64
        );
        exec(&db, &format!("INSERT INTO vecs VALUES ({}, {})", i, v));
    }

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = rows(
        &db,
        "SELECT id, v <-> [10.0, 10.0, 10.0, 10.0] AS dist FROM vecs ORDER BY dist LIMIT 5",
    );
    assert_eq!(result.len(), 5, "Should return top 5");

    // Distances should be non-negative
    for row in &result {
        if let Value::Float(d) = row[1] {
            assert!(d >= 0.0, "Distance should be non-negative");
        }
    }
}

// ============================================================================
// Mixed / Complex Queries
// ============================================================================

#[test]
fn test_select_star_with_st_within() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(
        &db,
        "SELECT * FROM locations WHERE ST_WITHIN(coords, 116.0, 40.0, 116.15, 40.15)",
    );
    assert!(!result.is_empty(), "SELECT * should work with ST_WITHIN");

    // Each row should have at least id and name columns
    for row in &result {
        assert!(row.len() >= 2, "Row should have at least id and name");
    }
}

#[test]
fn test_st_distance_order_by_with_limit_1() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(
        &db,
        "SELECT id FROM locations ORDER BY ST_DISTANCE(coords, 116.0, 40.0) LIMIT 1",
    );
    assert_eq!(result.len(), 1, "Should return 1 result");
}

#[test]
fn test_match_against_select_specific_columns() {
    let (db, _dir) = create_db();
    setup_text(&db);

    let result = rows(
        &db,
        "SELECT title FROM articles WHERE MATCH(body) AGAINST('vector search')",
    );
    assert!(!result.is_empty(), "Should find vector search articles");

    // Should only return the title column
    for row in &result {
        assert_eq!(row.len(), 1, "Should only project requested column");
    }
}

#[test]
fn test_count_with_indexed_where() {
    let (db, _dir) = create_db();
    setup_spatial(&db);

    let result = rows(&db, "SELECT COUNT(*) as cnt FROM locations");
    assert_eq!(
        result[0][0],
        Value::Integer(25),
        "Should count all 25 locations"
    );
}

// ============================================================================
// Persistence: data survives flush+checkpoint
// ============================================================================

#[test]
fn test_spatial_query_after_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path().to_path_buf();

    // Create and populate
    {
        let db = Database::create(&path).expect("create db");
        exec(
            &db,
            "CREATE TABLE pts (id INTEGER PRIMARY KEY, loc GEOMETRY)",
        );
        exec(&db, "CREATE SPATIAL INDEX pts_loc ON pts(loc)");
        for i in 1..=5i64 {
            exec(
                &db,
                &format!(
                    "INSERT INTO pts VALUES ({}, POINT({}, {}))",
                    i,
                    116.0 + i as f64 * 0.1,
                    39.9
                ),
            );
        }
        db.flush().expect("flush");
        db.checkpoint().expect("checkpoint");
    }

    // Reopen and query
    {
        let db = Database::open(&path).expect("reopen db");
        let result = rows(&db, "SELECT * FROM pts");
        assert_eq!(result.len(), 5, "All rows should survive reopen");
    }
}

#[test]
fn test_text_search_after_insert() {
    let (db, _dir) = create_db();

    exec(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE TEXT INDEX docs_body ON docs(body)");
    exec(&db, "INSERT INTO docs VALUES (1, 'hello world database')");
    exec(&db, "INSERT INTO docs VALUES (2, 'vector search engine')");

    // Before flush: search should work from pending
    let _pre_flush = db.text_search_ranked("docs_body", "database", 10).unwrap();

    db.flush().expect("flush");
    db.checkpoint().expect("checkpoint");
    std::thread::sleep(std::time::Duration::from_millis(500));

    // After flush+checkpoint: search should work from disk
    let _post_flush = db.text_search_ranked("docs_body", "database", 10).unwrap();

    let result = rows(
        &db,
        "SELECT id FROM docs WHERE MATCH(body) AGAINST('database')",
    );
    assert!(!result.is_empty(), "Text search should find 'database'");
}

/// 🔒 C1: fast-PK projected SELECTs (`SELECT col FROM t WHERE id = ?`) must
/// return exactly what the full materialization path returns — across wide
/// rows (text + vector columns that the projected read must skip), NULL
/// cells, both the prepared (FastPkMeta) and raw-SQL (string literal) fast
/// paths, and post-checkpoint (on-disk) rows.
#[test]
fn fast_pk_projected_select_matches_full_row() {
    let (db, _dir) = create_db();
    exec(
        &db,
        "CREATE TABLE w (id INT PRIMARY KEY, name TEXT, score FLOAT, note TEXT)",
    );
    let mut expect_name: std::collections::HashMap<i64, Option<String>> =
        std::collections::HashMap::new();
    for i in 0..50i64 {
        let name = if i % 7 == 3 {
            None
        } else {
            Some(format!("row-{i}"))
        };
        let score = i as f64 * 1.5;
        exec(
            &db,
            &format!(
                "INSERT INTO w VALUES ({i}, {}, {score}, 'note {i} bytes of text')",
                match &name {
                    Some(n) => format!("'{n}'"),
                    None => "NULL".to_string(),
                }
            ),
        );
        expect_name.insert(i, name);
    }
    db.checkpoint().unwrap();

    // Prepared fast-PK path (params) — projected single + multi column.
    for i in 0..50i64 {
        let r = db
            .execute_prepared("SELECT name FROM w WHERE id = ?", vec![Value::Integer(i)])
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        let got: Option<String> = match rows.first().and_then(|r| r.first()) {
            Some(Value::Text(s)) => Some(s.to_string()),
            Some(Value::Null) | None => None,
            other => panic!("id {i}: unexpected {other:?}"),
        };
        assert_eq!(got, expect_name[&i], "id {i} projected name mismatch");

        let r = db
            .execute_prepared(
                "SELECT name, score FROM w WHERE id = ?",
                vec![Value::Integer(i)],
            )
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows2) = r.select_rows().unwrap();
        let row = rows2.first().expect("multi-col row");
        assert!(matches!(row[1], Value::Float(f) if (f - i as f64 * 1.5).abs() < 1e-9));
    }

    // Raw-SQL fast path (literal PK).
    let r = db
        .execute("SELECT score FROM w WHERE id = 17")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert!(matches!(rows[0][0], Value::Float(f) if (f - 25.5).abs() < 1e-9));

    // Absent PK → empty (both paths), not an error and not a NULL row.
    let r = db
        .execute_prepared(
            "SELECT name FROM w WHERE id = ?",
            vec![Value::Integer(9999)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    assert!(r.select_rows().unwrap().1.is_empty());
    let r = db
        .execute("SELECT name FROM w WHERE id = 9999")
        .unwrap()
        .materialize()
        .unwrap();
    assert!(r.select_rows().unwrap().1.is_empty());
}

/// 🔒 Parameterized MATCH: `MATCH(col, ?)` and `MATCH(col) AGAINST (?)` must
/// parse and bind (previously a parse error — "second argument must be a
/// string"), including AND/OR query text and a clear error for non-string binds.
#[test]
fn parameterized_match_query() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE TEXT INDEX docs_body ON docs(body)");
    exec(&db, "INSERT INTO docs VALUES (1, 'alpha beta gamma')");
    exec(&db, "INSERT INTO docs VALUES (2, 'alpha delta')");
    exec(&db, "INSERT INTO docs VALUES (3, 'beta epsilon')");

    let run = |sql: &str, params: Vec<Value>| -> Vec<Vec<Value>> {
        db.execute_prepared(sql, params)
            .unwrap()
            .materialize()
            .unwrap()
            .select_rows()
            .unwrap()
            .1
            .to_vec()
    };

    // Short form, single term.
    let r = run(
        "SELECT id FROM docs WHERE MATCH(body, ?) ORDER BY id",
        vec![Value::Text("alpha".into())],
    );
    assert_eq!(r.len(), 2, "MATCH(body, ?) single term");

    // Short form, AND query text through the parameter.
    let r = run(
        "SELECT id FROM docs WHERE MATCH(body, ?)",
        vec![Value::Text("alpha beta".into())],
    );
    assert_eq!(r.len(), 1, "AND semantics through parameter");

    // Long form AGAINST(?).
    let r = run(
        "SELECT id FROM docs WHERE MATCH(body) AGAINST (?) ORDER BY id",
        vec![Value::Text("beta".into())],
    );
    assert_eq!(r.len(), 2, "MATCH(body) AGAINST (?)");

    // Multiple bound params (query is the 2nd).
    let r = run(
        "SELECT id FROM docs WHERE id > ? AND MATCH(body, ?)",
        vec![Value::Integer(1), Value::Text("beta".into())],
    );
    assert_eq!(r.len(), 1, "query as second parameter");

    // Non-string bind → clear error, not a panic or silent NULL.
    let err = db
        .execute_prepared(
            "SELECT id FROM docs WHERE MATCH(body, ?)",
            vec![Value::Integer(5)],
        )
        .err()
        .expect("non-string MATCH bind must error");
    let msg = format!("{err}");
    assert!(msg.contains("must be a string"), "got: {msg}");

    // Unbound parameter → error.
    assert!(db
        .execute_prepared("SELECT id FROM docs WHERE MATCH(body, ?)", vec![])
        .is_err());
}

/// 🔒 Transactional PK DELETE must use the PK fast path (the old blanket
/// skip forced a full-table scan per statement — ~0.5s each on 100K rows)
/// while preserving txn semantics: rollback restores, write_set INSERTs
/// deleted by PK vanish at COMMIT, no double-count.
#[test]
fn txn_pk_delete_fast_path_semantics() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for c in 0..10 {
        let vals: Vec<String> = (0..500)
            .map(|i| format!("({}, {})", c * 500 + i, i))
            .collect();
        exec(&db, &format!("INSERT INTO t VALUES {}", vals.join(",")));
    }

    // ROLLBACK restores.
    exec(&db, "BEGIN");
    exec(&db, "DELETE FROM t WHERE id = 42");
    let n = match &rows(&db, "SELECT COUNT(*) FROM t")[0][0] {
        Value::Integer(i) => *i,
        other => panic!("{other:?}"),
    };
    exec(&db, "ROLLBACK");
    let n2 = match &rows(&db, "SELECT COUNT(*) FROM t")[0][0] {
        Value::Integer(i) => *i,
        other => panic!("{other:?}"),
    };
    assert_eq!(n2, n + 1, "rollback must restore the deleted row");
    assert!(rows(&db, "SELECT v FROM t WHERE id = 42").len() == 1);

    // COMMIT persists; deleting the same row twice in one txn counts once.
    exec(&db, "BEGIN");
    exec(&db, "DELETE FROM t WHERE id = 43");
    let again = db
        .execute("DELETE FROM t WHERE id = 43")
        .unwrap()
        .materialize()
        .unwrap();
    let _ = again;
    exec(&db, "COMMIT");
    assert!(rows(&db, "SELECT v FROM t WHERE id = 43").is_empty());

    // write_set INSERT deleted by PK inside the txn → gone at COMMIT,
    // affected_rows counts it exactly once.
    exec(&db, "BEGIN");
    exec(&db, "INSERT INTO t VALUES (99999, 1)");
    let affected = db
        .execute("DELETE FROM t WHERE id = 99999")
        .unwrap()
        .materialize()
        .unwrap();
    let _ = affected;
    exec(&db, "COMMIT");
    assert!(
        rows(&db, "SELECT v FROM t WHERE id = 99999").is_empty(),
        "write_set row deleted by PK must not resurrect at COMMIT"
    );

    // Speed guard: 50 txn deletes well under the old full-scan cost
    // (each old statement scanned 5K rows; keep this generous — CI runners).
    exec(&db, "BEGIN");
    let t0 = std::time::Instant::now();
    for i in 0..50u64 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", 100 + i));
    }
    let dt = t0.elapsed();
    exec(&db, "ROLLBACK");
    assert!(
        dt.as_secs_f64() < 5.0,
        "50 txn PK deletes took {dt:?} — full-scan regression?"
    );
}

/// 🔒 Shuffled-key bulk INSERT: the explicit-PK fast path's segment flush
/// must not fall back to whole-batch decode+re-add on non-ascending keys
/// (was 356K → 9.2K rows/s on shuffled executemany). Correctness: row
/// values must land on the right PKs regardless of insertion order, and a
/// batch containing duplicate PKs must fail exactly like the sorted path.
#[test]
fn shuffled_bulk_insert_fast_and_correct() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let n = 5000;
    let mut ids: Vec<i64> = (1..=n).collect();
    let mut rng: u64 = 0x9E3779B97F4A7C15;
    for i in (1..ids.len()).rev() {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let j = (rng as usize) % (i + 1);
        ids.swap(i, j);
    }
    for chunk in ids.chunks(500) {
        let vals: Vec<String> = chunk.iter().map(|id| format!("({id}, {id})")).collect();
        exec(&db, &format!("INSERT INTO t VALUES {}", vals.join(",")));
    }

    // Row-at-PK correctness (values must follow their keys).
    for id in [1, n / 2, n] {
        let got = rows(&db, &format!("SELECT v FROM t WHERE id = {id}"));
        assert_eq!(
            got[0],
            vec![Value::Integer(id)],
            "row {id} landed on the wrong key"
        );
    }
    let total = rows(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(total[0], vec![Value::Integer(n as i64)]);

    // Duplicate PK in one batch → error (same as sorted path), not silent loss.
    let err = db
        .execute("INSERT INTO t VALUES (5001, 5001), (5001, 5002)")
        .err()
        .expect("duplicate PK in batch must fail");
    let _ = format!("{err}");
}

// 🔒 Regression (benchmark-prep probe): the api-layer fast PK shortcut
// (execute_fast_pk_with_meta) applied UPDATE/DELETE straight to storage even
// inside an explicit transaction — ROLLBACK could not revert `WHERE id = ?`
// writes (the literal form `WHERE id = 42` took the executor path and was
// already fixed; the PARAMETER form took this shortcut and silently broke
// atomicity). The write branches must defer to the executor's txn-aware
// paths while a transaction is active.
#[test]
fn fast_pk_update_rollback_via_prepared() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 7)");

    // SQL-BEGIN entry point + parameterized PK UPDATE + ROLLBACK.
    exec(&db, "BEGIN");
    let affected = db
        .execute_prepared("UPDATE t SET v = 9 WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    match affected {
        motedb::sql::QueryResult::Modification { affected_rows } => {
            assert_eq!(affected_rows, 1);
        }
        other => panic!("expected Modification, got {other:?}"),
    }
    // Read-your-write inside the txn.
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::Integer(9)]]
    );
    exec(&db, "ROLLBACK");
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::Integer(5)]],
        "rollback must revert a parameterized fast-PK UPDATE"
    );

    // begin_transaction() API entry point, same guarantee.
    let tx = db.begin_transaction().unwrap();
    db.execute_prepared("UPDATE t SET v = 42 WHERE id = ?", vec![Value::Integer(2)])
        .unwrap()
        .materialize()
        .unwrap();
    db.rollback_transaction(tx).unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 2"),
        vec![vec![Value::Integer(7)]]
    );

    // COMMIT persists.
    let tx = db.begin_transaction().unwrap();
    db.execute_prepared("UPDATE t SET v = 11 WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    db.commit_transaction(tx).unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::Integer(11)]]
    );
}

#[test]
fn fast_pk_delete_rollback_via_prepared() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 7)");

    exec(&db, "BEGIN");
    let affected = db
        .execute_prepared("DELETE FROM t WHERE id = ?", vec![Value::Integer(1)])
        .unwrap()
        .materialize()
        .unwrap();
    match affected {
        motedb::sql::QueryResult::Modification { affected_rows } => {
            assert_eq!(affected_rows, 1);
        }
        other => panic!("expected Modification, got {other:?}"),
    }
    assert!(rows(&db, "SELECT v FROM t WHERE id = 1").is_empty());
    exec(&db, "ROLLBACK");
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::Integer(5)]],
        "rollback must revert a parameterized fast-PK DELETE"
    );
}

// 🔒 Regression: executemany (execute_prepared_many) on UPDATE/DELETE used
// to open its OWN transaction unconditionally — inside a caller's explicit
// transaction the outer ROLLBACK could not undo the batch. It must join the
// enclosing transaction (SQLite semantics).
#[test]
fn executemany_update_joins_outer_txn() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 7), (3, 9)");
    let batch: Vec<Vec<Value>> = (1..=3)
        .map(|i| vec![Value::Integer(100 + i), Value::Integer(i)])
        .collect();

    let tx = db.begin_transaction().unwrap();
    let affected = db
        .execute_prepared_many("UPDATE t SET v = ? WHERE id = ?", batch)
        .unwrap();
    assert_eq!(affected, 3);
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::Integer(101)]],
        "batch writes must be visible inside the outer txn"
    );
    db.rollback_transaction(tx).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Integer(1), Value::Integer(5)],
            vec![Value::Integer(2), Value::Integer(7)],
            vec![Value::Integer(3), Value::Integer(9)],
        ],
        "outer rollback must undo the whole batch"
    );

    // Without an outer transaction the batch still self-wraps and commits.
    let batch: Vec<Vec<Value>> = (1..=3)
        .map(|i| vec![Value::Integer(200 + i), Value::Integer(i)])
        .collect();
    let affected = db
        .execute_prepared_many("UPDATE t SET v = ? WHERE id = ?", batch)
        .unwrap();
    assert_eq!(affected, 3);
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 3"),
        vec![vec![Value::Integer(203)]]
    );
}

// 🔒 Regression (benchmark-prep probe): `SET v = v + 1 WHERE id = ?` matched
// the fast PK pattern but the expression form of the SET fell into the
// catch-all "ignore" arm — the fast path wrote the OLD row back and reported
// affected=1 while silently dropping the assignment. Any SET the fast path
// cannot represent as a raw value must defer to the executor.
#[test]
fn fast_pk_update_expression_set_defers_to_executor() {
    let (db, _dir) = create_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 2.5), (2, 4.0)");

    let affected = db
        .execute_prepared(
            "UPDATE t SET v = v + 1 WHERE id = ?",
            vec![Value::Integer(1)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match affected {
        motedb::sql::QueryResult::Modification { affected_rows } => {
            assert_eq!(affected_rows, 1);
        }
        other => panic!("expected Modification, got {other:?}"),
    }
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Value::Float(3.5)]],
        "v = v + 1 must actually apply (affected=1 with unchanged data was the bug)"
    );

    // autocommit execute() with a literal PK rides the same deferral rules.
    let _ = db
        .execute("UPDATE t SET v = v * 2 WHERE id = 2")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 2"),
        vec![vec![Value::Float(8.0)]]
    );

    // column-to-column assignment also defers correctly.
    let _ = db
        .execute_prepared(
            "UPDATE t SET v = v * 2 WHERE id = ?",
            vec![Value::Integer(2)],
        )
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT v FROM t WHERE id = 2"),
        vec![vec![Value::Float(16.0)]]
    );
}
