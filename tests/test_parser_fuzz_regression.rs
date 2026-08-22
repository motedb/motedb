//! Regressions for inputs found by CI fuzzing (fuzz_sql_parser).
//! Each test replays a real crash artifact; the parser must return an
//! ordinary syntax error — never a stack overflow / panic / abort.

use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn exec_err(db: &Database, sql: &str) -> String {
    match db.execute(sql) {
        Ok(_) => "ok".to_string(),
        Err(e) => format!("{:?}", e),
    }
}

#[test]
fn crash_d4ff16a9_unbounded_vector_literal_nesting() {
    // fuzz_sql_parser crash-d4ff16a9 (CI run 9467597131): 1572 raw '['
    // bytes. `[` starts a vector literal whose elements recurse through
    // parse_expr_list → parse_expr → parse_prefix_expr once per byte with
    // no depth bound → stack overflow under ASAN. Fixed by the LBracket
    // recursion guard in parse_prefix_expr.
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let sql = format!("SELECT {}", "[".repeat(1572));
    // Must be a graceful syntax error, not a crash.
    let err = exec_err(&db, &sql);
    assert!(
        err.contains("nesting") || err.contains("Syntax") || err.contains("Parse"),
        "got: {err}"
    );

    // Same class: unbounded CASE-nesting via the test expression.
    let sql = format!("SELECT {}", "CASE ".repeat(400));
    let err = exec_err(&db, &sql);
    assert!(
        err.contains("nesting") || err.contains("Syntax") || err.contains("Parse"),
        "got: {err}"
    );
}

#[test]
fn legitimate_nesting_still_parses() {
    // The guard must not break realistic nested expressions/vector literals.
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::default()).unwrap();
    db.execute("CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR(8))")
        .unwrap();
    // Nested vector literal inside an expression.
    let r = db.execute("INSERT INTO v VALUES (1, [1,0.5,-1,2,3,4,5,6])");
    assert!(r.is_ok(), "vector literal must parse: {:?}", r.err());
    let r = db.execute("SELECT id FROM v WHERE id = CASE WHEN 1 > 0 THEN 1 ELSE 2 END");
    assert!(r.is_ok(), "CASE must parse: {:?}", r.err());
}
