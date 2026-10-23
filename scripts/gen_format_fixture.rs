//! Format-migration fixture generator (§11).
//!
//! Compiled and run inside a git worktree of EACH historical release tag
//! (scripts/gen_format_fixtures.sh orchestrates); writes a small canonical
//! database with the version's own code so tests/fixtures/formats/vX.Y.Z/
//! captures that release's真实 on-disk layout.
//!
//! Canonical content (kept to types stable across v0.12+):
//!   table mig:    50 rows (id INT PK, f FLOAT, s TEXT, ts TIMESTAMP)
//!   table mig_vec: 20 rows (id INT PK, v VECTOR(3)) — best effort; older
//!                  versions with different vector SQL just skip it (the
//!                  matrix test tolerates its absence).
//! Usage: gen_format_fixture <out_dir>   (writes <out_dir>/db/)

use motedb::Database;
use std::path::Path;

fn main() {
    let out = std::env::args().nth(1).expect("usage: gen_format_fixture <out_dir>");
    let db_path = Path::new(&out).join("db");
    let _ = std::fs::remove_dir_all(&db_path);
    let db = Database::create(&db_path).expect("create");

    db.execute("CREATE TABLE mig (id INT PRIMARY KEY, f FLOAT, s TEXT, ts TIMESTAMP)")
        .expect("create mig");
    for id in 1..=50i64 {
        db.execute(&format!(
            "INSERT INTO mig (id, f, s, ts) VALUES ({id}, {}, 'row{id}', {})",
            id as f64 * 0.5,
            1_700_000_000_000_000i64 + id * 1_000_000
        ))
        .expect("insert mig");
    }

    // Vector table best effort: some historical versions lack vector SQL
    // literals or VECTOR(n) DDL; the matrix only checks it when present.
    let vec_ok = db
        .execute("CREATE TABLE mig_vec (id INT PRIMARY KEY, v VECTOR(3))")
        .is_ok();
    if vec_ok {
        for id in 1..=20i64 {
            let r = db.execute(&format!(
                "INSERT INTO mig_vec (id, v) VALUES ({id}, [0.1, 0.2, 0.3])"
            ));
            if r.is_err() {
                eprintln!("SKIP_VEC_ROWS: insert failed at id={id}");
                break;
            }
        }
    } else {
        eprintln!("SKIP_VEC_TABLE: DDL failed");
    }

    db.execute("CHECKPOINT").expect("checkpoint");
    drop(db);
    println!("OK {}", db_path.display());
}
