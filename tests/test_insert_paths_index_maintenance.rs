//! Every INSERT path must leave the secondary indexes consistent with the
//! table. Found while benchmarking: multi-row INSERTs into tables with a
//! VECTOR column were forced through the per-row insert (one group-commit
//! fsync each — ~260 rows/s under the default preset, 770× slower than a
//! table without a vector column), because
//!   * the batch path's AUTO_INCREMENT fast lane skipped vector/text/spatial
//!     index maintenance, and
//!   * transactional inserts were only indexed *before* COMMIT (so ROLLBACK
//!     left ghost vectors) and single-row transactional inserts not at all.
//!
//! Index membership is checked through `Database::vector_search` directly,
//! since the SQL layer answers small tables with an exact scan.
use motedb::{DBConfig, Database};
use tempfile::TempDir;

const DIM: usize = 16;

struct Lcg(u64);
impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as f32) / (u32::MAX as f32)
    }
    fn vec(&mut self) -> Vec<f32> {
        // 4 decimals — exactly what `lit` writes into the SQL text, so read-back
        // comparisons are exact.
        (0..DIM)
            .map(|_| (self.next_f32() * 10_000.0).round() / 10_000.0)
            .collect()
    }
}

fn lit(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:.4}")).collect();
    format!("[{}]", parts.join(","))
}

/// Top-1 row id from the vector index for `q`.
fn top1(db: &Database, index: &str, q: &[f32]) -> Option<u64> {
    db.vector_search(index, q, 1)
        .unwrap()
        .first()
        .map(|(rid, _)| *rid)
}

fn count(db: &Database, table: &str) -> i64 {
    let r = db
        .execute(&format!("SELECT COUNT(*) FROM {table}"))
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    match &rows[0][0] {
        motedb::types::Value::Integer(n) => *n,
        other => panic!("bad count {other:?}"),
    }
}

#[test]
fn multi_row_autocommit_insert_reaches_vector_index() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute(&format!(
        "CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR({DIM}))"
    ))
    .unwrap();
    db.execute("CREATE VECTOR INDEX v_emb ON v(emb)").unwrap();

    let mut rng = Lcg(0xA11CE);
    let n = 300usize;
    let vecs: Vec<Vec<f32>> = (0..n).map(|_| rng.vec()).collect();
    // One multi-row statement — the batch path.
    let values: Vec<String> = vecs
        .iter()
        .enumerate()
        .map(|(i, v)| format!("({}, {})", i + 1, lit(v)))
        .collect();
    db.execute(&format!(
        "INSERT INTO v (id, emb) VALUES {}",
        values.join(",")
    ))
    .unwrap();
    assert_eq!(count(&db, "v"), n as i64);

    // Integer PK tables use the PK as row id, so the index must hand back i+1.
    for i in (0..n).step_by(7) {
        assert_eq!(
            top1(&db, "v_emb", &vecs[i]),
            Some(i as u64 + 1),
            "row {} missing from the vector index after multi-row INSERT",
            i + 1
        );
    }
}

#[test]
fn auto_increment_bulk_insert_reaches_vector_and_text_index() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute(&format!(
        "CREATE TABLE d (id INT PRIMARY KEY AUTO_INCREMENT, content TEXT, emb VECTOR({DIM}))"
    ))
    .unwrap();
    db.execute("CREATE VECTOR INDEX d_emb ON d(emb)").unwrap();
    db.execute("CREATE TEXT INDEX d_content ON d(content)")
        .unwrap();

    // ≥ 100 rows on an AUTO_INCREMENT table takes the fast batch lane.
    let syl = ["ba", "ke", "lu", "mo", "ri", "sa", "te", "vo", "wi", "zu"];
    let word = |i: usize| {
        format!(
            "{}{}{}",
            syl[i % 10],
            syl[(i / 10) % 10],
            syl[(i / 100) % 10]
        )
    };
    let mut rng = Lcg(0xB0B);
    let n = 250usize;
    let vecs: Vec<Vec<f32>> = (0..n).map(|_| rng.vec()).collect();
    let values: Vec<String> = vecs
        .iter()
        .enumerate()
        .map(|(i, v)| format!("('note {} about {}', {})", i, word(i), lit(v)))
        .collect();
    db.execute(&format!(
        "INSERT INTO d (content, emb) VALUES {}",
        values.join(",")
    ))
    .unwrap();
    assert_eq!(count(&db, "d"), n as i64);
    db.wait_for_indexes_ready();

    // Vector index: every sampled row's own vector must come back as top-1,
    // and the row id it names must hold that vector.
    for i in (0..n).step_by(9) {
        let rid = top1(&db, "d_emb", &vecs[i])
            .unwrap_or_else(|| panic!("empty vector_search result for row {i}"));
        let r = db
            .execute(&format!("SELECT emb FROM d WHERE id = {rid}"))
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "row id {rid} from the index is not in the table"
        );
        match &rows[0][0] {
            motedb::types::Value::Vector(v) => assert_eq!(
                v.as_slice(),
                &vecs[i][..],
                "row {i}: index top-1 (row id {rid}) holds a different vector"
            ),
            other => panic!("bad emb {other:?}"),
        }
    }

    // Text index: a bulk-inserted row's unique word must be searchable.
    for i in [3usize, 117, 249] {
        let hits = db.text_search_ranked("d_content", &word(i), 5).unwrap();
        assert!(
            !hits.is_empty(),
            "text index has no entry for bulk-inserted row {i} ({})",
            word(i)
        );
    }
}

#[test]
fn transactional_insert_indexed_at_commit_not_before() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute(&format!(
        "CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR({DIM}))"
    ))
    .unwrap();
    db.execute("CREATE VECTOR INDEX v_emb ON v(emb)").unwrap();

    let mut rng = Lcg(0xC0FFEE);
    let vecs: Vec<Vec<f32>> = (0..6).map(|_| rng.vec()).collect();

    // Single-row + multi-row inside one transaction, committed.
    db.execute("BEGIN").unwrap();
    db.execute(&format!(
        "INSERT INTO v (id, emb) VALUES (1, {})",
        lit(&vecs[0])
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO v (id, emb) VALUES (2, {}), (3, {})",
        lit(&vecs[1]),
        lit(&vecs[2])
    ))
    .unwrap();
    db.execute("COMMIT").unwrap();
    for id in 1..=3u64 {
        assert_eq!(
            top1(&db, "v_emb", &vecs[id as usize - 1]),
            Some(id),
            "committed row {id} missing from the vector index"
        );
    }

    // Same shapes, rolled back: nothing may reach the index.
    db.execute("BEGIN").unwrap();
    db.execute(&format!(
        "INSERT INTO v (id, emb) VALUES (4, {})",
        lit(&vecs[3])
    ))
    .unwrap();
    db.execute(&format!(
        "INSERT INTO v (id, emb) VALUES (5, {}), (6, {})",
        lit(&vecs[4]),
        lit(&vecs[5])
    ))
    .unwrap();
    db.execute("ROLLBACK").unwrap();
    assert_eq!(count(&db, "v"), 3);
    for id in 4..=6u64 {
        let got = db
            .vector_search("v_emb", &vecs[id as usize - 1], 3)
            .unwrap();
        assert!(
            got.iter().all(|(rid, _)| *rid != id),
            "rolled-back row {id} is a ghost in the vector index: {got:?}"
        );
    }
}
