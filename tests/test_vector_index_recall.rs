//! Vector-index recall regression: incremental single-row inserts must keep
//! the DiskANN graph reachable. Found via a recall probe — after
//! row-at-a-time inserts, recall@10 collapsed to 3%: set_neighbors'
//! sort+truncate(max_degree) silently dropped every newcomer (highest id
//! sorts last) once neighbor lists saturated at ~max_degree nodes, so all
//! later nodes were unreachable from the medoid.
use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

struct Lcg(u64);
impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as f32) / (u32::MAX as f32)
    }
    fn vec(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.next_f32()).collect()
    }
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// Load (id, vector) ground truth from the table itself.
fn load_truth(db: &Database) -> Vec<(u64, Vec<f32>)> {
    let r = db
        .execute("SELECT id, emb FROM v")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Integer(i), Value::Vector(v)) => (*i as u64, v.to_vec()),
            other => panic!("bad row {:?}", other),
        })
        .collect()
}

fn ids(db: &Database, sql: &str) -> Vec<u64> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.iter()
        .map(|row| match &row[0] {
            Value::Integer(i) => *i as u64,
            other => panic!("bad id {:?}", other),
        })
        .collect()
}

/// Average recall@k of `ORDER BY emb <-> q LIMIT k` vs brute-force truth.
fn avg_recall(db: &Database, truth: &[(u64, Vec<f32>)], qs: &[Vec<f32>], k: usize) -> f32 {
    let mut sum = 0.0f32;
    for q in qs {
        let qs: Vec<String> = q.iter().map(|x| format!("{x:.4}")).collect();
        let got: std::collections::HashSet<u64> = ids(
            db,
            &format!(
                "SELECT id FROM v ORDER BY emb <-> [{}] LIMIT {k}",
                qs.join(",")
            ),
        )
        .into_iter()
        .collect();
        let mut ds: Vec<(u64, f32)> = truth.iter().map(|(id, v)| (*id, l2(q, v))).collect();
        ds.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
        ds.truncate(k);
        let hits = ds.iter().filter(|(id, _)| got.contains(id)).count();
        sum += hits as f32 / k as f32;
    }
    sum / qs.len() as f32
}

const DIM: usize = 8;

#[test]
fn incremental_inserts_keep_recall() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute(&format!(
        "CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR({DIM}))"
    ))
    .unwrap();
    db.execute("CREATE VECTOR INDEX v_emb ON v(emb)").unwrap();

    let mut rng = Lcg(0xBEEF);
    let n = 500i64;
    // Row-at-a-time inserts — the pattern that used to strand nodes.
    for i in 1..=n {
        let v: Vec<String> = rng.vec(DIM).iter().map(|x| format!("{x:.4}")).collect();
        db.execute(&format!(
            "INSERT INTO v (id, emb) VALUES ({i}, [{}])",
            v.join(",")
        ))
        .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();

    let truth = load_truth(&db);
    assert_eq!(truth.len(), n as usize);
    let qs: Vec<Vec<f32>> = (0..20).map(|_| rng.vec(DIM)).collect();
    let recall = avg_recall(&db, &truth, &qs, 10);
    assert!(
        recall >= 0.9,
        "recall@10 after incremental inserts = {recall:.3} (was 0.03 before the fix)"
    );

    // Self-queries: the nearest neighbor of v_i is i itself.
    let mut misses = 0;
    for &(id, ref v) in truth.iter().step_by(25) {
        let qs: Vec<String> = v.iter().map(|x| format!("{x:.4}")).collect();
        let got = ids(
            &db,
            &format!(
                "SELECT id FROM v ORDER BY emb <-> [{}] LIMIT 1",
                qs.join(",")
            ),
        );
        if got.first().copied() != Some(id) {
            misses += 1;
        }
    }
    assert_eq!(misses, 0, "self-queries must hit the row itself");
}

#[test]
fn recall_survives_update_delete_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
        db.execute(&format!(
            "CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR({DIM}))"
        ))
        .unwrap();
        db.execute("CREATE VECTOR INDEX v_emb ON v(emb)").unwrap();
        let mut rng = Lcg(0xCAFE);
        for i in 1..=300i64 {
            let v: Vec<String> = rng.vec(DIM).iter().map(|x| format!("{x:.4}")).collect();
            db.execute(&format!(
                "INSERT INTO v (id, emb) VALUES ({i}, [{}])",
                v.join(",")
            ))
            .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();

        // Move half the vectors far away, delete a quarter of the rest.
        for i in 1..=150i64 {
            db.execute(&format!(
                "UPDATE v SET emb = [{i}.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0] WHERE id = {i}"
            ))
            .unwrap();
        }
        for i in 151..=225i64 {
            db.execute(&format!("DELETE FROM v WHERE id = {i}"))
                .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
        db.close().unwrap();
    }

    let db = Database::open(dir.path()).unwrap();
    let truth = load_truth(&db);
    assert_eq!(truth.len(), 225, "300 - 75 deletes");

    let mut rng = Lcg(0x1234);
    let qs: Vec<Vec<f32>> = (0..20).map(|_| rng.vec(DIM)).collect();
    let recall = avg_recall(&db, &truth, &qs, 10);
    assert!(
        recall >= 0.85,
        "recall@10 after UPDATE/DELETE/reopen = {recall:.3}"
    );

    // Deleted rows must never come back.
    let ghost = ids(
        &db,
        "SELECT id FROM v ORDER BY emb <-> [0.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0] LIMIT 10",
    );
    for id in &ghost {
        assert!(!(151..=225).contains(id), "deleted id {id} resurrected");
    }
}

/// Exercises the insert-churn rebuild path (len ≥ 1000, churn ≥ len/2):
/// 2100 single-row inserts cross the first rebuild threshold; recall must
/// hold after it. Slow (~15s) — run with --ignored.
#[test]
#[ignore = "slow: ~15s of single-row inserts"]
fn recall_across_churn_rebuild() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute(&format!(
        "CREATE TABLE v (id INT PRIMARY KEY, emb VECTOR({DIM}))"
    ))
    .unwrap();
    db.execute("CREATE VECTOR INDEX v_emb ON v(emb)").unwrap();

    let mut rng = Lcg(0xD00D);
    for i in 1..=2100i64 {
        let v: Vec<String> = rng.vec(DIM).iter().map(|x| format!("{x:.4}")).collect();
        db.execute(&format!(
            "INSERT INTO v (id, emb) VALUES ({i}, [{}])",
            v.join(",")
        ))
        .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();

    let truth = load_truth(&db);
    assert_eq!(truth.len(), 2100);
    let qs: Vec<Vec<f32>> = (0..20).map(|_| rng.vec(DIM)).collect();
    let recall = avg_recall(&db, &truth, &qs, 10);
    assert!(
        recall >= 0.99,
        "recall@10 across churn rebuild = {recall:.3} (exact-scan backed)"
    );
}
