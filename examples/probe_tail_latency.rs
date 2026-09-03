//! Tail-latency stability probe: record EVERY query's latency over two
//! 30-second phases (read-only steady state, then mixed read+write) and
//! report percentiles + worst outliers per query shape.
use motedb::{DBConfig, Database};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Lats {
    name: &'static str,
    v: Vec<(u64, usize)>, // (us, seq)
}
impl Lats {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            v: Vec::new(),
        }
    }
    fn push(&mut self, us: u64, seq: usize) {
        self.v.push((us, seq));
    }
    fn report(&self, phase: &str) {
        let mut sorted: Vec<u64> = self.v.iter().map(|x| x.0).collect();
        sorted.sort_unstable();
        if sorted.is_empty() {
            return;
        }
        let p = |q: f64| sorted[((sorted.len() as f64 - 1.0) * q) as usize];
        let med = p(0.50);
        let mut worst: Vec<_> = self.v.clone();
        worst.sort_unstable_by_key(|x| std::cmp::Reverse(x.0));
        let n_over = sorted.iter().filter(|&&x| x > med * 10 && x > 1000).count();
        println!(
            "  {:>10} [{}] n={:>6} P50={:>7.1}µs P95={:>8.1}µs P99={:>8.1}µs P99.9={:>9.1}µs max={:>9.1}µs  >10×median: {}",
            self.name, phase, sorted.len(), p(0.50) as f64, p(0.95) as f64, p(0.99) as f64, p(0.999) as f64, sorted[sorted.len()-1] as f64, n_over
        );
        for (us, seq) in worst.iter().take(3) {
            println!(
                "      worst: {:>9.1}ms at query #{}",
                *us as f64 / 1000.0,
                seq
            );
        }
    }
}

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_general(); // desktop defaults, auto_checkpoint ON
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, code TEXT, ts BIGINT)").unwrap();
    db.execute("CREATE INDEX idx_code ON t (code) USING COLUMN")
        .unwrap();

    let mut total = 0usize;
    while total < 1_000_000 {
        let end = (total + 5000).min(1_000_000);
        let mut sql = String::with_capacity(5000 * 70);
        sql.push_str("INSERT INTO t (name, val, code, ts) VALUES ");
        for i in total..end {
            sql.push_str(&format!(
                "('user_{}', {:.2}, 'CD_{:04}', {})",
                i % 1000,
                (i as f64) * 0.5,
                i % 1000,
                1_700_000_000_i64 + i as i64
            ));
            if i + 1 < end {
                sql.push(',');
            }
        }
        db.execute(&sql).unwrap();
        total = end;
    }
    println!("inserted 1M rows\n");

    let mut rng: u64 = 0x9E3779B97F4A7C15;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let total = total as u64;

    for phase in ["read-only", "read+write"] {
        let mut point = Lats::new("point");
        let mut idxeq = Lats::new("idx_eq");
        let mut idxlim = Lats::new("idx_lim");
        let mut filter = Lats::new("filter");
        let mut group = Lats::new("group");
        let mut insrt = Lats::new("insert");
        let t_end = Instant::now() + Duration::from_secs(30);
        let mut seq = 0usize;
        let mut ins_id = total;
        while Instant::now() < t_end {
            seq += 1;
            let id = next() % total + 1;
            let t = Instant::now();
            let _ = db.execute(&format!(
                "SELECT id, name, val, code, ts FROM t WHERE id = {id}"
            ));
            point.push(t.elapsed().as_micros() as u64, seq);

            let cd = next() % 1000;
            let t = Instant::now();
            let _ = db.execute(&format!(
                "SELECT id, name, val, code, ts FROM t WHERE code = 'CD_{cd:04}'"
            ));
            idxeq.push(t.elapsed().as_micros() as u64, seq);

            let t = Instant::now();
            let _ = db.execute(&format!(
                "SELECT id, name FROM t WHERE code = 'CD_{cd:04}' LIMIT 100"
            ));
            idxlim.push(t.elapsed().as_micros() as u64, seq);

            let th = next() % 1000;
            let t = Instant::now();
            let _ = db.execute(&format!("SELECT COUNT(*) FROM t WHERE val > {th}.0"));
            filter.push(t.elapsed().as_micros() as u64, seq);

            let t = Instant::now();
            let _ = db.execute("SELECT code, COUNT(*), AVG(val) FROM t GROUP BY code");
            group.push(t.elapsed().as_micros() as u64, seq);

            if phase == "read+write" {
                let b = ins_id;
                ins_id += 500;
                let mut sql = String::with_capacity(500 * 70);
                sql.push_str("INSERT INTO t (name, val, code, ts) VALUES ");
                for i in b..b + 500 {
                    sql.push_str(&format!(
                        "('user_{}', {:.2}, 'CD_{:04}', {})",
                        i % 1000,
                        (i as f64) * 0.5,
                        i % 1000,
                        1_700_000_000_i64 + i as i64
                    ));
                    if i + 1 < b + 500 {
                        sql.push(',');
                    }
                }
                let t = Instant::now();
                let _ = db.execute(&sql);
                insrt.push(t.elapsed().as_micros() as u64, seq);
            }
        }
        println!("── phase: {} ──", phase);
        point.report(phase);
        idxeq.report(phase);
        idxlim.report(phase);
        filter.report(phase);
        group.report(phase);
        if phase == "read+write" {
            insrt.report(phase);
        }
        println!();
    }
}
