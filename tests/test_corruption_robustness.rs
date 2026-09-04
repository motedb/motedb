//! Corruption-robustness: a foundational DB must return Err or degrade
//! cleanly on corrupted files — NEVER panic. Deterministic fuzz-lite with
//! bit flips + truncations on a real database directory, then reopen and
//! exercise the query surface. Found & fixed: B+Tree page deserialization
//! OOB (num_keys / value offsets / inline lengths), raw text-offset slicing
//! in GROUP BY, SegData::slice/get OOB.
use motedb::{DBConfig, Database};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn db_dir(root: &Path) -> PathBuf {
    root.with_extension("mote")
}

fn build_seed(root: &Path) {
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(root, config).unwrap();
    db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, code TEXT, ts BIGINT, v VECTOR(3))",
    )
    .unwrap();
    let mut sql = String::from("INSERT INTO t (name, val, code, ts, v) VALUES ");
    for i in 0..500 {
        sql.push_str(&format!(
            "('u_{}', {:.1}, 'CD_{}', {}, '[1.0,2.0,{}]')",
            i % 50,
            i as f64,
            i % 25,
            1_700_000_000_i64 + i as i64,
            i % 10
        ));
        if i + 1 < 500 {
            sql.push(',');
        }
    }
    db.execute(&sql).unwrap();
    db.execute("CREATE INDEX idx_code ON t (code) USING COLUMN")
        .unwrap();
    db.execute("CREATE TEXT INDEX ft_name ON t (name)").unwrap();
    db.execute("CHECKPOINT").unwrap();
}

fn files_under(p: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            if let Ok(md) = e.metadata() {
                if md.is_dir() {
                    v.extend(files_under(&e.path()));
                } else {
                    v.push(e.path());
                }
            }
        }
    }
    v
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for f in files_under(src) {
        let rel = f.strip_prefix(src).unwrap();
        let to = dst.join(rel);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(&f, &to).unwrap();
    }
}

fn exercise(db: &Database) {
    // Any of these may Err on corrupt data — they must not panic.
    let _ = db.execute("SELECT COUNT(*) FROM t");
    let _ = db.execute("SELECT id, name, val, code, ts FROM t WHERE id = 100");
    let _ = db.execute("SELECT code, COUNT(*) FROM t GROUP BY code");
    let _ = db.execute("SELECT id FROM t WHERE code = 'CD_7'");
    let _ = db.execute("SELECT id FROM t ORDER BY v <-> '[1,2,3]' LIMIT 5");
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

#[test]
fn corrupted_files_never_panic_on_reopen_and_query() {
    let seed = TempDir::new().unwrap();
    build_seed(seed.path());
    let src = db_dir(seed.path());

    let mut rng = Lcg(0xDEADBEEF12345678);
    let all = files_under(&src);
    let mut panics = 0;
    const ITERATIONS: usize = 80; // CI-sized; the 5000-iteration sweep runs as an example
    for it in 0..ITERATIONS {
        let work = TempDir::new().unwrap();
        let target = work.path().join("seed.mote");
        copy_dir(&src, &target);
        let files = files_under(&target);
        let ncorrupt = if it == 0 {
            0
        } else {
            1 + (rng.next() % 3) as usize
        };
        for _ in 0..ncorrupt {
            if files.is_empty() {
                break;
            }
            let f = &files[(rng.next() as usize) % files.len()];
            let Ok(md) = std::fs::metadata(f) else {
                continue;
            };
            let len = md.len();
            if len == 0 {
                continue;
            }
            if rng.next() % 4 == 0 {
                let keep = (rng.next() % len).max(1);
                let _ = std::fs::File::options().write(true).open(f).map(|mut fh| {
                    use std::io::Write;
                    let _ = fh.set_len(keep);
                    let _ = fh.flush();
                });
            } else {
                let pos = (rng.next() % len) as usize;
                let Ok(mut buf) = std::fs::read(f) else {
                    continue;
                };
                buf[pos] ^= 1u8 << (rng.next() % 8);
                let _ = std::fs::write(f, &buf);
            }
        }
        let target = target.clone();
        let r = std::panic::catch_unwind(move || {
            if let Ok(db) = Database::open(&target) {
                exercise(&db);
            }
        });
        if r.is_err() {
            panics += 1;
        }
    }
    assert_eq!(panics, 0, "corruption must degrade to Err, never panic");
}
