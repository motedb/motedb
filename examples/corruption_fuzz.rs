//! Corruption-robustness fuzz-lite: a foundational DB must return Err (or
//! recover cleanly) on corrupted files — NEVER panic.
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
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, code TEXT, ts BIGINT, v VECTOR(3))").unwrap();
    let mut sql = String::from("INSERT INTO t (name, val, code, ts, v) VALUES ");
    for i in 0..500 {
        sql.push_str(&format!(
            "('u_{}', {:.1}, 'CD_{}', {}, '[1.0,2.0,{}]')",
            i % 50,
            i as f64,
            i % 25,
            1700000000i64 + i as i64,
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

fn exercise(db: &Database) {
    // Any of these may Err on corrupt data — must not panic.
    let _ = db.execute("SELECT COUNT(*) FROM t");
    let _ = db.execute("SELECT id, name, val, code, ts FROM t WHERE id = 100");
    let _ = db.execute("SELECT code, COUNT(*) FROM t GROUP BY code");
    let _ = db.execute("SELECT id FROM t WHERE code = 'CD_7'");
    let _ = db.execute("SELECT id FROM t WHERE name MATCH AGAINST('u_1')");
    let _ = db.execute("SELECT id FROM t ORDER BY v <-> '[1,2,3]' LIMIT 5");
}

use std::sync::Mutex;
static LAST_PANIC: Mutex<String> = Mutex::new(String::new());

fn main() {
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_default();
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_default();
        *LAST_PANIC.lock().unwrap() = format!("{loc} | {msg}");
    }));
    let seed = TempDir::new().unwrap();
    build_seed(seed.path());
    let src = db_dir(seed.path());
    println!("seed files: {}", files_under(&src).len());

    let mut rng = Lcg(0xDEADBEEF12345678);
    let all = files_under(&src);
    let mut panics = 0;
    let mut oks = 0;
    let mut errs = 0;
    let iterations = 5000;
    for it in 0..iterations {
        let work = TempDir::new().unwrap();
        let target = work.path().join("seed.mote");
        copy_dir(&src, &target);
        let files = files_under(&target);
        // Deterministic corruption: pick a file, corrupt 1-3 bytes (bit flips),
        // and sometimes truncate.
        let ncorrupt = match std::env::var("FUZZ_CLEAN") {
            Ok(_) => 0,
            _ => {
                if it == 0 {
                    0
                } else {
                    1 + (rng.next() % 3) as usize
                }
            }
        };
        for _ in 0..ncorrupt {
            if files.is_empty() {
                break;
            }
            let f = &files[(rng.next() as usize) % files.len()];
            let meta = std::fs::metadata(f);
            let Ok(md) = meta else { continue };
            let len = md.len();
            if len == 0 {
                continue;
            }
            if rng.next() % 4 == 0 {
                // truncate to random prefix
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
                let bit = 1u8 << (rng.next() % 8);
                buf[pos] ^= bit;
                let _ = std::fs::write(f, &buf);
            }
        }
        let do_corrupt = it >= 1; // iteration 0 = pristine control
        if !do_corrupt { /* skip corruption */ }
        let r = std::panic::catch_unwind(|| match Database::open(&target) {
            Ok(db) => {
                exercise(&db);
                0
            }
            Err(_) => 1,
        });
        match r {
            Ok(0) => oks += 1,
            Ok(1) => errs += 1,
            Ok(_) => {}
            Err(_) => {
                panics += 1;
                println!("PANIC at iteration {it}: {}", LAST_PANIC.lock().unwrap());
            }
        }
    }
    println!("\niterations={iterations} ok={oks} clean_err={errs} PANICS={panics}");
}
