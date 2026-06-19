use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn rss() -> usize {
    let pid = std::process::id();
    std::process::Command::new("ps").args(["-o","rss=","-p",&pid.to_string()])
        .output().ok().and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok()).unwrap_or(0)
}

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, c TEXT, a FLOAT, r TEXT)").unwrap();
    for s in (0..300000).step_by(5000) {
        let e = (s+5000).min(300000);
        let mut b = String::new();
        for i in s..e {
            if !b.is_empty() { b.push(','); }
            b.push_str(&format!("('c_{}',{},'{}')",i%30000,(i as f64*1.7)%1000.0,if i%3==0{"US"}else{"EU"}));
        }
        db.execute(&format!("INSERT INTO t (c,a,r) VALUES {}", b)).unwrap();
    }
    eprintln!("[brk] INSERT: {} KB", rss());
    let _ = db.execute("SELECT COUNT(*) FROM t").unwrap();
    eprintln!("[brk] +COUNT: {} KB", rss());
    let _ = db.execute("SELECT * FROM t WHERE id = 150000").unwrap();
    eprintln!("[brk] +PK: {} KB", rss());
    let _ = db.execute("SELECT * FROM t WHERE r = 'US' LIMIT 10").unwrap();
    eprintln!("[brk] +WHERE: {} KB", rss());
    let _ = db.execute("SELECT c, COUNT(*) FROM t GROUP BY c").unwrap();
    eprintln!("[brk] +GROUPBY: {} KB", rss());
    let _ = db.execute("SELECT * FROM t").unwrap().materialize().unwrap();
    eprintln!("[brk] +FULLSCAN: {} KB", rss());
    println!("DONE");
}
