use motedb::{Database, DBConfig};
use tempfile::TempDir;

fn rss_kb() -> usize {
    let pid = std::process::id();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output().ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<usize>().ok())
        .unwrap_or(0)
}

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, customer TEXT, amount FLOAT, region TEXT)").unwrap();
    eprintln!("[mem] after create: {} KB", rss_kb());

    let n = 300_000usize;
    let bs = 5000;
    for start in (0..n).step_by(bs) {
        let end = (start + bs).min(n);
        let mut batch = String::new();
        for i in start..end {
            let region = if i % 3 == 0 { "US" } else { "EU" };
            if !batch.is_empty() { batch.push(','); }
            batch.push_str(&format!("('cust_{}',{:.2},'{}')", i % 30000, (i as f64 * 1.7) % 1000.0, region));
        }
        db.execute(&format!("INSERT INTO t (customer, amount, region) VALUES {}", batch)).unwrap();
    }
    eprintln!("[mem] after {} INSERT: {} KB", n, rss_kb());
    db.flush().unwrap();
    // Measure segment file sizes
    let ms_dir = dir.path().join("columnar_ms").join("t");
    let mut total = 0u64;
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(&ms_dir) {
        for e in entries.flatten() {
            if let Ok(m) = std::fs::metadata(e.path()) {
                total += m.len(); count += 1;
            }
        }
    }
    eprintln!("[seg] {} files, {} KB total on disk", count, total/1024);
    eprintln!("[mem] after flush: {} KB", rss_kb());
    // Measure segment file sizes
    // Walk all subdirs to find segment files
    fn walk_all(p: &std::path::Path, prefix: String) {
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let path = e.path();
                let name = format!("{}/{}", prefix, e.file_name().to_string_lossy());
                if let Ok(meta) = std::fs::metadata(&path) {
                    if meta.is_file() && meta.len() > 0 {
                        eprintln!("[file] {} = {}KB", name, meta.len()/1024);
                    }
                    if meta.is_dir() {
                        walk_all(&path, name);
                    }
                }
            }
        }
    }
    walk_all(dir.path(), "db".to_string());
    let dbp = dir.path().join("columnar_ms").join("t");
    let mut total_seg = 0u64;
    let mut seg_count = 0;
    if let Ok(entries) = std::fs::read_dir(&dbp) {
        for e in entries.flatten() {
            if e.path().extension().map(|x| x == "sst").unwrap_or(false) {
                total_seg += std::fs::metadata(e.path()).map(|m| m.len()).unwrap_or(0);
                seg_count += 1;
            }
        }
    }
    eprintln!("[seg] {} files, {} MB total on disk", seg_count, total_seg/1048576);
    let _ = db.execute("SELECT COUNT(*) FROM t").unwrap();
    eprintln!("[mem] after COUNT: {} KB", rss_kb());
    let _ = db.execute("SELECT * FROM t").unwrap();
    eprintln!("[mem] after SELECT *: {} KB", rss_kb());
    // List directories BEFORE checkpoint (TempDir still alive)
    for entry in std::fs::read_dir(dir.path()).ok().into_iter().flatten().flatten() {
        let name = entry.file_name();
        let meta = std::fs::metadata(entry.path()).ok();
        if meta.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
            let sz: u64 = std::fs::read_dir(entry.path()).ok()
                .into_iter().flatten().flatten()
                .filter_map(|e| std::fs::metadata(e.path()).ok())
                .map(|m| m.len()).sum();
            eprintln!("[dir] {} = {}MB", name.to_string_lossy(), sz/1048576);
        }
    }
    // Debug: walk the dir path
    let dbp = dir.path();
    eprintln!("[debug] db path = {:?}", dbp);
    fn walk(p: &std::path::Path, depth: usize) {
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let meta = std::fs::metadata(e.path());
                let sz = meta.as_ref().ok().map(|m| if m.is_file() { m.len() } else { 0 }).unwrap_or(0);
                if depth <= 2 {
                    eprintln!("[walk] {}{} {}B", "  ".repeat(depth), e.file_name().to_string_lossy(), sz);
                }
                if meta.map(|m| m.is_dir()).unwrap_or(false) {
                    walk(&e.path(), depth+1);
                }
            }
        }
    }
    walk(&dbp, 0);
    db.checkpoint().unwrap();
    eprintln!("[mem] after checkpoint: {} KB", rss_kb());
        // Measure disk usage of ColSegmentStore + LSM + WAL
    let db_dir = dir.path();
    // List actual directories
    for entry in std::fs::read_dir(db_dir).ok().into_iter().flatten().flatten() {
        let name = entry.file_name();
        let meta = std::fs::metadata(entry.path()).ok();
        if meta.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
            let sz: u64 = std::fs::read_dir(entry.path()).ok()
                .into_iter().flatten().flatten()
                .filter_map(|e| std::fs::metadata(e.path()).ok())
                .map(|m| m.len()).sum();
            eprintln!("[dir] {} = {}MB", name.to_string_lossy(), sz/1048576);
        }
    }
    let col_seg_size = std::fs::read_dir(db_dir.join("columnar_ms")).ok()
        .map(|entries| entries.filter_map(|e| std::fs::metadata(e.ok()?.path()).ok())
            .map(|m| m.len()).sum::<u64>()).unwrap_or(0);
    let lsm_size = std::fs::read_dir(db_dir.join("lsm")).ok()
        .map(|e| e.filter_map(|x| std::fs::metadata(x.ok()?.path()).ok())
            .map(|m| m.len()).sum::<u64>()).unwrap_or(0);
    let wal_size = std::fs::read_dir(db_dir.join("wal")).ok()
        .map(|e| e.filter_map(|x| std::fs::metadata(x.ok()?.path()).ok())
            .map(|m| m.len()).sum::<u64>()).unwrap_or(0);
    eprintln!("[disk] columnar_ms={}MB lsm={}MB wal={}MB", col_seg_size/1048576, lsm_size/1048576, wal_size/1048576);
    println!("DONE");
}

// Appended: measure disk usage
