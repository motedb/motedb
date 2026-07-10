use motedb::types::Value;
use motedb::Database;

fn dir_walk_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if path.is_file() {
        total += path.metadata().map(|m| m.len()).unwrap_or(0);
    } else if path.is_dir() {
        for entry in std::fs::read_dir(path).unwrap() {
            total += dir_walk_bytes(&entry.unwrap().path());
        }
    }
    total
}

fn collect_files(path: &std::path::Path, out: &mut Vec<(String, u64)>, depth: usize) {
    if path.is_file() {
        if let Ok(meta) = path.metadata() {
            out.push((path.display().to_string(), meta.len()));
        }
    } else if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                collect_files(&entry.path(), out, depth + 1);
            }
        }
    }
}

#[test]
#[ignore = "slow: measures disk usage of 100K rows, ~37s in debug"]
fn disk_usage_measurement() {
    let base = format!("/tmp/motedb_disk_test_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&base);
    let _ = std::fs::remove_dir_all(format!("{}.mote", &base));

    let db = Database::create(&base).unwrap();

    // Scalar-only table (4 cols: id INT, name TEXT, age INT, score FLOAT)
    db.execute("CREATE TABLE scalar (id INT PRIMARY KEY, name TEXT, age INT, score FLOAT)")
        .unwrap();
    for i in 0..10000 {
        db.execute_prepared(
            "INSERT INTO scalar VALUES (?, ?, ?, ?)",
            vec![
                Value::Integer(i),
                Value::text(format!("user_{}", i)),
                Value::Integer(20 + i % 50),
                Value::Float(50.0 + i as f64 % 100.0),
            ],
        )
        .unwrap();
    }

    db.execute("CREATE INDEX idx_scalar_age ON scalar(age)")
        .unwrap();
    db.checkpoint().unwrap();

    // Raw row size
    let row = vec![
        Value::Integer(42),
        Value::text("user_42".to_string()),
        Value::Integer(62),
        Value::Float(92.0),
    ];
    let col_types = [
        motedb::types::ColumnType::Integer,
        motedb::types::ColumnType::Text,
        motedb::types::ColumnType::Integer,
        motedb::types::ColumnType::Float,
    ];
    let encoded = motedb::storage::row_format::encode(&row, &col_types).unwrap();

    let mote_path = format!("{}.mote", base);
    let mote_bytes = dir_walk_bytes(std::path::Path::new(&mote_path));

    println!("\n=== Disk Usage: Scalar Table (10K rows, 4 cols) ===");
    println!("Total on disk: {} KB", mote_bytes / 1024);
    println!("Per-row on disk: {:.1} bytes", mote_bytes as f64 / 10000.0);
    println!("RawRow per row: {} bytes (pre-compression)", encoded.len());
    println!(
        "Compression ratio: {:.1}x",
        (encoded.len() as f64 * 10000.0) / mote_bytes as f64
    );

    // Sub-directory breakdown (all files, including hidden)
    println!("\n  Directory breakdown:");
    let mut all_files: Vec<(String, u64)> = Vec::new();
    collect_files(std::path::Path::new(&mote_path), &mut all_files, 0);
    let mut by_dir: Vec<(String, u64)> = Vec::new();
    for (path, size) in &all_files {
        let dir = path.rsplitn(2, '/').nth(1).unwrap_or(".").to_string();
        if let Some(entry) = by_dir.iter_mut().find(|(d, _)| d == &dir) {
            entry.1 += *size;
        } else {
            by_dir.push((dir, *size));
        }
    }
    by_dir.sort_by(|a, b| b.1.cmp(&a.1));
    for (dir, size) in &by_dir {
        println!("    {}: {} KB", dir, size / 1024);
    }
    println!(
        "    TOTAL: {} KB",
        all_files.iter().map(|(_, s)| s).sum::<u64>() / 1024
    );

    // Top 10 largest individual files
    println!("\n  Top 10 largest files:");
    all_files.sort_by(|a, b| b.1.cmp(&a.1));
    for (path, size) in all_files.iter().take(10) {
        println!("    {} bytes — {}", size, path);
    }

    // LSM SSTable files
    let lsm_dir = std::path::Path::new(&mote_path).join("lsm");
    if lsm_dir.exists() {
        println!("\n  SSTable files:");
        let mut sst_total = 0u64;
        for entry in std::fs::read_dir(&lsm_dir).unwrap() {
            let e = entry.unwrap();
            let meta = e.metadata().unwrap();
            let name = e.file_name().to_string_lossy().to_string();
            if meta.is_file() {
                sst_total += meta.len();
                println!("    {}: {} bytes", name, meta.len());
            }
        }
        println!("    SST total: {} KB", sst_total / 1024);
        println!("    SST per-row: {:.1} bytes", sst_total as f64 / 10000.0);
    }

    // Theoretical minimum
    println!("\n  Theoretical minimum (4 cols):");
    println!("    id (i64): 8, name ('user_42'): 8+7=15, age (i64): 8, score (f64): 8 = 39 bytes");
    println!(
        "    Actual: {} bytes ({:.0}% overhead)",
        encoded.len(),
        (encoded.len() as f64 / 39.0 - 1.0) * 100.0
    );

    // Embedded projection
    println!("\n=== Embedded Device Projection (IMU 100Hz, 6 floats) ===");
    let imu_rawrow = 16 + 6 * 8; // header + 6 fixed cols
    let imu_sst = imu_rawrow as f64 / (encoded.len() as f64 / (mote_bytes as f64 / 10000.0));
    let rows_per_day = 100 * 3600 * 24;
    println!("  RawRow per row: {} bytes", imu_rawrow);
    println!("  Estimated SST per row: {:.1} bytes", imu_sst);
    println!("  Rows/day: {}", rows_per_day);
    println!(
        "  1 day: {:.0} MB",
        rows_per_day as f64 * imu_sst / 1024.0 / 1024.0
    );
    println!(
        "  30 days: {:.0} MB",
        rows_per_day as f64 * imu_sst * 30.0 / 1024.0 / 1024.0
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&base);
    let _ = std::fs::remove_dir_all(format!("{}.mote", &base));
}
