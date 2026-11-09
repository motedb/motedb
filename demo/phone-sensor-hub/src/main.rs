//! Pocket Sensor Hub — an MoteDB edge demo that runs on an Android phone.
//!
//! v2: the phone becomes an edge node whose DATABASE is the brain — a
//! 1-NN activity classifier whose model storage is MoteDB itself:
//!   * `record <label>` — watch a gesture for ~10 s, store its window
//!     fingerprint as VECTOR(8) rows in `gestures` (the "training set").
//!   * `live`           — stream sensors, classify every half second via
//!     KNN against `gestures`, print the live label + confidence bars.
//!   * `serve`          — same, plus a zero-dependency web dashboard on
//!     http://<phone-ip>:8080 (open it from any laptop on the same WiFi).
//!   1) CAPTURE — Termux:API streams phone sensors as JSON lines on stdin:
//!        {"timestamp": 1762512345678, "sensors": [
//!           {"name": "ACCELEROMETER", "values": [x, y, z]}, ...]}
//!      `sensor_hub ingest` turns the stream into SQL rows (TIMESERIES + TTL).
//!   2) FINGERPRINT — every full minute, an 8-dim feature vector (VECTOR(8))
//!      lands in `minute_fp` for activity similarity search.
//!   3) CONTINUOUS VIEW — `minute_stats` keeps per-minute aggregation.
//!   4) EVENTS — |acc| spikes above a threshold become `events` rows.
//!   5) RETRIEVE — `find-similar <epoch-ms>` KNN-matches one minute against
//!      history ("which earlier minute felt like this one?").
//!   6) TRANSPORT — `export <file>` packs the whole DB into one snapshot for
//!      analysis on a laptop (Arrow/pandas on the other side).
//!
//! Run on the phone (Termux):
//!   pkg install rust git termux-api
//!   git clone https://github.com/motedb/motedb && cd motedb/demo/phone-sensor-hub
//!   cargo run --release -- ingest < <(termux-sensor -s 200 -d 0)
//!   cargo run --release -- report
//!   cargo run --release -- find-similar 1762512345678
//!   cargo run --release -- export /sdcard/sensor.mote
//!
//! Off-phone (macOS/Linux), feed a JSONL file the same way:
//!   ./sensor_hub ingest < sim.jsonl && ./sensor_hub report

mod dashboard;
mod live;

use std::io::BufRead;

use motedb::Database;
use motedb::types::Value;

const DB_DIR: &str = "sensorhub.mote";
/// |a| beyond this (m/s², gravity ≈ 9.81) counts as a shake/pick-up event.
const SHAKE_G: f64 = 15.0;

#[derive(serde::Deserialize)]
pub struct SensorLine {
    timestamp: i64, // epoch ms (Termux)
    #[serde(default)]
    sensors: Vec<SensorEntry>,
}

#[derive(serde::Deserialize)]
pub struct SensorEntry {
    name: String,
    #[serde(default)]
    values: Vec<f64>,
}

/// epoch-micros -> "YYYY-MM-DD HH:MM" (UTC), dependency-free (Hinnant's
/// civil-from-days). Good enough for a demo CLI.
pub fn fmt_minute(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60
    )
}

pub fn as_micros(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        Value::Timestamp(t) => t.as_micros(),
        _ => 0,
    }
}

pub fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => 0.0,
    }
}

fn open_db() -> Database {
    let db = if std::path::Path::new(DB_DIR).exists() {
        Database::open(DB_DIR)
    } else {
        Database::create(DB_DIR)
    }
    .expect("open/create db");
    db.execute(
        "CREATE TABLE IF NOT EXISTS sensor (
            ts TIMESTAMP,            -- micros
            ax FLOAT, ay FLOAT, az FLOAT,
            gx FLOAT, gy FLOAT, gz FLOAT
        ) TIMESERIES(ts) TTL 172800", // 2-day retention
    )
    .expect("create sensor table");
    db.execute(
        "CREATE TABLE IF NOT EXISTS minute_fp (
            minute TIMESTAMP,        -- minute bucket start, micros
            fp VECTOR(8),            -- activity fingerprint
            n  INTEGER
        )",
    )
    .expect("create minute_fp table");
    db.execute(
        "CREATE TABLE IF NOT EXISTS events (
            ts TIMESTAMP, kind TEXT, magnitude FLOAT
        )",
    )
    .expect("create events table");
    db.execute(
        "CREATE TABLE IF NOT EXISTS gestures (
            ts TIMESTAMP, label TEXT, fp VECTOR(8)
        )",
    )
    .expect("create gestures table");
    // Fingerprint index makes the live KNN window cheap; missing index is
    // fine too (exact scan at this scale is sub-millisecond).
    let _ = db.execute("CREATE VECTOR INDEX g_fp ON gestures(fp)");
    // (CREATE CONTINUOUS VIEW has no IF NOT EXISTS yet — tolerate the
    // "already exists" error across reruns.)
    let _ = db.execute(
        "CREATE CONTINUOUS VIEW minute_stats AS
            SELECT TIME_BUCKET('1m', ts) AS minute,
                   AVG(ax*ax + ay*ay + az*az) AS acc_power,
                   FIRST(ts, ts) AS first_ts,
                   LAST(ts, ts)  AS last_ts
            FROM sensor GROUP BY minute",
    );
    db
}

/// 8-dim fingerprint of a sample window: accelerometer magnitude
/// quartiles + gyro energy quartiles + sampling rate (samples/sec —
/// window-length invariant, so 3 s live windows and minute windows
/// fingerprint comparably).
pub fn fingerprint(samples: &[(f64, f64, f64, f64, f64, f64)], duration_s: f64) -> Vec<f32> {
    let mut mags: Vec<f64> = samples
        .iter()
        .map(|&(ax, ay, az, _, _, _)| (ax * ax + ay * ay + az * az).sqrt())
        .collect();
    let mut gyro: Vec<f64> = samples
        .iter()
        .map(|&(_, _, _, gx, gy, gz)| gx * gx + gy * gy + gz * gz)
        .collect();
    mags.sort_by(|a, b| a.partial_cmp(b).unwrap());
    gyro.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |v: &[f64], k: usize| v[v.len() * k / v.len().max(1)] as f32;
    vec![
        q(&mags, 0), q(&mags, 1), q(&mags, 2), q(&mags, 3),
        q(&gyro, 1), q(&gyro, 2), q(&gyro, 3),
        (samples.len() as f64 / duration_s.max(0.001)) as f32,
    ]
}

fn cmd_ingest(db: &Database) {
    let stdin = std::io::stdin();
    let mut n_lines = 0u64;
    let mut n_events = 0u64;

    // Per-minute accumulator -> one fingerprint per full minute.
    let mut cur_minute: i64 = -1;
    let mut bucket: Vec<(f64, f64, f64, f64, f64, f64)> = Vec::new();
    let mut minute_start_ts = 0i64;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed: SensorLine = match serde_json::from_str(line) {
            Ok(p) => p,
            Err(_) => continue, // tolerate malformed stream lines
        };
        let ts_micros = parsed.timestamp * 1000;
        let mut acc = None;
        let mut gyro = None;
        for s in &parsed.sensors {
            match (s.name.as_str(), s.values.as_slice()) {
                ("ACCELEROMETER", v) if v.len() == 3 => {
                    acc = Some((v[0], v[1], v[2]))
                }
                ("GYROSCOPE", v) if v.len() == 3 => gyro = Some((v[0], v[1], v[2])),
                _ => {}
            }
        }
        let (ax, ay, az) = acc.unwrap_or((0.0, 0.0, 0.0));
        let (gx, gy, gz) = gyro.unwrap_or((0.0, 0.0, 0.0));

        db.execute(&format!(
            "INSERT INTO sensor VALUES ({ts_micros}, {ax:.4}, {ay:.4}, {az:.4}, {gx:.4}, {gy:.4}, {gz:.4})"
        ))
        .expect("insert sensor row");

        let mag = (ax * ax + ay * ay + az * az).sqrt();
        if mag > SHAKE_G {
            db.execute(&format!(
                "INSERT INTO events VALUES ({ts_micros}, 'shake', {mag:.2})"
            ))
            .expect("insert event");
            n_events += 1;
        }

        let minute = ts_micros.div_euclid(60_000_000);
        if minute != cur_minute {
            if bucket.len() >= 30 {
                let fp = fingerprint(&bucket, bucket.len() as f64 / 5.0);
                let fp_str = format!(
                    "[{}]",
                    fp.iter().map(|v| format!("{v:.4}")).collect::<Vec<_>>().join(",")
                );
                let m0 = minute_start_ts.div_euclid(60_000_000) * 60_000_000;
                db.execute(&format!(
                    "INSERT INTO minute_fp VALUES ({m0}, '{fp_str}', {})",
                    bucket.len()
                ))
                .expect("insert fingerprint");
            }
            cur_minute = minute;
            minute_start_ts = ts_micros;
            bucket.clear();
        }
        bucket.push((ax, ay, az, gx, gy, gz));
        n_lines += 1;
        if n_lines % 500 == 0 {
            eprintln!("  ingested {n_lines} samples...");
        }
    }
    // Flush the trailing minute too.
    if bucket.len() >= 30 {
        let fp = fingerprint(&bucket, bucket.len() as f64 / 5.0);
        let fp_str = format!(
            "[{}]",
            fp.iter().map(|v| format!("{v:.4}")).collect::<Vec<_>>().join(",")
        );
        let m0 = minute_start_ts.div_euclid(60_000_000) * 60_000_000;
        db.execute(&format!(
            "INSERT INTO minute_fp VALUES ({m0}, '{fp_str}', {})",
            bucket.len()
        ))
        .expect("insert fingerprint");
    }
    db.checkpoint().ok();

    println!("ingested {n_lines} samples, {n_events} shake events -> ./{DB_DIR}");
}

fn cmd_report(db: &Database) {
    let rows = db
        .query("SELECT COUNT(*), MIN(ts), MAX(ts) FROM sensor")
        .expect("count");
    if let Some(r) = rows.first() {
        let n = r.first().and_then(|v| match v {
            Value::Integer(i) => Some(*i),
            _ => None,
        }).unwrap_or(0);
        let lo = r.get(1).map(as_micros).unwrap_or(0);
        let hi = r.get(2).map(as_micros).unwrap_or(0);
        println!("sensor rows: {n}  ({} → {})", fmt_minute(lo), fmt_minute(hi));
    }

    println!("\nminute_stats (continuous view, newest 8 minutes):");
    let rows = db
        .query("SELECT minute, ROUND(acc_power,1) FROM minute_stats ORDER BY minute DESC LIMIT 8")
        .expect("cv query");
    println!("  {:<18} {:>10}", "minute", "acc_power");
    for r in &rows {
        println!("  {:<18} {:>10}", fmt_minute(as_micros(&r[0])), as_f64(&r[1]));
    }

    let rows = db.query("SELECT COUNT(*) FROM events").expect("events count");
    let n = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| match v {
            Value::Integer(i) => Some(*i),
            _ => None,
        })
        .unwrap_or(0);
    println!("\nshake events (|acc| > {SHAKE_G} m/s²): {n}");

    let rows = db.query("SELECT COUNT(*) FROM minute_fp").expect("fp count");
    let n = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| match v {
            Value::Integer(i) => Some(*i),
            _ => None,
        })
        .unwrap_or(0);
    println!("fingerprinted minutes (VECTOR(8)): {n}");
}

fn cmd_find_similar(db: &Database, epoch_ms: i64) {
    // Locate the stored fingerprint of the minute containing epoch_ms.
    let target_minute = epoch_ms * 1000 / 60_000_000 * 60_000_000;
    let rows = db
        .query(&format!(
            "SELECT fp FROM minute_fp WHERE minute = {target_minute}"
        ))
        .expect("target fp");
    let fp: Vec<f32> = match rows.first().and_then(|r| r.first()) {
        Some(Value::Vector(v)) => v.0.to_vec(),
        _ => {
            eprintln!(
                "no fingerprinted minute at {} (ingest at least ~1 minute of data)",
                fmt_minute(target_minute)
            );
            std::process::exit(2);
        }
    };
    let q = format!(
        "[{}]",
        fp.iter().map(|v| format!("{v:.4}")).collect::<Vec<_>>().join(",")
    );
    let rows = db
        .query(&format!(
            "SELECT minute, n FROM minute_fp WHERE minute <> {target_minute} \
             ORDER BY fp <-> '{q}' LIMIT 5"
        ))
        .expect("knn");
    println!("minutes most similar to {}:", fmt_minute(target_minute));
    for (i, r) in rows.iter().enumerate() {
        let n = match &r[1] {
            Value::Integer(i) => *i,
            _ => 0,
        };
        println!("  {}. {}  ({n} samples)", i + 1, fmt_minute(as_micros(&r[0])));
    }
}

fn cmd_export(db: &Database, path: &str) {
    let stats = db
        .export_snapshot(path, false)
        .expect("export");
    println!(
        "snapshot: {path} ({} files, {} bytes) — pull with: adb pull {path}",
        stats.entries, stats.stored_bytes
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: sensor_hub <ingest|record <label>|live|serve|report|find-similar <epoch-ms>|export <path>>";
    let db = open_db();
    match args.get(1).map(String::as_str) {
        Some("ingest") => cmd_ingest(&db),
        Some("record") => {
            let label = args.get(2).cloned().unwrap_or_else(|| {
                eprintln!("{usage}");
                std::process::exit(2);
            });
            live::cmd_record(&db, &label);
        }
        Some("live") => live::run_live(&db, false),
        Some("serve") => live::run_live(&db, true),
        Some("report") => cmd_report(&db),
        Some("find-similar") => {
            let ts: i64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or_else(|| {
                eprintln!("{usage}");
                std::process::exit(2);
            });
            cmd_find_similar(&db, ts);
        }
        Some("export") => {
            let path = args.get(2).cloned().unwrap_or_else(|| {
                eprintln!("{usage}");
                std::process::exit(2);
            });
            cmd_export(&db, &path);
        }
        _ => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    }
}
