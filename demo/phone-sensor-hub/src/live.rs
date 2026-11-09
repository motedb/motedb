//! Live mode: the database IS the classifier.
//!
//! `gestures` rows (VECTOR(8) window fingerprints, recorded via `record`)
//! act as a 1-NN model store. A 3-second sliding window over the live
//! sensor stream is fingerprinted the same way and classified by KNN vote
//! (`ORDER BY fp <-> '[…]' LIMIT 5`). No ML framework — the engine's
//! vector path does all the work.

use std::collections::VecDeque;
use std::io::BufRead;
use std::sync::{Arc, Mutex};

use motedb::Database;
use motedb::types::Value;

use crate::{as_f64, as_micros, fmt_minute, fingerprint, SensorLine};

/// Everything the dashboard (and the terminal UI) needs, snapshotted.
#[derive(Clone, Default)]
pub struct SharedState {
    pub label: String,
    pub conf: f32,
    pub per_label: Vec<(String, f32)>,
    pub mags: Vec<f64>, // last ~6 s of |acc|, display ring
    pub samples: u64,
    pub shakes: u64,
    pub minute_rows: Vec<(i64, f64)>,
    pub last_ts: i64,
}

const WINDOW_US: i64 = 3_000_000; // sliding window: 3 s
const CLASSIFY_EVERY_US: i64 = 500_000;
const SHAKE_G: f64 = 15.0;

fn vote_top5(db: &Database, fp: &[f32]) -> (String, f32, Vec<(String, f32)>) {
    let q = format!(
        "[{}]",
        fp.iter().map(|v| format!("{v:.4}")).collect::<Vec<_>>().join(",")
    );
    let rows = db
        .query(&format!(
            "SELECT label FROM gestures ORDER BY fp <-> '{q}' LIMIT 5"
        ))
        .unwrap_or_default();
    let mut counts: Vec<(String, u32)> = Vec::new();
    for r in &rows {
        let label = match r.first() {
            Some(Value::Text(t)) => t.to_string(),
            _ => continue,
        };
        counts
            .iter_mut()
            .find(|(l, _)| *l == label)
            .map(|(_, c)| *c += 1)
            .unwrap_or_else(|| counts.push((label, 1)));
    }
    counts.sort_by(|a, b| b.1.cmp(&a.1));
    if counts.is_empty() {
        return ("(no gestures recorded)".into(), 0.0, vec![]);
    }
    let total: u32 = counts.iter().map(|(_, c)| c).sum();
    let (label, c) = counts[0].clone();
    let per = counts
        .iter()
        .map(|(l, c)| (l.clone(), *c as f32 / total as f32 * 100.0))
        .collect();
    (label, c as f32 / total as f32 * 100.0, per)
}

fn bar(pct: f32, width: usize) -> String {
    let n = (pct as usize * width / 100).min(width);
    format!("{}{}", "█".repeat(n), "░".repeat(width - n))
}

/// Insert one sample into the persistent tables (sensor + events).
fn insert_sample(db: &Database, ts_micros: i64, s: (f64, f64, f64, f64, f64, f64)) {
    let (ax, ay, az, gx, gy, gz) = s;
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
    }
}

fn parse_sensor_line(line: &str) -> Option<(i64, (f64, f64, f64, f64, f64, f64))> {
    let parsed: SensorLine = serde_json::from_str(line).ok()?;
    let ts = parsed.timestamp * 1000;
    let mut acc = None;
    let mut gyro = None;
    for s in &parsed.sensors {
        match (s.name.as_str(), s.values.as_slice()) {
            ("ACCELEROMETER", v) if v.len() == 3 => acc = Some((v[0], v[1], v[2])),
            ("GYROSCOPE", v) if v.len() == 3 => gyro = Some((v[0], v[1], v[2])),
            _ => {}
        }
    }
    let (ax, ay, az) = acc.unwrap_or((0.0, 0.0, 0.0));
    let (gx, gy, gz) = gyro.unwrap_or((0.0, 0.0, 0.0));
    Some((ts, (ax, ay, az, gx, gy, gz)))
}

/// `record <label>`: read ~15 s (or until EOF) and store 3 window
/// fingerprints into `gestures` — the 1-NN "training set".
pub fn cmd_record(db: &Database, label: &str) {
    let stdin = std::io::stdin();
    let mut all: Vec<(i64, (f64, f64, f64, f64, f64, f64))> = Vec::new();
    for line in stdin.lock().lines() {
        let Ok(l) = line else { break };
        if let Some(p) = parse_sensor_line(l.trim()) {
            all.push(p);
        }
    }
    if all.len() < 45 {
        eprintln!(
            "need at least ~45 samples for a recording, got {} — stream longer and retry",
            all.len()
        );
        std::process::exit(2);
    }
    // One fingerprint per full 3-second window — the same window length the
    // live classifier uses, so reference and query distributions match.
    let mut stored = 0usize;
    let mut i = 0usize;
    while i + 1 < all.len() {
        let t0 = all[i].0;
        let mut j = i;
        while j < all.len() && all[j].0 - t0 < 3_000_000 {
            j += 1;
        }
        if j - i >= 10 {
            let chunk = &all[i..j];
            let fp = fingerprint(&chunk.iter().map(|(_, s)| *s).collect::<Vec<_>>(), 3.0);
            let fp_str = format!(
                "[{}]",
                fp.iter().map(|v| format!("{v:.4}")).collect::<Vec<_>>().join(",")
            );
            let ts = chunk[0].0;
            db.execute(&format!(
                "INSERT INTO gestures VALUES ({ts}, '{label}', '{fp_str}')"
            ))
            .expect("insert gesture");
            stored += 1;
        }
        i = j;
    }
    println!(
        "recorded gesture '{label}' ({} samples → {stored} window fingerprints)",
        all.len()
    );
}

/// `live` / `serve`: stream stdin, classify continuously, optionally serve
/// the web dashboard on :8080.
pub fn run_live(db: &Database, with_server: bool) {
    let state = Arc::new(Mutex::new(SharedState::default()));

    if with_server {
        let st = Arc::clone(&state);
        std::thread::spawn(move || crate::dashboard::serve(st));
        let port = std::env::var("SENSOR_HUB_PORT").unwrap_or_else(|_| "8080".into());
        println!("dashboard: http://<this-phone-ip>:{port}  (Ctrl-C to stop)");
    }

    let stdin = std::io::stdin();
    let mut window: VecDeque<(i64, (f64, f64, f64, f64, f64, f64))> = VecDeque::new();
    let mut next_classify = 0i64;
    let mut last_minute = -1i64;
    let mut minute_start_ts = 0i64;
    let mut minute_bucket: Vec<(f64, f64, f64, f64, f64, f64)> = Vec::new();
    let mut last_print = std::time::Instant::now() - std::time::Duration::from_secs(1);

    for line in stdin.lock().lines() {
        let Ok(l) = line else { break };
        let Some((ts, s)) = parse_sensor_line(l.trim()) else { continue };

        insert_sample(db, ts, s);

        // Sliding window.
        window.push_back((ts, s));
        while let Some((old, _)) = window.front() {
            if ts - old > WINDOW_US {
                window.pop_front();
            } else {
                break;
            }
        }

        // Minute fingerprinting (same as ingest mode).
        let minute = ts.div_euclid(60_000_000);
        if minute != last_minute {
            if last_minute != -1 && minute_bucket.len() >= 30 {
                let fp = fingerprint(&minute_bucket, minute_bucket.len() as f64 / 5.0);
                let fp_str = format!(
                    "[{}]",
                    fp.iter().map(|v| format!("{v:.4}")).collect::<Vec<_>>().join(",")
                );
                let m0 = minute_start_ts.div_euclid(60_000_000) * 60_000_000;
                db.execute(&format!(
                    "INSERT INTO minute_fp VALUES ({m0}, '{fp_str}', {})",
                    minute_bucket.len()
                ))
                .ok();
            }
            last_minute = minute;
            minute_start_ts = ts;
            minute_bucket.clear();
        }
        minute_bucket.push(s);

        // Classify twice a second.
        if ts >= next_classify && window.len() >= 10 {
            next_classify = ts + CLASSIFY_EVERY_US;
            let fp = fingerprint(&window.iter().map(|(_, s)| *s).collect::<Vec<_>>(), 3.0);
            let (label, conf, per) = vote_top5(db, &fp);
            let mag = {
                let (ax, ay, az, _, _, _) = s;
                (ax * ax + ay * ay + az * az).sqrt()
            };
            {
                let mut st = state.lock().unwrap();
                st.label = label.clone();
                st.conf = conf;
                st.per_label = per.clone();
                st.mags.push(mag);
                let _ = st.mags.len().min(140); // ring
                if st.mags.len() > 140 {
                    st.mags.remove(0);
                }
                st.samples += 1;
                if mag > SHAKE_G {
                    st.shakes += 1;
                }
                st.last_ts = ts;
            }
            if last_print.elapsed() >= std::time::Duration::from_millis(250) {
                last_print = std::time::Instant::now();
                print!("\x1b[2J\x1b[H");
                println!("╔══════════════════════════════════════╗");
                println!("║  MoteDB live · 1-NN activity         ║");
                println!("╚══════════════════════════════════════╝");
                println!("  {label}  ({conf:.0}%)");
                for (l, p) in &per {
                    println!("  {:<10} {:>5.0}% {}", l, p, bar(*p, 24));
                }
                println!(
                    "\n  samples {} · shakes {} · {}",
                    window.len(),
                    state.lock().unwrap().shakes,
                    fmt_minute(ts)
                );
                println!("  (gestures = model, stored & queried as SQL VECTOR rows)");
                use std::io::Write as _;
                let _ = std::io::stdout().flush();
            }
        }
    }

    // Refresh minute rows for the dashboard one last time.
    let rows = db
        .query("SELECT minute, ROUND(acc_power,1) FROM minute_stats ORDER BY minute DESC LIMIT 12")
        .unwrap_or_default();
    let mut st = state.lock().unwrap();
    st.minute_rows = rows
        .iter()
        .map(|r| (as_micros(&r[0]), as_f64(&r[1])))
        .collect();
}
