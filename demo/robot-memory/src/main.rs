//! Robot Memory — a robot that remembers where the red cup is.
//!
//! The eyes are simulated (waypoint patrol + vision radius + noise);
//! the BRAIN is entirely MoteDB: every sighting is a row carrying all four
//! modalities at once —
//!
//!   * WHEN  (ts TIMESTAMP)            — full sighting history, forever
//!   * WHAT  (note TEXT, FTS-indexed)  — "红杯子 在茶几" (space-separated
//!                                       CJK notes tokenize without jieba)
//!   * WHERE (loc GEOMETRY + spatial)  — POINT3D(x, y, 0) on the floor plane
//!   * and denormalized x/y/zone columns for plain GROUP BY analytics
//!
//! Verified SQL paths only (see issue #17 for the one that isn't):
//!   * latest sighting:   `WHERE obj = '红杯子' LATEST BY obj`
//!   * nearby things:     `WHERE ts > … ORDER BY loc <-> ST_POINT(x, y)`
//!   * history timeline:  `ORDER BY ts ASC`
//!   * habitual places:   `GROUP BY zone`
//!
//! The punchline the demo exists for:
//!   a KV/vector store remembers "now"; a database remembers "always" —
//!   `where 红杯子` answers where it IS, where it WAS, and how long it
//!   stayed there.

mod world;

use motedb::types::Value;
use motedb::Database;

use world::{objects, zone_of, H, VISION_RADIUS, W, WAYPOINTS};

const DB_DIR: &str = "robotbrain.mote";

fn open_db() -> Database {
    let db = if std::path::Path::new(DB_DIR).exists() {
        Database::open(DB_DIR)
    } else {
        Database::create(DB_DIR)
    }
    .expect("open db");

    db.execute(
        "CREATE TABLE IF NOT EXISTS sightings (
            ts   TIMESTAMP,
            obj  TEXT,
            note TEXT,          -- '红杯子 在茶几' (FTS-indexed)
            x    FLOAT,
            y    FLOAT,
            zone TEXT,          -- denormalized for GROUP BY analytics
            loc  GEOMETRY       -- POINT3D(x, y, 0) — spatial queries
        )",
    )
    .expect("create sightings");
    let _ = db.execute("CREATE TEXT INDEX s_note ON sightings(note)");
    let _ = db.execute("CREATE SPATIAL INDEX s_loc ON sightings(loc)");
    db
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn fmt_ago(ts_micros: i64) -> String {
    let dt = (now_micros() - ts_micros).max(0) / 1_000_000;
    if dt < 60 {
        format!("{dt}秒前")
    } else if dt < 3600 {
        format!("{}分钟前", dt / 60)
    } else {
        format!("{}小时前", dt / 3600)
    }
}

fn point(v: &Value) -> (f64, f64) {
    // Geometry Debug is `Point3D(Point3D { x: 1.5, y: 2.0, z: 0.0 })`.
    // Extract marker-keyed numbers ("x:" / "y:") — a blind numeric split
    // would happily read the "3" inside "Point3D" as a coordinate.
    let s = format!("{v:?}");
    let grab = |key: &str| -> f64 {
        s.find(&format!("{key}: "))
            .map(|i| {
                let rest = &s[i + key.len() + 2..];
                let tok: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                    .collect();
                tok.parse().unwrap_or(0.0)
            })
            .unwrap_or(0.0)
    };
    (grab("x"), grab("y"))
}

/// One patrol = follow all waypoints; at each waypoint, record every object
/// within VISION_RADIUS (position + noise). The human script moves objects
/// between rounds.
fn cmd_patrol(db: &Database, from_round: usize, to_round: usize, clock_base: i64) {
    let mut objs = objects();
    let mut rng: u64 = 0x9E3779B97F4A7C15;
    let mut mut_rng = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        ((rng >> 11) as f64 / (1u64 << 53) as f64) - 0.5 // [-0.5, 0.5)
    };

    for round in from_round..=to_round {
        // Human interference (scripted story).
        for (r, name, nx, ny) in world::SCRIPT {
            if *r == round {
                if let Some(o) = objs.iter_mut().find(|o| o.name == *name) {
                    println!("   （有人把{name}挪走了…机器人下一轮才会发现）");
                    o.x = nx + mut_rng() * 0.4;
                    o.y = ny + mut_rng() * 0.4;
                }
            }
        }
        let mut seen = 0;
        // Simulated clock: each round sits 5 "minutes" before now, so the
        // history reads like a day of patrols rather than "0秒前" everywhere.
        // Deterministic story clock: round N sits at base + (N-1)*5min.
        // Monotonic by construction — no per-round now() drift.
        let round_clock = clock_base + (round as i64 - 1) * 5 * 60 * 1_000_000;
        for (wx, wy) in WAYPOINTS {
            let t = round_clock + seen as i64 * 1_000_000;
            for o in &objs {
                let dx = o.x - wx;
                let dy = o.y - wy;
                if (dx * dx + dy * dy).sqrt() <= VISION_RADIUS {
                    let jx = o.x + mut_rng() * 0.3;
                    let jy = o.y + mut_rng() * 0.3;
                    let zone = zone_of(jx, jy);
                    if std::env::var("RM_DEBUG").is_ok() {
                        eprintln!(
                            "ts={t} r{round} wp({wx:.1},{wy:.1}) {} -> ({jx:.1},{jy:.1}) {zone}",
                            o.name
                        );
                    }
                    db.execute(&format!(
                        "INSERT INTO sightings VALUES ({t}, '{}', '{} 在{zone}', \
                         {jx:.2}, {jy:.2}, '{zone}', POINT3D({jx:.2}, {jy:.2}, 0.0))",
                        o.name, o.name
                    ))
                    .expect("insert sighting");
                    seen += 1;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        println!("第 {round} 轮巡逻: 记录 {seen} 次目击");
    }
    db.checkpoint().ok();
    let n = db.query("SELECT COUNT(*) FROM sightings").unwrap();
    let total = match &n[0][0] {
        Value::Integer(i) => *i,
        _ => 0,
    };
    println!("记忆库共 {total} 条目击记录");
}

/// ASCII map: zones as letters, objects at their LATEST positions.
fn draw_map(db: &Database, highlight: Option<&str>) {
    let latest = db
        .query("SELECT obj, loc FROM sightings LATEST BY obj")
        .unwrap();
    let mut grid = vec![vec!['·'; W as usize]; H as usize];
    for (name, x0, y0, x1, y1) in world::ZONES {
        let ch = match *name {
            "茶几" => '桌',
            "沙发" => '发',
            "厨房" => '厨',
            "水槽" => '槽',
            "门口" => '门',
            _ => '走',
        };
        let _ = name;
        for gy in (*y0 as usize)..=(*y1 as usize).min(9) {
            for gx in (*x0 as usize)..=(*x1 as usize).min(9) {
                grid[gy][gx] = ch;
            }
        }
    }
    let mut marks: Vec<(f64, f64, char, bool)> = Vec::new();
    for r in &latest {
        let name = match &r[0] {
            Value::Text(t) => t.to_string(),
            _ => continue,
        };
        let (x, y) = point(&r[1]);
        let ch = name.chars().next().unwrap_or('?');
        marks.push((x, y, ch, Some(name.as_str()) == highlight));
    }
    for (x, y, ch, hi) in &marks {
        let gx = (x.round() as i64).clamp(0, 9) as usize;
        let gy = (y.round() as i64).clamp(0, 9) as usize;
        grid[gy][gx] = if *hi { '★' } else { *ch };
    }
    println!("\n     0  1  2  3  4  5  6  7  8  9");
    for (i, row) in grid.iter().enumerate() {
        let mut out = String::new();
        for cell in row {
            out.push(*cell);
            out.push_str("  ");
        }
        println!("  {i:>2} {out}");
    }
    println!("  图例: 桌=茶几 发=沙发 厨=厨房 槽=水槽 门=门口 · 走道  ★=查询目标");
}

fn cmd_where(db: &Database, name: &str) {
    // Latest sighting (verified path: equality + LATEST BY).
    let rows = db
        .query(&format!(
            "SELECT loc, ts, zone FROM sightings WHERE obj = '{name}' LATEST BY obj"
        ))
        .unwrap();
    if rows.is_empty() {
        println!("还没见过「{name}」。先 patrol 巡逻几轮。");
        return;
    }
    let (x, y) = point(&rows[0][0]);
    let ts = match &rows[0][1] {
        Value::Timestamp(t) => t.as_micros(),
        Value::Integer(i) => *i,
        _ => 0,
    };
    let zone = match &rows[0][2] {
        Value::Text(t) => t.to_string(),
        _ => String::new(),
    };
    println!(
        "\n「{name}」现在在: {zone} ({x:.1}, {y:.1})，{}目击",
        fmt_ago(ts)
    );

    // History — the database remembers "always", not just "now".
    let hist = db
        .query(&format!(
            // LIMIT workaround for issue #18: eq-WHERE + ORDER BY without
            // LIMIT projects the TIMESTAMP column as NULL.
            "SELECT ts, zone, x, y FROM sightings WHERE obj = '{name}' \
             ORDER BY ts ASC LIMIT 100000"
        ))
        .unwrap();
    if hist.len() > 1 {
        let first_ts = match &hist[0][0] {
            Value::Timestamp(t) => t.as_micros(),
            Value::Integer(i) => *i,
            _ => 0,
        };
        let (fx, fy) = (
            match &hist[0][2] {
                Value::Float(f) => *f,
                _ => 0.0,
            },
            match &hist[0][3] {
                Value::Float(f) => *f,
                _ => 0.0,
            },
        );
        let first_zone = zone_of(fx, fy);
        println!(
            "记忆: 共 {} 次目击; 最初在{first_zone} ({fx:.1},{fy:.1})，{}",
            hist.len(),
            fmt_ago(first_ts)
        );
        // Habitual place (verified: plain GROUP BY on denormalized column).
        let zones = db
            .query(&format!(
                "SELECT zone, COUNT(*) AS n FROM sightings WHERE obj = '{name}' \
                 GROUP BY zone ORDER BY n DESC LIMIT 1"
            ))
            .unwrap();
        if let Some(z) = zones.first() {
            let zn = match &z[0] {
                Value::Text(t) => t.to_string(),
                _ => String::new(),
            };
            let n = match &z[1] {
                Value::Integer(i) => *i,
                _ => 0,
            };
            let pct = n * 100 / hist.len() as i64;
            println!("习惯位置: {zn}（{pct}% 的目击都在这里）");
        }
    }
    draw_map(db, Some(name));
}

fn cmd_near(db: &Database, x: f64, y: f64, k: usize) {
    // Recent sightings ordered by spatial distance (verified path).
    let cutoff = now_micros() - 6_000_000; // last ~6 s of sim time is too tight for
                                           // a scripted demo; widen to everything for determinism:
    let _ = cutoff;
    let rows = db
        .query(&format!(
            "SELECT obj, loc, ts FROM sightings \
             ORDER BY loc <-> ST_POINT({x}, {y}) ASC LIMIT {k}"
        ))
        .unwrap();
    println!("\n离 ({x:.1}, {y:.1}) 最近的 {k} 次目击（含历史位置）:");
    for (i, r) in rows.iter().enumerate() {
        let obj = match &r[0] {
            Value::Text(t) => t.to_string(),
            _ => continue,
        };
        let (ox, oy) = point(&r[1]);
        let ts = match &r[2] {
            Value::Timestamp(t) => t.as_micros(),
            Value::Integer(i) => *i,
            _ => 0,
        };
        let d = ((ox - x).powi(2) + (oy - y).powi(2)).sqrt();
        println!(
            "  {}. {obj} ({ox:.1},{oy:.1}) 距离 {d:.1} · {}",
            i + 1,
            fmt_ago(ts)
        );
    }
}

fn cmd_timeline(db: &Database, name: &str) {
    let rows = db
        .query(&format!(
            // Same issue-#18 workaround as above.
            "SELECT ts, x, y, zone FROM sightings WHERE obj = '{name}' \
             ORDER BY ts ASC LIMIT 100000"
        ))
        .unwrap();
    if rows.is_empty() {
        println!("没有「{name}」的目击记录。");
        return;
    }
    println!("\n「{name}」目击时间线（数据库记得\"一直\"，不只是\"现在\"）:");
    let mut last_zone = String::new();
    for r in &rows {
        let ts = match &r[0] {
            Value::Timestamp(t) => t.as_micros(),
            Value::Integer(i) => *i,
            _ => 0,
        };
        let (x, y) = (
            match &r[1] {
                Value::Float(f) => *f,
                _ => 0.0,
            },
            match &r[2] {
                Value::Float(f) => *f,
                _ => 0.0,
            },
        );
        let zone = match &r[3] {
            Value::Text(t) => t.to_string(),
            _ => String::new(),
        };
        let mark = if zone != last_zone {
            "◀ 位置变化"
        } else {
            ""
        };
        println!("  {} ({x:.1},{y:.1}) {zone} {mark}", fmt_ago(ts));
        last_zone = zone;
    }
}

fn cmd_zones(db: &Database) {
    let rows = db
        .query("SELECT obj, zone, COUNT(*) AS n FROM sightings GROUP BY obj, zone ORDER BY obj, n DESC")
        .unwrap();
    println!("\n每件东西的习惯位置（GROUP BY obj, zone）:");
    let mut cur = String::new();
    for r in &rows {
        let obj = match &r[0] {
            Value::Text(t) => t.to_string(),
            _ => continue,
        };
        if obj != cur {
            if !cur.is_empty() {
                println!();
            }
            print!("  {obj}: ");
            cur = obj;
        }
        let zone = match &r[1] {
            Value::Text(t) => t.to_string(),
            _ => String::new(),
        };
        let n = match &r[2] {
            Value::Integer(i) => *i,
            _ => 0,
        };
        print!("{zone}×{n}  ");
    }
    println!();
}

fn usage() {
    eprintln!(
        "usage: robot_memory <command>\n\
           patrol [n]        模拟 n 轮巡逻（默认 8；红杯子中途会被挪走）\n\
           where <物体>      它在哪？（最新目击 + 历史 + 习惯位置 + 地图）\n\
           near <x> <y> [k]  离这点最近的目击\n\
           timeline <物体>   完整目击时间线\n\
           zones             每件东西的习惯位置\n\
           story             一键完整演示: 巡逻→杯子被挪→问答\n\
         物体: 红杯子 蓝碗 充电器 遥控器 钥匙"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db = open_db();
    match args.get(1).map(String::as_str) {
        Some("patrol") => {
            let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
            let base = now_micros() - n as i64 * 5 * 60 * 1_000_000;
            cmd_patrol(&db, 1, n, base);
        }
        Some("where") => match args.get(2) {
            Some(n) => cmd_where(&db, n),
            None => usage(),
        },
        Some("near") => {
            let x: f64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5.0);
            let y: f64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(5.0);
            let k: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(5);
            cmd_near(&db, x, y, k);
        }
        Some("timeline") => match args.get(2) {
            Some(n) => cmd_timeline(&db, n),
            None => usage(),
        },
        Some("zones") => cmd_zones(&db),
        Some("story") => {
            // Story is a scripted narrative — always start from an empty brain.
            drop(db);
            let _ = std::fs::remove_dir_all(DB_DIR);
            let db = open_db();
            let base = now_micros() - 8 * 5 * 60 * 1_000_000;
            println!("═══ 开场: 空记忆 ═══");
            cmd_where(&db, "红杯子");
            println!("\n═══ 巡逻 4 轮（杯子还在茶几）═══");
            cmd_patrol(&db, 1, 4, base);
            cmd_where(&db, "红杯子");
            println!("\n═══ 再巡逻 4 轮（中途有人把杯子挪进厨房）═══");
            cmd_patrol(&db, 5, 8, base);
            println!("\n═══ 问: 红杯子在哪? ═══");
            cmd_where(&db, "红杯子");
            println!("\n═══ 它的完整记忆时间线 ═══");
            cmd_timeline(&db, "红杯子");
            println!("\n═══ 我在走道(4,5)，附近有什么? ═══");
            cmd_near(&db, 4.0, 5.0, 5);
            cmd_zones(&db);
        }
        _ => usage(),
    }
}
