//! W3 probe: unranked LIMIT vs ranked top-k cost split on a 100K corpus.

use motedb::types::Value;
use motedb::Database;
use std::time::Instant;

#[test]
fn fts_limit_probe() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE ev (id INTEGER PRIMARY KEY, note TEXT)")
        .unwrap();
    let words = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november",
    ];
    let mut rng: u64 = 7;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for i in 0..100_000 {
        let k = 6 + (i % 5);
        let mut note = format!("row {i}");
        for j in 0..k {
            note.push(' ');
            note.push_str(words[(next() as usize) % words.len()]);
        }
        db.execute(format!("INSERT INTO ev VALUES ({}, '{}')", i + 1, note).as_str())
            .unwrap();
    }
    db.execute("CREATE TEXT INDEX ev_note ON ev(note)").unwrap();

    let warm = |sql: &str| {
        let _ = db.query(sql);
    };
    warm("SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10");
    warm(
        "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') ORDER BY BM25_SCORE() DESC LIMIT 10",
    );

    for (label, sql, iters) in [
        ("unranked LIMIT 10", "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10", 200),
        ("ranked LIMIT 10  ", "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') ORDER BY BM25_SCORE() DESC LIMIT 10", 200),
        ("count(*)         ", "SELECT COUNT(*) FROM ev WHERE MATCH(note, 'charlie delta')", 50),
    ] {
        let t0 = Instant::now();
        for _ in 0..iters { let _ = db.query(sql); }
        println!("{label}: {:?}/iter", t0.elapsed() / iters);
    }
    let n = match &db
        .query("SELECT COUNT(*) FROM ev WHERE MATCH(note, 'charlie delta')")
        .unwrap()[0][0]
    {
        Value::Integer(n) => *n,
        v => panic!("{v:?}"),
    };
    println!("matches: {n}");
}
