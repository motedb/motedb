//! Text-search set-equality probe: engine MATCH vs reference OR-token sets
//! on N synthetic docs (5 words from a 10-word vocab), at a size that
//! triggers the FTS auto-flush mid-backfill.
use motedb::{DBConfig, Database};

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| (1..=64).contains(&s.len()))
        .map(|s| s.to_string())
        .collect()
}

fn main() -> motedb::Result<()> {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let dir = tempfile::TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing())?;
    db.execute("CREATE TABLE d (id INT PRIMARY KEY, content TEXT)")?;
    let mut x = 12345u64;
    let mut word = || {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        (b"abcdefghij"[(x >> 33) as usize % 10] as char).to_string()
    };
    let mut docs: Vec<Vec<String>> = Vec::with_capacity(n);
    for _ in 0..n {
        docs.push((0..5).map(|_| word()).collect());
    }
    for s in (0..n).step_by(2000) {
        let vals: Vec<String> = docs[s..(s + 2000).min(n)]
            .iter()
            .enumerate()
            .map(|(j, w)| format!("({}, '{}')", s + j + 1, w.join(" ")))
            .collect();
        db.execute(&format!("INSERT INTO d VALUES {}", vals.join(",")))?;
    }
    db.execute("CREATE TEXT INDEX d_content ON d(content)")?;

    // doc id → token set
    let mut sets: Vec<std::collections::HashSet<String>> =
        docs.iter().map(|w| w.iter().cloned().collect()).collect();
    let vocab: Vec<String> = "abcdefghij".chars().map(|c| c.to_string()).collect();

    let mut bad = 0;
    for term in &vocab {
        let engine: std::collections::HashSet<u64> = db
            .text_search_ranked("d_content", term, 1_000_000)?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let truth: std::collections::HashSet<u64> = sets
            .iter()
            .enumerate()
            .filter(|(_, s)| s.contains(term))
            .map(|(i, _)| i as u64 + 1)
            .collect();
        if engine != truth {
            bad += 1;
            eprintln!(
                "term {term}: engine {} truth {} extra {} missing {}",
                engine.len(),
                truth.len(),
                engine.difference(&truth).count(),
                truth.difference(&engine).count()
            );
        }
    }
    // two-term OR
    for (a, b) in [("a", "b"), ("c", "j"), ("e", "f")] {
        let engine: std::collections::HashSet<u64> = db
            .text_search_ranked("d_content", &format!("{a} {b}"), 1_000_000)?
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let truth: std::collections::HashSet<u64> = sets
            .iter()
            .enumerate()
            .filter(|(_, s)| s.contains(a) || s.contains(b))
            .map(|(i, _)| i as u64 + 1)
            .collect();
        if engine != truth {
            bad += 1;
            eprintln!(
                "terms {a} {b}: engine {} truth {} missing {}",
                engine.len(),
                truth.len(),
                truth.difference(&engine).count()
            );
        }
    }
    // ranked top-k includes both halves' best docs
    let ranked = db.text_search_ranked("d_content", "a b", 10)?;
    eprintln!("ranked 'a b' top-10: {:?}", &ranked[..ranked.len().min(10)]);
    let _ = &mut sets;
    eprintln!("set divergences: {bad}/13");
    Ok(())
}
