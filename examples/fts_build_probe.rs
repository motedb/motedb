use motedb::index::tokenizers::Tokenizer as _;
use std::time::Instant;

fn main() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut idx = motedb::index::TextFTSIndex::new(dir.path().join("t")).unwrap();
    let n = 100_000u64;
    let docs: Vec<(u64, String)> = (0..n)
        .map(|i| {
            (
                i,
                format!(
                    "record alpha{} beta{} gamma{} delta{}",
                    i % 10,
                    i % 101,
                    i % 1009,
                    i
                ),
            )
        })
        .collect();
    let refs: Vec<(u64, &str)> = docs.iter().map(|(i, s)| (*i, s.as_str())).collect();

    // one giant batch_insert (what the executor's backfill feeds)
    let t0 = Instant::now();
    idx.batch_insert(&refs).unwrap();
    println!("batch_insert 100K: {:.2}s", t0.elapsed().as_secs_f64());

    let t0 = Instant::now();
    idx.flush().unwrap();
    println!("final flush: {:.2}s", t0.elapsed().as_secs_f64());
    println!("search alpha3: {:?}", idx.search("alpha3").unwrap().len());

    // tokenization-only cost
    let tok = motedb::index::tokenizers::WhitespaceTokenizer::default();
    let t0 = Instant::now();
    let mut total = 0usize;
    for (_, s) in docs.iter() {
        total += tok.tokenize(s).len();
    }
    println!(
        "tokenize-only 100K: {:.2}s ({total} tokens)",
        t0.elapsed().as_secs_f64()
    );
}
