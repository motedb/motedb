//! Parquet interop demo — proves the files MoteDB writes are readable by the
//! ecosystem (pyarrow/pandas/DuckDB/LeRobot) and vice versa.
//!
//! ```bash
//! # 1) MoteDB → parquet (then read it from Python with pyarrow)
//! cargo run --example parquet_interop -- /tmp/interop.mote write /tmp/out.parquet
//! python -c "import pyarrow.parquet as pq; t=pq.read_table('/tmp/out.parquet'); print(t.schema); print(t.num_rows)"
//!
//! # 2) parquet → MoteDB (file written by pyarrow)
//! python -c "import pyarrow as pa, pyarrow.parquet as pq; pq.write_table(pa.table({'id':pa.array(range(100)), 'emb': pa.array([[float(i)]*4 for i in range(100)], type=pa.list_(pa.float32(), 4))}), '/tmp/in.parquet')"
//! cargo run --example parquet_interop -- /tmp/interop2.mote read /tmp/in.parquet
//! ```
use motedb::Database;

fn main() -> motedb::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: parquet_interop <db> <write|read> <parquet-path>");
        std::process::exit(2);
    }
    let (db_path, mode, pq) = (&args[1], args[2].as_str(), &args[3]);
    let db = Database::create(db_path)?;

    match mode {
        "write" => {
            db.execute(
                "CREATE TABLE IF NOT EXISTS demo (\
                 id INT PRIMARY KEY, ts TIMESTAMP, name TEXT, score FLOAT, \
                 active BOOLEAN, emb VECTOR(4), payload BLOB)",
            )?;
            for i in 0..1000i64 {
                db.execute_prepared(
                    "INSERT INTO demo VALUES (?, ?, ?, ?, ?, ?, ?)",
                    vec![
                        motedb::types::Value::Integer(i),
                        motedb::types::Value::Timestamp(motedb::types::Timestamp::from_micros(
                            i * 1_000_000,
                        )),
                        if i % 9 == 0 {
                            motedb::types::Value::Null
                        } else {
                            motedb::types::Value::Text(motedb::types::ArcString::from(format!(
                                "row-{i}"
                            )))
                        },
                        motedb::types::Value::Float(i as f64 * 0.25),
                        motedb::types::Value::Bool(i % 2 == 0),
                        motedb::types::Value::Vector(motedb::types::ArcVec::new(vec![
                            i as f32, 1.0, -1.0, 0.5,
                        ])),
                        motedb::types::Value::Blob(motedb::types::ArcBytes::new(vec![
                            0xde, 0xad, 0xbe, i as u8,
                        ])),
                    ],
                )?;
            }
            db.checkpoint()?;
            let n = db
                .execute(&format!("EXPORT TABLE demo TO '{pq}'"))?
                .materialize()?;
            println!("exported: {n:?}");
        }
        "read" => {
            db.execute(
                "CREATE TABLE IF NOT EXISTS imported (\
                        id INT PRIMARY KEY, emb VECTOR(4))",
            )?;
            let n = db
                .execute(&format!("IMPORT PARQUET '{pq}' INTO imported"))?
                .materialize()?;
            println!("imported: {n:?}");
            let count = db.query("SELECT COUNT(*) FROM imported")?;
            println!("rows now: {:?}", count);
        }
        other => {
            eprintln!("unknown mode {other:?} — expected write|read");
            std::process::exit(2);
        }
    }
    Ok(())
}
