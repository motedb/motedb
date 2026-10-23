use motedb::Database;
use motedb::types::Value;
fn main() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("p.mote")).unwrap();
    db.execute("CREATE TABLE m (id INT PRIMARY KEY, g TEXT, v INT)").unwrap();
    // two partitions 'a'/'b' × v=10..50 each
    for id in 1..=10i64 {
        let g = if id <= 5 { "a" } else { "b" };
        db.execute(&format!("INSERT INTO m VALUES ({id}, '{g}', {})", (id - if id > 5 { 5 } else { 0 }) * 10))
            .unwrap();
    }
    let q = |expr: &str| -> Vec<String> {
        let sql = format!("SELECT id, {expr} AS w FROM m ORDER BY id");
        let r = db.execute(&sql).unwrap().materialize().unwrap();
        r.select_rows().unwrap().1.iter().map(|r| match &r[1] {
            Value::Integer(i) => i.to_string(), Value::Null => "NULL".into(), o => format!("{o:?}"),
        }).collect()
    };
    // PARTITION BY g + ROWS 1 PRECEDING..1 FOLLOWING: per-partition rolling 3.
    assert_eq!(q("SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"),
        vec!["30","60","90","120","90","30","60","90","120","90"], "partitioned rolling");
    // PARTITION + RANGE value interval.
    assert_eq!(q("SUM(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING)"),
        vec!["30","60","90","120","90","30","60","90","120","90"], "partitioned range interval");
    // LAG inside partitions.
    assert_eq!(q("LAG(v) OVER (PARTITION BY g ORDER BY v)"),
        vec!["NULL","10","20","30","40","NULL","10","20","30","40"], "partitioned lag");
    // DISTINCT + frame.
    assert_eq!(q("COUNT(DISTINCT v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"),
        vec!["5","4","3","2","1","5","4","3","2","1"], "distinct frame");
    println!("WINDOW PARTITION×FRAME ALL OK");
}
