//! 引号向量字面量统一:INSERT/UPDATE 接受 '[0.1, 0.2, …]'(SQL lexer 无
//! 方括号字面量 token,带引号形式到达写入路径时是 TEXT;KNN 谓词早已解析
//! 此拼写,写入不解析则同一查询风格"能排不能写")。维度不匹配仍须被
//! validate_row 拒绝并给出清晰错误。
use motedb::Database;
use tempfile::TempDir;

#[test]
fn quoted_vector_literal_inserts_and_queries() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("qv.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, e VECTOR(3), note TEXT)")
        .unwrap();

    // Spelled with quotes (spaces + no spaces) and bare — all must land as
    // real vectors.
    db.execute("INSERT INTO t VALUES (1, '[0.1, 0.2, 0.3]', 'q-spaced')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, '[0.4,0.5,0.6]', 'q-tight')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, [0.7, 0.8, 0.9], 'bare')")
        .unwrap();

    // Stored as real vectors: quoted KNN sorts correctly over all three.
    let r = db
        .execute("SELECT id FROM t ORDER BY e <-> '[0.1, 0.2, 0.3]' LIMIT 3")
        .unwrap()
        .materialize()
        .unwrap();
    let ids: Vec<i64> = r
        .select_rows()
        .unwrap()
        .1
        .iter()
        .map(|r| match &r[0] {
            motedb::types::Value::Integer(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "quoted inserts order by distance");

    // UPDATE takes the quoted form too.
    db.execute("UPDATE t SET e = '[0.1, 0.1, 0.1]' WHERE id = 3")
        .unwrap();
    let r = db
        .execute("SELECT id FROM t ORDER BY e <-> '[0.1, 0.1, 0.1]' LIMIT 1")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        r.select_rows().unwrap().1[0][0],
        motedb::types::Value::Integer(3),
        "updated row is now the nearest"
    );

    // Dimension mismatch must stay a hard error (not silently coerced).
    let err = db
        .execute("INSERT INTO t VALUES (4, '[0.1, 0.2]', 'bad-dim')")
        .err()
        .expect("wrong-dimension quoted vector must be rejected");
    assert!(
        err.to_string().contains("Tensor"),
        "error names the expected type: {err}"
    );

    // Non-vector text in a vector column is not hijacked by the parser.
    let err = db
        .execute("INSERT INTO t VALUES (5, 'not-a-vector', 'plain-text')")
        .err()
        .expect("plain text must not coerce");
    assert!(
        err.to_string().contains("Tensor"),
        "error names the expected type: {err}"
    );
}

#[test]
fn quoted_vector_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("qv2.mote");
    {
        let db = Database::create(&db_path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, e VECTOR(4))")
            .unwrap();
        for i in 0..20i64 {
            db.execute(&format!(
                "INSERT INTO t (e) VALUES ('[{i}.0, 0.5, 0.25, 0.125]')"
            ))
            .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
    }
    let db = Database::open(&db_path).unwrap();
    // Nearest-first over the persisted quoted vectors (i=0 is the exact match).
    let r = db
        .execute("SELECT id FROM t ORDER BY e <-> '[0.0, 0.5, 0.25, 0.125]' LIMIT 1")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        r.select_rows().unwrap().1[0][0],
        motedb::types::Value::Integer(1),
        "quoted vectors persisted and queryable after reopen"
    );
    let n = db
        .execute("SELECT COUNT(*) FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        n.select_rows().unwrap().1[0][0],
        motedb::types::Value::Integer(20)
    );
}
