//! BLOB type end-to-end: hex literals, non-UTF-8 payloads, size cap, NULL,
//! equality, LENGTH(bytes), durability across flush/reopen and mixed-schema
//! tables (BLOB + VECTOR + TEXT in one row).
use motedb::{types::Value, Database};
use tempfile::TempDir;

fn one(db: &Database, sql: &str) -> Value {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows[0][0].clone()
}

fn as_blob(v: &Value) -> Vec<u8> {
    match v {
        Value::Blob(b) => b.as_slice().to_vec(),
        other => panic!("expected blob, got {other:?}"),
    }
}

#[test]
fn blob_roundtrip_and_semantics() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("blob.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute(
            "CREATE TABLE f (id INTEGER PRIMARY KEY, name TEXT, payload BLOB, emb VECTOR(4))",
        )
        .unwrap();
        // Hex literal, including non-UTF-8 bytes (0xff 0xfe is invalid UTF-8).
        db.execute("INSERT INTO f VALUES (1, 'frame', X'00fffe0a0b', [0.0, 0.0, 0.0, 0.0])")
            .unwrap_or_else(|e| panic!("insert1 failed: {e}"));
        // Zero-length blob and NULL are distinct.
        db.execute("INSERT INTO f VALUES (2, 'empty', X'', [1.0, 1.0, 1.0, 1.0])")
            .unwrap();
        db.execute("INSERT INTO f VALUES (3, 'null', NULL, [2.0, 2.0, 2.0, 2.0])")
            .unwrap();
        // Parameterized-ish via a big literal: 4KB of 0xAB.
        let big = format!("X'{}'", "ab".repeat(4096));
        db.execute(&format!(
            "INSERT INTO f VALUES (4, 'big', {big}, [3.0, 3.0, 3.0, 3.0])"
        ))
        .unwrap();

        assert_eq!(
            as_blob(&one(&db, "SELECT payload FROM f WHERE id = 1")),
            vec![0x00, 0xff, 0xfe, 0x0a, 0x0b]
        );
        assert_eq!(
            as_blob(&one(&db, "SELECT payload FROM f WHERE id = 2")),
            Vec::<u8>::new()
        );
        assert!(matches!(
            one(&db, "SELECT payload FROM f WHERE id = 3"),
            Value::Null
        ));
        // 🔬 Experimental (v0.13.0): scalar functions over blob columns work
        // on some executor paths but are order/shape dependent — see the
        // CHANGELOG known-gaps note. Length via SQL is NOT asserted here.
        // Equality by bytes.
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload = X'00fffe0a0b'"),
            Value::Integer(1)
        );
        // NULL blob ≠ empty blob.
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload IS NULL"),
            Value::Integer(1)
        );
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload = X''"),
            Value::Integer(1)
        );
        // Odd hex / non-hex literals are rejected.
        assert!(db
            .execute("INSERT INTO f VALUES (9, 'x', X'0', [0,0,0,0])")
            .is_err());
        assert!(db
            .execute("INSERT INTO f VALUES (9, 'x', X'zz', [0,0,0,0])")
            .is_err());
        // WHERE equality on the blob column (compiled predicate path).
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload = X'00fffe0a0b'"),
            Value::Integer(1)
        );
        db.checkpoint().unwrap();
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload = X'00fffe0a0b'"),
            Value::Integer(1)
        );
    } // clean close

    // Reopen: durability + exact bytes (write buffer AND flushed segment paths).
    let db = Database::open(&path).unwrap();
    assert_eq!(
        as_blob(&one(&db, "SELECT payload FROM f WHERE id = 1")),
        vec![0x00, 0xff, 0xfe, 0x0a, 0x0b]
    );
    assert_eq!(
        one(&db, "SELECT COUNT(*) FROM f WHERE payload = X'00fffe0a0b'"),
        Value::Integer(1)
    );
    assert!(matches!(
        one(&db, "SELECT payload FROM f WHERE id = 3"),
        Value::Null
    ));
}

#[test]
fn blob_size_cap_rejected() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("cap.mote")).unwrap();
    db.execute("CREATE TABLE f (id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    let over = format!("X'{}'", "ab".repeat(16 * 1024 * 1024 / 2 + 2));
    let err = match db.execute(&format!("INSERT INTO f VALUES (1, {over})")) {
        Err(e) => e,
        Ok(_) => panic!("oversized blob must be rejected"),
    };
    assert!(
        err.to_string().contains("16 MiB"),
        "expected size-cap error, got: {err}"
    );
}

#[test]
fn blob_unflushed_buffer_reads() {
    // Rows living in the write buffer (no checkpoint) must read back as
    // BLOB, not lossy text — the buffered decode path has its own tag arm.
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("buf.mote")).unwrap();
    db.execute("CREATE TABLE f (id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    db.execute("INSERT INTO f VALUES (2, X'deadbeef00')")
        .unwrap();
    assert_eq!(
        as_blob(&one(&db, "SELECT payload FROM f WHERE id = 2")),
        vec![0xde, 0xad, 0xbe, 0xef, 0x00]
    );
    // UPDATE rewrites the buffered row; blob survives the newest-wins dedup.
    db.execute("UPDATE f SET payload = X'01' WHERE id = 2")
        .unwrap();
    assert_eq!(
        as_blob(&one(&db, "SELECT payload FROM f WHERE id = 2")),
        vec![0x01]
    );
}

#[test]
fn blob_core_three_column_shape() {
    // 3-column shape: core blob semantics without a VECTOR column.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bx.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE f (id INTEGER PRIMARY KEY, name TEXT, payload BLOB)")
            .unwrap();
        db.execute("INSERT INTO f VALUES (1, 'a', X'0a0b0c')")
            .unwrap();
        assert_eq!(
            as_blob(&one(&db, "SELECT payload FROM f WHERE id = 1")),
            vec![0x0a, 0x0b, 0x0c]
        );
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload = X'0a0b0c'"),
            Value::Integer(1)
        );
        db.checkpoint().unwrap();
        assert_eq!(
            as_blob(&one(&db, "SELECT payload FROM f WHERE id = 1")),
            vec![0x0a, 0x0b, 0x0c]
        );
        assert_eq!(
            one(&db, "SELECT COUNT(*) FROM f WHERE payload = X'0a0b0c'"),
            Value::Integer(1)
        );
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        as_blob(&one(&db, "SELECT payload FROM f WHERE id = 1")),
        vec![0x0a, 0x0b, 0x0c]
    );
    // Known v0.13.0 gaps (CHANGELOG): scalar functions over blob columns
    // (SELECT/WHERE LENGTH(payload)) are experimental — path-dependent.
    // Bare projection + equality + params are the supported surface.
}

#[test]
fn blob_under_int_where_and_remaining_gap() {
    // Root-caused & FIXED (v0.15): WHERE <int col> <op> <literal> routes
    // through scan_i64_filtered_limit, which (a) didn't load Blob columns
    // into its text-segment set (ColumnTypeTag::Text-only gate) and (b) had
    // no ColumnType::Blob emit arm — blob columns decoded as NULL under any
    // int-filtered query (bare or inside functions).
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("g.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, b BLOB)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello', X'0a0b0c0d')")
            .unwrap();
        db.execute("INSERT INTO t VALUES (2, 'world', X'0102')")
            .unwrap();

        // Buffered path: bare + function over blob under int WHERE.
        let r = db
            .execute("SELECT b FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert!(
            matches!(rows[0][0], Value::Blob(_)),
            "bare blob under int-eq WHERE"
        );
        let r = db
            .execute("SELECT LENGTH(b) FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert_eq!(rows[0][0], Value::Integer(4), "LENGTH under int-eq WHERE");
        let r = db
            .execute("SELECT LENGTH(b) FROM t WHERE id > 0")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert_eq!(
            rows[0][0],
            Value::Integer(4),
            "LENGTH under int-range WHERE"
        );
        let r = db
            .execute("SELECT b, LENGTH(b) FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert!(matches!(rows[0][0], Value::Blob(_)), "mixed bare+computed");
        assert_eq!(rows[0][1], Value::Integer(4), "computed alongside bare");

        // Persisted path.
        db.checkpoint().unwrap();
        let r = db
            .execute("SELECT LENGTH(b) FROM t WHERE id = 1")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert_eq!(rows[0][0], Value::Integer(4), "post-checkpoint");

        // Working surfaces stay working.
        let r = db
            .execute("SELECT COUNT(*) FROM t WHERE LENGTH(b) = 4")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert_eq!(rows[0][0], Value::Integer(1), "LENGTH in WHERE");
        let r = db
            .execute("SELECT id FROM t ORDER BY b ASC")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        assert_eq!(
            rows[0][0],
            Value::Integer(2),
            "ORDER BY blob (2-byte first)"
        );
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = db
        .execute("SELECT LENGTH(b) FROM t WHERE id = 2")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert_eq!(
        rows[0][0],
        Value::Integer(2),
        "reopen int WHERE + LENGTH(blob)"
    );
    db.close().unwrap();
}
