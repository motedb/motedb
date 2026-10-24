//! `LIKE … ESCAPE 'c'` — 标准转义子句 (2026-10-24 bug 猎杀轮补齐)。
//!
//! 之前不支持且 `$` 等字符触发离奇解析错误; `%`/`_` 无法按字面匹配。
//! 转义语义: 转义符的下一字符按字面匹配 (含 % _ 与转义符自身);
//! 尾随悬空转义符按字面转义符匹配 (宽松, SQLite 报错 — 见 docs/03)。
use motedb::Database;
use tempfile::TempDir;

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    r.select_rows()
        .unwrap()
        .1
        .iter()
        .map(|r| match &r[0] {
            motedb::types::Value::Integer(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect()
}

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("le.mote")).unwrap();
    db.execute("CREATE TABLE s (id INT PRIMARY KEY, t TEXT)")
        .unwrap();
    for (id, t) in [
        (1, "100% sure"),
        (2, "a_b"),
        (3, "axb"),
        (4, "a$b"),
        (5, "50%_off"),
        (6, "plain"),
    ] {
        db.execute(&format!("INSERT INTO s VALUES ({id}, '{t}')"))
            .unwrap();
    }
    (dir, db)
}

#[test]
fn escape_matches_literal_percent_and_underscore() {
    let (_dir, db) = setup();
    // '%' after escape is literal: only "100% sure".
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM s WHERE t LIKE '100$% %' ESCAPE '$' ORDER BY id"
        ),
        vec![1],
        "escaped % matches literally"
    );
    // '_' after escape is literal: only "a_b", not "axb".
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM s WHERE t LIKE 'a$_b' ESCAPE '$' ORDER BY id"
        ),
        vec![2],
        "escaped _ matches literally"
    );
    // The escape char itself, doubled: matches literal '$'.
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM s WHERE t LIKE 'a$$b' ESCAPE '$' ORDER BY id"
        ),
        vec![4],
        "doubled escape matches the escape char literally"
    );
    // Combined: literal % then literal _.
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM s WHERE t LIKE '50$%$_off' ESCAPE '$' ORDER BY id"
        ),
        vec![5],
        "escaped % and _ together"
    );
}

#[test]
fn escape_default_is_none_and_negation_composes() {
    let (_dir, db) = setup();
    // Without ESCAPE the old semantics are unchanged.
    assert_eq!(
        ids(&db, "SELECT id FROM s WHERE t LIKE 'a_b' ORDER BY id"),
        vec![2, 3, 4],
        "unescaped _ still wildcards (also 'a$b' — $ is a plain char without ESCAPE)"
    );
    // NOT LIKE with ESCAPE.
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM s WHERE t NOT LIKE 'a$_b' ESCAPE '$' AND id <= 4 ORDER BY id"
        ),
        vec![1, 3, 4],
        "NOT LIKE + ESCAPE"
    );
}

#[test]
fn escape_syntax_errors() {
    let (_dir, db) = setup();
    for (sql, needle) in [
        (
            "SELECT id FROM s WHERE t LIKE 'a_b' ESCAPE '$$'",
            "SINGLE character",
        ),
        (
            "SELECT id FROM s WHERE t LIKE 'a_b' ESCAPE",
            "string literal",
        ),
    ] {
        let err = db
            .execute(sql)
            .err()
            .unwrap_or_else(|| panic!("expected error: {sql}"));
        assert!(
            err.to_string().contains(needle),
            "{sql} → {err} (want '{needle}')"
        );
    }
}

#[test]
fn escaped_pattern_survives_reopen_and_indexed_path() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create(dir.path().join("le2.mote")).unwrap();
        db.execute("CREATE TABLE s (id INT PRIMARY KEY, t TEXT)")
            .unwrap();
        for i in 0..500 {
            db.execute(&format!("INSERT INTO s VALUES ({i}, 'item_{i}%tag')"))
                .unwrap();
        }
        db.execute("CREATE INDEX si ON s (t)").unwrap();
        db.execute("CHECKPOINT").unwrap();
    }
    let db = Database::open(dir.path().join("le2.mote")).unwrap();
    // Strings are 'item_{i}%tag'. The literal prefix "item_9%" (escaped %)
    // matches ONLY i=9 — "item_9%tag". i=90 is "item_90%tag" (next char '0',
    // not '%'). Unescaped wildcard form matches every i starting with 9.
    // LIKE is a FULL-string match: literal prefix needs a trailing (unescaped)
    // wildcard to act as a prefix filter. '$%%' = literal '%' + wildcard '%'.
    let got = ids(
        &db,
        "SELECT COUNT(*) FROM s WHERE t LIKE 'item_9$%%' ESCAPE '$'",
    );
    assert_eq!(got[0], 1, "literal item_9% prefix matches only i=9");
    // Unescaped % still wildcards: i in 0..500 whose text starts "item_9"
    // = {9, 90..99} = 11 rows.
    let wide = ids(&db, "SELECT COUNT(*) FROM s WHERE t LIKE 'item_9%'");
    assert_eq!(wide[0], 11, "unescaped wildcard form unchanged");
}
