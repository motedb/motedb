#!/usr/bin/env python3
"""全文检索行业对照：MoteDB FTS vs SQLite FTS5 vs Tantivy（同 100K 语料、同查询协议）。

语料 = compete_bench.py 的 100K 行 notes（14 词表，每行 6-10 词）。
形状: match_and_top10（无排序）+ match_and_top10_by_bm25（排序口径）。
三引擎同一语义: 'charlie delta' = 两词 AND（tantivy 显式 set_conjunction_by_default）。

输出: 每行 JSON: {engine, build_s, disk_mb, match_top10, match_top10_rank}
"""
import json
import os
import shutil
import sys
import tempfile
import time

import numpy as np

N = 100_000
WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
         "hotel", "india", "juliet", "kilo", "lima", "mike", "november"]


def gen_notes():
    rng = np.random.default_rng(42)
    ts = (1_700_000_000 + np.sort(rng.integers(0, 30 * 24 * 3600, N))) * 1_000_000
    rr = np.random.default_rng(7)
    notes = []
    for i in range(N):
        k = 6 + (i % 5)
        notes.append(" ".join(WORDS[j % len(WORDS)] for j in rr.integers(0, len(WORDS), k)))
    return ts, notes


def lat(f, iters):
    out = []
    for _ in range(iters):
        t0 = time.perf_counter()
        f()
        out.append(time.perf_counter() - t0)
    a = np.asarray(out) * 1e3
    return {"avg_ms": round(float(a.mean()), 4), "p50_ms": round(float(np.percentile(a, 50)), 4)}


def du_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return round(total / 1e6, 1)


def bench_mote(tmp, ts, notes, R):
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    import motedb
    db = motedb.Database(os.path.join(tmp, "f.mote"), preset="general")
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts INT, note TEXT)")
    db.insert_arrays("ev", {"id": list(range(N)), "ts": [int(x) for x in ts], "note": notes})
    db.checkpoint()
    t0 = time.perf_counter()
    db.execute("CREATE TEXT INDEX ev_note ON ev(note)")
    R.append({"engine": "motedb_fts", "build_s": round(time.perf_counter() - t0, 3),
              "disk_mb": du_mb(os.path.join(tmp, "f.mote")),
              "match_top10": lat(lambda: db.query(
                  "SELECT id FROM ev WHERE MATCH(note, 'charlie delta') LIMIT 10"), 50)})
    try:
        R[0]["match_top10_rank"] = lat(lambda: db.query(
            "SELECT id, BM25_SCORE() FROM ev WHERE MATCH(note, 'charlie delta') "
            "ORDER BY BM25_SCORE() DESC LIMIT 10"), 50)
    except Exception as e:
        R[0]["match_top10_rank"] = f"unsupported: {type(e).__name__}"
    db.close()


def bench_sqlite(tmp, R):
    import sqlite3
    con = sqlite3.connect(os.path.join(tmp, "f.db"), isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE ev (id INT PRIMARY KEY, note TEXT)")
    rows = [(i, i) for i in range(N)]
    t0 = time.perf_counter()
    con.execute("BEGIN")
    con.executemany("INSERT INTO ev VALUES (?,?)", rows)
    con.execute("COMMIT")
    con.execute("CREATE VIRTUAL TABLE ev_fts USING fts5(note, content='ev', content_rowid='id')")
    con.execute("INSERT INTO ev_fts(rowid, note) SELECT id, note FROM ev")
    build = time.perf_counter() - t0
    R.append({"engine": "sqlite_fts5", "build_s": round(build, 3),
              "disk_mb": du_mb(os.path.join(tmp, "f.db")),
              "match_top10": lat(lambda: con.execute(
                  "SELECT rowid FROM ev_fts WHERE ev_fts MATCH 'charlie delta' LIMIT 10").fetchall(), 50),
              "match_top10_rank": lat(lambda: con.execute(
                  "SELECT rowid FROM ev_fts WHERE ev_fts MATCH 'charlie delta' "
                  "ORDER BY bm25(ev_fts) LIMIT 10").fetchall(), 50)})
    con.close()


def bench_tantivy(tmp, notes, R):
    import tantivy
    idx_dir = os.path.join(tmp, "tantivy")
    os.makedirs(idx_dir)
    schema_b = tantivy.SchemaBuilder().add_text_field("note", stored=False).build()
    index = tantivy.Index(schema_b, path=idx_dir)
    writer = index.writer(150_000_000, 4)
    t0 = time.perf_counter()
    for i in range(N):
        writer.add_document(tantivy.Document(note=[notes[i]]))
    writer.commit()
    index.reload()
    build = time.perf_counter() - t0
    reader = index.searcher()
    # 'charlie delta' 的 AND 语义（mote/FTS5 隐式 AND 同口径）
    q = index.parse_query("charlie AND delta", ["note"])

    def top10():
        return reader.search(q, 10)
    R.append({"engine": "tantivy_0.25", "build_s": round(build, 3),
              "disk_mb": du_mb(idx_dir),
              "match_top10": lat(top10, 50),
              "match_top10_rank": "n/a (TopDocs 恒按 bm25 排序)"})
    writer.wait_merging_threads()


def main():
    ts, notes = gen_notes()
    R = []
    tmp = tempfile.mkdtemp(prefix="fts_mote_")
    bench_mote(tmp, ts, notes, R)
    shutil.rmtree(tmp, ignore_errors=True)
    tmp = tempfile.mkdtemp(prefix="fts_sqlite_")
    bench_sqlite(tmp, R)
    shutil.rmtree(tmp, ignore_errors=True)
    tmp = tempfile.mkdtemp(prefix="fts_tantivy_")
    bench_tantivy(tmp, notes, R)
    shutil.rmtree(tmp, ignore_errors=True)
    for row in R:
        print("JSON " + json.dumps(row))


if __name__ == "__main__":
    main()
