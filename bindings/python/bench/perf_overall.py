#!/usr/bin/env python3
"""
Overall performance benchmark: core SQL + all four modalities in one run.

Corpora are reused from the accuracy evals (deterministic, cached):
  * vector   190K x 384 all-nli embeddings (safely under EXACT_SCAN_MAX_ROWS,
             so `ORDER BY emb <-> ?` is the SIMD exact path; DiskANN build
             takes ~72 min and is NOT rebuilt here — its numbers are in README)
  * text     100K real all-nli sentences + TEXT index
  * spatial  200K 3D points + OCTREE index
  * time     1M rows, 64 sensors, TIMESERIES table

Measured per section: load throughput, then query latency (avg / p50 / p95)
over fresh, warmed runs. Results also dumped to perf_results.json.

Usage: python3 perf_overall.py [--ts-rows 1000000] [--vq 30] [--skip-load]
"""
import argparse
import glob
import json
import os
import shutil
import tempfile
import time

import numpy as np

CACHE = os.path.expanduser("~/.cache/motedb_eval")
DIM = 384


def log(msg=""):
    print(msg, flush=True)


def lat_stats(lat):
    lat = np.asarray(lat) * 1e3
    return {"avg_ms": float(lat.mean()), "p50_ms": float(np.percentile(lat, 50)),
            "p95_ms": float(np.percentile(lat, 95))}


def lat_str(lat):
    s = lat_stats(lat)
    return f"avg {s['avg_ms']:.3f}ms  p50 {s['p50_ms']:.3f}  p95 {s['p95_ms']:.3f}"


def load_corpus():
    d2 = np.fromfile(os.path.join(CACHE, "nli-corpus.f32"), dtype=np.float32)
    d2 = d2.reshape(-1, DIM)
    q = np.fromfile(os.path.join(CACHE, "nli-queries.f32"), dtype=np.float32)
    q = q.reshape(-1, DIM)
    return d2, q


def load_sentences(n):
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("HF_DATASETS_OFFLINE", "1")
    from datasets import load_dataset
    pairs = load_dataset("sentence-transformers/all-nli", "pair")["train"]
    seen, docs = set(), []
    for r in pairs:
        for s in (r["anchor"], r["positive"]):
            if s not in seen:
                seen.add(s)
                docs.append(s)
                if len(docs) >= n:
                    break
        if len(docs) >= n:
            break
    return docs


def esc(s):
    return s.replace("'", "''")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ts-rows", type=int, default=1_000_000)
    ap.add_argument("--vec-rows", type=int, default=190_000)
    ap.add_argument("--text-rows", type=int, default=100_000)
    ap.add_argument("--spat-rows", type=int, default=200_000)
    ap.add_argument("--vq", type=int, default=30, help="vector queries")
    ap.add_argument("--out", default=os.path.join(CACHE, "perf_results.json"))
    args = ap.parse_args()

    import motedb

    rng = np.random.default_rng(42)
    results = {"config": vars(args)}
    tmp = tempfile.mkdtemp(prefix="motedb_perf_")
    path = os.path.join(tmp, "perf.mote")

    D, Q = load_corpus()
    D = D[: args.vec_rows]
    log(f"corpus: {len(D):,} x {DIM} vectors, {args.ts_rows:,} ts rows planned")

    # Bulk loads run under the `edge` preset (the documented fast write path;
    # the vector table alone is ~12 min via a general-preset transaction).
    # Queries then run after close + reopen under `general`.
    db = motedb.Database(path, preset="edge")

    # ================================================================ loads
    log("\n=== LOAD THROUGHPUT (preset=edge) ===")

    # --- time-series: 1M rows, multi-row VALUES (autocommit group commit)
    n_sensors = 64
    per = args.ts_rows // n_sensors
    t0 = 1_700_000_000_000_000  # micros
    sid = np.repeat(np.arange(n_sensors), per).astype(np.int64)
    tsi = t0 + np.tile(np.arange(per) * 1_000_000, n_sensors) + \
        rng.integers(0, 200_000, size=n_sensors * per)
    val = np.round(10.0 + sid + 3.0 * np.sin(np.arange(n_sensors * per) / 97.0), 4)
    order = np.argsort(tsi)
    sid, tsi, val = sid[order], tsi[order], val[order]

    db.execute("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)")
    t_start = time.perf_counter()
    for s in range(0, len(tsi), 5000):
        chunk = slice(s, s + 5000)
        vals = ",".join(
            "(%d,%d,%.4f)" % (tsi[i], sid[i], val[i]) for i in range(*chunk.indices(len(tsi))))
        db.execute("INSERT INTO m VALUES " + vals)
    dt = time.perf_counter() - t_start
    results["load_ts_rows_per_s"] = len(tsi) / dt
    log(f"  time-series {len(tsi):,} rows: {dt:.1f}s  -> {len(tsi) / dt:,.0f} rows/s")

    # --- vector: executemany, committed in 20K-row transactions. One giant
    # transaction makes commit_transaction spend minutes inside
    # VersionStore::evict_if_needed (dashmap full-scan per commit) — measured
    # separately, see perf notes.
    db.execute(f"CREATE TABLE vecs (id INT PRIMARY KEY, emb VECTOR({DIM}))")
    t_start = time.perf_counter()
    sql = "INSERT INTO vecs (id, emb) VALUES (?, ?)"
    tx = None
    for s in range(0, len(D), 2000):
        if s % 20_000 == 0:
            if tx is not None:
                db.commit(tx)
            tx = db.begin()
        chunk = D[s:s + 2000]
        db.executemany(sql, [[s + j + 1, chunk[j].tolist()] for j in range(len(chunk))])
    db.commit(tx)
    dt = time.perf_counter() - t_start
    results["load_vec_rows_per_s"] = len(D) / dt
    log(f"  vector {len(D):,} x {DIM} (txn executemany): {dt:.1f}s  -> {len(D) / dt:,.0f} rows/s")

    # --- text: 100K real sentences, then TEXT index build
    docs = load_sentences(args.text_rows)
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, cat TEXT, content TEXT)")
    t_start = time.perf_counter()
    for s in range(0, len(docs), 2000):
        chunk = docs[s:s + 2000]
        vals = ",".join(f"({s + j + 1},'{chr(97 + (s + j) % 8)}','{esc(t)}')"
                        for j, t in enumerate(chunk))
        db.execute("INSERT INTO docs VALUES " + vals)
    dt = time.perf_counter() - t_start
    results["load_text_rows_per_s"] = len(docs) / dt
    log(f"  text {len(docs):,} docs: {dt:.1f}s  -> {len(docs) / dt:,.0f} rows/s")

    t_start = time.perf_counter()
    db.execute("CREATE TEXT INDEX docs_content ON docs(content)")
    dt = time.perf_counter() - t_start
    results["build_text_index_s"] = dt
    log(f"  CREATE TEXT INDEX: {dt:.1f}s")

    # --- spatial: 200K points, then octree bulk build
    nsp = args.spat_rows
    pts = rng.uniform(-1000, 1000, size=(nsp, 3)).round(4)
    db.execute("CREATE TABLE cloud (id INT PRIMARY KEY, pt GEOMETRY)")
    t_start = time.perf_counter()
    for s in range(0, nsp, 2000):
        vals = ",".join(f"({s + j + 1}, POINT3D({pts[s + j][0]}, {pts[s + j][1]}, {pts[s + j][2]}))"
                        for j in range(min(2000, nsp - s)))
        db.execute("INSERT INTO cloud VALUES " + vals)
    dt = time.perf_counter() - t_start
    results["load_spatial_rows_per_s"] = nsp / dt
    log(f"  spatial {nsp:,} points: {dt:.1f}s  -> {nsp / dt:,.0f} rows/s")

    t_start = time.perf_counter()
    db.execute("CREATE OCTREE INDEX cloud_pt ON cloud(pt)")
    dt = time.perf_counter() - t_start
    results["build_octree_index_s"] = dt
    log(f"  CREATE OCTREE INDEX: {dt:.1f}s")

    def tree_size(p):
        if os.path.isfile(p):
            return os.path.getsize(p)
        return sum(tree_size(os.path.join(p, f)) for f in os.listdir(p))
    size_mb = tree_size(path) / 1e6
    results["db_size_mb"] = size_mb
    log(f"  db size: {size_mb:.0f} MB")

    db.checkpoint()
    db.close()
    db = motedb.Database(path, preset="general")

    # ============================================================ core SQL
    log("\n=== CORE SQL (docs, 100K rows, PK) ===")
    ids = rng.choice(len(docs), size=2000, replace=False) + 1
    db.query("SELECT id FROM docs WHERE id = 1")  # warm
    lat = []
    for i in ids:
        t = time.perf_counter()
        db.query("SELECT content FROM docs WHERE id = ?", params=[int(i)])
        lat.append(time.perf_counter() - t)
    results["pk_point_lookup"] = lat_stats(lat)
    log(f"  PK point lookup x{len(ids)}:   [{lat_str(lat)}]")

    lat = []
    for _ in range(100):
        a = int(rng.integers(1, len(docs) - 10_000))
        t = time.perf_counter()
        _, r = db.query(f"SELECT COUNT(*) FROM docs WHERE id BETWEEN {a} AND {a + 10_000}")
        lat.append(time.perf_counter() - t)
        assert r[0][0] == 10_001, r[0][0]
    results["pk_range_count_10k"] = lat_stats(lat)
    log(f"  PK range COUNT (10K span) x100: [{lat_str(lat)}]")

    lat = []
    for _ in range(50):
        t = time.perf_counter()
        db.query("SELECT COUNT(*), MIN(id), MAX(id), AVG(id) FROM docs")
        lat.append(time.perf_counter() - t)
    results["full_scan_aggregate"] = lat_stats(lat)
    log(f"  full-scan aggregate x50:       [{lat_str(lat)}]")

    lat = []
    for _ in range(50):
        a = int(rng.integers(1, len(docs) - 2000))
        t = time.perf_counter()
        db.query(f"SELECT id, content FROM docs WHERE id > {a} ORDER BY id LIMIT 100")
        lat.append(time.perf_counter() - t)
    results["ordered_limit_100"] = lat_stats(lat)
    log(f"  ORDER BY id LIMIT 100 x50:     [{lat_str(lat)}]")

    # ============================================================= vector
    log(f"\n=== VECTOR (exact scan, {len(D):,} x {DIM}) ===")
    qi = rng.choice(len(Q), size=args.vq, replace=False)
    # numpy brute-force reference timing
    lat_np = []
    for i in qi:
        t = time.perf_counter()
        _ = np.argpartition(((D - Q[i]) ** 2).sum(1), 10)[:10]
        lat_np.append(time.perf_counter() - t)
    results["vector_numpy_bruteforce"] = lat_stats(lat_np)
    log(f"  numpy brute force x{len(qi)}:      [{lat_str(lat_np)}]")

    db.query("SELECT id FROM vecs ORDER BY emb <-> ? LIMIT 10", params=[Q[qi[0]].tolist()])
    lat, lat1 = [], []
    for i in qi:
        t = time.perf_counter()
        _, r = db.query("SELECT id FROM vecs ORDER BY emb <-> ? LIMIT 10",
                        params=[Q[i].tolist()])
        lat.append(time.perf_counter() - t)
        assert len(r) == 10
    results["vector_exact_knn10"] = lat_stats(lat)
    log(f"  ORDER BY emb <-> ? LIMIT 10:   [{lat_str(lat)}]")

    for i in qi[:20]:
        t = time.perf_counter()
        db.query("SELECT id FROM vecs ORDER BY emb <-> ? LIMIT 1",
                 params=[D[int(i) % len(D)].tolist()])
        lat1.append(time.perf_counter() - t)
    results["vector_exact_knn1_self"] = lat_stats(lat1)
    log(f"  self-query LIMIT 1 x20:        [{lat_str(lat1)}]")

    # ============================================================ spatial
    log(f"\n=== SPATIAL (octree, {nsp:,} points) ===")
    qp = pts[rng.choice(nsp, size=100, replace=False)]
    db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, 0.0, 0.0, 0.0, 10)")  # warm
    lat = []
    for p in qp:
        t = time.perf_counter()
        _, r = db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, {p[0]}, {p[1]}, {p[2]}, 10)")
        lat.append(time.perf_counter() - t)
        assert len(r) == 10
    results["spatial_knn10"] = lat_stats(lat)
    log(f"  ST_KNN_3D k=10 x100:           [{lat_str(lat)}]")

    lat = []
    for p in qp:
        t = time.perf_counter()
        db.query(f"SELECT id FROM cloud WHERE ST_RADIUS_3D(pt, {p[0]}, {p[1]}, {p[2]}, 50.0)")
        lat.append(time.perf_counter() - t)
    results["spatial_radius50"] = lat_stats(lat)
    log(f"  ST_RADIUS_3D r=50 x100:        [{lat_str(lat)}]")

    lat = []
    for p in qp[:50]:
        t = time.perf_counter()
        db.query(f"SELECT id FROM cloud WHERE ST_WITHIN_3D(pt, "
                 f"{p[0] - 50}, {p[1] - 50}, {p[2] - 50}, {p[0] + 50}, {p[1] + 50}, {p[2] + 50})")
        lat.append(time.perf_counter() - t)
    results["spatial_within_box100"] = lat_stats(lat)
    log(f"  ST_WITHIN_3D 100m box x50:     [{lat_str(lat)}]")

    # ============================================================== text
    log(f"\n=== TEXT (FTS index, {len(docs):,} docs) ===")
    from collections import Counter
    terms = Counter(w for d in docs[:20000] for w in
                    __import__("re").split(r"[^0-9a-zA-Z_]+", d.lower())
                    if 3 <= len(w) <= 12)
    common = [t for t, _ in terms.most_common(500) if t.isalpha()]
    qterms = [common[i] for i in rng.choice(len(common), size=100, replace=False)]
    db.query(f"SELECT id FROM docs WHERE MATCH(content, '{qterms[0]}') LIMIT 10")  # warm

    lat = []
    for qt in qterms:
        t = time.perf_counter()
        _, r = db.query(f"SELECT id, BM25_SCORE() FROM docs "
                        f"WHERE MATCH(content, '{qt}') LIMIT 10")
        lat.append(time.perf_counter() - t)
    results["text_match_ranked10"] = lat_stats(lat)
    log(f"  MATCH ranked top-10 x100:      [{lat_str(lat)}]")

    lat = []
    for i in range(0, 50):
        qt = f"{qterms[i]} {qterms[i + 50]}"
        t = time.perf_counter()
        db.query(f"SELECT id FROM docs WHERE MATCH(content, '{qt}') LIMIT 10")
        lat.append(time.perf_counter() - t)
    results["text_match_two_terms"] = lat_stats(lat)
    log(f"  MATCH two terms x50:           [{lat_str(lat)}]")

    lat = []
    for qt in qterms[:50]:
        t = time.perf_counter()
        _, r = db.query(f"SELECT COUNT(*) FROM docs WHERE MATCH(content, '{qt}')")
        lat.append(time.perf_counter() - t)
        assert r[0][0] > 0
    results["text_match_count"] = lat_stats(lat)
    log(f"  COUNT(*) WHERE MATCH x50:      [{lat_str(lat)}]")

    sub = docs[int(rng.integers(0, 5000))][:40]
    t = time.perf_counter()
    db.query(f"SELECT id FROM docs WHERE content LIKE '%{esc(sub)}%'")
    results["text_like_scan"] = lat_stats([time.perf_counter() - t])
    log(f"  LIKE '%…%' (no index, 1 run):  [{lat_str([results['text_like_scan']['avg_ms'] / 1e3])}]")

    # ======================================================== time-series
    log(f"\n=== TIME-SERIES ({len(tsi):,} rows, {n_sensors} sensors) ===")
    t_end = int(tsi.max())
    span_h = per * 1  # 1 hour of a sensor ~ per seconds
    lo, hi = t_end - 3_600_000, t_end
    db.query(f"SELECT COUNT(*) FROM m WHERE ts BETWEEN {lo} AND {hi}")  # warm

    lat = []
    for _ in range(100):
        # a random fully-contained 1h window (span is per seconds ≈ 4.3h)
        a = t_end - int(rng.integers(1, max(per - 3600, 2))) * 1_000_000
        t = time.perf_counter()
        _, r = db.query(f"SELECT COUNT(*) FROM m WHERE ts BETWEEN {a} AND {a + 3_600_000_000}")
        lat.append(time.perf_counter() - t)
    results["ts_range_count_1h"] = lat_stats(lat)
    log(f"  range COUNT 1h window x100:    [{lat_str(lat)}]")

    lat = []
    for _ in range(100):
        a = t_end - int(rng.integers(1, max(per - 3600, 2))) * 1_000_000
        t = time.perf_counter()
        db.query(f"SELECT AVG(v), MIN(v), MAX(v) FROM m WHERE ts BETWEEN {a} AND {a + 3_600_000_000}")
        lat.append(time.perf_counter() - t)
    results["ts_range_aggregate_1h"] = lat_stats(lat)
    log(f"  range AVG/MIN/MAX 1h x100:     [{lat_str(lat)}]")

    lat = []
    for _ in range(20):
        a = t_end - 6 * 3_600_000_000
        t = time.perf_counter()
        _, r = db.query(f"SELECT TIME_BUCKET('60s', ts) AS b, COUNT(*) FROM m "
                        f"WHERE ts >= {a} GROUP BY b")
        lat.append(time.perf_counter() - t)
    results["ts_time_bucket_6h"] = lat_stats(lat)
    log(f"  TIME_BUCKET('60s') 6h x20:     [{lat_str(lat)}]  ({len(r)} buckets)")

    lat = []
    for _ in range(20):
        t = time.perf_counter()
        _, r = db.query("SELECT ts, sid, v FROM m LATEST BY sid")
        lat.append(time.perf_counter() - t)
    results["ts_latest_by"] = lat_stats(lat)
    log(f"  LATEST BY sid x20:             [{lat_str(lat)}]  ({len(r)} rows)")

    lat = []
    for _ in range(100):
        t = time.perf_counter()
        db.query(f"SELECT ts, sid, v FROM m WHERE ts < {t_end} ORDER BY ts DESC LIMIT 100")
        lat.append(time.perf_counter() - t)
    results["ts_latest_100_desc"] = lat_stats(lat)
    log(f"  ORDER BY ts DESC LIMIT 100:    [{lat_str(lat)}]")

    lat = []
    for _ in range(20):
        t = time.perf_counter()
        db.query(f"SELECT sid, COUNT(*), AVG(v) FROM m WHERE ts >= {t_end - 6 * 3_600_000_000} "
                 f"GROUP BY sid")
        lat.append(time.perf_counter() - t)
    results["ts_group_by_sensor_6h"] = lat_stats(lat)
    log(f"  GROUP BY sid 6h window x20:    [{lat_str(lat)}]")

    # ============================================ writes: update / delete
    log("\n=== WRITES (update / delete) ===")
    tx = db.begin()
    t_start = time.perf_counter()
    n_upd = 2000
    for i in range(n_upd):
        db.execute(f"UPDATE docs SET cat = 'z' WHERE id = {i + 1}")
    dt = time.perf_counter() - t_start
    db.commit(tx)
    results["update_txn_rows_per_s"] = n_upd / dt
    log(f"  UPDATE x{n_upd} (1 txn): {dt:.2f}s -> {n_upd / dt:,.0f} rows/s")

    t_start = time.perf_counter()
    for i in range(n_upd, 2 * n_upd):
        db.execute(f"UPDATE docs SET cat = 'z' WHERE id = {i + 1}")
    dt = time.perf_counter() - t_start
    results["update_autocommit_rows_per_s"] = n_upd / dt
    log(f"  UPDATE x{n_upd} (autocommit): {dt:.2f}s -> {n_upd / dt:,.0f} rows/s")

    t_start = time.perf_counter()
    db.execute(f"DELETE FROM m WHERE ts < {int(tsi.max()) - 3_600_000_000}")
    dt = time.perf_counter() - t_start
    _, r = db.query("SELECT COUNT(*) FROM m")
    results["ts_delete_1h_s"] = dt
    results["ts_rows_after_delete"] = int(r[0][0])
    log(f"  DELETE FROM m (older than 1h): {dt:.2f}s -> {r[0][0]:,} rows left")

    # ============================================================ reopen
    log("\n=== REOPEN ===")
    t_start = time.perf_counter()
    db.close()
    t_close = time.perf_counter() - t_start
    t_start = time.perf_counter()
    db = motedb.Database(path, preset="general")
    t_open = time.perf_counter() - t_start
    results["close_s"], results["reopen_s"] = t_close, t_open
    log(f"  close {t_close:.2f}s, reopen {t_open:.2f}s")
    t_start = time.perf_counter()
    _, r = db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, {qp[0][0]}, {qp[0][1]}, {qp[0][2]}, 10)")
    results["reopen_first_spatial_query_s"] = time.perf_counter() - t_start
    log(f"  first spatial query after reopen: {results['reopen_first_spatial_query_s'] * 1e3:.1f}ms "
        f"({len(r)} rows)")

    db.close()
    with open(args.out, "w") as f:
        json.dump(results, f, indent=1)
    log(f"\nresults -> {args.out}")
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
