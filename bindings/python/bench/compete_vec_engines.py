#!/usr/bin/env python3
"""向量检索多引擎行业对照 v2 — MoteDB DiskANN vs FAISS / LanceDB / sqlite-vec /
DuckDB-VSS / usearch,同一语料、同一查询集、同一暴力 ground truth。

用法:
  python compete_vec_engines.py [--n 220000] [--dim 384] [--q 200]
      [--engines mote,faiss,lancedb,sqlite_vec,duckdb,usearch]
      [--out DIR]

每行输出一个 JSON: {engine_mode, recall@10, p50/p95/p99_ms, build_s,
ingest_kvecs_s, disk_mb, rss_mb}。缺哪个引擎的依赖就跳过哪个(faiss/lancedb/
sqlite_vec/duckdb/usearch 均为可选)。语料与 gt 协议复用 compete_vec_industry.py
(splitmix 聚簇分布 — ANN 基准必须用聚簇语料, 见 prof_ann.build_corpus 的教训注)。

方法学声明(报告必须随数据一起发布):
  - 语料: 聚簇合成(确定性 seed), 非真实嵌入; 数量/维度见 JSON
  - ground truth: numpy 暴力 top-k, 同一查询集(语料内采样, seed=42)
  - 各引擎取其官方默认索引参数 + 1-2 个调优档, 全部列出, 不挑好话
  - 延迟为单查询串行(python 侧计时), 含反序列化
"""
import argparse
import json
import os
import resource
import shutil
import sys
import tempfile
import time

import numpy as np

BENCH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BENCH)
sys.path.insert(0, os.path.join(BENCH, ".."))
import motedb  # noqa: E402
from prof_ann import build_corpus  # noqa: E402

ROOT = os.path.expanduser("~/.cache/motedb_ann_bench")
K = 10


def peak_rss_mb():
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return v / (1024 * 1024) if v > 10_000_000 else v / 1024.0


def ground_truth(corpus, queries, k=K):
    qs = queries.astype(np.float32)
    qn = (qs * qs).sum(axis=1, keepdims=True)
    cn = (corpus * corpus).sum(axis=1)
    part = np.argpartition(qn + cn - 2.0 * (qs @ corpus.T), k, axis=1)[:, :k]
    gt = np.empty((len(queries), k), dtype=np.int64)
    for r in range(part.shape[0]):
        d = qn[r] + cn - 2.0 * (qs[r] @ corpus.T)
        gt[r] = part[r][np.argsort(d[part[r]])]
    return gt + 1  # 统一 1 基 id


def timed_queries(search_fn, queries, gt, mode, out, build_s, ingest=None, disk_mb=None, id_map="one-based"):
    """公共计时循环: search_fn(query_vec) -> iterable of ids (int)."""
    times, hits = [], 0
    for i in range(len(queries)):
        t0 = time.perf_counter()
        got = {int(x) for x in search_fn(queries[i])}
        times.append(time.perf_counter() - t0)
        hits += len(got & set(gt[i].tolist()))
    a = np.asarray(times) * 1e3
    out.append({
        "engine_mode": mode,
        "recall@10": round(hits / (len(queries) * K), 4),
        "p50_ms": round(float(np.percentile(a, 50)), 3),
        "p95_ms": round(float(np.percentile(a, 95)), 3),
        "p99_ms": round(float(np.percentile(a, 99)), 3),
        "build_s": round(build_s, 3) if build_s is not None else None,
        "ingest_kvecs_s": round(ingest, 1) if ingest else None,
        "disk_mb": round(disk_mb, 1) if disk_mb else None,
        "rss_mb": round(peak_rss_mb(), 1),
        "id_map": id_map,
    })


# ── MoteDB ────────────────────────────────────────────────────────────────

def bench_mote(corpus, queries, gt, n, dim, out, root):
    db_dir = os.path.join(root, "bench.mote")
    if os.path.isdir(db_dir):
        shutil.rmtree(db_dir)
    os.makedirs(root, exist_ok=True)
    t0 = time.perf_counter()
    db = motedb.Database(db_dir)
    db.execute(f"CREATE TABLE t (id INT PRIMARY KEY, emb VECTOR({dim}))")
    B = 50_000
    for s in range(0, n, B):
        db.insert_arrays("t", {"id": np.arange(s + 1, min(s + B, n) + 1, dtype=np.int64),
                               "emb": np.ascontiguousarray(corpus[s:s + B])})
    ingest = n / 1e3 / (time.perf_counter() - t0)
    tb = time.perf_counter()
    db.execute("CREATE VECTOR INDEX vidx ON t(emb)")
    build_s = time.perf_counter() - tb
    for i in range(5):  # warm
        db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10", params=[queries[i].tolist()])
    timed_queries(
        lambda q: (r[0] for r in db.query("SELECT id FROM t ORDER BY emb <-> ? LIMIT 10",
                                          params=[q.tolist()])[1]),
        queries, gt, f"motedb_diskann_n{n}_d{dim}", out, build_s, ingest,
        disk_mb=sum(os.path.getsize(os.path.join(dp, fn))
                    for dp, _, fns in os.walk(db_dir) for fn in fns) / 1e6)
    db.close()


# ── FAISS (flat / IVF / HNSW) ─────────────────────────────────────────────

def bench_faiss(corpus, queries, gt, out):
    import faiss
    d = corpus.shape[1]
    c = np.ascontiguousarray(corpus)

    t0 = time.perf_counter()
    flat = faiss.IndexFlatL2(d)
    flat.add(c)
    timed_queries(lambda q: flat.search(q[None, :], K)[1][0],
                  queries, gt - 1, "faiss_flat_exact", out, time.perf_counter() - t0,
                  id_map="zero-based")

    t0 = time.perf_counter()
    ivf = faiss.IndexIVFFlat(faiss.IndexFlatL2(d), d, 1024, faiss.METRIC_L2)
    ivf.train(np.ascontiguousarray(corpus[::2].copy()))
    ivf.add(c)
    build_ivf = time.perf_counter() - t0
    for nprobe in (8, 32):
        faiss.ParameterSpace().set_index_parameter(ivf, "nprobe", nprobe)
        timed_queries(lambda q, ivf=ivf: ivf.search(q[None, :], K)[1][0],
                      queries, gt - 1, f"faiss_ivfflat_nprobe{nprobe}", out, build_ivf,
                      id_map="zero-based")

    t0 = time.perf_counter()
    hnsw = faiss.IndexHNSWFlat(d, 16, faiss.METRIC_L2)
    hnsw.add(c)
    build_h = time.perf_counter() - t0
    for ef in (32, 64, 256):
        faiss.ParameterSpace().set_index_parameter(hnsw, "efSearch", ef)
        timed_queries(lambda q, hnsw=hnsw: hnsw.search(q[None, :], K)[1][0],
                      queries, gt - 1, f"faiss_hnsw16_ef{ef}", out, build_h,
                      id_map="zero-based")


# ── LanceDB (无索引精确 + IVF_PQ) ────────────────────────────────────────

def bench_lancedb(corpus, queries, gt, n, dim, out):
    import lancedb
    import pyarrow as pa

    uri = tempfile.mkdtemp(prefix="lancedb-bench-")
    db = lancedb.connect(uri)
    data = pa.table({
        "id": pa.array(np.arange(1, n + 1, dtype=np.int64)),
        "vector": pa.FixedSizeListArray.from_arrays(
            pa.array(corpus.reshape(-1), type=pa.float32()), dim),
    })
    t0 = time.perf_counter()
    tbl = db.create_table("t", data=data, mode="overwrite")
    ingest = n / 1e3 / (time.perf_counter() - t0)

    timed_queries(lambda q: (r["id"] for r in tbl.search(q.tolist()).limit(K).to_list()),
                  queries, gt, f"lancedb_flat_n{n}_d{dim}", out, 0.0, ingest,
                  disk_mb=sum(os.path.getsize(os.path.join(uri, f)) for f in os.listdir(uri)) / 1e6)

    t0 = time.perf_counter()
    try:
        from lancedb.index import IvfPq
        tbl.create_index("vector", config=IvfPq(distance_type="l2"))
    except ImportError:
        tbl.create_index(metric="l2")  # deprecated but present
    build_s = time.perf_counter() - t0
    for nprobe in (1, 20):
        timed_queries(
            lambda q, np_=nprobe: (r["id"] for r in tbl.search(q.tolist()).limit(K).nprobes(np_).to_list()),
            queries, gt, f"lancedb_ivfpq_nprobes{nprobe}", out, build_s)
    shutil.rmtree(uri, ignore_errors=True)


# ── sqlite-vec (vec0 精确扫描) ────────────────────────────────────────────

def bench_sqlite_vec(corpus, queries, gt, n, dim, out):
    import sqlite3
    import sqlite_vec

    if not hasattr(sqlite3.Connection, "enable_load_extension"):
        out.append({"engine_mode": "sqlite_vec", "skipped":
                    "this python build lacks sqlite3 extension loading (python.org framework build)"})
        return
    path = tempfile.mktemp(suffix=".sqlite3")
    con = sqlite3.connect(path)
    con.enable_load_extension(True)
    sqlite_vec.load(con)
    con.enable_load_extension(False)
    con.execute(f"CREATE VIRTUAL TABLE t USING vec0(embedding float[{dim}])")
    t0 = time.perf_counter()
    con.executemany(
        "INSERT INTO t(rowid, embedding) VALUES (?, ?)",
        ((i + 1, sqlite_vec.serialize_float32(corpus[i])) for i in range(n)))
    con.commit()
    ingest = n / 1e3 / (time.perf_counter() - t0)

    def search(q):
        rows = con.execute(
            "SELECT rowid, distance FROM t WHERE embedding MATCH ? AND k = 10",
            [sqlite_vec.serialize_float32(q)]).fetchall()
        return (r[0] for r in rows)

    for i in range(3):
        search(queries[i])
    timed_queries(search, queries, gt, f"sqlite_vec_exact_n{n}_d{dim}", out, 0.0, ingest,
                  disk_mb=os.path.getsize(path) / 1e6)
    con.close()
    os.unlink(path)


# ── DuckDB + VSS (HNSW) ───────────────────────────────────────────────────

def bench_duckdb(corpus, queries, gt, n, dim, out):
    import duckdb
    import pyarrow as pa

    con = duckdb.connect()  # HNSW (vss) is in-memory only on persistent DBs
    try:
        con.execute("LOAD vss")
    except Exception as e:
        try:
            con.execute("INSTALL vss")
            con.execute("LOAD vss")
        except Exception:
            out.append({"engine_mode": "duckdb_hnsw", "skipped": f"vss extension unavailable: {str(e)[:80]}"})
            con.close()
            return
    con.execute(f"CREATE TABLE t(id BIGINT, embedding FLOAT[{dim}])")
    data = pa.table({
        "id": pa.array(np.arange(1, n + 1, dtype=np.int64)),
        "vector": pa.FixedSizeListArray.from_arrays(
            pa.array(corpus.reshape(-1), type=pa.float32()), dim),
    })
    t0 = time.perf_counter()
    con.register("src", data)
    con.execute("INSERT INTO t SELECT id, vector FROM src")
    ingest = n / 1e3 / (time.perf_counter() - t0)
    tb = time.perf_counter()
    con.execute("CREATE INDEX t_hnsw ON t USING HNSW (embedding)")
    build_s = time.perf_counter() - tb

    def search(q):
        rows = con.execute(
            f"SELECT id FROM t ORDER BY array_distance(embedding, ?::FLOAT[{dim}]) LIMIT 10",
            [q.tolist()]).fetchall()
        return (r[0] for r in rows)

    for i in range(3):
        search(queries[i])
    timed_queries(search, queries, gt, f"duckdb_hnsw_n{n}_d{dim}", out, build_s, ingest)
    con.close()


# ── usearch (HNSW 系单机库) ───────────────────────────────────────────────

def bench_usearch(corpus, queries, gt, n, dim, out):
    from usearch.index import Index

    idx = Index(ndim=dim, metric="l2sq", dtype="f32")
    t0 = time.perf_counter()
    idx.add(np.arange(1, n + 1, dtype=np.longlong), corpus)
    ingest = n / 1e3 / (time.perf_counter() - t0)  # add 即建索引(增量构图)

    def search(q):
        return (int(m.key) for m in idx.search(q, K))

    for i in range(3):
        search(queries[i])
    timed_queries(search, queries, gt, f"usearch_l2_n{n}_d{dim}", out, 0.0, ingest)


# ── main ──────────────────────────────────────────────────────────────────

ADAPTERS = {
    "mote": lambda a: bench_mote(a["corpus"], a["queries"], a["gt"], a["n"], a["dim"], a["out"], a["root"]),
    "faiss": lambda a: bench_faiss(a["corpus"], a["queries"], a["gt"], a["out"]),
    "lancedb": lambda a: bench_lancedb(a["corpus"], a["queries"], a["gt"], a["n"], a["dim"], a["out"]),
    "sqlite_vec": lambda a: bench_sqlite_vec(a["corpus"], a["queries"], a["gt"], a["n"], a["dim"], a["out"]),
    "duckdb": lambda a: bench_duckdb(a["corpus"], a["queries"], a["gt"], a["n"], a["dim"], a["out"]),
    "usearch": lambda a: bench_usearch(a["corpus"], a["queries"], a["gt"], a["n"], a["dim"], a["out"]),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=220_000)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--q", type=int, default=200)
    ap.add_argument("--engines", default="mote,faiss,lancedb,sqlite_vec,duckdb,usearch")
    ap.add_argument("--out", default=None, help="optional jsonl output path")
    args = ap.parse_args()

    root = os.path.join(ROOT, f"idx-{args.n}-{args.dim}")
    os.makedirs(root, exist_ok=True)
    corpus_path = os.path.join(root, "corpus.npy")
    if os.path.exists(corpus_path):
        corpus = np.load(corpus_path)
        if corpus.shape != (args.n, args.dim):
            corpus = build_corpus(args.n, args.dim)
            np.save(corpus_path, corpus)
    else:
        corpus = build_corpus(args.n, args.dim)
        np.save(corpus_path, corpus)

    rng = np.random.default_rng(42)
    queries = corpus[rng.integers(0, len(corpus), args.q)].astype(np.float32)
    gt = ground_truth(corpus, queries)
    print(f"# corpus {corpus.shape}, queries {args.q}, k {K}", file=sys.stderr)

    out = []
    ctx = {"corpus": corpus, "queries": queries, "gt": gt, "n": args.n,
           "dim": args.dim, "out": out, "root": root}
    for name in args.engines.split(","):
        name = name.strip()
        fn = ADAPTERS.get(name)
        if fn is None:
            print(f"# unknown engine {name}", file=sys.stderr)
            continue
        try:
            fn(ctx)
        except ImportError as e:
            out.append({"engine_mode": name, "skipped": f"dependency missing: {e}"})
        except Exception as e:
            out.append({"engine_mode": name, "skipped": f"error: {type(e).__name__}: {str(e)[:120]}"})

    for row in out:
        print("JSON " + json.dumps(row))
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as f:
            for row in out:
                f.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    main()
