#!/usr/bin/env python3
"""FTS query-shape benchmark: mote vs SQLite FTS5, with analytic parity.

Workstream B baseline/target harness (plan: dual-word top-10 0.141ms →
≤0.03ms). Shapes (LIMIT 10, BM25 ranked):
  single_high   one term, df ~10%      (100K docs → 10K matches)
  single_low    one term, df ~0.1%     (~99 matches)
  and_high_mid  'a b'    (AND default) (~99 matches)
  and_high_low  'a b'    (AND default) (~10 matches)
  and_3term     'a b c'  (AND default) (~0-1 matches)
  or_high_low   'a OR b' (explicit OR) (~10.1K matches)

Corpus docs draw from PAIRWISE-COPRIME residue streams (alpha=i%10,
beta=i%101, gamma=i%1009, delta=i), so every query's exact match set is
analytically known — parity is checked against that closed form via
COUNT(*). Every query runs exactly ONCE (distinct terms per query): the
engine caches top-K results per query string, so min-over-repeats
measures the cache, not the search.

Usage: python3 prof_fts.py [--n 100000] [--qs 200]
"""

import argparse
import os
import shutil
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb  # noqa: E402


def doc(i):
    # tf variance: alpha repeats 1..3× (exercises BM25 tf saturation and
    # gives the B2 block-max skip tables real spread; match SETS are
    # unchanged — parity still checks token membership).
    rep = " ".join([f"alpha{i % 10}"] * ((i % 3) + 1))
    return f"record {rep} beta{i % 101} gamma{i % 1009} delta{i}"


def truth_count(kind, n, a=0, b=0, c=0):
    """Analytic |match set| over 0-based doc ids [0, n)."""
    if kind == "alpha":
        return sum(1 for i in range(n) if i % 10 == a)
    if kind == "gamma":
        return sum(1 for i in range(n) if i % 1009 == c)
    if kind == "and":  # alpha_a AND beta_b
        return sum(1 for i in range(n) if i % 10 == a and i % 101 == b)
    if kind == "and_ag":  # alpha_a AND gamma_c
        return sum(1 for i in range(n) if i % 10 == a and i % 1009 == c)
    if kind == "and3":  # alpha_a AND beta_b AND gamma_c
        return sum(1 for i in range(n) if i % 10 == a and i % 101 == b and i % 1009 == c)
    if kind == "or":  # alpha_a OR gamma_c
        return sum(1 for i in range(n) if i % 10 == a or i % 1009 == c)
    raise ValueError(kind)


def esc(s):
    return s.replace("'", "''")


def build_corpus(tmp, n):
    path = os.path.join(tmp, "fts.mote")
    t0 = time.perf_counter()
    db = motedb.Database(path, preset="general")
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)")
    B = 5000
    for s in range(0, n, B):
        vals = ",".join(f"({s + j + 1},'{esc(doc(s + j))}')" for j in range(min(B, n - s)))
        db.execute(f"INSERT INTO docs VALUES {vals}")
    t_insert = time.perf_counter() - t0
    t0 = time.perf_counter()
    db.execute("CREATE TEXT INDEX docs_content ON docs(content)")
    t_index = time.perf_counter() - t0
    t0 = time.perf_counter()
    db.checkpoint()
    db.close()
    t_ckpt = time.perf_counter() - t0
    return path, t_insert, t_index, t_ckpt


def build_sqlite(tmp, n):
    path = os.path.join(tmp, "fts.sqlite")
    con = sqlite3.connect(path)
    con.execute("CREATE VIRTUAL TABLE docs USING fts5(content)")
    t0 = time.perf_counter()
    con.executemany("INSERT INTO docs VALUES (?)", ((doc(i),) for i in range(n)))
    con.commit()
    t_build = time.perf_counter() - t0
    return con, t_build


def percentile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100.0))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000)
    ap.add_argument("--qs", type=int, default=200)
    args = ap.parse_args()
    N, QS = args.n, args.qs

    # Query families: DISTINCT queries each (defeats the per-query top-K
    # cache — every alpha/beta/gamma value is fresh). alphaX ⇔ i%10==X,
    # betaX ⇔ i%101==X, gammaX ⇔ i%1009==X.
    shapes = {
        "single_high": [f"alpha{q % 10}" for q in range(QS)],
        "single_low": [f"gamma{q % 1009}" for q in range(QS)],
        "and_high_mid": [f"alpha{q % 10} beta{(7 + q) % 101}" for q in range(QS)],
        "and_high_low": [f"alpha{q % 10} gamma{(5 + q) % 1009}" for q in range(QS)],
        "and_3term": [
            f"alpha{q % 10} beta{(7 + q) % 101} gamma{(5 + q) % 1009}" for q in range(QS)
        ],
        "or_high_low": [f"alpha{q % 10} OR gamma{(5 + q) % 1009}" for q in range(QS)],
    }

    tmp = tempfile.mkdtemp(prefix="motedb_fts_bench_")
    print(f"corpus: {N:,} docs × ~60B   queries/shape: {QS}")
    try:
        path, t_ins, t_idx, t_ckpt = build_corpus(tmp, N)
        print(f"insert {t_ins:6.2f}s   CREATE TEXT INDEX {t_idx:6.2f}s   checkpoint {t_ckpt:6.2f}s")

        db = motedb.Database(path, preset="general")
        con, t_sql_build = build_sqlite(tmp, N)
        print(f"sqlite FTS5 build {t_sql_build:6.2f}s")

        # Warm both engines (posting/page caches) with throwaway queries.
        for q in ("alpha0 beta0", "alpha1 gamma1", "alpha2 OR gamma2", "delta42"):
            db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 10")
            con.execute(
                "SELECT rowid FROM docs WHERE docs MATCH ? ORDER BY bm25(docs) LIMIT 10", (q,)
            ).fetchall()

        # ---- parity: COUNT(*) vs analytic truth (also exercises unranked path)
        fails = 0
        checks = [
            ("single_high", "alpha3", truth_count("alpha", N, a=3)),
            ("single_low", "gamma5", truth_count("gamma", N, c=5)),
            ("and_high_mid", "alpha3 beta7", truth_count("and", N, a=3, b=7)),
            ("and_high_low", "alpha3 gamma5", truth_count("and_ag", N, a=3, c=5)),
            ("or_high_low", "alpha3 OR gamma5", truth_count("or", N, a=3, c=5)),
        ]
        for label, q, exp in checks:
            _, rows = db.query(f"SELECT COUNT(*) FROM docs WHERE MATCH(content, '{esc(q)}')")
            got = rows[0][0]
            ok = got == exp
            fails += not ok
            print(f"  parity {label:14} {q!r:26} got {got:>7}  expect {exp:>7}  {'OK' if ok else 'MISMATCH'}")
        # LIMIT 10 ranked: sets with ≥10 matches must return exactly 10.
        for label, q, m in [
            ("single_high", "alpha3", truth_count("alpha", N, a=3)),
            ("and_high_mid", "alpha3 beta7", truth_count("and", N, a=3, b=7)),
            ("or_high_low", "alpha3 OR gamma5", truth_count("or", N, a=3, c=5)),
        ]:
            _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 10")
            want = min(10, m)
            ok = len(rows) == want
            fails += not ok
            print(f"  ranked  {label:14} {q!r:26} → {len(rows)} rows (expect {want})  {'OK' if ok else 'MISMATCH'}")

        # ---- latency: every query exactly once (no repeats → no result cache)
        print(f"\n{'shape':<14} {'mote p50':>9} {'p95':>9}   {'fts5 p50':>9} {'p95':>9}")
        for label, qs in shapes.items():
            mote = []
            for q in qs:
                t0 = time.perf_counter()
                db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 10")
                mote.append(time.perf_counter() - t0)
            sql = []
            for q in qs:
                t0 = time.perf_counter()
                con.execute(
                    "SELECT rowid FROM docs WHERE docs MATCH ? ORDER BY bm25(docs) LIMIT 10", (q,)
                ).fetchall()
                sql.append(time.perf_counter() - t0)
            m50, m95 = percentile(mote, 50) * 1e3, percentile(mote, 95) * 1e3
            s50, s95 = percentile(sql, 50) * 1e3, percentile(sql, 95) * 1e3
            print(f"{label:<14} {m50:7.3f}ms {m95:7.3f}ms   {s50:7.3f}ms {s95:7.3f}ms")
        print("\nPARITY", "ALL OK" if fails == 0 else f"{fails} FAILURES")
        return 0 if fails == 0 else 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
