#!/usr/bin/env python3
"""Scale probe (D1): 10M-row narrow table under memory observation.

Shapes: bulk load, checkpoint+reopen, COUNT/GROUP BY, JOIN against a small
dimension table, range aggregate, sampled point reads. Reports per-stage
wall time, peak RSS (getrusage) and stage RSS delta (ps). Vector table is a
SEPARATE probe (prof_scale_vec.py) — this one stays narrow-table only.

Usage: python3 prof_scale.py [--n 10000000] [--root DIR]
"""
import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import motedb  # noqa: E402


def peak_rss_mb():
    # macOS ru_maxrss is in BYTES (Linux: KB) — both land on MB here.
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return v / (1024 * 1024) if v > 10_000_000 else v / 1024.0


def cur_rss_mb():
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        return int(out) / 1024.0
    except Exception:
        return -1.0


class Stage:
    def __init__(self, report, name):
        self.r, self.name = report, name

    def __enter__(self):
        self.t0 = time.perf_counter()
        self.rss0 = cur_rss_mb()
        self.peak0 = peak_rss_mb()
        return self

    def __exit__(self, *exc):
        dt = time.perf_counter() - self.t0
        self.r[self.name] = {
            "s": round(dt, 3),
            "rss_stage_mb": round(cur_rss_mb() - self.rss0, 1),
            "rss_peak_mb": round(peak_rss_mb(), 1),
        }
        print(
            f"  {self.name:26} {dt:8.2f}s  stageRSS {cur_rss_mb() - self.rss0:8.1f}MB"
            f"  peakRSS {peak_rss_mb():8.1f}MB",
            flush=True,
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10_000_000)
    ap.add_argument("--root", default=None, help="persistent dir (default tempdir)")
    args = ap.parse_args()
    N = args.n

    tmp = args.root or tempfile.mkdtemp(prefix="motedb_scale_")
    path = os.path.join(tmp, "scale.mote")
    report = {"n": N, "rows_per_stage": {}}
    print(f"D1 scale probe: {N:,} rows × 4 cols at {tmp}")
    try:
        with Stage(report["rows_per_stage"], "s1_load") as st:
            db = motedb.Database(path, preset="general")
            db.execute(
                "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP,"
                " device TEXT, val FLOAT)"
            )
            db.execute(
                "CREATE TABLE sen (device TEXT PRIMARY KEY, zone INT)"
            )
            db.executemany(
                "INSERT INTO sen VALUES (?, ?)",
                [(f"dev-{i % 32}", i % 8) for i in range(32)],
            )
            B = 50_000
            for s in range(0, N, B):
                j = min(s + B, N)
                db.insert_arrays(
                    "ev",
                    {
                        "id": range(s, j),
                        "ts": range(1_700_000_000_000_000 + s, 1_700_000_000_000_000 + j),
                        "device": [f"dev-{(s + k) % 32}" for k in range(j - s)],
                        "val": [k * 0.5 for k in range(s, j)],
                    },
                )
        report["load_rows_per_s"] = round(N / report["rows_per_stage"]["s1_load"]["s"])
        print(f"    load rate: {report['load_rows_per_s']:,} rows/s", flush=True)

        with Stage(report["rows_per_stage"], "s2_checkpoint_reopen"):
            db.checkpoint()
            db.close()
            db = motedb.Database(path, preset="general")
            _, rows = db.query("SELECT COUNT(*) FROM ev")
            assert rows[0][0] == N, f"row count {rows[0][0]} != {N}"

        with Stage(report["rows_per_stage"], "s3_groupby"):
            _, rows = db.query(
                "SELECT device, COUNT(*), AVG(val) FROM ev GROUP BY device ORDER BY device"
            )
            assert len(rows) == 32, f"groupby rows {len(rows)}"

        with Stage(report["rows_per_stage"], "s4_join_groupby"):
            _, rows = db.query(
                "SELECT s.zone, COUNT(*) FROM ev e JOIN sen s ON e.device = s.device"
                " GROUP BY s.zone ORDER BY s.zone"
            )
            assert len(rows) == 8, f"join rows {len(rows)}"

        with Stage(report["rows_per_stage"], "s5_range_agg"):
            _, rows = db.query(
                "SELECT COUNT(*), AVG(val) FROM ev WHERE ts >= ? AND ts <= ?",
                params=[1_700_000_000_000_000, 1_700_000_000_000_000 + N // 10],
            )
            assert rows[0][0] == N // 10 + 1, f"range count {rows[0][0]}"

        import random
        rng = random.Random(3)
        with Stage(report["rows_per_stage"], "s6_point_reads"):
            lat = []
            for _ in range(1000):
                i = rng.randrange(N)
                t0 = time.perf_counter()
                db.query("SELECT val FROM ev WHERE id = ?", params=[i])
                lat.append(time.perf_counter() - t0)
            lat.sort()
            report["point_p50_us"] = round(lat[500] * 1e6, 1)
            report["point_p95_us"] = round(lat[950] * 1e6, 1)

        db.close()
        print(f"    point p50 {report['point_p50_us']}µs p95 {report['point_p95_us']}µs")
        report["disk_mb"] = round(
            sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, _, fs in os.walk(tmp)
                for f in fs
            )
            / 1e6,
            1,
        )
        print(f"  disk footprint: {report['disk_mb']}MB")
        out = os.path.expanduser("~/.cache/motedb_eval/scale_results.json")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        json.dump(report, open(out, "w"), indent=1)
        print(f"results → {out}")
        return 0
    finally:
        if not args.root:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
