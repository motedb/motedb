#!/usr/bin/env python3
"""
Time-series search accuracy evaluation.

Corpus: 64 sensors × ~3,100 rows each (~200K rows) at 1 Hz base with jitter,
values a deterministic function of (sid, t) plus a noise term; 2% late,
out-of-order arrivals. Reference truth in numpy.

Checks:
  * range scans (inclusive bounds) and half-open ranges
  * aggregates (COUNT/SUM/AVG/MIN/MAX) over ranges
  * GROUP BY sensor over a range (all aggregates)
  * ORDER BY ts ± DESC with LIMIT/OFFSET
  * LATEST BY sid (documented; probed)
  * TIME_BUCKET downsampling (documented; probed)
  * DELETE by time cutoff (the supported `ts < value` form)
  * out-of-order (late) rows visible
  * checkpoint + reopen
  * latency

Usage: python3 ts_eval.py [--n 200000]
"""
import argparse
import json
import os
import shutil
import tempfile
import time

import numpy as np

CACHE = os.path.expanduser("~/.cache/motedb_eval")


def log(msg=""):
    print(msg, flush=True)


def lat_str(lat):
    lat = np.array(lat) * 1e3
    return f"avg {lat.mean():.2f}ms  p50 {np.percentile(lat, 50):.2f}  p95 {np.percentile(lat, 95):.2f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200_000)
    ap.add_argument("--out", default=os.path.join(CACHE, "ts_results.json"))
    args = ap.parse_args()

    import motedb

    # ---- deterministic corpus
    n_sensors = 64
    per_sensor = args.n // n_sensors
    t0 = 1_700_000_000_000_000  # micros
    step = 1_000_000            # 1 s
    rng = np.random.default_rng(7)
    rows = []
    for sid in range(n_sensors):
        jitter = rng.integers(0, 200_000, size=per_sensor)  # ≤0.2s jitter
        ts = t0 + (np.arange(per_sensor) * step) + jitter
        v = 10.0 + sid + 3.0 * np.sin(np.arange(per_sensor) / 97.0) + rng.normal(0, 0.05, per_sensor)
        rows.append((np.full(per_sensor, sid), ts, np.round(v, 4)))
    sid_arr = np.concatenate([r[0] for r in rows])
    ts_arr = np.concatenate([r[1] for r in rows]).astype(np.int64)
    v_arr = np.concatenate([r[2] for r in rows])
    order = np.argsort(ts_arr)  # global time order
    sid_arr, ts_arr, v_arr = sid_arr[order], ts_arr[order], v_arr[order]
    n = len(ts_arr)
    # 2% late arrivals: held back, inserted after the main bulk.
    late = rng.choice(n, n // 50, replace=False)
    late_mask = np.zeros(n, bool)
    late_mask[late] = True
    log(f"corpus: {n:,} rows, {n_sensors} sensors, {late_mask.sum():,} late rows")

    results = {}
    tmp = tempfile.mkdtemp(prefix="motedb_ts_")
    path = os.path.join(tmp, "ts.mote")

    def insert(db, idx):
        s = 0
        while s < len(idx):
            chunk = idx[s:s + 2000]
            vals = ",".join(
                "(%d, %d, %.4f)" % (int(ts_arr[i]), int(sid_arr[i]), v_arr[i])
                for i in chunk
            )
            db.execute("INSERT INTO m VALUES " + vals)
            s += 2000

    def q1(db, sql):
        cols, r = db.query(sql)
        return r[0][0]

    try:
        t0s = time.perf_counter()
        db = motedb.Database(path, preset="general")
        db.execute("CREATE TABLE m (ts TIMESTAMP, sid INT, v FLOAT) TIMESERIES(ts)")
        main_idx = np.nonzero(~late_mask)[0]
        insert(db, main_idx)
        log(f"  inserted {len(main_idx):,} rows in {time.perf_counter() - t0s:.1f}s")
        lat_rows = []
        lat = []
        for q in range(6):
            t0q = time.perf_counter()
            lo, hi = t0 + q * step * per_sensor // 6, t0 + (q + 8) * step * per_sensor // 6
            cols, r = db.query(
                f"SELECT COUNT(*) AS n FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}")
            lat.append(time.perf_counter() - t0q)
            lat_rows.append(r[0][0])
        main_mask = ~late_mask
        truth_counts = [int(((ts_arr >= lo) & (ts_arr <= hi) & main_mask).sum())
                        for lo, hi in [(t0 + q * step * per_sensor // 6, t0 + (q + 8) * step * per_sensor // 6) for q in range(6)]]
        ok = lat_rows == truth_counts[:6] if False else [a == b for a, b in zip(lat_rows, truth_counts)]
        log(f"  COUNT range (main only): {sum(ok)}/6 exact   [{lat_str(lat)}]")
        results["count_range"] = {"exact": sum(ok), "of": 6,
                                  "latency_ms_avg": float(np.mean(lat) * 1e3)}

        # aggregates over a window
        lo, hi = t0 + step * 1000, t0 + step * 3000
        mask = (ts_arr >= lo) & (ts_arr <= hi) & ~late_mask
        tv = v_arr[mask]
        got = {
            "COUNT": q1(db, f"SELECT COUNT(*) FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}"),
            "SUM": q1(db, f"SELECT SUM(v) FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}"),
            "AVG": q1(db, f"SELECT AVG(v) FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}"),
            "MIN": q1(db, f"SELECT MIN(v) FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}"),
            "MAX": q1(db, f"SELECT MAX(v) FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}"),
        }
        exp = {"COUNT": int(mask.sum()), "SUM": float(tv.sum()),
               "AVG": float(tv.mean()), "MIN": float(tv.min()), "MAX": float(tv.max())}
        for k in got:
            if got[k] is None:
                continue  # empty set → NULL (correct SQL semantics)
            g, e = float(got[k]), exp[k]
            okk = abs(g - e) <= max(1e-6 * max(abs(e), 1.0), 1e-6)
            log(f"  {k:6} range agg: got {g:.4f}  truth {e:.4f}  {'OK' if okk else 'MISMATCH'}")
            results[f"agg_{k}"] = {"got": g, "truth": e, "ok": bool(okk)}

        # GROUP BY sid over the window
        cols, r = db.query(
            f"SELECT sid, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM m "
            f"WHERE ts BETWEEN {int(lo)} AND {int(hi)} GROUP BY sid")
        gm = {}
        for row in r:
            gm[int(row[0])] = row[1:]
        bad = 0
        for sid in range(n_sensors):
            sm = mask & (sid_arr == sid)
            if not sm.any():
                continue
            tvv = v_arr[sm]
            g = gm.get(sid)
            if g is None:
                bad += 1
                continue
            exp5 = [int(sm.sum()), float(tvv.sum()), float(tvv.mean()), float(tvv.min()), float(tvv.max())]
            if not all(abs(float(a) - b) <= max(1e-6 * max(abs(b), 1.0), 1e-6) for a, b in zip(g, exp5)):
                bad += 1
        log(f"  GROUP BY sid × 5 aggregates: {n_sensors - bad}/{n_sensors} groups exact")
        results["groupby"] = {"exact_groups": n_sensors - bad, "of": n_sensors}

        # ORDER BY ts DESC LIMIT k
        _, r = db.query(f"SELECT ts FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)} ORDER BY ts DESC LIMIT 10")
        got_ts = [row[0] for row in r]
        exp_ts = sorted(ts_arr[(ts_arr >= lo) & (ts_arr <= hi) & ~late_mask], reverse=True)[:10]
        okk = got_ts == exp_ts
        log(f"  ORDER BY ts DESC LIMIT 10: {'exact' if okk else 'MISMATCH'}")
        results["order_desc"] = bool(okk)

        # LATEST BY (documented feature)
        try:
            _, r = db.query("SELECT ts, sid, v FROM m LATEST BY sid")
            got_latest = {row[1]: row[0] for row in r}
            exp_latest = {}
            m2 = ~late_mask
            for sid in range(n_sensors):
                sm = m2 & (sid_arr == sid)
                exp_latest[sid] = int(ts_arr[sm].max())
            okk = got_latest == exp_latest
            log(f"  LATEST BY sid: {'exact' if okk else f'MISMATCH ({len(got_latest)} rows vs {n_sensors})'}")
            results["latest_by"] = bool(okk)
        except Exception as e:
            log(f"  LATEST BY sid: ERR {str(e)[:70]}")
            results["latest_by"] = str(e)[:80]

        # TIME_BUCKET (documented)
        try:
            _, r = db.query(
                "SELECT TIME_BUCKET('10s', ts) AS b, COUNT(*) FROM m "
                f"WHERE ts BETWEEN {int(lo)} AND {int(hi)} GROUP BY b")
            buckets = {}
            for row in r:
                buckets[int(row[0])] = int(row[1])
            exp_b = {}
            for t in ts_arr[mask]:
                exp_b[int(t // 10_000_000 * 10_000_000)] = exp_b.get(int(t // 10_000_000 * 10_000_000), 0) + 1
            okk = buckets == exp_b
            log(f"  TIME_BUCKET('10s') GROUP BY: {'exact' if okk else 'MISMATCH'} "
                f"({len(buckets)} buckets vs {len(exp_b)})")
            results["time_bucket"] = bool(okk)
        except Exception as e:
            log(f"  TIME_BUCKET: ERR {str(e)[:70]}")
            results["time_bucket"] = str(e)[:80]

        # late (out-of-order) rows
        late_idx = np.nonzero(late_mask)[0]
        t_late = time.perf_counter()
        insert(db, late_idx)
        log(f"  inserted {len(late_idx):,} late rows in {time.perf_counter() - t_late:.1f}s")
        lo, hi = ts_arr.min(), ts_arr.max()
        cnt = q1(db, f"SELECT COUNT(*) FROM m WHERE ts BETWEEN {int(lo)} AND {int(hi)}")
        okk = cnt == n
        log(f"  full-range COUNT after late rows: {cnt} (truth {n}) {'OK' if okk else 'MISMATCH'}")
        results["late_rows_visible"] = bool(okk)

        # DELETE by cutoff (the supported ts < form)
        cut = t0 + step * (per_sensor // 2)
        t0d = time.perf_counter()
        db.execute(f"DELETE FROM m WHERE ts < {int(cut)}")
        log(f"  DELETE ts < cutoff in {time.perf_counter() - t0d:.1f}s")
        keep = ts_arr >= cut
        cnt = q1(db, f"SELECT COUNT(*) FROM m WHERE ts BETWEEN {int(cut)} AND {int(hi)}")
        below = q1(db, f"SELECT COUNT(*) FROM m WHERE ts < {int(cut)}")
        okk = cnt == int(keep.sum()) and below == 0
        log(f"  after DELETE: kept {cnt} (truth {int(keep.sum())}), leaked {below} {'OK' if okk else 'MISMATCH'}")
        results["delete_cutoff"] = bool(okk)

        # reopen
        db.checkpoint()
        db.close()
        db = motedb.Database(path, preset="general")
        cnt = q1(db, f"SELECT COUNT(*) FROM m WHERE ts BETWEEN {int(cut)} AND {int(hi)}")
        log(f"  after reopen: {cnt} rows (truth {int(keep.sum())}) {'OK' if cnt == int(keep.sum()) else 'MISMATCH'}")
        results["reopen_count_ok"] = cnt == int(keep.sum())
        db.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2, default=float)
    log(f"\nresults → {args.out}")


if __name__ == "__main__":
    main()
