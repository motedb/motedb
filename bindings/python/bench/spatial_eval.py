#!/usr/bin/env python3
"""
Spatial (i-Octree) search accuracy evaluation.

Corpus: a synthetic indoor LiDAR-style scan — floor, ceiling, four walls, six
boxes, two cylindrical pillars, sparse clutter — with 1 cm sensor noise and a
few exact duplicates. Density is highly non-uniform (thin planar slabs), which
is what stresses an octree's splitting. Coordinates are rounded to 0.1 mm so
the SQL literals and the numpy ground truth are identical numbers.

Checked against numpy (float64) brute force:
  * ST_KNN_3D            recall@k (ties counted as correct) + result ordering
  * ORDER BY ST_DISTANCE_3D LIMIT k   ids + the returned distance VALUES
  * ST_RADIUS_3D         exact set equality (precision / recall), boundary
  * ST_WITHIN_3D         exact set equality
  * 2D sugar: ST_KNN / ST_WITHIN / ST_DISTANCE on POINT(x, y)
  * lifecycle: CHECKPOINT, close+reopen, DELETE, UPDATE, incremental inserts
  * point read of the GEOMETRY column after CHECKPOINT
  * latency + index build time

Usage: python3 spatial_eval.py [--n 200000] [--preset general]
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


# ---------------------------------------------------------------- data
def indoor_scan(n, seed=7):
    """Return (n, 3) float64 points of a 20×15×3 m room, rounded to 0.1 mm."""
    rng = np.random.default_rng(seed)
    W, D, H = 20.0, 15.0, 3.0
    parts = []

    def plane(m, ax, val, lo, hi):
        p = rng.uniform(lo, hi, size=(m, 3))
        p[:, ax] = val
        return p

    frac = np.array([0.30, 0.15, 0.075, 0.075, 0.075, 0.075, 0.15, 0.05, 0.05])
    counts = (frac / frac.sum() * n).astype(int)
    counts[-1] += n - counts.sum()
    m = iter(counts)
    parts.append(plane(next(m), 2, 0.0, [0, 0, 0], [W, D, 0]))          # floor
    parts.append(plane(next(m), 2, H, [0, 0, 0], [W, D, 0]))            # ceiling
    parts.append(plane(next(m), 0, 0.0, [0, 0, 0], [0, D, H]))          # walls
    parts.append(plane(next(m), 0, W, [0, 0, 0], [0, D, H]))
    parts.append(plane(next(m), 1, 0.0, [0, 0, 0], [W, 0, H]))
    parts.append(plane(next(m), 1, D, [0, 0, 0], [W, 0, H]))
    # six boxes (surfaces)
    box_pts = next(m)
    boxes = []
    for _ in range(6):
        c = rng.uniform([2, 2, 0], [W - 2, D - 2, 0])
        s = rng.uniform([0.4, 0.4, 0.4], [1.5, 1.5, 1.2])
        lo, hi = c - [s[0] / 2, s[1] / 2, 0], c + [s[0] / 2, s[1] / 2, s[2]]
        k = box_pts // 6
        faces = []
        for ax in range(3):
            for v in (lo[ax], hi[ax]):
                q = rng.uniform(lo, hi, size=(k // 6, 3))
                q[:, ax] = v
                faces.append(q)
        boxes.append(np.vstack(faces))
    parts.append(np.vstack(boxes))
    # two pillars (cylinders)
    pil = next(m)
    cyl = []
    for cx, cy in ((5.0, 5.0), (15.0, 10.0)):
        k = pil // 2
        th = rng.uniform(0, 2 * np.pi, k)
        z = rng.uniform(0, H, k)
        cyl.append(np.column_stack([cx + 0.3 * np.cos(th), cy + 0.3 * np.sin(th), z]))
    parts.append(np.vstack(cyl))
    # clutter
    parts.append(rng.uniform([0, 0, 0], [W, D, H], size=(next(m), 3)))

    pts = np.vstack(parts)
    pts += rng.normal(0, 0.01, size=pts.shape)  # 1 cm sensor noise
    # exact duplicates (same coordinates, different ids) to exercise ties
    dup_src = rng.choice(len(pts), size=100, replace=False)
    pts = np.vstack([pts, pts[dup_src]])
    rng.shuffle(pts)
    return np.round(pts, 4)


def fmt(v):
    return f"{v:.4f}"


# ---------------------------------------------------------------- truth
def knn_truth(P, q, k):
    d = np.sqrt(((P - q) ** 2).sum(1))
    idx = np.argpartition(d, k - 1)[:k]
    idx = idx[np.argsort(d[idx], kind="stable")]
    return idx, d


def recall_ties(got_ids, d, k):
    """Fraction of returned ids whose true distance ≤ the true k-th distance."""
    dk = np.partition(d, k - 1)[k - 1]
    ok = sum(1 for i in got_ids if d[i] <= dk + 1e-9)
    return ok / k


# ---------------------------------------------------------------- db helpers
def ids_of(rows):
    return [r[0] for r in rows]


def insert_points(db, table, P, id_offset=0, batch=2000):
    t0 = time.perf_counter()
    for s in range(0, len(P), batch):
        vals = ",".join(
            f"({id_offset + s + j + 1}, POINT3D({fmt(p[0])}, {fmt(p[1])}, {fmt(p[2])}))"
            for j, p in enumerate(P[s:s + batch])
        )
        db.execute(f"INSERT INTO {table} VALUES {vals}")
    dt = time.perf_counter() - t0
    log(f"  inserted {len(P)} points in {dt:.1f}s ({len(P) / dt:.0f} pts/s)")


def lat_str(lat):
    lat = np.array(lat) * 1e3
    return f"avg {lat.mean():.2f}ms  p50 {np.percentile(lat, 50):.2f}  p95 {np.percentile(lat, 95):.2f}"


# ---------------------------------------------------------------- checks
def check_knn(db, table, P, Q, ks, label, res):
    for k in ks:
        rec, ordered, lat, nret = [], 0, [], []
        for q in Q:
            t0 = time.perf_counter()
            _, rows = db.query(
                f"SELECT id FROM {table} WHERE ST_KNN_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, {k})"
            )
            lat.append(time.perf_counter() - t0)
            got = [r - 1 for r in ids_of(rows)]
            nret.append(len(got))
            _, d = knn_truth(P, q, k)
            rec.append(recall_ties(got, d, k) if got else 0.0)
            dg = d[got]
            ordered += int(np.all(np.diff(dg) >= -1e-9))
        r = float(np.mean(rec))
        log(f"  {label} ST_KNN_3D k={k:<3} recall {r:.4f}  ordered {ordered}/{len(Q)}  "
            f"returned {np.mean(nret):.1f}/{k}   [{lat_str(lat)}]")
        res[f"knn@{k}"] = {"recall": r, "ordered": ordered / len(Q),
                           "latency_ms_avg": float(np.mean(lat) * 1e3)}


def check_order_by(db, table, P, Q, k, label, res):
    rec, dist_ok, lat = [], 0, []
    max_rel_err = 0.0
    for q in Q:
        t0 = time.perf_counter()
        _, rows = db.query(
            f"SELECT id, ST_DISTANCE_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}) AS d "
            f"FROM {table} ORDER BY d LIMIT {k}"
        )
        lat.append(time.perf_counter() - t0)
        got = [r[0] - 1 for r in rows]
        _, d = knn_truth(P, q, k)
        rec.append(recall_ties(got, d, k) if got else 0.0)
        # Returned distance values must be the Euclidean distance.
        rel = [abs(r[1] - d[r[0] - 1]) / max(d[r[0] - 1], 1e-9) for r in rows]
        if rel:
            max_rel_err = max(max_rel_err, max(rel))
            dist_ok += int(max(rel) < 1e-3)
    r = float(np.mean(rec))
    log(f"  {label} ORDER BY ST_DISTANCE_3D LIMIT {k}: recall {r:.4f}  "
        f"distance values correct {dist_ok}/{len(Q)} (max rel err {max_rel_err:.3g})   [{lat_str(lat)}]")
    res[f"orderby@{k}"] = {"recall": r, "distance_values_ok": dist_ok / len(Q),
                           "max_rel_err": max_rel_err, "latency_ms_avg": float(np.mean(lat) * 1e3)}


def check_radius(db, table, P, Q, radii, label, res):
    for r in radii:
        prec, rec, lat, sizes = [], [], [], []
        for q in Q:
            t0 = time.perf_counter()
            _, rows = db.query(
                f"SELECT id FROM {table} WHERE ST_RADIUS_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, {r})"
            )
            lat.append(time.perf_counter() - t0)
            got = set(i - 1 for i in ids_of(rows))
            d = np.sqrt(((P - q) ** 2).sum(1))
            truth = set(np.nonzero(d <= r + 1e-9)[0].tolist())
            sizes.append(len(truth))
            if truth or got:
                inter = len(got & truth)
                prec.append(inter / len(got) if got else 1.0)
                rec.append(inter / len(truth) if truth else 1.0)
        log(f"  {label} ST_RADIUS_3D r={r:<5} precision {np.mean(prec):.4f}  recall {np.mean(rec):.4f}  "
            f"(avg {np.mean(sizes):.0f} pts)   [{lat_str(lat)}]")
        res[f"radius@{r}"] = {"precision": float(np.mean(prec)), "recall": float(np.mean(rec)),
                              "latency_ms_avg": float(np.mean(lat) * 1e3)}


def check_bbox(db, table, P, boxes, label, res):
    prec, rec, lat, sizes = [], [], [], []
    for lo, hi in boxes:
        t0 = time.perf_counter()
        _, rows = db.query(
            f"SELECT id FROM {table} WHERE ST_WITHIN_3D(pt, {fmt(lo[0])}, {fmt(lo[1])}, {fmt(lo[2])}, "
            f"{fmt(hi[0])}, {fmt(hi[1])}, {fmt(hi[2])})"
        )
        lat.append(time.perf_counter() - t0)
        got = set(i - 1 for i in ids_of(rows))
        inside = np.all((P >= lo - 1e-9) & (P <= hi + 1e-9), axis=1)
        truth = set(np.nonzero(inside)[0].tolist())
        sizes.append(len(truth))
        inter = len(got & truth)
        prec.append(inter / len(got) if got else 1.0)
        rec.append(inter / len(truth) if truth else 1.0)
    log(f"  {label} ST_WITHIN_3D: precision {np.mean(prec):.4f}  recall {np.mean(rec):.4f}  "
        f"(avg {np.mean(sizes):.0f} pts)   [{lat_str(lat)}]")
    res["bbox"] = {"precision": float(np.mean(prec)), "recall": float(np.mean(rec)),
                   "latency_ms_avg": float(np.mean(lat) * 1e3)}


def check_point_read(db, table, P, label, res):
    rng = np.random.default_rng(3)
    lat, ok = [], 0
    ids = rng.integers(1, len(P) + 1, 30)
    for i in ids:
        t0 = time.perf_counter()
        _, rows = db.query(f"SELECT id, pt FROM {table} WHERE id = {int(i)}")
        lat.append(time.perf_counter() - t0)
        ok += int(len(rows) == 1 and rows[0][1] is not None)
    log(f"  {label} point read of GEOMETRY column: {ok}/{len(ids)} non-NULL   [{lat_str(lat)}]  "
        f"(sample value: {rows[0][1]!r})")
    res["point_read_non_null"] = ok / len(ids)
    res["point_read_ms_avg"] = float(np.mean(lat) * 1e3)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200_000)
    ap.add_argument("--nq", type=int, default=100)
    ap.add_argument("--preset", default="general")
    ap.add_argument("--out", default=os.path.join(CACHE, "spatial_results.json"))
    args = ap.parse_args()

    import motedb
    log(f"motedb: {motedb.__file__}")
    P = indoor_scan(args.n)
    rng = np.random.default_rng(11)
    # Half the queries hug surfaces (a stored point + 5 cm offset), half are
    # free-space points anywhere in the room.
    near = P[rng.choice(len(P), args.nq // 2, replace=False)] + rng.normal(0, 0.05, (args.nq // 2, 3))
    free = rng.uniform([0, 0, 0], [20, 15, 3], size=(args.nq - args.nq // 2, 3))
    Q = np.round(np.vstack([near, free]), 4)
    boxes = []
    for _ in range(args.nq):
        lo = rng.uniform([0, 0, 0], [17, 12, 2])
        hi = lo + rng.uniform([0.3, 0.3, 0.3], [3, 3, 1.5])
        boxes.append((np.round(lo, 4), np.round(hi, 4)))
    log(f"corpus {len(P):,} points ({args.n:,} + 100 exact duplicates), {len(Q)} queries, preset={args.preset}")

    results = {"n": int(len(P)), "preset": args.preset}
    tmp = tempfile.mkdtemp(prefix="motedb_spatial_")
    path = os.path.join(tmp, "scan.mote")
    try:
        db = motedb.Database(path, preset=args.preset)
        db.execute("CREATE TABLE cloud (id INT PRIMARY KEY, pt GEOMETRY)")
        insert_points(db, "cloud", P)
        db.checkpoint()

        log("\n=== no index ===")
        r0 = {}
        try:
            _, rows = db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, 1.0, 1.0, 1.0, 5)")
            log(f"  ST_KNN_3D without index → {len(rows)} rows")
        except Exception as e:
            log(f"  ST_KNN_3D without index → error: {str(e)[:100]}")
        check_order_by(db, "cloud", P, Q[:20], 10, "no-index", r0)
        check_radius(db, "cloud", P, Q[:20], [0.3], "no-index", r0)
        check_bbox(db, "cloud", P, boxes[:20], "no-index", r0)
        results["no_index"] = r0

        log("\n=== CREATE OCTREE INDEX (bulk backfill) ===")
        t0 = time.perf_counter()
        db.execute("CREATE OCTREE INDEX cloud_pt ON cloud(pt)")
        db.checkpoint()
        results["index_build_s"] = time.perf_counter() - t0
        log(f"  built in {results['index_build_s']:.1f}s")
        r1 = {}
        check_knn(db, "cloud", P, Q, [1, 10, 100], "octree", r1)
        check_order_by(db, "cloud", P, Q, 10, "octree", r1)
        check_radius(db, "cloud", P, Q, [0.05, 0.3, 1.0], "octree", r1)
        check_bbox(db, "cloud", P, boxes, "octree", r1)
        check_point_read(db, "cloud", P, "octree", r1)
        # 2D sugar on the same 3D data: ST_KNN(pt,x,y,k) ≡ ST_KNN_3D with z=0.
        q = Q[0]
        _, rows = db.query(f"SELECT id FROM cloud WHERE ST_KNN(pt, {fmt(q[0])}, {fmt(q[1])}, 5)")
        got = [r - 1 for r in ids_of(rows)]
        _, d = knn_truth(P, np.array([q[0], q[1], 0.0]), 5)
        log(f"  2D ST_KNN(pt,x,y,5) ≡ 3D with z=0: recall {recall_ties(got, d, 5):.2f}")
        # COUNT(*) with a spatial predicate — does it take the index or the
        # per-row path?
        t0 = time.perf_counter()
        _, rows = db.query(f"SELECT COUNT(*) FROM cloud WHERE ST_RADIUS_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, 0.3)")
        t1 = time.perf_counter()
        d = np.sqrt(((P - q) ** 2).sum(1))
        log(f"  COUNT(*) WHERE ST_RADIUS_3D: {rows[0][0]} (truth {(d <= 0.3).sum()}) in {(t1 - t0) * 1e3:.1f}ms")
        t0 = time.perf_counter()
        _, rows = db.query(f"SELECT COUNT(*) FROM cloud WHERE ST_KNN_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, 10)")
        t1 = time.perf_counter()
        log(f"  COUNT(*) WHERE ST_KNN_3D k=10: {rows[0][0]} (truth 10) in {(t1 - t0) * 1e3:.1f}ms")
        results["octree"] = r1

        log("\n=== close + reopen ===")
        db.close()
        db = motedb.Database(path, preset=args.preset)
        r2 = {}
        check_knn(db, "cloud", P, Q[:50], [10], "reopen", r2)
        check_radius(db, "cloud", P, Q[:50], [0.3], "reopen", r2)
        results["reopen"] = r2

        log("\n=== DELETE 5% + UPDATE 5% (move to the ceiling corner) ===")
        n = len(P)
        del_ids = rng.choice(n, n // 20, replace=False)
        upd_ids = rng.choice(np.setdiff1d(np.arange(n), del_ids), n // 20, replace=False)
        t0 = time.perf_counter()
        for s in range(0, len(del_ids), 500):
            chunk = ",".join(str(int(i) + 1) for i in del_ids[s:s + 500])
            db.execute(f"DELETE FROM cloud WHERE id IN ({chunk})")
        for i in upd_ids:
            db.execute(f"UPDATE cloud SET pt = POINT3D(19.9, 14.9, 2.9) WHERE id = {int(i) + 1}")
        log(f"  {len(del_ids)} deletes + {len(upd_ids)} updates in {time.perf_counter() - t0:.1f}s")
        P2 = P.copy()
        P2[upd_ids] = [19.9, 14.9, 2.9]
        alive = np.ones(n, bool)
        alive[del_ids] = False
        # Deleted rows must never come back; moved rows must be found at the new spot.
        ghosts, found_moved, lat = 0, 0, []
        for q in Q[:50]:
            _, rows = db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, 20)")
            got = [r - 1 for r in ids_of(rows)]
            ghosts += sum(1 for i in got if not alive[i])
        _, rows = db.query("SELECT id FROM cloud WHERE ST_RADIUS_3D(pt, 19.9, 14.9, 2.9, 0.001)")
        got = set(i - 1 for i in ids_of(rows))
        found_moved = len(got & set(upd_ids.tolist()))
        stale = sum(1 for i in got if i not in set(upd_ids.tolist()) and not (np.abs(P2[i] - [19.9, 14.9, 2.9]) < 0.002).all())
        log(f"  ghosts of deleted rows in kNN top-20 over 50 queries: {ghosts}")
        log(f"  moved rows found at new location: {found_moved}/{len(upd_ids)}  (unexpected extra ids there: {stale})")
        # kNN recall against the post-edit truth (alive rows only).
        Pa = P2[alive]
        idx_map = np.nonzero(alive)[0]
        rec = []
        for q in Q[:50]:
            _, rows = db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, 10)")
            got = [r - 1 for r in ids_of(rows)]
            got_local = [np.searchsorted(idx_map, i) for i in got if alive[i]]
            _, d = knn_truth(Pa, q, 10)
            rec.append(recall_ties(got_local, d, 10) if got_local else 0.0)
        log(f"  kNN@10 recall after edits: {np.mean(rec):.4f}")
        results["after_edits"] = {"ghosts": ghosts, "moved_found": found_moved / len(upd_ids),
                                  "knn10_recall": float(np.mean(rec))}

        log("\n=== incremental inserts through the live index (+5,000 multi-row) ===")
        extra = np.round(rng.uniform([0, 0, 0], [20, 15, 3], size=(5000, 3)), 4)
        insert_points(db, "cloud", extra, id_offset=n)
        P3 = np.vstack([P2, extra])
        alive3 = np.concatenate([alive, np.ones(5000, bool)])
        Pa = P3[alive3]
        idx_map = np.nonzero(alive3)[0]
        rec, new_seen = [], 0
        for q in Q[:50]:
            _, rows = db.query(f"SELECT id FROM cloud WHERE ST_KNN_3D(pt, {fmt(q[0])}, {fmt(q[1])}, {fmt(q[2])}, 10)")
            got = [r - 1 for r in ids_of(rows)]
            new_seen += sum(1 for i in got if i >= n)
            got_local = [np.searchsorted(idx_map, i) for i in got if alive3[i]]
            _, d = knn_truth(Pa, q, 10)
            rec.append(recall_ties(got_local, d, 10) if got_local else 0.0)
        log(f"  kNN@10 recall with new rows: {np.mean(rec):.4f}  (new rows appearing in results: {new_seen})")
        results["after_incremental"] = {"knn10_recall": float(np.mean(rec)), "new_rows_seen": new_seen}
        db.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2, default=float)
    log(f"\nresults → {args.out}")


if __name__ == "__main__":
    main()
