#!/usr/bin/env python3
"""
Full-text search accuracy evaluation.

Corpus: 100K real sentences from all-nli (cached from the vector eval).
Reference: a from-scratch BM25 in numpy that mirrors the engine's documented
parameters — whitespace-ish tokenizer (lowercase, split on non-alphanumeric
except '_', tokens 1..64 chars), k1 = 1.5, b = 0.75,
idf = ln(1 + (N - df + 0.5) / (df + 0.5)), score = Σ_t idf·tf·(k1+1) /
(tf + k1·(1 - b + b·dl/avgdl)).

Checks:
  * bare MATCH set semantics (index path) vs the reference OR-token set
  * ranked top-k (the fast path returns BM25 order): overlap@k + order
  * case / punctuation / unknown-term behaviour
  * compound predicates: MATCH … AND other (known to drop the AND)
  * aggregates: COUNT(*) WHERE MATCH (known broken)
  * lifecycle: CHECKPOINT, reopen, DELETE, UPDATE, +multi-row inserts
  * Chinese with ngram(2) vs a reference n-gram set matcher
  * index vs no-index semantics divergence
  * latency + index build time

Usage: python3 text_eval.py [--n 100000] [--nq 60]
"""
import argparse
import json
import os
import re
import shutil
import tempfile
import time

import numpy as np

CACHE = os.path.expanduser("~/.cache/motedb_eval")
TOKEN_RE = re.compile(r"[^0-9a-zA-Z_]+")


def log(msg=""):
    print(msg, flush=True)


# ---------------------------------------------------------------- reference
def tokenize(text):
    return [t for t in TOKEN_RE.split(text.lower()) if 1 <= len(t) <= 64]


def parse_groups(query):
    """Mirror src/index/text_query.rs: uppercase OR splits OR-of-AND groups,
    uppercase AND is a no-op separator, everything else is a term."""
    groups, current = [], []
    for w in [w for w in TOKEN_RE.split(query) if w]:
        if w == "OR":
            if current:
                groups.append(current)
                current = []
        elif w == "AND":
            pass
        else:
            current.append(w.lower())
    if current:
        groups.append(current)
    return [sorted(set(g)) for g in groups if g]


class ReferenceBM25:
    def __init__(self, docs, quantize_fieldnorm=False):
        self.docs = [tokenize(d) for d in docs]
        self.N = len(self.docs)
        self.avgdl = max(np.mean([len(d) for d in self.docs]), 1.0)
        self.quantize = quantize_fieldnorm
        self.df = {}
        for d in self.docs:
            for t in set(d):
                self.df[t] = self.df.get(t, 0) + 1

    def _dl(self, i):
        dl = len(self.docs[i])
        if not self.quantize:
            return dl
        # Mirror FieldNormTable: byte = clamp(round(log2(dl/avg)*16 + 128)),
        # decoded back as 2^((byte-128)/16) * avgdl.
        byte = int(round(np.log2(dl / self.avgdl) * 16 + 128))
        byte = max(0, min(255, byte))
        return (2.0 ** ((byte - 128) / 16.0)) * self.avgdl

    def idf(self, t):
        df = self.df.get(t, 0)
        if df == 0:
            return None
        return float(np.log(1.0 + (self.N - df + 0.5) / (df + 0.5)))

    def match_set(self, query):
        """OR-of-AND-groups (the engine's FTS5-compatible semantics since
        0.12): a doc matches when it contains every token of at least one
        group; uppercase OR splits groups, implicit AND within a group."""
        groups = parse_groups(query)
        if not groups:
            return set()
        return {
            i
            for i, d in enumerate(self.docs)
            if any(all(t in d for t in g) for g in groups)
        }

    def score(self, i, terms):
        s = 0.0
        dl = self._dl(i)
        for t in terms:
            tf = self.docs[i].count(t)
            if tf == 0:
                continue
            idf = self.idf(t)
            if idf is None:
                continue
            s += idf * tf * 2.5 / (tf + 1.5 * (1 - 0.75 + 0.75 * dl / self.avgdl))
        return s

    def topk(self, query, k):
        terms = tokenize(query)
        cands = self.match_set(query)
        scored = sorted(((self.score(i, terms), i) for i in cands), reverse=True)
        return scored[:k]


def esc(s):
    return s.replace("'", "''")


def lat_str(lat):
    lat = np.array(lat) * 1e3
    return f"avg {lat.mean():.2f}ms  p50 {np.percentile(lat, 50):.2f}  p95 {np.percentile(lat, 95):.2f}"


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100_000)
    ap.add_argument("--nq", type=int, default=60)
    ap.add_argument("--out", default=os.path.join(CACHE, "text_results.json"))
    args = ap.parse_args()

    import motedb

    # Documents: the cached all-nli sentences (id = index + 1).
    import glob
    npy = sorted(glob.glob(os.path.join(CACHE, "nli-corpus-220000-*.npy")))
    if not npy:
        raise SystemExit("run vector_recall_eval.py first (embedding cache doubles as the corpus)")
    # We need the raw texts, not embeddings — rebuild from the dataset.
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
                if len(docs) >= args.n:
                    break
        if len(docs) >= args.n:
            break
    log(f"corpus: {len(docs):,} unique real sentences")

    ref = ReferenceBM25(docs)
    vocab = [t for t, df in ref.df.items() if df >= 5]
    vocab.sort(key=lambda t: ref.df[t])
    rng = np.random.default_rng(5)
    queries = []
    # rare / mid / common single terms
    for df_target in (5, 200, 5000):
        pool = [t for t in vocab if abs(ref.df[t] - df_target) <= df_target // 3]
        queries += [pool[i] for i in rng.choice(len(pool), size=6, replace=False)]
    # term pairs (co-occurring and not)
    for _ in range(12):
        a, b = rng.choice(vocab, size=2)
        queries.append(f"{a} {b}")
    queries = queries[: args.nq]

    results = {}
    tmp = tempfile.mkdtemp(prefix="motedb_text_")
    path = os.path.join(tmp, "text.mote")
    try:
        t0 = time.perf_counter()
        db = motedb.Database(path, preset="general")
        db.execute("CREATE TABLE docs (id INT PRIMARY KEY, cat TEXT, content TEXT)")
        for s in range(0, len(docs), 2000):
            chunk = docs[s:s + 2000]
            vals = ",".join(
                f"({s+j+1}, '{chr(97 + (s+j) % 4)}', '{esc(t)}')" for j, t in enumerate(chunk)
            )
            db.execute(f"INSERT INTO docs VALUES {vals}")
        log(f"  inserted {len(docs):,} docs in {time.perf_counter() - t0:.1f}s")
        t0 = time.perf_counter()
        db.execute("CREATE TEXT INDEX docs_content ON docs(content)")
        build_s = time.perf_counter() - t0
        log(f"  CREATE TEXT INDEX: {build_s:.1f}s")
        results["index_build_s"] = build_s

        # ---- 1. bare MATCH set semantics (OR over tokens)
        prec, rec, lat = [], [], []
        for q in queries:
            t0 = time.perf_counter()
            _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}')")
            lat.append(time.perf_counter() - t0)
            got = {r[0] - 1 for r in rows}
            truth = ref.match_set(q)
            inter = len(got & truth)
            if truth or got:
                prec.append(inter / len(got) if got else 1.0)
                rec.append(inter / len(truth) if truth else 1.0)
        log(f"  MATCH set vs reference AND-group set: precision {np.mean(prec):.4f}  recall {np.mean(rec):.4f}   [{lat_str(lat)}]")
        results["match_set"] = {"precision": float(np.mean(prec)), "recall": float(np.mean(rec)),
                                "latency_ms_avg": float(np.mean(lat) * 1e3)}

        # ---- 2. ranked top-k quality (BM25). Doc IDs at the top-k boundary
        # are arbitrary when many docs tie on score (same tf + length), so
        # compare SCORES: the best returned doc's reference score vs the best
        # reference score (coverage), and the returned docs' min reference
        # score vs the reference k-th best (boundary). Ordering: returned docs'
        # reference scores must be non-increasing.
        k = 10
        refq = ReferenceBM25(docs, quantize_fieldnorm=True)
        best_cov, boundary_ok, order_ok, lat = [], 0, 0, []
        for q in queries[:30]:
            t0 = time.perf_counter()
            _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT {k}")
            lat.append(time.perf_counter() - t0)
            got = [r[0] - 1 for r in rows]
            terms = tokenize(q)
            ref_scored = sorted(((refq.score(i, terms), i) for i in refq.match_set(q)), reverse=True)
            if not got or not ref_scored:
                continue
            ref_best = ref_scored[0][0]
            ref_k = ref_scored[min(k, len(ref_scored)) - 1][0]
            got_scores = [ref.score(i, terms) for i in got]
            best_cov.append(max(got_scores) / ref_best if ref_best > 0 else 1.0)
            boundary_ok += int(min(got_scores) >= ref_k - 1e-4)
            order_ok += int(all(got_scores[i] >= got_scores[i + 1] - 1e-6 for i in range(len(got_scores) - 1)))
        n = len(best_cov)
        log(f"  MATCH … LIMIT 10 vs reference BM25: best-score coverage {np.mean(best_cov):.4f}  "
            f"boundary-ok {boundary_ok}/{n}  order-consistent {order_ok}/{n}   [{lat_str(lat)}]")
        results["ranked_top10"] = {"best_coverage": float(np.mean(best_cov)),
                                   "boundary_ok": boundary_ok / n,
                                   "order_consistent": order_ok / n,
                                   "latency_ms_avg": float(np.mean(lat) * 1e3)}

        # ---- 3. case / punctuation / unknown terms
        for label, q, expect_any in [
            ("upper-case query", queries[0].upper(), True),
            ("unknown term only", "zzzqqqxyzzw", False),
            # AND semantics: a known term ANDed with an unknown one is empty.
            ("term AND unknown", f"{queries[0]} zzzqqqxyzzw", False),
            # …while explicit OR is the escape hatch back to the term alone.
            ("term OR unknown", f"{queries[0]} OR zzzqqqxyzzw", True),
        ]:
            _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 5")
            got_n = len(rows)
            if expect_any:
                log(f"  {label:22} -> {got_n} rows")
            else:
                log(f"  {label:22} -> {got_n} rows (expect 0)")

        # ---- 4. compound predicate (KNOWN BUG: AND dropped)
        q = queries[0]
        _, rows_all = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 200")
        _, rows_a = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') AND cat = 'a' LIMIT 200")
        truth_a = {i for i in ref.match_set(q) if (i % 4) == 0}
        wrong = sum(1 for (r,) in rows_a if (r - 1) % 4 != 0)
        log(f"  MATCH AND cat='a': returned {len(rows_a)} rows, {wrong} violate cat='a' (truth set size {len(truth_a)})")
        results["compound_and_violations"] = wrong

        # ---- 5. aggregate (KNOWN BUG)
        try:
            cols, rows = db.query(f"SELECT COUNT(*) FROM docs WHERE MATCH(content, '{esc(q)}')")
            truth_n = len(ref.match_set(q))
            log(f"  COUNT(*) WHERE MATCH: rows={rows!r} (truth {truth_n})")
            results["count_match"] = {"returned": str(rows), "truth": truth_n}
        except Exception as e:
            log(f"  COUNT(*) WHERE MATCH: ERR {str(e)[:60]}")

        # ---- 5b. index vs no-index semantics divergence (before edits, so
        # both sides hold the same documents)
        db.execute("CREATE TABLE noidx (id INT PRIMARY KEY, content TEXT)")
        for s in range(0, 2000, 1000):
            vals = ",".join(f"({s+j+1}, '{esc(t)}')" for j, t in enumerate(docs[s:s+1000]))
            db.execute(f"INSERT INTO noidx VALUES {vals}")
        diverged = 0
        for q in queries[:20]:
            _, w = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') AND id <= 2000")
            _, wo = db.query(f"SELECT id FROM noidx WHERE MATCH(content, '{esc(q)}')")
            if {r[0] for r in w} != {r[0] for r in wo}:
                diverged += 1
        log(f"  indexed vs no-index MATCH divergence: {diverged}/20 queries give different sets")
        results["index_vs_noindex_divergence"] = diverged

        # ---- 6. lifecycle
        n = len(docs)
        del_ids = rng.choice(n, n // 20, replace=False)
        upd_ids = rng.choice(np.setdiff1d(np.arange(n), del_ids), n // 20, replace=False)
        t0 = time.perf_counter()
        for s in range(0, len(del_ids), 500):
            chunk = ",".join(str(int(i) + 1) for i in del_ids[s:s + 500])
            db.execute(f"DELETE FROM docs WHERE id IN ({chunk})")
        for i in upd_ids:
            db.execute(f"UPDATE docs SET content = 'kumquat zephyr document {int(i)}' WHERE id = {int(i) + 1}")
        log(f"  {len(del_ids)} deletes + {len(upd_ids)} updates in {time.perf_counter() - t0:.1f}s")
        alive = np.ones(n, bool)
        alive[del_ids] = False
        docs_upd = list(docs)
        for i in upd_ids:
            docs_upd[i] = f"kumquat zephyr document {i}"
        ref2 = ReferenceBM25([d for d, a in zip(docs_upd, alive) if a])
        id_map = [i for i in range(n) if alive[i]]
        # deleted docs must vanish
        ghosts = 0
        for q in queries[:20]:
            _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 100")
            ghosts += sum(1 for (r,) in rows if not alive[r - 1])
        # updated docs: old terms gone for their id, new terms findable
        _, rows = db.query("SELECT id FROM docs WHERE MATCH(content, 'kumquat') LIMIT 100000")
        found_moved = len({r - 1 for r, in rows} & set(upd_ids.tolist()))
        # set accuracy after edits
        prec, rec = [], []
        for q in queries[:20]:
            _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(q)}') LIMIT 100000")
            got = {r - 1 for r, in rows}
            # ref2's indices are positions in the alive-only list; map back to
            # the original doc ids (the old code mixed the two id spaces).
            tset = {id_map[j] for j in ref2.match_set(q)}
            inter = len(got & tset)
            prec.append(inter / len(got) if got else 1.0)
            rec.append(inter / len(tset) if tset else 1.0)
        log(f"  after edits: ghosts {ghosts}, moved-docs findable {found_moved}/{len(upd_ids)}, set precision {np.mean(prec):.4f} recall {np.mean(rec):.4f}")
        # incremental multi-row inserts
        extra = [f"brand new sentence number {i} with quokka" for i in range(5000)]
        vals = ",".join(f"({n + j + 1}, 'a', '{esc(t)}')" for j, t in enumerate(extra))
        db.execute(f"INSERT INTO docs VALUES {vals}")
        _, rows = db.query("SELECT id FROM docs WHERE MATCH(content, 'quokka') LIMIT 10")
        log(f"  +5,000 multi-row inserts: 'quokka' finds {len(rows)} rows (expect 10)")
        db.checkpoint()
        db.close()
        db = motedb.Database(path, preset="general")
        _, rows = db.query(f"SELECT id FROM docs WHERE MATCH(content, '{esc(queries[0])}') LIMIT 5")
        log(f"  after reopen: MATCH returns {len(rows)} rows")

        # ---- 7. Chinese with ngram(2)
        zh_words = ["机器人", "传感器", "数据库", "自动驾驶", "深度学习", "云平台", "操作系统", "神经网络"]
        zh = []
        for i in range(3000):
            ws = [zh_words[j] for j in rng.choice(len(zh_words), size=rng.integers(2, 5))]
            zh.append("".join(ws))
        db.execute("CREATE TABLE zh (id INT PRIMARY KEY, content TEXT)")
        for s in range(0, len(zh), 1000):
            vals = ",".join(f"({s+j+1}, '{esc(t)}')" for j, t in enumerate(zh[s:s+1000]))
            db.execute(f"INSERT INTO zh VALUES {vals}")
        try:
            db.execute("CREATE TEXT INDEX zh_content ON zh(content) USING TOKENIZER ngram(2)")
        except Exception as e:
            log(f"  ngram(2) tokenizer: CREATE failed: {str(e)[:70]}")
        else:
            prec = rec = None
            zhq = zh_words[0]  # 机器人
            grams = [zhq[i:i+2] for i in range(len(zhq) - 1)]
            truth = {i for i, t in enumerate(zh) if any(g in t for g in grams)}
            try:
                _, rows = db.query(f"SELECT id FROM zh WHERE MATCH(content, '{zhq}') LIMIT 5000")
                got = {r - 1 for r, in rows}
                inter = len(got & truth)
                prec = inter / len(got) if got else 1.0
                rec = inter / len(truth) if truth else 1.0
                log(f"  Chinese ngram(2) '{zhq}': precision {prec:.4f} recall {rec:.4f} (truth {len(truth)})")
            except Exception as e:
                log(f"  Chinese ngram(2) MATCH: ERR {str(e)[:70]}")
            results["zh_ngram2"] = {"precision": prec, "recall": rec}

        db.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2, default=float)
    log(f"\nresults → {args.out}")


if __name__ == "__main__":
    main()
