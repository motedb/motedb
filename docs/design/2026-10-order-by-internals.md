# A 1M-row ORDER BY that took 1.2 seconds

> Public write-up, kept in the repo so the claims stay checkable. Every number
> below comes from the benchmark scripts referenced in the text; the
> regression test is `tests/test_timeseries_semantics.rs::ts_order_limit_int_ts_column`.

### How three stacked bugs turned "SELECT the 10 newest rows" into a 680× regression — and how an adversarial benchmark against SQLite and DuckDB is what found them

*MoteDB is an embedded multimodal database written in Rust. One SQL engine carries vectors, time
series, full text and binary in the same table. MIT licensed. This post is about one specific
optimization, in detail, because the interesting part is not the number — it's how the number
turned out to be wrong in three different ways at once.*

---

## The setup

Last autumn I wrote a benchmark that puts MoteDB next to SQLite and DuckDB on workloads neither
of them was designed for: one million time-series rows across 64 sensors with 2% out-of-order
arrivals, plus 500K 3D points in a LiDAR-ish indoor shape. The script lives in the repo as
`bindings/python/bench/compete_spatial_ts.py`.

The point of writing it was to find the *shape* of the gaps, not to win a number. Which is why
the most useful thing it produced was not a benchmark result at all.

It was a row in the output that said `ts_order_limit: 1196 ms`.

`ORDER BY ts DESC LIMIT 10` on one million rows, in an embedded columnar store, took **1.196
seconds**. Not "slower than I'd like" — a full second to return ten rows, on a query that any
reasonable database should answer in well under a millisecond.

## The symptom was an EXPLAIN plan that lied

Before touching anything, I checked what the engine *thought* it was doing:

```
EXPLAIN SELECT ts, note FROM t ORDER BY ts DESC LIMIT 10
-- => topk_by_ts  (fast path)
```

The top-k kernel was selected. The plan was correct. The plan was a lie.

This is the uncomfortable part of the story, and I'd rather state it plainly than bury it: the
fast path existed, the planner picked it, the plan printed it, and the kernel was **never
entered**. The Python binding's streaming entry point simply didn't have a route to
`try_ts_order_limit` — it fell through to the general path, which does a full scan and a sort.
EXPLAIN was reporting the fast path because the planner had no way to know the executor would
never get there.

A plan that is correct about the algorithm and wrong about the routing is worse than no plan at
all, because it stops you from looking.

## Three bugs, stacked

Once I got into the kernel, the slowness turned out to be three separate defects layered on top
of each other. Fixing any one of them would have made the benchmark look better and left the
system just as broken.

### Bug 1 — the decoder only understood one of the two timestamp encodings

`src/storage/columnar/store.rs`, in `topk_by_ts`, pass 1 reads the `ts` column of each segment
and decodes it to microseconds:

```rust
let ts_micros = match ts_block.encoding {
    ColumnEncoding::GorillaTimestamp => {
        gorilla::decode_timestamps(&ts_block.data, segment.row_count as usize)
    }
    ColumnEncoding::DeltaVarint => {
        gorilla::decode_integers(&ts_block.data, segment.row_count as usize)
    }
    _ => continue, // non-gorilla ts: caller falls back to the general path
};
```

That second arm is the fix. Before it, the match had exactly one arm — `GorillaTimestamp` — and
everything else hit `continue`.

And here is the trap: **MoteDB has two timestamp representations.** A `TIMESTAMP` column is
stored with a Gorilla timestamp encoding. An `INT` column is stored with DeltaVarint, the
integer cousin of the same encoding family. Both decode to `i64` microseconds and both are
perfectly valid `ts` values for a `TIMESERIES` table — the SQL dialect explicitly supports
`ts INT`.

So on a `TIMESERIES(ts)` table where `ts` happened to be declared `INT`, pass 1 decoded
**nothing at all**. It walked every segment, hit `continue` on every one, and returned an empty
candidate set.

This is the kind of bug that is invisible in a test suite. If every test uses `TIMESTAMP`, the
one-armed match is correct for every test. My regression test now pins the `INT` case explicitly.

### Bug 2 — segment metadata was never written for `INT` columns

With pass 1 returning nothing, the obvious question is: why didn't the zone-map prune everything
and fail fast? Because it *didn't* have any metadata to prune with.

`src/storage/columnar/write_buffer.rs` keeps a separate tracking buffer per column type:

```rust
pub(crate) fn push(&mut self, value: Value) {
    match (self, value) {
        (Self::Timestamp(v), Value::Timestamp(ts)) => v.push(Some(ts.as_micros())),
        (Self::Timestamp(v), Value::Null) => v.push(None),
        (Self::Integer(v), Value::Integer(i)) => v.push(Some(i)),
        ...
        // Type mismatch: push NULL to keep column alignment
        (Self::Timestamp(v), _) => v.push(None),
```

An `INT` column lands in `Self::Integer`, not `Self::Timestamp`. The timestamp tracker is a
*typed* buffer, so nothing ever wrote the segment's `min_timestamp` / `max_timestamp`. Every
segment in an `INT`-ts table carried the metadata `(0, 0)`.

And `(0, 0)` is a perfectly ordinary-looking value. The zone-map gate compares
`segment.max_timestamp >= threshold`; with a threshold of, say, 1.7 × 10¹⁵, a segment claiming
its newest row is from epoch zero gets pruned. Correct behaviour for real metadata, catastrophic
for missing metadata.

### Bug 3 — the zone-map gate had no way to say "I don't know"

The gate was written assuming that `(0, 0)` means what it says. The fix has to distinguish
"this segment genuinely contains no timestamps" from "this segment's timestamps were never
tracked":

```rust
// 🔑 K1: zone-map gate — skip segments whose extremes can't reach
// the threshold. ⚠️ Legacy segments written before the INT-ts
// zone-map fix carry (0,0) metadata with rows > 0 — never prune.
let metadata_alive =
    segment.max_timestamp != 0 || segment.min_timestamp != 0 || segment.row_count == 0;
let qualifies = if !metadata_alive {
    true
} else if desc {
    segment.max_timestamp >= threshold
} else {
    segment.min_timestamp <= threshold.wrapping_neg()
};
```

`metadata_alive` is a "do I actually know anything?" check, not a value check. Segments we have
no information about are never pruned. This is the kind of guard that should have been there
from the start, and the comment above it now says so out loud so the next person doesn't
"optimize" it away.

### The fourth problem, which was not a bug but a waste

Even with all three fixed, the kernel was wasteful in a way I hadn't expected. Pass 2 re-read
and re-decoded the **entire segment's needed columns once per surviving row**. For a top-k query
that returns 10 rows, that's up to 10 full-segment decodes. The fix was to group by segment
and decode each segment's columns exactly once, then filter rows in memory.

## What it looks like now

```
1M rows, ORDER BY ts DESC LIMIT 10

before:  1,196 ms
after:      1.76 ms      (680×)
```

For reference, on the same machine and the same data:

| Engine | `ORDER BY ts DESC LIMIT 10` over 1M rows | Note |
|---|---|---|
| **MoteDB** | **1.76 ms** | full columnar scan + top-k, no composite index |
| DuckDB | 2.1 ms | full scan |
| SQLite | 0.006 ms | **composite index `(sid, ts DESC)` hit** |

Read that table honestly: **SQLite wins this shape, and by a lot.** Its 0.006 ms is an index
lookup — `ORDER BY` on an indexed column with a `LIMIT` is the one case where a B-tree beats
every columnar scan engine, because the answer is ten rows and the index knows where the ten
rows are. My scan does not.

We can have the same thing. A homologous `(sid, ts DESC)` index is a legitimate project for this
engine, and it isn't the one I did here. But calling a 1.76 ms full scan "680× faster" while
quietly omitting that a competitor answers in 6 microseconds would be exactly the kind of
number that gets a database project caught out in eighteen months.

## The numbers in that table came from an adversarial harness, not a stopwatch

I don't trust benchmark numbers I produced, especially mine, and especially when I wrote the
engine. So the numbers come from a harness that tries to prove them wrong:

`bindings/python/bench/adversarial_verify.py` runs 400+ randomized queries and compares the
result **set** against SQLite as the oracle — not the timing, the actual rows. FTS match sets are
compared against SQLite's FTS5. Vector recall is compared against a numpy exact search. Dirty
data, update/delete re-verification, kill -9 crash parity, and cold-process latency are all
in the same gate.

The time-series and spatial evaluations (`ts_eval.py`, `spatial_eval.py`) run the same idea
against MoteDB's own semantics: set equality, ordering, reopen behaviour, all of it.

The regression test for this specific bug is
`tests/test_timeseries_semantics.rs::ts_order_limit_int_ts_column`, and it covers the
dimensions that actually broke: before and after checkpoint, `ASC` as well as `DESC`, `OFFSET`,
and top-1 correctness.

## The part I'd still call unfinished

While measuring this, the same benchmark surfaced that `LATEST BY` is still an O(N) per-row
`HashMap` fold: 176 ms on 1M rows, against DuckDB's `arg_max` at 1.5 ms. The fix is
per-segment max-ts metadata plus a merge, the same shape as the zone-map work above, and it is
planned but not done. I'm noting it here because a blog post that only lists wins is marketing,
and the interesting question for anyone reading this is "what does this thing still do badly?"
— which is also the only question that tells you whether the benchmarks are honest.

For what it's worth, the same run found MoteDB ahead on the rest of that workload: range
aggregation at p50 0.004 ms (zone-map pruning; SQLite 4.4 ms, DuckDB 0.41 ms), spatial
`bbox WITHIN` at p50 0.001 ms (SQLite 0.39 ms, DuckDB 1.33 ms), and KNN-10 at p50 0.002 ms
(SQLite's full scan 41 ms, DuckDB 1.6 ms). But those are different shapes of query, and the
comparison that matters is the one where the other engine wins.

## How this stays from regressing

Two gates, both in CI:

- **`cargo fmt --check`** — strict, hard gate. The tree is fmt-clean.
- **`perf-gate.yml`** — asserts core query-shape latencies **as a ratio against the full-scan
  baseline**, not as absolute milliseconds. The point is to be machine-independent so a slow CI
  runner doesn't flake the build. The budgets carry roughly 2× headroom over the measured
  ratios. They exist to catch *complexity-class* regressions — the kind where a text-equality
  path that should be O(N) accidentally becomes O(N²) — not percent-level noise.
- `cargo-deny` runs the supply-chain audit (vulnerabilities + licenses).
- Integration tests run the full workspace suite twice, plus a native arm64 test runner,
  because arm64 is the actual deployment target.

## If you want to see it move

The 680× is in a 17-second clip:

> **`motedb-clip-perf-landscape-en.mp4`** — 1M rows, one `ORDER BY`, 1.2 s → 1.76 ms.
> (`motedb-videos.zip`)

The engine is here:

```bash
cargo add motedb
```

The benchmark that found the bug is here:
`bindings/python/bench/compete_spatial_ts.py`. Run it against SQLite and DuckDB yourself —
if my numbers are wrong, the script is the thing that proves it, and I would genuinely like to
be corrected.

---

*MoteDB is MIT licensed. Vector search, time series, full text and binary in one embedded SQL
engine, with Rust, Python and Node.js bindings.*
