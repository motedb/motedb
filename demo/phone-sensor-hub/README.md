# Pocket Sensor Hub — your Android phone as an edge database node

A ~350-line MoteDB demo that runs **on an Android phone** (via Termux) and
exercises the full edge story: capture → per-minute aggregation → activity
fingerprints → on-device similarity search → single-file snapshot for the
laptop.

```
Termux:API sensor stream (JSON, 5–50 Hz)
        │
        ▼
  sensor_hub ingest          ──►  sensor  (TIMESERIES + 2-day TTL)
        │                          minute_fp  (VECTOR(8) per minute)
        │                          events     (|acc| > 15 m/s² → shake)
        │                          minute_stats (continuous view)
        ▼
  sensor_hub report           ──► per-minute acc_power table in your terminal
  sensor_hub find-similar <ts> ─► KNN: "which earlier minute felt like this?"
  sensor_hub export <file>    ──► one .mote snapshot → adb pull → pandas
```

## On the phone (Termux)

1. Install [Termux](https://f-droid.org/en/packages/com.termux/) and the
   **Termux:API** companion app (both from F-Droid — the Play build is stale).
2. In Termux:

```bash
pkg update && pkg install rust git binutils termux-api
git clone https://github.com/motedb/motedb
cd motedb/demo/phone-sensor-hub
cargo run --release -- ingest < <(termux-sensor -s 200 -d 0)   # Ctrl-C to stop
cargo run --release -- report
cargo run --release -- export /sdcard/sensor.mote
```

`termux-sensor -s 200` streams accelerometer + gyroscope every 200 ms.
Carry the phone around for a few minutes — walk, shake it, leave it on a
desk — then `report` and `find-similar`.

## Off-phone (macOS/Linux) — simulated data

```bash
python3 simulate.py > sim.jsonl       # 20 min: idle / walk / idle / shake / walk
cargo run --release -- ingest < sim.jsonl
cargo run --release -- report
# KNN sanity: the three activity types retrieve their own phases
cargo run --release -- find-similar $(( $(head -1 sim.jsonl | python3 -c \
  'import json,sys;print(json.loads(sys.stdin.readline())["timestamp"])') + 8*60*1000 ))
```

## What each piece demonstrates

| Piece | MoteDB capability |
|---|---|
| `sensor` table | `TIMESERIES(ts) TTL` — bounded retention on edge devices (TTL actually evicts: point the simulator's clock a few days back and the rows vanish) |
| `minute_stats` | `CREATE CONTINUOUS VIEW` + `TIME_BUCKET('1m')` — define once, query like a table |
| `minute_fp` | `VECTOR(8)` columns with string-literal inserts |
| `find-similar` | `ORDER BY fp <-> '[…]'` KNN over fingerprints |
| `export` | single-file snapshot (CRC per file) — the transport step; open read-only anywhere with `Database::open_readonly` |
| build | `default-features = false` — no jemalloc/jieba/rayon, the smallest cross-target surface (Termux `aarch64-linux-android`) |

## Notes

- The demo depends on the **published crate** (`motedb` 0.18.x from
  crates.io), not the workspace — it is intentionally standalone.
- Fingerprints are quantile statistics of |acc| and gyro energy per minute —
  deliberately simple; swap in your own features and the KNN query stays
  identical.
