# MCAP Import (Robotics Data On-ramp)

[MCAP](https://mcap.dev) is the container format used by `ros2 bag` and
Foxglove. MoteDB imports MCAP recordings so the "worth remembering" slice of
a robot's data — sightings, state transitions, decoded signals — becomes
queryable next to your other tables. Bags stay the system of record for
full-rate capture and replay; the database is for asking questions.

## What you get

One table per topic:

```sql
CREATE TABLE topic__joint_states (
    _uid         INTEGER PRIMARY KEY,  -- MCAP message id (dedup key)
    log_time     TIMESTAMP,            -- auto time index
    publish_time TIMESTAMP,
    data         BLOB                  -- raw message bytes
    -- + any extra columns your decode callback returns
)
```

- `_uid` is the MCAP message id: **re-importing the same file is a no-op**
  (crash-safe resumes — an interrupted import can simply be re-run).
- `log_time` gets an automatic time index: `WHERE log_time BETWEEN …` is a
  range scan, not a full read.
- `data` keeps the raw payload; decode typed fields at import time (Python,
  below) or in SQL via `LENGTH(data)`-style BLOB functions.

## CLI

```bash
motedb import mcap robot.mote recording.mcap \
  --include joint_states --include /tf \
  --exclude diagnostics \
  --prefix topic
```

| Option | Meaning |
|---|---|
| `--include SUBSTR` | import only topics whose name contains SUBSTR (repeatable) |
| `--exclude SUBSTR` | drop matching topics (wins over include) |
| `--prefix P` | table-name prefix (default `topic` → `topic__<name>`) |

## Python (with typed decode)

```python
import motedb

db = motedb.Database("robot.mote")

def decode_joint(ts_ns: int, raw: bytes) -> dict:
    # ROS2 CDR / protobuf decoding stays at the Python boundary
    return {"pos": [0.0, 1.5, -0.2], "effort": 3.25}

summary = db.import_mcap(
    "recording.mcap",
    include=["joint_states"],   # substring filter, optional
    exclude=["diagnostics"],
    prefix="topic",
    decode={"/joint_states": decode_joint},  # topic-substring → callable
)
for t in summary["topics"]:
    print(t["topic"], "->", t["table"], t["rows"], "rows")
```

The `decode` callback receives `(log_time_ns, raw_bytes)` and returns a dict
of extra columns (values: `int` / `float` / `str` / `bytes` /
`list[float]` — lists land in a `VECTOR` column, so decoded embeddings are
KNN-queryable right away).

## Rust

```rust
use motedb::mcap_import::{import_mcap_file, McapImportOptions};

let opts = McapImportOptions {
    prefix: "topic".into(),
    include: vec!["joint_states".into()],
    exclude: vec![],
    batch_rows: 1000,
};
// The 4th argument is a per-message decode hook (like Python's):
// FnMut(topic: &str, log_time_ns: u64, raw: &[u8]) -> HookColumns.
// Return empty HookColumns to keep just the raw BLOB row.
let summary = import_mcap_file(&db, "recording.mcap".as_ref(), &opts,
                               |_topic, _ts, _raw| Default::default())?;
```

## Bags record, the database remembers

The intended split:

| | rosbag / MCAP | MoteDB |
|---|---|---|
| Full-rate sensor streams | ✅ | — |
| Replay & visualization (Foxglove) | ✅ | — |
| "Where was the cup at 10:02?" | — | ✅ |
| FTS + vector + spatial + time in one query | — | ✅ |

Import the topics worth querying, keep bagging the rest.

---

- Previous: [20 MCP Server](./20-mcp.md)
- Next: [Benchmark Methodology](./benchmark_methodology.md)
