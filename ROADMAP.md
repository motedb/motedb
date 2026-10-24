# Roadmap

> MoteDB is **pre-1.0**, and this roadmap changes with community feedback.
> **Vote and propose in [Discussions](https://github.com/motedb/motedb/discussions).**

Legend: `🟢 shipped` · `🟡 in progress` · `🔵 planned` · `⚪ not planned` · `🔴 blocked`

---

## The problem we are solving

MoteDB exists so that **multimodal data on edge devices — vectors, time series,
text and binary blobs — is carried by one embedded SQL engine, with no
operations required from collection to transport to deployment.**

The roadmap runs on three tracks: **capability completion**, **distribution
reach**, and **community usability**.

---

## 1. Capability completion

### 🟢 v0.15.2 (current)

- Unified storage and query for multimodal data (VECTOR / TIMESTAMP / TEXT / BLOB)
- Five index families: column, vector (DiskANN), full-text (BM25), spatial
  (R-Tree), time-series
- Hybrid search: BM25 × KNN with RRF fusion
- Explicit window frames (`ROWS` / `RANGE BETWEEN`)
- Parquet export/import, LeRobot v3 dataset export, MCAP import
- Arrow / pandas interop for training pipelines
- Three shapes: collection directory → single-file snapshot (CRC + zstd) →
  read-only container
- Crash-injection, differential fuzzing, corruption-recovery and perf-gate
  checks in CI

### 🔵 Next up (v0.16 – v0.18)

**SQL surface**

- [ ] `DECIMAL` type
- [ ] `DATE` type
- [ ] Cross-statement server-side cursors
- [ ] BLOB scalar functions and BLOB output under `ORDER BY` (known gap since v0.13)

**Performance and scale**

- [ ] `LATEST BY` performance on tables > 1M rows (large-table merge path)
- [ ] Segment-level min/max pruning
- [ ] Dictionary-encoded aggregation path
- [ ] Time-series segment rewrite

**Developer experience**

- [ ] Python type stubs (`.pyi`) + annotations
- [ ] Richer interactive CLI shell

---

### ⚪ Not planned (with reasons)

| Item | Why |
|---|---|
| Server mode / multi-process concurrent writes | Contradicts the "embedded, zero-ops" positioning. Open an issue and describe your scenario if you need it |
| Distributed / multi-node replication | The current target is single-machine edge. We would like to hear the fleet-scale scenario spelled out |
| Built-in inference engine (ONNX and similar) | Outside the product boundary. We would rather make Arrow interop excellent |
| Multi-user permission system | The host application owns that in embedded deployments |

---

## 2. Distribution reach

### 🟢 Shipped

- [x] Rust crate on crates.io — `cargo add motedb`
- [x] Node.js package on npm — `npm install motedb` (v0.15.2)
- [x] Python package on PyPI — `pip install motedb-python` (imported as `motedb`)
- [x] Prebuilt binaries for macOS (arm64 / x64) and Linux (x64 / arm64)

### 🔵 Planned

- [ ] npm prebuilt packages for **musl** (x64 / arm64) and **Windows x64** — the
      npm package declares them as optional dependencies, but they are not
      published yet, so `npm install motedb` currently fails on Windows and on
      Alpine-based images
- [ ] Publish the **`motedb`** name on PyPI (the package is currently
      `motedb-python`)
- [ ] Publish a **tagged Go module release** for the Go binding (today the
      proxy only resolves a pseudo-version off the default branch)
- [ ] Standalone documentation site (versioned `docs/`, with site search)
- [ ] Homebrew tap
- [ ] conda-forge recipe
- [ ] Official Docker Hub / GHCR images (including arm64)
- [ ] Wider prebuilt matrix: aarch64 Linux, armv7, riscv64

---

## 3. Community usability

### 🟡 In progress

- [ ] GitHub Discussions — being enabled now
- [x] Community foundation files: CONTRIBUTING, Code of Conduct, SECURITY,
      GOVERNANCE
- [x] Issue and pull-request templates
- [x] Published first-response commitment (≤ 48 hours)
- [ ] Mailing list (release notices + community newsletter)

### 🔵 Planned

- [ ] A continuously refreshed `good first issue` armory
- [ ] Show & Tell section and a showcase area on the website
- [ ] Monthly community newsletter
- [ ] Fortnightly office hours
- [ ] Quarterly challenge: "put your robot on motedb"
- [ ] Ecosystem integrations — **done**: ROS 2 / Foxglove bags via MCAP import,
      LeRobot v3 datasets via `EXPORT LEROBOT` · **next**: Isaac Lab

---

## Vote: what do you want most?

**Reply with your priority in [Discussions](https://github.com/motedb/motedb/discussions)**
and the order will follow the votes:

1. ☐ Distributed / multi-node sync (fleet scenarios)
2. ☐ Server mode
3. ☐ Built-in inference / ONNX integration
4. ☐ A stronger vector index than the current DiskANN
5. ☐ Encryption / access control
6. ☐ A graphical query tool (CLI TUI or web UI)
7. ☐ More language bindings (Java / C# / Swift)
8. ☐ Deeper Arrow / DataFusion integration
9. ☐ A fuller set of time-series analysis functions (window functions,
   interpolation, downsampling)
10. ☐ Something else (write it in a discussion)

---

## Release cadence

**We do not promise dates.** The release rhythm of an embedded database should
follow completeness, not the calendar.

Every release ships with: a CHANGELOG (including breaking changes and
credits), an updated performance report, and a contributor list.

---

*Want to shape the roadmap? Open an [issue](https://github.com/motedb/motedb/issues)
or a [discussion](https://github.com/motedb/motedb/discussions).*
