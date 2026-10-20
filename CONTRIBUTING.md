# Contributing to MoteDB

Thanks for your interest. MoteDB is a pre-1.0 embedded database with an
unusually strict quality bar for its size — crash-injection CI, daily fuzzing,
a machine-independent perf gate, differential tests against SQLite. Most of
this guide exists to help you clear that bar on the first try.

- **Questions / "is this a bug?"** → [GitHub Discussions](https://github.com/motedb/motedb/discussions)
- **Bugs / install problems / feature ideas** → the issue templates
- **First contribution?** → issues labelled
  [`good first issue`](https://github.com/motedb/motedb/labels/good%20first%20issue)
- **Bigger changes** → open a Discussion first; features land behind a short
  design note in `docs/design/` (see any `2026-10-*.md` there for the shape:
  goal / design anchored to existing code / files touched / tests /
  acceptance criteria)

Chinese or English are both fine everywhere — issues, PRs, commit messages,
design notes. The history is mostly Chinese; the user-facing docs are English.

## Development setup

| Component | Needs |
|---|---|
| Engine (Rust) | Rust ≥ 1.87 via `rustup` (the crate's `rust-version`) |
| Python binding | Python ≥ 3.9, `pip install maturin` |
| Node binding | Node ≥ 18, `npm install --no-save @napi-rs/cli` inside `bindings/node` |
| Go binding | Go ≥ 1.21, a C toolchain for cgo |
| Fuzzing | nightly toolchain + `cargo +nightly install cargo-fuzz` |

```bash
git clone https://github.com/motedb/motedb && cd motedb
cargo build
cargo test --lib --all-features        # the fast gate — seconds
```

## The gates (what CI runs, and how to run it locally)

| Gate | Command | Notes |
|---|---|---|
| Format | `cargo fmt --check` | strict — fails CI |
| Lint | `cargo clippy --all-targets --all-features --no-deps` | advisory today; keep new code warning-free |
| Supply chain | `cargo deny check advisories licenses sources` | every new dependency must pass; install with `cargo install cargo-deny --locked` |
| Unit | `cargo test --lib --all-features` | ~400 inline tests, runs in seconds, on ubuntu + macOS + arm64 |
| Integration | `CI=1 cargo test --workspace --all-features -- --test-threads=1` | slow (250+ suites). Run just what you touched: `cargo test --test <suite_name>`. `CI=1` skips hardware-threshold tests (RSS/latency) that only pass on known hardware |
| Crash injection | kill -9 loop in CI | touching WAL / checkpoint / manifest / recovery → run `cargo test --test test_crash_recovery` and friends locally |
| Perf gate | `cargo build --release --example perf_smoke && ./target/release/examples/perf_smoke` | latency *ratios* vs a full-scan baseline, so it is machine independent; it catches complexity-class regressions, not 5% drifts |
| Fuzz | `cd fuzz && cargo +nightly fuzz run fuzz_sql_parser` | see `fuzz/README.md`; parser or recovery changes should get a few minutes of fuzzing |
| Python binding | `cd bindings/python && maturin develop --release && python -m pytest tests e2e` | |
| Node binding | `cd bindings/node && cargo build --release && cp target/release/libmotedb_node.* motedb.<platform>.node && node smoke_ci.js` | see `bindings/node/README.md` for the exact file name |
| Go binding | `cargo build --release && cd bindings/go && go test ./...` | |
| Release smoke | `make release-smoke-local` | installs the working tree the way a user would (wheel / cdylib / path dep) and runs the README examples — run before tagging |

## Making a change

1. **Branch from `main`**, keep PRs focused. One behavior change per PR is the
   norm here; refactors travel separately from fixes.
2. **Tests are not optional.** New behavior gets a test; a bug fix gets a
   regression test that fails before the fix. SQL surface changes should also
   extend `tests/test_fuzz_sql_combinations` templates and, where SQLite has
   the same feature, the differential tests (`tests/test_differential_*`).
3. **Storage format changes must stay readable from previous versions.** Add
   a migration test (open a fixture written by the previous layout) and read
   `docs/design/2026-10-file-layout.md` first — the "temp + fsync + rename",
   orphan-adoption and immutable-segment rules are load-bearing.
4. **Public API.** Pre-1.0 we may break the Rust `pub` surface, but never
   casually: call it out in the PR. FFI, Python, Node and Go signatures move
   together — a new engine capability is "done" when all wired bindings expose
   it (see how `export_snapshot` / `open_readonly` landed in 0.15).
5. **Docs move with code.** If the SQL surface changes, update the
   `SQL Support` section of `README.md` and the relevant `docs/*.md`. Add a
   line to `CHANGELOG.md` under the upcoming version heading (create
   `## [Unreleased]` if it does not exist yet).
6. **Logging** goes through the `log` facade macros (`debug_log!`,
   `warn_log!`, …) — never `eprintln!`/`println!` in library code. A library
   must not write to stderr unconditionally.
7. **Commit messages**: `type(scope): summary`, type ∈ `feat` `fix` `perf`
   `docs` `test` `refactor` `chore` `ci`. Example from the log:
   `fix(ckpt): F2 脏标记在持久化失败时保留 — AUTO_INCREMENT 不再可能静默丢失`.

The pull request template has the checklist version of the above.

## Code style

- `rustfmt` defaults; CI is strict about it.
- Comments state constraints the code cannot show (invariants, why a lock is
  held here, what a crash in this window would do) — not what the next line
  does. Match the comment density of the file you are in.
- No new `unsafe` without a `// SAFETY:` comment stating the invariant that
  makes it sound. mmap, SIMD distance kernels and FFI are the existing
  `unsafe` islands; keep it that way.
- Avoid adding dependencies. If one is needed, it must pass `cargo deny`, and
  edge builds (`default-features = false`) should not pay for it — gate it
  behind a feature when it is not core.

## Benchmarks and performance claims

Do not paste ad-hoc timing numbers into PRs or docs. Use the reproducible
harness — `make compete-quick` (or `make compete` for the core set) — and
attach the JSON from `benchmark_results/<timestamp>/`. The methodology and the
reasons the numbers are comparable are in `docs/benchmark_methodology.md`.
Performance claims in `README.md` cite the harness that produced them.

## Reporting security issues

Please do not open public issues for exploitable bugs — see
[`SECURITY.md`](SECURITY.md).

## License

MIT. By contributing you agree that your contributions are licensed under the
same terms.
