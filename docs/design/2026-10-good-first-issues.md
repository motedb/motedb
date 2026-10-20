# good first issue 草稿(10 条,待在 GitHub 上创建)

> 状态:**草稿**(2026-10-20)。Phase 0 社区地基的一部分(见
> `2026-10-industry-parity-plan.md` §4)。每条都锚定到本次 Phase 0 排查时
> 实际观察到的事实,带验收判据,新手可独立完成。正文为英文,可直接粘贴为
> issue;标签建议:`good first issue` + 一个领域标签。
> 创建方式:GitHub 网页逐条粘贴,或 `gh issue create --title … --body-file …
> --label "good first issue,<area>"`(需先在仓库建好标签)。

---

## 1. Silence the jemalloc `background_thread` warning on macOS

**Labels:** good first issue · rust · dx

Every `import motedb` (macOS wheel) and every Rust binary built with the
default `jemalloc` feature prints this to stderr on macOS:

```
<jemalloc>: option background_thread currently supports pthread only
```

It is harmless — jemalloc simply has no background purging thread on macOS —
but it is the first thing a new user sees. The `background_threads` cargo
feature of `tikv-jemallocator` is what requests it (`Cargo.toml`, the
`[target.'cfg(not(target_env = "msvc"))'.dependencies]` block). Gate that
feature off for `target_os = "macos"` (split the target table into
macOS / everything-else-non-MSVC) while keeping
`unprefixed_malloc_on_supported_platforms` everywhere. The Python binding on
macOS pulls `jemalloc` from the root crate (`bindings/python/Cargo.toml`), so
one change fixes both.

**Acceptance:** on macOS, `cargo run --example hello_world 2>&1 >/dev/null`
and `python -c "import motedb" 2>&1` print nothing; `cargo test --lib
--all-features` still green on Linux and macOS; `docs/18-install-troubleshooting.md`
drops the two paragraphs about the message.

## 2. Publish musllinux wheels for the Python binding (Alpine)

**Labels:** good first issue · ci · python

`pip install motedb-python` has no wheel for musl Linux, so Alpine containers
fall back to a source build that needs Rust 1.87. The Node binding already
ships musl binaries (`node-publish.yml`); do the same for Python in
`.github/workflows/python-wheels.yml`: add `x86_64-unknown-linux-musl` and
`aarch64-unknown-linux-musl` matrix entries using `maturin-action` with
`manylinux: musllinux_1_2`. Note the Linux Python build is already
`default-features = false` (no jemalloc), so no allocator concerns.

**Acceptance:** the wheels job uploads `*musllinux_1_2_x86_64*` and
`*musllinux_1_2_aarch64*`; a smoke step in a `python:3.12-alpine` container
runs the install + import + one SQL statement; `docs/18-install-troubleshooting.md`
platform table gains the musl row; `release-smoke.yml` gets a `python-alpine`
job mirroring `node-alpine`.

## 3. Compile the README code blocks as doctests

**Labels:** good first issue · docs · rust

`README.md` and `examples/hello_world.rs` already drifted once (different row
sets), and the README's install snippets were two minor versions stale until
2026-10. Make the README's Rust blocks compile in `cargo test`:

```rust
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme {}
```

Non-Rust blocks (` ```sql `, ` ```python `, ` ```bash `, ` ```toml `) are
ignored by rustdoc automatically; the Rust block(s) need to be complete
programs or marked ` ```rust,no_run ` / ` ```rust,ignore ` where they rely on
files. Keep `cargo test --lib` fast — doctests run in `cargo test --doc`.

**Acceptance:** `cargo test --doc` compiles every Rust block in README;
introducing a typo into the README snippet fails CI; CONTRIBUTING's "Docs move
with code" bullet mentions it.

## 4. Clean up `motedb.__version__` in the Python package

**Labels:** good first issue · python

`bindings/python/python/motedb/__init__.py` derives `__version__` through a
leftover hack (`__import__("_native_version_hack", …) if False else None`)
and a hard-coded `"0.12.0"` fallback. Replace with
`importlib.metadata.version("motedb-python")` first, falling back to the
native module's version attribute, and delete the dead line.

**Acceptance:** `python -c "import motedb; print(motedb.__version__)"` prints
the installed version with and without the package metadata present (e.g.
`maturin develop`); a unit test in `bindings/python/tests/` asserts
`motedb.__version__ == importlib.metadata.version("motedb-python")`.

## 5. README *SQL Support*: document `CREATE TABLE IF NOT EXISTS` and audit for other supported-but-unlisted syntax

**Labels:** good first issue · docs

The parser accepts `CREATE TABLE IF NOT EXISTS` (`src/sql/parser.rs`, around
the "Optional IF NOT EXISTS" comment) but the README's *SQL Support* section
does not list it. Walk the parser's statement/clause entry points and compare
against the README list; add what is missing (candidates to check: `CREATE
INDEX IF NOT EXISTS`, `INSERT OR IGNORE`, `LIKE`/`IN`/`BETWEEN`/`EXISTS`,
`CAST`, `EXPLAIN`, `LATEST BY`, `TIME_BUCKET`, `ST_*` functions).

**Acceptance:** every syntax the parser accepts and tests cover is listed once
in README *SQL Support* with a one-line description; nothing listed is
unsupported.

## 6. English docstrings for Python `insert_arrays` / `fetch_arrays`

**Labels:** good first issue · python · docs

The two highest-value Python APIs have Chinese-only docstrings
(`bindings/python/src/lib.rs`, the `#[pyo3]` methods `insert_arrays` and
`fetch_arrays`). Add an English description first (dtype mapping, zero-copy
conditions, NULL/TEXT fallbacks, return shape) and keep the Chinese text below
it.

**Acceptance:** `help(motedb.Database.insert_arrays)` shows English + Chinese;
`docs/14-api-reference.md` links to or mirrors the same text.

## 7. CI lint for shell scripts and workflow files

**Labels:** good first issue · ci

`scripts/*.sh` (compete, release smoke, e2e) and `.github/workflows/*.yml`
have no linter. Add a small `lint-infra` job to `ci.yml` running
`shellcheck` on `scripts/*.sh` and `actionlint` on the workflows (both have
GitHub Actions wrappers). Fix whatever they flag in the existing files.

**Acceptance:** the job runs on pull requests touching those paths and passes;
a deliberate shellcheck violation fails it.

## 8. Node `index.d.ts` audit + `tsc` type check in the build-only path

**Labels:** good first issue · node

Make sure every method the native `Database` exposes (`execute`, `query`,
`checkpoint`, `backupTo`, `exportSnapshot`, `openSnapshot`, `openReadonly`,
`close`, `doctor`, …) has a typing with a JSDoc line in
`bindings/node/index.d.ts`, and add a `tsc --noEmit` check of a tiny
TypeScript consumer to the PR (build-only) path of `node-publish.yml` so the
typings cannot silently drift from `src/lib.rs`.

**Acceptance:** a TypeScript file calling each method compiles; removing a
method from `index.d.ts` fails the PR check.

## 9. `motedb-cli doctor --json`

**Labels:** good first issue · cli

`motedb-cli doctor <path>` prints a human PASS/WARN report and exits 0/1/2.
Fleet tooling wants the same as JSON. Add a `--json` flag that emits one
object (`{status, checks:[{name, status, detail}], …}`) built from the same
`DoctorStatus` data the text renderer uses (`src/bin/motedb-cli.rs` and the
`doctor()` API in the engine), without changing the exit codes.

**Acceptance:** `motedb-cli doctor --json db.mote | jq .status` works;
output is stable (keys documented in `docs/14-api-reference.md`); text mode
unchanged; a CLI test in `bindings/python/tests/test_e2e_cli.py` or a Rust
integration test covers it.

## 10. Make `docs/01-quick-start.md` snippets a compiled example

**Labels:** good first issue · docs · rust

The quick start's Rust snippets are hand-maintained and were two minor
versions stale until 2026-10. Move them into `examples/quick_start.rs` (one
runnable program following the doc's sections) and have the doc reference the
file's sections (or embed via `include_str!`-style markers) so the code is
compiled by `cargo build --examples` in CI.

**Acceptance:** `cargo run --example quick_start` runs clean; every code
block in the doc corresponds to a block in the example; CONTRIBUTING mentions
"docs snippets live in examples/".
