# Install Troubleshooting

One engine, three registries. The names differ slightly per ecosystem — most
"it doesn't install" reports are one of the rows below.

| Language | Install | Package name | Import / use as |
|---|---|---|---|
| Rust | `cargo add motedb` | crates.io `motedb` | `use motedb::Database;` |
| Python | `pip install motedb-python` | PyPI `motedb-python` | `import motedb` |
| Node.js | `npm install motedb` | npm `motedb` (+ one `motedb-<platform>` optional dependency) | `require('motedb')` |
| Go | build from source | — | see `bindings/go/README.md` |

> There is **no** `pip install motedb` — the PyPI package is `motedb-python`
> (the module you import is still `motedb`).

## 0. Verify in ten seconds

```bash
# Python — prints the installed version and runs a vector query
python -c "import motedb, importlib.metadata as m; print(m.version('motedb-python')); db=motedb.Database('/tmp/v.mote'); db.execute('CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, e VECTOR(2))'); db.execute('INSERT INTO t VALUES (1,[1.0,1.0])'); print(db.execute('SELECT id FROM t ORDER BY e <-> [1.0,1.0] LIMIT 1'))"

# Node.js
node -e "const D=require('motedb'); const db=new D('/tmp/v.mote'); db.execute('CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, e VECTOR(2))'); console.log(require('motedb/package.json').version, db.query('SELECT COUNT(*) AS n FROM t'))"

# Rust — compile the README example against the published crate
cargo new smoke && cd smoke && cargo add motedb && curl -sL https://raw.githubusercontent.com/motedb/motedb/main/examples/hello_world.rs -o src/main.rs && cargo run
```

The repository ships the same checks as a script: `scripts/release_smoke.sh`
installs the published packages into throwaway environments and runs the
README examples. If it passes on your machine, the packages are fine and the
problem is in your project's setup.

## 1. Python

**`No matching distribution found for motedb-python`**
pip found no wheel for your platform/interpreter and there is no pure-Python
fallback. Prebuilt wheels cover:

| OS | Arch | Python |
|---|---|---|
| Linux (glibc / manylinux) | x86_64, aarch64 | ≥ 3.9 (abi3 — one wheel serves all newer versions, including 3.14) |
| macOS | arm64 (Apple Silicon), x86_64 | ≥ 3.9 |
| Windows | x86_64 | ≥ 3.9 |

Not covered: **musl Linux (Alpine)**, Windows arm64, 32-bit, FreeBSD. On those,
build from source — it needs Rust ≥ 1.87:

```bash
pip install maturin
git clone https://github.com/motedb/motedb && cd motedb/bindings/python
maturin build --release && pip install target/wheels/motedb_python-*.whl
```

Also check `pip --version` points at the interpreter you think it does
(`python -m pip install motedb-python` avoids that whole class of mistakes).

**`<jemalloc>: option background_thread currently supports pthread only`** on
`import motedb` (macOS only). Harmless stderr noise from the allocator used by
the macOS wheel; the Linux and Windows wheels use the system allocator and are
silent. Nothing is misconfigured — tracked as a cosmetic fix.

**`cannot allocate memory in static TLS block`** (Linux). Only affects very old
versions whose Linux wheel still bundled jemalloc. Upgrade:
`pip install -U motedb-python`.

**`AttributeError: module 'motedb' has no attribute ...`** — usually a stale
install shadowing the new one. Check which file is loaded and the version:

```bash
python -c "import motedb, importlib.metadata as m; print(motedb.__file__, m.version('motedb-python'))"
```

**Optional dependencies.** `insert_arrays` reads numeric and vector columns
through the buffer protocol, so pass `numpy` arrays there (text columns are
plain `str` lists); `fetch_arrays` imports `numpy` lazily and falls back to
`bytes` objects without it. `query_arrow` / `query_pandas` need `pyarrow`
(`pip install pyarrow`). None of these are pulled in automatically, to keep
edge installs small.

## 2. Node.js

**How loading works.** `motedb` is a pure-JS loader; the compiled addon lives
in one platform package pulled in through `optionalDependencies`:
`motedb-darwin-arm64`, `motedb-darwin-x64`, `motedb-linux-x64-gnu`,
`motedb-linux-arm64-gnu`, `motedb-win32-x64-msvc`. Exactly one of them should
be installed:

```bash
npm ls | grep motedb-
```

**`motedb native library not found for <platform>`** — the platform package is
missing. Causes, most common first:

1. Optional dependencies were skipped: `npm install --no-optional`,
   `--omit=optional`, or `NPM_CONFIG_OPTIONAL=false` in the environment.
2. A lockfile generated on another OS. Older npm versions recorded only the
   platform package of the machine that ran `npm install`; `npm ci` on a
   different OS then installs nothing usable. Regenerate the lockfile with
   npm ≥ 9, or install with `npm install --force`.
3. pnpm: add the target platforms to `supportedArchitectures` in
   `.npmrc`/`package.json` when deploying to a different OS/libc than the
   machine that resolved the lockfile.
4. An unlisted platform (e.g. Linux on riscv64, FreeBSD): build from source —
   see "Build from source" in `bindings/node/README.md`, then either copy the
   cdylib next to `index.js` as `motedb.<platform>-<arch>[-libc].node` or set
   `MOTEDB_NODE_LIB=/abs/path/to/motedb.node`.

**Alpine / musl.** No musl platform packages are published yet (the build
step exists in `node-publish.yml`; the matrix entries are enabled once a PR
run proves them). The loader already detects musl — from `/usr/bin/ldd` or
the process report — and will require `motedb-linux-<arch>-musl` as soon as
it exists; today it reports `not found for linux-x64-musl`, which is
expected: build from source inside the Alpine image (Rust ≥ 1.87,
`apk add build-base`). If the message says `-gnu` inside an Alpine container,
the detection itself failed — open an issue with the output of
`node -p "process.report.getReport().header"`.

**Electron.** The addon is built against Node-API, which is ABI-stable across
Node and Electron versions; no `electron-rebuild` needed.

**Version check:** `node -p "require('motedb/package.json').version"`.

## 3. Rust

**`package 'motedb' requires rustc 1.87 or newer`** — `rustup update stable`.

**Build time.** The first build compiles the engine plus `zstd-sys` (C) and,
on non-MSVC targets, jemalloc; expect a few minutes. Release builds use thin
LTO. Reuse the build cache across projects with
[`sccache`](https://github.com/mozilla/sccache) if you build often.

**Features.** Default = `tokenizer-jieba` + `rayon` + `jemalloc`. For small
edge binaries:

```toml
motedb = { version = "0.15", default-features = false, features = ["jemalloc"] }
```

On Windows MSVC the `jemalloc` feature is accepted but resolves to nothing
(the C allocator cannot build there); the system allocator is used.

**`<jemalloc>: option background_thread currently supports pthread only`** on
macOS — same harmless message as in Python above; disable the `jemalloc`
feature if stderr must stay clean.

**Cross-compiling to aarch64 Linux.** CI does it on every push; the toolchain
setup is in `.github/workflows/ci.yml` (cross-compile job). `cargo-zigbuild`
also works for both gnu and musl targets.

## 4. Go

The Go binding is cgo over the static library (`target/release/libmotedb.a`),
built from source. Follow `bindings/go/README.md`; `CGO_CFLAGS` /
`CGO_LDFLAGS` point it at a prebuilt library elsewhere.

## 5. Still stuck

Open a [Discussion](https://github.com/motedb/motedb/discussions) or an
issue using the **Installation problem** template, and paste the output of
the matching one-liner from section 0 plus:

```bash
uname -a                                  # or `ver` on Windows
python -V && python -m pip --version      # Python
node -v && npm -v                         # Node
rustc -V && cargo -V                      # Rust
```
