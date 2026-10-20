# Security Policy

MoteDB is an embedded library: it has no network surface of its own, and it
runs inside the trust boundary of the process that links it. Security here is
about **what untrusted bytes can do to that process** — SQL text, database
files, snapshot containers and robotics captures that may come from somewhere
else.

## Supported versions

Pre-1.0, fixes ship in the **latest 0.x minor** only. Older minors do not get
backports; upgrading is intended to be an in-place open (the engine migrates
older on-disk layouts automatically — see `RELEASE_NOTES.md`).

| Version | Supported |
|---|---|
| latest 0.x (currently 0.15.x) | ✅ |
| older | ❌ — upgrade |

## Reporting a vulnerability

Please **do not** open a public issue for anything exploitable.

1. Preferred: GitHub private vulnerability reporting —
   **Security → Report a vulnerability** on
   <https://github.com/motedb/motedb/security/advisories/new>.
2. Or email <motedb@126.com> with the subject `[motedb security]`.

Include the version, platform, a reproducer (a SQL statement, a crafted file,
or a binding call sequence) and what you observed. Encrypted mail is not
required.

What to expect: an acknowledgement within **72 hours**, a severity assessment
within a week, and for high-severity issues a fix target of **14 days** with
coordinated disclosure. You will be credited in the release notes unless you
prefer otherwise.

## What counts

In scope — a report is welcome if untrusted input can cause:

- Memory unsafety, panics or undefined behaviour from **SQL text** (parser,
  planner, executor, scalar functions, FTS query syntax).
- The same from **database files**: WAL records, columnar segments,
  `MANIFEST`, `meta.bin`, index files (BTree / Vamana / FTS / i-Octree) —
  including truncated, bit-flipped or adversarially crafted files.
- The same from **MOTESNAP containers** (`EXPORT DB TO`, `open_readonly`),
  including any way to make import write outside its target directory
  (path traversal is guarded; a bypass is a vulnerability).
- The same from **MCAP imports**.
- Soundness bugs in the existing `unsafe` islands: mmap access, SIMD distance
  kernels, FFI (`src/ffi.rs`) and what the Python / Node / Go bindings do
  with it (use-after-close, aliasing, lifetime bugs reachable from the
  scripting side).
- Durability/integrity violations that silently lose or corrupt committed
  data (we treat these as security-relevant, not just bugs).

Out of scope — please file a regular issue instead:

- Resource exhaustion from a legitimately large query or dataset opened by
  the same trust domain (documented limits, not vulnerabilities).
- Anything requiring the attacker to already have write access to the
  database directory with the host's own privileges.
- Issues in third-party dependencies with no MoteDB-specific exposure
  (report upstream; `cargo deny` tracks advisories in CI).

## Hardening already in place

- `cargo deny check advisories` on every push.
- Daily fuzzing under AddressSanitizer: `fuzz_sql_parser`, `fuzz_wal_recover`
  (`fuzz/README.md`); a parser stack overflow was found and fixed on day one.
- kill -9 crash-injection loop in CI for the durability path.
- CRC32 on manifest records, `meta.bin` sections and every file inside a
  snapshot container; corrupt catalog sections fail fast rather than
  silently resetting.
- Read-only container mode rejects all write paths at the engine level.
