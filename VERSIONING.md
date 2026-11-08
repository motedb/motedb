# MoteDB Versioning Policy

This file defines MoteDB's semantic-versioning (semver) policy and the
**storage-format compatibility policy** — for an embedded database the latter
directly determines whether users can upgrade, and is the core of the 1.0
promise.

## 1. semver (code API)

- **0.x (current)**: minor (`0.15 → 0.16`) may break APIs; patch releases
  only fix bugs. The Rust crate / Python wheel / npm package share one
  version number.
- **1.x**: minor may add functionality and refactor without changing the
  semantics of existing signatures; any breaking change (removing/renaming
  public items, changing observable behavior) waits for **2.0**.
- Public surface:
  - Rust: public items of the `motedb` crate (re-exports + `pub` modules).
    Before release, diff the `cargo public-api` inventory (`docs/api/`);
    anything not an addition requires an explanation.
  - Python: classes and methods of the `motedb` package (frozen list in
    `docs/14-api-reference.md`).
  - Node: exported objects and TS types of the `motedb` npm package.
- SQL surface follows "additions don't break": new syntax/functions don't
  bump minor and aren't breaking; changing the return semantics of existing
  statements is breaking and waits for 2.0.

## 2. Storage-format versioning

**Promise: within 1.x, any 1.x version opens a database directory/snapshot
written by any earlier 1.x/0.15+ version, readable without lossy migration.**

- When the write format evolves, **the read side must keep decoding the old
  format**; the immutable-segment design lets most optimizations land as
  "new segments take effect" while old segments stay readable in their old
  format — this is the precondition for not bumping to 2.0.
- Breaking format changes (requiring rewrite / undecodable old segments)
  can only appear in **2.0**, accompanied by a `motedb-migrate` tool.
- Format version markers:
  - columnar MANIFEST: embedded `VERSION` (currently 2; readers reject
    higher versions with an explicit "database written by a newer MoteDB");
  - `.mote` snapshot container: magic + version in the header, same policy;
  - WAL frame format: fields are append-only, never reordered.
- Historical layouts from 0.12 → 0.15 migrate automatically on open
  (forward migration stays throughout 1.x; migration happens once at open
  and never touches the original files until checkpoint).

## 3. Release process

1. Tagging `vX.Y.Z` triggers: crates.io publish → PyPI (wheel ×8 platforms,
   trusted publishing) → npm (root + platform packages, idempotent and
   retryable) → release-smoke (install smoke across 8 platforms).
2. A failure on one end never blocks the others; failures open an issue
   automatically, and recovery = re-run the same workflow (idempotent).
3. RELEASE_NOTES.md is updated every release; any format change must be
   declared there and reflected in section 2 of this file.

## 4. Deprecation policy

APIs deprecated within 1.x: marked `#[deprecated]` (Rust) / documented
(Python/Node), kept for at least 2 minor versions before removal; removal
itself is 2.0.
