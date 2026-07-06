# Fuzz targets for MoteDB

These targets use [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) +
libFuzzer to hunt for panics/aborts on untrusted input. They live in a separate
crate (`motedb-fuzz`) so the `libfuzzer-sys` dependency is never pulled into a
downstream consumer's build.

## Setup

Requires the nightly toolchain and `cargo-fuzz`:

```bash
rustup toolchain install nightly
cargo +nightly install cargo-fuzz
```

## Targets

| Target | What it fuzzes | Risk it addresses |
|--------|----------------|-------------------|
| `fuzz_sql_parser` | `Lexer::tokenize` → `Parser::parse` on arbitrary bytes | The hand-written recursive-descent parser (~1780 lines): stack overflow on deep nesting, str-slice panics on multi-byte UTF-8, index panics on malformed tokens. |
| `fuzz_wal_recover` | `WALManager::open` + `recover()` on an arbitrary `.wal` file | Crash recovery on a partially-written / corrupted WAL (e.g. power loss mid-write): out-of-bounds reads, integer-overflow in frame lengths, decode panics. |

## Running

```bash
# Build only (verifies the harness compiles; no fuzzing):
cargo +nightly fuzz build

# Run a target for a fixed wall-clock budget (recommended):
cargo +nightly fuzz run fuzz_sql_parser -- -max_total_time=60 -max_len=4096
cargo +nightly fuzz run fuzz_wal_recover  -- -max_total_time=60 -max_len=4096

# Run continuously until a crash is found:
cargo +nightly fuzz run fuzz_sql_parser

# Reproduce a crash found earlier:
cargo +nightly fuzz run fuzz_sql_parser fuzz/artifacts/fuzz_sql_parser/<crash-file>

# Minimize a crashing input:
cargo +nightly fuzz tmin fuzz_sql_parser fuzz/artifacts/fuzz_sql_parser/<crash-file>
```

Crashes are written to `fuzz/artifacts/<target>/`. Corpus is under
`fuzz/corpus/<target>/` (seed it with interesting inputs to speed coverage).

## Findings

- **`fuzz_sql_parser` (2026-07):** found a panic in `Lexer::current_utf8_char` —
  slicing `&str` at a byte offset that landed inside a multi-byte UTF-8
  sequence. Fixed in `src/sql/lexer.rs` by decoding from the raw byte slice;
  regression test `test_lexer_multibyte_utf8_no_panic` added. After the fix the
  target ran 1.1M+ iterations without a crash.
