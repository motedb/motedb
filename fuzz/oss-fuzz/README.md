# OSS-Fuzz integration (staged)

This directory stages the three files required to onboard MoteDB into
[OSS-Fuzz](https://github.com/google/oss-fuzz) (continuous fuzzing with the
libFuzzer engine + AddressSanitizer, free for open source):

- `project.yaml` — project metadata
- `Dockerfile` — build image
- `build.sh` — builds every target in `fuzz/fuzz_targets/` via cargo-fuzz

## How to submit

1. Fork https://github.com/google/oss-fuzz
2. Copy the three files to `projects/motedb/` in your fork
3. Open a pull request against google/oss-fuzz (maintainers review build
   scripts interactively — expect one or two iterations)
4. Once merged, fuzzing runs continuously; the "Fuzzing: OSS-Fuzz" badge
   (status badge URL from the OSS-Fuzz project page) goes into the README.

The existing targets (`fuzz_sql_parser`, `fuzz_wal_recover`) run untrusted
input through the parser and WAL recovery paths; new targets should follow
the same rule of thumb — fuzz the boundary where bytes meet logic.
