#!/bin/bash -eu
# OSS-Fuzz build script for MoteDB's cargo-fuzz targets.
# Builds every target in fuzz/fuzz_targets/ with the libFuzzer engine and the
# sanitizer chosen by the OSS-Fuzz runtime ($SANITIZER).
#
# Copied to google/oss-fuzz/projects/motedb/build.sh.

cargo fuzz --version || cargo install cargo-fuzz --locked

# OSS-Fuzz provides $LIB_FUZZING_ENGINE (libFuzzer) and $SANITIZER flags via
# RUSTFLAGS; cargo-fuzz inherits them when build.sh runs from the fuzz dir.
cd "$SRC/motedb/fuzz"

# Some corporate environments need the exact toolchain pinned in rust-toolchain;
# cargo-fuzz uses the nightly toolchain by default, which OSS-Fuzz's rust image
# provides.
for target in $(cargo fuzz list); do
    cargo fuzz build -O --fuzz-target "$target"
done

# cargo-fuzz outputs to fuzz/target/<arch>/<sanitizer>/{release}/<target>;
# OSS-Fuzz expects the raw fuzzers in $OUT named without the "fuzz_" prefix.
find target -type f -name "fuzz_*" -perm -u+x -exec cp {} "$OUT/" \;
for f in "$OUT"/fuzz_*; do
    mv "$f" "$OUT/$(basename "$f" | sed 's/^fuzz_//')"
done
