#!/usr/bin/env bash
# §11 format-migration matrix: generate sample databases with each
# historical release's own code (git worktree per tag), so the current
# version's open-and-migrate path can be tested against real layouts.
#
# Usage: scripts/gen_format_fixtures.sh [tag ...]   (default: all matrix tags)
# Output: tests/fixtures/formats/<tag>/db/
#
# Regenerate when the matrix gains a new release tag:
#   scripts/gen_format_fixtures.sh v0.15.3
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -gt 0 ]; then TAGS=("$@"); else TAGS=(v0.12.9 v0.13.0 v0.14.0 v0.15.2); fi
OUT="tests/fixtures/formats"
WORKROOT="$(mktemp -d /tmp/motedb-fixtures.XXXXXX)"
trap 'git worktree list >/dev/null 2>&1; for t in "${TAGS[@]}"; do git worktree remove --force "$WORKROOT/$t" 2>/dev/null || true; done; rm -rf "$WORKROOT"' EXIT

for tag in "${TAGS[@]}"; do
    echo "── $tag ──────────────────────────────────────────"
    wt="$WORKROOT/$tag"
    git worktree add --detach "$wt" "$tag"
    mkdir -p "$wt/examples"
    cp scripts/gen_format_fixture.rs "$wt/examples/"
    dest="$PWD/$OUT/$tag"
    rm -rf "$dest"
    mkdir -p "$dest"
    (cd "$wt" && cargo run --release --quiet --example gen_format_fixture "$dest")
    du -sh "$dest/db" || true
    git worktree remove --force "$wt"
done
echo "done: $OUT"
