#!/usr/bin/env bash
# §11 API freeze check: diff the crate's public API against the frozen
# baseline (docs/api/public-api-baseline.txt).
#
# Pass: additions only. Fail: any removed/changed item (a line starting with
# `<` after diffing baseline → current), which per VERSIONING.md is a 2.0-only
# change. Auto-trait impl noise (Freeze/Send/Sync/Unpin/UnwindSafe/RefUnwindSafe)
# is ignored — it shifts with internal field types, not user API.
#
# Usage:
#   scripts/check_public_api.sh               # check against baseline
#   scripts/check_public_api.sh --update      # rewrite the baseline (release time)
#
# Requires: cargo-public-api (cargo install cargo-public-api) and a nightly
# toolchain (rustdoc JSON).
set -euo pipefail
cd "$(dirname "$0")/.."

BASELINE="docs/api/public-api-baseline.txt"
CURRENT="$(mktemp)"
trap 'rm -f "$CURRENT"' EXIT

cargo public-api --simplified > "$CURRENT"

if [ "${1:-}" = "--update" ]; then
    cp "$CURRENT" "$BASELINE"
    echo "baseline updated: $(wc -l < "$BASELINE") items"
    exit 0
fi

if [ ! -f "$BASELINE" ]; then
    echo "no baseline at $BASELINE — run scripts/check_public_api.sh --update first" >&2
    exit 2
fi

REMOVED=$(diff "$BASELINE" "$CURRENT" | grep '^<' | grep -vE "Freeze|RefUnwindSafe|Send for|Sync for|Unpin for|UnwindSafe" || true)
ADDED=$(diff "$BASELINE" "$CURRENT" | grep -c '^>' || true)

if [ -n "$REMOVED" ]; then
    echo "❌ API freeze violation — removed or changed public items:" >&2
    echo "$REMOVED" | sed 's/^</  /' >&2
    echo "" >&2
    echo "Per VERSIONING.md removals/signature changes are 2.0-only." >&2
    echo "If this is an intentional pre-1.0 change, update the baseline with" >&2
    echo "  scripts/check_public_api.sh --update" >&2
    exit 1
fi

echo "✓ API freeze: no removals/changes (+$ADDED additions vs baseline)"
