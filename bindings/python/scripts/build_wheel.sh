#!/usr/bin/env bash
# Build a PEP 517-style wheel without maturin (for environments where pip
# can't fetch it). Prefer `maturin build --release` when available — this
# script exists because we hit exactly that case (flaky network + ancient
# pip). Produces: target/wheels/motedb-<ver>-cp39-abi3-<platform>.whl
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=$(grep -m1 '^version' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
cargo build --release

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/motedb" "$STAGE/motedb-$VERSION.dist-info"

# .so suffix: abi3 keeps one name across CPython versions.
case "$(uname)" in
  Darwin) EXT=abi3.so; TAG_PLAT="macosx_11_0_arm64" ;;
  Linux)  EXT=abi3.so; TAG_PLAT="manylinux_2_28_$(uname -m)" ;;
esac
cp "target/release/libmotedb.dylib" "$STAGE/motedb/motedb.$EXT" 2>/dev/null \
  || cp "target/release/libmotedb.so" "$STAGE/motedb/motedb.$EXT"

cat > "$STAGE/motedb/__init__.py" <<'PY'
from .motedb import Database, PyDatabase, __version__

__all__ = ["Database", "__version__"]
PY

cat > "$STAGE/motedb-$VERSION.dist-info/METADATA" <<META
Metadata-Version: 2.1
Name: motedb
Version: $VERSION
Summary: AI-native embedded multimodal database (SQL + vector + FTS + spatial)
License: MIT
Requires-Python: >=3.9
Description-Content-Type: text/markdown

Embedded multimodal database for embodied intelligence: SQL with ACID
transactions, DiskANN vector search, BM25 full-text search, spatial
indexing. https://github.com/motedb/motedb
META

cat > "$STAGE/motedb-$VERSION.dist-info/WHEEL" <<WHL
Wheel-Version: 1.0
Generator: motedb-handbuild
Root-Is-Purelib: false
Tag: cp39-abi3-$TAG_PLAT
WHL

python3 - "$STAGE" "$VERSION" <<'PY'
import hashlib, os, base64, sys
stage, version = sys.argv[1], sys.argv[2]
records = []
for root, _, files in os.walk(stage):
    for f in sorted(files):
        p = os.path.join(root, f)
        rel = os.path.relpath(p, stage).replace(os.sep, '/')
        if rel.endswith('RECORD'):
            continue
        h = base64.urlsafe_b64encode(
            hashlib.sha256(open(p, 'rb').read()).digest()
        ).rstrip(b'=').decode()
        records.append(f"{rel},sha256={h},{os.path.getsize(p)}")
records.append(f'motedb-{version}.dist-info/RECORD,,')
open(os.path.join(stage, f'motedb-{version}.dist-info/RECORD'), 'w').write(
    '\n'.join(records) + '\n')
PY

mkdir -p target/wheels
OUT="target/wheels/motedb-$VERSION-cp39-abi3-$TAG_PLAT.whl"
(cd "$STAGE" && zip -q -r - .) > "$OUT"
echo "built: $OUT ($(stat -f%z "$OUT" 2>/dev/null || stat -c%s "$OUT") bytes)"
