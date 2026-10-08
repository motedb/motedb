#!/usr/bin/env bash
# 在 Linux 容器内复现测评报告的 "首开 PK 点查 10× 慢于重开" 现象。
# 用法: bash scripts/repro_point_query_gap.sh [amd64|arm64]
set -euo pipefail
cd "$(dirname "$0")/.."

PLATFORM="${1:-arm64}"
case "$PLATFORM" in
  amd64) PLAT_FLAG="--platform linux/amd64" ;;
  arm64) PLAT_FLAG="--platform linux/arm64" ;;
esac

exec docker run --rm $PLAT_FLAG \
    -v "$PWD":/src -w /src \
    -v "$HOME/.cargo/registry":/usr/local/cargo/registry \
    -e CARGO_TARGET_DIR=/tmp/target \
    rust:latest bash -ceu '
    set -e
    echo "=== env ==="; uname -m; python3 --version
    echo "=== build wheel ==="
    cd bindings/python
    cargo build --release --offline 2>&1 | tail -1
    VERSION=$(grep -m1 "^version" Cargo.toml | sed "s/version = \"\(.*\)\"/\1/")
    STAGE=$(mktemp -d)
    mkdir -p "$STAGE/motedb" "$STAGE/motedb_python-$VERSION.dist-info"
    cp /tmp/target/release/libmotedb.so "$STAGE/motedb/motedb.abi3.so"
    cp python/motedb/__init__.py "$STAGE/motedb/__init__.py"
    cat > "$STAGE/motedb_python-$VERSION.dist-info/METADATA" <<META
Metadata-Version: 2.1
Name: motedb-python
Version: $VERSION
META
    echo "Wheel * * " > "$STAGE/motedb_python-$VERSION.dist-info/WHEEL"
    echo "motedb/__init__.py,motedb/motedb.abi3.so" > /dev/null
    cd "$STAGE"
    python3 -m ensurepip --upgrade >/dev/null 2>&1 || true
    python3 -m pip install --quiet numpy psutil 2>&1 | tail -1
    python3 -c "import numpy, psutil" || { echo "FATAL: numpy/psutil unavailable in container"; exit 1; }
    python3 -m pip install --quiet --force-reinstall --no-deps .
    python3 -c "import motedb; print(\"wheel ok\", motedb.__version__)"
    cd /src/bindings/python
    echo "=== bench (first-open vs reopen point query) ==="
    python3 bench/compete_bench.py --engine mote 2>/dev/null | grep -o "\"q_point[^,]*\|\"q_point_reopen[^,]*"
'
