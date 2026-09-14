#!/usr/bin/env bash
# Build motedb-cli + the Python wheel inside a clean Linux container and run
# the external-integration E2E workload there. Uses the LOCAL rust:latest
# image and the host's cargo registry cache, so no registry access needed
# (the configured Docker mirror is unreachable in this environment).
#
#   bash scripts/docker_e2e.sh          # builds + runs E2E in docker
#
set -euo pipefail
cd "$(dirname "$0")/.."

exec docker run --rm \
    -v "$PWD":/src -w /src \
    -v "$HOME/.cargo/registry":/usr/local/cargo/registry \
    -e MOTE_CLI=/usr/local/bin/motedb-cli -e CARGO_TARGET_DIR=/tmp/target \
    rust:latest bash -ceu '
    echo "=== environment ==="
    uname -m; cat /etc/os-release | head -1; python3 --version; cargo --version

    echo "=== build motedb-cli ==="
    cargo build --release --offline -p motedb --bin motedb-cli
    cp target/release/motedb-cli /usr/local/bin/motedb-cli
    motedb-cli --version

    echo "=== build python wheel (hand-rolled, no zip/maturin needed) ==="
    cd bindings/python
    cargo build --release --offline
    VERSION=$(grep -m1 "^version" Cargo.toml | sed "s/version = \"\(.*\)\"/\1/")
    STAGE=$(mktemp -d)
    mkdir -p "$STAGE/motedb" "$STAGE/motedb-$VERSION.dist-info"
    cp /tmp/target/release/libmotedb.so "$STAGE/motedb/motedb.abi3.so"
    cat > "$STAGE/motedb/__init__.py" <<PY
from .motedb import Database, PyDatabase, __version__

__all__ = ["Database", "__version__"]
PY
    cat > "$STAGE/motedb-$VERSION.dist-info/METADATA" <<META
Metadata-Version: 2.1
Name: motedb
Version: $VERSION
Requires-Python: >=3.9
META
    cat > "$STAGE/motedb-$VERSION.dist-info/WHEEL" <<WHL
Wheel-Version: 1.0
Generator: docker-e2e
Root-Is-Purelib: false
Tag: cp39-abi3-linux_x86_64
WHL
    python3 - "$STAGE" "$VERSION" <<PY
import hashlib, os, base64, sys, zipfile
stage, version = sys.argv[1], sys.argv[2]
records = []
with zipfile.ZipFile("/tmp/motedb.whl", "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, files in os.walk(stage):
        for f in sorted(files):
            p = os.path.join(root, f)
            rel = os.path.relpath(p, stage)
            data = open(p, "rb").read()
            h = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            z.writestr(rel, data)
            records.append(f"{rel},sha256={h},{len(data)}")
    records.append(f"motedb-{version}.dist-info/RECORD,,")
    z.writestr(f"motedb-{version}.dist-info/RECORD", "\n".join(records) + "\n")
print("wheel bytes:", os.path.getsize("/tmp/motedb.whl"))
PY

    echo "=== install wheel (unzip to a PYTHONPATH dir; no pip/venv needed) ==="
    python3 - <<PY
import zipfile, os
os.makedirs("/opt/py", exist_ok=True)
zipfile.ZipFile("/tmp/motedb.whl").extractall("/opt/py")
print("installed:", sorted(os.listdir("/opt/py/motedb")))
PY

    echo "=== smoke: import + version ==="
    PYTHONPATH=/opt/py python3 -c "import motedb; print('motedb', motedb.__version__)"

    echo "=== run E2E workload ==="
    cd /src
    PYTHONPATH=/opt/py python3 bindings/python/e2e/e2e_workload.py --root /tmp/e2e
'
