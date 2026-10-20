#!/usr/bin/env bash
# Release smoke — install the PUBLISHED MoteDB packages into throwaway
# environments and run the README examples against them. This is the last
# gate of a release: it catches "we published, but nobody can install it"
# (missing platform wheel, broken optionalDependency, loader bug, API drift
# between README and the shipped artifact). CI runs it from
# .github/workflows/release-smoke.yml on every v* tag; run it by hand with
# `make release-smoke` (published) or `make release-smoke-local` (working tree).
#
# Usage:
#   scripts/release_smoke.sh [-v VERSION] [-l python,node,rust] [--local] [--wait SECS]
#
#   -v VERSION   version to install (default: the version in Cargo.toml)
#   -l LANGS     comma-separated subset of python,node,rust (default: all three)
#   --local      smoke the WORKING TREE instead of the registries:
#                python → wheel built with maturin, node → cargo-built cdylib,
#                rust → path dependency. Use this before tagging a release.
#   --wait SECS  poll each registry until VERSION is visible, up to SECS
#                seconds (default 0 = fail immediately). npm and PyPI lag the
#                publish by minutes, so CI passes ~1800.
#
# Exit status is non-zero if any selected language fails. Every language runs
# in its own temp dir, removed on exit (set KEEP_TMP=1 to inspect it).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION=""
LANGS="python,node,rust"
LOCAL=0
WAIT=0

while [ $# -gt 0 ]; do
  case "$1" in
    -v|--version) VERSION="$2"; shift 2 ;;
    -l|--langs)   LANGS="$2"; shift 2 ;;
    --local)      LOCAL=1; shift ;;
    --wait)       WAIT="$2"; shift 2 ;;
    -h|--help)    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$VERSION" ]; then
  VERSION="$(grep -m1 '^version = ' "$ROOT/Cargo.toml" | cut -d'"' -f2)"
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/motedb-release-smoke.XXXXXX")"
cleanup() {
  if [ "${KEEP_TMP:-0}" = "1" ]; then
    echo "KEEP_TMP=1 — leaving $TMP in place"
  else
    rm -rf "$TMP"
  fi
}
trap cleanup EXIT

log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32mOK\033[0m  %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; }

# Git Bash on the Windows runners has `python` but not `python3`.
find_python() {
  if command -v python3 >/dev/null 2>&1; then echo python3
  elif command -v python >/dev/null 2>&1; then echo python
  else return 1
  fi
}
venv_bin() { if [ -d "$1/Scripts" ]; then echo "$1/Scripts"; else echo "$1/bin"; fi; }

has_lang() { case ",$LANGS," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

# ---------------------------------------------------------------------------
# Registry propagation wait (published mode only)
# ---------------------------------------------------------------------------
registry_url() {
  case "$1" in
    python) echo "https://pypi.org/pypi/motedb-python/$VERSION/json" ;;
    node)   echo "https://registry.npmjs.org/motedb/$VERSION" ;;
    rust)   echo "https://crates.io/api/v1/crates/motedb/$VERSION" ;;
  esac
}

wait_for_registry() {
  local lang="$1" url deadline
  url="$(registry_url "$lang")"
  deadline=$(( $(date +%s) + WAIT ))
  while :; do
    # crates.io rejects requests without a User-Agent.
    if curl -fsS -A "motedb-release-smoke" -o /dev/null --max-time 20 "$url"; then
      ok "$lang: $VERSION visible at $url"
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      fail "$lang: $VERSION not visible at $url after ${WAIT}s"
      return 1
    fi
    echo "   waiting for $lang $VERSION to appear on the registry…"
    sleep 30
  done
}

# ---------------------------------------------------------------------------
# Python: pip install motedb-python → README quick start
# ---------------------------------------------------------------------------
smoke_python() {
  log "python ($( [ $LOCAL = 1 ] && echo 'local wheel' || echo "motedb-python==$VERSION" ))"
  local py venv pybin wheel
  py="$(find_python)" || { fail "python: no python3/python on PATH"; return 1; }
  venv="$TMP/py-venv"
  "$py" -m venv "$venv"
  pybin="$(venv_bin "$venv")"
  "$pybin/python" -m pip install --quiet --upgrade pip

  if [ $LOCAL = 1 ]; then
    wheel="$(ls "$ROOT"/bindings/python/target/wheels/motedb_python-*.whl 2>/dev/null | head -1 || true)"
    if [ -z "$wheel" ]; then
      echo "   no wheel under bindings/python/target/wheels — building with maturin (slow)"
      "$pybin/python" -m pip install --quiet maturin
      (cd "$ROOT/bindings/python" && "$pybin/python" -m maturin build --release --quiet)
      wheel="$(ls "$ROOT"/bindings/python/target/wheels/motedb_python-*.whl | head -1)"
    fi
    "$pybin/python" -m pip install --quiet "$wheel" numpy pyarrow
  else
    "$pybin/python" -m pip install --quiet "motedb-python==$VERSION" numpy pyarrow
  fi

  MOTEDB_EXPECT_VERSION="$( [ $LOCAL = 1 ] && echo '' || echo "$VERSION" )" \
  "$pybin/python" - <<'PY'
import os, tempfile, importlib.metadata as md
import numpy as np
import motedb

expect = os.environ.get("MOTEDB_EXPECT_VERSION")
installed = md.version("motedb-python")
if expect and installed != expect:
    raise SystemExit(f"installed motedb-python {installed}, expected {expect}")

d = tempfile.mkdtemp()
path = os.path.join(d, "smoke.mote")
db = motedb.Database(path)
db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, emb VECTOR(4), tag TEXT)")

n = 1000
rng = np.random.default_rng(42)
db.insert_arrays("ev", {
    "id":  np.arange(n, dtype=np.int64),
    "ts":  np.arange(n, dtype=np.int64) * 1_000_000,
    "emb": rng.random((n, 4), dtype=np.float32),
    "tag": [f"t{i % 16}" for i in range(n)],
})

rows = db.execute("SELECT COUNT(*) AS n FROM ev")
assert rows[0]["n"] == n, rows

nn = db.execute("SELECT id FROM ev ORDER BY emb <-> [0.5, 0.5, 0.5, 0.5] LIMIT 3")
assert len(nn) == 3, nn

cols, arrays = db.fetch_arrays("SELECT id, ts FROM ev WHERE id < ?", params=[10])
assert "id" in cols and len(arrays["id"]) == 10, (cols, {k: len(v) for k, v in arrays.items()})

table = db.query_arrow("SELECT id, tag FROM ev WHERE tag = ? LIMIT 5", params=["t3"])
assert table.num_rows == 5 and table.column_names == ["id", "tag"], table

db.close()
again = motedb.Database(path)
assert again.execute("SELECT COUNT(*) AS n FROM ev")[0]["n"] == n
again.close()
print(f"python smoke OK (motedb-python {installed})")
PY
  ok "python"
}

# ---------------------------------------------------------------------------
# Node: npm install motedb → bindings/node/smoke_ci.js against the package
# ---------------------------------------------------------------------------
smoke_node() {
  log "node ($( [ $LOCAL = 1 ] && echo 'cargo-built cdylib' || echo "motedb@$VERSION" ))"
  command -v node >/dev/null 2>&1 || { fail "node: no node on PATH"; return 1; }

  if [ $LOCAL = 1 ]; then
    local lib
    (cd "$ROOT/bindings/node" && cargo build --release --quiet)
    lib="$(ls "$ROOT"/bindings/node/target/release/libmotedb_node.dylib \
              "$ROOT"/bindings/node/target/release/libmotedb_node.so \
              "$ROOT"/bindings/node/target/release/motedb_node.dll 2>/dev/null | head -1 || true)"
    [ -n "$lib" ] || { fail "node: cdylib not found under bindings/node/target/release"; return 1; }
    # Node only dlopen()s *.node files.
    cp "$lib" "$TMP/motedb_local.node"
    (cd "$ROOT/bindings/node" && MOTEDB_NODE_LIB="$TMP/motedb_local.node" node smoke_ci.js)
  else
    local proj="$TMP/node-proj"
    mkdir -p "$proj"
    (cd "$proj" \
      && npm init -y >/dev/null \
      && npm install --no-audit --no-fund --loglevel=error "motedb@$VERSION" \
      && cp "$ROOT/bindings/node/smoke_ci.js" . \
      && node -e "const v=require('motedb/package.json').version; if(v!=='$VERSION'){console.error('installed motedb '+v+', expected $VERSION');process.exit(1)}" \
      && MOTEDB_SMOKE_PKG=motedb node smoke_ci.js)
  fi
  ok "node"
}

# ---------------------------------------------------------------------------
# Rust: cargo add motedb → examples/hello_world.rs as a downstream binary
# ---------------------------------------------------------------------------
smoke_rust() {
  log "rust ($( [ $LOCAL = 1 ] && echo 'path dependency' || echo "motedb@$VERSION" ))"
  command -v cargo >/dev/null 2>&1 || { fail "rust: no cargo on PATH"; return 1; }
  local proj="$TMP/rust-proj"
  cargo new --bin --quiet "$proj" >/dev/null
  cp "$ROOT/examples/hello_world.rs" "$proj/src/main.rs"
  if [ $LOCAL = 1 ]; then
    (cd "$proj" && cargo add --quiet motedb --path "$ROOT")
  else
    (cd "$proj" && cargo add --quiet "motedb@$VERSION")
  fi
  # Debug profile: the example is tiny and a release build of the engine adds
  # minutes of LTO for no extra signal.
  (cd "$proj" && cargo run --quiet)
  ok "rust"
}

# ---------------------------------------------------------------------------
main() {
  echo "MoteDB release smoke — version $VERSION, langs $LANGS, mode $( [ $LOCAL = 1 ] && echo local || echo published )"
  local status=0 lang
  for lang in python node rust; do
    has_lang "$lang" || continue
    if [ $LOCAL = 0 ] && [ "$WAIT" -gt 0 ]; then
      wait_for_registry "$lang" || { status=1; continue; }
    fi
    if ! "smoke_$lang"; then
      fail "$lang smoke failed"
      status=1
    fi
  done
  echo
  if [ $status = 0 ]; then ok "all selected languages passed ($LANGS)"; else fail "one or more languages failed"; fi
  return $status
}

main
