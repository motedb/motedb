#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# One-command reproducible benchmark suite.
#
#   ./scripts/compete.sh [core|quick|full]
#
# Runs the Rust bench binaries, the Python cross-engine compete suite
# (MoteDB vs SQLite vs DuckDB vs FAISS — optional engines skipped
# gracefully if not installed), and the adversarial correctness harness,
# saving every log + extracted JSON into benchmark_results/<UTC-timestamp>/.
#
# Exit 0 iff every step passed. benchmark_results/ is git-ignored.
# Optional env:
#   PIP_INSTALL=1   auto `pip install motedb-python==<Cargo.toml version>`
#                   when the module is missing or version-mismatched.
# ─────────────────────────────────────────────────────────────────────────────
set -u
MODE="${1:-core}"
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
OUT="benchmark_results/$(date -u +%Y%m%d_%H%M%S)_${MODE}"
mkdir -p "$OUT"

log()   { printf '%s  %s\n' "$(date -u +%H:%M:%S)" "$*"; }
STEP=0; FAILED=0
run_step() { # run_step <log-stem> <cmd...>
  local name="$1"; shift
  STEP=$((STEP+1))
  log "[$STEP/$TOTAL] RUN  $name"
  if "$@" >"$OUT/$name.log" 2>&1; then
    log "[$STEP/$TOTAL] OK   $name"
  else
    log "[$STEP/$TOTAL] FAIL $name  (see $OUT/$name.log)"
    FAILED=$((FAILED+1))
  fi
}
have_py() { python3 -c "import $1" >/dev/null 2>&1; }

# ── Python binding sanity: motedb importable and version-matched ────────────
CARGO_VER="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
PY_VER="$(python3 -c 'import motedb; print(getattr(motedb,"__version__",""))' 2>/dev/null || true)"
if [ -z "$PY_VER" ]; then
  if [ "${PIP_INSTALL:-0}" = "1" ]; then
    log "motedb-python missing → pip install motedb-python==$CARGO_VER"
    python3 -m pip install --quiet "motedb-python==$CARGO_VER" || true
    PY_VER="$(python3 -c 'import motedb; print(getattr(motedb,"__version__",""))' 2>/dev/null || true)"
  fi
fi
if [ -n "$PY_VER" ] && [ "$PY_VER" != "$CARGO_VER" ]; then
  log "WARN: motedb-python $PY_VER != Cargo.toml $CARGO_VER (pip install motedb-python==$CARGO_VER to pin)"
fi
if [ -z "$PY_VER" ]; then
  log "FATAL: motedb-python not importable. Install with: pip install motedb-python==$CARGO_VER (or rerun with PIP_INSTALL=1)"
  exit 2
fi
log "motedb-python $PY_VER / rust $CARGO_VER → results in $OUT"

# ── Step plan ────────────────────────────────────────────────────────────────
RUST_BENCH=(cargo test --release --test -- --ignored --nocapture --test-threads=1)
# cargo needs the test target between --test and --; build commands explicitly:
rust_bench() { # rust_bench <test-name>
  cargo test --release --test "$1" -- --ignored --nocapture --test-threads=1
}
py() { ( cd bindings/python/bench && python3 "$@" ); }

declare -a STEPS=()
if [ "$MODE" = "quick" ]; then
  STEPS=(rust_quick py_compete_bench_mote py_compete_bench_sqlite adversarial_verify)
elif [ "$MODE" = "full" ]; then
  STEPS=(rust_quick rust_comprehensive rust_timeseries rust_vs_sqlite_100k
         py_compete_bench_mote py_compete_bench_sqlite py_spatial_mote py_spatial_sqlite
         py_writes_mote py_writes_sqlite py_compete_vec py_compete_fts py_compete_scale
         adversarial_verify)
  have_py duckdb && STEPS+=(py_compete_bench_duckdb py_spatial_duckdb py_writes_duckdb py_compete_scale_duckdb)
  have_py faiss  && STEPS+=(py_compete_faiss)
else # core
  STEPS=(rust_quick rust_comprehensive rust_timeseries rust_vs_sqlite_100k
         py_compete_bench_mote py_compete_bench_sqlite py_spatial_mote py_spatial_sqlite
         adversarial_verify)
  have_py duckdb && STEPS+=(py_compete_bench_duckdb py_spatial_duckdb)
  have_py faiss  && STEPS+=(py_compete_faiss)
fi
TOTAL=${#STEPS[@]}

for s in "${STEPS[@]}"; do
  case "$s" in
    rust_quick)          run_step "$s" rust_bench bench_quick_baseline ;;
    rust_comprehensive)  run_step "$s" rust_bench bench_comprehensive ;;
    rust_timeseries)     run_step "$s" rust_bench bench_timeseries_index ;;
    rust_vs_sqlite_100k) run_step "$s" rust_bench bench_vs_sqlite_100k ;;
    py_compete_bench_mote)    run_step "$s" py compete_bench.py --engine mote ;;
    py_compete_bench_sqlite)  run_step "$s" py compete_bench.py --engine sqlite ;;
    py_compete_bench_duckdb)  run_step "$s" py compete_bench.py --engine duckdb ;;
    py_compete_faiss)         run_step "$s" py compete_bench.py --engine faiss ;;
    py_spatial_mote)          run_step "$s" py compete_spatial_ts.py --engine mote ;;
    py_spatial_sqlite)        run_step "$s" py compete_spatial_ts.py --engine sqlite ;;
    py_spatial_duckdb)        run_step "$s" py compete_spatial_ts.py --engine duckdb ;;
    py_writes_mote)           run_step "$s" py compete_writes.py --engine mote ;;
    py_writes_sqlite)         run_step "$s" py compete_writes.py --engine sqlite ;;
    py_writes_duckdb)         run_step "$s" py compete_writes.py --engine duckdb ;;
    py_compete_vec)           run_step "$s" py compete_vec_industry.py ;;
    py_compete_fts)           run_step "$s" py compete_fts_industry.py ;;
    py_compete_scale)         run_step "$s" py compete_scale.py --engine mote ;;
    py_compete_scale_duckdb)  run_step "$s" py compete_scale.py --engine duckdb ;;
    adversarial_verify)       run_step "$s" py adversarial_verify.py ;;
    *) log "SKIP unknown step $s" ;;
  esac
done

# ── Summary: pull test verdicts + every "JSON ..." result line ──────────────
SUM="$OUT/SUMMARY.txt"
{
  echo "MoteDB reproducible benchmark suite — $(date -u +%FT%TZ)"
  echo "mode=$MODE  motedb-python=$PY_VER  rust=$CARGO_VER"
  echo "steps: $TOTAL total, $FAILED failed"
  echo
  for f in "$OUT"/*.log; do
    echo "── $(basename "$f")"
    grep -E "^test result:" "$f" | sed 's/^/   /'
    grep "^JSON " "$f" | head -40 | cut -c1-400
    echo
  done
} > "$SUM"

if [ "$FAILED" -eq 0 ]; then
  log "ALL $TOTAL STEPS PASSED → $SUM"
  exit 0
fi
log "$FAILED/$TOTAL STEPS FAILED → $SUM"
exit 1
