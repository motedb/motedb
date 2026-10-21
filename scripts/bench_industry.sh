#!/usr/bin/env bash
# 公开可复现向量基准 — 多引擎 × 可选规模档。
#   make bench-industry            # 220K×384 标准档
#   make bench-industry N=1000000  # 1M 档(磁盘 ~2GB, 30-60 分钟)
#   make bench-industry DIM=1536 N=100000
#
# 依赖(Python): motedb(本仓库 wheel), numpy; 引擎可选装, 缺哪个跳哪个:
#   pip install faiss-cpu lancedb sqlite-vec duckdb usearch
# 结果: benchmark_results/vec-industry-<ts>/results.jsonl + 控制台 JSON 行。
set -euo pipefail
cd "$(dirname "$0")/.."

N="${N:-220000}"
DIM="${DIM:-384}"
Q="${Q:-200}"
PY="${PYTHON:-python3}"
TS="$(date +%Y-%m-%d)"
OUT="benchmark_results/vec-industry-${TS}"

$PY bindings/python/bench/compete_vec_engines.py \
  --n "$N" --dim "$DIM" --q "$Q" \
  --out "$OUT/results.jsonl"

echo
echo "== rendered report =="
$PY scripts/bench_report.py "$OUT/results.jsonl" docs/benchmark_report_${TS}.md
