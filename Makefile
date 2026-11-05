# MoteDB — developer entry points.
#
#   make compete        One-command reproducible benchmark suite (core set).
#                       Logs + JSON land in benchmark_results/<timestamp>/.
#   make compete-quick  Fast smoke: 10K-row Rust bench + mote/sqlite only.
#   make compete-full   Adds writes / vec / fts / scale suites (+duckdb/faiss
#                       when installed). Long (30-60 min).
#   make test           Unit tests (lib, all features).
#   make wheel          Build the Python binding from LOCAL source and install
#                       it. Run this before `make compete` whenever you changed
#                       engine code — the compete suite drives the Python
#                       binding, which otherwise benchmarks the last PyPI
#                       install, not your working tree.
#   make bench-industry Multi-engine vector benchmark (MoteDB vs FAISS/
#                       LanceDB/sqlite-vec/DuckDB-VSS/usearch), 220K×384 default;
#                       N=/DIM= override; renders docs/benchmark_report_<date>.md.
#   make release-smoke  Install the PUBLISHED packages (crates.io / PyPI / npm,
#                       version from Cargo.toml) into throwaway envs and run
#                       the README examples. The post-release gate; CI runs it
#                       on every v* tag. Narrow with LANGS=python,node.
#   make release-smoke-local
#                       Same smoke against the working tree (wheel + cdylib +
#                       path dep) — run before tagging.
#
# See docs/benchmark_methodology.md for what is measured and how.

.PHONY: compete compete-quick compete-full test test-fast wheel release-smoke release-smoke-local bench-industry vec-recall

LANGS ?= python,node,rust

compete:
	./scripts/compete.sh core

compete-quick:
	./scripts/compete.sh quick

# 0.18 B3: 向量召回测量（增量 stranded 场景 vs 暴力精确扫）。
#   测量模式: 只打印; 断言模式: MOTE_VEC_RECALL_ASSERT=1 且
#   MOTE_VEC_RECALL_MIN=<阈值>（默认 0.98@10）。
vec-recall:
	cargo test --release --test bench_vec_recall -- --ignored --nocapture

compete-full:
	./scripts/compete.sh full

test:
	cargo test --lib --all-features

# 0.18 A 工作流: 全量门禁的提速形态。独立 profile（Cargo.toml
# [profile.release-test]: 关 LTO + codegen-units=16）——300+ 测试二进制
# 的链接时间是全量门禁的大头（release LTO 下 ~2h），正确性测试不需要
# LTO 性能; 独立 profile 不污染 release 构建缓存。PASS/FAIL 等价。
test-fast:
	cargo test --profile release-test --no-fail-fast

# 0.18 发版清单: 文档版本单源（与 CI lint job 同一脚本）。
check-docs:
	./scripts/check_doc_versions.sh

wheel:
	python3 -m pip install --quiet maturin
	cd bindings/python && python3 -m maturin build --release
	python3 -m pip install --quiet --force-reinstall --no-deps bindings/python/target/wheels/motedb_python-*.whl

release-smoke:
	./scripts/release_smoke.sh -l $(LANGS)

bench-industry:
	PYTHON=$(or $(PYTHON),python3) ./scripts/bench_industry.sh

release-smoke-local:
	./scripts/release_smoke.sh -l $(LANGS) --local
