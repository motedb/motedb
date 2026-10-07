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
#
# See docs/benchmark_methodology.md for what is measured and how.

.PHONY: compete compete-quick compete-full test wheel

compete:
	./scripts/compete.sh core

compete-quick:
	./scripts/compete.sh quick

compete-full:
	./scripts/compete.sh full

test:
	cargo test --lib --all-features

wheel:
	python3 -m pip install --quiet maturin
	cd bindings/python && python3 -m maturin build --release
	python3 -m pip install --quiet --force-reinstall --no-deps bindings/python/target/wheels/motedb_python-*.whl
