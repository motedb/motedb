//! Interop layer — columnar data out to (and in from) ecosystem formats.
//!
//! Phase 1 of `docs/design/2026-10-industry-parity-plan.md`: the training-data
//! ecosystem (pyarrow, pandas, DuckDB, LeRobot) speaks Parquet, so Parquet is
//! the bridge everything else (LeRobot export, DataLoader recipes) builds on.

pub mod parquet;
