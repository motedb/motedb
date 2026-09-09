//! `doctor` — operational self-check for a running MoteDB instance.
//!
//! One structured report: table layout, memory budgets, index coverage,
//! build errors, disk breakdown — each as a named PASS/WARN check, plus an
//! overall verdict. Exposed as `Database::doctor()` (Rust), `motedb-cli
//! doctor <path>` and `PyDatabase.doctor()`.

use crate::database::core::MoteDB;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorStatus {
    Pass,
    Warn,
    Fail,
}

impl DoctorStatus {
    pub fn label(&self) -> &'static str {
        match self {
            DoctorStatus::Pass => "PASS",
            DoctorStatus::Warn => "WARN",
            DoctorStatus::Fail => "FAIL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DoctorCheck {
    pub name: String,
    pub status: DoctorStatus,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub version: String,
    pub db_path: String,
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    pub fn worst(&self) -> DoctorStatus {
        if self.checks.iter().any(|c| c.status == DoctorStatus::Fail) {
            DoctorStatus::Fail
        } else if self.checks.iter().any(|c| c.status == DoctorStatus::Warn) {
            DoctorStatus::Warn
        } else {
            DoctorStatus::Pass
        }
    }

    /// Human-readable report (what the CLI prints).
    pub fn format_text(&self) -> String {
        let mut out = format!("MoteDB doctor v{} — {}\n", self.version, self.db_path);
        let mut pass = 0;
        let mut warn = 0;
        let mut fail = 0;
        for c in &self.checks {
            out.push_str(&format!(
                "[{}] {}: {}\n",
                c.status.label(),
                c.name,
                c.detail
            ));
            match c.status {
                DoctorStatus::Pass => pass += 1,
                DoctorStatus::Warn => warn += 1,
                DoctorStatus::Fail => fail += 1,
            }
        }
        out.push_str(&format!(
            "verdict: {} ({} pass, {} warn, {} fail)\n",
            self.worst().label(),
            pass,
            warn,
            fail
        ));
        out
    }
}

fn dir_size(p: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                total += dir_size(&path);
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
    }
    total
}

impl MoteDB {
    /// Run all health checks and return a structured report. Read-only:
    /// no locks held beyond momentary cache reads; safe on a live database.
    pub fn doctor(&self) -> DoctorReport {
        let mut checks: Vec<DoctorCheck> = Vec::new();

        // ── Per-table layout: rows, segments, write buffer, col-cache ──
        let mut total_cache = 0usize;
        let mut total_rows = 0usize;
        for e in self.col_segment_stores.iter() {
            let (table, store) = (e.key(), e.value());
            let segs = store.segments_snapshot();
            let rows: usize = segs.iter().map(|s| s.sst.num_rows).sum();
            let buffered = store.buffered_row_count();
            let mut cache_bytes = 0usize;
            for seg in segs.iter() {
                let (c, ..) = seg.debug_resident_bytes();
                cache_bytes += c;
            }
            let budget = store.col_cache_budget();
            total_cache += cache_bytes;
            total_rows += rows;

            checks.push(DoctorCheck {
                name: format!("table.{table}.layout"),
                status: if segs.len() > 8 {
                    DoctorStatus::Warn
                } else {
                    DoctorStatus::Pass
                },
                detail: format!(
                    "storage_records={} (physical, pre-compaction) segments={} write_buffer={} unflushed rows",
                    rows,
                    segs.len(),
                    buffered
                ),
            });

            if buffered > 10_000 {
                checks.push(DoctorCheck {
                    name: format!("durability.{table}.write_buffer"),
                    status: DoctorStatus::Warn,
                    detail: format!(
                        "{buffered} rows ({} KB) only in the write buffer — CHECKPOINT to make them durable",
                        store.buffered_bytes() / 1024
                    ),
                });
            }

            if budget > 0 && cache_bytes >= budget * 9 / 10 {
                checks.push(DoctorCheck {
                    name: format!("memory.{table}.col_cache"),
                    status: DoctorStatus::Warn,
                    detail: format!(
                        "column cache at {:.1}/{:.1} MB — near the trim threshold, expect eviction churn",
                        cache_bytes as f64 / 1048576.0,
                        budget as f64 / 1048576.0
                    ),
                });
            }
        }

        // ── Vector index coverage: index entries vs table rows ──
        for index_name in self
            .index_registry
            .list_by_type(crate::database::index_metadata::IndexType::Vector)
        {
            let (table, column) = match self.index_registry.resolve_index_name(&index_name) {
                Some(tc) => tc,
                None => continue,
            };
            let entries = self
                .vector_indexes
                .get(&index_name)
                .map(|r| r.value().read().len())
                .unwrap_or(0);
            let table_rows = self
                .col_segment_stores
                .get(&table)
                .map(|s| {
                    s.value()
                        .segments_snapshot()
                        .iter()
                        .map(|g| g.sst.num_rows)
                        .sum::<usize>()
                        + s.value().buffered_row_count()
                })
                .unwrap_or(0);
            let covered = table_rows == 0 || entries >= table_rows.saturating_sub(table_rows / 20);
            checks.push(DoctorCheck {
                name: format!("index.{index_name}.coverage"),
                status: if covered {
                    DoctorStatus::Pass
                } else {
                    DoctorStatus::Warn
                },
                detail: format!(
                    "{} entries vs {} storage records on {}.{} ({:.0}%){}",
                    entries,
                    table_rows,
                    table,
                    column,
                    if table_rows > 0 {
                        entries as f64 / table_rows as f64 * 100.0
                    } else {
                        100.0
                    },
                    if covered {
                        ""
                    } else {
                        " — index is behind; ANN falls back to the exact scan (correct, slower at scale)"
                    },
                ),
            });
        }

        // ── Index build errors ──
        let build_errors = self
            .index_build_errors
            .load(std::sync::atomic::Ordering::Relaxed);
        checks.push(DoctorCheck {
            name: "index.build_errors".into(),
            status: if build_errors > 0 {
                DoctorStatus::Warn
            } else {
                DoctorStatus::Pass
            },
            detail: format!("{build_errors} background index build errors"),
        });

        // ── Memory rollup ──
        checks.push(DoctorCheck {
            name: "memory.col_cache_total".into(),
            status: DoctorStatus::Pass,
            detail: format!(
                "{:.1} MB decoded column cache across {} table(s), {} total storage records",
                total_cache as f64 / 1048576.0,
                self.col_segment_stores.len(),
                total_rows
            ),
        });

        // ── Disk breakdown ──
        let wal = dir_size(&self.path.join("wal"));
        let lsm = dir_size(&self.path.join("lsm"));
        let indexes = dir_size(&self.path.join("indexes"));
        let colms = dir_size(&self.path.join("columnar_ms"));
        let total = wal + lsm + indexes + colms;
        checks.push(DoctorCheck {
            name: "disk.layout".into(),
            status: DoctorStatus::Pass,
            detail: format!(
                "total {:.1} MB (columnar {:.1}, wal {:.1}, lsm {:.1}, indexes {:.1})",
                total as f64 / 1048576.0,
                colms as f64 / 1048576.0,
                wal as f64 / 1048576.0,
                lsm as f64 / 1048576.0,
                indexes as f64 / 1048576.0,
            ),
        });

        DoctorReport {
            version: env!("CARGO_PKG_VERSION").to_string(),
            db_path: self.path.display().to_string(),
            checks,
        }
    }
}
