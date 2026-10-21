//! LeRobot v3 dataset export — the "MCAP → SQL → training" bridge.
//!
//! SQL: `EXPORT LEROBOT <table> TO '<dir>' [FPS <n>] [TASKS <text-column>]`
//!
//! The source table must be a `CREATE EPISODE TABLE` (its `EPISODE BY
//! (key, ts) GAP` config drives segmentation). Rows are ordered by
//! (key, ts), split into episodes at the config's gap threshold, and written
//! in the exact on-disk shape `LeRobotDataset` (lerobot ≥ 0.6, codebase
//! version v3.0) produces — verified against a reference dataset generated
//! with the official writer:
//!
//! ```text
//! <dir>/meta/info.json
//! <dir>/meta/stats.json
//! <dir>/meta/tasks.parquet                     (task_index int64, task string)
//! <dir>/meta/episodes/chunk-000/file-000.parquet
//!        (episode_index, tasks list<string>, length, dataset_from_index,
//!         dataset_to_index, meta/episodes/chunk_index, meta/episodes/file_index)
//! <dir>/data/chunk-000/file-000.parquet
//!        (features…, timestamp f32, frame_index i64, episode_index i64,
//!         index i64, task_index i64)
//! ```
//!
//! Column mapping: `VECTOR(n>1)` → `float32` shape (n,) stored as
//! `fixed_size_list<element: float>`; `FLOAT` → `float32` (1,) scalar;
//! `INTEGER` → `int64` (1,); `TEXT` → `string` (1,). The episode key/timestamp
//! columns and the optional task column are consumed by the frame fields.
//! BLOB/SPATIAL columns are rejected (video is out of scope v1). All episodes
//! land in `file-000.parquet` (size-based rotation is future work).
//!
//! Quantiles in `stats.json` are nearest-rank approximations — they feed
//! normalization heuristics, not correctness.

use std::path::Path;

/// Export knobs (SQL `[FPS n] [TASKS col]`).
#[derive(Debug, Clone)]
pub struct LeRobotOptions {
    pub fps: u32,
    pub task_column: Option<String>,
}

impl Default for LeRobotOptions {
    fn default() -> Self {
        Self {
            fps: 30,
            task_column: None,
        }
    }
}

/// What got written — surfaces in the SQL result message.
#[derive(Debug, Clone, Copy)]
pub struct LeRobotSummary {
    pub episodes: usize,
    pub frames: usize,
    pub tasks: usize,
}

impl crate::sql::QueryExecutor {
    /// `EXPORT LEROBOT t TO 'dir'` — see the module docs.
    pub fn export_table_lerobot(
        &self,
        table: &str,
        dest: &Path,
        opts: &LeRobotOptions,
    ) -> crate::Result<LeRobotSummary> {
        #[cfg(feature = "parquet")]
        {
            imp::run(self, table, dest, opts)
        }
        #[cfg(not(feature = "parquet"))]
        {
            let _ = (table, dest, opts);
            Err(crate::MoteDBError::InvalidArgument(
                "parquet support is not compiled in — rebuild with the `parquet` feature".into(),
            ))
        }
    }
}

#[cfg(feature = "parquet")]
mod imp {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Float32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    use super::super::parquet::imp::rows_of;
    use super::{LeRobotOptions, LeRobotSummary};
    use crate::sql::{Lexer, Parser, QueryExecutor};
    use crate::types::{ArcString, ColumnDef, ColumnType, Value};

    /// One exported feature column.
    #[derive(Debug, Clone)]
    enum Feature {
        /// float32 shape (1,) — scalar column.
        F32Scalar,
        /// float32 shape (n,) — fixed_size_list<element: float>.
        F32Vector(usize),
        /// int64 shape (1,).
        I64Scalar,
        /// string shape (1,).
        StrScalar,
    }

    impl Feature {
        fn dtype(&self) -> &'static str {
            match self {
                Feature::F32Scalar | Feature::F32Vector(_) => "float32",
                Feature::I64Scalar => "int64",
                Feature::StrScalar => "string",
            }
        }
        fn shape(&self) -> Vec<usize> {
            match self {
                Feature::F32Scalar | Feature::I64Scalar | Feature::StrScalar => vec![1],
                Feature::F32Vector(n) => vec![*n],
            }
        }
        fn is_numeric(&self) -> bool {
            !matches!(self, Feature::StrScalar)
        }
    }

    fn perr(e: impl std::fmt::Display) -> crate::MoteDBError {
        crate::MoteDBError::InvalidArgument(format!("lerobot export: {e}"))
    }

    pub fn run(
        exec: &QueryExecutor,
        table: &str,
        dest: &Path,
        opts: &LeRobotOptions,
    ) -> crate::Result<LeRobotSummary> {
        let schema = exec.db_handle().table_registry.get_table(table)?;
        let ep_cfg = schema.episode_config.clone().ok_or_else(|| {
            crate::MoteDBError::InvalidArgument(format!(
                "EXPORT LEROBOT requires a CREATE EPISODE TABLE — '{table}' has no EPISODE BY config"
            ))
        })?;

        if dest.exists()
            && fs::read_dir(dest)
                .map(|mut d| d.next().is_some())
                .unwrap_or(false)
        {
            return Err(crate::MoteDBError::InvalidArgument(format!(
                "destination '{}' exists and is not empty",
                dest.display()
            )));
        }
        for dir in ["data/chunk-000", "meta/episodes/chunk-000"] {
            fs::create_dir_all(dest.join(dir)).map_err(perr)?;
        }

        // Feature columns: everything except the episode key/ts and the task
        // column. Schema order is preserved.
        let mut features: Vec<(String, Feature, usize)> = Vec::new(); // (name, feature, schema position)
        for (pos, col) in schema.columns.iter().enumerate() {
            if col.name == ep_cfg.key_col || col.name == ep_cfg.ts_col {
                continue;
            }
            if opts.task_column.as_deref() == Some(col.name.as_str()) {
                continue;
            }
            let f = match col.col_type {
                ColumnType::Float => Feature::F32Scalar,
                ColumnType::Integer => Feature::I64Scalar,
                ColumnType::Text => Feature::StrScalar,
                ColumnType::Tensor(n) if n > 1 => Feature::F32Vector(n),
                ColumnType::Tensor(_) => Feature::F32Scalar,
                ref other => {
                    return Err(crate::MoteDBError::InvalidArgument(format!(
                        "column '{}' ({other:?}) cannot map to a LeRobot feature — \
                         drop it (SELECT a projection into a staging table) or handle video separately",
                        col.name
                    )))
                }
            };
            features.push((col.name.clone(), f, pos));
        }

        // Pull every row, then sort by (key, ts) ourselves — deterministic
        // and independent of the executor's ORDER BY fast paths.
        let sql = format!("SELECT * FROM {table}");
        let stmt = Parser::new(Lexer::new(&sql).tokenize()?).parse()?;
        let mut rows: Vec<Vec<Value>> =
            rows_of(exec.execute_streaming_ref(&stmt)?)?.collect::<crate::Result<Vec<_>>>()?;
        let key_pos = column_position(&schema.columns, &ep_cfg.key_col)?;
        let ts_pos = column_position(&schema.columns, &ep_cfg.ts_col)?;
        let task_pos = match &opts.task_column {
            Some(c) => Some(column_position(&schema.columns, c)?),
            None => None,
        };
        let key_of = |row: &Vec<Value>| -> crate::Result<String> {
            match row.get(key_pos) {
                Some(Value::Text(s)) => Ok(s.as_str().to_string()),
                other => Err(crate::MoteDBError::InvalidArgument(format!(
                    "episode key column '{}' must be TEXT, got {other:?}",
                    ep_cfg.key_col
                ))),
            }
        };
        let ts_of = |row: &Vec<Value>| -> crate::Result<i64> {
            match row.get(ts_pos) {
                Some(Value::Timestamp(t)) => Ok(t.as_micros()),
                other => Err(crate::MoteDBError::InvalidArgument(format!(
                    "episode timestamp column '{}' must be TIMESTAMP, got {other:?}",
                    ep_cfg.ts_col
                ))),
            }
        };
        rows.sort_by(|a, b| {
            (key_of(a).unwrap_or_default(), ts_of(a).unwrap_or(i64::MIN))
                .cmp(&(key_of(b).unwrap_or_default(), ts_of(b).unwrap_or(i64::MIN)))
        });

        // Segment into episodes, materializing frame records.
        #[derive(Debug)]
        struct Frame {
            feature_values: Vec<Value>, // feature order, source positions
            ts_rel_secs: f32,
            episode: usize,
            task_idx: i64,
        }
        struct EpisodeRec {
            index: usize,
            length: usize,
            from_index: usize,
            to_index: usize,
            tasks: Vec<ArcString>,
        }
        let mut frames: Vec<Frame> = Vec::with_capacity(rows.len());
        let mut episodes: Vec<EpisodeRec> = Vec::new();
        let mut task_index: Vec<ArcString> = Vec::new(); // first-seen order
        let default_task = ArcString::from("default");
        let mut cur_key = None::<String>;
        let mut cur_ep_start = 0i64;
        let mut cur_ep = 0usize;
        let mut cur_ep_len = 0usize;
        let mut cur_task: Option<ArcString> = None;

        let flush_episode = |episodes: &mut Vec<EpisodeRec>,
                             cur_ep: usize,
                             cur_ep_len: usize,
                             from: usize,
                             cur_task: &ArcString| {
            episodes.push(EpisodeRec {
                index: cur_ep,
                length: cur_ep_len,
                from_index: from,
                to_index: from + cur_ep_len,
                tasks: vec![ArcString::from(cur_task.as_str())],
            });
        };

        for row in rows {
            let key = key_of(&row)?;
            let ts = ts_of(&row)?;
            let new_episode = match &cur_key {
                None => true,
                Some(k) if *k != key => true,
                Some(_) => ts - cur_ep_start > ep_cfg.gap_micros.max(0),
            };
            if new_episode {
                if cur_key.is_some() {
                    flush_episode(
                        &mut episodes,
                        cur_ep,
                        cur_ep_len,
                        frames.len() - cur_ep_len,
                        cur_task.as_ref().unwrap_or(&default_task),
                    );
                }
                cur_key = Some(key);
                cur_ep = episodes.len();
                cur_ep_start = ts;
                cur_ep_len = 0;
                cur_task = None;
            }
            let task = match (task_pos, &cur_task) {
                (Some(tp), None) => {
                    let t = match row.get(tp) {
                        Some(Value::Text(s)) => Some(ArcString::from(s.as_str())),
                        _ => None,
                    }
                    .unwrap_or_else(|| ArcString::from("default"));
                    cur_task = Some(ArcString::from(t.as_str()));
                    ArcString::from(t.as_str())
                }
                (_, Some(t)) => ArcString::from(t.as_str()),
                (None, None) => ArcString::from("default"),
            };
            let task_idx = task_index
                .iter()
                .position(|t| t.as_str() == task.as_str())
                .unwrap_or_else(|| {
                    task_index.push(ArcString::from(task.as_str()));
                    task_index.len() - 1
                }) as i64;
            frames.push(Frame {
                feature_values: features
                    .iter()
                    .map(|(_, _, pos)| row.get(*pos).cloned().unwrap_or(Value::Null))
                    .collect(),
                ts_rel_secs: ((ts - cur_ep_start) as f64 / 1_000_000.0) as f32,
                episode: cur_ep,
                task_idx,
            });
            cur_ep_len += 1;
        }
        if cur_key.is_some() {
            flush_episode(
                &mut episodes,
                cur_ep,
                cur_ep_len,
                frames.len() - cur_ep_len,
                cur_task.as_ref().unwrap_or(&default_task),
            );
        }
        if frames.is_empty() {
            return Err(crate::MoteDBError::InvalidArgument(
                "table has no rows — nothing to export".into(),
            ));
        }
        let total_frames = frames.len();

        // ---- data parquet ------------------------------------------------
        let mut fields: Vec<Field> = features
            .iter()
            .map(|(name, f, _)| {
                let dt = match f {
                    Feature::F32Scalar | Feature::StrScalar | Feature::I64Scalar => match f {
                        Feature::F32Scalar => DataType::Float32,
                        Feature::I64Scalar => DataType::Int64,
                        _ => DataType::Utf8,
                    },
                    Feature::F32Vector(n) => DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::Float32, true)),
                        *n as i32,
                    ),
                };
                // Feature columns are nullable; the default columns are not.
                Field::new(name, dt, true)
            })
            .collect();
        for (name, dt) in [
            ("timestamp", DataType::Float32),
            ("frame_index", DataType::Int64),
            ("episode_index", DataType::Int64),
            ("index", DataType::Int64),
            ("task_index", DataType::Int64),
        ] {
            fields.push(Field::new(name, dt, false));
        }
        let data_schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(features.len() + 5);
        for (fi, (_, feat, _)) in features.iter().enumerate() {
            let col: ArrayRef = match feat {
                Feature::F32Scalar => Arc::new(Float32Array::from(
                    frames
                        .iter()
                        .map(|f| match &f.feature_values[fi] {
                            Value::Float(v) => Some(*v as f32),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                Feature::I64Scalar => Arc::new(Int64Array::from(
                    frames
                        .iter()
                        .map(|f| match &f.feature_values[fi] {
                            Value::Integer(v) => Some(*v),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                Feature::StrScalar => Arc::new(StringArray::from(
                    frames
                        .iter()
                        .map(|f| match &f.feature_values[fi] {
                            Value::Text(s) => Some(s.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )),
                Feature::F32Vector(dim) => {
                    let mut b = FixedSizeListBuilder::<Float32Builder>::with_capacity(
                        Float32Builder::with_capacity(frames.len() * dim),
                        *dim as i32,
                        frames.len(),
                    )
                    .with_field(Field::new("element", DataType::Float32, true));
                    for f in &frames {
                        match &f.feature_values[fi] {
                            Value::Vector(v) if v.len() == *dim => {
                                for x in v.0.iter() {
                                    b.values().append_value(*x);
                                }
                                b.append(true);
                            }
                            _ => {
                                for _ in 0..*dim {
                                    b.values().append_null();
                                }
                                b.append(false);
                            }
                        }
                    }
                    Arc::new(b.finish())
                }
            };
            columns.push(col);
        }
        // The five default-column sequences, computed once and shared with
        // stats.json below.
        let mut ep_counter = vec![0i64; episodes.len()];
        let frame_index_seq: Vec<i64> = frames
            .iter()
            .map(|f| {
                let v = ep_counter[f.episode];
                ep_counter[f.episode] += 1;
                v
            })
            .collect();
        let timestamp_seq: Vec<f32> = frames.iter().map(|f| f.ts_rel_secs).collect();
        let episode_seq: Vec<i64> = frames.iter().map(|f| f.episode as i64).collect();
        let index_seq: Vec<i64> = (0..total_frames as i64).collect();
        let task_seq: Vec<i64> = frames.iter().map(|f| f.task_idx).collect();

        columns.push(Arc::new(Float32Array::from(
            timestamp_seq.iter().copied().map(Some).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            frame_index_seq
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            episode_seq.iter().copied().map(Some).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            index_seq.iter().copied().map(Some).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            task_seq.iter().copied().map(Some).collect::<Vec<_>>(),
        )));
        let batch = RecordBatch::try_new(data_schema, columns).map_err(perr)?;
        write_parquet(&dest.join("data/chunk-000/file-000.parquet"), batch)?;

        // ---- meta/tasks.parquet -------------------------------------------
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("task_index", DataType::Int64, false),
                Field::new("task", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    (0..task_index.len() as i64).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    task_index.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(perr)?;
        write_parquet(&dest.join("meta/tasks.parquet"), batch)?;

        // ---- meta/episodes/… ------------------------------------------------
        let mut tasks_b = ListBuilder::new(StringBuilder::new());
        for ep in &episodes {
            tasks_b.values().append_value(ep.tasks[0].as_str());
            tasks_b.append(true);
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("episode_index", DataType::Int64, false),
                Field::new(
                    "tasks",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    false,
                ),
                Field::new("length", DataType::Int64, false),
                Field::new("dataset_from_index", DataType::Int64, false),
                Field::new("dataset_to_index", DataType::Int64, false),
                Field::new("meta/episodes/chunk_index", DataType::Int64, false),
                Field::new("meta/episodes/file_index", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    episodes.iter().map(|e| e.index as i64).collect::<Vec<_>>(),
                )),
                Arc::new(tasks_b.finish()),
                Arc::new(Int64Array::from(
                    episodes.iter().map(|e| e.length as i64).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    episodes
                        .iter()
                        .map(|e| e.from_index as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    episodes
                        .iter()
                        .map(|e| e.to_index as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![0i64; episodes.len()])),
                Arc::new(Int64Array::from(vec![0i64; episodes.len()])),
            ],
        )
        .map_err(perr)?;
        write_parquet(
            &dest.join("meta/episodes/chunk-000/file-000.parquet"),
            batch,
        )?;

        // ---- meta/info.json ---------------------------------------------------
        let mut features_json = serde_json::Map::new();
        for (name, f, _) in &features {
            features_json.insert(
                name.clone(),
                serde_json::json!({"dtype": f.dtype(), "shape": f.shape(), "names": null}),
            );
        }
        for (name, dtype) in [
            ("timestamp", "float32"),
            ("frame_index", "int64"),
            ("episode_index", "int64"),
            ("index", "int64"),
            ("task_index", "int64"),
        ] {
            features_json.insert(
                name.into(),
                serde_json::json!({"dtype": dtype, "shape": [1], "names": null}),
            );
        }
        let info = serde_json::json!({
            "codebase_version": "v3.0",
            "fps": opts.fps,
            "features": features_json,
            "total_episodes": episodes.len(),
            "total_frames": total_frames,
            "total_tasks": task_index.len(),
            "chunks_size": 1000,
            "data_files_size_in_mb": 100,
            "video_files_size_in_mb": 200,
            "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
            "video_path": null,
            "robot_type": null,
            "splits": {"train": format!("0:{}", episodes.len())},
        });
        fs::write(
            dest.join("meta/info.json"),
            serde_json::to_string_pretty(&info).map_err(perr)?,
        )
        .map_err(perr)?;

        // ---- meta/stats.json ---------------------------------------------------
        let mut stats = serde_json::Map::new();
        let mut add_stats = |name: &str, samples: Vec<Vec<f64>>| {
            // samples: one Vec per dimension (empty → all NULL).
            if samples.iter().all(|s| s.is_empty()) {
                return;
            }
            let n = samples.iter().map(|s| s.len()).sum::<usize>();
            let mut entry = serde_json::Map::new();
            for stat in [
                "min", "max", "mean", "std", "q01", "q10", "q50", "q90", "q99",
            ] {
                let q = stat[1..].parse::<f64>().map(|p| p / 100.0).ok();
                let vals: Vec<f64> = samples
                    .iter()
                    .map(|s| {
                        let mut s = s.clone();
                        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        match stat {
                            "min" => stat_min(&mut s),
                            "max" => stat_max(&mut s),
                            "mean" => stat_mean(&mut s),
                            "std" => stat_std(&mut s),
                            _ => quantile(&mut s, q.unwrap_or(0.5)),
                        }
                    })
                    .collect();
                entry.insert(stat.into(), serde_json::json!(vals));
            }
            entry.insert(
                "count".into(),
                serde_json::json!([n / samples.len().max(1)]),
            );
            stats.insert(name.to_string(), serde_json::Value::Object(entry));
        };
        for (fi, (name, feat, _)) in features.iter().enumerate() {
            if !feat.is_numeric() {
                continue;
            }
            match feat {
                Feature::F32Vector(dim) => {
                    let mut per_dim = vec![Vec::new(); *dim];
                    for f in &frames {
                        if let Value::Vector(v) = &f.feature_values[fi] {
                            for (d, x) in v.0.iter().enumerate() {
                                per_dim[d].push(*x as f64);
                            }
                        }
                    }
                    add_stats(name, per_dim);
                }
                _ => {
                    let vals: Vec<f64> = frames
                        .iter()
                        .filter_map(|f| match &f.feature_values[fi] {
                            Value::Float(v) => Some(*v),
                            Value::Integer(v) => Some(*v as f64),
                            _ => None,
                        })
                        .collect();
                    add_stats(name, vec![vals]);
                }
            }
        }
        // The five default columns carry stats too (verified against the
        // reference dataset's stats.json key list).
        add_stats(
            "timestamp",
            vec![timestamp_seq.iter().map(|v| *v as f64).collect()],
        );
        add_stats(
            "frame_index",
            vec![frame_index_seq.iter().map(|v| *v as f64).collect()],
        );
        add_stats(
            "episode_index",
            vec![episode_seq.iter().map(|v| *v as f64).collect()],
        );
        add_stats("index", vec![index_seq.iter().map(|v| *v as f64).collect()]);
        add_stats(
            "task_index",
            vec![task_seq.iter().map(|v| *v as f64).collect()],
        );
        fs::write(
            dest.join("meta/stats.json"),
            serde_json::to_string_pretty(&serde_json::Value::Object(stats)).map_err(perr)?,
        )
        .map_err(perr)?;

        Ok(LeRobotSummary {
            episodes: episodes.len(),
            frames: total_frames,
            tasks: task_index.len(),
        })
    }

    fn write_parquet(path: &Path, batch: RecordBatch) -> crate::Result<()> {
        let file = fs::File::create(path).map_err(perr)?;
        let mut w = ArrowWriter::try_new(file, batch.schema(), None).map_err(perr)?;
        w.write(&batch).map_err(perr)?;
        w.close().map_err(perr)?;
        Ok(())
    }

    fn column_position(columns: &[ColumnDef], name: &str) -> crate::Result<usize> {
        columns.iter().position(|c| c.name == name).ok_or_else(|| {
            crate::MoteDBError::InvalidArgument(format!("column '{name}' not found"))
        })
    }

    // ---- stat helpers (population std, nearest-rank quantiles) ------------
    fn stat_min(s: &mut [f64]) -> f64 {
        s.first().copied().unwrap_or(0.0)
    }
    fn stat_max(s: &mut [f64]) -> f64 {
        s.last().copied().unwrap_or(0.0)
    }
    fn stat_mean(s: &mut [f64]) -> f64 {
        if s.is_empty() {
            return 0.0;
        }
        s.iter().sum::<f64>() / s.len() as f64
    }
    fn stat_std(s: &mut [f64]) -> f64 {
        if s.len() < 2 {
            return 0.0;
        }
        let m = stat_mean(s);
        (s.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / s.len() as f64).sqrt()
    }
    /// Nearest-rank quantile on the pre-sorted slice; approximate by design
    /// (stats feed normalization heuristics, not correctness).
    fn quantile(s: &mut [f64], q: f64) -> f64 {
        if s.is_empty() {
            return 0.0;
        }
        let idx = ((s.len() as f64 - 1.0) * q).round() as usize;
        s[idx.min(s.len() - 1)]
    }
}
