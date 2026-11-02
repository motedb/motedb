//! MoteDB Node.js bindings (napi-rs v3).
//!
//! API mirrors the Python binding's surface:
//!   const db = new Database(path)   // create-or-open
//!   db.execute(sql, params?)        // mutations/DDL → { affectedRows }
//!   db.query(sql, params?)          // SELECT → row objects[]
//!   db.checkpoint() / db.backupTo(dst) / db.close() / db.doctor()
//!
//! Value mapping (JS ⇄ engine):
//!   number ↔ Integer/Float · string ↔ Text · boolean ↔ Bool · null ↔ NULL
//!   number[] ↔ VECTOR(dim) · Buffer ↔ BLOB · TIMESTAMP → µs-since-epoch
//!   number (exact in f64 below 2^53).
use base64::Engine as _;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use motedb_core::sql::QueryResult;
use motedb_core::types::{ArcBytes, ArcVec, Value as MValue};

fn py_err(e: motedb_core::MoteDBError) -> Error {
    Error::new(Status::GenericFailure, format!("{e}"))
}

fn st_err(e: motedb_core::StorageError) -> Error {
    Error::new(Status::GenericFailure, format!("{e}"))
}

/// One params-array element → engine Value.
///
/// 🔑 P0 correctness: JS numbers are ALL doubles, and napi's `get::<i64>`
/// does not fail on a double — it TRUNCATES (9.9 → 9, 1e-7 → 0). An
/// integer-first probe therefore corrupted every non-integral param.
/// Numbers are probed as f64 (strict — non-numbers fail cleanly) and split
/// by fract(); BigInt converts through its own lossless API.
fn elem_to_mote(arr: &Array, i: u32) -> Result<MValue> {
    if let Some(f) = arr.get::<f64>(i).ok().flatten() {
        if f.is_finite() && f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0 {
            return Ok(MValue::Integer(f as i64));
        }
        return Ok(MValue::Float(f));
    }
    if let Some(b) = arr.get::<napi::bindgen_prelude::BigInt>(i).ok().flatten() {
        let (v, lossless) = b.get_i64();
        if !lossless {
            return Err(Error::new(
                Status::InvalidArg,
                format!("BigInt parameter at index {i} does not fit in i64"),
            ));
        }
        return Ok(MValue::Integer(v));
    }
    if let Some(x) = arr.get::<bool>(i).ok().flatten() {
        return Ok(MValue::Bool(x));
    }
    if let Some(b) = arr.get::<Buffer>(i).ok().flatten() {
        return Ok(MValue::Blob(ArcBytes::new(b.as_ref().to_vec())));
    }
    if let Some(s) = arr.get::<String>(i).ok().flatten() {
        return Ok(MValue::Text(motedb_core::types::ArcString(
            std::sync::Arc::from(s.as_str()),
        )));
    }
    if let Some(v) = arr.get::<Vec<f64>>(i).ok().flatten() {
        return Ok(MValue::Vector(ArcVec::new(
            v.into_iter().map(|x| x as f32).collect(),
        )));
    }
    // null LAST: napi 3.14's Null::from_napi_value doesn't validate the js
    // type, so an early probe matched every element.
    if arr.get::<Null>(i).ok().flatten().is_some() {
        return Ok(MValue::Null);
    }
    Err(Error::new(
        Status::InvalidArg,
        format!(
            "unsupported parameter type at index {i} (number | string | bool | null | number[] | Buffer)"
        ),
    ))
}

/// Execute-side helper: convert an optional JS params array.
fn convert_params(params: Option<&Array>) -> Result<Vec<MValue>> {
    let Some(arr) = params else {
        return Ok(Vec::new());
    };
    let len = arr.len();
    let mut out = Vec::with_capacity(len as usize);
    for i in 0..len {
        out.push(elem_to_mote(arr, i)?);
    }
    Ok(out)
}

/// Convert one engine value → JSON (row-object cell).
/// BLOB → base64 string (JSON has no bytes type).
fn value_to_json(v: &MValue) -> serde_json::Value {
    match v {
        MValue::Null | MValue::TextDoc(_) => serde_json::Value::Null,
        MValue::Integer(i) => serde_json::Value::Number((*i).into()),
        MValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        MValue::Bool(b) => serde_json::Value::Bool(*b),
        MValue::Text(t) => serde_json::Value::String(t.as_str().to_string()),
        MValue::Timestamp(t) => serde_json::Value::Number(t.as_micros().into()),
        // 🔖 DATE → 'YYYY-MM-DD' (ISO string round-trips on insert/compare).
        MValue::Date(d) => serde_json::Value::String(d.to_iso()),
        MValue::Vector(vec) => {
            let items: Vec<serde_json::Value> = vec
                .iter()
                .map(|&x| {
                    serde_json::Number::from_f64(x as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();
            serde_json::Value::Array(items)
        }
        MValue::Blob(b) => serde_json::Value::String(BASE64.encode(b.as_slice())),
        MValue::Tensor(_) => serde_json::Value::Null,
        MValue::Spatial(g) => {
            let mut m = serde_json::Map::new();
            match &**g {
                motedb_core::types::Geometry::Point(p) => {
                    m.insert("type".into(), serde_json::Value::String("Point".into()));
                    m.insert("x".into(), serde_json::json!(p.x));
                    m.insert("y".into(), serde_json::json!(p.y));
                }
                motedb_core::types::Geometry::Point3D(p) => {
                    m.insert("type".into(), serde_json::Value::String("Point3D".into()));
                    m.insert("x".into(), serde_json::json!(p.x));
                    m.insert("y".into(), serde_json::json!(p.y));
                    m.insert("z".into(), serde_json::json!(p.z));
                }
                motedb_core::types::Geometry::LineString(pts) => {
                    m.insert(
                        "type".into(),
                        serde_json::Value::String("LineString".into()),
                    );
                    let list: Vec<serde_json::Value> =
                        pts.iter().map(|p| serde_json::json!([p.x, p.y])).collect();
                    m.insert("points".into(), serde_json::Value::Array(list));
                }
                motedb_core::types::Geometry::Polygon(pts) => {
                    m.insert("type".into(), serde_json::Value::String("Polygon".into()));
                    let list: Vec<serde_json::Value> =
                        pts.iter().map(|p| serde_json::json!([p.x, p.y])).collect();
                    m.insert("points".into(), serde_json::Value::Array(list));
                }
            }
            serde_json::Value::Object(m)
        }
        // 🔒 Value is #[non_exhaustive]: unknown (future) variants map to
        // null so an older npm package never breaks on a newer engine.
        // Must stay the LAST arm — an earlier wildcard shadows every
        // concrete arm above it.
        _ => serde_json::Value::Null,
    }
}

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

#[napi(object)]
pub struct ExecResult {
    pub affected_rows: f64,
}

#[napi]
pub struct Database {
    inner: motedb_core::Database,
    /// Latest begun-but-unresolved transaction id (commit/rollback with no
    /// argument act on it) — mirrors the Python binding's `last_tx`.
    last_tx: std::sync::Mutex<Option<u64>>,
}

impl Database {
    /// Transaction id for commit()/rollback(): the explicit argument, else
    /// the latest begin(). Ids are small counters, exact as f64.
    fn resolve_tx(&self, tx: Option<f64>) -> Result<u64> {
        if let Some(t) = tx {
            if !t.is_finite() || t < 0.0 || t.fract() != 0.0 {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("invalid transaction id {t}"),
                ));
            }
            return Ok(t as u64);
        }
        self.last_tx.lock().unwrap().ok_or_else(|| {
            Error::new(
                Status::InvalidArg,
                "no transaction: pass the id returned by begin() or call begin() first",
            )
        })
    }
}

#[napi]
impl Database {
    /// Create-or-open an embedded database at `path`.
    #[napi(constructor)]
    pub fn new(path: String) -> Result<Self> {
        let inner = if std::path::Path::new(&path).exists() {
            motedb_core::Database::open(&path)
        } else {
            motedb_core::Database::create(&path)
        }
        .map_err(py_err)?;
        Ok(Self {
            inner,
            last_tx: std::sync::Mutex::new(None),
        })
    }

    /// 🔑 容器模式: open a MOTESNAP container read-only.
    #[napi]
    pub fn open_readonly(container: String) -> Result<Self> {
        let inner = motedb_core::Database::open_readonly(&container).map_err(py_err)?;
        Ok(Self {
            inner,
            last_tx: std::sync::Mutex::new(None),
        })
    }

    /// Run a mutation/DDL statement (with optional `?` params). Returns
    /// { affectedRows }.
    #[napi]
    pub fn execute(&self, sql: String, params: Option<Array>) -> Result<ExecResult> {
        let affected = match &params {
            None => self.inner.execute(&sql).map_err(py_err)?.affected_rows(),
            Some(arr) => {
                let converted = convert_params(Some(arr))?;
                self.inner
                    .execute_prepared(&sql, converted)
                    .map_err(py_err)?
                    .affected_rows()
            }
        };
        Ok(ExecResult {
            affected_rows: affected as f64,
        })
    }

    /// Run a SELECT; rows come back as objects (column → value).
    /// BLOB columns are base64 strings; VECTOR columns are number arrays;
    /// TIMESTAMP columns are µs-since-epoch numbers.
    #[napi]
    pub fn query(&self, sql: String, params: Option<Array>) -> Result<Vec<serde_json::Value>> {
        let result = match &params {
            None => self.inner.execute(&sql).map_err(py_err)?,
            Some(arr) => {
                let converted = convert_params(Some(arr))?;
                self.inner
                    .execute_prepared(&sql, converted)
                    .map_err(py_err)?
            }
        };
        let materialized = result.materialize().map_err(st_err)?;
        let QueryResult::Select { columns, rows } = materialized else {
            return Err(Error::new(
                Status::GenericFailure,
                "query() expects a SELECT — use execute() for mutations",
            ));
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut m = serde_json::Map::with_capacity(columns.len());
            for (ci, col) in columns.iter().enumerate() {
                if col.starts_with("__") {
                    continue; // internal helper columns
                }
                if let Some(v) = row.get(ci) {
                    m.insert(col.clone(), value_to_json(v));
                }
            }
            out.push(serde_json::Value::Object(m));
        }
        Ok(out)
    }

    /// Run one INSERT/UPDATE/DELETE template once per parameter row as a
    /// single batch (one multi-row INSERT, one WAL fsync). `params` is an
    /// array of per-row parameter arrays. Returns { affectedRows }.
    #[napi]
    pub fn execute_many(&self, sql: String, params: Array) -> Result<ExecResult> {
        let n = params.len();
        let mut batch: Vec<Vec<MValue>> = Vec::with_capacity(n as usize);
        for i in 0..n {
            let row = params.get::<Array>(i).ok().flatten().ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    format!("params[{i}] must be an array of parameters"),
                )
            })?;
            batch.push(convert_params(Some(&row))?);
        }
        let affected = self
            .inner
            .execute_prepared_many(&sql, batch)
            .map_err(py_err)?;
        Ok(ExecResult {
            affected_rows: affected as f64,
        })
    }

    /// Columnar bulk insert — the bulk-load form of the Python binding:
    ///   db.insertArrays("ev", { id: [1, 2], emb: [[0.1, 0.2], [0.3, 0.4]], tag: ["a", "b"] })
    /// Values are placed by schema position (column names are matched
    /// case-insensitively); omitted columns are NULL, so an auto-increment PK
    /// can be left out. Every given column must have the same length. Skips
    /// SQL parsing and per-row parameter binding. Returns the row count.
    #[napi]
    pub fn insert_arrays(&self, table: String, columns: Object) -> Result<f64> {
        let schema_cols = self.inner.table_columns(&table).map_err(py_err)?;
        let mut by_pos: Vec<Option<Vec<MValue>>> = vec![None; schema_cols.len()];
        for name in Object::keys(&columns)? {
            let pos = schema_cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    Error::new(
                        Status::InvalidArg,
                        format!("unknown column '{name}' for table '{table}'"),
                    )
                })?;
            if by_pos[pos].is_some() {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("column '{name}' given twice"),
                ));
            }
            let arr = columns.get::<Array>(&name)?.ok_or_else(|| {
                Error::new(
                    Status::InvalidArg,
                    format!("column '{name}' must be an array of values"),
                )
            })?;
            let len = arr.len();
            let mut vals = Vec::with_capacity(len as usize);
            for i in 0..len {
                vals.push(elem_to_mote(&arr, i).map_err(|e| {
                    Error::new(Status::InvalidArg, format!("column '{name}': {}", e.reason))
                })?);
            }
            by_pos[pos] = Some(vals);
        }
        if by_pos.iter().all(|c| c.is_none()) {
            return Err(Error::new(Status::InvalidArg, "columns object is empty"));
        }
        let nrows = by_pos
            .iter()
            .find_map(|c| c.as_ref().map(|v| v.len()))
            .unwrap_or(0);
        for (i, c) in by_pos.iter().enumerate() {
            if let Some(vals) = c {
                if vals.len() != nrows {
                    return Err(Error::new(
                        Status::InvalidArg,
                        format!(
                            "column '{}' has {} rows, expected {}",
                            schema_cols[i],
                            vals.len(),
                            nrows
                        ),
                    ));
                }
            }
        }
        if nrows == 0 {
            return Ok(0.0);
        }
        let ncols = by_pos.len();
        let mut rows: Vec<Vec<MValue>> = Vec::with_capacity(nrows);
        for r in 0..nrows {
            let mut row = Vec::with_capacity(ncols);
            for c in by_pos.iter_mut() {
                row.push(match c {
                    Some(vals) => std::mem::replace(&mut vals[r], MValue::Null),
                    None => MValue::Null,
                });
            }
            rows.push(row);
        }
        let n = self.inner.insert_rows(&table, rows).map_err(py_err)?;
        Ok(n as f64)
    }

    /// Hybrid search: BM25 text hits and vector KNN hits fused by Reciprocal
    /// Rank Fusion. Rows come back as objects plus `__rrf__` (fused score,
    /// higher = better), `__bm25__` (null when the text list missed the doc)
    /// and `__distance__` (null when KNN missed it).
    ///   db.hybridSearch("docs_body", "rust database", "docs_emb", [0.1, 0.2, 0.3, 0.4], 10)
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub fn hybrid_search(
        &self,
        text_index: String,
        text_query: String,
        vector_index: String,
        query_vector: Vec<f64>,
        k: Option<u32>,
        rrf_k: Option<u32>,
        fetch_mult: Option<u32>,
    ) -> Result<Vec<serde_json::Value>> {
        let qv: Vec<f32> = query_vector.into_iter().map(|x| x as f32).collect();
        let (names, rows, hits) = self
            .inner
            .hybrid_search_rows(
                &text_index,
                &text_query,
                &vector_index,
                &qv,
                k.unwrap_or(10) as usize,
                rrf_k.unwrap_or(60) as usize,
                fetch_mult.unwrap_or(4) as usize,
            )
            .map_err(py_err)?;
        let score = |s: Option<f32>| {
            s.and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        };
        let mut out = Vec::with_capacity(rows.len());
        for (row, hit) in rows.iter().zip(hits.iter()) {
            let mut m = serde_json::Map::with_capacity(names.len() + 3);
            for (col, v) in names.iter().zip(row.iter()) {
                m.insert(col.clone(), value_to_json(v));
            }
            m.insert("__rrf__".into(), score(Some(hit.rrf)));
            m.insert("__bm25__".into(), score(hit.bm25));
            m.insert("__distance__".into(), score(hit.distance));
            out.push(serde_json::Value::Object(m));
        }
        Ok(out)
    }

    /// Begin a transaction; returns its id. Statements run through this
    /// handle until commit()/rollback() belong to it.
    #[napi]
    pub fn begin(&self) -> Result<f64> {
        let tx = self.inner.begin_transaction().map_err(py_err)?;
        *self.last_tx.lock().unwrap() = Some(tx);
        Ok(tx as f64)
    }

    /// Commit a transaction begun with begin() (no argument = the latest).
    #[napi]
    pub fn commit(&self, tx: Option<f64>) -> Result<()> {
        let tx = self.resolve_tx(tx)?;
        self.inner.commit_transaction(tx).map_err(py_err)?;
        *self.last_tx.lock().unwrap() = None;
        Ok(())
    }

    /// Roll back a transaction begun with begin() (no argument = the latest).
    #[napi]
    pub fn rollback(&self, tx: Option<f64>) -> Result<()> {
        let tx = self.resolve_tx(tx)?;
        self.inner.rollback_transaction(tx).map_err(py_err)?;
        *self.last_tx.lock().unwrap() = None;
        Ok(())
    }

    /// Flush pending writes + truncate the WAL.
    #[napi]
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint().map_err(py_err)
    }

    /// Online snapshot into a new directory (must be outside the DB dir).
    #[napi]
    pub fn backup_to(&self, dest: String) -> Result<()> {
        self.inner.backup_to(&dest).map_err(st_err)
    }

    /// 🔑 P4: pack the whole database into a single MOTESNAP file.
    /// Returns the number of packed files.
    #[napi]
    pub fn export_snapshot(&self, dest: String, compressed: Option<bool>) -> Result<i32> {
        self.inner
            .export_snapshot(&dest, compressed.unwrap_or(false))
            .map(|s| s.entries as i32)
            .map_err(st_err)
    }

    /// 🔑 P4: unpack a MOTESNAP into a fresh directory and open it.
    #[napi]
    pub fn open_snapshot(src: String, dest_dir: String) -> Result<Self> {
        let inner = motedb_core::Database::open_snapshot(&src, &dest_dir).map_err(st_err)?;
        Ok(Self {
            inner,
            last_tx: std::sync::Mutex::new(None),
        })
    }

    /// Close the database (flush + clean-close marker).
    #[napi]
    pub fn close(&self) -> Result<()> {
        self.inner.close().map_err(py_err)
    }

    /// Self-check report: array of {name, status, detail}.
    #[napi]
    pub fn doctor(&self) -> Result<Vec<serde_json::Value>> {
        let report = self.inner.doctor();
        let mut out = Vec::with_capacity(report.checks.len());
        for c in &report.checks {
            out.push(serde_json::json!({
                "name": c.name,
                "status": c.status.label(),
                "detail": c.detail,
            }));
        }
        Ok(out)
    }
}
