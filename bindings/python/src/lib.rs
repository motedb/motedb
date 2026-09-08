//! Python bindings for MoteDB.
//!
//! ```python
//! import motedb
//! db = motedb.Database("/path/store", preset="edge")
//! db.execute("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, v VECTOR(3), doc TEXT)")
//! db.execute("INSERT INTO t (id, v, doc) VALUES (?, ?, ?)", params=[1, [0.1, 0.2, 0.3], "hello"])
//! rows = db.query("SELECT id, doc FROM t ORDER BY v <-> ? LIMIT 5", params=[[0.1, 0.2, 0.3]])
//! db.close()
//! ```
use motedb_core::types::Value as MValue;
use motedb_core::{DBConfig, Database};
use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

// ── Value conversion ──────────────────────────────────────────────────

fn mote_to_py(v: &MValue) -> PyObject {
    Python::with_gil(|py| -> PyObject {
        match v {
            MValue::Null => py.None(),
            MValue::Integer(i) => i.into_py(py),
            MValue::Float(f) => f.into_py(py),
            MValue::Bool(b) => b.into_py(py),
            MValue::Text(t) => t.as_str().into_py(py),
            MValue::Vector(vec) => {
                let list: Vec<f64> = vec.0.iter().map(|&x| x as f64).collect();
                list.into_py(py)
            }
            MValue::Timestamp(ts) => ts.as_micros().into_py(py),
            // Tensor / Spatial / TextDoc: not first-class in Python yet —
            // render as a tag so users see WHAT it is instead of garbage.
            MValue::Tensor(_) => "<tensor>".into_py(py),
            MValue::Spatial(_) => "<geometry>".into_py(py),
            MValue::TextDoc(_) => "<textdoc>".into_py(py),
        }
    })
}

fn py_to_mote(v: &Bound<'_, PyAny>) -> PyResult<MValue> {
    use pyo3::types::PyAnyMethods as _;
    if v.is_none() {
        return Ok(MValue::Null);
    }
    if let Ok(b) = v.extract::<bool>() {
        return Ok(MValue::Bool(b));
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(MValue::Integer(i));
    }
    if let Ok(f) = v.extract::<f64>() {
        return Ok(MValue::Float(f));
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(MValue::Text(s.into()));
    }
    // list/tuple of numbers → embedding vector (f32 storage)
    if let Ok(seq) = v.extract::<Vec<f64>>() {
        return Ok(MValue::Vector(motedb_core::types::ArcVec::new(
            seq.iter().map(|&x| x as f32).collect(),
        )));
    }
    if let Ok(seq) = v.extract::<Vec<i64>>() {
        return Ok(MValue::Vector(motedb_core::types::ArcVec::new(
            seq.iter().map(|&x| x as f32).collect(),
        )));
    }
    Err(PyValueError::new_err(format!(
        "unsupported parameter type: {}",
        v.get_type()
    )))
}

fn py_err(e: motedb_core::StorageError) -> PyErr {
    // Connection-ish errors (open on a corrupt/locked store) map to
    // ConnectionError; everything else is a runtime/query error.
    let msg = e.to_string();
    if msg.contains("IO error") || msg.contains("Database already exists") {
        PyConnectionError::new_err(msg)
    } else {
        PyRuntimeError::new_err(msg)
    }
}

fn config_for_preset(preset: Option<&str>, path: &str) -> PyResult<DBConfig> {
    let mut config = match preset {
        None => DBConfig::for_general(),
        Some("general") => DBConfig::for_general(),
        Some("edge") => DBConfig::for_edge(),
        Some("robotics") => DBConfig::for_robotics(),
        Some("embodied") => DBConfig::for_embodied(),
        Some(other) => {
            return Err(PyValueError::new_err(format!(
                "unknown preset {other:?} (use general|edge|robotics|embodied)"
            )))
        }
    };
    // The Python layer is for applications, not benchmarks: keep the
    // background auto-checkpoint (durability) enabled with default cadence.
    let _ = &mut config;
    let _ = path;
    Ok(config)
}

// ── Database ──────────────────────────────────────────────────────────

/// An embedded MoteDB database.
///
/// Create (new store) or open (existing store) — the same call does both
/// when the store already exists and its format is compatible.
#[pyclass]
struct PyDatabase {
    db: Database,
}

#[pymethods]
impl PyDatabase {
    /// Database(path, preset=None, create=True)
    #[new]
    #[pyo3(signature = (path, preset=None, create=true))]
    fn new(path: &str, preset: Option<&str>, create: bool) -> PyResult<Self> {
        let config = config_for_preset(preset, path)?;
        let db = if create {
            if std::path::Path::new(path).exists() {
                Database::open_with_config(path, config).map_err(py_err)?
            } else {
                Database::create_with_config(path, config).map_err(py_err)?
            }
        } else {
            Database::open_with_config(path, config).map_err(py_err)?
        };
        Ok(Self { db })
    }

    /// Execute a statement. SELECT returns a list of row dicts;
    /// INSERT/UPDATE/DELETE returns the affected-row count; DDL returns None.
    #[pyo3(signature = (sql, params=None))]
    fn execute(&self, sql: &str, params: Option<Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let result = self.run(sql, params)?;
        Python::with_gil(|py| -> PyResult<PyObject> {
            use pyo3::types::PyAnyMethods as _;
            match result {
                motedb_core::QueryResult::Select { columns, rows } => {
                    let list = pyo3::types::PyList::empty_bound(py);
                    for row in rows {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        for (col, val) in columns.iter().zip(row.iter()) {
                            dict.set_item(col, mote_to_py(val))?;
                        }
                        list.append(dict)?;
                    }
                    Ok(list.into_any().unbind().into())
                }
                motedb_core::QueryResult::Modification { affected_rows } => {
                    Ok(affected_rows.into_py(py).into())
                }
                _ => Ok(py.None()),
            }
        })
    }

    /// Execute a SELECT and return (columns, list-of-row-tuples).
    /// For large results this is materially cheaper than dicts.
    #[pyo3(signature = (sql, params=None))]
    fn query(&self, sql: &str, params: Option<Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let result = self.run(sql, params)?;
        Python::with_gil(|py| -> PyResult<PyObject> {
            use pyo3::types::PyAnyMethods as _;
            match result {
                motedb_core::QueryResult::Select { columns, rows } => {
                    let cols: Vec<String> = columns;
                    let list = pyo3::types::PyList::empty_bound(py);
                    for row in rows {
                        let tuple = pyo3::types::PyTuple::new_bound(
                            py,
                            row.iter().map(mote_to_py).collect::<Vec<_>>(),
                        );
                        list.append(tuple)?;
                    }
                    let t = pyo3::types::PyTuple::new_bound(
                        py,
                        vec![cols.into_py(py), list.into_any().unbind().into()],
                    );
                    Ok(t.into_any().unbind().into())
                }
                _ => Err(PyValueError::new_err("query() expects a SELECT statement")),
            }
        })
    }

    /// Begin a transaction; returns its id (pass to commit/rollback).
    fn begin(&self) -> PyResult<u64> {
        self.db.begin_transaction().map_err(py_err)
    }

    /// Commit a transaction begun with begin().
    fn commit(&self, tx: u64) -> PyResult<()> {
        self.db.commit_transaction(tx).map_err(py_err)
    }

    /// Roll back a transaction begun with begin().
    fn rollback(&self, tx: u64) -> PyResult<()> {
        self.db.rollback_transaction(tx).map_err(py_err)
    }

    /// Flush buffers + fsync (durability point).
    fn checkpoint(&self) -> PyResult<()> {
        self.db.checkpoint().map_err(py_err)
    }

    /// Reclaim disk space (full compaction).
    fn vacuum(&self) -> PyResult<()> {
        self.db.vacuum().map_err(py_err)
    }

    /// Close the database (also happens on GC/drop).
    fn close(&self) -> PyResult<()> {
        self.db.close().map_err(py_err)
    }

    fn __repr__(&self) -> String {
        "MoteDB::Database".to_string()
    }
}

impl PyDatabase {
    fn run(
        &self,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<motedb_core::QueryResult> {
        let streaming = match params {
            None => self.db.execute(sql).map_err(py_err)?,
            Some(p) => {
                use pyo3::types::PyAnyMethods as _;
                let list = p
                    .extract::<Vec<Bound<'_, PyAny>>>()
                    .map_err(|_| PyValueError::new_err("params must be a list"))?;
                let vals: Vec<MValue> = list
                    .iter()
                    .map(py_to_mote)
                    .collect::<PyResult<Vec<_>>>()?;
                self.db
                    .execute_prepared(sql, vals)
                    .map_err(py_err)?
            }
        };
        streaming.materialize().map_err(py_err)
    }
}

#[pymodule]
fn motedb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // `Database` is the primary entry point — expose it under that name;
    // PyDatabase remains importable for introspection.
    m.add_class::<PyDatabase>()?;
    m.add("Database", m.getattr("PyDatabase")?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
