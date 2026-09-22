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

/// FxHash-style multiplicative hasher for the per-query TEXT intern map.
/// std's SipHash costs ~1.3ms per 100K-row projection (unique strings are
/// still hashed for the lookup); the keys are untrusted-free column data,
/// not attacker-controlled table keys, so a fast non-crypto hash suffices.
#[derive(Default)]
struct FxHasher {
    hash: u64,
}
impl std::hash::Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            self.hash = (self.hash.rotate_left(5) ^ u64::from_le_bytes(buf)).wrapping_mul(0x517cc1b727220a95);
        }
    }
    fn write_u8(&mut self, i: u8) {
        self.hash = (self.hash.rotate_left(5) ^ i as u64).wrapping_mul(0x517cc1b727220a95);
    }
    fn write_usize(&mut self, i: usize) {
        self.hash = (self.hash.rotate_left(5) ^ i as u64).wrapping_mul(0x517cc1b727220a95);
    }
    fn finish(&self) -> u64 {
        self.hash
    }
}
type FxBuild = std::hash::BuildHasherDefault<FxHasher>;
type InternMap = std::collections::HashMap<std::sync::Arc<str>, PyObject, FxBuild>;

/// `mote_to_py` with a per-query intern cache for TEXT values.
/// Low-cardinality columns (device ids, enums, statuses) map thousands of
/// rows onto a handful of distinct strings: every hit saves a PyUnicode_New
/// + copy. Uniquely-valued columns stop growing the cache at 8192 entries,
/// so their only overhead is one hash lookup per value.
fn mote_to_py_cached(v: &MValue, text_cache: &mut InternMap) -> PyObject {
    if let MValue::Text(t) = v {
        if let Some(hit) = text_cache.get(t.as_str()) {
            return Python::with_gil(|py| hit.clone_ref(py));
        }
        let obj: PyObject = Python::with_gil(|py| t.as_str().into_py(py));
        if text_cache.len() < 8192 {
            let cloned = Python::with_gil(|py| obj.clone_ref(py));
            text_cache.insert(t.0.clone(), cloned);
        }
        return obj;
    }
    mote_to_py(v)
}

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
            // Spatial: {"type": "Point3D", "x": .., "y": .., "z": ..} /
            // {"type": "Point", "x": .., "y": ..} / {"type": "LineString",
            // "points": [[x, y], ...]} / {"type": "Polygon", ...} — round
            // trippable by py_to_mote below (used to be a "<geometry>" tag,
            // so Python could read back neither coordinates nor equality).
            MValue::Spatial(g) => {
                use motedb_core::types::Geometry;
                match &**g {
                    Geometry::Point(p) => {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        dict.set_item("type", "Point").ok();
                        dict.set_item("x", p.x).ok();
                        dict.set_item("y", p.y).ok();
                        dict.into_any().unbind()
                    }
                    Geometry::Point3D(p) => {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        dict.set_item("type", "Point3D").ok();
                        dict.set_item("x", p.x).ok();
                        dict.set_item("y", p.y).ok();
                        dict.set_item("z", p.z).ok();
                        dict.into_any().unbind()
                    }
                    Geometry::LineString(pts) => {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        dict.set_item("type", "LineString").ok();
                        let list: Vec<(f64, f64)> = pts.iter().map(|p| (p.x, p.y)).collect();
                        dict.set_item("points", list).ok();
                        dict.into_any().unbind()
                    }
                    Geometry::Polygon(pts) => {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        dict.set_item("type", "Polygon").ok();
                        let list: Vec<(f64, f64)> = pts.iter().map(|p| (p.x, p.y)).collect();
                        dict.set_item("points", list).ok();
                        dict.into_any().unbind()
                    }
                }
            }
            // Tensor / TextDoc: not first-class in Python yet — render as a
            // tag so users see WHAT it is instead of garbage.
            MValue::Tensor(_) => "<tensor>".into_py(py),
            MValue::TextDoc(_) => "<textdoc>".into_py(py),
        }
    })
}

/// 列值提取: numpy 数组 (buffer 协议) → 类型化切片; 2D float32 → 向量列;
/// 其它 (str 列表/标量列表) → 逐对象 py_to_mote。
fn extract_column_values(obj: &Bound<'_, pyo3::types::PyAny>) -> PyResult<Vec<motedb_core::types::Value>> {
    use motedb_core::types::Value as MValue;
    use pyo3::types::PyAnyMethods as _;

    // 1) numpy 风格数组: tobytes() 一次 memcpy 出原始 C 序字节 (abi3 无
    //    buffer 协议 — PyBuffer 需要完整 API), dtype/shape 自省解码。
    //    仍是零逐元素 Python 对象。
    if obj.hasattr("tobytes")? && obj.hasattr("dtype")? {
        let dtype: String = obj
            .getattr("dtype")?
            .getattr("str")?
            .extract()?;
        let shape: Vec<usize> = obj.getattr("shape")?.extract()?;
        // 🔑 bytes 对象必须 downcast 成 PyBytes 用 as_bytes() 借切片 —
        // extract::<Vec<u8>> 走通用序列提取, 每字节建一个 PyLong (100K×384
        // 的导入 95% 时间在这, sample 剖析实锤), 慢两个数量级。
        let bytes_obj = obj.call_method0("tobytes")?;
        let bytes_bound = bytes_obj
            .downcast::<pyo3::types::PyBytes>()
            .map_err(|_| {
                PyValueError::new_err("tobytes() did not return a bytes object")
            })?;
        return decode_array_bytes(&dtype, &shape, bytes_bound.as_bytes());
    }
    // 2) 同构列表快路径: 单次批量 C-API 提取 — 逐对象 py_to_mote (每次
    //    先试 bool/i64/f64 再 String) 曾占导入的 2/3。
    if let Ok(strs) = obj.extract::<Vec<String>>() {
        return Ok(strs.into_iter().map(|s| MValue::Text(s.into())).collect());
    }
    if let Ok(ints) = obj.extract::<Vec<i64>>() {
        return Ok(ints.into_iter().map(MValue::Integer).collect());
    }
    if let Ok(floats) = obj.extract::<Vec<f64>>() {
        return Ok(floats.into_iter().map(MValue::Float).collect());
    }
    // 3) 混合列表回退
    if let Ok(seq) = obj.extract::<Vec<Bound<'_, pyo3::types::PyAny>>>() {
        let mut out = Vec::with_capacity(seq.len());
        for item in &seq {
            out.push(py_to_mote(item)?);
        }
        return Ok(out);
    }
    Err(PyValueError::new_err(
        "column value must be a numpy array or a list",
    ))
}

/// 解码 numpy tobytes 的原始字节 (本机小端) 为逐行 Value。
fn decode_array_bytes(
    dtype: &str,
    shape: &[usize],
    bytes: &[u8],
) -> PyResult<Vec<motedb_core::types::Value>> {
    use motedb_core::types::Value as MValue;
    let code = dtype.replace(['<', '=', '|', '>'], "");
    // numpy unicode ('<U7'): UTF-32LE 定宽、尾部 \0 填充 — tobytes 后按
    // itemsize 步长切行, 逐 u32 码点解码 (跳过逐 str 的 C-API 提取)。
    if let Some(nstr) = code.strip_prefix('U') {
        let chars_per_item: usize = nstr.parse().map_err(|_| {
            PyValueError::new_err(format!("bad unicode dtype: {}", dtype))
        })?;
        if shape.len() != 1 {
            return Err(PyValueError::new_err("unicode array must be 1-D"));
        }
        let stride = chars_per_item * 4;
        let n = shape[0];
        let mut out = Vec::with_capacity(n);
        for r in 0..n {
            let row = &bytes[r * stride..(r + 1) * stride];
            let mut s = String::with_capacity(chars_per_item);
            for c in (0..stride).step_by(4) {
                let cp = u32::from_le_bytes([row[c], row[c + 1], row[c + 2], row[c + 3]]);
                if cp == 0 {
                    break;
                }
                s.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
            }
            out.push(MValue::Text(s.into()));
        }
        return Ok(out);
    }
    let code = code.to_ascii_lowercase();
    match (code.as_str(), shape) {
        ("i8" | "l" | "int64", [_n]) => Ok(bytes
            .chunks_exact(8)
            .map(|c| MValue::Integer(i64::from_le_bytes(c.try_into().unwrap())))
            .collect()),
        ("f8" | "d" | "float64", [_n]) => Ok(bytes
            .chunks_exact(8)
            .map(|c| MValue::Float(f64::from_le_bytes(c.try_into().unwrap())))
            .collect()),
        ("f4" | "f" | "float32", [_n]) => Ok(bytes
            .chunks_exact(4)
            .map(|c| MValue::Float(f32::from_le_bytes(c.try_into().unwrap()) as f64))
            .collect()),
        ("f4" | "f" | "float32", [n, d]) => {
            // 2D float32 → 向量列
            let (n, d) = (*n, *d);
            let mut out = Vec::with_capacity(n);
            let mut off = 0usize;
            for _ in 0..n {
                let mut row = Vec::with_capacity(d);
                for _ in 0..d {
                    if off + 4 > bytes.len() {
                        return Err(PyValueError::new_err("array bytes shorter than shape"));
                    }
                    let c: [u8; 4] = bytes[off..off + 4].try_into().unwrap();
                    row.push(f32::from_le_bytes(c));
                    off += 4;
                }
                out.push(MValue::Vector(motedb_core::types::ArcVec::new(row)));
            }
            Ok(out)
        }
        _ => Err(PyValueError::new_err(format!(
            "unsupported numpy dtype/shape: {} {:?}",
            dtype, shape
        ))),
    }
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
    // dict with "type" Point/Point3D/LineString/Polygon → geometry
    if let Ok(dict) =
        v.extract::<std::collections::HashMap<String, pyo3::Bound<pyo3::types::PyAny>>>()
    {
        if let Some(t) = dict.get("type").and_then(|t| t.extract::<String>().ok()) {
            use motedb_core::types::{Geometry, Point, Point3D};
            let geom = match t.as_str() {
                "Point" => {
                    let x = dict.get("x").and_then(|v| v.extract::<f64>().ok());
                    let y = dict.get("y").and_then(|v| v.extract::<f64>().ok());
                    match (x, y) {
                        (Some(x), Some(y)) => Geometry::Point(Point::new(x, y)),
                        _ => {
                            return Err(PyValueError::new_err(
                                "Point geometry requires numeric 'x' and 'y'",
                            ))
                        }
                    }
                }
                "Point3D" => {
                    let x = dict.get("x").and_then(|v| v.extract::<f64>().ok());
                    let y = dict.get("y").and_then(|v| v.extract::<f64>().ok());
                    let z = dict.get("z").and_then(|v| v.extract::<f64>().ok());
                    match (x, y, z) {
                        (Some(x), Some(y), Some(z)) => Geometry::Point3D(Point3D::new(x, y, z)),
                        _ => {
                            return Err(PyValueError::new_err(
                                "Point3D geometry requires numeric 'x', 'y' and 'z'",
                            ))
                        }
                    }
                }
                "LineString" | "Polygon" => {
                    // 🔑 Python users pass [[x, y], ...] lists; pyo3's
                    // Vec<(f64, f64)> extraction only accepts real tuples, so
                    // every list-of-lists insert failed with "requires a
                    // non-empty 'points' list". Accept BOTH shapes.
                    let pts_raw = dict
                        .get("points")
                        .and_then(|v| v.extract::<Vec<(f64, f64)>>().ok())
                        .map(|tuples| {
                            tuples.into_iter().map(|(x, y)| vec![x, y]).collect::<Vec<_>>()
                        })
                        .or_else(|| {
                            dict.get("points")
                                .and_then(|v| v.extract::<Vec<Vec<f64>>>().ok())
                        });
                    let pts = match pts_raw {
                        Some(p) if !p.is_empty() => p,
                        _ => {
                            return Err(PyValueError::new_err(format!(
                                "{t} geometry requires a non-empty 'points' list of [x, y] pairs"
                            )))
                        }
                    };
                    let mut parsed: Vec<Point> = Vec::with_capacity(pts.len());
                    for pair in &pts {
                        if pair.len() != 2 {
                            return Err(PyValueError::new_err(format!(
                                "{t} geometry points must be [x, y] pairs (got {} values)",
                                pair.len()
                            )));
                        }
                        parsed.push(Point::new(pair[0], pair[1]));
                    }
                    if t == "LineString" {
                        Geometry::LineString(parsed)
                    } else {
                        Geometry::Polygon(parsed)
                    }
                }
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown geometry type {other:?} (Point|Point3D|LineString|Polygon)"
                    )))
                }
            };
            return Ok(MValue::Spatial(Box::new(geom)));
        }
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
    fn execute(&self, py: Python<'_>, sql: &str, params: Option<Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let result = self.run(py, sql, params)?;
        Python::with_gil(|py| -> PyResult<PyObject> {
            use pyo3::types::PyAnyMethods as _;
            match result {
                motedb_core::QueryResult::Select { columns, rows } => {
                    // 🚀 Create (and hash) each column-name PyString ONCE per
                    // query. The old per-row `set_item(&String, ..)` built a
                    // fresh PyUnicode key for every (row, column) pair — for
                    // a 100K×5 result that's 500K PyUnicode_New + 500K str
                    // hashes before any value conversion runs; CPython caches
                    // the hash inside the object, so reusing the key objects
                    // makes every later SetItem hash-free.
                    let keys: Vec<PyObject> =
                        columns.iter().map(|c| c.as_str().into_py(py)).collect();
                    let list = pyo3::types::PyList::empty_bound(py);
                    let mut text_cache: InternMap = std::collections::HashMap::default();
                    for row in rows {
                        let dict = pyo3::types::PyDict::new_bound(py);
                        for (key, val) in keys.iter().zip(row.iter()) {
                            dict.set_item(key.clone(), mote_to_py_cached(val, &mut text_cache))?;
                        }
                        list.append(dict)?;
                    }
                    Ok(list.into_any().unbind())
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
    fn query(&self, py: Python<'_>, sql: &str, params: Option<Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let result = self.run(py, sql, params)?;
        Python::with_gil(|py| -> PyResult<PyObject> {
            use pyo3::types::PyAnyMethods as _;
            match result {
                motedb_core::QueryResult::Select { columns, rows } => {
                    let cols: Vec<String> = columns;
                    let list = pyo3::types::PyList::empty_bound(py);
                    let mut text_cache: InternMap = std::collections::HashMap::default();
                    for row in rows {
                        let tuple = pyo3::types::PyTuple::new_bound(
                            py,
                            row.iter()
                                .map(|v| mote_to_py_cached(v, &mut text_cache))
                                .collect::<Vec<_>>(),
                        );
                        list.append(tuple)?;
                    }
                    let t = pyo3::types::PyTuple::new_bound(
                        py,
                        vec![cols.into_py(py), list.into_any().unbind()],
                    );
                    Ok(t.into_any().unbind())
                }
                _ => Err(PyValueError::new_err("query() expects a SELECT statement")),
            }
        })
    }

    /// Per-table budget (bytes) for decoded VECTOR columns — edge presets cap
    /// this so vector top-k cannot exceed the device's memory ceiling.
    fn set_vector_cache_budget(&self, table: &str, bytes: usize) -> PyResult<()> {
        self.db.set_vector_cache_budget(table, bytes).map_err(py_err)
    }

    /// Current per-table decoded-VECTOR cache budget (bytes).
    fn vector_cache_budget_bytes(&self, table: &str) -> PyResult<usize> {
        self.db.vector_cache_budget_bytes(table).map_err(py_err)
    }

    /// Begin a transaction; returns its id (pass to commit/rollback).
    fn begin(&self) -> PyResult<u64> {
        self.db.begin_transaction().map_err(py_err)
    }

    /// Execute one INSERT once per parameter set (executemany).
    /// All rows go through a single multi-row INSERT — one WAL fsync for the
    /// whole batch. `params` is a list of per-row parameter lists.
    /// Returns the affected-row count.
    /// 🔥 列式批量插入: `db.insert_arrays("ev", {"id": np_ids, "emb": emb2d, ...})`
    /// numpy 数组经 buffer 协议零拷贝读取 (i64/f64/f32/2D-f32), 字符串列接受
    /// str 列表。跳过 SQL 解析、参数绑定与逐行 Python 对象构造 — 100K×384
    /// 导入的 Python 侧成本从 ~0.8s (.tolist() 每 384 浮点建列表) 降到 ~0。
    #[pyo3(signature = (table, columns))]
    fn insert_arrays(&self, py: Python<'_>, table: &str, columns: Bound<'_, pyo3::types::PyDict>) -> PyResult<u64> {
        use pyo3::types::PyAnyMethods as _;
        use motedb_core::types::Value as MValue;

        // 🔑 按 schema 位置放置列值。旧实现按字典序转置 — 字典序 ≠ schema
        // 序时值静默落错列 (TEXT 列收到 Float 被清成空串), 省略前导自增 PK
        // 时 row[0] 被 auto id 覆盖, 100 行数据全毁且无报错。
        let schema_cols = self
            .db
            .table_columns(table)
            .map_err(py_err)?;
        let mut by_pos: Vec<Option<Vec<MValue>>> = vec![None; schema_cols.len()];
        for key in columns.keys() {
            let name = key
                .extract::<String>()
                .map_err(|_| PyValueError::new_err("column keys must be strings"))?;
            let pos = schema_cols
                .iter()
                .position(|c| c.eq_ignore_ascii_case(&name))
                .ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "unknown column '{}' for table '{}'",
                        name, table
                    ))
                })?;
            if by_pos[pos].is_some() {
                return Err(PyValueError::new_err(format!(
                    "column '{}' given twice",
                    name
                )));
            }
            let obj = columns
                .get_item(&name)
                .map_err(|_| PyValueError::new_err(format!("column '{}' missing", name)))?
                .unwrap_or_else(|| py.None().into_bound(py));
            by_pos[pos] = Some(extract_column_values(&obj)?);
        }
        if by_pos.iter().all(|c| c.is_none()) {
            return Err(PyValueError::new_err("columns dict is empty"));
        }
        let nrows = by_pos
            .iter()
            .find_map(|c| c.as_ref().map(|v| v.len()))
            .unwrap_or(0);
        for (i, c) in by_pos.iter().enumerate() {
            if let Some(vals) = c {
                if vals.len() != nrows {
                    return Err(PyValueError::new_err(format!(
                        "column '{}' has {} rows, expected {}",
                        schema_cols[i], vals.len(), nrows
                    )));
                }
            }
        }
        if nrows == 0 {
            return Ok(0);
        }
        // 转置成 schema 全宽行: 缺失列 = NULL (自增 PK 的 NULL 在批量路径
        // 由引擎填 auto id; 逐列消费避免整列 clone)。
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
        py.allow_threads(move || self.db.insert_rows(table, rows).map_err(py_err))
    }

    #[pyo3(signature = (sql, params))]
    fn executemany(&self, py: Python<'_>, sql: &str, params: Bound<'_, PyAny>) -> PyResult<usize> {
        use pyo3::types::PyAnyMethods as _;
        let list = params
            .extract::<Vec<Bound<'_, PyAny>>>()
            .map_err(|_| PyValueError::new_err("params must be a list of parameter lists"))?;
        let mut batch: Vec<Vec<MValue>> = Vec::with_capacity(list.len());
        for row in &list {
            let vals = row
                .extract::<Vec<Bound<'_, PyAny>>>()
                .map_err(|_| PyValueError::new_err("each params item must be a list"))?;
            let mut converted = Vec::with_capacity(vals.len());
            for v in &vals {
                converted.push(py_to_mote(v)?);
            }
            batch.push(converted);
        }
        // 🔑 GIL released for the Rust-side batch execution (see run()).
        py.allow_threads(move || self.db.execute_prepared_many(sql, batch))
            .map(|n| n as usize)
            .map_err(py_err)
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

    /// Operational self-check: list of {"name", "status", "detail"} dicts
    /// plus a "verdict" ("PASS" | "WARN" | "FAIL").
    fn doctor(&self) -> PyResult<PyObject> {
        let report = self.db.doctor();
        Python::with_gil(|py| -> PyResult<PyObject> {
            use pyo3::types::PyAnyMethods as _;
            let list = pyo3::types::PyList::empty_bound(py);
            for c in &report.checks {
                let dict = pyo3::types::PyDict::new_bound(py);
                dict.set_item("name", &c.name)?;
                dict.set_item("status", c.status.label())?;
                dict.set_item("detail", &c.detail)?;
                list.append(dict)?;
            }
            let out = pyo3::types::PyDict::new_bound(py);
            out.set_item("verdict", report.worst().label())?;
            out.set_item("checks", list)?;
            Ok(out.into_any().unbind())
        })
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
        py: Python<'_>,
        sql: &str,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<motedb_core::QueryResult> {
        // 🔑 Release the GIL for the whole Rust-side execution (params are
        // converted to Rust values first; the result converts back after).
        // Holding it serialized all DB calls across threads — a tight reader
        // loop starved a concurrent writer down to ~66 rows/s (vs 322 solo).
        let streaming = match params {
            None => py.allow_threads(|| self.db.execute(sql)).map_err(py_err)?,
            Some(p) => {
                use pyo3::types::PyAnyMethods as _;
                let list = p
                    .extract::<Vec<Bound<'_, PyAny>>>()
                    .map_err(|_| PyValueError::new_err("params must be a list"))?;
                let vals: Vec<MValue> =
                    list.iter().map(py_to_mote).collect::<PyResult<Vec<_>>>()?;
                py.allow_threads(move || self.db.execute_prepared(sql, vals))
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
