//! FFI (Foreign Function Interface) for C/Python/Node.js
//!
//! C ABI 导出接口，用于动态链接库

use crate::MoteDB;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

/// 不透明指针类型
pub struct MoteDBHandle {
    db: Arc<MoteDB>,
}

/// 打开数据库
///
/// # Safety
/// - path 必须是有效的 C 字符串
#[no_mangle]
pub unsafe extern "C" fn motedb_open(path: *const c_char) -> *mut MoteDBHandle {
    if path.is_null() {
        return ptr::null_mut();
    }

    let c_str = unsafe { CStr::from_ptr(path) };
    let path_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    match MoteDB::open(path_str) {
        Ok(db) => Box::into_raw(Box::new(MoteDBHandle { db: Arc::new(db) })),
        Err(_) => ptr::null_mut(),
    }
}

/// 关闭数据库
///
/// # Safety
/// - handle 必须是有效的 MoteDBHandle 指针
#[no_mangle]
pub unsafe extern "C" fn motedb_close(handle: *mut MoteDBHandle) {
    if !handle.is_null() {
        let _ = unsafe { Box::from_raw(handle) };
    }
}

/// 执行 SQL 查询
///
/// # Safety
/// - handle 必须是有效的 MoteDBHandle 指针
/// - sql 必须是有效的 C 字符串
#[no_mangle]
pub unsafe extern "C" fn motedb_execute(
    handle: *mut MoteDBHandle,
    sql: *const c_char,
) -> *mut c_char {
    if handle.is_null() || sql.is_null() {
        return ptr::null_mut();
    }

    let handle = unsafe { &mut *handle };
    let c_str = unsafe { CStr::from_ptr(sql) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    // ✅ 使用流式 API 并立即物化
    use crate::sql::{Lexer, Parser, QueryExecutor};

    let result = (|| -> crate::Result<_> {
        let mut lexer = Lexer::new(sql_str);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let statement = parser.parse()?;
        let executor = QueryExecutor::new(handle.db.clone());
        let streaming_result = executor.execute_streaming(statement)?;
        streaming_result.materialize()
    })();

    match result {
        Ok(result) => {
            let json = format!("{:?}", result);
            match CString::new(json) {
                Ok(c_string) => c_string.into_raw(),
                Err(_) => ptr::null_mut(),
            }
        }
        Err(e) => {
            let error = format!("Error: {}", e);
            match CString::new(error) {
                Ok(c_string) => c_string.into_raw(),
                Err(_) => ptr::null_mut(),
            }
        }
    }
}

/// 释放字符串内存
///
/// # Safety
/// - s 必须是由 motedb_execute 返回的指针
#[no_mangle]
pub unsafe extern "C" fn motedb_free_string(s: *mut c_char) {
    if !s.is_null() {
        let _ = unsafe { CString::from_raw(s) };
    }
}

// ============================================================================
// v0.15 C ABI (Go SDK): JSON-parameterized high-level surface.
//
// Conventions:
// - All fallible calls return NULL (or a negative/0 sentinel) and record the
//   error in a global slot readable via `motedb_last_error()`.
// - Strings crossing the boundary are UTF-8, owned by the caller after
//   return and released with `motedb_free_string`.
// - Params travel as a JSON array; results as a JSON object:
//     params:  [1, "s", true, null, [0.1,0.2], {"$blob": "base64"}]
//     result:  {"columns":[..],"rows":[[..],..],"affectedRows":n}
//   Value mapping: VECTOR ↔ JSON array of numbers, BLOB ↔ {"$blob": b64},
//   TIMESTAMP ↔ integer µs.
// ============================================================================

use std::sync::Mutex;

static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

fn set_err(msg: String) {
    if let Ok(mut slot) = LAST_ERROR.lock() {
        *slot = Some(msg);
    }
}

fn take_err() -> String {
    LAST_ERROR
        .lock()
        .ok()
        .and_then(|mut s| s.take())
        .unwrap_or_else(|| "unknown error".to_string())
}

/// Handle type for the v0.15 ABI (high-level Database wrapper).
pub struct DbHandle {
    db: crate::api::Database,
}

/// Last error message (empty string when none). The returned pointer stays
/// valid until the next fallible call on any thread.
///
/// # Safety
/// - The returned pointer must be released with `motedb_db_free_string`
///   (or ignored); it must not outlive the next fallible FFI call.
#[no_mangle]
pub unsafe extern "C" fn motedb_last_error() -> *const c_char {
    let msg = take_err();
    match CString::new(msg) {
        Ok(c) => c.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Open (or create when missing) a database. Returns NULL on error.
///
/// # Safety
/// - `path` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_open(path: *const c_char) -> *mut DbHandle {
    if path.is_null() {
        set_err("path is NULL".into());
        return ptr::null_mut();
    }
    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path) = path.to_str() else {
        set_err("path is not UTF-8".into());
        return ptr::null_mut();
    };
    let opened = if std::path::Path::new(path).exists() {
        crate::api::Database::open(path)
    } else {
        crate::api::Database::create(path)
    };
    match opened {
        Ok(db) => Box::into_raw(Box::new(DbHandle { db })),
        Err(e) => {
            set_err(format!("{e}"));
            ptr::null_mut()
        }
    }
}

/// Close and free the handle (flush + clean-close marker).
///
/// # Safety
/// - `handle` must come from `motedb_db_open` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_close(handle: *mut DbHandle) {
    if !handle.is_null() {
        let h = unsafe { Box::from_raw(handle) };
        let _ = h.db.close();
    }
}

/// Flush pending writes + truncate the WAL. Returns 0 on success, -1 on error.
///
/// # Safety
/// - `handle` must be valid.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_checkpoint(handle: *mut DbHandle) -> i32 {
    if handle.is_null() {
        set_err("handle is NULL".into());
        return -1;
    }
    let h = unsafe { &*handle };
    match h.db.checkpoint() {
        Ok(()) => 0,
        Err(e) => {
            set_err(format!("{e}"));
            -1
        }
    }
}

/// Online snapshot into `dest` (must be outside the DB dir). 0 / -1.
///
/// # Safety
/// - both pointers must be valid C strings / handle.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_backup(handle: *mut DbHandle, dest: *const c_char) -> i32 {
    if handle.is_null() || dest.is_null() {
        set_err("handle or dest is NULL".into());
        return -1;
    }
    let h = unsafe { &*handle };
    let dest = unsafe { CStr::from_ptr(dest) };
    let Ok(dest) = dest.to_str() else {
        set_err("dest is not UTF-8".into());
        return -1;
    };
    match h.db.backup_to(dest) {
        Ok(()) => 0,
        Err(e) => {
            set_err(format!("{e}"));
            -1
        }
    }
}

fn json_to_value(v: &serde_json::Value) -> Result<MValue2, String> {
    use crate::types::{ArcBytes, ArcVec, Value as V2};
    match v {
        serde_json::Value::Null => Ok(V2::Null),
        serde_json::Value::Bool(b) => Ok(V2::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(V2::Integer(i))
            } else {
                Ok(V2::Float(n.as_f64().unwrap_or(0.0)))
            }
        }
        serde_json::Value::String(s) => Ok(V2::Text(crate::types::ArcString(
            std::sync::Arc::from(s.as_str()),
        ))),
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                let f = it
                    .as_f64()
                    .ok_or_else(|| "vector elements must be numbers".to_string())?;
                out.push(f as f32);
            }
            Ok(V2::Vector(ArcVec::new(out)))
        }
        serde_json::Value::Object(m) => {
            if let Some(serde_json::Value::String(b64)) = m.get("$blob") {
                use base64::Engine as _;
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(b64.as_bytes())
                    .map_err(|e| format!("$blob base64: {e}"))?;
                return Ok(V2::Blob(ArcBytes::new(raw)));
            }
            Err("unsupported param object (only {\"$blob\": base64})".to_string())
        }
    }
}

fn value_to_json(v: &MValue2) -> serde_json::Value {
    use crate::types::Value as V2;
    match v {
        V2::Null => serde_json::Value::Null,
        V2::Integer(i) => serde_json::Value::Number((*i).into()),
        V2::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        V2::Bool(b) => serde_json::Value::Bool(*b),
        V2::Text(t) => serde_json::Value::String(t.as_str().to_string()),
        V2::Timestamp(t) => serde_json::Value::Number(t.as_micros().into()),
        V2::Vector(vec) => {
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
        // Symmetric with the param form: blobs cross the JSON boundary as
        // {"$blob": base64} so Go (and any consumer) can distinguish them
        // from TEXT unambiguously.
        V2::Blob(b) => {
            use base64::Engine as _;
            let mut m = serde_json::Map::new();
            m.insert(
                "$blob".into(),
                serde_json::Value::String(
                    base64::engine::general_purpose::STANDARD.encode(b.as_slice()),
                ),
            );
            serde_json::Value::Object(m)
        }
        V2::Spatial(g) => {
            let mut m = serde_json::Map::new();
            match &**g {
                crate::types::Geometry::Point(p) => {
                    m.insert("type".into(), "Point".into());
                    m.insert("x".into(), serde_json::json!(p.x));
                    m.insert("y".into(), serde_json::json!(p.y));
                }
                crate::types::Geometry::Point3D(p) => {
                    m.insert("type".into(), "Point3D".into());
                    m.insert("x".into(), serde_json::json!(p.x));
                    m.insert("y".into(), serde_json::json!(p.y));
                    m.insert("z".into(), serde_json::json!(p.z));
                }
                crate::types::Geometry::LineString(pts) => {
                    m.insert("type".into(), "LineString".into());
                    let list: Vec<serde_json::Value> =
                        pts.iter().map(|p| serde_json::json!([p.x, p.y])).collect();
                    m.insert("points".into(), serde_json::Value::Array(list));
                }
                crate::types::Geometry::Polygon(pts) => {
                    m.insert("type".into(), "Polygon".into());
                    let list: Vec<serde_json::Value> =
                        pts.iter().map(|p| serde_json::json!([p.x, p.y])).collect();
                    m.insert("points".into(), serde_json::Value::Array(list));
                }
            }
            serde_json::Value::Object(m)
        }
        _ => serde_json::Value::Null,
    }
}

type MValue2 = crate::types::Value;

/// Unified execute+query: runs `sql` with the optional JSON params array and
/// returns a JSON result string:
///   {"columns":[..],"rows":[[v,...],..],"affectedRows":n}
/// NULL return on error (see `motedb_last_error`). Free with
/// `motedb_free_string`.
///
/// # Safety
/// - all pointers must be valid C strings / handle.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_exec(
    handle: *mut DbHandle,
    sql: *const c_char,
    params_json: *const c_char,
) -> *mut c_char {
    if handle.is_null() || sql.is_null() {
        set_err("handle or sql is NULL".into());
        return ptr::null_mut();
    }
    let h = unsafe { &*handle };
    let sql = unsafe { CStr::from_ptr(sql) };
    let Ok(sql) = sql.to_str() else {
        set_err("sql is not UTF-8".into());
        return ptr::null_mut();
    };
    let params: Vec<MValue2> = if params_json.is_null() {
        Vec::new()
    } else {
        let pj = unsafe { CStr::from_ptr(params_json) };
        let Ok(pj) = pj.to_str() else {
            set_err("params_json is not UTF-8".into());
            return ptr::null_mut();
        };
        let parsed: Vec<serde_json::Value> = match serde_json::from_str(pj) {
            Ok(v) => v,
            Err(e) => {
                set_err(format!("params JSON: {e}"));
                return ptr::null_mut();
            }
        };
        let mut out = Vec::with_capacity(parsed.len());
        for (i, p) in parsed.iter().enumerate() {
            match json_to_value(p) {
                Ok(v) => out.push(v),
                Err(e) => {
                    set_err(format!("param {i}: {e}"));
                    return ptr::null_mut();
                }
            }
        }
        out
    };

    let run = || -> Result<(Vec<String>, Vec<Vec<MValue2>>, usize), String> {
        let streaming = if params.is_empty() {
            h.db.execute(sql).map_err(|e| e.to_string())?
        } else {
            h.db.execute_prepared(sql, params)
                .map_err(|e| e.to_string())?
        };
        let materialized = streaming.materialize().map_err(|e| e.to_string())?;
        match materialized {
            crate::sql::QueryResult::Select { columns, rows } => {
                let n = rows.len();
                Ok((columns, rows, n))
            }
            other => Ok((Vec::new(), Vec::new(), other.affected_rows())),
        }
    };
    match run() {
        Ok((columns, rows, affected)) => {
            let mut map = serde_json::Map::new();
            map.insert(
                "columns".into(),
                serde_json::Value::Array(
                    columns.into_iter().map(serde_json::Value::String).collect(),
                ),
            );
            let rows_json: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| serde_json::Value::Array(r.iter().map(value_to_json).collect()))
                .collect();
            map.insert("rows".into(), serde_json::Value::Array(rows_json));
            map.insert(
                "affectedRows".into(),
                serde_json::Value::Number((affected as i64).into()),
            );
            let out = serde_json::Value::Object(map).to_string();
            match CString::new(out) {
                Ok(c) => c.into_raw(),
                Err(_) => ptr::null_mut(),
            }
        }
        Err(e) => {
            set_err(e);
            ptr::null_mut()
        }
    }
}

/// Free a string returned by `motedb_db_exec` / `motedb_last_error`.
///
/// # Safety
/// - `s` must be a pointer from those functions (or NULL).
#[no_mangle]
pub unsafe extern "C" fn motedb_db_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// 🔑 P4: pack the database into a single MOTESNAP snapshot file.
/// Returns the packed entry count (>= 0) on success; -1 on failure (see
/// motedb_last_error).
///
/// # Safety
/// `handle` must come from motedb_db_open and stay open for the call;
/// `dest` must be a valid NUL-terminated UTF-8 C string.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_export(
    handle: *mut DbHandle,
    dest: *const c_char,
    compressed: i32,
) -> i32 {
    if handle.is_null() || dest.is_null() {
        set_err("handle or dest is NULL".into());
        return -1;
    }
    let h = unsafe { &*handle };
    let dest = unsafe { CStr::from_ptr(dest) };
    let Ok(dest) = dest.to_str() else {
        set_err("dest is not UTF-8".into());
        return -1;
    };
    match h
        .db
        .export_snapshot(std::path::Path::new(dest), compressed != 0)
    {
        Ok(stats) => stats.entries as i32,
        Err(e) => {
            set_err(format!("{e}"));
            -1
        }
    }
}

/// 🔑 P4: unpack a MOTESNAP snapshot into a fresh directory (then open it
/// with motedb_db_open). Returns the unpacked entry count (>= 0) on
/// success; -1 on failure.
///
/// # Safety
/// `src` and `dest` must be valid NUL-terminated UTF-8 C strings.
#[no_mangle]
pub unsafe extern "C" fn motedb_db_import(src: *const c_char, dest: *const c_char) -> i32 {
    if src.is_null() || dest.is_null() {
        set_err("src or dest is NULL".into());
        return -1;
    }
    let src = unsafe { CStr::from_ptr(src) };
    let dest = unsafe { CStr::from_ptr(dest) };
    let (Ok(src), Ok(dest)) = (src.to_str(), dest.to_str()) else {
        set_err("src or dest is not UTF-8".into());
        return -1;
    };
    match crate::database::core::MoteDB::import_snapshot(
        std::path::Path::new(src),
        std::path::Path::new(dest),
    ) {
        Ok(stats) => stats.entries as i32,
        Err(e) => {
            set_err(format!("{e}"));
            -1
        }
    }
}
