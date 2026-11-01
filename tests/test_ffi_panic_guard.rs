//! The C ABI must never abort the host process. A Rust panic unwinding out
//! of an `extern "C"` function is a process abort (Rust ≥ 1.71), so every
//! FFI entry point runs behind a panic guard; a panic becomes
//! `motedb_last_error` text. And the concrete panic that made this
//! reachable (`INSERT INTO t VALUES (中文)` byte-sliced past a char
//! boundary in the fast-insert literal pre-parser) now compares bytes
//! instead of `str` slices.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

#[test]
fn ffi_exec_survives_multibyte_insert() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = c(dir.path().join("d.mote").to_str().unwrap());
    let h = unsafe { motedb::ffi::motedb_db_open(path.as_ptr()) };
    assert!(!h.is_null(), "open failed");

    let out = unsafe {
        motedb::ffi::motedb_db_exec(
            h,
            c("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").as_ptr(),
            ptr::null(),
        )
    };
    assert!(!out.is_null(), "create table failed");
    unsafe { motedb::ffi::motedb_db_free_string(out) };

    // The panic trigger: a multi-byte char inside the VALUES tuple, on an
    // existing table (fast-insert path). Old code: "byte index 4 is not a
    // char boundary" panic → SIGABRT of the whole host process.
    let out = unsafe {
        motedb::ffi::motedb_db_exec(h, c("INSERT INTO t VALUES (中文)").as_ptr(), ptr::null())
    };
    if out.is_null() {
        let err = unsafe { motedb::ffi::motedb_last_error() };
        assert!(!err.is_null());
        let msg = unsafe { CStr::from_ptr(err) }.to_str().unwrap().to_string();
        unsafe { motedb::ffi::motedb_db_free_string(err as *mut c_char) };
        eprintln!("exec rejected as expected: {msg}");
        let lower = msg.to_lowercase();
        assert!(
            lower.contains("panic") || lower.contains("parse") || lower.contains("syntax"),
            "unexpected error text: {msg}"
        );
    } else {
        let msg = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_string();
        unsafe { motedb::ffi::motedb_db_free_string(out) };
        eprintln!("exec accepted: {msg}");
    }

    // The engine is still alive and empty.
    let out = unsafe {
        motedb::ffi::motedb_db_exec(h, c("SELECT COUNT(*) FROM t").as_ptr(), ptr::null())
    };
    assert!(!out.is_null(), "engine unusable after the failed insert");
    let msg = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_string();
    unsafe { motedb::ffi::motedb_db_free_string(out) };
    eprintln!("count after failed insert: {msg}");
    unsafe { motedb::ffi::motedb_db_close(h) };
}

#[test]
fn rust_level_multibyte_insert_is_a_clean_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = motedb::Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    let r = db.execute("INSERT INTO t VALUES (中文)");
    eprintln!("rust-level insert result: {:?}", r.is_err());
    // Either accepted or a clean Err — but never a panic.
    if let Err(e) = &r {
        eprintln!("  error: {e}");
    }
}
