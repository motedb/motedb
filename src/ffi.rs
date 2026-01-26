//! FFI (Foreign Function Interface) for C/Python/Node.js
//! 
//! C ABI 导出接口，用于动态链接库

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use crate::MoteDB;

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
        Ok(db) => Box::into_raw(Box::new(MoteDBHandle {
            db: Arc::new(db),
        })),
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
