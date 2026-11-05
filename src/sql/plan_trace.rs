//! 🔖 B4（0.18）: EXPLAIN 决策点实录——执行器在**真实决策点**写
//! thread-local plan trace，EXPLAIN 开启采集后重放执行并原样输出。
//!
//! 设计转向（相对 v1 启发式模型）: 计划永远与实际一致、无漂移——
//! 不再维护第二套计划模型。单表快路径的无执行成本描述保留为
//! `fallback`（采集不可用时）。
//!
//! 用法: 决策点一行 `crate::sql::plan_trace::record("scan", "...")`;
//! 未开启采集时为两个原子检查 + 空返回（近零开销）。

use std::cell::RefCell;
use std::fmt::Write as _;

thread_local! {
    /// `Some(buf)` = 采集中（EXPLAIN 重放）; `None` = 常规执行（no-op）。
    static COLLECTOR: RefCell<Option<Vec<(String, String)>>> = const { RefCell::new(None) };
}

/// 开启采集并执行 `f`，返回（trace, f 的结果）。嵌套调用（子查询重放）
/// 里层共享同一 collector——一次 EXPLAIN 输出完整树。
pub fn with_collection<T>(f: impl FnOnce() -> T) -> (Vec<(String, String)>, T) {
    let nested = COLLECTOR.with(|c| c.borrow().is_some());
    if nested {
        // 嵌套: 已在采集中则直接执行（追加到同一缓冲）。
        return (Vec::new(), f());
    }
    COLLECTOR.with(|c| *c.borrow_mut() = Some(Vec::new()));
    let out = f();
    let trace = COLLECTOR
        .with(|c| c.borrow_mut().take())
        .unwrap_or_default();
    (trace, out)
}

/// 决策点实录。采集关闭时近零开销（一次 thread-local 读 + None 检查）。
#[inline]
pub fn record(op: &str, detail: impl FnOnce() -> String) {
    COLLECTOR.with(|c| {
        let mut guard = c.borrow_mut();
        if let Some(buf) = guard.as_mut() {
            buf.push((op.to_string(), detail()));
        }
    });
}

/// 便捷: 格式化版（采集关闭时不构造字符串）。
#[macro_export]
macro_rules! trace_plan {
    ($op:expr, $($arg:tt)*) => {
        $crate::sql::plan_trace::record($op, || format!($($arg)*))
    };
    ($op:expr) => {
        $crate::sql::plan_trace::record($op, || String::new())
    };
}

/// 渲染为 EXPLAIN 行（step | operator | detail | source）。
pub fn render(trace: Vec<(String, String)>) -> Vec<Vec<crate::types::Value>> {
    trace
        .into_iter()
        .enumerate()
        .map(|(i, (op, detail))| {
            vec![
                crate::types::Value::Integer(i as i64 + 1),
                crate::types::Value::text_from(&op),
                crate::types::Value::text_from(&detail),
                crate::types::Value::text_from("live"),
            ]
        })
        .collect()
}

/// 供单元测试使用。
pub fn is_collecting() -> bool {
    COLLECTOR.with(|c| c.borrow().is_some())
}

#[allow(dead_code)]
fn _fmt_guard() {
    // 保证 Write trait 引用不因重构消失。
    let mut s = String::new();
    let _ = write!(s, "{}", 1);
}
