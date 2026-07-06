pub mod ast;
pub mod evaluator;
pub mod executor;
pub mod lexer;
pub mod optimizer;
pub mod parser;
pub mod row_converter;
/// MoteDB Lightweight SQL Engine
///
/// A zero-dependency, high-performance SQL engine designed for embedded use.
///
/// Architecture:
/// - Lexer: Tokenizes SQL strings
/// - Parser: Builds AST from tokens
/// - Executor: Executes queries using storage engine
/// - Optimizer: Query optimization (future)
pub mod token;

pub use ast::{BinaryOperator, CreateTableStmt, Expr, InsertStmt, SelectStmt, Statement};
pub use evaluator::ExprEvaluator;
pub use executor::{
    ForEachResult, QueryExecutor, QueryResult, StreamingControl, StreamingQueryResult,
};
pub use lexer::Lexer;
pub use optimizer::{IndexStats, QueryOptimizer, QueryPlan, ScanMethod};
pub use parser::Parser;
pub use row_converter::{row_to_sql_row, rows_to_sql_rows, sql_row_to_row};
pub use token::{Token, TokenType};

// ✅ 移除了传统的 execute_sql()，改用流式 API
// 所有查询现在都使用零内存开销的流式执行
