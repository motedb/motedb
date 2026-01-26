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
pub mod lexer;
pub mod ast;
pub mod parser;
pub mod executor;
pub mod evaluator;
pub mod row_converter;
pub mod optimizer;

pub use token::{Token, TokenType};
pub use lexer::Lexer;
pub use ast::{Statement, SelectStmt, InsertStmt, CreateTableStmt, Expr, BinaryOperator};
pub use parser::Parser;
pub use executor::{QueryExecutor, QueryResult, StreamingQueryResult};
pub use evaluator::ExprEvaluator;
pub use row_converter::{row_to_sql_row, sql_row_to_row, rows_to_sql_rows};
pub use optimizer::{QueryOptimizer, QueryPlan, ScanMethod, IndexStats};


// ✅ 移除了传统的 execute_sql()，改用流式 API
// 所有查询现在都使用零内存开销的流式执行
