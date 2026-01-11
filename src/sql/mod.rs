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
pub use executor::{QueryExecutor, QueryResult};
pub use evaluator::ExprEvaluator;
pub use row_converter::{row_to_sql_row, sql_row_to_row, rows_to_sql_rows};
pub use optimizer::{QueryOptimizer, QueryPlan, ScanMethod, IndexStats};

use crate::error::Result;

/// Parse and execute a SQL statement
pub fn execute_sql(db: std::sync::Arc<crate::database::MoteDB>, sql: &str) -> Result<QueryResult> {
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let statement = parser.parse()?;
    let executor = QueryExecutor::new(db);
    executor.execute(statement)
}
