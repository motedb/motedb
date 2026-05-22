#!/usr/bin/env rust
//! MoteDB 命令行工具
//! 
//! 用法:
//!   motedb-cli                    - 启动交互式 SQL shell
//!   motedb-cli <db_path>          - 打开指定数据库
//!   motedb-cli exec "SQL"         - 执行单条 SQL 语句
//!   motedb-cli --version          - 显示版本信息
//!   motedb-cli --help             - 显示帮助信息

use motedb::*;
use motedb::sql::{Lexer, Parser, QueryExecutor};  // ✅ 使用流式 API
use std::env;
use std::io::{self, Write, BufRead};
use std::sync::Arc;
use std::path::PathBuf;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() {
    if let Err(e) = run() {
        eprintln!("❌ Error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    
    match args.len() {
        1 => {
            // 交互式模式 (默认数据库)
            interactive_mode(None)?;
        }
        2 => {
            match args[1].as_str() {
                "--version" | "-v" => {
                    println!("MoteDB v{}", VERSION);
                    println!("高性能嵌入式数据库引擎");
                }
                "--help" | "-h" => {
                    print_help();
                }
                path => {
                    // 打开指定数据库
                    interactive_mode(Some(PathBuf::from(path)))?;
                }
            }
        }
        3 => {
            if args[1] == "exec" {
                // 执行单条 SQL
                execute_single_sql(None, &args[2])?;
            } else {
                print_help();
                return Err(StorageError::InvalidData("Invalid arguments".to_string()));
            }
        }
        4 => {
            if args[1] == "exec" {
                // motedb-cli exec <db_path> "SQL"
                execute_single_sql(Some(PathBuf::from(&args[2])), &args[3])?;
            } else {
                print_help();
                return Err(StorageError::InvalidData("Invalid arguments".to_string()));
            }
        }
        _ => {
            print_help();
            return Err(StorageError::InvalidData("Too many arguments".to_string()));
        }
    }
    
    Ok(())
}

fn print_help() {
    println!(r#"
MoteDB v{} - 高性能嵌入式数据库引擎

用法:
  motedb-cli                    启动交互式 SQL shell (默认数据库: ./motedb_data)
  motedb-cli <db_path>          打开指定数据库
  motedb-cli exec "SQL"         执行单条 SQL 语句 (使用默认数据库)
  motedb-cli exec <db_path> "SQL"  执行单条 SQL 语句 (指定数据库)
  motedb-cli --version          显示版本信息
  motedb-cli --help             显示此帮助信息

示例:
  # 启动交互式 shell
  motedb-cli

  # 打开指定数据库
  motedb-cli /path/to/database

  # 执行单条 SQL
  motedb-cli exec "CREATE TABLE users (id INTEGER, name TEXT)"
  motedb-cli exec mydb "SELECT * FROM users"

支持的索引类型:
  • B-Tree 索引 (主键)
  • LSM-Tree 索引 (高吞吐写入)
  • 全文搜索索引 (FTS)
  • 向量索引 (DiskANN)
  • 空间索引 (R-Tree)
  • 时间序列索引 (Timestamp)
  • 列值索引 (Column Value)

更多信息: https://github.com/yourusername/motedb
"#, VERSION);
}

fn interactive_mode(db_path: Option<PathBuf>) -> Result<()> {
    let path = db_path.unwrap_or_else(|| PathBuf::from("./motedb_data"));
    
    println!("🚀 MoteDB v{}", VERSION);
    println!("📂 Database: {}", path.display());
    println!("💡 Type '.help' for help, '.exit' to quit\n");
    
    // 打开或创建数据库
    let db = Arc::new(if path.exists() {
        MoteDB::open(&path)?
    } else {
        println!("✨ Creating new database...\n");
        MoteDB::create(&path)?
    });
    
    let stdin = io::stdin();
    let mut buffer = String::new();
    let mut multiline_sql = String::new();
    
    loop {
        // 显示提示符
        if multiline_sql.is_empty() {
            print!("motedb> ");
        } else {
            print!("     -> ");
        }
        io::stdout().flush().unwrap();
        
        // 读取输入
        buffer.clear();
        if stdin.lock().read_line(&mut buffer).is_err() {
            break;
        }
        
        let input = buffer.trim();
        
        // 特殊命令
        if input.starts_with('.') {
            if !multiline_sql.is_empty() {
                eprintln!("⚠️  Warning: Incomplete SQL statement discarded");
                multiline_sql.clear();
            }
            
            match input {
                ".exit" | ".quit" => {
                    println!("👋 Goodbye!");
                    break;
                }
                ".help" => {
                    print_interactive_help();
                }
                ".tables" => {
                    list_tables(&db)?;
                }
                ".schema" => {
                    show_all_schemas(&db)?;
                }
                cmd if cmd.starts_with(".schema ") => {
                    let table = &cmd[8..];
                    show_table_schema(&db, table)?;
                }
                _ => {
                    eprintln!("❌ Unknown command: {}", input);
                    println!("💡 Type '.help' for available commands");
                }
            }
            continue;
        }
        
        // 跳过空行
        if input.is_empty() {
            continue;
        }
        
        // 累积多行 SQL
        multiline_sql.push_str(input);
        multiline_sql.push(' ');
        
        // 检查是否是完整的 SQL 语句 (以分号结尾)
        if input.ends_with(';') {
            // 执行 SQL
            let sql = multiline_sql.trim_end_matches(';').trim();
            
            // ✅ 使用流式 API 并物化
            let result = (|| -> Result<_> {
                let mut lexer = Lexer::new(sql);
                let tokens = lexer.tokenize()?;
                let mut parser = Parser::new(tokens);
                let statement = parser.parse()?;
                let executor = QueryExecutor::new(db.clone());
                let streaming_result = executor.execute_streaming(statement)?;
                streaming_result.materialize()
            })();
            
            match result {
                Ok(result) => {
                    display_result(result);
                }
                Err(e) => {
                    eprintln!("❌ Error: {}", e);
                }
            }
            
            multiline_sql.clear();
        }
    }
    
    Ok(())
}

fn execute_single_sql(db_path: Option<PathBuf>, sql: &str) -> Result<()> {
    let path = db_path.unwrap_or_else(|| PathBuf::from("./motedb_data"));
    
    // 打开或创建数据库
    let db = Arc::new(if path.exists() {
        MoteDB::open(&path)?
    } else {
        MoteDB::create(&path)?
    });
    
    // ✅ 使用流式 API 并物化
    let mut lexer = Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let statement = parser.parse()?;
    let executor = QueryExecutor::new(db);
    let streaming_result = executor.execute_streaming(statement)?;
    let result = streaming_result.materialize()?;
    
    display_result(result);
    
    Ok(())
}

fn display_result(result: sql::QueryResult) {
    use sql::QueryResult;
    
    match result {
        QueryResult::Definition { message } => {
            println!("✅ {}", message);
        }
        QueryResult::Modification { affected_rows } => {
            println!("✅ {} row(s) affected", affected_rows);
        }
        QueryResult::Select { columns, rows } => {
            display_table(&columns, &rows);
        }
    }
}

fn display_table(columns: &[String], rows: &[Vec<types::Value>]) {
    use types::Value;
    
    if rows.is_empty() {
        println!("📊 No results");
        return;
    }
    
    // 计算列宽
    let mut widths: Vec<usize> = columns.iter()
        .map(|col| col.len())
        .collect();
    
    for row in rows {
        for (i, value) in row.iter().enumerate() {
            let len = match value {
                Value::Null => 4,
                Value::Integer(n) => n.to_string().len(),
                Value::Float(f) => format!("{:.2}", f).len(),
                Value::Text(s) => s.len().min(50),
                Value::Bool(b) => b.to_string().len(),
                Value::Vector(_) => 12,
                Value::Tensor(_) => 12,
                Value::Spatial(_) => 12,
                Value::TextDoc(_) => 12,
                Value::Timestamp(_) => 19,  // "YYYY-MM-DD HH:MM:SS" format
            };
            if i < widths.len() {
                widths[i] = widths[i].max(len);
            }
        }
    }
    
    // 打印表头
    print!("┌");
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < widths.len() - 1 {
            print!("┬");
        }
    }
    println!("┐");
    
    print!("│");
    for (i, col) in columns.iter().enumerate() {
        let width = widths.get(i).unwrap_or(&10);
        print!(" {:width$} ", col, width = width);
        if i < columns.len() - 1 {
            print!("│");
        }
    }
    println!("│");
    
    print!("├");
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < widths.len() - 1 {
            print!("┼");
        }
    }
    println!("┤");
    
    // 打印数据行
    for row in rows {
        print!("│");
        for (i, value) in row.iter().enumerate() {
            let width = widths.get(i).unwrap_or(&10);
            let s = match value {
                Value::Null => "NULL".to_string(),
                Value::Integer(n) => n.to_string(),
                Value::Float(f) => format!("{:.2}", f),
                Value::Text(s) => {
                    if s.len() > 50 {
                        format!("{}...", &s[..47])
                    } else {
                        (**s).clone()
                    }
                }
                Value::Bool(b) => b.to_string(),
                Value::Vector(_) => "<vector>".to_string(),
                Value::Tensor(_) => "<tensor>".to_string(),
                Value::Spatial(_) => "<geometry>".to_string(),
                Value::TextDoc(_) => "<textdoc>".to_string(),
                Value::Timestamp(ts) => {
                    // Format timestamp as microseconds
                    format!("{} μs", ts.as_micros())
                },
            };
            print!(" {:width$} ", s, width = width);
            if i < row.len() - 1 {
                print!("│");
            }
        }
        println!("│");
    }
    
    print!("└");
    for (i, width) in widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < widths.len() - 1 {
            print!("┴");
        }
    }
    println!("┘");
    
    println!("\n📊 {} row(s) returned", rows.len());
}

fn print_interactive_help() {
    println!(r#"
特殊命令:
  .help              显示此帮助
  .exit, .quit       退出程序
  .tables            列出所有表
  .schema            显示所有表的结构
  .schema <table>    显示指定表的结构

SQL 示例:
  CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
  CREATE INDEX idx_email ON users(email) USING COLUMN_VALUE;
  INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
  SELECT * FROM users WHERE email = 'alice@example.com';
  UPDATE users SET name = 'Bob' WHERE id = 1;
  DELETE FROM users WHERE id = 1;

支持的索引类型:
  COLUMN_VALUE    列值索引 (快速等值查询)
  FTS             全文搜索索引
  VECTOR          向量索引 (相似度搜索)
  SPATIAL         空间索引 (地理位置)
  TIMESTAMP       时间序列索引
"#);
}

fn list_tables(db: &MoteDB) -> Result<()> {
    let tables = db.list_tables()?;
    
    if tables.is_empty() {
        println!("📊 No tables found");
    } else {
        println!("📋 Tables:");
        for table in tables {
            println!("  • {}", table);
        }
    }
    
    Ok(())
}

fn show_all_schemas(db: &MoteDB) -> Result<()> {
    let tables = db.list_tables()?;
    
    if tables.is_empty() {
        println!("📊 No tables found");
        return Ok(());
    }
    
    for table in tables {
        show_table_schema(db, &table)?;
        println!();
    }
    
    Ok(())
}

fn show_table_schema(db: &MoteDB, table_name: &str) -> Result<()> {
    let schema = db.get_table_schema(table_name)?;
    
    println!("📋 Table: {}", table_name);
    println!("┌─────────────────┬──────────────┬──────────┐");
    println!("│ Column          │ Type         │ Nullable │");
    println!("├─────────────────┼──────────────┼──────────┤");
    
    for col in &schema.columns {
        let type_str = format!("{:?}", col.col_type);
        let nullable = if col.nullable { "YES" } else { "NO" };
        println!("│ {:15} │ {:12} │ {:8} │", col.name, type_str, nullable);
    }
    
    println!("└─────────────────┴──────────────┴──────────┘");
    
    Ok(())
}
