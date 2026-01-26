/// Abstract Syntax Tree for SQL statements
use crate::types::Value;

/// Top-level SQL statement
#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    DropTable(DropTableStmt),
    DropIndex(DropIndexStmt),
    ShowTables,
    DescribeTable(String),  // table name
}

/// SELECT statement
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub distinct: bool,                    // SELECT DISTINCT
    pub columns: Vec<SelectColumn>,
    pub from: TableRef,                    // Changed from String to support JOINs
    pub where_clause: Option<Expr>,
    pub group_by: Option<Vec<String>>,     // GROUP BY column_list
    pub having: Option<Expr>,              // HAVING condition
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub latest_by: Option<Vec<String>>,    // LATEST BY column_list
}

/// Table reference in FROM clause (supports JOINs and subqueries)
#[derive(Debug, Clone)]
pub enum TableRef {
    /// Single table: table_name [AS alias]
    Table { name: String, alias: Option<String> },
    /// JOIN: left_table JOIN_TYPE right_table ON condition
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        join_type: JoinType,
        on_condition: Expr,
    },
    /// Subquery in FROM: (SELECT ...) AS alias
    /// 
    /// Example: FROM (SELECT id, name FROM users WHERE age > 18) AS adults
    Subquery {
        query: Box<SelectStmt>,
        alias: String,  // Alias is required for subqueries in FROM
    },
}

/// JOIN types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    Star,                          // *
    Column(String),                // column_name
    ColumnWithAlias(String, String), // column_name AS alias
    Expr(Expr, Option<String>),    // expression [AS alias]
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,  // true = ASC, false = DESC
}

/// INSERT statement
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,  // None means all columns
    pub values: Vec<Vec<Expr>>,        // Multiple rows
}

/// UPDATE statement
#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,  // column = expr
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Expr>,
}

/// CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
    Timestamp,
    Vector(Option<usize>),    // Vector dimension
    Geometry,
}

/// CREATE INDEX statement
#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table: String,
    pub column: String,
    pub index_type: IndexType,
}

#[derive(Debug, Clone)]
pub enum IndexType {
    BTree,
    Column,      // 🆕 Column value index (same as BTree but explicit name)
    Text,
    Vector,
    Spatial,
    Timestamp,
}

/// DROP TABLE statement
#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub table: String,
}

/// DROP INDEX statement
#[derive(Debug, Clone)]
pub struct DropIndexStmt {
    pub index_name: String,
}

/// Expression
#[derive(Debug, Clone)]
pub enum Expr {
    /// Column reference
    Column(String),
    
    /// Literal value
    Literal(Value),
    
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    
    /// Function call
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,  // For COUNT(DISTINCT column)
    },
    
    /// 🆕 Window function call
    /// 
    /// Syntax: function_name(args) OVER ([PARTITION BY ...] [ORDER BY ...])
    /// 
    /// Examples:
    /// - ROW_NUMBER() OVER (ORDER BY id)
    /// - RANK() OVER (PARTITION BY category ORDER BY score DESC)
    /// - LAG(price, 1) OVER (PARTITION BY product_id ORDER BY date)
    WindowFunction {
        func: WindowFunc,
        partition_by: Option<Vec<String>>,  // PARTITION BY columns
        order_by: Option<Vec<OrderByExpr>>, // ORDER BY in window
    },
    
    /// IN expression: column IN (val1, val2, ...) or column IN (SELECT ...)
    /// 
    /// Examples:
    /// - WHERE id IN (1, 2, 3)  -> list contains literal expressions
    /// - WHERE id IN (SELECT user_id FROM orders)  -> list contains a single Subquery expression
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,  // Either multiple literals OR a single Subquery
        negated: bool,
    },
    
    /// BETWEEN expression: column BETWEEN low AND high
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    
    /// LIKE expression: column LIKE pattern
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    
    /// IS NULL expression
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    
    /// Subquery expression
    /// 
    /// Used in multiple contexts:
    /// - WHERE x IN (SELECT ...)
    /// - WHERE x = (SELECT ...)  (scalar subquery)
    /// - SELECT (SELECT ...) AS col (scalar subquery in projection)
    Subquery(Box<SelectStmt>),
    
    /// MATCH...AGAINST full-text search
    /// 
    /// Syntax: MATCH(column) AGAINST(query_string)
    /// Returns: BM25 relevance score (Float)
    /// 
    /// Examples:
    /// - WHERE MATCH(content) AGAINST('rust database')
    /// - ORDER BY MATCH(content) AGAINST('search query') DESC
    /// - SELECT MATCH(content) AGAINST('keyword') AS score
    Match {
        column: String,
        query: String,
    },
    
    /// KNN_SEARCH vector similarity search
    /// 
    /// Syntax: KNN_SEARCH(vector_column, query_vector, k)
    /// Returns: Bool (true if in top-k results)
    /// 
    /// Examples:
    /// - WHERE KNN_SEARCH(embedding, [0.1, 0.2], 10)
    /// - Used with KNN_DISTANCE() for scoring
    KnnSearch {
        column: String,
        query_vector: Vec<f32>,
        k: usize,
    },
    
    /// KNN_DISTANCE vector distance function
    /// 
    /// Syntax: KNN_DISTANCE(vector_column, query_vector)
    /// Returns: Float (distance/similarity score)
    /// 
    /// Examples:
    /// - SELECT KNN_DISTANCE(embedding, [0.1, 0.2]) AS distance
    /// - ORDER BY KNN_DISTANCE(embedding, [0.1, 0.2])
    KnnDistance {
        column: String,
        query_vector: Vec<f32>,
    },
    
    /// ST_WITHIN spatial range query
    /// 
    /// Syntax: ST_WITHIN(point_column, min_x, min_y, max_x, max_y)
    /// Returns: Bool (true if point is within bounding box)
    /// 
    /// Examples:
    /// - WHERE ST_WITHIN(location, 0, 0, 100, 100)
    /// - Used for spatial filtering with spatial index
    StWithin {
        column: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    },
    
    /// ST_DISTANCE spatial distance function
    /// 
    /// Syntax: ST_DISTANCE(point_column, x, y)
    /// Returns: Float (Euclidean distance)
    /// 
    /// Examples:
    /// - SELECT ST_DISTANCE(location, 50, 50) AS distance
    /// - ORDER BY ST_DISTANCE(location, 50, 50)
    StDistance {
        column: String,
        x: f64,
        y: f64,
    },
    
    /// ST_KNN spatial k-nearest neighbors
    /// 
    /// Syntax: ST_KNN(point_column, x, y, k)
    /// Returns: Bool (true if in top-k nearest neighbors)
    /// 
    /// Examples:
    /// - WHERE ST_KNN(location, 50, 50, 10)
    /// - Used with ST_DISTANCE for scoring
    StKnn {
        column: String,
        x: f64,
        y: f64,
        k: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Comparison
    Eq,   // =
    Ne,   // !=
    Lt,   // <
    Gt,   // >
    Le,   // <=
    Ge,   // >=
    
    // Logical
    And,
    Or,
    
    // Arithmetic
    Add,  // +
    Sub,  // -
    Mul,  // *
    Div,  // /
    Mod,  // %
    
    // E-SQL Vector Distance Operators
    L2Distance,      // <-> (Euclidean distance)
    CosineDistance,  // <=> (Cosine distance)
    DotProduct,      // <#> (Inner product)
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}

impl BinaryOperator {
    /// Get operator precedence (higher = tighter binding)
    pub fn precedence(&self) -> u8 {
        match self {
            BinaryOperator::Or => 1,
            BinaryOperator::And => 2,
            BinaryOperator::Eq | BinaryOperator::Ne |
            BinaryOperator::Lt | BinaryOperator::Gt |
            BinaryOperator::Le | BinaryOperator::Ge => 3,
            // Vector distance operators have same precedence as comparison
            BinaryOperator::L2Distance | BinaryOperator::CosineDistance | BinaryOperator::DotProduct => 3,
            BinaryOperator::Add | BinaryOperator::Sub => 4,
            BinaryOperator::Mul | BinaryOperator::Div | BinaryOperator::Mod => 5,
        }
    }
}

/// 🆕 Window function types
#[derive(Debug, Clone)]
pub enum WindowFunc {
    /// ROW_NUMBER() - sequential number of row within partition
    RowNumber,
    /// RANK() - rank with gaps for ties
    Rank,
    /// DENSE_RANK() - rank without gaps for ties
    DenseRank,
    /// LAG(expr, offset, default) - value from previous row
    Lag {
        expr: Box<Expr>,
        offset: Option<usize>,  // Default: 1
        default: Option<Box<Expr>>,  // Default: NULL
    },
    /// LEAD(expr, offset, default) - value from next row
    Lead {
        expr: Box<Expr>,
        offset: Option<usize>,  // Default: 1
        default: Option<Box<Expr>>,  // Default: NULL
    },
}
