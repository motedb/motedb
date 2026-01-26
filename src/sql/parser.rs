/// SQL Parser - converts tokens into AST
use super::token::{Token, TokenType};
use super::ast::*;
use crate::error::{Result, MoteDBError};
use crate::types::Value;

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, position: 0 }
    }
    
    /// Parse a SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = match &self.current().token_type {
            TokenType::Select => Statement::Select(self.parse_select()?),
            TokenType::Insert => Statement::Insert(self.parse_insert()?),
            TokenType::Update => Statement::Update(self.parse_update()?),
            TokenType::Delete => Statement::Delete(self.parse_delete()?),
            TokenType::Create => self.parse_create()?,
            TokenType::Drop => self.parse_drop()?,
            TokenType::Show => self.parse_show()?,
            TokenType::Describe | TokenType::Desc => self.parse_describe()?,
            _ => return Err(self.error("Expected SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, SHOW, or DESCRIBE")),
        };
        
        // Optionally consume semicolon
        if matches!(self.current().token_type, TokenType::Semicolon) {
            self.advance();
        }
        
        Ok(stmt)
    }
    
    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<SelectStmt> {
        self.expect(TokenType::Select)?;
        
        // Parse DISTINCT (optional)
        let distinct = self.match_token(TokenType::Distinct);
        
        // Parse columns
        let columns = self.parse_select_columns()?;
        
        // FROM clause
        self.expect(TokenType::From)?;
        let from = self.parse_table_ref()?;
        
        // WHERE clause (optional)
        let where_clause = if self.match_token(TokenType::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        
        // GROUP BY clause (optional)
        let group_by = if self.match_token(TokenType::Group) {
            self.expect(TokenType::By)?;
            Some(self.parse_column_list()?)
        } else {
            None
        };
        
        // HAVING clause (optional, requires GROUP BY)
        let having = if self.match_token(TokenType::Having) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        
        // ORDER BY clause (optional)
        let order_by = if self.match_token(TokenType::Order) {
            self.expect(TokenType::By)?;
            Some(self.parse_order_by()?)
        } else {
            None
        };
        
        // LIMIT clause (optional)
        let limit = if self.match_token(TokenType::Limit) {
            Some(self.parse_usize()?)
        } else {
            None
        };
        
        // OFFSET clause (optional)
        let offset = if self.match_token(TokenType::Offset) {
            Some(self.parse_usize()?)
        } else {
            None
        };
        
        // LATEST BY clause (optional)
        let latest_by = if self.match_token(TokenType::Latest) {
            self.expect(TokenType::By)?;
            Some(self.parse_column_list()?)
        } else {
            None
        };
        
        Ok(SelectStmt {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            latest_by,
        })
    }
    
    fn parse_column_list(&mut self) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(columns)
    }
    
    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();
        
        loop {
            if matches!(self.current().token_type, TokenType::Star) {
                self.advance();
                columns.push(SelectColumn::Star);
            } else {
                // Try to parse as expression
                let expr = self.parse_expr(0)?;
                
                // Check for AS alias
                let alias = if self.match_token(TokenType::As) {
                    Some(self.parse_identifier()?)
                } else {
                    None
                };
                
                // If it's a simple column reference, use Column variant
                if let Expr::Column(name) = expr {
                    if let Some(alias) = alias {
                        columns.push(SelectColumn::ColumnWithAlias(name, alias));
                    } else {
                        columns.push(SelectColumn::Column(name));
                    }
                } else {
                    columns.push(SelectColumn::Expr(expr, alias));
                }
            }
            
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        
        if columns.is_empty() {
            return Err(self.error("Expected at least one column in SELECT"));
        }
        
        Ok(columns)
    }
    
    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>> {
        let mut order_by = Vec::new();
        
        loop {
            let expr = self.parse_expr(0)?;
            let asc = if self.match_token(TokenType::Desc) {
                false
            } else {
                self.match_token(TokenType::Asc); // Optional
                true
            };
            
            order_by.push(OrderByExpr { expr, asc });
            
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        
        Ok(order_by)
    }
    
    /// Parse table reference with JOIN support
    /// Syntax: table1 [AS alias1] [JOIN table2 [AS alias2] ON condition]
    fn parse_table_ref(&mut self) -> Result<TableRef> {
        // Parse left table
        let mut left = self.parse_single_table()?;
        
        // Parse JOINs (can chain multiple JOINs)
        while self.is_join_keyword() {
            let join_type = self.parse_join_type()?;
            let right = self.parse_single_table()?;
            
            // Expect ON condition
            self.expect(TokenType::On)?;
            let on_condition = self.parse_expr(0)?;
            
            left = TableRef::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                on_condition,
            };
        }
        
        Ok(left)
    }
    
    /// Parse a single table reference: table_name [AS alias] or (SELECT ...) AS alias
    fn parse_single_table(&mut self) -> Result<TableRef> {
        // Check for subquery: (SELECT ...)
        if matches!(self.current().token_type, TokenType::LParen) {
            self.advance(); // consume '('
            
            // Expect SELECT
            if !matches!(self.current().token_type, TokenType::Select) {
                return Err(self.error("Expected SELECT in subquery"));
            }
            
            let subquery = self.parse_select()?;
            self.expect(TokenType::RParen)?;
            
            // Alias is REQUIRED for subqueries in FROM
            let alias = if self.match_token(TokenType::As) {
                self.parse_identifier()?
            } else if matches!(self.current().token_type, TokenType::Identifier(_)) {
                // Allow implicit alias (without AS keyword)
                self.parse_identifier()?
            } else {
                return Err(self.error("Subquery in FROM clause must have an alias"));
            };
            
            return Ok(TableRef::Subquery {
                query: Box::new(subquery),
                alias,
            });
        }
        
        // Regular table
        let name = self.parse_identifier()?;
        
        // Check for optional AS alias
        let alias = if self.match_token(TokenType::As) {
            Some(self.parse_identifier()?)
        } else if matches!(self.current().token_type, TokenType::Identifier(_)) {
            // Allow implicit alias (without AS keyword)
            Some(self.parse_identifier()?)
        } else {
            None
        };
        
        Ok(TableRef::Table { name, alias })
    }
    
    /// Check if current token is a JOIN keyword
    fn is_join_keyword(&self) -> bool {
        matches!(
            self.current().token_type,
            TokenType::Join | TokenType::Inner | TokenType::Left | TokenType::Right
        )
    }
    
    /// Parse JOIN type
    fn parse_join_type(&mut self) -> Result<JoinType> {
        let join_type = match self.current().token_type {
            TokenType::Inner => {
                self.advance();
                self.expect(TokenType::Join)?;
                JoinType::Inner
            }
            TokenType::Left => {
                self.advance();
                self.match_token(TokenType::Outer); // OUTER is optional
                self.expect(TokenType::Join)?;
                JoinType::Left
            }
            TokenType::Right => {
                self.advance();
                self.match_token(TokenType::Outer); // OUTER is optional
                self.expect(TokenType::Join)?;
                JoinType::Right
            }
            TokenType::Join => {
                self.advance();
                JoinType::Inner // Default to INNER JOIN
            }
            _ => return Err(self.error("Expected JOIN keyword")),
        };
        
        Ok(join_type)
    }
    
    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<InsertStmt> {
        self.expect(TokenType::Insert)?;
        self.expect(TokenType::Into)?;
        
        let table = self.parse_identifier()?;
        
        // Optional column list
        let columns = if matches!(self.current().token_type, TokenType::LParen) {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect(TokenType::RParen)?;
            Some(cols)
        } else {
            None
        };
        
        self.expect(TokenType::Values)?;
        
        // Parse value rows
        let mut values = Vec::new();
        loop {
            self.expect(TokenType::LParen)?;
            let row = self.parse_expr_list()?;
            self.expect(TokenType::RParen)?;
            values.push(row);
            
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        
        Ok(InsertStmt { table, columns, values })
    }
    
    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<UpdateStmt> {
        self.expect(TokenType::Update)?;
        let table = self.parse_identifier()?;
        self.expect(TokenType::Set)?;
        
        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            let column = self.parse_identifier()?;
            self.expect(TokenType::Eq)?;
            let expr = self.parse_expr(0)?;
            assignments.push((column, expr));
            
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        
        // WHERE clause (optional)
        let where_clause = if self.match_token(TokenType::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        
        Ok(UpdateStmt { table, assignments, where_clause })
    }
    
    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<DeleteStmt> {
        self.expect(TokenType::Delete)?;
        self.expect(TokenType::From)?;
        let table = self.parse_identifier()?;
        
        // WHERE clause (optional)
        let where_clause = if self.match_token(TokenType::Where) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };
        
        Ok(DeleteStmt { table, where_clause })
    }
    
    /// Parse CREATE statement
    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(TokenType::Create)?;
        
        match &self.current().token_type {
            TokenType::Table => Ok(Statement::CreateTable(self.parse_create_table()?)),
            TokenType::Index => Ok(Statement::CreateIndex(self.parse_create_index()?)),
            TokenType::Text | TokenType::Vector | TokenType::Geometry | TokenType::Timestamp => {
                // Index type keywords: TEXT INDEX, VECTOR INDEX, etc.
                Ok(Statement::CreateIndex(self.parse_create_index()?))
            }
            TokenType::Identifier(id) => {
                // Check for SPATIAL keyword (GEOMETRY is already handled above)
                let id_upper = id.to_uppercase();
                if id_upper == "SPATIAL" {
                    Ok(Statement::CreateIndex(self.parse_create_index()?))
                } else {
                    Err(self.error("Expected TABLE or INDEX after CREATE"))
                }
            }
            _ => Err(self.error("Expected TABLE or INDEX after CREATE")),
        }
    }
    
    fn parse_create_table(&mut self) -> Result<CreateTableStmt> {
        self.expect(TokenType::Table)?;
        let table = self.parse_identifier()?;
        
        self.expect(TokenType::LParen)?;
        let columns = self.parse_column_defs()?;
        self.expect(TokenType::RParen)?;
        
        Ok(CreateTableStmt { table, columns })
    }
    
    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::new();
        
        loop {
            let name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;
            
            // Parse constraints
            let mut nullable = true;
            let mut primary_key = false;
            
            // NOT NULL
            if self.match_token(TokenType::Not) {
                self.expect(TokenType::Null)?;
                nullable = false;
            }
            
            // PRIMARY KEY
            if self.match_token(TokenType::Primary) {
                self.expect(TokenType::Key)?;
                primary_key = true;
                nullable = false;  // PRIMARY KEY implies NOT NULL
            }
            
            columns.push(ColumnDef {
                name,
                data_type,
                nullable,
                primary_key,
            });
            
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        
        Ok(columns)
    }
    
    fn parse_data_type(&mut self) -> Result<DataType> {
        let data_type = match &self.current().token_type {
            TokenType::Integer => DataType::Integer,
            TokenType::Float => DataType::Float,
            TokenType::Text => DataType::Text,
            TokenType::Boolean => DataType::Boolean,
            TokenType::Timestamp => DataType::Timestamp,
            TokenType::Geometry => DataType::Geometry,
            TokenType::Vector => {
                self.advance();
                if self.match_token(TokenType::LParen) {
                    let dim = self.parse_usize()?;
                    self.expect(TokenType::RParen)?;
                    return Ok(DataType::Vector(Some(dim)));
                } else {
                    return Ok(DataType::Vector(None));
                }
            }
            _ => return Err(self.error("Expected data type")),
        };
        
        self.advance();
        Ok(data_type)
    }
    
    fn parse_create_index(&mut self) -> Result<CreateIndexStmt> {
        // Parse optional index type: TEXT/VECTOR/SPATIAL/TIMESTAMP
        let index_type = match &self.current().token_type {
            TokenType::Text => {
                self.advance();
                IndexType::Text
            }
            TokenType::Vector => {
                self.advance();
                IndexType::Vector
            }
            TokenType::Geometry => {
                self.advance();
                IndexType::Spatial
            }
            TokenType::Timestamp => {
                self.advance();
                IndexType::Timestamp
            }
            TokenType::Identifier(ref id) => {
                // Also check for SPATIAL keyword (not a data type)
                let id_upper = id.to_uppercase();
                match id_upper.as_str() {
                    "SPATIAL" => {
                        self.advance();
                        IndexType::Spatial
                    }
                    _ => IndexType::BTree,  // Default
                }
            }
            _ => IndexType::BTree,  // Default
        };
        
        self.expect(TokenType::Index)?;
        let index_name = self.parse_identifier()?;
        self.expect(TokenType::On)?;
        let table = self.parse_identifier()?;
        self.expect(TokenType::LParen)?;
        let column = self.parse_identifier()?;
        self.expect(TokenType::RParen)?;
        
        // 🆕 Parse optional USING clause: USING COLUMN|BTREE|...
        let final_index_type = if self.current().token_type == TokenType::Using {
            self.advance(); // consume USING
            
            // Clone the identifier to avoid borrow issues
            let token_type = self.current().token_type.clone();
            
            match token_type {
                TokenType::Identifier(id) => {
                    let id_upper = id.to_uppercase();
                    self.advance();
                    match id_upper.as_str() {
                        "COLUMN" => IndexType::Column,
                        "BTREE" => IndexType::BTree,
                        "TEXT" => IndexType::Text,
                        "VECTOR" => IndexType::Vector,
                        "SPATIAL" => IndexType::Spatial,
                        "TIMESTAMP" => IndexType::Timestamp,
                        _ => return Err(MoteDBError::ParseError(
                            format!("Unknown index type: {}", id)
                        )),
                    }
                }
                TokenType::Text => {
                    self.advance();
                    IndexType::Text
                }
                TokenType::Vector => {
                    self.advance();
                    IndexType::Vector
                }
                TokenType::Timestamp => {
                    self.advance();
                    IndexType::Timestamp
                }
                _ => return Err(MoteDBError::ParseError(
                    "Expected index type after USING".to_string()
                )),
            }
        } else {
            // No USING clause, use the index_type determined at the beginning
            index_type
        };
        
        Ok(CreateIndexStmt {
            index_name,
            table,
            column,
            index_type: final_index_type,
        })
    }
    
    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(TokenType::Drop)?;
        
        match &self.current().token_type {
            TokenType::Table => {
                self.advance();
                let table = self.parse_identifier()?;
                Ok(Statement::DropTable(DropTableStmt { table }))
            }
            TokenType::Index => {
                self.advance();
                let index_name = self.parse_identifier()?;
                Ok(Statement::DropIndex(DropIndexStmt { index_name }))
            }
            _ => Err(self.error("Expected TABLE or INDEX after DROP")),
        }
    }
    
    /// Parse SHOW statement
    fn parse_show(&mut self) -> Result<Statement> {
        self.expect(TokenType::Show)?;
        
        if self.match_token(TokenType::Tables) {
            Ok(Statement::ShowTables)
        } else {
            Err(self.error("Expected TABLES after SHOW"))
        }
    }
    
    /// Parse DESCRIBE statement
    fn parse_describe(&mut self) -> Result<Statement> {
        // Accept both DESC and DESCRIBE
        if !matches!(self.current().token_type, TokenType::Describe | TokenType::Desc) {
            return Err(self.error("Expected DESCRIBE or DESC"));
        }
        self.advance();
        
        let table_name = self.parse_identifier()?;
        Ok(Statement::DescribeTable(table_name))
    }
    
    /// Parse expression using Pratt parsing (handles operator precedence elegantly)
    fn parse_expr(&mut self, min_precedence: u8) -> Result<Expr> {
        // Parse prefix (unary operators, literals, identifiers, etc.)
        let mut left = self.parse_prefix_expr()?;
        
        // Parse infix operators
        while let Some(op) = self.try_parse_binary_op() {
            let precedence = op.precedence();
            if precedence < min_precedence {
                break;
            }
            
            self.advance(); // consume operator
            let right = self.parse_expr(precedence + 1)?;
            
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        
        // Handle special postfix operators
        left = self.parse_postfix_expr(left)?;
        
        Ok(left)
    }
    
    fn parse_prefix_expr(&mut self) -> Result<Expr> {
        match &self.current().token_type {
            // Unary operators
            TokenType::Not => {
                self.advance();
                let expr = self.parse_expr(10)?; // High precedence
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                })
            }
            TokenType::Minus => {
                self.advance();
                let expr = self.parse_expr(10)?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            TokenType::Plus => {
                self.advance();
                let expr = self.parse_expr(10)?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            
            // Parenthesized expression OR subquery
            TokenType::LParen => {
                self.advance();
                
                // Check if this is a subquery (SELECT ...)
                if matches!(self.current().token_type, TokenType::Select) {
                    let subquery = self.parse_select()?;
                    self.expect(TokenType::RParen)?;
                    return Ok(Expr::Subquery(Box::new(subquery)));
                }
                
                // Otherwise, it's a regular parenthesized expression
                let expr = self.parse_expr(0)?;
                self.expect(TokenType::RParen)?;
                Ok(expr)
            }
            
            // Literals
            TokenType::Number(n) => {
                let n = *n;
                self.advance();
                // If the number has no fractional part, parse as Integer
                if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                    Ok(Expr::Literal(Value::Integer(n as i64)))
                } else {
                    Ok(Expr::Literal(Value::Float(n)))
                }
            }
            TokenType::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::Literal(Value::Text(s)))
            }
            TokenType::True => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(true)))
            }
            TokenType::False => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(false)))
            }
            TokenType::Null => {
                self.advance();
                Ok(Expr::Literal(Value::Null))
            }
            
            // ARRAY[...] literal for vectors
            TokenType::Array => {
                self.advance();
                self.expect(TokenType::LBracket)?;
                
                // Parse array elements
                let mut elements = Vec::new();
                if !matches!(self.current().token_type, TokenType::RBracket) {
                    loop {
                        // Parse numeric literal
                        let elem = match &self.current().token_type {
                            TokenType::Number(n) => {
                                let n = *n;
                                self.advance();
                                n as f32
                            }
                            TokenType::Minus => {
                                self.advance();
                                if let TokenType::Number(n) = self.current().token_type {
                                    let n = n;
                                    self.advance();
                                    -(n as f32)
                                } else {
                                    return Err(self.error("Expected number after minus sign in ARRAY"));
                                }
                            }
                            _ => return Err(self.error("ARRAY elements must be numeric literals")),
                        };
                        elements.push(elem);
                        
                        if !self.match_token(TokenType::Comma) {
                            break;
                        }
                    }
                }
                
                self.expect(TokenType::RBracket)?;
                Ok(Expr::Literal(Value::Vector(elements)))
            }
            
            // Identifier or function call or qualified column
            TokenType::Identifier(_) => {
                let name = self.parse_identifier()?;
                
                // Check for qualified column name (table.column)
                if matches!(self.current().token_type, TokenType::Dot) {
                    self.advance(); // consume the dot
                    let column_name = self.parse_identifier()?;
                    let qualified_name = format!("{}.{}", name, column_name);
                    return Ok(Expr::Column(qualified_name));
                }
                
                // Check for function call
                if matches!(self.current().token_type, TokenType::LParen) {
                    self.advance();
                    
                    // Check for DISTINCT keyword (COUNT(DISTINCT column))
                    let distinct = self.match_token(TokenType::Distinct);
                    
                    let args = if matches!(self.current().token_type, TokenType::RParen) {
                        Vec::new()
                    } else if matches!(self.current().token_type, TokenType::Star) {
                        // Special case: COUNT(*), support * as argument
                        self.advance();
                        vec![Expr::Column("*".to_string())]
                    } else {
                        self.parse_expr_list()?
                    };
                    self.expect(TokenType::RParen)?;
                    
                    // Special handling for POINT(x, y) constructor
                    if name.to_uppercase() == "POINT" {
                        if args.len() != 2 {
                            return Err(self.error("POINT() requires exactly 2 arguments (x, y)"));
                        }
                        
                        // Evaluate arguments to get numeric values
                        let x = match &args[0] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("POINT() arguments must be numeric literals")),
                        };
                        
                        let y = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("POINT() arguments must be numeric literals")),
                        };
                        
                        use crate::types::{Geometry, Point};
                        Ok(Expr::Literal(Value::Spatial(Geometry::Point(Point::new(x, y)))))
                    } else if name.to_uppercase() == "MATCH" {
                        // Special handling for MATCH(column) AGAINST(query)
                        if args.len() != 1 {
                            return Err(self.error("MATCH() requires exactly 1 column argument"));
                        }
                        
                        // Extract column name
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => return Err(self.error("MATCH() argument must be a column name")),
                        };
                        
                        // Expect AGAINST keyword
                        if !self.match_keyword("AGAINST") {
                            return Err(self.error("Expected AGAINST after MATCH(column)"));
                        }
                        
                        // Expect (query_string)
                        self.expect(TokenType::LParen)?;
                        let query = match self.current().token_type {
                            TokenType::String(ref s) => s.clone(),
                            _ => return Err(self.error("AGAINST() requires a string literal")),
                        };
                        self.advance();
                        self.expect(TokenType::RParen)?;
                        
                        Ok(Expr::Match { column, query })
                    } else if name.to_uppercase() == "KNN_SEARCH" {
                        // KNN_SEARCH(vector_column, query_vector, k)
                        if args.len() != 3 {
                            return Err(self.error("KNN_SEARCH() requires 3 arguments: column, query_vector, k"));
                        }
                        
                        // Extract column name
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => return Err(self.error("KNN_SEARCH() first argument must be a column name")),
                        };
                        
                        // Extract query vector
                        let query_vector = match &args[1] {
                            Expr::Literal(Value::Vector(vec)) => vec.clone(),
                            _ => return Err(self.error("KNN_SEARCH() second argument must be a vector literal [...]")),
                        };
                        
                        // Extract k
                        let k = match &args[2] {
                            Expr::Literal(Value::Integer(i)) if *i > 0 => *i as usize,
                            _ => return Err(self.error("KNN_SEARCH() third argument must be a positive integer")),
                        };
                        
                        Ok(Expr::KnnSearch { column, query_vector, k })
                    } else if name.to_uppercase() == "KNN_DISTANCE" {
                        // KNN_DISTANCE(vector_column, query_vector)
                        if args.len() != 2 {
                            return Err(self.error("KNN_DISTANCE() requires 2 arguments: column, query_vector"));
                        }
                        
                        // Extract column name
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => return Err(self.error("KNN_DISTANCE() first argument must be a column name")),
                        };
                        
                        // Extract query vector
                        let query_vector = match &args[1] {
                            Expr::Literal(Value::Vector(vec)) => vec.clone(),
                            _ => return Err(self.error("KNN_DISTANCE() second argument must be a vector literal [...]")),
                        };
                        
                        Ok(Expr::KnnDistance { column, query_vector })
                    } else if name.to_uppercase() == "ST_WITHIN" {
                        // ST_WITHIN(point_column, min_x, min_y, max_x, max_y)
                        if args.len() != 5 {
                            return Err(self.error("ST_WITHIN() requires 5 arguments: column, min_x, min_y, max_x, max_y"));
                        }
                        
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => return Err(self.error("ST_WITHIN() first argument must be a column name")),
                        };
                        
                        let min_x = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_WITHIN() min_x must be a number")),
                        };
                        
                        let min_y = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_WITHIN() min_y must be a number")),
                        };
                        
                        let max_x = match &args[3] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_WITHIN() max_x must be a number")),
                        };
                        
                        let max_y = match &args[4] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_WITHIN() max_y must be a number")),
                        };
                        
                        Ok(Expr::StWithin { column, min_x, min_y, max_x, max_y })
                    } else if name.to_uppercase() == "ST_DISTANCE" {
                        // ST_DISTANCE(point_column, x, y)
                        if args.len() != 3 {
                            return Err(self.error("ST_DISTANCE() requires 3 arguments: column, x, y"));
                        }
                        
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => return Err(self.error("ST_DISTANCE() first argument must be a column name")),
                        };
                        
                        let x = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_DISTANCE() x must be a number")),
                        };
                        
                        let y = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_DISTANCE() y must be a number")),
                        };
                        
                        Ok(Expr::StDistance { column, x, y })
                    } else if name.to_uppercase() == "ST_KNN" {
                        // ST_KNN(point_column, x, y, k)
                        if args.len() != 4 {
                            return Err(self.error("ST_KNN() requires 4 arguments: column, x, y, k"));
                        }
                        
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => return Err(self.error("ST_KNN() first argument must be a column name")),
                        };
                        
                        let x = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_KNN() x must be a number")),
                        };
                        
                        let y = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("ST_KNN() y must be a number")),
                        };
                        
                        let k = match &args[3] {
                            Expr::Literal(Value::Integer(i)) if *i > 0 => *i as usize,
                            _ => return Err(self.error("ST_KNN() k must be a positive integer")),
                        };
                        
                        Ok(Expr::StKnn { column, x, y, k })
                    } else {
                        Ok(Expr::FunctionCall { name, args, distinct })
                    }
                } else {
                    Ok(Expr::Column(name))
                }
            }
            
            // Vector literal [1.0, 2.0, 3.0]
            TokenType::LBracket => {
                self.advance();
                let values = self.parse_expr_list()?;
                self.expect(TokenType::RBracket)?;
                
                // Convert to Value::Vector
                let floats: Result<Vec<f32>> = values.into_iter().enumerate().map(|(idx, e)| {
                    match e {
                        Expr::Literal(Value::Float(f)) => Ok(f as f32),
                        Expr::Literal(Value::Integer(i)) => Ok(i as f32),
                        // 🔧 支持负数：-1.0 会被解析成 UnaryOp
                        Expr::UnaryOp { op, expr } if matches!(op, UnaryOperator::Minus) => {
                            match *expr {
                                Expr::Literal(Value::Float(f)) => Ok(-(f as f32)),
                                Expr::Literal(Value::Integer(i)) => Ok(-(i as f32)),
                                _ => Err(self.error(&format!("Invalid vector element at index {}", idx))),
                            }
                        }
                        _ => {
                            eprintln!("🔍 向量解析失败 at index {}: expr = {:?}", idx, e);
                            Err(self.error(&format!("Vector elements must be numbers (found {:?} at index {})", e, idx)))
                        }
                    }
                }).collect();
                
                Ok(Expr::Literal(Value::Vector(floats?)))
            }
            
            _ => Err(self.error("Expected expression")),
        }
    }
    
    fn parse_postfix_expr(&mut self, mut expr: Expr) -> Result<Expr> {
        loop {
            match &self.current().token_type {
                TokenType::Is => {
                    self.advance();
                    let negated = self.match_token(TokenType::Not);
                    self.expect(TokenType::Null)?;
                    expr = Expr::IsNull {
                        expr: Box::new(expr),
                        negated,
                    };
                }
                TokenType::Not => {
                    // Check for NOT IN, NOT LIKE, NOT BETWEEN
                    self.advance();
                    
                    match &self.current().token_type {
                        TokenType::In => {
                            self.advance();
                            self.expect(TokenType::LParen)?;
                            
                            let list = if matches!(self.current().token_type, TokenType::Select) {
                                let subquery = self.parse_select()?;
                                vec![Expr::Subquery(Box::new(subquery))]
                            } else {
                                self.parse_expr_list()?
                            };
                            
                            self.expect(TokenType::RParen)?;
                            expr = Expr::In {
                                expr: Box::new(expr),
                                list,
                                negated: true,
                            };
                        }
                        TokenType::Like => {
                            self.advance();
                            let pattern = self.parse_expr(4)?;
                            expr = Expr::Like {
                                expr: Box::new(expr),
                                pattern: Box::new(pattern),
                                negated: true,
                            };
                        }
                        TokenType::Between => {
                            self.advance();
                            let low = self.parse_expr(4)?;
                            self.expect(TokenType::And)?;
                            let high = self.parse_expr(4)?;
                            expr = Expr::Between {
                                expr: Box::new(expr),
                                low: Box::new(low),
                                high: Box::new(high),
                                negated: true,
                            };
                        }
                        _ => return Err(self.error("Expected IN, LIKE, or BETWEEN after NOT")),
                    }
                }
                TokenType::Like => {
                    self.advance();
                    let pattern = self.parse_expr(4)?; // Same precedence as comparison
                    expr = Expr::Like {
                        expr: Box::new(expr),
                        pattern: Box::new(pattern),
                        negated: false,
                    };
                }
                TokenType::In => {
                    self.advance();
                    self.expect(TokenType::LParen)?;
                    
                    // Check if this is a subquery: IN (SELECT ...)
                    let list = if matches!(self.current().token_type, TokenType::Select) {
                        // Parse subquery
                        let subquery = self.parse_select()?;
                        vec![Expr::Subquery(Box::new(subquery))]
                    } else {
                        // Parse expression list: IN (1, 2, 3)
                        self.parse_expr_list()?
                    };
                    
                    self.expect(TokenType::RParen)?;
                    expr = Expr::In {
                        expr: Box::new(expr),
                        list,
                        negated: false,
                    };
                }
                TokenType::Between => {
                    self.advance();
                    let low = self.parse_expr(4)?;
                    self.expect(TokenType::And)?;
                    let high = self.parse_expr(4)?;
                    expr = Expr::Between {
                        expr: Box::new(expr),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: false,
                    };
                }
                _ => break,
            }
        }
        
        Ok(expr)
    }
    
    fn try_parse_binary_op(&self) -> Option<BinaryOperator> {
        match &self.current().token_type {
            TokenType::Eq => Some(BinaryOperator::Eq),
            TokenType::Ne => Some(BinaryOperator::Ne),
            TokenType::Lt => Some(BinaryOperator::Lt),
            TokenType::Gt => Some(BinaryOperator::Gt),
            TokenType::Le => Some(BinaryOperator::Le),
            TokenType::Ge => Some(BinaryOperator::Ge),
            TokenType::And => Some(BinaryOperator::And),
            TokenType::Or => Some(BinaryOperator::Or),
            TokenType::Plus => Some(BinaryOperator::Add),
            TokenType::Minus => Some(BinaryOperator::Sub),
            TokenType::Star => Some(BinaryOperator::Mul),
            TokenType::Slash => Some(BinaryOperator::Div),
            TokenType::Percent => Some(BinaryOperator::Mod),
            // E-SQL Vector Distance Operators
            TokenType::L2Distance => Some(BinaryOperator::L2Distance),
            TokenType::CosineDistance => Some(BinaryOperator::CosineDistance),
            TokenType::DotProduct => Some(BinaryOperator::DotProduct),
            _ => None,
        }
    }
    
    // Helper methods
    
    fn parse_identifier(&mut self) -> Result<String> {
        if let TokenType::Identifier(name) = &self.current().token_type {
            let name = name.clone();
            self.advance();
            Ok(name)
        } else {
            Err(self.error("Expected identifier"))
        }
    }
    
    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut list = Vec::new();
        loop {
            list.push(self.parse_identifier()?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(list)
    }
    
    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut list = Vec::new();
        loop {
            list.push(self.parse_expr(0)?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(list)
    }
    
    fn parse_usize(&mut self) -> Result<usize> {
        if let TokenType::Number(n) = self.current().token_type {
            if n < 0.0 || n.fract() != 0.0 {
                return Err(self.error("Expected non-negative integer"));
            }
            self.advance();
            Ok(n as usize)
        } else {
            Err(self.error("Expected number"))
        }
    }
    
    fn current(&self) -> &Token {
        &self.tokens[self.position]
    }
    
    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }
    
    fn match_token(&mut self, token_type: TokenType) -> bool {
        if std::mem::discriminant(&self.current().token_type) == std::mem::discriminant(&token_type) {
            self.advance();
            true
        } else {
            false
        }
    }
    
    fn match_keyword(&mut self, keyword: &str) -> bool {
        if let TokenType::Identifier(ref id) = self.current().token_type {
            if id.to_uppercase() == keyword.to_uppercase() {
                self.advance();
                return true;
            }
        }
        false
    }
    
    fn expect(&mut self, token_type: TokenType) -> Result<()> {
        if std::mem::discriminant(&self.current().token_type) == std::mem::discriminant(&token_type) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("Expected {:?}", token_type)))
        }
    }
    
    fn error(&self, msg: &str) -> MoteDBError {
        let token = self.current();
        MoteDBError::ParseError(format!(
            "{} at line {} column {}",
            msg, token.line, token.column
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::lexer::Lexer;

    fn parse_sql(sql: &str) -> Result<Statement> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    #[test]
    fn test_parse_simple_select() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select(s) => {
                match &s.from {
                    TableRef::Table { name, .. } => assert_eq!(name, "users"),
                    _ => panic!("Expected simple table reference"),
                }
                assert!(matches!(s.columns[0], SelectColumn::Star));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = parse_sql("SELECT id, name FROM users WHERE age > 18").unwrap();
        match stmt {
            Statement::Select(s) => {
                match &s.from {
                    TableRef::Table { name, .. } => assert_eq!(name, "users"),
                    _ => panic!("Expected simple table reference"),
                }
                assert_eq!(s.columns.len(), 2);
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO users (id, name) VALUES (1, 'John')").unwrap();
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.table, "users");
                assert_eq!(i.columns.as_ref().unwrap().len(), 2);
                assert_eq!(i.values.len(), 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE users SET name = 'Jane' WHERE id = 1").unwrap();
        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.table, "users");
                assert_eq!(u.assignments.len(), 1);
                assert!(u.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE age < 18").unwrap();
        match stmt {
            Statement::Delete(d) => {
                assert_eq!(d.table, "users");
                assert!(d.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse_sql("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        match stmt {
            Statement::CreateTable(c) => {
                assert_eq!(c.table, "users");
                assert_eq!(c.columns.len(), 2);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }
}
