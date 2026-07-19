use super::ast::*;
/// SQL Parser - converts tokens into AST
use super::token::{Token, TokenType};
use crate::error::{MoteDBError, Result};
use crate::types::Value;

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    /// Auto-increment counter for unnamed ? parameters
    next_param_idx: usize,
    /// Recursion depth guard for expression parsing (prevents stack overflow)
    recursion_depth: usize,
}

/// Maximum recursion depth for parenthesized / unary expressions. Kept low
/// because each level consumes several parser stack frames and test/runtime
/// threads often have small stacks (~512KB). 64 levels covers any realistic
/// SQL expression while bounding worst-case stack usage to a few hundred KB.
const MAX_RECURSION_DEPTH: usize = 64;
/// Maximum identifier length (table/column names) — prevents DoS via memory exhaustion
const MAX_IDENTIFIER_LENGTH: usize = 4096;

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
            next_param_idx: 1,
            recursion_depth: 0,
        }
    }

    /// Parse a SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        // 🆕 WITH clause — parsed once at the top so the CTEs are visible to
        // both halves of a UNION. Returns (ctes, is_recursive_marker).
        let (mut ctes, _recursive_marker) = if matches!(self.current().token_type, TokenType::With) {
            self.parse_with_clause()?
        } else {
            (Vec::new(), false)
        };

        let stmt = match &self.current().token_type {
            TokenType::Select => {
                let select = self.parse_select()?;
                // Check for UNION / UNION ALL
                if matches!(self.current().token_type, TokenType::Union) {
                    self.advance(); // consume UNION
                    let all = if matches!(self.current().token_type, TokenType::All) {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    if !matches!(self.current().token_type, TokenType::Select) {
                        return Err(self.error("Expected SELECT after UNION"));
                    }
                    let right = self.parse_select()?;
                    Statement::SetOp {
                        left: Box::new(select),
                        right: Box::new(right),
                        op: SetOp::Union,
                        all,
                        ctes: std::mem::take(&mut ctes),
                    }
                } else {
                    Statement::Select {
                        stmt: select,
                        ctes: std::mem::take(&mut ctes),
                    }
                }
            }
            TokenType::Insert => Statement::Insert(self.parse_insert()?),
            TokenType::Update => Statement::Update(self.parse_update()?),
            TokenType::Delete => Statement::Delete(self.parse_delete()?),
            TokenType::Create => self.parse_create()?,
            TokenType::Drop => self.parse_drop()?,
            TokenType::Alter => Statement::AlterTable(self.parse_alter_table()?),
            TokenType::Begin => self.parse_begin()?,
            TokenType::Commit => self.parse_commit()?,
            TokenType::Rollback => self.parse_rollback()?,
            TokenType::Show => self.parse_show()?,
            TokenType::Describe | TokenType::Desc => self.parse_describe()?,
            _ => return Err(self.error("Expected SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, SHOW, DESCRIBE, BEGIN, COMMIT, or ROLLBACK")),
        };

        // Reject WITH attached to a non-query statement. (Also catches the
        // case where the parser advanced past WITH but didn't consume ctes.)
        if !ctes.is_empty() {
            return Err(self.error(
                "WITH clause is only valid before SELECT (or SELECT ... UNION ...)"));
        }

        // Optionally consume semicolon
        if matches!(self.current().token_type, TokenType::Semicolon) {
            self.advance();
        }

        // Reject multiple statements (security: prevents silent truncation)
        if !matches!(self.current().token_type, TokenType::Eof) {
            return Err(self
                .error("Multiple statements are not supported; unexpected input after statement"));
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

        // FROM clause (optional - for queries like SELECT LAST_INSERT_ID())
        let from = if self.match_token(TokenType::From) {
            Some(self.parse_table_ref()?)
        } else {
            None
        };

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

    /// Parse a WITH clause: `WITH [RECURSIVE] name [(col, ...)] AS ( SELECT ... ), ...`
    ///
    /// Returns `(Vec<CteDef>, is_recursive)`. The caller is responsible for
    /// attaching the CTEs to the following SELECT/SetOp statement.
    ///
    /// RECURSIVE is accepted syntactically (matches SQL standard) but v1 of
    /// the executor rejects any CTE body that self-references — see
    /// `QueryExecutor::apply_ctes`.
    fn parse_with_clause(&mut self) -> Result<(Vec<CteDef>, bool)> {
        self.expect(TokenType::With)?; // consume WITH
        let is_recursive = self.match_token(TokenType::Recursive);

        let mut ctes = Vec::new();
        loop {
            // CTE name
            let name = self.parse_identifier()?;
            // Optional column list: ( col, col, ... )
            let columns = if self.match_token(TokenType::LParen) {
                let cols = self.parse_identifier_list()?;
                self.expect(TokenType::RParen)?;
                Some(cols)
            } else {
                None
            };
            // AS
            self.expect(TokenType::As)?;
            // ( SELECT ... )
            self.expect(TokenType::LParen)?;
            if !matches!(self.current().token_type, TokenType::Select) {
                return Err(self.error("Expected SELECT inside CTE body"));
            }
            let query = self.parse_select()?;
            self.expect(TokenType::RParen)?;

            ctes.push(CteDef {
                name,
                columns,
                query,
            });

            // Another CTE?
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok((ctes, is_recursive))
    }

    fn parse_column_list(&mut self) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_qualified_column_name()?);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        Ok(columns)
    }

    /// Parse a column reference that may be table-qualified: `name` or `tbl.name`.
    /// Used by GROUP BY / ORDER BY column lists where bare identifiers
    /// (`parse_identifier`) would stop at the dot.
    fn parse_qualified_column_name(&mut self) -> Result<String> {
        let first = self.parse_identifier()?;
        // Optional `.col` suffix for table-qualified references.
        if matches!(self.current().token_type, TokenType::Dot) {
            self.advance(); // consume '.'
            let second = self.parse_identifier()?;
            Ok(format!("{}.{}", first, second))
        } else {
            Ok(first)
        }
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
            TokenType::Join
                | TokenType::Inner
                | TokenType::Left
                | TokenType::Right
                | TokenType::Full
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
            TokenType::Full => {
                self.advance();
                self.match_token(TokenType::Outer); // OUTER is optional
                self.expect(TokenType::Join)?;
                JoinType::Full
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

        Ok(InsertStmt {
            table,
            columns,
            values,
        })
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

        Ok(UpdateStmt {
            table,
            assignments,
            where_clause,
        })
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

        Ok(DeleteStmt {
            table,
            where_clause,
        })
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
                // Check for SPATIAL/OCTREE keywords (GEOMETRY is already handled above)
                let id_upper = id.to_uppercase();
                if id_upper == "SPATIAL" || id_upper == "OCTREE" {
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

        // 🆕 Optional IF NOT EXISTS (must come BEFORE the table name in SQL
        // standard syntax: `CREATE TABLE IF NOT EXISTS name (...)`).
        // Parse "IF NOT EXISTS" — IF and EXISTS are identifiers (not
        // registered keywords), NOT is the Not keyword.
        let mut if_not_exists = false;
        if let TokenType::Identifier(id) = &self.current().token_type {
            if id.eq_ignore_ascii_case("IF") {
                self.advance();
                // Expect NOT (which is a registered keyword)
                if !self.match_token(TokenType::Not) {
                    return Err(self.error("Expected NOT after IF"));
                }
                // Expect EXISTS (identifier)
                if let TokenType::Identifier(id3) = &self.current().token_type {
                    if id3.eq_ignore_ascii_case("EXISTS") {
                        self.advance();
                        if_not_exists = true;
                    } else {
                        return Err(self.error("Expected EXISTS after NOT"));
                    }
                } else {
                    return Err(self.error("Expected EXISTS after NOT"));
                }
            }
        }

        let table = self.parse_identifier()?;

        self.expect(TokenType::LParen)?;
        let columns = self.parse_column_defs()?;
        self.expect(TokenType::RParen)?;

        // Parse optional TIMESERIES(ts_column) clause
        let mut table_type = crate::types::TableType::Standard;
        let mut timeseries_column = None;

        if self.match_token(TokenType::Timeseries) {
            table_type = crate::types::TableType::TimeSeries;
            self.expect(TokenType::LParen)?;
            timeseries_column = Some(self.parse_identifier()?);
            self.expect(TokenType::RParen)?;
        }

        // Parse optional TTL clause: TTL 7d / TTL 24h / TTL 30m / TTL 3600s
        let mut ttl = None;
        if self.match_token(TokenType::Ttl) {
            ttl = Some(self.parse_ttl_duration()?);
        }

        Ok(CreateTableStmt {
            table,
            columns,
            table_type,
            timeseries_column,
            ttl,
            if_not_exists,
        })
    }

    /// Parse TTL duration: NUMBER followed by s/m/h/d suffix
    /// Examples: 7d, 24h, 30m, 3600s
    fn parse_ttl_duration(&mut self) -> Result<crate::types::TTLDuration> {
        let value = self.parse_i64()?;
        if value <= 0 {
            return Err(self.error("TTL duration must be positive"));
        }

        // Look for a unit suffix as an identifier
        let unit = if let TokenType::Identifier(ref id) = self.current().token_type {
            let u = id.to_lowercase();
            self.advance();
            u
        } else {
            // Default to seconds if no unit specified
            "s".to_string()
        };

        let duration = match unit.as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => {
                crate::types::TTLDuration::from_secs(value as u64)
            }
            "m" | "min" | "mins" | "minute" | "minutes" => {
                crate::types::TTLDuration::from_mins(value as u64)
            }
            "h" | "hr" | "hrs" | "hour" | "hours" => {
                crate::types::TTLDuration::from_hours(value as u64)
            }
            "d" | "day" | "days" => crate::types::TTLDuration::from_days(value as u64),
            _ => return Err(self.error(&format!("Unknown TTL unit: '{}'", unit))),
        };

        Ok(duration)
    }

    fn parse_column_defs(&mut self) -> Result<Vec<ColumnDef>> {
        let mut columns = Vec::new();

        loop {
            let name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;

            // Parse constraints in any order
            let mut nullable = true;
            let mut primary_key = false;
            let mut auto_increment = false;
            let mut auto_increment_start: Option<i64> = None;

            loop {
                // NOT NULL
                if self.match_token(TokenType::Not) {
                    self.expect(TokenType::Null)?;
                    nullable = false;
                    continue;
                }

                // NULL (explicit nullable)
                if self.match_token(TokenType::Null) {
                    nullable = true;
                    continue;
                }

                // PRIMARY KEY
                if self.match_token(TokenType::Primary) {
                    self.expect(TokenType::Key)?;
                    primary_key = true;
                    nullable = false;
                    continue;
                }

                // AUTO_INCREMENT
                if self.match_token(TokenType::AutoIncrement) {
                    auto_increment = true;
                    if !primary_key {
                        return Err(self.error("AUTO_INCREMENT can only be used with PRIMARY KEY"));
                    }
                    if data_type != DataType::Integer && data_type != DataType::BigInt {
                        return Err(self.error(
                            "AUTO_INCREMENT can only be used with INTEGER or BIGINT columns",
                        ));
                    }
                    if self.match_token(TokenType::Eq) {
                        let start = self.parse_i64()?;
                        if start < 0 {
                            return Err(self.error("AUTO_INCREMENT start must be non-negative"));
                        }
                        auto_increment_start = Some(start);
                    }
                    continue;
                }

                break;
            }

            columns.push(ColumnDef {
                name,
                data_type,
                nullable,
                primary_key,
                auto_increment,
                auto_increment_start,
            });

            // Check for duplicate column names
            if columns.len() > 1 {
                let new_name = &columns.last().unwrap().name;
                if columns[..columns.len() - 1]
                    .iter()
                    .any(|c| c.name == *new_name)
                {
                    return Err(self.error(&format!("Duplicate column name '{}'", new_name)));
                }
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let data_type = match &self.current().token_type {
            TokenType::Integer => DataType::Integer,
            TokenType::BigInt => DataType::BigInt,
            TokenType::Float => DataType::Float,
            TokenType::Text => DataType::Text,
            TokenType::Boolean => DataType::Boolean,
            TokenType::Timestamp => DataType::Timestamp,
            TokenType::Geometry => DataType::Geometry,
            TokenType::Identifier(name) if name.to_uppercase() == "POINT3D" => DataType::Geometry,
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
                IndexType::Octree
            }
            TokenType::Timestamp => {
                self.advance();
                IndexType::Timestamp
            }
            TokenType::Identifier(ref id) => {
                // Also check for SPATIAL/OCTREE keywords (not a data type)
                let id_upper = id.to_uppercase();
                match id_upper.as_str() {
                    "SPATIAL" => {
                        self.advance();
                        IndexType::Octree
                    }
                    "OCTREE" => {
                        self.advance();
                        IndexType::Octree
                    }
                    _ => IndexType::BTree, // Default
                }
            }
            _ => IndexType::BTree, // Default
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
                        "SPATIAL" => IndexType::Octree,
                        "OCTREE" => IndexType::Octree,
                        "TIMESTAMP" => IndexType::Timestamp,
                        _ => {
                            return Err(MoteDBError::ParseError(format!(
                                "Unknown index type: {}",
                                id
                            )))
                        }
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
                _ => {
                    return Err(MoteDBError::ParseError(
                        "Expected index type after USING".to_string(),
                    ))
                }
            }
        } else {
            // No USING clause, use the index_type determined at the beginning
            index_type
        };

        // Parse optional WITH clause: WITH (metric = 'l2' | 'cosine')
        let mut metric = None;
        let token_type = self.current().token_type.clone();
        if let TokenType::Identifier(ref id) = token_type {
            if id.to_uppercase() == "WITH" {
                self.advance(); // consume WITH
                self.expect(TokenType::LParen)?;

                // Parse key = value pairs
                loop {
                    let key = self.parse_identifier()?;
                    let key_upper = key.to_uppercase();
                    self.expect(TokenType::Eq)?;

                    match key_upper.as_str() {
                        "METRIC" => {
                            let value = self.parse_identifier()?;
                            let value_lower = value.to_lowercase();
                            match value_lower.as_str() {
                                "l2" | "euclidean" => metric = Some("l2".to_string()),
                                "cosine" => metric = Some("cosine".to_string()),
                                _ => {
                                    return Err(MoteDBError::ParseError(format!(
                                        "Unknown metric '{}'. Use 'l2' or 'cosine'",
                                        value
                                    )))
                                }
                            }
                        }
                        _ => {
                            return Err(MoteDBError::ParseError(format!(
                                "Unknown WITH option '{}'. Supported: metric",
                                key
                            )))
                        }
                    }

                    if !self.match_token(TokenType::Comma) {
                        break;
                    }
                }

                self.expect(TokenType::RParen)?;
            }
        }

        Ok(CreateIndexStmt {
            index_name,
            table,
            column,
            index_type: final_index_type,
            metric,
        })
    }

    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(TokenType::Drop)?;

        match &self.current().token_type {
            TokenType::Table => {
                self.advance();
                // Optional IF EXISTS clause (parsed as two identifiers).
                let if_exists = if matches!(&self.current().token_type, TokenType::Identifier(ref w) if w.eq_ignore_ascii_case("IF"))
                {
                    self.advance();
                    match &self.current().token_type {
                        TokenType::Identifier(ref w) if w.eq_ignore_ascii_case("EXISTS") => {
                            self.advance();
                            true
                        }
                        _ => return Err(self.error("Expected EXISTS after IF")),
                    }
                } else {
                    false
                };
                let table = self.parse_identifier()?;
                Ok(Statement::DropTable(DropTableStmt { table, if_exists }))
            }
            TokenType::Index => {
                self.advance();
                let index_name = self.parse_identifier()?;
                // Optionally consume ON table_name (MySQL-compatible syntax)
                if self.match_token(TokenType::On) {
                    let _ = self.parse_identifier()?;
                }
                Ok(Statement::DropIndex(DropIndexStmt { index_name }))
            }
            _ => Err(self.error("Expected TABLE or INDEX after DROP")),
        }
    }

    /// Parse BEGIN [TRANSACTION]
    fn parse_begin(&mut self) -> Result<Statement> {
        self.expect(TokenType::Begin)?;
        // Optionally consume "TRANSACTION" keyword (not reserved, so it's an Identifier token)
        if matches!(&self.current().token_type, TokenType::Identifier(ref s) if s.eq_ignore_ascii_case("transaction"))
        {
            self.advance();
        }
        Ok(Statement::BeginTransaction)
    }

    /// Parse COMMIT TRANSACTION
    fn parse_commit(&mut self) -> Result<Statement> {
        self.expect(TokenType::Commit)?;
        Ok(Statement::CommitTransaction)
    }

    /// Parse ROLLBACK TRANSACTION
    fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect(TokenType::Rollback)?;
        Ok(Statement::RollbackTransaction)
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
        if !matches!(
            self.current().token_type,
            TokenType::Describe | TokenType::Desc
        ) {
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

        loop {
            // Try infix binary operators first
            if let Some(op) = self.try_parse_binary_op() {
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
                continue;
            }

            // Try postfix operators (IS NULL, IN, LIKE, BETWEEN, NOT IN/LIKE/BETWEEN)
            if self.can_parse_postfix() {
                left = self.parse_single_postfix(left)?;
                continue;
            }

            break;
        }

        Ok(left)
    }

    fn parse_prefix_expr(&mut self) -> Result<Expr> {
        match &self.current().token_type {
            // CASE WHEN ... THEN ... [WHEN ... THEN ...] [ELSE ...] END
            TokenType::Case => {
                self.advance(); // consume CASE
                let mut whens = Vec::new();
                while matches!(self.current().token_type, TokenType::When) {
                    self.advance(); // consume WHEN
                    let cond = self.parse_expr(0)?;
                    // expect() already calls advance() — don't double-advance.
                    self.expect(TokenType::Then)?;
                    let result = self.parse_expr(0)?;
                    whens.push((cond, result));
                }
                let else_expr = if matches!(self.current().token_type, TokenType::Else) {
                    self.advance(); // consume ELSE
                    Some(Box::new(self.parse_expr(0)?))
                } else {
                    None
                };
                self.expect(TokenType::End)?; // consumes END
                Ok(Expr::Case { whens, else_expr })
            }
            // Unary operators (with depth guard to prevent stack overflow on chained NOT/-)
            TokenType::Not => {
                self.advance();
                self.recursion_depth += 1;
                if self.recursion_depth > MAX_RECURSION_DEPTH {
                    self.recursion_depth -= 1;
                    return Err(self.error("Expression nesting too deep"));
                }
                let expr = self.parse_expr(10)?;
                self.recursion_depth -= 1;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                })
            }
            TokenType::Minus => {
                self.advance();
                self.recursion_depth += 1;
                if self.recursion_depth > MAX_RECURSION_DEPTH {
                    self.recursion_depth -= 1;
                    return Err(self.error("Expression nesting too deep"));
                }
                let expr = self.parse_expr(10)?;
                self.recursion_depth -= 1;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            TokenType::Plus => {
                self.advance();
                self.recursion_depth += 1;
                if self.recursion_depth > MAX_RECURSION_DEPTH {
                    self.recursion_depth -= 1;
                    return Err(self.error("Expression nesting too deep"));
                }
                let expr = self.parse_expr(10)?;
                self.recursion_depth -= 1;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }

            // Parenthesized expression OR subquery
            TokenType::LParen => {
                self.advance();

                // Guard against stack overflow on deeply nested parens. Each
                // nesting level adds several parser stack frames (parse_expr ->
                // parse_prefix_expr -> ...), so the limit must stay well below
                // the per-thread stack size (~512KB for test threads). 64 levels
                // is ample for any real SQL and bounds the worst-case stack to
                // a few hundred KB.
                self.recursion_depth += 1;
                if self.recursion_depth > MAX_RECURSION_DEPTH {
                    self.recursion_depth -= 1;
                    return Err(self.error("Expression nesting too deep"));
                }

                // Check if this is a subquery (SELECT ...)
                let result = if matches!(self.current().token_type, TokenType::Select) {
                    let subquery = self.parse_select()?;
                    self.expect(TokenType::RParen)?;
                    Ok(Expr::Subquery(Box::new(subquery)))
                } else {
                    // Otherwise, it's a regular parenthesized expression
                    let expr = self.parse_expr(0)?;
                    self.expect(TokenType::RParen)?;
                    Ok(expr)
                };

                self.recursion_depth -= 1;
                result
            }

            // Literals
            TokenType::Number(n) => {
                let n = *n;
                self.advance();
                // Integer if no fraction and within i64 range (beware f64 precision loss).
                // f64 cannot exactly represent i64::MAX/MIN, so clamp the f64
                // boundaries 9223372036854775808.0 (=2^63, the value i64::MAX
                // rounds to) to i64::MAX, and -9223372036854775808.0 to i64::MIN,
                // so the extreme values round-trip as Integer instead of Float.
                if n.fract() == 0.0 {
                    if n == 9223372036854775808.0 {
                        return Ok(Expr::Literal(Value::Integer(i64::MAX)));
                    }
                    if n == -9223372036854775808.0 {
                        return Ok(Expr::Literal(Value::Integer(i64::MIN)));
                    }
                    if n > i64::MIN as f64 && n < 9223372036854775808.0 {
                        let v = n as i64;
                        if (v as f64 - n).abs() < 0.5 {
                            return Ok(Expr::Literal(Value::Integer(v)));
                        }
                    }
                }
                Ok(Expr::Literal(Value::Float(n)))
            }
            TokenType::String(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expr::Literal(Value::text(s)))
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

            // Bind variable (? or ?1, ?2, ...)
            TokenType::Parameter(idx) => {
                let idx = if *idx == 0 {
                    // Unnamed ?: auto-assign sequential 1-based index
                    let next = self.next_param_idx;
                    self.next_param_idx += 1;
                    next
                } else {
                    *idx
                };
                self.advance();
                Ok(Expr::Parameter(idx))
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
                                    self.advance();
                                    -(n as f32)
                                } else {
                                    return Err(
                                        self.error("Expected number after minus sign in ARRAY")
                                    );
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
                Ok(Expr::Literal(Value::Vector(crate::types::ArcVec::new(
                    elements,
                ))))
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

                    // Special handling for POINT(x, y) constructor — auto-converts to 3D (z=0)
                    if name.to_uppercase() == "POINT" {
                        if args.len() != 2 {
                            return Err(self.error("POINT() requires exactly 2 arguments (x, y)"));
                        }

                        // Evaluate arguments to get numeric values (supports negatives)
                        let x = self.eval_num(&args[0]).map_err(|_| {
                            self.error("POINT() arguments must be numeric literals")
                        })?;
                        let y = self.eval_num(&args[1]).map_err(|_| {
                            self.error("POINT() arguments must be numeric literals")
                        })?;

                        use crate::types::{Geometry, Point3D};
                        Ok(Expr::Literal(Value::spatial(Geometry::Point3D(
                            Point3D::new(x, y, 0.0),
                        ))))
                    } else if name.to_uppercase() == "POINT3D" {
                        if args.len() != 3 {
                            return Err(
                                self.error("POINT3D() requires exactly 3 arguments (x, y, z)")
                            );
                        }
                        let x = match &args[0] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => {
                                return Err(
                                    self.error("POINT3D() arguments must be numeric literals")
                                )
                            }
                        };
                        let y = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => {
                                return Err(
                                    self.error("POINT3D() arguments must be numeric literals")
                                )
                            }
                        };
                        let z = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => {
                                return Err(
                                    self.error("POINT3D() arguments must be numeric literals")
                                )
                            }
                        };
                        use crate::types::{Geometry as G3, Point3D};
                        Ok(Expr::Literal(Value::spatial(G3::Point3D(Point3D::new(
                            x, y, z,
                        )))))
                    } else if name.to_uppercase() == "MATCH"
                        || name.to_uppercase() == "MATCH_AGAINST"
                    {
                        // MATCH(column) AGAINST(query) or MATCH(column, query)
                        if args.len() == 2 {
                            // Short form: MATCH(column, query_text)
                            let column = match &args[0] {
                                Expr::Column(col_name) => col_name.clone(),
                                _ => {
                                    return Err(
                                        self.error("MATCH() first argument must be a column name")
                                    )
                                }
                            };
                            let query = match &args[1] {
                                Expr::Literal(Value::Text(s)) => s.to_string(),
                                _ => {
                                    return Err(
                                        self.error("MATCH() second argument must be a string")
                                    )
                                }
                            };
                            Ok(Expr::Match {
                                column,
                                query,
                                phrase: false,
                            })
                        } else if args.len() == 1 {
                            // Long form: MATCH(column) AGAINST(query)
                            let column = match &args[0] {
                                Expr::Column(col_name) => col_name.clone(),
                                _ => {
                                    return Err(self.error("MATCH() argument must be a column name"))
                                }
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

                            // Detect phrase query: query starts and ends with double quotes
                            let phrase =
                                query.starts_with('"') && query.ends_with('"') && query.len() >= 2;
                            let query = if phrase {
                                query[1..query.len() - 1].to_string()
                            } else {
                                query
                            };

                            Ok(Expr::Match {
                                column,
                                query,
                                phrase,
                            })
                        } else {
                            Err(self.error("MATCH() requires 1 or 2 arguments"))
                        }
                    } else if name.to_uppercase() == "KNN_SEARCH" {
                        // KNN_SEARCH(vector_column, query_vector, k)
                        if args.len() != 3 {
                            return Err(self.error(
                                "KNN_SEARCH() requires 3 arguments: column, query_vector, k",
                            ));
                        }

                        // Extract column name
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                return Err(
                                    self.error("KNN_SEARCH() first argument must be a column name")
                                )
                            }
                        };

                        // Extract query vector
                        let query_vector =
                            match &args[1] {
                                Expr::Literal(Value::Vector(vec)) => vec.clone(),
                                _ => return Err(self.error(
                                    "KNN_SEARCH() second argument must be a vector literal [...]",
                                )),
                            };

                        // Extract k
                        let k = match &args[2] {
                            Expr::Literal(Value::Integer(i)) if *i > 0 => *i as usize,
                            _ => {
                                return Err(self.error(
                                    "KNN_SEARCH() third argument must be a positive integer",
                                ))
                            }
                        };

                        Ok(Expr::KnnSearch {
                            column,
                            query_vector,
                            k,
                        })
                    } else if name.to_uppercase() == "KNN_DISTANCE" {
                        // KNN_DISTANCE(vector_column, query_vector)
                        if args.len() != 2 {
                            return Err(self.error(
                                "KNN_DISTANCE() requires 2 arguments: column, query_vector",
                            ));
                        }

                        // Extract column name
                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                return Err(self
                                    .error("KNN_DISTANCE() first argument must be a column name"))
                            }
                        };

                        // Extract query vector
                        let query_vector =
                            match &args[1] {
                                Expr::Literal(Value::Vector(vec)) => vec.clone(),
                                _ => return Err(self.error(
                                    "KNN_DISTANCE() second argument must be a vector literal [...]",
                                )),
                            };

                        Ok(Expr::KnnDistance {
                            column,
                            query_vector,
                        })
                    } else if name.to_uppercase() == "ST_WITHIN" {
                        // ST_WITHIN(point_column, min_x, min_y, max_x, max_y)
                        if args.len() != 5 {
                            return Err(self.error("ST_WITHIN() requires 5 arguments: column, min_x, min_y, max_x, max_y"));
                        }

                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                return Err(
                                    self.error("ST_WITHIN() first argument must be a column name")
                                )
                            }
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

                        // 2D ST_WITHIN: the z range is unbounded (±∞) so it does
                        // not filter out any point based on z. Using z∈[0,0] would
                        // wrongly exclude points whose stored z ≠ 0.
                        Ok(Expr::StWithin3D {
                            column,
                            min_x,
                            min_y,
                            min_z: f64::NEG_INFINITY,
                            max_x,
                            max_y,
                            max_z: f64::INFINITY,
                        })
                    } else if name.to_uppercase() == "ST_DISTANCE" {
                        // ST_DISTANCE(point_column, x, y)
                        if args.len() != 3 {
                            return Err(
                                self.error("ST_DISTANCE() requires 3 arguments: column, x, y")
                            );
                        }

                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                return Err(self
                                    .error("ST_DISTANCE() first argument must be a column name"))
                            }
                        };

                        let x = self
                            .eval_num(&args[1])
                            .map_err(|_| self.error("ST_DISTANCE() x must be a number"))?;
                        let y = self
                            .eval_num(&args[2])
                            .map_err(|_| self.error("ST_DISTANCE() y must be a number"))?;

                        Ok(Expr::StDistance3D {
                            column,
                            x,
                            y,
                            z: 0.0,
                        })
                    } else if name.to_uppercase() == "ST_KNN" {
                        // ST_KNN(point_column, x, y, k)
                        if args.len() != 4 {
                            return Err(
                                self.error("ST_KNN() requires 4 arguments: column, x, y, k")
                            );
                        }

                        let column = match &args[0] {
                            Expr::Column(col_name) => col_name.clone(),
                            _ => {
                                return Err(
                                    self.error("ST_KNN() first argument must be a column name")
                                )
                            }
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

                        Ok(Expr::StKnn3D {
                            column,
                            x,
                            y,
                            z: 0.0,
                            k,
                        })
                    } else if name.to_uppercase() == "ST_WITHIN_3D" {
                        if args.len() != 7 {
                            return Err(self.error("ST_WITHIN_3D() requires 7 arguments: column, min_x, min_y, min_z, max_x, max_y, max_z"));
                        }
                        let column = match &args[0] {
                            Expr::Column(n) => n.clone(),
                            _ => {
                                return Err(
                                    self.error("ST_WITHIN_3D() first argument must be a column")
                                )
                            }
                        };
                        let nums: Result<Vec<f64>> =
                            args[1..]
                                .iter()
                                .map(|a| match a {
                                    Expr::Literal(Value::Float(f)) => Ok(*f),
                                    Expr::Literal(Value::Integer(i)) => Ok(*i as f64),
                                    _ => Err(self
                                        .error("ST_WITHIN_3D() bounds must be numeric literals")),
                                })
                                .collect();
                        let nums = nums?;
                        Ok(Expr::StWithin3D {
                            column,
                            min_x: nums[0],
                            min_y: nums[1],
                            min_z: nums[2],
                            max_x: nums[3],
                            max_y: nums[4],
                            max_z: nums[5],
                        })
                    } else if name.to_uppercase() == "ST_DISTANCE_3D" {
                        if args.len() != 4 {
                            return Err(self
                                .error("ST_DISTANCE_3D() requires 4 arguments: column, x, y, z"));
                        }
                        let column = match &args[0] {
                            Expr::Column(n) => n.clone(),
                            _ => {
                                return Err(
                                    self.error("ST_DISTANCE_3D() first argument must be a column")
                                )
                            }
                        };
                        let x = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("x must be numeric")),
                        };
                        let y = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("y must be numeric")),
                        };
                        let z = match &args[3] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("z must be numeric")),
                        };
                        Ok(Expr::StDistance3D { column, x, y, z })
                    } else if name.to_uppercase() == "ST_KNN_3D" {
                        if args.len() != 5 {
                            return Err(
                                self.error("ST_KNN_3D() requires 5 arguments: column, x, y, z, k")
                            );
                        }
                        let column = match &args[0] {
                            Expr::Column(n) => n.clone(),
                            _ => {
                                return Err(
                                    self.error("ST_KNN_3D() first argument must be a column")
                                )
                            }
                        };
                        let x = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("x must be numeric")),
                        };
                        let y = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("y must be numeric")),
                        };
                        let z = match &args[3] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("z must be numeric")),
                        };
                        let k = match &args[4] {
                            Expr::Literal(Value::Integer(i)) if *i > 0 => *i as usize,
                            _ => return Err(self.error("ST_KNN_3D() k must be a positive integer")),
                        };
                        Ok(Expr::StKnn3D { column, x, y, z, k })
                    } else if name.to_uppercase() == "ST_RADIUS_3D" {
                        if args.len() != 5 {
                            return Err(self.error(
                                "ST_RADIUS_3D() requires 5 arguments: column, x, y, z, radius",
                            ));
                        }
                        let column = match &args[0] {
                            Expr::Column(n) => n.clone(),
                            _ => {
                                return Err(
                                    self.error("ST_RADIUS_3D() first argument must be a column")
                                )
                            }
                        };
                        let x = match &args[1] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("x must be numeric")),
                        };
                        let y = match &args[2] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("y must be numeric")),
                        };
                        let z = match &args[3] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("z must be numeric")),
                        };
                        let radius = match &args[4] {
                            Expr::Literal(Value::Float(f)) => *f,
                            Expr::Literal(Value::Integer(i)) => *i as f64,
                            _ => return Err(self.error("radius must be numeric")),
                        };
                        Ok(Expr::StRadius3D {
                            column,
                            x,
                            y,
                            z,
                            radius,
                        })
                    } else {
                        Ok(Expr::FunctionCall {
                            name,
                            args,
                            distinct,
                        })
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
                let floats: Vec<f32> = values
                    .into_iter()
                    .enumerate()
                    .map(|(idx, e)| {
                        match e {
                            Expr::Literal(Value::Float(f)) => Ok(f as f32),
                            Expr::Literal(Value::Integer(i)) => Ok(i as f32),
                            // 🔧 支持负数：-1.0 会被解析成 UnaryOp
                            Expr::UnaryOp {
                                op: UnaryOperator::Minus,
                                expr,
                            } => match *expr {
                                Expr::Literal(Value::Float(f)) => Ok(-(f as f32)),
                                Expr::Literal(Value::Integer(i)) => Ok(-(i as f32)),
                                _ => {
                                    Err(self
                                        .error(&format!("Invalid vector element at index {}", idx)))
                                }
                            },
                            _ => {
                                debug_log!("🔍 向量解析失败 at index {}: expr = {:?}", idx, e);
                                Err(self.error(&format!(
                                    "Vector elements must be numbers (found {:?} at index {})",
                                    e, idx
                                )))
                            }
                        }
                    })
                    .collect::<Result<Vec<f32>>>()?;

                Ok(Expr::Literal(Value::Vector(crate::types::ArcVec::new(
                    floats,
                ))))
            }

            _ => Err(self.error("Expected expression")),
        }
    }

    /// Check if the current token starts a postfix operator.
    fn can_parse_postfix(&self) -> bool {
        match &self.current().token_type {
            TokenType::Is => true,
            TokenType::Not => {
                matches!(
                    self.peek_token_type(),
                    TokenType::In | TokenType::Like | TokenType::Between
                )
            }
            TokenType::Like | TokenType::In | TokenType::Between => true,
            _ => false,
        }
    }

    /// Parse a single postfix operator (IS NULL, IN, LIKE, BETWEEN, NOT IN/LIKE/BETWEEN).
    fn parse_single_postfix(&mut self, expr: Expr) -> Result<Expr> {
        match &self.current().token_type {
            TokenType::Is => {
                self.advance();
                let negated = self.match_token(TokenType::Not);
                self.expect(TokenType::Null)?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated,
                })
            }
            TokenType::Not => {
                self.advance(); // consume NOT

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
                        Ok(Expr::In {
                            expr: Box::new(expr),
                            list,
                            negated: true,
                        })
                    }
                    TokenType::Like => {
                        self.advance();
                        let pattern = self.parse_expr(4)?;
                        Ok(Expr::Like {
                            expr: Box::new(expr),
                            pattern: Box::new(pattern),
                            negated: true,
                        })
                    }
                    TokenType::Between => {
                        self.advance();
                        let low = self.parse_expr(4)?;
                        self.expect(TokenType::And)?;
                        let high = self.parse_expr(4)?;
                        Ok(Expr::Between {
                            expr: Box::new(expr),
                            low: Box::new(low),
                            high: Box::new(high),
                            negated: true,
                        })
                    }
                    _ => Err(self.error("Expected IN, LIKE, or BETWEEN after NOT")),
                }
            }
            TokenType::Like => {
                self.advance();
                let pattern = self.parse_expr(4)?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: false,
                })
            }
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
                Ok(Expr::In {
                    expr: Box::new(expr),
                    list,
                    negated: false,
                })
            }
            TokenType::Between => {
                self.advance();
                let low = self.parse_expr(4)?;
                self.expect(TokenType::And)?;
                let high = self.parse_expr(4)?;
                Ok(Expr::Between {
                    expr: Box::new(expr),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: false,
                })
            }
            _ => unreachable!("can_parse_postfix should prevent this"),
        }
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
            if name.len() > MAX_IDENTIFIER_LENGTH {
                return Err(self.error("Identifier too long"));
            }
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

    fn eval_num(&self, expr: &Expr) -> std::result::Result<f64, ()> {
        match expr {
            Expr::Literal(Value::Float(f)) => Ok(*f),
            Expr::Literal(Value::Integer(i)) => Ok(*i as f64),
            Expr::UnaryOp { op, expr, .. } => {
                let v = self.eval_num(expr)?;
                match op {
                    UnaryOperator::Minus => Ok(-v),
                    UnaryOperator::Plus => Ok(v),
                    _ => Err(()),
                }
            }
            _ => Err(()),
        }
    }

    fn parse_usize(&mut self) -> Result<usize> {
        if let TokenType::Number(n) = self.current().token_type {
            if n < 0.0 || n.fract() != 0.0 {
                return Err(self.error("Expected non-negative integer"));
            }
            if n > usize::MAX as f64 {
                return Err(self.error("Number too large for usize"));
            }
            let v = n as usize;
            if (v as f64 - n).abs() > 0.5 {
                return Err(self.error("Number too large for usize (precision loss)"));
            }
            self.advance();
            Ok(v)
        } else {
            Err(self.error("Expected number"))
        }
    }

    /// 🚀 Phase 4: Parse i64 (支持负数)
    fn parse_i64(&mut self) -> Result<i64> {
        if let TokenType::Number(n) = self.current().token_type {
            if n.fract() != 0.0 {
                return Err(self.error("Expected integer"));
            }
            if n > i64::MAX as f64 || n < i64::MIN as f64 {
                return Err(self.error("Integer out of range"));
            }
            let v = n as i64;
            if (v as f64 - n).abs() > 0.5 {
                return Err(self.error("Integer out of range (precision loss)"));
            }
            self.advance();
            Ok(v)
        } else {
            Err(self.error("Expected number"))
        }
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or_else(|| {
            static EOF: Token = Token {
                token_type: TokenType::Eof,
                line: 0,
                column: 0,
            };
            &EOF
        })
    }

    fn peek_token_type(&self) -> &TokenType {
        if self.position + 1 < self.tokens.len() {
            &self.tokens[self.position + 1].token_type
        } else {
            &TokenType::Eof
        }
    }

    fn advance(&mut self) {
        if !self.tokens.is_empty() && self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }

    fn match_token(&mut self, token_type: TokenType) -> bool {
        if std::mem::discriminant(&self.current().token_type) == std::mem::discriminant(&token_type)
        {
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
        if std::mem::discriminant(&self.current().token_type) == std::mem::discriminant(&token_type)
        {
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

    /// 🆕 Parse ALTER TABLE statement
    ///
    /// Syntax: ALTER TABLE table_name AUTO_INCREMENT = value
    ///      |  ALTER TABLE table_name ADD [COLUMN] name type [DEFAULT value]
    fn parse_alter_table(&mut self) -> Result<AlterTableStmt> {
        self.expect(TokenType::Alter)?;
        self.expect(TokenType::Table)?;

        let table = self.parse_identifier()?;

        // Branch on ADD vs AUTO_INCREMENT
        if matches!(self.current().token_type, TokenType::Add) {
            self.advance();
            // Optional COLUMN keyword (parsed as identifier since it's not a
            // registered keyword — avoids breaking USING COLUMN in CREATE INDEX).
            if let TokenType::Identifier(name) = &self.current().token_type {
                if name.eq_ignore_ascii_case("COLUMN") {
                    self.advance();
                }
            }
            let col_name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;
            // Optional DEFAULT value
            let default_value = if matches!(self.current().token_type, TokenType::Default) {
                self.advance();
                // Parse a literal: number, string, true/false, null
                let val = match &self.current().token_type {
                    TokenType::Number(n) => {
                        let f = *n;
                        let v = if f.fract() == 0.0 && f.abs() < i64::MAX as f64 {
                            crate::types::Value::Integer(f as i64)
                        } else {
                            crate::types::Value::Float(f)
                        };
                        self.advance();
                        v
                    }
                    TokenType::String(s) => {
                        let v = crate::types::Value::Text(s.clone().into());
                        self.advance();
                        v
                    }
                    TokenType::True => { self.advance(); crate::types::Value::Bool(true) }
                    TokenType::False => { self.advance(); crate::types::Value::Bool(false) }
                    TokenType::Null => { self.advance(); crate::types::Value::Null }
                    _ => return Err(self.error("Expected literal value for DEFAULT")),
                };
                Some(val)
            } else {
                None
            };
            Ok(AlterTableStmt {
                table,
                action: AlterTableAction::AddColumn {
                    name: col_name,
                    data_type,
                    default_value,
                },
            })
        } else {
            // AUTO_INCREMENT = value
            self.expect(TokenType::AutoIncrement)?;
            self.expect(TokenType::Eq)?;

            let value = match &self.current().token_type {
                TokenType::Number(n) => {
                    let f = *n;
                    if f < 0.0 || f > i64::MAX as f64 || f.fract() != 0.0 {
                        return Err(self.error("AUTO_INCREMENT value must be a non-negative integer"));
                    }
                    let value = f as i64;
                    self.advance();
                    value
                }
                _ => return Err(self.error("Expected integer value for AUTO_INCREMENT")),
            };

            Ok(AlterTableStmt {
                table,
                action: AlterTableAction::SetAutoIncrement(value),
            })
        }
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
            Statement::Select { stmt: s, .. } => {
                match &s.from {
                    Some(TableRef::Table { name, .. }) => assert_eq!(name, "users"),
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
            Statement::Select { stmt: s, .. } => {
                match &s.from {
                    Some(TableRef::Table { name, .. }) => assert_eq!(name, "users"),
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
