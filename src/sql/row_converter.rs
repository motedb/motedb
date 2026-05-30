/// Row conversion utilities - converts between storage Row and SQL SqlRow
use crate::types::{Row, SqlRow, Value, TableSchema, ColumnType};
use crate::error::Result;

/// Convert storage Row (Vec<Value>) to SQL SqlRow (HashMap<String, Value>)
pub fn row_to_sql_row(row: &Row, schema: &TableSchema) -> Result<SqlRow> {
    let mut sql_row = SqlRow::new();
    
    for (i, col_def) in schema.columns.iter().enumerate() {
        let value = row.get(i).cloned().unwrap_or(Value::Null);
        sql_row.insert(col_def.name.clone(), value);
    }
    
    Ok(sql_row)
}

/// Convert SQL SqlRow (HashMap<String, Value>) to storage Row (Vec<Value>)
pub fn sql_row_to_row(sql_row: &SqlRow, schema: &TableSchema) -> Result<Row> {
    let mut row = Vec::with_capacity(schema.columns.len());

    for col_def in &schema.columns {
        let value = sql_row
            .get(&col_def.name)
            .cloned()
            .unwrap_or(Value::Null);

        // Enforce NOT NULL constraint (skip for AUTO_INCREMENT — system fills value)
        if !col_def.nullable && !col_def.auto_increment && matches!(value, Value::Null) {
            return Err(crate::error::MoteDBError::InvalidArgument(
                format!("Column '{}' cannot be null", col_def.name)
            ));
        }

        // Type coercion for INSERT statements
        let coerced_value = match (&col_def.col_type, &value) {
            // Integer to Timestamp conversion
            (ColumnType::Timestamp, Value::Integer(i)) => {
                use crate::types::Timestamp;
                Value::Timestamp(Timestamp::from_micros(*i))
            }
            // Integer to Float conversion
            (ColumnType::Float, Value::Integer(i)) => Value::Float(*i as f64),
            // Pass through
            _ => value,
        };

        row.push(coerced_value);
    }

    Ok(row)
}

/// Convert a batch of storage rows to SQL rows
pub fn rows_to_sql_rows(rows: Vec<(u64, Row)>, schema: &TableSchema) -> Result<Vec<(u64, SqlRow)>> {
    rows.into_iter()
        .map(|(row_id, row)| {
            row_to_sql_row(&row, schema).map(|sql_row| (row_id, sql_row))
        })
        .collect()
}

/// Build a storage Row directly from resolved values, using column names to map into schema order.
/// Skips the HashMap intermediary when the caller has already resolved expressions to Values.
pub fn values_to_row_by_columns(
    values: &[Value],
    columns: &[String],
    schema: &TableSchema,
) -> Result<Row> {
    let mut row = vec![Value::Null; schema.columns.len()];

    for (i, col_name) in columns.iter().enumerate() {
        let val = values.get(i).cloned().unwrap_or(Value::Null);
        // Find the column position in schema
        if let Some(col_def) = schema.get_column(col_name) {
            // Skip AUTO_INCREMENT columns — system fills them
            if col_def.auto_increment {
                continue;
            }
            // Enforce NOT NULL
            if !col_def.nullable && matches!(val, Value::Null) {
                return Err(crate::error::MoteDBError::InvalidArgument(
                    format!("Column '{}' cannot be null", col_name)
                ));
            }
            // Type coercion
            let coerced = match (&col_def.col_type, &val) {
                (ColumnType::Timestamp, Value::Integer(ts)) => {
                    Value::Timestamp(crate::types::Timestamp::from_micros(*ts))
                }
                (ColumnType::Float, Value::Integer(i)) => Value::Float(*i as f64),
                _ => val,
            };
            row[col_def.position] = coerced;
        }
    }

    Ok(row)
}

/// Build a storage Row directly from values already in schema order.
/// Used by fast INSERT path where column list matches schema exactly.
pub fn values_to_row_schema_order(
    values: &[Value],
    schema: &TableSchema,
) -> Result<Row> {
    let mut row = Vec::with_capacity(schema.columns.len());

    for (i, col_def) in schema.columns.iter().enumerate() {
        if col_def.auto_increment {
            row.push(Value::Null);
            continue;
        }
        let val = values.get(i).cloned().unwrap_or(Value::Null);
        // Enforce NOT NULL
        if !col_def.nullable && matches!(val, Value::Null) {
            return Err(crate::error::MoteDBError::InvalidArgument(
                format!("Column '{}' cannot be null", col_def.name)
            ));
        }
        // Type coercion
        let coerced = match (&col_def.col_type, &val) {
            (ColumnType::Timestamp, Value::Integer(ts)) => {
                Value::Timestamp(crate::types::Timestamp::from_micros(*ts))
            }
            (ColumnType::Float, Value::Integer(i)) => Value::Float(*i as f64),
            _ => val,
        };
        row.push(coerced);
    }

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, ColumnType, ArcString};
    use std::sync::Arc;

    #[test]
    fn test_row_to_sql_row() {
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), ColumnType::Integer, 0),
                ColumnDef::new("name".to_string(), ColumnType::Text, 1),
            ],
        );
        
        let row = vec![
            Value::Integer(1),
            Value::Text(ArcString(Arc::from("Alice"))),
        ];

        let sql_row = row_to_sql_row(&row, &schema).unwrap();

        assert_eq!(sql_row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(sql_row.get("name"), Some(&Value::Text(ArcString(Arc::from("Alice")))));
    }

    #[test]
    fn test_sql_row_to_row() {
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), ColumnType::Integer, 0),
                ColumnDef::new("name".to_string(), ColumnType::Text, 1),
            ],
        );

        let mut sql_row = SqlRow::new();
        sql_row.insert("id".to_string(), Value::Integer(1));
        sql_row.insert("name".to_string(), Value::Text(ArcString(Arc::from("Alice"))));

        let row = sql_row_to_row(&sql_row, &schema).unwrap();

        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::Text(ArcString(Arc::from("Alice"))));
    }

    #[test]
    fn test_round_trip() {
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnDef::new("id".to_string(), ColumnType::Integer, 0),
                ColumnDef::new("name".to_string(), ColumnType::Text, 1),
                ColumnDef::new("age".to_string(), ColumnType::Integer, 2),
            ],
        );
        
        let original_row = vec![
            Value::Integer(42),
            Value::Text(ArcString(Arc::from("Bob"))),
            Value::Integer(30),
        ];
        
        let sql_row = row_to_sql_row(&original_row, &schema).unwrap();
        let converted_row = sql_row_to_row(&sql_row, &schema).unwrap();
        
        assert_eq!(original_row, converted_row);
    }
}
