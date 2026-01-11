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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, ColumnType};

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
            Value::Text("Alice".to_string()),
        ];
        
        let sql_row = row_to_sql_row(&row, &schema).unwrap();
        
        assert_eq!(sql_row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(sql_row.get("name"), Some(&Value::Text("Alice".to_string())));
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
        sql_row.insert("name".to_string(), Value::Text("Alice".to_string()));
        
        let row = sql_row_to_row(&sql_row, &schema).unwrap();
        
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::Text("Alice".to_string()));
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
            Value::Text("Bob".to_string()),
            Value::Integer(30),
        ];
        
        let sql_row = row_to_sql_row(&original_row, &schema).unwrap();
        let converted_row = sql_row_to_row(&sql_row, &schema).unwrap();
        
        assert_eq!(original_row, converted_row);
    }
}
