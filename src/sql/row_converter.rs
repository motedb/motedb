use crate::error::Result;
/// Row conversion utilities - converts between storage Row and SQL SqlRow
use crate::types::{ColumnType, Row, SqlRow, TableSchema, Value};

/// Convert storage Row (Vec<Value>) to SQL SqlRow (HashMap<String, Value>)
pub fn row_to_sql_row(row: &Row, schema: &TableSchema) -> Result<SqlRow> {
    let mut sql_row = SqlRow::with_capacity(schema.columns.len());
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
        let value = sql_row.get(&col_def.name).cloned().unwrap_or(Value::Null);

        // Enforce NOT NULL constraint (skip for AUTO_INCREMENT — system fills value)
        if !col_def.nullable && !col_def.auto_increment && matches!(value, Value::Null) {
            return Err(crate::error::MoteDBError::InvalidArgument(format!(
                "Column '{}' cannot be null",
                col_def.name
            )));
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
    // Pre-extract column names (avoid cloning String per row per column).
    let col_names: Vec<&String> = schema.columns.iter().map(|c| &c.name).collect();
    rows.into_iter()
        .map(|(row_id, row)| {
            let mut sql_row = SqlRow::with_capacity(col_names.len());
            for (i, name) in col_names.iter().enumerate() {
                let value = row.get(i).cloned().unwrap_or(Value::Null);
                sql_row.insert((*name).clone(), value);
            }
            Ok((row_id, sql_row))
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
                return Err(crate::error::MoteDBError::InvalidArgument(format!(
                    "Column '{}' cannot be null",
                    col_name
                )));
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
pub fn values_to_row_schema_order(values: &[Value], schema: &TableSchema) -> Result<Row> {
    let mut row = Vec::with_capacity(schema.columns.len());

    for (i, col_def) in schema.columns.iter().enumerate() {
        if col_def.auto_increment {
            // 🔑 If the user provided an explicit value for the AUTO_INCREMENT
            // column, keep it (e.g. INSERT INTO t VALUES (100, 'x') on an
            // AUTO_INCREMENT PK table should store id=100, not NULL).
            let val = values.get(i).cloned().unwrap_or(Value::Null);
            if !matches!(val, Value::Null) {
                row.push(val);
            } else {
                row.push(Value::Null);
            }
            continue;
        }
        let val = values.get(i).cloned().unwrap_or(Value::Null);
        // Enforce NOT NULL
        if !col_def.nullable && matches!(val, Value::Null) {
            return Err(crate::error::MoteDBError::InvalidArgument(format!(
                "Column '{}' cannot be null",
                col_def.name
            )));
        }
        // Type coercion
        let coerced = match (&col_def.col_type, &val) {
            (ColumnType::Timestamp, Value::Integer(ts)) => {
                Value::Timestamp(crate::types::Timestamp::from_micros(*ts))
            }
            (ColumnType::Timestamp, Value::Text(s)) => {
                // Parse ISO 8601 date/datetime strings into microseconds.
                // Supported: "2024-01-15", "2024-01-15 10:30:00", "2024-01-15T10:30:00"
                parse_datetime(s.as_str()).unwrap_or_else(|| val.clone())
            }
            (ColumnType::Float, Value::Integer(i)) => Value::Float(*i as f64),
            _ => val,
        };
        row.push(coerced);
    }

    Ok(row)
}

/// Parse an ISO 8601 date/datetime string into a Timestamp (microseconds since epoch).
/// Supports: "2024-01-15", "2024-01-15 10:30:00", "2024-01-15T10:30:00".
/// Returns None if the string doesn't match a known format (caller falls back).
fn parse_datetime(s: &str) -> Option<Value> {
    use crate::types::Timestamp;
    // Try parsing as integer microseconds first (numeric timestamp).
    if let Ok(micros) = s.parse::<i64>() {
        return Some(Value::Timestamp(Timestamp::from_micros(micros)));
    }
    // Split date and optional time.
    let (date_part, time_part) = if let Some(idx) = s.find(['T', ' ']) {
        (&s[..idx], Some(&s[idx + 1..]))
    } else {
        (s, None)
    };
    // Parse date: YYYY-MM-DD
    let dparts: Vec<&str> = date_part.split('-').collect();
    if dparts.len() != 3 { return None; }
    let year: i32 = dparts[0].parse().ok()?;
    let month: u32 = dparts[1].parse().ok()?;
    let day: u32 = dparts[2].parse().ok()?;
    if month < 1 || month > 12 || day < 1 || day > 31 { return None; }
    // Parse time: HH:MM:SS (seconds optional)
    let (hour, min, sec) = if let Some(tp) = time_part {
        let tparts: Vec<&str> = tp.split(':').collect();
        let h: u32 = tparts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let m: u32 = tparts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let s: u32 = tparts.get(2).and_then(|s| s.split('.').next().and_then(|n| n.parse().ok())).unwrap_or(0);
        (h, m, s)
    } else {
        (0, 0, 0)
    };
    // Convert to Unix epoch microseconds using Howard Hinnant's algorithm.
    let days = days_from_civil(year, month, day)?;
    let micros = days as i64 * 86_400_000_000 + hour as i64 * 3_600_000_000 + min as i64 * 60_000_000 + sec as i64 * 1_000_000;
    Some(Value::Timestamp(Timestamp::from_micros(micros)))
}

/// Convert civil date to days since Unix epoch (1970-01-01).
fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i64> {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = m as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era as i64 * 146097 + doe as i64 - 719468)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ArcString, ColumnDef, ColumnType};
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
        assert_eq!(
            sql_row.get("name"),
            Some(&Value::Text(ArcString(Arc::from("Alice"))))
        );
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
        sql_row.insert(
            "name".to_string(),
            Value::Text(ArcString(Arc::from("Alice"))),
        );

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
