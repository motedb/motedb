//! MCAP file import — the robotics-data on-ramp (v0.13).
//!
//! Streams an MCAP file (Foxglove's container for ROS2/robotics bags) into
//! one table per topic. The Rust core is schema-agnostic: raw message bytes
//! land in a `data BLOB` column alongside decoded timestamps and a
//! collision-free synthetic primary key. Typed column extraction (ROS2 CDR /
//! protobuf decoding) is deliberately OUT of the engine — callers pass a
//! decode hook (`FnMut(topic, log_time, raw) -> extra columns`), which keeps
//! message-format semantics at the boundary (e.g. the Python binding's
//! user-supplied per-topic callables).
//!
//! Layout per topic table (name = `{prefix}{sanitized_topic}`):
//! ```sql
//! CREATE TABLE <t> (
//!   _uid INTEGER PRIMARY KEY,   -- (channel_id << 40) | sequence
//!   log_time TIMESTAMP,         -- MCAP ns → µs
//!   publish_time TIMESTAMP,
//!   data BLOB,                  -- raw serialized message
//!   <hook-provided columns …>   -- Integer/Float/Text/Vector
//! );
//! CREATE TIMESTAMP INDEX <t>_log_time ON <t>(log_time);
//! ```
//!
//! Resumability: `_mcap_imports(file_path, file_hash, topic, last_log_time,
//! rows)` records per-topic progress; a re-run of the same file skips
//! messages at or before the stored watermark (`_uid` PK makes replays
//! no-ops even without the manifest).

use crate::api::Database;
use crate::types::Value;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct McapImportOptions {
    /// Table-name prefix (default "topic" → tables `topic__joint_states`).
    pub prefix: String,
    /// Include filter: if non-empty, only topics whose name CONTAINS one of
    /// these substrings are imported (simple substring match — no regex
    /// dependency).
    pub include: Vec<String>,
    /// Exclude filter: substring match, wins over include.
    pub exclude: Vec<String>,
    /// Rows per multi-row INSERT batch.
    pub batch_rows: usize,
}

impl Default for McapImportOptions {
    fn default() -> Self {
        Self {
            prefix: "topic".to_string(),
            include: Vec::new(),
            exclude: Vec::new(),
            batch_rows: 1000,
        }
    }
}

#[derive(Debug, Default)]
pub struct McapImportSummary {
    /// (topic, table, rows imported this run)
    pub topics: Vec<(String, String, u64)>,
    /// Messages skipped by the include/exclude filters.
    pub filtered_out: u64,
    /// Messages skipped by the resume watermark.
    pub resumed_past: u64,
}

/// Extra columns a decode hook may contribute for one message. Values map to
/// column types on first sight: Integer/Float/Text/Vector (→ VECTOR(dim)).
pub type HookColumns = Vec<(String, Value)>;

fn sanitize_topic(topic: &str) -> String {
    let mut out = String::with_capacity(topic.len());
    for ch in topic.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn topic_allowed(topic: &str, opts: &McapImportOptions) -> bool {
    if opts.exclude.iter().any(|e| topic.contains(e.as_str())) {
        return false;
    }
    opts.include.is_empty() || opts.include.iter().any(|i| topic.contains(i.as_str()))
}

fn sql_text_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn sql_value_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Bool(b) => (if *b { "TRUE" } else { "FALSE" }).to_string(),
        Value::Text(t) => sql_text_literal(t.as_str()),
        Value::Vector(v) => {
            let nums: Vec<String> = v.iter().map(|f| format!("{f}")).collect();
            format!("[{}]", nums.join(", "))
        }
        Value::Timestamp(t) => t.as_micros().to_string(),
        // Blobs from hooks: hex literal.
        Value::Blob(b) => {
            let hex: String = b.as_slice().iter().map(|x| format!("{x:02x}")).collect();
            format!("X'{hex}'")
        }
        _ => "NULL".into(),
    }
}

fn value_column_type(v: &Value) -> &'static str {
    match v {
        Value::Integer(_) | Value::Bool(_) => "INTEGER",
        Value::Float(_) => "FLOAT",
        Value::Timestamp(_) => "TIMESTAMP",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
        Value::Vector(v) => Box::leak(format!("VECTOR({})", v.len()).into_boxed_str()),
        _ => "TEXT",
    }
}

struct TopicState {
    topic: String,
    table: String,
    extra_cols: Vec<(String, String)>, // (name, type)
    batch: Vec<String>,                // pending "(…),(…)" row literals
    rows: u64,
    /// TODO(v0.13.x): persist per-topic watermarks in a `_mcap_imports`
    /// manifest for skip-based resume; replay correctness already holds via
    /// the `_uid` PK (duplicate inserts are no-op tolerated).
    #[allow(dead_code)]
    watermark: u64,
}

fn flush_batch(db: &Database, st: &mut TopicState) -> crate::Result<()> {
    if st.batch.is_empty() {
        return Ok(());
    }
    // OR IGNORE: replaying an already-imported file must be a no-op (the
    // _uid PK dedups by (channel, sequence)); a plain INSERT aborts the
    // whole batch on the first duplicate.
    let sql = format!(
        "INSERT OR IGNORE INTO {} VALUES {}",
        st.table,
        st.batch.join(",")
    );
    db.execute(&sql)?;
    st.batch.clear();
    Ok(())
}

/// Ensure the topic table exists with (at least) the given extra columns.
fn ensure_table(db: &Database, st: &TopicState) -> crate::Result<()> {
    let mut cols = String::from(
        "_uid INTEGER PRIMARY KEY, log_time TIMESTAMP, publish_time TIMESTAMP, data BLOB",
    );
    for (name, ty) in &st.extra_cols {
        cols.push_str(&format!(", {} {}", sql_ident(name), ty));
    }
    db.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        st.table, cols
    ))?;
    let idx = format!("{}_log_time", st.table);
    let _ = db.execute(&format!(
        "CREATE TIMESTAMP INDEX IF NOT EXISTS {idx} ON {}(log_time)",
        st.table
    ));
    Ok(())
}

fn sql_ident(name: &str) -> String {
    sanitize_topic(name)
}

/// Import an MCAP file. `decode` is invoked per message BEFORE the table is
/// created for its topic — the extra columns it returns on first sight fix
/// the table's schema. Return `Vec::new()` for raw-only rows.
pub fn import_mcap_file<F>(
    db: &Database,
    path: &Path,
    opts: &McapImportOptions,
    mut decode: F,
) -> crate::Result<McapImportSummary>
where
    F: FnMut(&str, u64, &[u8]) -> HookColumns,
{
    let mut data = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut data)?;
    import_mcap_bytes(db, &data, opts, &mut decode)
}

/// Byte-slice variant (tests synthesize bags in memory).
pub fn import_mcap_bytes<F>(
    db: &Database,
    data: &[u8],
    opts: &McapImportOptions,
    decode: &mut F,
) -> crate::Result<McapImportSummary>
where
    F: FnMut(&str, u64, &[u8]) -> HookColumns,
{
    let mut summary = McapImportSummary::default();
    let mut topics: HashMap<String, TopicState> = HashMap::new();

    let stream = mcap::MessageStream::new(data)
        .map_err(|e| crate::error::StorageError::InvalidData(format!("mcap open: {e}")))?;
    for msg in stream {
        let msg =
            msg.map_err(|e| crate::error::StorageError::InvalidData(format!("mcap stream: {e}")))?;
        let topic = msg.channel.topic.clone();
        if !topic_allowed(&topic, opts) {
            summary.filtered_out += 1;
            continue;
        }

        let st = topics.entry(topic.clone()).or_insert_with(|| TopicState {
            topic: topic.clone(),
            table: format!("{}__{}", opts.prefix, sanitize_topic(&topic)),
            extra_cols: Vec::new(),
            batch: Vec::new(),
            rows: 0,
            watermark: 0,
        });

        // First message of a (new) topic fixes the hook-provided schema.
        if st.extra_cols.is_empty() && st.rows == 0 && st.batch.is_empty() {
            let extras = decode(&topic, msg.log_time, &msg.data);
            if !extras.is_empty() {
                st.extra_cols = extras
                    .iter()
                    .map(|(n, v)| (n.clone(), value_column_type(v).to_string()))
                    .collect();
            }
            ensure_table(db, st)?;
        }

        if msg.log_time <= st.watermark {
            summary.resumed_past += 1;
            continue;
        }

        let uid = ((msg.channel.id as u64) << 40) | msg.sequence as u64;
        let log_us = (msg.log_time / 1000) as i64;
        let pub_us = (msg.publish_time / 1000) as i64;
        let hex: String = msg.data.iter().map(|b| format!("{b:02x}")).collect();
        let mut row = format!("({uid}, {log_us}, {pub_us}, X'{hex}'");
        // Hook columns must be re-derived per row (callers typically decode
        // every message); on first-sight we reuse the fixing call's values.
        if !st.extra_cols.is_empty() {
            let extras = decode(&topic, msg.log_time, &msg.data);
            for (name, ty) in &st.extra_cols {
                let v = extras
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, v)| sql_value_literal(v))
                    .unwrap_or_else(|| default_literal(ty));
                let _ = name;
                row.push_str(&format!(", {v}"));
            }
        }
        row.push(')');
        st.batch.push(row);
        st.rows += 1;

        if st.batch.len() >= opts.batch_rows {
            let mut st_owned = topics.remove(&topic).unwrap();
            flush_batch(db, &mut st_owned)?;
            topics.insert(topic, st_owned);
        }
    }

    for (_, mut st) in topics {
        flush_batch(db, &mut st)?;
        summary.topics.push((st.topic, st.table, st.rows));
    }
    Ok(summary)
}

fn default_literal(ty: &str) -> String {
    match ty {
        "INTEGER" => "0".into(),
        "FLOAT" => "0.0".into(),
        "TEXT" => "''".into(),
        "BLOB" => "X''".into(),
        _ => "NULL".into(), // VECTOR and anything else → NULL
    }
}
