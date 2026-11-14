//! ClickHouse's types as SpelaionDB's logical types, and a result's columns built from the
//! server's text.
//!
//! What a value looks like is always the server's own text, except for the types the core
//! keeps as numbers or dates:
//!
//! - integers up to 64 bits signed (and 32 unsigned) are `Int`; `UInt64` and the 128- and
//!   256-bit integers do not fit and are `Decimal`, kept as the server's text;
//! - `Float32`, `Float64` and `BFloat16` are `Float`: the same text for every value the server
//!   writes without an exponent; infinities, NaN and exponents are written the core's way;
//! - `Date` and `Date32` are `Date`, the same text;
//! - `Bool` is `Bool` (`true`, `false`), `UUID` is `Uuid`, `Decimal(…)` is `Decimal`, `String`
//!   and `FixedString(…)` are `Text`, `JSON` is `Json`, `Array(…)` is `Array`;
//! - everything else (`DateTime`, `DateTime64`, `Enum`, `Tuple`, `Map`, `IPv4`, `IPv6`,
//!   intervals, geometry…) is `Native`, the server's text with its type.
//!
//! `Nullable(…)` and `LowCardinality(…)` are the type they wrap.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// The type a `Nullable(…)` or `LowCardinality(…)` wraps, unwrapped all the way.
pub fn unwrapped(native: &str) -> &str {
    let mut t = native.trim();
    loop {
        let inner = ["Nullable(", "LowCardinality("]
            .iter()
            .find_map(|prefix| t.strip_prefix(prefix).and_then(|r| r.strip_suffix(')')));
        match inner {
            Some(inner) => t = inner.trim(),
            None => return t,
        }
    }
}

/// The logical type of a ClickHouse type, as `TabSeparatedWithNamesAndTypes` names it.
pub fn logical(native: &str) -> LogicalType {
    let t = unwrapped(native);
    match t {
        "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" => LogicalType::Int,
        "UInt64" | "Int128" | "UInt128" | "Int256" | "UInt256" => LogicalType::Decimal,
        "Float32" | "Float64" | "BFloat16" => LogicalType::Float,
        "Bool" => LogicalType::Bool,
        "Date" | "Date32" => LogicalType::Date,
        "String" => LogicalType::Text,
        "UUID" => LogicalType::Uuid,
        "JSON" => LogicalType::Json,
        _ if t.starts_with("FixedString(") => LogicalType::Text,
        _ if t.starts_with("Decimal") => LogicalType::Decimal,
        _ if t.starts_with("JSON(") => LogicalType::Json,
        _ if t.starts_with("Array(") => LogicalType::Array,
        _ => LogicalType::Native,
    }
}

/// Whether `TabSeparated` writes this type's text as it is, without escaping it: arrays,
/// tuples and maps (and the geometry types made of them) write their own quoted text, whose
/// strings are already escaped and hold no tab or line end. Every other type's text is escaped.
pub fn written_raw(native: &str) -> bool {
    let t = unwrapped(native);
    ["Array(", "Tuple(", "Map(", "Nested("]
        .iter()
        .any(|prefix| t.starts_with(prefix))
        || matches!(
            t,
            "Point" | "Ring" | "LineString" | "MultiLineString" | "Polygon" | "MultiPolygon"
        )
}

/// A result's columns, as the header of `TabSeparatedWithNamesAndTypes` gives them.
pub fn columns(names: Vec<String>, types: Vec<String>) -> Result<Vec<ColumnMeta>> {
    if names.len() != types.len() {
        return Err(DriverError::Other(format!(
            "the server sent {} column names and {} types",
            names.len(),
            types.len()
        )));
    }
    Ok(names
        .into_iter()
        .zip(types)
        .map(|(name, native_type)| ColumnMeta {
            name,
            logical: logical(&native_type),
            native_type,
            native_id: None,
            origin: None,
        })
        .collect())
}

enum Builder {
    Int(Int64Builder),
    Float(Float64Builder),
    Bool(BooleanBuilder),
    Date(Date32Builder),
    Text(StringBuilder),
}

/// The rows of one result, a batch at a time.
pub struct Batches {
    schema: Arc<ResultSchema>,
    builders: Vec<Builder>,
    rows: usize,
}

impl Batches {
    pub fn new(schema: Arc<ResultSchema>) -> Self {
        let mut batches = Self {
            schema,
            builders: Vec::new(),
            rows: 0,
        };
        batches.builders = batches.fresh();
        batches
    }

    fn fresh(&self) -> Vec<Builder> {
        self.schema
            .columns
            .iter()
            .map(|column| match column.logical {
                LogicalType::Int => Builder::Int(Int64Builder::new()),
                LogicalType::Float => Builder::Float(Float64Builder::new()),
                LogicalType::Bool => Builder::Bool(BooleanBuilder::new()),
                LogicalType::Date => Builder::Date(Date32Builder::new()),
                LogicalType::Decimal
                | LogicalType::Text
                | LogicalType::Bytes
                | LogicalType::Time
                | LogicalType::Timestamp
                | LogicalType::TimestampTz
                | LogicalType::Uuid
                | LogicalType::Json
                | LogicalType::Interval
                | LogicalType::Array
                | LogicalType::Native => Builder::Text(StringBuilder::new()),
            })
            .collect()
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// One row, its values as the server's text (`None` for NULL).
    pub fn push(&mut self, values: Vec<Option<String>>) -> Result<()> {
        if values.len() != self.builders.len() {
            return Err(DriverError::Other(format!(
                "a row of {} values in a result of {} columns",
                values.len(),
                self.builders.len()
            )));
        }
        for ((builder, value), column) in self
            .builders
            .iter_mut()
            .zip(values)
            .zip(&self.schema.columns)
        {
            let bad = |value: &str| DriverError::Decode {
                type_name: column.native_type.clone(),
                message: format!("{value:?} is not a value of this type"),
            };
            match (builder, value) {
                (Builder::Int(b), None) => b.append_null(),
                (Builder::Float(b), None) => b.append_null(),
                (Builder::Bool(b), None) => b.append_null(),
                (Builder::Date(b), None) => b.append_null(),
                (Builder::Text(b), None) => b.append_null(),
                (Builder::Int(b), Some(v)) => b.append_value(v.parse().map_err(|_| bad(&v))?),
                (Builder::Float(b), Some(v)) => b.append_value(v.parse().map_err(|_| bad(&v))?),
                (Builder::Bool(b), Some(v)) => b.append_value(match v.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(bad(&v)),
                }),
                (Builder::Date(b), Some(v)) => b.append_value(days(&v).ok_or_else(|| bad(&v))?),
                (Builder::Text(b), Some(v)) => b.append_value(v),
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// The rows pushed since the last batch, as one batch; `None` when there are none.
    pub fn take(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let fresh = self.fresh();
        let builders = std::mem::replace(&mut self.builders, fresh);
        self.rows = 0;
        let arrays: Vec<ArrayRef> = builders
            .into_iter()
            .map(|builder| -> ArrayRef {
                match builder {
                    Builder::Int(mut b) => Arc::new(b.finish()),
                    Builder::Float(mut b) => Arc::new(b.finish()),
                    Builder::Bool(mut b) => Arc::new(b.finish()),
                    Builder::Date(mut b) => Arc::new(b.finish()),
                    Builder::Text(mut b) => Arc::new(b.finish()),
                }
            })
            .collect();
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map(Some)
            .map_err(|e| DriverError::Other(format!("building a batch: {e}")))
    }
}

/// Days since 1970-01-01 of `YYYY-MM-DD`.
fn days(text: &str) -> Option<i32> {
    let mut parts = text.splitn(3, '-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let d: i64 = parts.next()?.parse().ok()?;
    if text.len() != 10 || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i32::try_from(era * 146_097 + doe - 719_468).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrappers_come_off() {
        assert_eq!(unwrapped("Nullable(Int32)"), "Int32");
        assert_eq!(unwrapped("LowCardinality(Nullable(String))"), "String");
        assert_eq!(unwrapped("Array(Nullable(Int8))"), "Array(Nullable(Int8))");
    }

    #[test]
    fn types_map_to_logical_types() {
        for (native, logical_type) in [
            ("Int64", LogicalType::Int),
            ("UInt32", LogicalType::Int),
            ("UInt64", LogicalType::Decimal),
            ("Int256", LogicalType::Decimal),
            ("Nullable(Float32)", LogicalType::Float),
            ("Bool", LogicalType::Bool),
            ("Date32", LogicalType::Date),
            ("LowCardinality(String)", LogicalType::Text),
            ("FixedString(3)", LogicalType::Text),
            ("Decimal(10, 2)", LogicalType::Decimal),
            ("UUID", LogicalType::Uuid),
            ("JSON", LogicalType::Json),
            ("Array(String)", LogicalType::Array),
            ("DateTime('UTC')", LogicalType::Native),
            ("DateTime64(3)", LogicalType::Native),
            ("Enum8('a' = 1)", LogicalType::Native),
            ("Map(String, UInt8)", LogicalType::Native),
            ("IPv4", LogicalType::Native),
        ] {
            assert_eq!(logical(native), logical_type, "{native}");
        }
    }

    #[test]
    fn nested_types_are_written_as_they_are() {
        for native in [
            "Array(String)",
            "Tuple(UInt8, String)",
            "Map(String, UInt8)",
            "Point",
            "Ring",
        ] {
            assert!(written_raw(native), "{native}");
        }
        for native in [
            "String",
            "JSON",
            "Variant(String, UInt8)",
            "Nullable(String)",
            "Enum8('a' = 1)",
        ] {
            assert!(!written_raw(native), "{native}");
        }
    }

    #[test]
    fn dates_count_days_from_1970() {
        assert_eq!(days("1970-01-01"), Some(0));
        assert_eq!(days("1970-01-02"), Some(1));
        assert_eq!(days("1969-12-31"), Some(-1));
        assert_eq!(days("2000-03-01"), Some(11_017));
        assert_eq!(days("1900-01-01"), Some(-25_567));
        assert_eq!(days("2299-12-31"), Some(120_529));
        assert_eq!(days("2024-13-01"), None);
        assert_eq!(days("x"), None);
    }
}
