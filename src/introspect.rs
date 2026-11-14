//! What the database holds, from ClickHouse's `system` tables. Names travel as bound
//! parameters (`{db:String}`), never written into the query.
//!
//! ClickHouse's databases are the explorer's schemas. A table's primary key is its sort order,
//! not unique: it is shown as an index, never as a primary key, so nothing pages or edits rows
//! by it.

use spelaion_driver_api::{
    ColumnInfo, DriverError, IndexInfo, ObjectInfo, ObjectKind, Result, SchemaColumn, SchemaInfo,
    TableInfo,
};

use crate::ClickhouseConnection;

/// The databases ClickHouse keeps for itself.
const SYSTEM_DATABASES: [&str; 3] = ["system", "INFORMATION_SCHEMA", "information_schema"];

fn text(row: &[Option<String>], i: usize) -> String {
    row.get(i).cloned().flatten().unwrap_or_default()
}

fn optional(row: &[Option<String>], i: usize) -> Option<String> {
    row.get(i).cloned().flatten().filter(|s| !s.is_empty())
}

pub async fn list_schemas(conn: &ClickhouseConnection) -> Result<Vec<SchemaInfo>> {
    let rows = conn
        .rows("SELECT name FROM system.databases ORDER BY name", &[])
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let name = text(row, 0);
            SchemaInfo {
                system: SYSTEM_DATABASES.contains(&name.as_str()),
                name,
            }
        })
        .collect())
}

fn kind(engine: &str) -> ObjectKind {
    match engine {
        "View" | "LiveView" | "WindowView" => ObjectKind::View,
        "MaterializedView" => ObjectKind::MaterializedView,
        // Tables whose rows are somewhere else: another server, files, a bucket.
        "MySQL" | "PostgreSQL" | "MongoDB" | "ODBC" | "JDBC" | "URL" | "File" | "S3" | "HDFS"
        | "AzureBlobStorage" | "Kafka" | "RabbitMQ" | "NATS" | "Redis" | "SQLite" => {
            ObjectKind::ForeignTable
        }
        _ => ObjectKind::Table,
    }
}

pub async fn list_objects(conn: &ClickhouseConnection, schema: &str) -> Result<Vec<ObjectInfo>> {
    let rows = conn
        .rows(
            "SELECT name, engine, total_rows, comment FROM system.tables \
             WHERE database = {db:String} AND NOT is_temporary ORDER BY name",
            &[("db", schema)],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| ObjectInfo {
            schema: schema.to_string(),
            name: text(row, 0),
            kind: kind(&text(row, 1)),
            estimated_rows: row.get(2).cloned().flatten().and_then(|n| n.parse().ok()),
            comment: optional(row, 3),
        })
        .collect())
}

pub async fn describe_table(
    conn: &ClickhouseConnection,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    let params = [("db", schema), ("t", name)];
    let table = conn
        .rows(
            "SELECT engine, total_rows, comment, sorting_key, primary_key, partition_key \
             FROM system.tables WHERE database = {db:String} AND name = {t:String}",
            &params,
        )
        .await?;
    let Some(table) = table.first() else {
        return Err(DriverError::Other(format!(
            "there is no table {schema}.{name}"
        )));
    };
    let columns = conn
        .rows(
            "SELECT name, type, default_kind, default_expression, comment FROM system.columns \
             WHERE database = {db:String} AND table = {t:String} ORDER BY position",
            &params,
        )
        .await?;
    let columns = columns
        .iter()
        .map(|row| {
            let native_type = text(row, 1);
            let default = match (text(row, 2).as_str(), optional(row, 3)) {
                (_, None) => None,
                ("DEFAULT" | "", Some(expr)) => Some(expr),
                (kind, Some(expr)) => Some(format!("{kind} {expr}")),
            };
            ColumnInfo {
                name: text(row, 0),
                nullable: native_type.starts_with("Nullable("),
                native_type,
                default,
                comment: optional(row, 4),
            }
        })
        .collect();

    let mut indexes = Vec::new();
    for (key, label, clause) in [
        (3, "sorting key", "ORDER BY"),
        (4, "primary key", "PRIMARY KEY"),
        (5, "partition key", "PARTITION BY"),
    ] {
        if let Some(expr) = optional(table, key) {
            indexes.push(IndexInfo {
                name: label.into(),
                unique: false,
                primary: false,
                definition: format!("{clause} {expr}"),
            });
        }
    }
    let skipping = conn
        .rows(
            "SELECT name, expr, type_full, granularity FROM system.data_skipping_indices \
             WHERE database = {db:String} AND table = {t:String} ORDER BY name",
            &params,
        )
        .await?;
    for row in &skipping {
        let index = text(row, 0);
        indexes.push(IndexInfo {
            definition: format!(
                "INDEX {} {} TYPE {} GRANULARITY {}",
                crate::quote(&index),
                text(row, 1),
                text(row, 2),
                text(row, 3)
            ),
            name: index,
            unique: false,
            primary: false,
        });
    }

    Ok(TableInfo {
        object: ObjectInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: kind(&text(table, 0)),
            estimated_rows: table.get(1).cloned().flatten().and_then(|n| n.parse().ok()),
            comment: optional(table, 2),
        },
        columns,
        primary_key: Vec::new(),
        indexes,
        foreign_keys: Vec::new(),
        constraints: Vec::new(),
    })
}

pub async fn list_columns(conn: &ClickhouseConnection, schema: &str) -> Result<Vec<SchemaColumn>> {
    let rows = conn
        .rows(
            "SELECT table, name, type FROM system.columns WHERE database = {db:String} \
             ORDER BY table, position",
            &[("db", schema)],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| SchemaColumn {
            table: text(row, 0),
            name: text(row, 1),
            native_type: text(row, 2),
        })
        .collect())
}

pub async fn relation_definition(
    conn: &ClickhouseConnection,
    schema: &str,
    name: &str,
) -> Result<String> {
    let rows = conn
        .rows(
            "SELECT create_table_query FROM system.tables \
             WHERE database = {db:String} AND name = {t:String}",
            &[("db", schema), ("t", name)],
        )
        .await?;
    rows.first()
        .and_then(|row| optional(row, 0))
        .map(|sql| spelaion_driver_api::terminated(&sql))
        .ok_or_else(|| DriverError::Other(format!("there is no table {schema}.{name}")))
}
