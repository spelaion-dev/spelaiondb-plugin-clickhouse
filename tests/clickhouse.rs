//! Tests against a real ClickHouse server.
//!
//! They need `SPELAION_TEST_CLICKHOUSE=host:port` (HTTP) and, for TLS,
//! `SPELAION_TEST_CLICKHOUSE_HTTPS=host:port` (user, password and database: `spelaion`).
//! Locally: `ci/services.sh` and
//! `SPELAION_TEST_CLICKHOUSE=127.0.0.1:18123 SPELAION_TEST_CLICKHOUSE_HTTPS=127.0.0.1:18443 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, InsertTarget,
    LogicalType, ObjectKind, ResultSchema, Secret, TextRows, TlsMode, Value,
};
use spelaion_driver_clickhouse::ClickhouseDriver;

fn addr(variable: &str) -> Option<(String, u16)> {
    let Ok(addr) = std::env::var(variable) else {
        eprintln!("{variable} not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.to_string(), port.parse().expect("port")))
}

fn params(host: String, port: u16) -> ConnectParams {
    ConnectParams {
        host,
        port,
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion")),
        database: "spelaion".into(),
        tls: TlsMode::Disable,
        ..Default::default()
    }
}

async fn connect() -> Option<Box<dyn Connection>> {
    let (host, port) = addr("SPELAION_TEST_CLICKHOUSE")?;
    Some(
        ClickhouseDriver
            .connect(&params(host, port))
            .await
            .expect("connects"),
    )
}

/// The events of one statement.
async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

/// The schema and the rows of a statement, every value read back as a cell.
async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    let mut schema = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.expect("a result"), rows)
}

async fn exec(conn: &dyn Connection, sql: &str) {
    run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
}

/// Every type, with the server's own text for the value (`toString`, what `clickhouse-client`
/// shows in its tab-separated output) next to it: the cell must read the same, and have the
/// logical type and the native type the driver gives it.
#[tokio::test]
async fn every_type_reads_as_the_server_writes_it() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    exec(conn, "SET session_timezone = 'UTC'").await;
    exec(conn, "SET allow_suspicious_low_cardinality_types = 1").await;
    let zoo: &[(&str, LogicalType, &str)] = &[
        ("toInt8(-128)", LogicalType::Int, "Int8"),
        ("toInt16(-32768)", LogicalType::Int, "Int16"),
        ("toInt32(2147483647)", LogicalType::Int, "Int32"),
        ("toInt64(-9223372036854775808)", LogicalType::Int, "Int64"),
        ("toUInt8(255)", LogicalType::Int, "UInt8"),
        ("toUInt16(65535)", LogicalType::Int, "UInt16"),
        ("toUInt32(4294967295)", LogicalType::Int, "UInt32"),
        (
            "toUInt64(18446744073709551615)",
            LogicalType::Decimal,
            "UInt64",
        ),
        (
            "toInt128('-170141183460469231731687303715884105728')",
            LogicalType::Decimal,
            "Int128",
        ),
        (
            "toUInt128('340282366920938463463374607431768211455')",
            LogicalType::Decimal,
            "UInt128",
        ),
        (
            "toInt256('-12345678901234567890123456789012345678901234567890')",
            LogicalType::Decimal,
            "Int256",
        ),
        (
            "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')",
            LogicalType::Decimal,
            "UInt256",
        ),
        ("toFloat32(0.1)", LogicalType::Float, "Float32"),
        ("toFloat64(-1234.5625)", LogicalType::Float, "Float64"),
        (
            "toFloat64(0.30000000000000004)",
            LogicalType::Float,
            "Float64",
        ),
        (
            "toDecimal32(-12.34, 2)",
            LogicalType::Decimal,
            "Decimal(9, 2)",
        ),
        (
            "toDecimal64(123456789.123456789, 9)",
            LogicalType::Decimal,
            "Decimal(18, 9)",
        ),
        (
            "toDecimal128('1.10', 4)",
            LogicalType::Decimal,
            "Decimal(38, 4)",
        ),
        (
            "toDecimal256('-0.5', 30)",
            LogicalType::Decimal,
            "Decimal(76, 30)",
        ),
        ("true", LogicalType::Bool, "Bool"),
        ("false", LogicalType::Bool, "Bool"),
        ("'plain text'", LogicalType::Text, "String"),
        (
            "'tab\there, line\nthere, back\\\\slash, quote \\' and ñ'",
            LogicalType::Text,
            "String",
        ),
        ("'\\\\N'", LogicalType::Text, "String"),
        ("''", LogicalType::Text, "String"),
        (
            "toFixedString('abcd', 4)",
            LogicalType::Text,
            "FixedString(4)",
        ),
        (
            "toLowCardinality('low')",
            LogicalType::Text,
            "LowCardinality(String)",
        ),
        ("toDate('2024-02-29')", LogicalType::Date, "Date"),
        ("toDate('1970-01-01')", LogicalType::Date, "Date"),
        ("toDate32('1900-01-01')", LogicalType::Date, "Date32"),
        ("toDate32('2299-12-31')", LogicalType::Date, "Date32"),
        (
            "toDateTime('2024-02-29 23:59:58', 'UTC')",
            LogicalType::Native,
            "DateTime('UTC')",
        ),
        (
            "toDateTime('2024-02-29 23:59:58', 'Asia/Tokyo')",
            LogicalType::Native,
            "DateTime('Asia/Tokyo')",
        ),
        (
            "toDateTime64('2024-02-29 23:59:58.123456', 6, 'UTC')",
            LogicalType::Native,
            "DateTime64(6, 'UTC')",
        ),
        (
            "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')",
            LogicalType::Uuid,
            "UUID",
        ),
        ("toIPv4('192.168.0.1')", LogicalType::Native, "IPv4"),
        ("toIPv6('2001:db8::1')", LogicalType::Native, "IPv6"),
        (
            "CAST('b', 'Enum8(\\'a\\' = 1, \\'b\\' = 2)')",
            LogicalType::Native,
            "Enum8('a' = 1, 'b' = 2)",
        ),
        (
            "CAST('x', 'Enum16(\\'x\\' = 1000)')",
            LogicalType::Native,
            "Enum16('x' = 1000)",
        ),
        ("[1, 2, 3]", LogicalType::Array, "Array(UInt8)"),
        (
            "['a', 'b\\'c', 'tab\there']",
            LogicalType::Array,
            "Array(String)",
        ),
        (
            "[[1], [], [2, 3]]",
            LogicalType::Array,
            "Array(Array(UInt8))",
        ),
        ("[NULL, 1]", LogicalType::Array, "Array(Nullable(UInt8))"),
        (
            "tuple(1, 'a', NULL)",
            LogicalType::Native,
            "Tuple(UInt8, String, Nullable(Nothing))",
        ),
        (
            "map('k', 1, 'l', 2)",
            LogicalType::Native,
            "Map(String, UInt8)",
        ),
        ("toIntervalDay(3)", LogicalType::Native, "IntervalDay"),
        ("CAST((1.5, 2.5), 'Point')", LogicalType::Native, "Point"),
        (
            "CAST([(0, 0), (1, 1)], 'Ring')",
            LogicalType::Native,
            "Ring",
        ),
        ("toNullable(42)", LogicalType::Int, "Nullable(UInt8)"),
        (
            "toLowCardinality(toNullable('lcn'))",
            LogicalType::Text,
            "LowCardinality(Nullable(String))",
        ),
        ("'{\"a\": 1}'::JSON", LogicalType::Json, "JSON"),
    ];
    for (expr, logical, native) in zoo {
        let sql = format!("SELECT {expr} AS v, toString(v) AS t");
        let (schema, rows) = query(conn, &sql).await;
        let column = &schema.columns[0];
        assert_eq!(column.native_type, *native, "{expr}");
        assert_eq!(column.logical, *logical, "{expr}");
        assert_eq!(rows.len(), 1, "{expr}");
        let Value::Text(server) = &rows[0][1] else {
            panic!("{expr}: toString is not text: {:?}", rows[0][1]);
        };
        assert_eq!(&rows[0][0].to_string(), server, "{expr}");
    }

    // A short `FixedString` is padded with zero bytes, as `clickhouse-client` writes it
    // (`ab\0\0`); `toString` drops them.
    let (_, rows) = query(conn, "SELECT toFixedString('ab', 4)").await;
    assert_eq!(rows[0][0], Value::Text("ab\0\0".into()));

    // NULL is NULL, whatever the type.
    for expr in [
        "CAST(NULL, 'Nullable(Int32)')",
        "CAST(NULL, 'Nullable(String)')",
        "CAST(NULL, 'Nullable(Date)')",
        "CAST(NULL, 'Nullable(Float64)')",
        "CAST(NULL, 'Nullable(Bool)')",
        "CAST(NULL, 'Nullable(DateTime)')",
    ] {
        let (_, rows) = query(conn, &format!("SELECT {expr}")).await;
        assert_eq!(rows[0][0], Value::Null, "{expr}");
    }
}

#[tokio::test]
async fn a_session_keeps_what_its_statements_set() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    assert_eq!(
        conn.current_database().await.unwrap().as_deref(),
        Some("spelaion")
    );
    exec(conn, "SET max_threads = 3").await;
    let (_, rows) = query(conn, "SELECT getSetting('max_threads')").await;
    assert_eq!(rows[0][0].to_string(), "3");
    exec(conn, "CREATE TEMPORARY TABLE kept (a UInt8)").await;
    exec(conn, "INSERT INTO kept VALUES (1), (2)").await;
    let (_, rows) = query(conn, "SELECT count() FROM kept").await;
    assert_eq!(rows[0][0].to_string(), "2");
    conn.select_database("system").await.unwrap();
    assert_eq!(
        conn.current_database().await.unwrap().as_deref(),
        Some("system")
    );
    exec(conn, "USE spelaion").await;
    assert_eq!(
        conn.current_database().await.unwrap().as_deref(),
        Some("spelaion")
    );
    assert!(!conn.is_closed());
    assert!(conn.server_version().await.unwrap().starts_with("25."));
}

/// SpelaionDB's read-only barrier on ClickHouse (ADR-020 there) is the session's `readonly`
/// setting: once set, the session refuses every write and every way of setting it back.
#[tokio::test]
async fn a_read_only_session_cannot_be_made_writable_again() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    exec(
        conn,
        "CREATE TABLE IF NOT EXISTS ro_target (a UInt8) ENGINE = Memory",
    )
    .await;
    exec(conn, "SET readonly = 2").await;
    for sql in [
        "INSERT INTO ro_target VALUES (1)",
        "SET readonly = 0",
        "SET readonly = 1",
        "SELECT 1 SETTINGS readonly = 0",
        "DROP TABLE ro_target",
        "CREATE TABLE ro_other (a UInt8) ENGINE = Memory",
        "TRUNCATE TABLE ro_target",
        "ALTER TABLE ro_target DELETE WHERE 1",
    ] {
        match run(conn, sql).await {
            Err(DriverError::Database { code, .. }) => {
                assert_eq!(code.as_deref(), Some("164"), "{sql}");
            }
            other => panic!("{sql}: {other:?}"),
        }
    }
    let (_, rows) = query(conn, "SELECT getSetting('readonly')").await;
    assert_eq!(rows[0][0].to_string(), "2");
    // The connection is still usable for reads.
    let (_, rows) = query(conn, "SELECT count() FROM ro_target").await;
    assert_eq!(rows[0][0].to_string(), "0");
}

#[tokio::test]
async fn statements_without_a_result_say_how_many_rows_they_wrote() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    exec(conn, "DROP TABLE IF EXISTS written").await;
    exec(
        conn,
        "CREATE TABLE written (a UInt32) ENGINE = MergeTree ORDER BY a",
    )
    .await;
    let events = run(conn, "INSERT INTO written SELECT number FROM numbers(1234)")
        .await
        .unwrap();
    assert!(matches!(
        events.as_slice(),
        [ExecEvent::Done {
            rows_affected: Some(1234)
        }]
    ));
    let events = run(conn, "DROP TABLE written").await.unwrap();
    assert!(matches!(
        events.as_slice(),
        [ExecEvent::Done {
            rows_affected: None
        }]
    ));
}

#[tokio::test]
async fn a_large_result_comes_in_batches() {
    let Some(conn) = connect().await else { return };
    let opts = ExecOptions {
        batch_size: 10_000,
        ..Default::default()
    };
    let events: Vec<ExecEvent> = conn
        .execute(
            "SELECT number, toString(number) FROM numbers(1000000)",
            &opts,
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let batches: Vec<usize> = events
        .iter()
        .filter_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        })
        .collect();
    assert_eq!(batches.iter().sum::<usize>(), 1_000_000);
    assert!(batches.iter().all(|&n| n <= 10_000));
    assert!(matches!(events.first(), Some(ExecEvent::Columns(_))));
    assert!(matches!(events.last(), Some(ExecEvent::Done { .. })));
}

#[tokio::test]
async fn an_error_after_the_first_rows_is_the_servers_error() {
    let Some(conn) = connect().await else { return };
    let result = run(
        conn.as_ref(),
        "SELECT number, throwIf(number = 300000) FROM numbers(400000) SETTINGS max_block_size = 1000",
    )
    .await;
    match result {
        Err(DriverError::Database { code, message, .. }) => {
            assert_eq!(code.as_deref(), Some("395"));
            assert!(
                message.contains("FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
                "{message}"
            );
        }
        other => panic!("{other:?}"),
    }
    // The session is still there.
    let (_, rows) = query(conn.as_ref(), "SELECT 1").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn refusals_carry_the_servers_code() {
    let Some(conn) = connect().await else { return };
    match run(conn.as_ref(), "SELECT * FROM not_a_table").await {
        Err(DriverError::Database { code, message, .. }) => {
            assert_eq!(code.as_deref(), Some("60"));
            assert!(!message.contains("version"), "{message}");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_cancel_stops_the_statement_that_runs_and_no_later_one() {
    let Some(conn) = connect().await else { return };
    let cancel = conn.cancel_handle();
    let started = Instant::now();
    let (result, ()) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(20),
            run(conn.as_ref(), "SELECT count() FROM system.numbers"),
        ),
        async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            cancel.cancel().await.unwrap();
        }
    );
    let result = result.expect("the cancel was obeyed");
    assert!(matches!(result, Err(DriverError::Cancelled)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(15));
    // Nothing sent after the cancel is touched.
    let (_, rows) = query(conn.as_ref(), "SELECT 42").await;
    assert_eq!(rows[0][0].to_string(), "42");
    // And the statement is gone from the server.
    let (_, rows) = query(
        conn.as_ref(),
        "SELECT count() FROM system.processes WHERE query = 'SELECT count() FROM system.numbers'",
    )
    .await;
    assert_eq!(rows[0][0].to_string(), "0");
}

/// SDB-359: a result nobody reads any more (an agent's read past its row cap) is stopped, and
/// the statement sent right after it on the same session runs. The server keeps the session
/// locked until it has ended the stopped request; the next one must not be refused for it
/// (`SESSION_IS_LOCKED`), however soon it follows. Many times, because the server releases the
/// session a moment after the client gives up on the request, and the next request races it.
#[tokio::test]
async fn a_statement_right_after_an_abandoned_result_runs() {
    let Some(conn) = connect().await else { return };
    let options = ExecOptions {
        batch_size: 5,
        ..Default::default()
    };
    for round in 0..40 {
        let mut events = conn
            .execute("SELECT number FROM system.numbers", &options)
            .await
            .unwrap_or_else(|e| panic!("round {round}: {e}"));
        // The first rows, then the result is dropped, as the core does past a read's cap.
        loop {
            match events.try_next().await {
                Ok(Some(ExecEvent::Batch(_))) => break,
                Ok(Some(_)) => {}
                other => panic!("round {round}: {other:?}"),
            }
        }
        drop(events);
        let started = Instant::now();
        let (_, rows) = query(conn.as_ref(), &format!("SELECT {round}")).await;
        assert_eq!(rows[0][0].to_string(), round.to_string());
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "round {round}: {:?}",
            started.elapsed()
        );
    }
    assert!(!conn.is_closed());
}

/// SDB-359: statements back to back on one session, with and without a result, and after a
/// cancel: none is refused because the one before it still holds the session.
#[tokio::test]
async fn statements_back_to_back_on_one_session_never_find_it_locked() {
    let Some(conn) = connect().await else { return };
    for round in 0..300 {
        let sql = match round % 3 {
            0 => format!("SELECT {round}"),
            1 => "SET max_threads = 2".to_string(),
            _ => "SELECT number FROM numbers(1000) FORMAT TabSeparatedWithNamesAndTypes".into(),
        };
        run(conn.as_ref(), &sql)
            .await
            .unwrap_or_else(|e| panic!("round {round}: {sql}\n→ {e}"));
    }
    let cancel = conn.cancel_handle();
    for round in 0..10 {
        let (result, ()) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(20),
                run(conn.as_ref(), "SELECT count() FROM system.numbers"),
            ),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                cancel.cancel().await.unwrap();
            }
        );
        let result = result.expect("the cancel was obeyed");
        assert!(matches!(result, Err(DriverError::Cancelled)), "{result:?}");
        let (_, rows) = query(conn.as_ref(), &format!("SELECT {round}")).await;
        assert_eq!(rows[0][0].to_string(), round.to_string());
    }
    assert!(!conn.is_closed());
}

#[tokio::test]
async fn bound_values_are_never_part_of_the_statement() {
    let Some(conn) = connect().await else { return };
    let value = "it's a \\ value'; DROP TABLE x; --".to_string();
    let events: Vec<ExecEvent> = conn
        .execute_bound(
            "SELECT {p1:String} AS a, {p2:String} AS b, {p1:String} = a AS same",
            vec![value.clone(), "2".into()],
            &ExecOptions::default(),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let batch = events
        .iter()
        .find_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.clone()),
            _ => None,
        })
        .expect("a row");
    assert_eq!(
        Value::from_array(batch.column(0), 0, LogicalType::Text).unwrap(),
        Value::Text(value)
    );
    assert_eq!(
        Value::from_array(batch.column(1), 0, LogicalType::Text).unwrap(),
        Value::Text("2".into())
    );
}

#[tokio::test]
async fn the_explorer_reads_the_catalog() {
    let Some(conn) = connect().await else { return };
    let conn = conn.as_ref();
    exec(conn, "DROP DATABASE IF EXISTS explored").await;
    exec(conn, "CREATE DATABASE explored").await;
    exec(
        conn,
        "CREATE TABLE explored.events (id UInt64, at DateTime DEFAULT now(), note Nullable(String) COMMENT 'free text', \
         INDEX note_idx note TYPE bloom_filter GRANULARITY 4) \
         ENGINE = MergeTree ORDER BY (id, at) PARTITION BY toYYYYMM(at) COMMENT 'what happened'",
    )
    .await;
    exec(
        conn,
        "CREATE VIEW explored.recent AS SELECT * FROM explored.events",
    )
    .await;
    exec(
        conn,
        "INSERT INTO explored.events (id, note) VALUES (1, 'a'), (2, NULL)",
    )
    .await;

    let schemas = conn.list_schemas().await.unwrap();
    assert!(schemas.iter().any(|s| s.name == "explored" && !s.system));
    assert!(schemas.iter().any(|s| s.name == "system" && s.system));
    assert!(
        conn.list_databases()
            .await
            .unwrap()
            .contains(&"explored".to_string())
    );

    let objects = conn.list_objects("explored").await.unwrap();
    let names: Vec<(&str, ObjectKind)> =
        objects.iter().map(|o| (o.name.as_str(), o.kind)).collect();
    assert_eq!(
        names,
        [("events", ObjectKind::Table), ("recent", ObjectKind::View)]
    );
    assert_eq!(objects[0].estimated_rows, Some(2));
    assert_eq!(objects[0].comment.as_deref(), Some("what happened"));

    let table = conn.describe_table("explored", "events").await.unwrap();
    let columns: Vec<(&str, &str, bool)> = table
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.native_type.as_str(), c.nullable))
        .collect();
    assert_eq!(
        columns,
        [
            ("id", "UInt64", false),
            ("at", "DateTime", false),
            ("note", "Nullable(String)", true)
        ]
    );
    assert_eq!(table.columns[1].default.as_deref(), Some("now()"));
    assert_eq!(table.columns[2].comment.as_deref(), Some("free text"));
    // A sort order, never a primary key: nothing pages or edits rows by it.
    assert!(table.primary_key.is_empty());
    let definitions: Vec<&str> = table
        .indexes
        .iter()
        .map(|i| i.definition.as_str())
        .collect();
    assert!(definitions.contains(&"ORDER BY id, at"), "{definitions:?}");
    assert!(
        definitions.contains(&"PARTITION BY toYYYYMM(at)"),
        "{definitions:?}"
    );
    assert!(
        definitions.contains(&"INDEX `note_idx` note TYPE bloom_filter GRANULARITY 4"),
        "{definitions:?}"
    );

    let all = conn.list_columns("explored").await.unwrap();
    assert_eq!(all.len(), 6);
    assert!(
        conn.list_referencing_keys("explored", "events")
            .await
            .unwrap()
            .is_empty()
    );

    let definition = conn
        .relation_definition("explored", "recent")
        .await
        .unwrap();
    assert!(
        definition.starts_with("CREATE VIEW explored.recent"),
        "{definition}"
    );
    assert!(definition.ends_with(';'));

    // A name with quotes and backticks travels as a parameter.
    assert!(conn.list_objects("no`such'db").await.unwrap().is_empty());
    exec(conn, "DROP DATABASE explored").await;
}

fn ca() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testing/keys/ca.crt")
}

#[tokio::test]
async fn tls_modes_verify_as_libpq_does() {
    let Some((host, port)) = addr("SPELAION_TEST_CLICKHOUSE_HTTPS") else {
        return;
    };
    let Some((plain_host, plain_port)) = addr("SPELAION_TEST_CLICKHOUSE") else {
        return;
    };
    let try_mode = |host: &str, port: u16, tls: TlsMode, ca: Option<PathBuf>| {
        let mut p = params(host.to_string(), port);
        p.tls = tls;
        p.ca_certificate = ca;
        async move { ClickhouseDriver.connect(&p).await.map(|_| ()) }
    };
    // The certificate is for "localhost", not for 127.0.0.1.
    assert!(
        try_mode("localhost", port, TlsMode::VerifyFull, Some(ca()))
            .await
            .is_ok()
    );
    match try_mode("127.0.0.1", port, TlsMode::VerifyFull, Some(ca())).await {
        Err(DriverError::Connection(message)) => assert!(message.contains("TLS"), "{message}"),
        other => panic!("{other:?}"),
    }
    assert!(
        try_mode("127.0.0.1", port, TlsMode::VerifyCa, Some(ca()))
            .await
            .is_ok()
    );
    // Not signed by a trusted authority: the system's do not know the test one.
    assert!(
        try_mode("localhost", port, TlsMode::VerifyFull, None)
            .await
            .is_err()
    );
    assert!(try_mode(&host, port, TlsMode::Require, None).await.is_ok());
    assert!(try_mode(&host, port, TlsMode::Prefer, None).await.is_ok());
    // A plain port: prefer falls back, require refuses.
    assert!(
        try_mode(&plain_host, plain_port, TlsMode::Prefer, None)
            .await
            .is_ok()
    );
    assert!(
        try_mode(&plain_host, plain_port, TlsMode::Require, None)
            .await
            .is_err()
    );
    // Plain HTTP to the HTTPS port.
    assert!(try_mode(&host, port, TlsMode::Disable, None).await.is_err());

    // A whole statement over TLS.
    let mut p = params("localhost".into(), port);
    p.tls = TlsMode::VerifyFull;
    p.ca_certificate = Some(ca());
    let conn = ClickhouseDriver.connect(&p).await.unwrap();
    let (_, rows) = query(conn.as_ref(), "SELECT number FROM numbers(3)").await;
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn a_wrong_password_is_a_connection_error_that_does_not_repeat_it() {
    let Some((host, port)) = addr("SPELAION_TEST_CLICKHOUSE") else {
        return;
    };
    let mut p = params(host, port);
    p.password = Some(Secret::new("not-the-password-7f3a"));
    match ClickhouseDriver.connect(&p).await {
        Err(DriverError::Connection(message)) => {
            assert!(!message.contains("not-the-password-7f3a"), "{message}");
        }
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("logged in with a wrong password"),
    }
}

#[tokio::test]
async fn connecting_to_the_address_given_keeps_the_host_name() {
    let Some((_, port)) = addr("SPELAION_TEST_CLICKHOUSE") else {
        return;
    };
    // As through a tunnel: the host does not resolve, the address is where the server is.
    let mut p = params("clickhouse.invalid".into(), 1);
    p.connect_addr = Some(format!("127.0.0.1:{port}").parse().unwrap());
    let conn = ClickhouseDriver
        .connect(&p)
        .await
        .expect("connects through the address");
    let (_, rows) = query(conn.as_ref(), "SELECT 1").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn a_database_that_is_not_there_is_a_connection_error() {
    let Some((host, port)) = addr("SPELAION_TEST_CLICKHOUSE") else {
        return;
    };
    let mut p = params(host, port);
    p.database = "nowhere_to_be_found".into();
    assert!(matches!(
        ClickhouseDriver.connect(&p).await,
        Err(DriverError::Connection(_))
    ));
}

/// Imports (ADR-038, SDB-324): every value is data of `FORMAT TabSeparated`, never SQL text, and
/// each stays in its own column whatever it holds.
#[tokio::test]
async fn imported_rows_are_data_and_land_whole() {
    let Some(conn) = connect().await else { return };
    let caps = conn.capabilities();
    assert!(caps.insert_rows && caps.bound_params && caps.explain);
    exec(&*conn, "DROP TABLE IF EXISTS import_rows").await;
    exec(
        &*conn,
        "CREATE TABLE import_rows (id Int32, name Nullable(String), amount Decimal(10, 2), \
         day Date, ok Bool) ENGINE = MergeTree ORDER BY id",
    )
    .await;
    let target = InsertTarget {
        schema: "spelaion".into(),
        table: "import_rows".into(),
        columns: ["id", "name", "amount", "day", "ok"]
            .map(String::from)
            .to_vec(),
    };
    let rows: Vec<Vec<Option<String>>> = [
        ("1", Some("plain"), "10.50", "2024-01-02", "true"),
        ("2", None, "0", "2024-02-29", "false"),
        (
            "3",
            Some("tab\there\nand a line"),
            "-3.25",
            "1999-12-31",
            "true",
        ),
        (
            "4",
            Some("x'); DROP TABLE import_rows; --"),
            "1",
            "2000-01-01",
            "false",
        ),
        ("5", Some("\\N"), "2", "2000-01-02", "true"),
        (
            "6",
            Some("back\\slash ñandú 🦀"),
            "3",
            "2000-01-03",
            "false",
        ),
    ]
    .into_iter()
    .map(|(id, name, amount, day, ok)| {
        vec![
            Some(id.to_string()),
            name.map(str::to_string),
            Some(amount.to_string()),
            Some(day.to_string()),
            Some(ok.to_string()),
        ]
    })
    .collect();
    let inserted = conn
        .insert_rows(&target, TextRows::from_rows(5, &rows).unwrap())
        .await
        .expect("inserts");
    assert_eq!(inserted, 6);
    let (_, got) = query(
        &*conn,
        "SELECT id, name, toString(amount), toString(day), ok FROM import_rows ORDER BY id",
    )
    .await;
    let text = |v: &Value| match v {
        Value::Null => None,
        other => Some(other.to_string()),
    };
    let got: Vec<Vec<Option<String>>> = got
        .iter()
        .map(|row| row.iter().map(text).collect())
        .collect();
    assert_eq!(got[2][1].as_deref(), Some("tab\there\nand a line"));
    assert_eq!(
        got[3][1].as_deref(),
        Some("x'); DROP TABLE import_rows; --")
    );
    assert_eq!(got[1][1], None, "a NULL stays NULL");
    assert_eq!(
        got[4][1].as_deref(),
        Some("\\N"),
        "the text \\N is not NULL"
    );
    assert_eq!(got[5][1].as_deref(), Some("back\\slash ñandú 🦀"));
    assert_eq!(got[0][2].as_deref(), Some("10.5"));
    assert_eq!(got[1][3].as_deref(), Some("2024-02-29"));

    // A NULL where the column cannot hold one is refused, not turned into the column's default.
    let null_id = TextRows::from_rows(
        5,
        &[vec![
            None,
            Some("x".into()),
            Some("1".into()),
            Some("2000-01-01".into()),
            Some("true".into()),
        ]],
    )
    .unwrap();
    assert!(conn.insert_rows(&target, null_id).await.is_err());
    // A value the column cannot read is the server's error; the session goes on.
    let bad = TextRows::from_rows(
        5,
        &[vec![
            Some("7".into()),
            None,
            Some("not a number".into()),
            Some("2000-01-01".into()),
            Some("true".into()),
        ]],
    )
    .unwrap();
    assert!(conn.insert_rows(&target, bad).await.is_err());
    let (_, count) = query(&*conn, "SELECT count() FROM import_rows").await;
    assert_eq!(count[0][0].to_string(), "6");

    // Many rows go in several requests.
    let many: Vec<Vec<Option<String>>> = (100..20_100)
        .map(|n| {
            vec![
                Some(n.to_string()),
                Some("x".repeat(500)),
                Some("1".into()),
                Some("2000-01-01".into()),
                Some("true".into()),
            ]
        })
        .collect();
    let inserted = conn
        .insert_rows(&target, TextRows::from_rows(5, &many).unwrap())
        .await
        .unwrap();
    assert_eq!(inserted, 20_000);
    let (_, count) = query(&*conn, "SELECT count() FROM import_rows").await;
    assert_eq!(count[0][0].to_string(), "20006");
    exec(&*conn, "DROP TABLE import_rows").await;
}

/// `EXPLAIN json = 1` answers one row whose one value is the plan as JSON: what SpelaionDB's
/// plan view reads (SDB-324).
#[tokio::test]
async fn explain_answers_the_plan_as_json() {
    let Some(conn) = connect().await else { return };
    let (schema, rows) = query(
        &*conn,
        "EXPLAIN json = 1, indexes = 1, actions = 1 SELECT number FROM system.numbers LIMIT 3",
    )
    .await;
    assert_eq!(schema.columns.len(), 1);
    assert_eq!(rows.len(), 1);
    let text = rows[0][0].to_string();
    let plan: serde_json::Value = serde_json::from_str(&text).expect("JSON");
    assert!(plan[0]["Plan"]["Node Type"].is_string(), "{text}");
}
