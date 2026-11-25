//! ClickHouse for SpelaionDB, served as a plugin (SpelaionDB's ADR-041 and ADR-042).
//!
//! The driver speaks ClickHouse's HTTP interface ([`http`]). Each connection is one
//! server-side session (`session_id`), so what a statement sets (`SET`, `USE`, temporary
//! tables) holds for the next ones, as in any other engine. After the first request the session
//! is always asked for with `session_check=1`: a session the server no longer has (it expired,
//! the server restarted) is an error that loses the connection, never a new empty session in
//! its place, which would silently drop what was set in it (SpelaionDB's read-only mode among
//! them: `SET readonly`, ADR-020 there).
//!
//! The session's requests go one at a time, through an actor ([`actor`]): ClickHouse refuses a
//! second request on a session while one runs. A cancel goes on a connection of its own, as
//! `KILL QUERY` on the running statement's `query_id`, repeated until that statement has ended
//! (a `KILL` that arrives before the statement is registered finds nothing). The server holds
//! the session until it has ended a request, so every answer is read to its end before the next
//! request goes out, even one nobody reads any more: it is killed, then drained.
//!
//! Results come as `TabSeparatedWithNamesAndTypes` ([`tsv`], [`types`]): the server's own text
//! for every value, read as it arrives and handed on in Arrow batches.
//!
//! What ClickHouse does not have is declared, and SpelaionDB hides it: transactions, editing
//! rows by primary key (its primary key is a sort order, not unique), importing rows.

pub mod http;
mod introspect;
pub mod tls;
pub mod tsv;
pub mod types;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ColumnMeta, ConnectParams, Connection, Driver, DriverError,
    EventStream, ExecEvent, ExecOptions, KeepAlive, LogicalType, ObjectInfo, ReferencingKey,
    Result, ResultSchema, SchemaColumn, SchemaInfo, TableInfo,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::http::{Client, Endpoint, SESSION_NOT_FOUND, send_alone, server_error};
use crate::tls::ClientIdentity;
use crate::tsv::{Lines, fields, unescape};
use crate::types::Batches;

/// The format every statement's result is asked for in.
const FORMAT: &str = "TabSeparatedWithNamesAndTypes";

/// What the server writes in a result's body before an exception that happened after the
/// result had started (and then it cuts the response short).
const EXCEPTION_MARKER: &[u8] = b"__exception__";

/// An idle session is touched this often, so the server keeps it: it forgets a session idle for
/// `session_timeout` (60 s unless the server says otherwise).
const TOUCH_EVERY: Duration = Duration::from_secs(25);

/// How often a cancel repeats its `KILL QUERY`, and for how long at most.
const KILL_EVERY: Duration = Duration::from_millis(200);
const KILL_FOR: Duration = Duration::from_secs(30);

/// The most a stopped statement's answer is read for, once nobody wants it.
const ABANDONED_FOR: Duration = Duration::from_secs(10);

/// The most an internal query's result may take.
const MAX_ROWS_BODY: usize = 256 * 1024 * 1024;

/// The most rows of an import one request carries, in bytes of `TabSeparated` text.
const MAX_INSERT_BODY: usize = 8 * 1024 * 1024;

/// The `clickhouse` engine.
#[derive(Debug, Default, Clone)]
pub struct ClickhouseDriver;

#[async_trait]
impl Driver for ClickhouseDriver {
    fn engine(&self) -> &str {
        "clickhouse"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(ClientIdentity { cert, key }),
            (None, None) => None,
            _ => {
                return Err(DriverError::Connection(
                    "a client certificate needs its key, and a key its certificate".into(),
                ));
            }
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let endpoint = Arc::new(Endpoint::new(
            params.host.clone(),
            params.port,
            params.connect_addr,
            tls,
            params.connect_timeout(),
            params.user.clone(),
            params.password.clone(),
        ));
        let mut session = Session {
            client: Client::new(endpoint.clone()),
            id: unique_id("spelaion-session"),
            checked: false,
        };
        let open = async {
            // The first request creates the session; every later one requires it.
            session.rows("SELECT 1", &[]).await?;
            if !params.database.is_empty() {
                session
                    .rows(&format!("USE {}", quote(&params.database)), &[])
                    .await?;
            }
            Ok::<_, DriverError>(())
        };
        match tokio::time::timeout(params.connect_timeout(), open).await {
            Ok(Ok(())) => {}
            Ok(Err(DriverError::Database { message, .. })) => {
                return Err(DriverError::Connection(message));
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    params.connect_timeout().as_secs()
                )));
            }
        }

        let closed = Arc::new(AtomicBool::new(false));
        let (cancel_tx, cancel_rx) = watch::channel(0u64);
        let cancel = Arc::new(Cancel {
            sent: AtomicU64::new(0),
            upto: cancel_tx,
        });
        let (requests, receiver) = mpsc::channel(16);
        tokio::spawn(actor(session, receiver, cancel_rx, closed.clone()));
        Ok(Box::new(ClickhouseConnection {
            capabilities: Capabilities {
                transactions: false,
                cancel: true,
                // SpelaionDB reads `EXPLAIN json = 1` into its plan view (SDB-324).
                explain: true,
                editable_results: false,
                schemas: true,
                insert_rows: true,
                // A `String` column takes bytes as they are, escaped in the body (SDB-464).
                insert_bytes: true,
                // `execute_bound` binds `{p1:String}`, which SpelaionDB's data search writes
                // for ClickHouse (SDB-324).
                bound_params: true,
            },
            requests,
            cancel,
            closed,
        }))
    }
}

/// `name` as a quoted identifier.
pub fn quote(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// An id no other request of this process, or of another, has: the session's and each query's.
fn unique_id(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "{prefix}-{}-{nanos:x}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

/// The server-side session and the connection its requests go on.
struct Session {
    client: Client,
    id: String,
    /// The session exists on the server: every request from now on requires it.
    checked: bool,
}

impl Session {
    fn params<'a>(&'a self, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut params = vec![
            ("session_id", self.id.as_str()),
            ("session_check", if self.checked { "1" } else { "0" }),
            ("default_format", FORMAT),
        ];
        params.extend_from_slice(extra);
        params
    }

    async fn send(
        &mut self,
        sql: &str,
        extra: &[(&str, &str)],
    ) -> Result<hyper::Response<Incoming>> {
        let params = self.params(extra);
        let params: Vec<(String, String)> = params
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let borrowed: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let response = self.client.send(&borrowed, sql.to_string()).await?;
        self.checked = true;
        Ok(response)
    }

    /// Runs `statement` (an `INSERT … FORMAT TabSeparated`) with `rows` as its data: the
    /// statement goes in the `query` parameter and the rows are the body, so ClickHouse reads
    /// them as data in that format and never as SQL. A NULL for a column that cannot hold one
    /// is an error, not the column's default.
    async fn insert(&mut self, statement: &str, rows: Vec<u8>) -> Result<()> {
        let params = self.params(&[("query", statement), ("input_format_null_as_default", "0")]);
        let params: Vec<(String, String)> = params
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let borrowed: Vec<(&str, &str)> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let response = self.client.send(&borrowed, rows).await?;
        self.checked = true;
        // The answer is empty; read it to the end so the connection can be used again.
        let mut body = response.into_body();
        while let Some(frame) = body.frame().await {
            frame.map_err(|e| DriverError::Connection(format!("the answer was cut: {e}")))?;
        }
        Ok(())
    }

    /// A whole result as text: what the driver's own queries read.
    async fn rows(
        &mut self,
        sql: &str,
        extra: &[(&str, &str)],
    ) -> Result<Vec<Vec<Option<String>>>> {
        let response = self.send(sql, extra).await?;
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        loop {
            match body.frame().await {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        bytes.extend_from_slice(&data);
                        if bytes.len() > MAX_ROWS_BODY {
                            return Err(DriverError::Other(
                                "the catalog's answer is too large".into(),
                            ));
                        }
                    }
                }
                Some(Err(e)) => return Err(cut_off(&bytes, &e.to_string())),
                None => break,
            }
        }
        let mut lines = Lines::default();
        lines.push(&bytes);
        let mut out = Vec::new();
        let mut header = 0;
        while let Some(line) = lines.next_line() {
            // Names, then types.
            if header < 2 {
                header += 1;
                continue;
            }
            out.push(fields(line).map(unescape).collect());
        }
        Ok(out)
    }
}

/// What a response that was cut short means: the server's exception after the marker, or a
/// lost connection.
fn cut_off(rest: &[u8], cause: &str) -> DriverError {
    if let Some(at) = find(rest, EXCEPTION_MARKER) {
        let text = String::from_utf8_lossy(&rest[at + EXCEPTION_MARKER.len()..]);
        return server_error(text.trim());
    }
    DriverError::Connection(format!("the result was cut off: {cause}"))
}

/// The line that starts an exception: the marker, ended by `\r\n` as the server writes it. A
/// value that reads the same is data (its `\r` would be escaped).
fn is_marker(line: &[u8]) -> bool {
    line.strip_suffix(b"\r").unwrap_or(line) == EXCEPTION_MARKER
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Cancels: remembers the last statement sent when it was asked, so that statement and every
/// one queued before it stop (a queued one never starts), and none sent after it is touched.
struct Cancel {
    /// The number of the last statement sent.
    sent: AtomicU64,
    /// Every statement up to this number is cancelled.
    upto: watch::Sender<u64>,
}

struct ClickhouseCancel(Arc<Cancel>);

#[async_trait]
impl CancelHandle for ClickhouseCancel {
    async fn cancel(&self) -> Result<()> {
        let sent = self.0.sent.load(Ordering::SeqCst);
        self.0.upto.send_if_modified(|upto| {
            if *upto < sent {
                *upto = sent;
                true
            } else {
                false
            }
        });
        Ok(())
    }
}

enum Request {
    Execute {
        sql: String,
        params: Vec<String>,
        batch_size: usize,
        seq: u64,
        events: mpsc::Sender<Result<ExecEvent>>,
        started: oneshot::Sender<Result<()>>,
    },
    Rows {
        sql: String,
        params: Vec<(String, String)>,
        reply: oneshot::Sender<Result<Vec<Vec<Option<String>>>>>,
    },
    /// An `INSERT … FORMAT TabSeparated` statement and the rows it reads, kept apart: the rows
    /// travel as the request's body, never as SQL text.
    Insert {
        statement: String,
        /// The `TabSeparated` body: bytes, since a `String` column takes any (SDB-464).
        rows: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// The session's requests, one at a time, and a touch when it is idle.
async fn actor(
    mut session: Session,
    mut requests: mpsc::Receiver<Request>,
    cancel: watch::Receiver<u64>,
    closed: Arc<AtomicBool>,
) {
    loop {
        let request = tokio::select! {
            request = requests.recv() => match request {
                Some(request) => request,
                None => return,
            },
            _ = tokio::time::sleep(TOUCH_EVERY) => {
                if let Err(e) = session.rows("SELECT 1", &[]).await {
                    mark_lost(&e, &closed);
                }
                continue;
            }
        };
        match request {
            Request::Execute {
                sql,
                params,
                batch_size,
                seq,
                events,
                started,
            } => {
                let mut run = Run {
                    session: &mut session,
                    cancel: cancel.clone(),
                    seq,
                    closed: &closed,
                };
                run.execute(&sql, &params, batch_size, events, started)
                    .await;
            }
            Request::Rows { sql, params, reply } => {
                let borrowed: Vec<(&str, &str)> = params
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let result = session.rows(&sql, &borrowed).await;
                if let Err(e) = &result {
                    mark_lost(e, &closed);
                }
                let _ = reply.send(result);
            }
            Request::Insert {
                statement,
                rows,
                reply,
            } => {
                let result = session.insert(&statement, rows).await;
                if let Err(e) = &result {
                    mark_lost(e, &closed);
                }
                let _ = reply.send(result);
            }
        }
    }
}

/// An error that means the session is gone marks the connection closed.
fn mark_lost(error: &DriverError, closed: &AtomicBool) {
    let lost = match error {
        DriverError::Connection(_) => true,
        DriverError::Database { code, .. } => code.as_deref() == Some(SESSION_NOT_FOUND),
        DriverError::Cancelled | DriverError::Decode { .. } | DriverError::Other(_) => false,
    };
    if lost {
        closed.store(true, Ordering::SeqCst);
    }
}

/// One statement on the session.
struct Run<'a> {
    session: &'a mut Session,
    cancel: watch::Receiver<u64>,
    seq: u64,
    closed: &'a AtomicBool,
}

impl Run<'_> {
    fn cancelled(&self) -> bool {
        *self.cancel.borrow() >= self.seq
    }

    async fn execute(
        &mut self,
        sql: &str,
        params: &[String],
        batch_size: usize,
        events: mpsc::Sender<Result<ExecEvent>>,
        started: oneshot::Sender<Result<()>>,
    ) {
        if self.cancelled() {
            let _ = started.send(Err(DriverError::Cancelled));
            return;
        }
        let query_id = unique_id("spelaion-query");
        let names: Vec<String> = (1..=params.len()).map(|n| format!("param_p{n}")).collect();
        let mut extra: Vec<(&str, &str)> = vec![("query_id", query_id.as_str())];
        extra.extend(
            names
                .iter()
                .map(String::as_str)
                .zip(params.iter().map(String::as_str)),
        );

        let endpoint = self.session.client.endpoint().clone();
        let mut killer = Killer::new(endpoint, query_id.clone());
        let mut cancel = self.cancel.clone();
        let seq = self.seq;

        // Until its headers arrive: a cancel kills it, and its answer is then the cancel.
        let response = {
            let send = self.session.send(sql, &extra);
            tokio::pin!(send);
            loop {
                tokio::select! {
                    response = &mut send => break response,
                    changed = cancel.changed(), if !killer.started() => {
                        if changed.is_ok() && *cancel.borrow() >= seq {
                            killer.start();
                        }
                    }
                }
            }
        };
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                killer.stop();
                mark_lost(&e, self.closed);
                let e = if killer.started() { cancelled_or(e) } else { e };
                let _ = started.send(Err(e));
                return;
            }
        };
        let _ = started.send(Ok(()));

        let format = response
            .headers()
            .get("X-ClickHouse-Format")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(FORMAT)
            .to_string();
        let written = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
            .and_then(|v| v.get("written_rows")?.as_str()?.parse::<u64>().ok());
        let mut reader = Reader::new(format, batch_size.max(1));
        let mut body = response.into_body();
        let outcome: Result<()> = loop {
            let frame = tokio::select! {
                frame = body.frame() => frame,
                changed = cancel.changed(), if !killer.started() => {
                    if changed.is_ok() && *cancel.borrow() >= seq {
                        killer.start();
                    }
                    continue;
                }
            };
            match frame {
                Some(Ok(frame)) => {
                    let Ok(data) = frame.into_data() else {
                        continue;
                    };
                    match reader.push(&data) {
                        Ok(out) => {
                            if !send_all(&events, out).await {
                                // Nobody reads the result any more: the statement stops, and
                                // its answer is read to its end before the next request. The
                                // server holds the session until it has ended this request; a
                                // connection dropped here would have the next request race
                                // that (SDB-359).
                                killer.start();
                                drain(&mut body).await;
                                killer.stop();
                                return;
                            }
                        }
                        Err(e) => break Err(e),
                    }
                }
                Some(Err(e)) => break Err(cut_off(reader.lines.rest(), &e.to_string())),
                None => break Ok(()),
            }
        };
        killer.stop();
        let outcome = outcome.and_then(|()| reader.finish());
        match outcome {
            Ok(out) => {
                let had_result = reader.schema.is_some();
                if !send_all(&events, out).await {
                    return;
                }
                let rows_affected = if had_result {
                    None
                } else {
                    written.filter(|&n| n > 0)
                };
                let _ = events.send(Ok(ExecEvent::Done { rows_affected })).await;
            }
            Err(e) => {
                mark_lost(&e, self.closed);
                let e = if killer.started() { cancelled_or(e) } else { e };
                let _ = events.send(Err(e)).await;
            }
        }
    }
}

/// After a cancel, a lost or cut-off answer is the cancel.
fn cancelled_or(error: DriverError) -> DriverError {
    match error {
        DriverError::Connection(_) | DriverError::Cancelled => DriverError::Cancelled,
        other => other,
    }
}

async fn send_all(events: &mpsc::Sender<Result<ExecEvent>>, out: Vec<ExecEvent>) -> bool {
    for event in out {
        if events.send(Ok(event)).await.is_err() {
            return false;
        }
    }
    true
}

/// `KILL QUERY` on a statement, again and again on connections of their own, until the
/// statement's own request ends.
struct Killer {
    endpoint: Arc<Endpoint>,
    query_id: String,
    task: Option<(tokio::task::JoinHandle<()>, oneshot::Sender<()>)>,
    started: bool,
}

impl Killer {
    fn new(endpoint: Arc<Endpoint>, query_id: String) -> Self {
        Self {
            endpoint,
            query_id,
            task: None,
            started: false,
        }
    }

    fn started(&self) -> bool {
        self.started
    }

    fn start(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        let endpoint = self.endpoint.clone();
        // Only letters, digits and `-`: nothing to escape.
        let sql = format!("KILL QUERY WHERE query_id = '{}' ASYNC", self.query_id);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let until = tokio::time::Instant::now() + KILL_FOR;
            while tokio::time::Instant::now() < until {
                let _ = send_alone(&endpoint, &[("default_format", FORMAT)], sql.clone()).await;
                tokio::select! {
                    _ = tokio::time::sleep(KILL_EVERY) => {}
                    _ = &mut stopped => return,
                }
            }
        });
        self.task = Some((task, stop));
    }

    /// The statement's request has ended: no more `KILL`s.
    fn stop(&mut self) {
        if let Some((_, stop)) = self.task.take() {
            let _ = stop.send(());
        }
    }
}

/// Reads what is left of an answer nobody wants, until the server ends it, for
/// [`ABANDONED_FOR`] at most: the `KILL`s stop it long before that. Past it the connection is
/// dropped, and a statement that the server has still not ended may hold the session for the
/// next request, which the server then refuses (`SESSION_IS_LOCKED`), without running it.
async fn drain(body: &mut Incoming) {
    let _ = tokio::time::timeout(ABANDONED_FOR, async {
        while let Some(Ok(_)) = body.frame().await {}
    })
    .await;
}

/// A statement's result, read as its bytes arrive.
struct Reader {
    /// The format the server answered in: ours, or one the statement named (`FORMAT JSON`).
    format: String,
    lines: Lines,
    batch_size: usize,
    names: Option<Vec<String>>,
    schema: Option<Arc<ResultSchema>>,
    batches: Option<Batches>,
    /// The marker of an exception was seen: what follows is held until the response ends.
    held: bool,
    /// The response ended as it should: a marker is data.
    ended: bool,
    /// By column: its values are written as they are, not escaped ([`types::written_raw`]).
    raw: Vec<bool>,
}

impl Reader {
    fn new(format: String, batch_size: usize) -> Self {
        Self {
            format,
            lines: Lines::default(),
            batch_size,
            names: None,
            schema: None,
            batches: None,
            held: false,
            ended: false,
            raw: Vec::new(),
        }
    }

    fn ours(&self) -> bool {
        self.format == FORMAT
    }

    fn push(&mut self, data: &[u8]) -> Result<Vec<ExecEvent>> {
        self.lines.push(data);
        self.drain()
    }

    fn drain(&mut self) -> Result<Vec<ExecEvent>> {
        let mut out = Vec::new();
        while !self.held {
            let ours = self.ours();
            let Some(line) = self.lines.next_line() else {
                break;
            };
            if ours && !self.ended && is_marker(line) {
                // Data or an exception: the end of the response says which.
                let length = line.len();
                self.held = true;
                self.lines.unread(length + 1);
                break;
            }
            let line = line.to_vec();
            self.line(&line, &mut out)?;
        }
        Ok(out)
    }

    fn line(&mut self, line: &[u8], out: &mut Vec<ExecEvent>) -> Result<()> {
        if !self.ours() {
            // A format the statement chose: one column of text, a line per row.
            if self.schema.is_none() {
                self.start(
                    vec![ColumnMeta {
                        name: "result".into(),
                        logical: LogicalType::Text,
                        native_type: self.format.clone(),
                        native_id: None,
                        origin: None,
                    }],
                    out,
                );
            }
            return self.row(vec![Some(String::from_utf8_lossy(line).into_owned())], out);
        }
        if self.schema.is_some() {
            let values = fields(line)
                .enumerate()
                .map(|(i, field)| {
                    if self.raw.get(i).copied().unwrap_or(false) {
                        Some(String::from_utf8_lossy(field).into_owned())
                    } else {
                        unescape(field)
                    }
                })
                .collect();
            return self.row(values, out);
        }
        let values: Vec<Option<String>> = fields(line).map(unescape).collect();
        match (&self.names, &self.schema) {
            (None, _) => {
                self.names = Some(values.into_iter().map(Option::unwrap_or_default).collect());
                Ok(())
            }
            (Some(_), None) => {
                let names = self.names.take().unwrap_or_default();
                let types = values.into_iter().map(Option::unwrap_or_default).collect();
                let columns = types::columns(names, types)?;
                self.names = Some(Vec::new());
                self.start(columns, out);
                Ok(())
            }
            (Some(_), Some(_)) => self.row(values, out),
        }
    }

    fn start(&mut self, columns: Vec<ColumnMeta>, out: &mut Vec<ExecEvent>) {
        self.raw = columns
            .iter()
            .map(|column| self.ours() && types::written_raw(&column.native_type))
            .collect();
        let schema = Arc::new(ResultSchema::new(columns));
        self.batches = Some(Batches::new(schema.clone()));
        out.push(ExecEvent::Columns(schema.clone()));
        self.schema = Some(schema);
    }

    fn row(&mut self, values: Vec<Option<String>>, out: &mut Vec<ExecEvent>) -> Result<()> {
        let batches = self.batches.as_mut().expect("columns come first");
        batches.push(values)?;
        if batches.rows() >= self.batch_size
            && let Some(batch) = batches.take()?
        {
            out.push(ExecEvent::Batch(batch));
        }
        Ok(())
    }

    /// The response ended as it should: what was held was data after all.
    fn finish(&mut self) -> Result<Vec<ExecEvent>> {
        self.held = false;
        self.ended = true;
        let mut out = self.drain()?;
        if !self.lines.rest().is_empty() {
            if self.ours() {
                return Err(DriverError::Other(
                    "the result ended in the middle of a row".into(),
                ));
            }
            let rest = self.lines.rest().to_vec();
            self.line(&rest, &mut out)?;
        }
        if let Some(batches) = self.batches.as_mut()
            && let Some(batch) = batches.take()?
        {
            out.push(ExecEvent::Batch(batch));
        }
        Ok(out)
    }
}

struct ClickhouseConnection {
    capabilities: Capabilities,
    requests: mpsc::Sender<Request>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
}

impl ClickhouseConnection {
    fn gone() -> DriverError {
        DriverError::Connection("the connection is closed".into())
    }

    async fn run(&self, sql: &str, params: Vec<String>, opts: &ExecOptions) -> Result<EventStream> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let seq = self.cancel.sent.fetch_add(1, Ordering::SeqCst) + 1;
        let (events, receiver) = mpsc::channel(4);
        let (started, answer) = oneshot::channel();
        self.requests
            .send(Request::Execute {
                sql: sql.to_string(),
                params,
                batch_size: opts.batch_size,
                seq,
                events,
                started,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())??;
        Ok(tokio_stream(receiver))
    }

    /// One of the driver's own queries, with its values bound as `{name:String}` parameters.
    pub(crate) async fn rows(
        &self,
        sql: &str,
        params: &[(&str, &str)],
    ) -> Result<Vec<Vec<Option<String>>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Rows {
                sql: sql.to_string(),
                params: params
                    .iter()
                    .map(|(name, value)| (format!("param_{name}"), value.to_string()))
                    .collect(),
                reply,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())?
    }
}

fn tokio_stream(mut receiver: mpsc::Receiver<Result<ExecEvent>>) -> EventStream {
    futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed()
}

#[async_trait]
impl Connection for ClickhouseConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        self.run(sql, Vec::new(), opts).await
    }

    /// The values are bound as query parameters (`param_p1`, `param_p2`…), which the statement
    /// reads as `{p1:String}`, `{p2:String}`…: never written into its text.
    async fn execute_bound(
        &self,
        sql: &str,
        params: Vec<String>,
        opts: &ExecOptions,
    ) -> Result<EventStream> {
        self.run(sql, params, opts).await
    }

    /// `INSERT INTO table (columns) FORMAT TabSeparated`, with the rows in the request's body
    /// (ADR-038): only the quoted names are SQL text, every value is data in that format,
    /// escaped so it stays one field ([`tsv::escape`]). The rows go in requests of at most
    /// [`MAX_INSERT_BODY`] bytes. ClickHouse has no transactions: a failed request leaves the
    /// requests before it inserted, as the import's progress says.
    async fn insert_rows(
        &self,
        target: &spelaion_driver_api::InsertTarget,
        rows: spelaion_driver_api::TextRows,
    ) -> Result<u64> {
        target.check_rows(&rows)?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let table = if target.schema.is_empty() {
            quote(&target.table)
        } else {
            format!("{}.{}", quote(&target.schema), quote(&target.table))
        };
        let columns: Vec<String> = target.columns.iter().map(|c| quote(c)).collect();
        let statement = format!(
            "INSERT INTO {table} ({}) FORMAT TabSeparated",
            columns.join(", ")
        );
        let mut inserted = 0u64;
        let mut body = Vec::new();
        let mut in_body = 0u64;
        for row in 0..rows.len() {
            tsv::push_row(rows.cells(row), &mut body);
            in_body += 1;
            if body.len() >= MAX_INSERT_BODY || row + 1 == rows.len() {
                let (reply, answer) = oneshot::channel();
                self.requests
                    .send(Request::Insert {
                        statement: statement.clone(),
                        rows: std::mem::take(&mut body),
                        reply,
                    })
                    .await
                    .map_err(|_| Self::gone())?;
                answer.await.map_err(|_| Self::gone())??;
                inserted += in_body;
                in_body = 0;
            }
        }
        Ok(inserted)
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(ClickhouseCancel(self.cancel.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        let rows = self.rows("SELECT version()", &[]).await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().flatten())
            .unwrap_or_default())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        self.rows("SELECT 1", &[]).await?;
        Ok(KeepAlive::Alive)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        let rows = self.rows("SELECT currentDatabase()", &[]).await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().flatten())
            .filter(|name| !name.is_empty()))
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        self.rows(&format!("USE {}", quote(database)), &[]).await?;
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(introspect::list_schemas(self)
            .await?
            .into_iter()
            .map(|schema| schema.name)
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        introspect::list_schemas(self).await
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        introspect::list_objects(self, schema).await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        introspect::describe_table(self, schema, name).await
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        introspect::list_columns(self, schema).await
    }

    /// ClickHouse has no foreign keys.
    async fn list_referencing_keys(
        &self,
        _schema: &str,
        _name: &str,
    ) -> Result<Vec<ReferencingKey>> {
        Ok(Vec::new())
    }

    async fn relation_definition(&self, schema: &str, name: &str) -> Result<String> {
        introspect::relation_definition(self, schema, name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(reader: &mut Reader, chunks: &[&[u8]]) -> Result<Vec<ExecEvent>> {
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend(reader.push(chunk)?);
        }
        out.extend(reader.finish()?);
        Ok(out)
    }

    fn rows_of(events: &[ExecEvent]) -> usize {
        events
            .iter()
            .map(|e| match e {
                ExecEvent::Batch(b) => b.num_rows(),
                _ => 0,
            })
            .sum()
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote("a"), "`a`");
        assert_eq!(quote("a`b\\c"), "`a\\`b\\\\c`");
    }

    #[test]
    fn a_result_reads_whatever_its_pieces() {
        let mut reader = Reader::new(FORMAT.into(), 2);
        let out = events(
            &mut reader,
            &[
                b"n\ts\nUInt8\tNullable(St",
                b"ring)\n1\ta\n2\t\\N\n3",
                b"\tc\n",
            ],
        )
        .unwrap();
        assert!(matches!(&out[0], ExecEvent::Columns(s) if s.columns.len() == 2));
        assert_eq!(rows_of(&out), 3);
    }

    #[test]
    fn an_empty_body_has_no_result() {
        let mut reader = Reader::new(FORMAT.into(), 10);
        let out = events(&mut reader, &[]).unwrap();
        assert!(out.is_empty());
        assert!(reader.schema.is_none());
    }

    #[test]
    fn a_value_that_looks_like_the_exception_marker_is_data() {
        let mut reader = Reader::new(FORMAT.into(), 10);
        let out = events(&mut reader, &[b"s\nString\n__exception__\nafter\n"]).unwrap();
        assert_eq!(rows_of(&out), 2);
    }

    #[test]
    fn an_exception_after_the_rows_is_the_servers_error() {
        let mut reader = Reader::new(FORMAT.into(), 10);
        let out = reader
            .push(b"n\nUInt64\n1\n__exception__\r\nCode: 395. DB::Exception: boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.8.1.1)\n")
            .unwrap();
        assert!(matches!(&out[0], ExecEvent::Columns(_)));
        match cut_off(reader.lines.rest(), "cut") {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("395"));
                assert_eq!(message, "boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)");
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            cut_off(b"1\t2", "eof"),
            DriverError::Connection(_)
        ));
    }

    #[test]
    fn a_format_the_statement_chose_is_one_column_of_lines() {
        let mut reader = Reader::new("JSONEachRow".into(), 10);
        let out = events(&mut reader, &[b"{\"a\":1}\n{\"a\":2}"]).unwrap();
        match &out[0] {
            ExecEvent::Columns(schema) => {
                assert_eq!(schema.columns[0].native_type, "JSONEachRow");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(rows_of(&out), 2);
    }

    #[test]
    fn a_row_cut_in_the_middle_is_an_error() {
        let mut reader = Reader::new(FORMAT.into(), 10);
        reader.push(b"n\nUInt8\n1").unwrap();
        assert!(reader.finish().is_err());
    }
}
