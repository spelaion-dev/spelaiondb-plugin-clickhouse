//! ClickHouse's HTTP interface, over a connection this driver opens itself.
//!
//! Every request goes to the address SpelaionDB gave (`connect_addr`, the local end of a tunnel
//! it opened) or to `host:port`, and never through a proxy: the core decides the route. TLS is
//! verified against `host`, which is also the name sent in SNI and in `Host`.
//!
//! The user and the password travel in ClickHouse's own headers, never in the address: an
//! address can end up in a server's log.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::client::conn::http1::SendRequest;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use spelaion_driver_api::{DriverError, Result, Secret};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::tls::Tls;

/// A connection kept open longer than this between two requests is not used again: the server
/// closes idle ones (`keep_alive_timeout`, 10 s by default), and a request sent on a connection
/// the server is closing would be lost on the way. A new one is opened instead.
const REUSE_WITHIN: Duration = Duration::from_secs(3);

/// The most of an error's body that is read.
const MAX_ERROR_BODY: usize = 64 * 1024;

/// ClickHouse's code for a query that was cancelled (`QUERY_WAS_CANCELLED`).
pub const QUERY_WAS_CANCELLED: &str = "394";
/// ClickHouse's code for a session it does not have (`SESSION_NOT_FOUND`): it expired, or the
/// server restarted.
pub const SESSION_NOT_FOUND: &str = "372";

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Where the server is and how to reach it.
pub struct Endpoint {
    /// The name TLS verifies and `Host` carries.
    host: String,
    port: u16,
    /// Where to connect, if not `host:port`.
    connect_addr: Option<SocketAddr>,
    tls: Tls,
    /// `prefer` once decided: 0 not yet, 1 TLS, 2 plain.
    preferred: AtomicU8,
    connect_timeout: Duration,
    user: String,
    password: Option<Secret>,
}

impl Endpoint {
    pub fn new(
        host: String,
        port: u16,
        connect_addr: Option<SocketAddr>,
        tls: Tls,
        connect_timeout: Duration,
        user: String,
        password: Option<Secret>,
    ) -> Self {
        Self {
            host,
            port,
            connect_addr,
            tls,
            preferred: AtomicU8::new(0),
            connect_timeout,
            user,
            password,
        }
    }

    /// The password, to take it out of anything shown (the server repeats nothing of it, but an
    /// error from a proxy on the way might).
    fn scrub(&self, text: &str) -> String {
        match &self.password {
            Some(password) if !password.expose().is_empty() => {
                text.replace(password.expose(), "***")
            }
            _ => text.to_string(),
        }
    }

    async fn tcp(&self) -> Result<TcpStream> {
        let stream = match self.connect_addr {
            Some(addr) => TcpStream::connect(addr).await,
            None => TcpStream::connect((self.host.as_str(), self.port)).await,
        };
        let stream = stream.map_err(|e| {
            DriverError::Connection(format!("could not reach {}:{}: {e}", self.host, self.port))
        })?;
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    async fn tls_stream(
        &self,
        config: &Arc<rustls::ClientConfig>,
        tcp: TcpStream,
    ) -> std::io::Result<Box<dyn Io>> {
        let name = ServerName::try_from(self.host.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let stream = TlsConnector::from(config.clone())
            .connect(name, tcp)
            .await?;
        Ok(Box::new(stream))
    }

    /// A new connection, with the TLS its mode asks for.
    async fn open(&self) -> Result<Box<dyn Io>> {
        let tcp = self.tcp().await?;
        match &self.tls {
            Tls::Off => Ok(Box::new(tcp)),
            Tls::Required(config) => self
                .tls_stream(config, tcp)
                .await
                .map_err(|e| DriverError::Connection(format!("TLS: {e}"))),
            Tls::Preferred(config) => match self.preferred.load(Ordering::SeqCst) {
                1 => self
                    .tls_stream(config, tcp)
                    .await
                    .map_err(|e| DriverError::Connection(format!("TLS: {e}"))),
                2 => Ok(Box::new(tcp)),
                _ => match self.tls_stream(config, tcp).await {
                    Ok(stream) => {
                        self.preferred.store(1, Ordering::SeqCst);
                        Ok(stream)
                    }
                    // The server does not speak TLS on this port: plain HTTP, on a new
                    // connection (the first one heard a handshake).
                    Err(_) => {
                        self.preferred.store(2, Ordering::SeqCst);
                        Ok(Box::new(self.tcp().await?))
                    }
                },
            },
        }
    }

    /// A new HTTP/1.1 connection, within the connect timeout.
    async fn handshake(&self) -> Result<SendRequest<Full<Bytes>>> {
        let open = async {
            let io = self.open().await?;
            let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
                .await
                .map_err(|e| DriverError::Connection(format!("HTTP: {e}")))?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(sender)
        };
        match tokio::time::timeout(self.connect_timeout, open).await {
            Ok(result) => result,
            Err(_) => Err(DriverError::Connection(format!(
                "could not connect to {}:{} within {} s",
                self.host,
                self.port,
                self.connect_timeout.as_secs()
            ))),
        }
    }

    fn request(
        &self,
        params: &[(&str, &str)],
        body: impl Into<Bytes>,
    ) -> Result<Request<Full<Bytes>>> {
        let mut path = String::from("/?");
        for (i, (name, value)) in params.iter().enumerate() {
            if i > 0 {
                path.push('&');
            }
            path.push_str(&encode(name));
            path.push('=');
            path.push_str(&encode(value));
        }
        let host = if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        };
        let user = if self.user.is_empty() {
            "default"
        } else {
            &self.user
        };
        let mut builder = Request::post(path)
            .header(hyper::header::HOST, host)
            .header(hyper::header::CONTENT_TYPE, "text/plain; charset=UTF-8")
            .header("X-ClickHouse-User", user);
        if let Some(password) = &self.password {
            builder = builder.header("X-ClickHouse-Key", password.expose());
        }
        builder
            .body(Full::new(body.into()))
            .map_err(|e| DriverError::Other(format!("HTTP request: {e}")))
    }
}

/// One connection to the server, opened again when the last one is too old or closed. Never
/// shared: a session's requests go one at a time.
pub struct Client {
    endpoint: Arc<Endpoint>,
    open: Option<(SendRequest<Full<Bytes>>, Instant)>,
}

impl Client {
    pub fn new(endpoint: Arc<Endpoint>) -> Self {
        Self {
            endpoint,
            open: None,
        }
    }

    pub fn endpoint(&self) -> &Arc<Endpoint> {
        &self.endpoint
    }

    /// Sends one request and returns the server's answer once its headers arrive. An answer
    /// that is not `200` is the server's error, read here. A request is sent once: if it is
    /// lost on the way, it is not sent again.
    pub async fn send(
        &mut self,
        params: &[(&str, &str)],
        body: impl Into<Bytes>,
    ) -> Result<Response<Incoming>> {
        let request = self.endpoint.request(params, body)?;
        let reusable = match &mut self.open {
            Some((sender, used)) if used.elapsed() < REUSE_WITHIN && !sender.is_closed() => {
                sender.ready().await.is_ok()
            }
            _ => false,
        };
        if !reusable {
            self.open = Some((self.endpoint.handshake().await?, Instant::now()));
        }
        let (sender, used) = self.open.as_mut().expect("just opened");
        let response = sender.send_request(request).await.map_err(|e| {
            DriverError::Connection(self.endpoint.scrub(&format!("the request was lost: {e}")))
        });
        *used = Instant::now();
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                self.open = None;
                return Err(e);
            }
        };
        if response.status() == StatusCode::OK {
            return Ok(response);
        }
        let status = response.status();
        let body = read_bounded(response.into_body(), MAX_ERROR_BODY).await;
        Err(self.error(status, &body))
    }

    fn error(&self, status: StatusCode, body: &[u8]) -> DriverError {
        let text = self.endpoint.scrub(String::from_utf8_lossy(body).trim());
        if text.contains("DB::Exception") {
            return server_error(&text);
        }
        if text.is_empty() {
            DriverError::Connection(format!("the server answered {status}"))
        } else {
            DriverError::Connection(format!("the server answered {status}: {text}"))
        }
    }
}

/// A request on a connection of its own, outside any session: what a cancel sends while the
/// session's connection is busy with the statement it cancels.
pub async fn send_alone(
    endpoint: &Arc<Endpoint>,
    params: &[(&str, &str)],
    body: impl Into<Bytes>,
) -> Result<Vec<u8>> {
    let mut client = Client::new(endpoint.clone());
    let response = client.send(params, body).await?;
    Ok(read_bounded(response.into_body(), MAX_ERROR_BODY).await)
}

/// Up to `limit` bytes of a body; what does not arrive is left out.
async fn read_bounded(mut body: Incoming, limit: usize) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < limit {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    out.extend_from_slice(&data);
                }
            }
            Some(Err(_)) | None => break,
        }
    }
    out.truncate(limit);
    out
}

/// A ClickHouse exception as a driver error: `Code: 60. DB::Exception: Unknown table … (UNKNOWN_TABLE) (version 25.8.1.1)`.
/// The code is ClickHouse's number; the message keeps the code's name and leaves out the
/// server's version. A cancelled query is [`DriverError::Cancelled`].
pub fn server_error(text: &str) -> DriverError {
    let text = text.trim();
    let (code, rest) = match text
        .strip_prefix("Code: ")
        .and_then(|rest| rest.split_once(". "))
    {
        Some((code, rest)) if code.bytes().all(|b| b.is_ascii_digit()) => {
            (Some(code.to_string()), rest)
        }
        _ => (None, text),
    };
    if code.as_deref() == Some(QUERY_WAS_CANCELLED) {
        return DriverError::Cancelled;
    }
    let message = rest.strip_prefix("DB::Exception: ").unwrap_or(rest);
    let message = match message.rfind(" (version ") {
        Some(end) if message.ends_with(')') => &message[..end],
        _ => message,
    };
    DriverError::Database {
        code,
        message: message.trim().to_string(),
        detail: None,
        hint: None,
        position: None,
    }
}

/// `application/x-www-form-urlencoded`'s encoding of a query parameter.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exceptions_keep_the_code_and_the_name_and_drop_the_version() {
        let error = server_error(
            "Code: 60. DB::Exception: Unknown table expression identifier 'x' in scope SELECT * FROM x. (UNKNOWN_TABLE) (version 25.8.33.6 (official build))\n",
        );
        match error {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("60"));
                assert_eq!(
                    message,
                    "Unknown table expression identifier 'x' in scope SELECT * FROM x. (UNKNOWN_TABLE)"
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            server_error(
                "Code: 394. DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED) (version 25.8.33.6 (official build))"
            ),
            DriverError::Cancelled
        ));
        match server_error("something else") {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code, None);
                assert_eq!(message, "something else");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parameters_are_encoded() {
        assert_eq!(encode("a b&c=d/é"), "a%20b%26c%3Dd%2F%C3%A9");
        assert_eq!(encode("Tab-Separated_1.~"), "Tab-Separated_1.~");
    }
}
