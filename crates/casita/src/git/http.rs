//! Minimal read-only smart-HTTP adapter for one bound native Git view.
//!
//! The adapter deliberately implements only the two stateless upload-pack
//! endpoints. It closes each HTTP/1.1 connection after one response, which is
//! standards-compliant and keeps request parsing small and bounded.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::{BlobStore, GitFetchError, GitFetchService, MetadataStore};

const MAX_HTTP_HEADERS: usize = 64 * 1024;
const STREAM_BUFFER_BYTES: usize = 1024 * 1024;

/// Production resource and deadline controls for the smart-HTTP listener.
#[derive(Clone)]
#[non_exhaustive]
pub struct GitHttpOptions {
    /// Maximum accepted connections being handled simultaneously.
    pub max_connections: usize,
    /// Maximum requests generating packs simultaneously.
    pub max_pack_generations: usize,
    /// Aggregate bytes retained for upload-pack request bodies.
    pub max_inflight_request_bytes: usize,
    /// Per-pack bounded pipe between generation and socket writes.
    pub response_buffer_bytes: usize,
    /// Total time allowed to receive request headers.
    pub header_timeout: Duration,
    /// Total time allowed to queue and receive a request body.
    pub request_body_timeout: Duration,
    /// Maximum time without progress while reading request bytes.
    pub idle_timeout: Duration,
    /// Total lifetime of one accepted HTTP request.
    pub total_request_timeout: Duration,
    /// Maximum time allowed for one response socket write.
    pub response_write_timeout: Duration,
    /// Total time allowed for pack generation, including output backpressure.
    pub pack_generation_timeout: Duration,
    /// Time to drain accepted connections after listener shutdown.
    pub graceful_shutdown_timeout: Duration,
    /// Permit binding directly to a non-loopback address.
    ///
    /// Casita does not provide TLS or client authentication. Set this only
    /// behind a trusted network boundary or an authenticating TLS proxy.
    pub allow_non_loopback: bool,
    /// Optional structured completion hook, called once per accepted socket.
    pub observer: Option<Arc<dyn GitHttpObserver>>,
}

impl std::fmt::Debug for GitHttpOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GitHttpOptions")
            .field("max_connections", &self.max_connections)
            .field("max_pack_generations", &self.max_pack_generations)
            .field(
                "max_inflight_request_bytes",
                &self.max_inflight_request_bytes,
            )
            .field("response_buffer_bytes", &self.response_buffer_bytes)
            .field("header_timeout", &self.header_timeout)
            .field("request_body_timeout", &self.request_body_timeout)
            .field("idle_timeout", &self.idle_timeout)
            .field("total_request_timeout", &self.total_request_timeout)
            .field("response_write_timeout", &self.response_write_timeout)
            .field("pack_generation_timeout", &self.pack_generation_timeout)
            .field("graceful_shutdown_timeout", &self.graceful_shutdown_timeout)
            .field("allow_non_loopback", &self.allow_non_loopback)
            .field("observer", &self.observer.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl Default for GitHttpOptions {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_pack_generations: 2,
            max_inflight_request_bytes: 16 * 1024 * 1024,
            response_buffer_bytes: STREAM_BUFFER_BYTES,
            header_timeout: Duration::from_secs(10),
            request_body_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(15),
            total_request_timeout: Duration::from_secs(10 * 60),
            response_write_timeout: Duration::from_secs(30),
            pack_generation_timeout: Duration::from_secs(10 * 60),
            graceful_shutdown_timeout: Duration::from_secs(30),
            allow_non_loopback: false,
            observer: None,
        }
    }
}

/// Receives a structured outcome after an accepted connection finishes.
pub trait GitHttpObserver: Send + Sync + 'static {
    /// Record one connection outcome. Implementations must return promptly.
    fn observe(&self, event: &GitHttpEvent);
}

impl<F> GitHttpObserver for F
where
    F: Fn(&GitHttpEvent) + Send + Sync + 'static,
{
    fn observe(&self, event: &GitHttpEvent) {
        self(event);
    }
}

/// Structured completion event for logging or metrics adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHttpEvent {
    /// Remote socket address.
    pub peer: SocketAddr,
    /// Final request or connection outcome.
    pub outcome: GitHttpOutcome,
    /// Wall-clock time spent handling this connection.
    pub elapsed: Duration,
}

/// Final outcome for one accepted smart-HTTP connection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum GitHttpOutcome {
    /// One request was answered with the given status.
    Served {
        /// HTTP method.
        method: String,
        /// Exact request target.
        target: String,
        /// HTTP response status.
        status: u16,
    },
    /// The configured simultaneous-connection limit rejected the socket.
    ConnectionLimit,
    /// A configured deadline elapsed.
    TimedOut(GitHttpTimeout),
    /// The peer disconnected or another socket operation failed.
    IoFailure,
    /// Request syntax, authorization, repository, or pack processing failed.
    RequestFailure,
}

/// Deadline stage reported by [`GitHttpOutcome::TimedOut`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GitHttpTimeout {
    /// Receiving HTTP headers.
    Headers,
    /// Waiting for aggregate body budget or receiving the request body.
    RequestBody,
    /// A socket read made no progress.
    Idle,
    /// The complete request exceeded its lifetime.
    TotalRequest,
    /// A socket response write made no progress.
    ResponseWrite,
    /// Pack generation exceeded its lifetime.
    PackGeneration,
    /// Accepted connections did not drain during shutdown.
    GracefulShutdown,
}

#[derive(Debug)]
struct ServedRequest {
    method: String,
    target: String,
    status: u16,
}

/// Listen forever and serve one exact native Git view below `route`.
///
/// `route` must be an absolute path without query or trailing slash, normally
/// `/<view>.git`. Dropping or aborting the returned future stops the listener.
pub async fn serve_git_smart_http<PS, SS>(
    listener: TcpListener,
    route: String,
    service: GitFetchService<PS, SS>,
) -> Result<(), GitHttpError>
where
    PS: BlobStore + Clone + Send + Sync + 'static,
    SS: MetadataStore + Clone + Send + Sync + 'static,
{
    serve_git_smart_http_with_shutdown(
        listener,
        route,
        service,
        GitHttpOptions::default(),
        std::future::pending(),
    )
    .await
}

/// Serve smart HTTP with explicit production limits and graceful shutdown.
///
/// Once `shutdown` resolves, the listener stops accepting connections and
/// waits up to [`GitHttpOptions::graceful_shutdown_timeout`] for accepted
/// requests to finish.
#[tracing::instrument(name = "git.http.serve_with_shutdown", skip_all)]
pub async fn serve_git_smart_http_with_shutdown<PS, SS, F>(
    listener: TcpListener,
    route: String,
    service: GitFetchService<PS, SS>,
    options: GitHttpOptions,
    shutdown: F,
) -> Result<(), GitHttpError>
where
    PS: BlobStore + Clone + Send + Sync + 'static,
    SS: MetadataStore + Clone + Send + Sync + 'static,
    F: Future<Output = ()>,
{
    validate_route(&route)?;
    options.validate(service.limits().max_request_bytes)?;
    let local_address = listener.local_addr()?;
    if !local_address.ip().is_loopback() && !options.allow_non_loopback {
        return Err(GitHttpError::NonLoopbackBind(local_address));
    }
    let connections = Arc::new(Semaphore::new(options.max_connections));
    let pack_generations = Arc::new(Semaphore::new(options.max_pack_generations));
    let request_bytes = Arc::new(Semaphore::new(options.max_inflight_request_bytes));
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            completed = tasks.join_next(), if !tasks.is_empty() => {
                let _ = completed;
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let Ok(connection_permit) = connections.clone().try_acquire_owned() else {
                    if let Some(observer) = &options.observer {
                        observer.observe(&GitHttpEvent {
                            peer,
                            outcome: GitHttpOutcome::ConnectionLimit,
                            elapsed: Duration::ZERO,
                        });
                    }
                    drop(stream);
                    continue;
                };
                let route = route.clone();
                let service = service.clone();
                let options = options.clone();
                let pack_generations = pack_generations.clone();
                let request_bytes = request_bytes.clone();
                tasks.spawn(async move {
                    let started = Instant::now();
                    let result = timeout(
                        options.total_request_timeout,
                        handle_connection(
                            stream,
                            &route,
                            &service,
                            &options,
                            pack_generations,
                            request_bytes,
                        ),
                    )
                    .await;
                    let outcome = match result {
                        Ok(Ok(served)) => GitHttpOutcome::Served {
                            method: served.method,
                            target: served.target,
                            status: served.status,
                        },
                        Err(_) => GitHttpOutcome::TimedOut(GitHttpTimeout::TotalRequest),
                        Ok(Err(GitHttpError::Timeout(stage))) => GitHttpOutcome::TimedOut(stage),
                        Ok(Err(GitHttpError::Io(_))) => GitHttpOutcome::IoFailure,
                        Ok(Err(_)) => GitHttpOutcome::RequestFailure,
                    };
                    if let Some(observer) = &options.observer {
                        observer.observe(&GitHttpEvent {
                            peer,
                            outcome,
                            elapsed: started.elapsed(),
                        });
                    }
                    drop(connection_permit);
                });
            }
        }
    }

    let drain = async { while tasks.join_next().await.is_some() {} };
    if timeout(options.graceful_shutdown_timeout, drain)
        .await
        .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        return Err(GitHttpError::Timeout(GitHttpTimeout::GracefulShutdown));
    }
    Ok(())
}

/// Smart-HTTP listener or request failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GitHttpError {
    /// Socket I/O failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Configured repository route is unsafe or ambiguous.
    #[error("invalid Git HTTP route: {0}")]
    InvalidRoute(String),
    /// HTTP request syntax or framing is invalid.
    #[error("invalid Git HTTP request: {0}")]
    InvalidRequest(String),
    /// Bound fetch service rejected the Git request.
    #[error(transparent)]
    Fetch(#[from] GitFetchError),
    /// A configured server deadline elapsed.
    #[error("Git HTTP {0:?} deadline exceeded")]
    Timeout(GitHttpTimeout),
    /// Server resource options are internally inconsistent.
    #[error("invalid Git HTTP options: {0}")]
    InvalidOptions(String),
    /// Direct non-loopback exposure was not explicitly enabled.
    #[error("Git HTTP refuses non-loopback bind {0} without explicit trust-boundary opt-in")]
    NonLoopbackBind(SocketAddr),
}

impl GitHttpOptions {
    fn validate(&self, max_request_bytes: usize) -> Result<(), GitHttpError> {
        for (name, value) in [
            ("max_connections", self.max_connections),
            ("max_pack_generations", self.max_pack_generations),
        ] {
            if value == 0 {
                return Err(GitHttpError::InvalidOptions(format!(
                    "{name} must be greater than zero"
                )));
            }
            if value > Semaphore::MAX_PERMITS {
                return Err(GitHttpError::InvalidOptions(format!(
                    "{name} exceeds Tokio's semaphore limit"
                )));
            }
        }
        if self.response_buffer_bytes == 0 {
            return Err(GitHttpError::InvalidOptions(
                "response_buffer_bytes must be greater than zero".into(),
            ));
        }
        if self.max_inflight_request_bytes < max_request_bytes {
            return Err(GitHttpError::InvalidOptions(format!(
                "max_inflight_request_bytes ({}) must cover max_request_bytes ({max_request_bytes})",
                self.max_inflight_request_bytes
            )));
        }
        if self.max_inflight_request_bytes > u32::MAX as usize
            || self.max_inflight_request_bytes > Semaphore::MAX_PERMITS
        {
            return Err(GitHttpError::InvalidOptions(
                "max_inflight_request_bytes exceeds the semaphore permit limit".into(),
            ));
        }
        self.max_pack_generations
            .checked_mul(self.response_buffer_bytes)
            .ok_or_else(|| {
                GitHttpError::InvalidOptions(
                    "aggregate streaming response buffer size overflows usize".into(),
                )
            })?;
        let deadlines = [
            ("header_timeout", self.header_timeout),
            ("request_body_timeout", self.request_body_timeout),
            ("idle_timeout", self.idle_timeout),
            ("total_request_timeout", self.total_request_timeout),
            ("response_write_timeout", self.response_write_timeout),
            ("pack_generation_timeout", self.pack_generation_timeout),
            ("graceful_shutdown_timeout", self.graceful_shutdown_timeout),
        ];
        if let Some((name, _)) = deadlines.into_iter().find(|(_, value)| value.is_zero()) {
            return Err(GitHttpError::InvalidOptions(format!(
                "{name} must be greater than zero"
            )));
        }
        Ok(())
    }
}

fn validate_route(route: &str) -> Result<(), GitHttpError> {
    if !route.starts_with('/')
        || route == "/"
        || route.ends_with('/')
        || route.contains('?')
        || route.contains('#')
        || route.contains("..")
        || route.bytes().any(|byte| byte <= 0x20 || byte == 0x7f)
    {
        return Err(GitHttpError::InvalidRoute(route.to_owned()));
    }
    Ok(())
}

#[tracing::instrument(name = "git.http.request", skip_all)]
async fn handle_connection<PS, SS>(
    mut stream: TcpStream,
    route: &str,
    service: &GitFetchService<PS, SS>,
    options: &GitHttpOptions,
    pack_generations: Arc<Semaphore>,
    request_bytes: Arc<Semaphore>,
) -> Result<ServedRequest, GitHttpError>
where
    PS: BlobStore + Clone,
    SS: MetadataStore + Clone,
{
    let mut request = Vec::new();
    let header_end = timeout(options.header_timeout, async {
        loop {
            if request.len() >= MAX_HTTP_HEADERS {
                return Ok(None);
            }
            let mut buffer = [0u8; 4096];
            let read = timeout(options.idle_timeout, stream.read(&mut buffer))
                .await
                .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::Idle))??;
            if read == 0 {
                return Err(GitHttpError::InvalidRequest(
                    "connection ended before headers".into(),
                ));
            }
            request.extend_from_slice(&buffer[..read]);
            if let Some(offset) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                return Ok(Some(offset + 4));
            }
        }
    })
    .await
    .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::Headers))??;
    let Some(header_end) = header_end else {
        write_response(
            &mut stream,
            431,
            "text/plain; charset=utf-8",
            b"request headers too large\n",
            options,
        )
        .await?;
        return Ok(ServedRequest {
            method: String::new(),
            target: String::new(),
            status: 431,
        });
    };
    let headers = std::str::from_utf8(&request[..header_end])
        .map_err(|_| GitHttpError::InvalidRequest("headers are not UTF-8/ASCII".into()))?;
    let mut lines = headers.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| GitHttpError::InvalidRequest("missing request line".into()))?;
    let mut parts = request_line.split_ascii_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| GitHttpError::InvalidRequest("missing method".into()))?
        .to_owned();
    let target = parts
        .next()
        .ok_or_else(|| GitHttpError::InvalidRequest("missing request target".into()))?
        .to_owned();
    if parts.next() != Some("HTTP/1.1") || parts.next().is_some() {
        return Err(GitHttpError::InvalidRequest(
            "only one HTTP/1.1 request line is accepted".into(),
        ));
    }
    let BodyHeaders {
        framing,
        coding,
        expect_continue,
    } = parse_body_headers(lines)?;

    if method == "GET" {
        let expected = format!("{route}/info/refs?service=git-upload-pack");
        if target != expected {
            write_response(
                &mut stream,
                404,
                "text/plain; charset=utf-8",
                b"not found\n",
                options,
            )
            .await?;
            return Ok(ServedRequest {
                method,
                target,
                status: 404,
            });
        }
        let status = match service.info_refs() {
            Ok(body) => {
                write_response(
                    &mut stream,
                    200,
                    "application/x-git-upload-pack-advertisement",
                    &body,
                    options,
                )
                .await?;
                200
            }
            Err(error) => write_fetch_error(&mut stream, error, options).await?,
        };
        return Ok(ServedRequest {
            method,
            target,
            status,
        });
    }

    if method != "POST" || target != format!("{route}/git-upload-pack") {
        write_response(
            &mut stream,
            404,
            "text/plain; charset=utf-8",
            b"not found\n",
            options,
        )
        .await?;
        return Ok(ServedRequest {
            method,
            target,
            status: 404,
        });
    }
    let max_request_bytes = service.limits().max_request_bytes;
    let rejection = match (framing, coding) {
        (BodyFraming::Missing, _) => {
            return Err(GitHttpError::InvalidRequest(
                "POST requires Content-Length or chunked Transfer-Encoding".into(),
            ));
        }
        (BodyFraming::Unsupported, _) => Some((501, b"unsupported Transfer-Encoding\n".as_slice())),
        (_, BodyCoding::Unsupported) => Some((415, b"unsupported Content-Encoding\n".as_slice())),
        (BodyFraming::Length(length), _) if length > max_request_bytes => {
            Some((413, b"upload-pack request too large\n".as_slice()))
        }
        _ => None,
    };
    if let Some((status, body)) = rejection {
        write_response(
            &mut stream,
            status,
            "text/plain; charset=utf-8",
            body,
            options,
        )
        .await?;
        return Ok(ServedRequest {
            method,
            target,
            status,
        });
    }
    if expect_continue {
        write_all_timed(
            &mut stream,
            b"HTTP/1.1 100 Continue\r\n\r\n",
            options.response_write_timeout,
        )
        .await?;
    }
    // Only an identity body of declared length has a known size up front.
    // Chunked or gzip bodies reserve the whole decoded limit instead.
    let budget = match (framing, coding) {
        (BodyFraming::Length(length), BodyCoding::Identity) => length,
        _ => max_request_bytes,
    };
    let body_permits = u32::try_from(budget)
        .map_err(|_| GitHttpError::InvalidRequest("request body length exceeds u32".into()))?;
    let received = timeout(options.request_body_timeout, async {
        let permit = if body_permits == 0 {
            None
        } else {
            Some(
                request_bytes
                    .acquire_many_owned(body_permits)
                    .await
                    .map_err(|_| {
                        GitHttpError::InvalidOptions("request byte budget closed".into())
                    })?,
            )
        };
        let body = read_request_body(
            &mut stream,
            &request[header_end..],
            framing,
            coding,
            max_request_bytes,
            options.idle_timeout,
        )
        .await?;
        Ok::<_, BodyError>((permit, body))
    })
    .await
    .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::RequestBody))?;
    let (_body_budget, body) = match received {
        Ok(received) => received,
        Err(BodyError::TooLarge) => {
            write_response(
                &mut stream,
                413,
                "text/plain; charset=utf-8",
                b"upload-pack request too large\n",
                options,
            )
            .await?;
            return Ok(ServedRequest {
                method,
                target,
                status: 413,
            });
        }
        Err(BodyError::Http(error)) => return Err(error),
    };
    if body == b"0000" {
        // Before streaming a request larger than `http.postBuffer`, Git
        // probes the endpoint with a lone flush packet and expects the empty
        // success that upload-pack gives a request without wants.
        write_response(
            &mut stream,
            200,
            "application/x-git-upload-pack-result",
            b"",
            options,
        )
        .await?;
        return Ok(ServedRequest {
            method,
            target,
            status: 200,
        });
    }
    match service.prepare_upload_pack(&body).await {
        Ok(prepared) => {
            let pack_deadline = prepared
                .done()
                .then(|| tokio::time::Instant::now() + options.pack_generation_timeout);
            let _pack_permit = if let Some(deadline) = pack_deadline {
                Some(
                    tokio::time::timeout_at(deadline, pack_generations.acquire_owned())
                        .await
                        .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::PackGeneration))?
                        .map_err(|_| {
                            GitHttpError::InvalidOptions("pack semaphore closed".into())
                        })?,
                )
            } else {
                None
            };
            write_streaming_upload_pack(&mut stream, service, prepared, options, pack_deadline)
                .await?;
            Ok(ServedRequest {
                method,
                target,
                status: 200,
            })
        }
        Err(error) => {
            let status = write_fetch_error(&mut stream, error, options).await?;
            Ok(ServedRequest {
                method,
                target,
                status,
            })
        }
    }
}

#[tracing::instrument(name = "git.http.stream_pack", skip_all)]
async fn write_streaming_upload_pack<PS, SS>(
    stream: &mut TcpStream,
    service: &GitFetchService<PS, SS>,
    prepared: crate::git::fetch::PreparedUploadPack,
    options: &GitHttpOptions,
    pack_deadline: Option<tokio::time::Instant>,
) -> Result<(), GitHttpError>
where
    PS: BlobStore + Clone,
    SS: MetadataStore + Clone,
{
    let content_length = prepared.response_bytes();
    write_response_header(
        stream,
        200,
        "application/x-git-upload-pack-result",
        content_length,
        options,
    )
    .await?;
    let aggregate_transport_chunks = prepared.uses_cached_pack();
    let (mut producer, mut consumer) = tokio::io::duplex(options.response_buffer_bytes);
    let generate = async {
        let result = if let Some(deadline) = pack_deadline {
            tokio::time::timeout_at(
                deadline,
                service.write_prepared_upload_pack_to(prepared, &mut producer),
            )
            .await
            .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::PackGeneration))?
        } else {
            service
                .write_prepared_upload_pack_to(prepared, &mut producer)
                .await
        };
        producer.shutdown().await?;
        result.map_err(GitHttpError::Fetch)
    };
    let send = async {
        let mut buffer = vec![0u8; options.response_buffer_bytes];
        loop {
            // Side-band payloads arrive in roughly 64 KiB writes. Fill one
            // transport buffer before emitting an HTTP chunk so a multi-GiB
            // pack does not turn into three timed socket writes per side-band
            // frame. EOF still flushes the final partial chunk.
            let mut read = 0;
            while read < buffer.len() {
                let next = consumer.read(&mut buffer[read..]).await?;
                if next == 0 {
                    break;
                }
                read += next;
                if !aggregate_transport_chunks {
                    break;
                }
            }
            if read == 0 {
                break;
            }
            if content_length.is_some() {
                write_all_timed(stream, &buffer[..read], options.response_write_timeout).await?;
            } else {
                write_all_timed(
                    stream,
                    format!("{read:x}\r\n").as_bytes(),
                    options.response_write_timeout,
                )
                .await?;
                write_all_timed(stream, &buffer[..read], options.response_write_timeout).await?;
                write_all_timed(stream, b"\r\n", options.response_write_timeout).await?;
            }
        }
        Ok::<(), GitHttpError>(())
    };
    tokio::try_join!(generate, send)?;
    if content_length.is_none() {
        write_all_timed(stream, b"0\r\n\r\n", options.response_write_timeout).await?;
    }
    timeout(options.response_write_timeout, stream.shutdown())
        .await
        .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::ResponseWrite))??;
    Ok(())
}

async fn write_fetch_error(
    stream: &mut TcpStream,
    error: GitFetchError,
    options: &GitHttpOptions,
) -> Result<u16, GitHttpError> {
    let status = match error {
        GitFetchError::UnauthorizedOid(_) => 403,
        GitFetchError::Protocol(_) | GitFetchError::Limit(_) => 400,
        _ => 500,
    };
    write_response(
        stream,
        status,
        "text/plain; charset=utf-8",
        format!("{error}\n").as_bytes(),
        options,
    )
    .await?;
    Ok(status)
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    options: &GitHttpOptions,
) -> Result<(), GitHttpError> {
    write_response_header(stream, status, content_type, Some(body.len()), options).await?;
    write_all_timed(stream, body, options.response_write_timeout).await?;
    timeout(options.response_write_timeout, stream.shutdown())
        .await
        .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::ResponseWrite))??;
    Ok(())
}

async fn write_response_header(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    content_length: Option<usize>,
    options: &GitHttpOptions,
) -> Result<(), GitHttpError> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Content Too Large",
        415 => "Unsupported Media Type",
        431 => "Request Header Fields Too Large",
        501 => "Not Implemented",
        _ => "Internal Server Error",
    };
    let framing = content_length.map_or_else(
        || "Transfer-Encoding: chunked\r\n".to_owned(),
        |length| format!("Content-Length: {length}\r\n"),
    );
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n{framing}Cache-Control: no-cache\r\nConnection: close\r\n\r\n"
    );
    write_all_timed(stream, header.as_bytes(), options.response_write_timeout).await
}

async fn write_all_timed(
    stream: &mut TcpStream,
    bytes: &[u8],
    deadline: Duration,
) -> Result<(), GitHttpError> {
    timeout(deadline, stream.write_all(bytes))
        .await
        .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::ResponseWrite))??;
    Ok(())
}

/// How the request headers delimit the message body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyFraming {
    /// Neither Content-Length nor Transfer-Encoding was sent.
    Missing,
    /// Exactly this many raw bytes follow the headers.
    Length(usize),
    /// Exactly the `chunked` transfer coding.
    Chunked,
    /// Any other transfer coding list.
    Unsupported,
}

/// Content coding applied to the upload-pack request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyCoding {
    Identity,
    Gzip,
    Unsupported,
}

#[derive(Debug, PartialEq, Eq)]
struct BodyHeaders {
    framing: BodyFraming,
    coding: BodyCoding,
    expect_continue: bool,
}

/// Interpret the header fields which govern body framing. Ambiguous framing
/// fails the request outright, because a proxy in front of this listener may
/// have delimited the body differently (request smuggling).
fn parse_body_headers<'a>(
    lines: impl Iterator<Item = &'a str>,
) -> Result<BodyHeaders, GitHttpError> {
    let mut content_length = None;
    let mut transfer_encoding = None;
    let mut content_encoding = None;
    let mut expect_continue = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| GitHttpError::InvalidRequest("malformed header".into()))?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(GitHttpError::InvalidRequest(
                    "duplicate Content-Length".into(),
                ));
            }
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(GitHttpError::InvalidRequest(
                    "invalid Content-Length".into(),
                ));
            }
            content_length = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| GitHttpError::InvalidRequest("invalid Content-Length".into()))?,
            );
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if transfer_encoding.is_some() {
                return Err(GitHttpError::InvalidRequest(
                    "duplicate Transfer-Encoding".into(),
                ));
            }
            transfer_encoding = Some(value);
        } else if name.eq_ignore_ascii_case("content-encoding") {
            if content_encoding.is_some() {
                return Err(GitHttpError::InvalidRequest(
                    "duplicate Content-Encoding".into(),
                ));
            }
            content_encoding = Some(value);
        } else if name.eq_ignore_ascii_case("expect") && value.eq_ignore_ascii_case("100-continue")
        {
            expect_continue = true;
        }
    }
    let framing = match (content_length, transfer_encoding) {
        (Some(_), Some(_)) => {
            return Err(GitHttpError::InvalidRequest(
                "Content-Length and Transfer-Encoding are mutually exclusive".into(),
            ));
        }
        (Some(length), None) => BodyFraming::Length(length),
        (None, Some(coding)) if coding.eq_ignore_ascii_case("chunked") => BodyFraming::Chunked,
        (None, Some(_)) => BodyFraming::Unsupported,
        (None, None) => BodyFraming::Missing,
    };
    let coding = match content_encoding {
        None => BodyCoding::Identity,
        Some(coding) if coding.eq_ignore_ascii_case("identity") => BodyCoding::Identity,
        Some(coding)
            if coding.eq_ignore_ascii_case("gzip") || coding.eq_ignore_ascii_case("x-gzip") =>
        {
            BodyCoding::Gzip
        }
        Some(_) => BodyCoding::Unsupported,
    };
    Ok(BodyHeaders {
        framing,
        coding,
        expect_continue,
    })
}

const BODY_READ_BYTES: usize = 64 * 1024;
const MAX_CHUNK_SIZE_DIGITS: usize = 16;

#[derive(Debug)]
enum BodyError {
    /// The raw or decoded body exceeds the request limit.
    TooLarge,
    Http(GitHttpError),
}

impl From<GitHttpError> for BodyError {
    fn from(error: GitHttpError) -> Self {
        Self::Http(error)
    }
}

impl From<std::io::Error> for BodyError {
    fn from(error: std::io::Error) -> Self {
        Self::Http(GitHttpError::Io(error))
    }
}

fn invalid_body(message: &str) -> BodyError {
    BodyError::Http(GitHttpError::InvalidRequest(message.into()))
}

/// Decoded request body which refuses to grow past its limit.
struct BoundedBody {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl std::io::Write for BoundedBody {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.limit - self.bytes.len() {
            self.exceeded = true;
            return Err(std::io::Error::other("request body exceeds limit"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Streaming content decoder. Gzip output is bounded as it is inflated, so a
/// small compressed body cannot expand past the request limit in memory.
enum BodyDecoder {
    Identity(BoundedBody),
    Gzip(Box<flate2::write::GzDecoder<BoundedBody>>),
}

impl BodyDecoder {
    fn new(coding: BodyCoding, limit: usize, capacity: usize) -> Result<Self, BodyError> {
        let body = BoundedBody {
            bytes: Vec::with_capacity(capacity.min(limit)),
            limit,
            exceeded: false,
        };
        match coding {
            BodyCoding::Identity => Ok(Self::Identity(body)),
            BodyCoding::Gzip => Ok(Self::Gzip(Box::new(flate2::write::GzDecoder::new(body)))),
            BodyCoding::Unsupported => Err(invalid_body("unsupported Content-Encoding")),
        }
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), BodyError> {
        use std::io::Write;
        let result = match self {
            Self::Identity(body) => body.write_all(bytes),
            Self::Gzip(decoder) => decoder.write_all(bytes),
        };
        result.map_err(|_| self.error())
    }

    fn finish(self) -> Result<Vec<u8>, BodyError> {
        match self {
            Self::Identity(body) => Ok(body.bytes),
            Self::Gzip(mut decoder) => {
                if decoder.try_finish().is_err() {
                    return Err(Self::Gzip(decoder).error());
                }
                Ok(std::mem::take(&mut decoder.get_mut().bytes))
            }
        }
    }

    fn error(&self) -> BodyError {
        let exceeded = match self {
            Self::Identity(body) => body.exceeded,
            Self::Gzip(decoder) => decoder.get_ref().exceeded,
        };
        if exceeded {
            BodyError::TooLarge
        } else {
            invalid_body("invalid gzip request body")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    Size,
    Data(u64),
    DataCr,
    DataLf,
    LastCr,
    LastLf,
    Done,
}

/// Strict `chunked` transfer decoder: hexadecimal sizes of at most sixteen
/// digits, no chunk extensions, no trailer fields, and CRLF line endings.
struct ChunkedDecoder {
    state: ChunkState,
    size_line: Vec<u8>,
}

impl ChunkedDecoder {
    fn new() -> Self {
        Self {
            state: ChunkState::Size,
            size_line: Vec::with_capacity(MAX_CHUNK_SIZE_DIGITS + 1),
        }
    }

    fn done(&self) -> bool {
        self.state == ChunkState::Done
    }

    /// Feed raw bytes and return how many belong to the chunked body.
    fn feed(&mut self, input: &[u8], output: &mut BodyDecoder) -> Result<usize, BodyError> {
        let mut consumed = 0;
        while consumed < input.len() {
            let byte = input[consumed];
            self.state = match self.state {
                ChunkState::Done => break,
                ChunkState::Size => {
                    consumed += 1;
                    if byte == b'\n' {
                        let size = self.parse_size()?;
                        if size == 0 {
                            ChunkState::LastCr
                        } else {
                            ChunkState::Data(size)
                        }
                    } else if self.size_line.len() > MAX_CHUNK_SIZE_DIGITS {
                        return Err(invalid_body("invalid chunk size"));
                    } else {
                        self.size_line.push(byte);
                        ChunkState::Size
                    }
                }
                ChunkState::Data(remaining) => {
                    let available = input.len() - consumed;
                    let take = usize::try_from(remaining)
                        .map_or(available, |remaining| remaining.min(available));
                    output.write(&input[consumed..consumed + take])?;
                    consumed += take;
                    let remaining = remaining - take as u64;
                    if remaining == 0 {
                        ChunkState::DataCr
                    } else {
                        ChunkState::Data(remaining)
                    }
                }
                ChunkState::DataCr | ChunkState::LastCr if byte != b'\r' => {
                    return Err(invalid_body("chunk is not terminated by CRLF"));
                }
                ChunkState::DataLf | ChunkState::LastLf if byte != b'\n' => {
                    return Err(invalid_body("chunk is not terminated by CRLF"));
                }
                ChunkState::DataCr => {
                    consumed += 1;
                    ChunkState::DataLf
                }
                ChunkState::DataLf => {
                    consumed += 1;
                    ChunkState::Size
                }
                ChunkState::LastCr => {
                    consumed += 1;
                    ChunkState::LastLf
                }
                ChunkState::LastLf => {
                    consumed += 1;
                    ChunkState::Done
                }
            };
        }
        Ok(consumed)
    }

    fn parse_size(&mut self) -> Result<u64, BodyError> {
        let digits = self
            .size_line
            .strip_suffix(b"\r")
            .filter(|digits| {
                !digits.is_empty()
                    && digits.len() <= MAX_CHUNK_SIZE_DIGITS
                    && digits.iter().all(u8::is_ascii_hexdigit)
            })
            .ok_or_else(|| invalid_body("invalid chunk size"))?;
        let size = digits.iter().fold(0u64, |size, digit| {
            (size << 4) | u64::from(char::from(*digit).to_digit(16).unwrap_or_default())
        });
        self.size_line.clear();
        Ok(size)
    }
}

/// Read and decode one request body. `received` holds the body bytes which
/// arrived with the headers. Both the raw bytes after the headers and the
/// decoded body are held to `limit`.
async fn read_request_body<R>(
    stream: &mut R,
    received: &[u8],
    framing: BodyFraming,
    coding: BodyCoding,
    limit: usize,
    idle_timeout: Duration,
) -> Result<Vec<u8>, BodyError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut read_timed = async |buffer: &mut [u8]| {
        let read = timeout(idle_timeout, stream.read(buffer))
            .await
            .map_err(|_| GitHttpError::Timeout(GitHttpTimeout::Idle))??;
        if read == 0 {
            return Err(invalid_body("connection ended before request body"));
        }
        Ok::<_, BodyError>(read)
    };
    match framing {
        BodyFraming::Length(length) => {
            if length > limit {
                return Err(BodyError::TooLarge);
            }
            if received.len() > length {
                return Err(invalid_body("request carries bytes beyond Content-Length"));
            }
            let capacity = if coding == BodyCoding::Identity {
                length
            } else {
                0
            };
            let mut decoder = BodyDecoder::new(coding, limit, capacity)?;
            decoder.write(received)?;
            let mut remaining = length - received.len();
            let mut buffer = vec![0u8; remaining.min(BODY_READ_BYTES)];
            while remaining > 0 {
                let window = remaining.min(buffer.len());
                let read = read_timed(&mut buffer[..window]).await?;
                decoder.write(&buffer[..read])?;
                remaining -= read;
            }
            decoder.finish()
        }
        BodyFraming::Chunked => {
            if received.len() > limit {
                return Err(BodyError::TooLarge);
            }
            let mut decoder = BodyDecoder::new(coding, limit, 0)?;
            let mut chunked = ChunkedDecoder::new();
            let mut raw = received.len();
            let mut surplus = received.len() - chunked.feed(received, &mut decoder)?;
            let mut buffer = vec![0u8; BODY_READ_BYTES];
            while !chunked.done() {
                let read = read_timed(&mut buffer).await?;
                raw += read;
                if raw > limit {
                    return Err(BodyError::TooLarge);
                }
                surplus = read - chunked.feed(&buffer[..read], &mut decoder)?;
            }
            if surplus > 0 {
                return Err(invalid_body(
                    "request carries bytes beyond the chunked body",
                ));
            }
            decoder.finish()
        }
        BodyFraming::Missing | BodyFraming::Unsupported => {
            Err(invalid_body("request body framing is not supported"))
        }
    }
}

#[cfg(test)]
mod body_tests {
    use std::io::Write;

    use super::*;

    const LIMIT: usize = 64 * 1024;

    fn headers(fields: &[&str]) -> Result<BodyHeaders, GitHttpError> {
        parse_body_headers(fields.iter().copied())
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn chunked(bytes: &[u8], chunk: usize) -> Vec<u8> {
        let mut framed = Vec::new();
        for part in bytes.chunks(chunk) {
            framed.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
            framed.extend_from_slice(part);
            framed.extend_from_slice(b"\r\n");
        }
        framed.extend_from_slice(b"0\r\n\r\n");
        framed
    }

    fn payload() -> Vec<u8> {
        (0..10_000u32)
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    /// Deliver `raw` split between bytes that arrived with the headers and
    /// bytes still waiting on the socket.
    async fn read(
        raw: &[u8],
        split: usize,
        framing: BodyFraming,
        coding: BodyCoding,
    ) -> Result<Vec<u8>, BodyError> {
        let (received, mut rest) = raw.split_at(split.min(raw.len()));
        read_request_body(
            &mut rest,
            received,
            framing,
            coding,
            LIMIT,
            Duration::from_secs(60),
        )
        .await
    }

    fn assert_invalid(result: Result<Vec<u8>, BodyError>) {
        assert!(
            matches!(
                result,
                Err(BodyError::Http(GitHttpError::InvalidRequest(_)))
            ),
            "{result:?}"
        );
    }

    #[test]
    fn header_framing_and_coding_are_recognised() {
        assert_eq!(
            headers(&["Content-Length: 12", "Content-Encoding: gzip"]).unwrap(),
            BodyHeaders {
                framing: BodyFraming::Length(12),
                coding: BodyCoding::Gzip,
                expect_continue: false,
            }
        );
        assert_eq!(
            headers(&["Transfer-Encoding: Chunked", "Expect: 100-continue"]).unwrap(),
            BodyHeaders {
                framing: BodyFraming::Chunked,
                coding: BodyCoding::Identity,
                expect_continue: true,
            }
        );
        let parsed = headers(&["Transfer-Encoding: gzip, chunked"]).unwrap();
        assert_eq!(parsed.framing, BodyFraming::Unsupported);
        let parsed = headers(&["Content-Length: 1", "Content-Encoding: br"]).unwrap();
        assert_eq!(parsed.coding, BodyCoding::Unsupported);
        let parsed = headers(&["Content-Length: 1", "Content-Encoding: gzip, gzip"]).unwrap();
        assert_eq!(parsed.coding, BodyCoding::Unsupported);
        assert_eq!(headers(&[]).unwrap().framing, BodyFraming::Missing);
    }

    #[test]
    fn ambiguous_framing_headers_are_rejected() {
        for fields in [
            &["Content-Length: 4", "Transfer-Encoding: chunked"][..],
            &["Transfer-Encoding: chunked", "Content-Length: 4"],
            &["Transfer-Encoding: chunked", "Transfer-Encoding: chunked"],
            &["Content-Length: 4", "Content-Length: 4"],
            &["Content-Length: +4"],
            &["Content-Length: 4, 4"],
            &["Content-Length:"],
            &["Content-Encoding: gzip", "Content-Encoding: gzip"],
        ] {
            assert!(
                matches!(headers(fields), Err(GitHttpError::InvalidRequest(_))),
                "{fields:?}"
            );
        }
    }

    #[tokio::test]
    async fn gzip_length_body_is_decoded() {
        let payload = payload();
        let raw = gzip(&payload);
        for split in [0, 7, raw.len()] {
            let body = read(
                &raw,
                split,
                BodyFraming::Length(raw.len()),
                BodyCoding::Gzip,
            )
            .await
            .unwrap();
            assert_eq!(body, payload);
        }
    }

    #[tokio::test]
    async fn chunked_body_is_decoded() {
        let payload = payload();
        for chunk in [64, 1000, payload.len()] {
            let raw = chunked(&payload, chunk);
            for split in [0, 3, raw.len()] {
                let body = read(&raw, split, BodyFraming::Chunked, BodyCoding::Identity)
                    .await
                    .unwrap();
                assert_eq!(body, payload);
            }
        }
        let empty = read(b"0\r\n\r\n", 0, BodyFraming::Chunked, BodyCoding::Identity)
            .await
            .unwrap();
        assert!(empty.is_empty());
        let uppercase = read(
            b"00A\r\n0123456789\r\n0\r\n\r\n",
            0,
            BodyFraming::Chunked,
            BodyCoding::Identity,
        )
        .await
        .unwrap();
        assert_eq!(uppercase, b"0123456789");
    }

    #[tokio::test]
    async fn chunked_gzip_body_is_decoded() {
        let payload = payload();
        let raw = chunked(&gzip(&payload), 333);
        let body = read(&raw, 10, BodyFraming::Chunked, BodyCoding::Gzip)
            .await
            .unwrap();
        assert_eq!(body, payload);
    }

    #[tokio::test]
    async fn gzip_bomb_is_rejected_at_the_decoded_limit() {
        let bomb = gzip(&vec![0u8; 64 * LIMIT]);
        assert!(bomb.len() < LIMIT / 8, "the bomb must be small on the wire");
        let result = read(&bomb, 0, BodyFraming::Length(bomb.len()), BodyCoding::Gzip).await;
        assert!(matches!(result, Err(BodyError::TooLarge)), "{result:?}");
        let raw = chunked(&bomb, 4096);
        let result = read(&raw, 0, BodyFraming::Chunked, BodyCoding::Gzip).await;
        assert!(matches!(result, Err(BodyError::TooLarge)), "{result:?}");
    }

    #[tokio::test]
    async fn limits_include_their_boundary() {
        let raw = gzip(&vec![7u8; LIMIT]);
        let body = read(&raw, 0, BodyFraming::Length(raw.len()), BodyCoding::Gzip)
            .await
            .unwrap();
        assert_eq!(body.len(), LIMIT);
        let raw = gzip(&vec![7u8; LIMIT + 1]);
        let result = read(&raw, 0, BodyFraming::Length(raw.len()), BodyCoding::Gzip).await;
        assert!(matches!(result, Err(BodyError::TooLarge)), "{result:?}");

        // One chunk of four hex digits adds thirteen framing bytes, which
        // count against the raw limit.
        let payload = vec![7u8; LIMIT - 13];
        let raw = chunked(&payload, LIMIT);
        assert_eq!(raw.len(), LIMIT);
        let body = read(&raw, 0, BodyFraming::Chunked, BodyCoding::Identity)
            .await
            .unwrap();
        assert_eq!(body, payload);
        let raw = chunked(&vec![7u8; LIMIT - 12], LIMIT);
        let result = read(&raw, 0, BodyFraming::Chunked, BodyCoding::Identity).await;
        assert!(matches!(result, Err(BodyError::TooLarge)), "{result:?}");
    }

    #[tokio::test]
    async fn oversized_raw_chunked_stream_is_rejected() {
        // One byte chunks carry five framing bytes each, so the raw limit
        // stops this stream long before the decoded body reaches it.
        let mut raw = Vec::new();
        while raw.len() <= LIMIT {
            raw.extend_from_slice(b"1\r\na\r\n");
        }
        let result = read(&raw, 0, BodyFraming::Chunked, BodyCoding::Identity).await;
        assert!(matches!(result, Err(BodyError::TooLarge)), "{result:?}");
        let result = read(
            &[0u8; 16],
            0,
            BodyFraming::Length(LIMIT + 1),
            BodyCoding::Identity,
        )
        .await;
        assert!(matches!(result, Err(BodyError::TooLarge)), "{result:?}");
    }

    #[tokio::test]
    async fn malformed_chunked_framing_is_rejected() {
        for raw in [
            &b"\r\n"[..],
            b"g\r\nx\r\n0\r\n\r\n",
            b"1 \r\nx\r\n0\r\n\r\n",
            b" 1\r\nx\r\n0\r\n\r\n",
            b"+1\r\nx\r\n0\r\n\r\n",
            b"-1\r\nx\r\n0\r\n\r\n",
            b"0x1\r\nx\r\n0\r\n\r\n",
            b"1;name=value\r\nx\r\n0\r\n\r\n",
            b"1\nx\r\n0\r\n\r\n",
            b"00000000000000001\r\nx\r\n0\r\n\r\n",
            b"ffffffffffffffffff\r\n",
            b"1\r\nxy\r\n0\r\n\r\n",
            b"1\r\nx\n0\r\n\r\n",
            b"1\r\nx\r\n0\r\nTrailer: value\r\n\r\n",
            b"1\r\nx\r\n0\r\n\n",
            b"1\r\nx\r\n0\r\n\r\nextra",
            b"1\r\nx\r\n",
            b"5\r\nabc",
        ] {
            for split in [0, raw.len()] {
                assert_invalid(read(raw, split, BodyFraming::Chunked, BodyCoding::Identity).await);
            }
        }
    }

    #[tokio::test]
    async fn malformed_gzip_is_rejected() {
        let payload = payload();
        let valid = gzip(&payload);
        let truncated = &valid[..valid.len() - 4];
        let mut trailing = valid.clone();
        trailing.extend_from_slice(b"0000");
        let mut corrupt_crc = valid.clone();
        let crc = corrupt_crc.len() - 8;
        corrupt_crc[crc] ^= 1;
        for raw in [&b""[..], b"0000", truncated, &trailing, &corrupt_crc] {
            assert_invalid(read(raw, 0, BodyFraming::Length(raw.len()), BodyCoding::Gzip).await);
        }
        assert_invalid(read(b"0000", 4, BodyFraming::Length(2), BodyCoding::Identity).await);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::{
        CanonicalRefName, GitFetchLimits, GitFetchRequest, NativeGitImportOptions,
        repository::Repository,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unmodified_git_client_clones_and_checks_out_a_shallow_view() {
        let source = tempfile::tempdir().unwrap();
        let git = |directory: &std::path::Path, args: &[&str]| {
            let output = Command::new("git")
                .current_dir(directory)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@example.com")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@example.com")
                .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
                .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        git(source.path(), &["init", "-q", "-b", "main"]);
        std::fs::write(source.path().join("hello.txt"), b"first\n").unwrap();
        let mut randomish = Vec::with_capacity(256 * 1024);
        let mut state = 0x1234_5678u32;
        for _ in 0..randomish.capacity() {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            randomish.push(state as u8);
        }
        std::fs::write(source.path().join("random.bin"), &randomish).unwrap();
        git(source.path(), &["add", "hello.txt", "random.bin"]);
        git(source.path(), &["commit", "-q", "-m", "first"]);
        std::fs::write(source.path().join("hello.txt"), b"second\n").unwrap();
        git(source.path(), &["commit", "-q", "-am", "second"]);
        git(source.path(), &["gc", "--quiet"]);

        let repository = Repository::memory().unwrap();
        repository
            .import_native_git_view(
                source.path(),
                &NativeGitImportOptions {
                    view_name: "origin".into(),
                    refs: Vec::new(),
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        let (_, imported_view) = crate::read_git_view(&repository, "origin")
            .await
            .unwrap()
            .unwrap();
        assert!(
            imported_view.pack.is_some(),
            "an exact source-native pack should be retained for full clones"
        );
        let uncached_repository = Repository::memory().unwrap();
        uncached_repository
            .import_native_git_view(
                source.path(),
                &NativeGitImportOptions {
                    view_name: "uncached".into(),
                    max_cached_pack_bytes: 0,
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        let (_, uncached_view) = crate::read_git_view(&uncached_repository, "uncached")
            .await
            .unwrap()
            .unwrap();
        assert!(
            uncached_view.pack.is_none(),
            "a zero cache limit must not retain the exact source pack"
        );
        let service = GitFetchService::bind(&repository, "origin", GitFetchLimits::default())
            .await
            .unwrap();
        let source_pack_path = std::fs::read_dir(source.path().join(".git/objects/pack"))
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "pack")
            })
            .unwrap();
        let main = imported_view
            .resolve_ref(&CanonicalRefName::try_from("refs/heads/main").unwrap())
            .unwrap();
        let cached = service
            .build_pack(&GitFetchRequest {
                wants: vec![main.native_id().to_vec()],
                haves: Vec::new(),
                depth: None,
                done: true,
                multi_ack_detailed: false,
                side_band_64k: false,
            })
            .await
            .unwrap();
        assert_eq!(cached.pack, std::fs::read(source_pack_path).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = events.clone();
        let mut options = GitHttpOptions {
            max_connections: 1,
            max_pack_generations: 1,
            response_buffer_bytes: 1024,
            header_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(1),
            ..GitHttpOptions::default()
        };
        options.observer = Some(Arc::new(move |event: &GitHttpEvent| {
            observed.lock().unwrap().push(event.clone());
        }));
        let (shutdown_send, shutdown_receive) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_git_smart_http_with_shutdown(
            listener,
            "/origin.git".into(),
            service,
            options,
            async {
                let _ = shutdown_receive.await;
            },
        ));

        // One deliberately idle client occupies the only connection permit;
        // the next accepted socket is rejected without spawning more work.
        let mut held = TcpStream::connect(address).await.unwrap();
        held.write_all(b"G").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut byte = [0u8; 1];
        for _ in 0..32 {
            // Keep the permit occupied while testing overload, even when a
            // busy runner needs longer than one idle interval for the burst.
            held.write_all(b" ").await.unwrap();
            let mut excess = TcpStream::connect(address).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(1), excess.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        }
        assert_eq!(
            timeout(Duration::from_secs(2), held.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );

        let checkout_parent = tempfile::tempdir().unwrap();
        let checkout = checkout_parent.path().join("clone");
        let output = Command::new("git")
            .current_dir(checkout_parent.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args([
                "clone",
                "--quiet",
                "--depth",
                "1",
                &format!("http://{address}/origin.git"),
                checkout.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git clone: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(checkout.join("hello.txt")).unwrap(),
            b"second\n"
        );
        assert_eq!(
            std::fs::read(checkout.join("random.bin")).unwrap(),
            randomish
        );
        let count = git(&checkout, &["rev-list", "--count", "HEAD"]);
        assert_eq!(String::from_utf8_lossy(&count.stdout).trim(), "1");

        let full_checkout = checkout_parent.path().join("full-clone");
        let output = Command::new("git")
            .current_dir(checkout_parent.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args([
                "clone",
                "--quiet",
                &format!("http://{address}/origin.git"),
                full_checkout.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        shutdown_send.send(()).unwrap();
        server.await.unwrap().unwrap();
        {
            let events = events.lock().unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.outcome == GitHttpOutcome::ConnectionLimit)
                    .count(),
                32
            );
            assert!(events.iter().any(|event| {
                matches!(event.outcome, GitHttpOutcome::Served { status: 200, .. })
            }));
            assert!(
                events.iter().any(|event| {
                    event.outcome == GitHttpOutcome::TimedOut(GitHttpTimeout::Idle)
                })
            );
        }
        assert!(
            output.status.success(),
            "full git clone: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(full_checkout.join("hello.txt")).unwrap(),
            b"second\n"
        );
        let count = git(&full_checkout, &["rev-list", "--count", "HEAD"]);
        assert_eq!(String::from_utf8_lossy(&count.stdout).trim(), "2");

        std::fs::write(source.path().join("hello.txt"), b"third\n").unwrap();
        git(source.path(), &["commit", "-q", "-am", "third"]);
        let tip = git(source.path(), &["rev-parse", "HEAD"]);
        let tip = String::from_utf8_lossy(&tip.stdout).trim().to_owned();
        repository
            .import_native_git_view(
                source.path(),
                &NativeGitImportOptions {
                    view_name: "origin".into(),
                    refs: Vec::new(),
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        let service = GitFetchService::bind(&repository, "origin", GitFetchLimits::default())
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_git_smart_http(
            listener,
            "/origin.git".into(),
            service,
        ));
        let url = format!("http://{address}/origin.git");
        git(
            &full_checkout,
            &[
                "fetch",
                "--quiet",
                &url,
                "+refs/heads/main:refs/remotes/origin/main",
            ],
        );
        server.abort();
        let _ = server.await;

        let fetched = git(&full_checkout, &["rev-parse", "refs/remotes/origin/main"]);
        assert_eq!(String::from_utf8_lossy(&fetched.stdout).trim(), tip);
        git(
            &full_checkout,
            &["fsck", "--full", "--strict", "--no-progress"],
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unmodified_git_client_clones_a_sha256_view() {
        let source = tempfile::tempdir().unwrap();
        let git = |directory: &std::path::Path, args: &[&str]| {
            let output = Command::new("git")
                .current_dir(directory)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .env("GIT_AUTHOR_NAME", "test")
                .env("GIT_AUTHOR_EMAIL", "test@example.com")
                .env("GIT_COMMITTER_NAME", "test")
                .env("GIT_COMMITTER_EMAIL", "test@example.com")
                .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
                .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        git(
            source.path(),
            &["init", "-q", "--object-format=sha256", "-b", "main"],
        );
        std::fs::write(source.path().join("hello.txt"), b"sha256\n").unwrap();
        git(source.path(), &["add", "hello.txt"]);
        git(source.path(), &["commit", "-q", "-m", "first"]);

        let repository = Repository::memory().unwrap();
        repository
            .import_native_git_view(
                source.path(),
                &NativeGitImportOptions {
                    view_name: "sha256".into(),
                    refs: Vec::new(),
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        let service = GitFetchService::bind(&repository, "sha256", GitFetchLimits::default())
            .await
            .unwrap();
        assert_eq!(service.object_format(), crate::GitObjectFormat::Sha256);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_git_smart_http(
            listener,
            "/sha256.git".into(),
            service,
        ));

        let checkout_parent = tempfile::tempdir().unwrap();
        let checkout = checkout_parent.path().join("clone");
        let output = Command::new("git")
            .current_dir(checkout_parent.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args([
                "clone",
                "--quiet",
                &format!("http://{address}/sha256.git"),
                checkout.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        server.abort();
        assert!(
            output.status.success(),
            "SHA-256 git clone: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read(checkout.join("hello.txt")).unwrap(),
            b"sha256\n"
        );
        let format = git(&checkout, &["rev-parse", "--show-object-format"]);
        assert_eq!(String::from_utf8_lossy(&format.stdout).trim(), "sha256");
    }

    /// Stock Git gzips upload-pack bodies above 1 KiB and streams them with
    /// chunked framing once they exceed `http.postBuffer`, which Git never
    /// lowers below one 64 KiB packet. Many advertised branches produce one
    /// want line each, so fetching a subset exercises gzip and a full clone
    /// with the smallest post buffer exercises chunked framing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unmodified_git_client_sends_gzip_and_chunked_request_bodies() {
        const BRANCHES: usize = 1_500;
        let git = |directory: &std::path::Path, args: &[&str], input: Option<&[u8]>| {
            let mut child = Command::new("git")
                .current_dir(directory)
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .args(args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdin = child.stdin.take().unwrap();
            if let Some(input) = input {
                std::io::Write::write_all(&mut stdin, input).unwrap();
            }
            drop(stdin);
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        let source = tempfile::tempdir().unwrap();
        git(source.path(), &["init", "-q", "-b", "main"], None);
        let mut stream = String::new();
        for index in 0..BRANCHES {
            let content = format!("{index}\n");
            stream.push_str(&format!(
                "commit refs/heads/b{index:04}\nmark :{}\ncommitter test <test@example.com> 0 +0000\ndata 1\nx\n",
                index + 1
            ));
            if index > 0 {
                stream.push_str(&format!("from :{index}\n"));
            }
            stream.push_str(&format!(
                "M 644 inline file\ndata {}\n{content}\n",
                content.len()
            ));
        }
        stream.push_str(&format!("reset refs/heads/main\nfrom :{BRANCHES}\n\n"));
        git(
            source.path(),
            &["fast-import", "--quiet"],
            Some(stream.as_bytes()),
        );

        let repository = Repository::memory().unwrap();
        repository
            .import_native_git_view(
                source.path(),
                &NativeGitImportOptions {
                    view_name: "branches".into(),
                    refs: Vec::new(),
                    ..NativeGitImportOptions::default()
                },
            )
            .await
            .unwrap();
        let service = GitFetchService::bind(&repository, "branches", GitFetchLimits::default())
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_git_smart_http(
            listener,
            "/branches.git".into(),
            service,
        ));
        let url = format!("http://{address}/branches.git");

        // One hundred wants encode to about 5 KiB: gzip with Content-Length.
        let client = tempfile::tempdir().unwrap();
        git(client.path(), &["init", "-q", "--bare"], None);
        git(
            client.path(),
            &[
                "fetch",
                "--quiet",
                &url,
                "+refs/heads/b00*:refs/remotes/origin/b00*",
            ],
            None,
        );
        let fetched = git(
            client.path(),
            &[
                "for-each-ref",
                "--format=%(refname)",
                "refs/remotes/origin/",
            ],
            None,
        );
        assert_eq!(
            String::from_utf8_lossy(&fetched.stdout).lines().count(),
            100
        );

        // Every branch as a want encodes to about 75 KiB, which exceeds the
        // smallest post buffer Git accepts: chunked framing without gzip.
        let checkout_parent = tempfile::tempdir().unwrap();
        let checkout = checkout_parent.path().join("clone");
        git(
            checkout_parent.path(),
            &[
                "-c",
                "http.postBuffer=1024",
                "clone",
                "--quiet",
                "--mirror",
                &url,
                checkout.to_str().unwrap(),
            ],
            None,
        );

        // Unsupported codings are refused before any body byte is read, and
        // the lone flush probe Git sends ahead of a chunked request succeeds.
        for (headers, body, status) in [
            ("Content-Length: 0\r\nContent-Encoding: br\r\n", "", "415"),
            ("Transfer-Encoding: gzip\r\n", "", "501"),
            ("Content-Length: 4\r\n", "0000", "200"),
        ] {
            let mut connection = TcpStream::connect(address).await.unwrap();
            connection
                .write_all(
                    format!(
                        "POST /branches.git/git-upload-pack HTTP/1.1\r\nHost: test\r\n{headers}\r\n{body}"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            connection.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8_lossy(&response);
            assert!(
                response.starts_with(&format!("HTTP/1.1 {status} ")),
                "{headers:?}: {response}"
            );
        }
        server.abort();
        let _ = server.await;
        let cloned = git(
            &checkout,
            &["for-each-ref", "--format=%(refname)", "refs/heads/"],
            None,
        );
        assert_eq!(
            String::from_utf8_lossy(&cloned.stdout).lines().count(),
            BRANCHES + 1
        );
        git(
            &checkout,
            &["fsck", "--full", "--strict", "--no-progress"],
            None,
        );
    }

    /// Exercise the full smart-HTTP update path at the scale which exposed
    /// regressions in practice. The fixture stays outside the repository: a
    /// shared bare clone reuses its object database while its private ref is
    /// moved from `HEAD^` to `HEAD`.
    ///
    /// Run with a complete local Nixpkgs checkout, for example:
    /// `CASITA_NIXPKGS_REPOSITORY=/path/to/nixpkgs cargo test --all-features
    /// cold_fetches_nixpkgs_then_one_commit -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires a complete local nixpkgs clone and substantial disk, memory, and time"]
    async fn cold_fetches_nixpkgs_then_one_commit() {
        const VIEW: &str = "nixpkgs";
        const REF: &str = "refs/heads/casita-scale";

        let nixpkgs = std::env::var_os("CASITA_NIXPKGS_REPOSITORY")
            .map(std::path::PathBuf::from)
            .expect("set CASITA_NIXPKGS_REPOSITORY to a complete local nixpkgs checkout");

        let git = |repository: &std::path::Path, args: &[&str]| {
            let mut command = Command::new("git");
            command
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_SYSTEM", "/dev/null")
                .env("GIT_OPTIONAL_LOCKS", "0");
            if repository.join("HEAD").is_file() && repository.join("objects").is_dir() {
                command.arg(format!("--git-dir={}", repository.display()));
            } else {
                command.arg("-C").arg(repository);
            }
            let output = command.args(args).output().unwrap();
            assert!(
                output.status.success(),
                "git -C {} {args:?}: {}",
                repository.display(),
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        let output_text = |output: std::process::Output| {
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };

        let tip = output_text(git(&nixpkgs, &["rev-parse", "HEAD"]));
        let parent = output_text(git(&nixpkgs, &["rev-parse", "HEAD^"]));
        assert_eq!(
            output_text(git(
                &nixpkgs,
                &["rev-list", "--count", &format!("{parent}..{tip}")]
            )),
            "1",
            "the fixture must advance the selected ref by exactly one commit"
        );

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("nixpkgs-source.git");
        let output = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args(["clone", "--quiet", "--shared", "--bare"])
            .arg(&nixpkgs)
            .arg(&source)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git clone --shared --bare {}: {}",
            nixpkgs.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        git(&source, &["symbolic-ref", "HEAD", REF]);
        git(&source, &["update-ref", REF, &parent]);

        let repository = Repository::local(temp.path().join("casita")).await.unwrap();
        let import_options = NativeGitImportOptions {
            view_name: VIEW.into(),
            refs: vec![CanonicalRefName::try_from(REF).unwrap()],
            ..NativeGitImportOptions::default()
        };
        let cold_import = repository
            .import_native_git_view(&source, &import_options)
            .await
            .unwrap();
        assert!(
            cold_import.objects > 10_000,
            "the fixture is unexpectedly small (only {} reachable objects)",
            cold_import.objects
        );

        // Use a manual, generously bounded test: Nixpkgs's cold pack exceeds
        // the production default response cap, by design.
        let limits = GitFetchLimits {
            max_pack_bytes: 8 * 1024 * 1024 * 1024,
            ..GitFetchLimits::default()
        };
        let service = GitFetchService::bind(&repository, VIEW, limits.clone())
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_git_smart_http(
            listener,
            "/nixpkgs.git".into(),
            service,
        ));

        let client = temp.path().join("client.git");
        git(temp.path(), &["init", "--bare", client.to_str().unwrap()]);
        let url = format!("http://{address}/nixpkgs.git");
        let refspec = format!("+{REF}:{REF}");
        git(&client, &["fetch", "--quiet", &url, &refspec]);
        assert_eq!(output_text(git(&client, &["rev-parse", REF])), parent);
        server.abort();
        let _ = server.await;

        // Move exactly the private fixture ref, then re-import and fetch the
        // one-commit delta into the existing client object database.
        git(&source, &["update-ref", REF, &tip]);
        repository
            .import_native_git_view(&source, &import_options)
            .await
            .unwrap();
        let service = GitFetchService::bind(&repository, VIEW, limits)
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_git_smart_http(
            listener,
            "/nixpkgs.git".into(),
            service,
        ));
        let url = format!("http://{address}/nixpkgs.git");
        git(&client, &["fetch", "--quiet", &url, &refspec]);
        server.abort();
        let _ = server.await;

        assert_eq!(output_text(git(&client, &["rev-parse", REF])), tip);
        git(&client, &["fsck", "--full", "--strict", "--no-progress"]);
    }
}
