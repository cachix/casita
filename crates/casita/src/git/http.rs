//! Read-only smart HTTP for one bound native Git view.
//!
//! Hyper owns HTTP/1 framing. Casita owns Git semantics, bounded content
//! decoding, request concurrency, and pack generation.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderMap};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{Sleep, timeout};

use crate::{BlobStore, GitFetchError, GitFetchService, MetadataStore};

const MAX_HTTP_HEADERS: usize = 64 * 1024;
const STREAM_BUFFER_BYTES: usize = 1024 * 1024;
type HttpBody = UnsyncBoxBody<Bytes, std::io::Error>;

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
    /// Maximum time without progress while writing a response.
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
                        handle_connection(stream, route, service, options.clone(), pack_generations, request_bytes),
                    ).await;
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
                        observer.observe(&GitHttpEvent { peer, outcome, elapsed: started.elapsed() });
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
            if value == 0 || value > Semaphore::MAX_PERMITS {
                return Err(GitHttpError::InvalidOptions(format!(
                    "{name} must fit Tokio's semaphore limit and be nonzero"
                )));
            }
        }
        if self.response_buffer_bytes == 0 {
            return Err(GitHttpError::InvalidOptions(
                "response_buffer_bytes must be greater than zero".into(),
            ));
        }
        if self.max_inflight_request_bytes < max_request_bytes
            || self.max_inflight_request_bytes > u32::MAX as usize
            || self.max_inflight_request_bytes > Semaphore::MAX_PERMITS
        {
            return Err(GitHttpError::InvalidOptions(
                "max_inflight_request_bytes must cover max_request_bytes and fit the semaphore"
                    .into(),
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

// Apply idle deadlines to the transport as well as the decoded request body.
// Hyper's header deadline covers total header time; this wrapper catches a
// stalled socket write while Hyper owns the response framing.
struct TimedIo {
    inner: TcpStream,
    read_timeout: Duration,
    write_timeout: Duration,
    read_sleep: Option<Pin<Box<Sleep>>>,
    write_sleep: Option<Pin<Box<Sleep>>>,
    timed_out: Arc<AtomicU8>,
}

impl TimedIo {
    fn new(inner: TcpStream, options: &GitHttpOptions, timed_out: Arc<AtomicU8>) -> Self {
        Self {
            inner,
            read_timeout: options.idle_timeout,
            write_timeout: options.response_write_timeout,
            read_sleep: None,
            write_sleep: None,
            timed_out,
        }
    }
    fn pending(
        sleep: &mut Option<Pin<Box<Sleep>>>,
        duration: Duration,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<usize>> {
        let timer = sleep.get_or_insert_with(|| Box::pin(tokio::time::sleep(duration)));
        if timer.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "socket idle timeout",
            )))
        } else {
            Poll::Pending
        }
    }
}

impl AsyncRead for TimedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                self.read_sleep = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let deadline = self.read_timeout;
                match Self::pending(&mut self.read_sleep, deadline, cx) {
                    Poll::Ready(Err(error)) => {
                        self.timed_out.store(1, Ordering::Relaxed);
                        Poll::Ready(Err(error))
                    }
                    _ => Poll::Pending,
                }
            }
        }
    }
}

impl AsyncWrite for TimedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(result) => {
                self.write_sleep = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let deadline = self.write_timeout;
                match Self::pending(&mut self.write_sleep, deadline, cx) {
                    Poll::Ready(Err(error)) => {
                        self.timed_out.store(2, Ordering::Relaxed);
                        Poll::Ready(Err(error))
                    }
                    _ => Poll::Pending,
                }
            }
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tracing::instrument(name = "git.http.request", skip_all)]
async fn handle_connection<PS, SS>(
    stream: TcpStream,
    route: String,
    service: GitFetchService<PS, SS>,
    options: GitHttpOptions,
    pack_generations: Arc<Semaphore>,
    request_bytes: Arc<Semaphore>,
) -> Result<ServedRequest, GitHttpError>
where
    PS: BlobStore + Clone + Send + Sync + 'static,
    SS: MetadataStore + Clone + Send + Sync + 'static,
{
    let result_slot = Arc::new(Mutex::new(None));
    let timed_out = Arc::new(AtomicU8::new(0));
    let request_seen = Arc::new(AtomicU8::new(0));
    let handler_options = options.clone();
    let handler = service_fn({
        let result_slot = result_slot.clone();
        let request_seen = request_seen.clone();
        move |request: Request<Incoming>| {
            let route = route.clone();
            let service = service.clone();
            let options = handler_options.clone();
            let pack_generations = pack_generations.clone();
            let request_bytes = request_bytes.clone();
            let result_slot = result_slot.clone();
            let request_seen = request_seen.clone();
            async move {
                request_seen.store(1, Ordering::Relaxed);
                let method = request.method().to_string();
                let target = request.uri().to_string();
                let result = handle_request(
                    request,
                    &route,
                    &service,
                    &options,
                    pack_generations,
                    request_bytes,
                )
                .await;
                let response = match result {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        *result_slot.lock().unwrap() = Some(Ok(ServedRequest {
                            method,
                            target,
                            status,
                        }));
                        response
                    }
                    Err(error) => {
                        let status = match error {
                            GitHttpError::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
                            GitHttpError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
                            _ => StatusCode::INTERNAL_SERVER_ERROR,
                        };
                        *result_slot.lock().unwrap() = Some(Err(error));
                        text_response(status, "invalid Git HTTP request\n")
                    }
                };
                Ok::<_, Infallible>(response)
            }
        }
    });
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .timer(TokioTimer::new())
        .header_read_timeout(options.header_timeout)
        .max_buf_size(MAX_HTTP_HEADERS);
    let io = TokioIo::new(TimedIo::new(stream, &options, timed_out.clone()));
    let connection = builder.serve_connection(io, handler);
    let wire_result = connection.await;
    if timed_out.load(Ordering::Relaxed) == 2 {
        return Err(GitHttpError::Timeout(GitHttpTimeout::ResponseWrite));
    }
    if timed_out.load(Ordering::Relaxed) == 1 {
        return Err(GitHttpError::Timeout(GitHttpTimeout::Idle));
    }
    if let Err(error) = wire_result {
        if request_seen.load(Ordering::Relaxed) == 0 && error.is_timeout() {
            return Err(GitHttpError::Timeout(GitHttpTimeout::Headers));
        }
        return Err(GitHttpError::InvalidRequest(error.to_string()));
    }
    let mut slot = result_slot.lock().unwrap();
    slot.take().unwrap_or_else(|| {
        Err(GitHttpError::InvalidRequest(
            "no HTTP request was served".into(),
        ))
    })
}

async fn handle_request<PS, SS>(
    request: Request<Incoming>,
    route: &str,
    service: &GitFetchService<PS, SS>,
    options: &GitHttpOptions,
    pack_generations: Arc<Semaphore>,
    request_bytes: Arc<Semaphore>,
) -> Result<Response<HttpBody>, GitHttpError>
where
    PS: BlobStore + Clone + Send + Sync + 'static,
    SS: MetadataStore + Clone + Send + Sync + 'static,
{
    let target = request.uri().to_string();
    if request.method() == Method::GET {
        if target != format!("{route}/info/refs?service=git-upload-pack") {
            return Ok(text_response(StatusCode::NOT_FOUND, "not found\n"));
        }
        return Ok(match service.info_refs() {
            Ok(body) => response(
                StatusCode::OK,
                "application/x-git-upload-pack-advertisement",
                body,
            ),
            Err(error) => fetch_error_response(error),
        });
    }
    if request.method() != Method::POST || target != format!("{route}/git-upload-pack") {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found\n"));
    }
    let coding = content_coding(request.headers())?;
    if coding == BodyCoding::Unsupported {
        return Ok(text_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported Content-Encoding\n",
        ));
    }
    let max_request_bytes = service.limits().max_request_bytes;
    if !request.headers().contains_key(header::CONTENT_LENGTH)
        && !request.headers().contains_key(header::TRANSFER_ENCODING)
    {
        return Err(GitHttpError::InvalidRequest(
            "POST requires Content-Length or chunked Transfer-Encoding".into(),
        ));
    }
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    if declared.is_some_and(|length| length > max_request_bytes) {
        return Ok(text_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "upload-pack request too large\n",
        ));
    }
    let budget = if coding == BodyCoding::Identity {
        declared.unwrap_or(max_request_bytes)
    } else {
        max_request_bytes
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
            request.into_body(),
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
        Ok(value) => value,
        Err(BodyError::TooLarge) => {
            return Ok(text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "upload-pack request too large\n",
            ));
        }
        Err(BodyError::Http(error)) => return Err(error),
    };
    if body == b"0000" {
        return Ok(response(
            StatusCode::OK,
            "application/x-git-upload-pack-result",
            Bytes::new(),
        ));
    }
    match service.prepare_upload_pack(&body).await {
        Ok(prepared) => {
            let deadline = prepared
                .done()
                .then(|| tokio::time::Instant::now() + options.pack_generation_timeout);
            let _permit = if let Some(deadline) = deadline {
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
            // Keep the permit for the lifetime of the streamed body.
            Ok(streaming_response(
                service.clone(),
                prepared,
                options,
                deadline,
                _permit,
            ))
        }
        Err(error) => Ok(fetch_error_response(error)),
    }
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<HttpBody> {
    let body = body.into();
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "close")
        .header(header::CONTENT_LENGTH, body.len().to_string())
        .body(
            Full::new(body)
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .unwrap()
}

fn text_response(status: StatusCode, body: &'static str) -> Response<HttpBody> {
    response(status, "text/plain; charset=utf-8", body)
}

fn fetch_error_response(error: GitFetchError) -> Response<HttpBody> {
    let status = match error {
        GitFetchError::UnauthorizedOid(_) => StatusCode::FORBIDDEN,
        GitFetchError::Protocol(_) | GitFetchError::Limit(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    response(status, "text/plain; charset=utf-8", format!("{error}\n"))
}

#[tracing::instrument(name = "git.http.stream_pack", skip_all)]
fn streaming_response<PS, SS>(
    service: GitFetchService<PS, SS>,
    prepared: crate::git::fetch::PreparedUploadPack,
    options: &GitHttpOptions,
    deadline: Option<tokio::time::Instant>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Response<HttpBody>
where
    PS: BlobStore + Clone + Send + Sync + 'static,
    SS: MetadataStore + Clone + Send + Sync + 'static,
{
    let content_length = prepared.response_bytes();
    let aggregate = prepared.uses_cached_pack();
    let (mut producer, mut consumer) = tokio::io::duplex(options.response_buffer_bytes);
    let generation = tokio::spawn(async move {
        let _permit = permit;
        let result = if let Some(deadline) = deadline {
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
    });
    let buffer_size = options.response_buffer_bytes;
    let stream = async_stream::try_stream! {
        let mut buffer = vec![0u8; buffer_size];
        loop {
            let mut size = consumer.read(&mut buffer).await?;
            if size == 0 { break; }
            if aggregate {
                while size < buffer.len() {
                    let next = consumer.read(&mut buffer[size..]).await?;
                    if next == 0 { break; }
                    size += next;
                }
            }
            yield Frame::data(Bytes::copy_from_slice(&buffer[..size]));
        }
        generation.await.map_err(std::io::Error::other)?
            .map_err(std::io::Error::other)?;
    };
    let body = StreamBody::new(stream).boxed_unsync();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-git-upload-pack-result")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "close");
    if let Some(length) = content_length {
        builder = builder.header(header::CONTENT_LENGTH, length.to_string());
    }
    builder.body(body).unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyCoding {
    Identity,
    Gzip,
    Unsupported,
}

fn content_coding(headers: &HeaderMap) -> Result<BodyCoding, GitHttpError> {
    let mut values = headers.get_all(header::CONTENT_ENCODING).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(GitHttpError::InvalidRequest(
            "duplicate Content-Encoding".into(),
        ));
    }
    Ok(match value.and_then(|value| value.to_str().ok()) {
        None => BodyCoding::Identity,
        Some(value) if value.eq_ignore_ascii_case("identity") => BodyCoding::Identity,
        Some(value)
            if value.eq_ignore_ascii_case("gzip") || value.eq_ignore_ascii_case("x-gzip") =>
        {
            BodyCoding::Gzip
        }
        _ => BodyCoding::Unsupported,
    })
}

#[derive(Debug)]
enum BodyError {
    TooLarge,
    Http(GitHttpError),
}
impl From<GitHttpError> for BodyError {
    fn from(error: GitHttpError) -> Self {
        Self::Http(error)
    }
}
fn invalid_body(message: &str) -> BodyError {
    BodyError::Http(GitHttpError::InvalidRequest(message.into()))
}

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

enum BodyDecoder {
    Identity(BoundedBody),
    Gzip(Box<flate2::write::GzDecoder<BoundedBody>>),
}
impl BodyDecoder {
    fn new(coding: BodyCoding, limit: usize) -> Result<Self, BodyError> {
        let body = BoundedBody {
            bytes: Vec::new(),
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

async fn read_request_body(
    mut incoming: Incoming,
    coding: BodyCoding,
    limit: usize,
    idle_timeout: Duration,
) -> Result<Vec<u8>, BodyError> {
    let mut decoder = BodyDecoder::new(coding, limit)?;
    let mut received = 0usize;
    while let Some(frame) = timeout(idle_timeout, incoming.frame())
        .await
        .map_err(|_| BodyError::Http(GitHttpError::Timeout(GitHttpTimeout::Idle)))?
    {
        let frame = frame.map_err(|error| invalid_body(&error.to_string()))?;
        if let Ok(data) = frame.into_data() {
            received = received
                .checked_add(data.len())
                .ok_or(BodyError::TooLarge)?;
            if received > limit {
                return Err(BodyError::TooLarge);
            }
            decoder.write(&data)?;
        }
    }
    decoder.finish()
}

#[cfg(test)]
mod body_tests {
    use std::io::Write;

    use super::*;

    fn gzip(input: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn gzip_output_is_bounded_while_decoding() {
        let compressed = gzip(&vec![0; 64 * 1024]);
        let mut decoder = BodyDecoder::new(BodyCoding::Gzip, 1024).unwrap();
        assert!(matches!(
            decoder.write(&compressed),
            Err(BodyError::TooLarge)
        ));

        let mut decoder = BodyDecoder::new(BodyCoding::Gzip, 64 * 1024).unwrap();
        decoder.write(&compressed).unwrap();
        assert_eq!(decoder.finish().unwrap().len(), 64 * 1024);
    }

    #[test]
    fn malformed_gzip_is_rejected() {
        let valid = gzip(b"payload");
        let mut corrupt_crc = valid.clone();
        let crc = corrupt_crc.len() - 8;
        corrupt_crc[crc] ^= 1;
        let mut trailing = valid.clone();
        trailing.extend_from_slice(b"extra");
        for bytes in [
            b"not gzip".as_slice(),
            &valid[..valid.len() - 4],
            &corrupt_crc,
            &trailing,
        ] {
            let mut decoder = BodyDecoder::new(BodyCoding::Gzip, 1024).unwrap();
            let result = decoder.write(bytes).and_then(|()| decoder.finish());
            assert!(matches!(result, Err(BodyError::Http(_))));
        }
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
            // Hyper rejects unsupported transfer framing before dispatch.
            ("Transfer-Encoding: gzip\r\n", "", "400"),
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
