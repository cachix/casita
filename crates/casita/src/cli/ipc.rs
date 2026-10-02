//! Local JSON-RPC service for thin synchronous Casita clients.
//!
//! The protocol uses one bounded JSON-RPC 2.0 object per newline-delimited
//! frame. `json-rpc-rs` owns JSON-RPC parsing, request validation, routing,
//! and response encoding; Casita owns the local endpoint and artifact methods.
//!
//! The service is part of the `casita` command-line front end, not the library.
//! It uses only the public `casita` and `casita::experimental` APIs. Clients
//! such as the Cargo integration derive the same endpoint path, so the naming
//! in [`endpoint`] and the wire protocol are a compatibility contract.

mod import;
mod restore;
#[cfg(test)]
mod tests;

use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::time::Duration;

/// Resource bounds for the local IPC listener.
#[derive(Debug, Clone)]
pub(crate) struct IpcOptions {
    /// Maximum simultaneous connections, including idle clients. An excess
    /// connection receives one JSON-RPC error frame and is closed. Defaults
    /// to 64.
    pub(crate) max_connections: usize,
    /// Total time to receive each complete frame, including idle time.
    /// Defaults to 60 seconds; reconnect after an idle connection is closed.
    pub(crate) frame_timeout: Duration,
    /// Total time to write and flush each response. Defaults to 30 seconds.
    pub(crate) response_timeout: Duration,
}

impl Default for IpcOptions {
    fn default() -> Self {
        Self {
            max_connections: 64,
            frame_timeout: Duration::from_secs(60),
            response_timeout: Duration::from_secs(30),
        }
    }
}

/// Protocol version implemented by this service.
const VERSION: u64 = 1;
/// Absolute maximum JSON bytes in a single frame, excluding its LF.
const MAX_FRAME_BYTES: usize = 1_048_576;
/// Minimum negotiated frame limit.
const MIN_FRAME_BYTES: usize = 4_096;

#[cfg(unix)]
/// Returns the Unix-domain socket used by a repository.
fn endpoint(repository_dir: impl AsRef<Path>) -> PathBuf {
    let hash = blake3::hash(repository_dir.as_ref().to_string_lossy().as_bytes()).to_hex();
    PathBuf::from("/tmp")
        .join("casita")
        .join(format!("cargo-{}.sock", &hash.as_str()[..16]))
}

#[cfg(windows)]
/// Returns the Windows named pipe used by a repository.
fn endpoint(repository_dir: impl AsRef<Path>) -> String {
    let hash = blake3::hash(repository_dir.as_ref().to_string_lossy().as_bytes()).to_hex();
    format!(r"\\.\pipe\casita-cargo-v{}", &hash.as_str()[..16])
}

/// Serve the repository-local IPC endpoint until the process is stopped, with
/// explicit connection and transport deadlines. Artifact operations are not
/// timed out: the frame deadline restarts after the response is sent.
#[cfg(any(unix, windows))]
pub(crate) async fn serve_with_options(
    repository_dir: impl AsRef<Path>,
    options: IpcOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if options.max_connections == 0
        || options.max_connections > tokio::sync::Semaphore::MAX_PERMITS
        || options.frame_timeout.is_zero()
        || options.response_timeout.is_zero()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "IPC connection count and deadlines must be positive and supported",
        )
        .into());
    }
    let repository_dir = repository_dir.as_ref();
    let repository =
        std::sync::Arc::new(casita::experimental::Repository::local(repository_dir).await?);
    platform::serve(repository, repository_dir, options).await
}

#[cfg(not(any(unix, windows)))]
pub(crate) async fn serve_with_options(
    _repository_dir: impl AsRef<Path>,
    _options: IpcOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Casita IPC requires Unix-domain sockets or Windows named pipes",
    )
    .into())
}

mod server {
    use std::path::PathBuf;
    use std::sync::Arc;

    use jsonrpc_core::{Error as RpcError, ErrorCode, IoHandler, Params, Value};
    use serde::{Deserialize, Serialize};
    use tokio::io::{
        AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _,
        BufReader,
    };
    use tokio::sync::Mutex;

    use super::{MAX_FRAME_BYTES, MIN_FRAME_BYTES, VERSION};
    use casita::RootName;
    use casita::experimental::{BlobGc, MetadataStore, Repository};

    #[derive(Default)]
    struct Session {
        initialized: bool,
        max_frame_bytes: usize,
        shutdown: bool,
    }

    #[derive(Deserialize)]
    struct InitializeParams {
        versions: Vec<u64>,
        #[serde(default = "default_max_frame_bytes")]
        max_frame_bytes: usize,
    }

    #[derive(Serialize)]
    struct InitializeResult {
        version: u64,
        max_frame_bytes: usize,
        capabilities: [&'static str; 3],
        importers: &'static [&'static str],
    }

    #[derive(Deserialize)]
    struct ArtifactParams {
        root: String,
        path: PathBuf,
    }

    #[derive(Serialize)]
    struct CheckoutResult {
        present: bool,
    }

    fn default_max_frame_bytes() -> usize {
        MAX_FRAME_BYTES
    }

    #[cfg(test)]
    pub(super) async fn serve_connection<PS, SS, S>(
        repository: Arc<Repository<PS, SS>>,
        stream: S,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        serve_connection_with_options(repository, stream, super::IpcOptions::default()).await
    }

    #[tracing::instrument(name = "ipc.connection", skip_all)]
    pub(super) async fn serve_connection_with_options<PS, SS, S>(
        repository: Arc<Repository<PS, SS>>,
        stream: S,
        options: super::IpcOptions,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let session = Arc::new(Mutex::new(Session {
            max_frame_bytes: MAX_FRAME_BYTES,
            ..Session::default()
        }));
        let dispatcher = dispatcher(repository, Arc::clone(&session));
        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            line.clear();
            let limit = session.lock().await.max_frame_bytes;
            // The extra byte is the LF for a valid maximum-size frame. Take
            // bounds allocation even if the peer never sends a newline or EOF.
            let read = tokio::time::timeout(
                options.frame_timeout,
                (&mut reader).take((limit + 1) as u64).read_line(&mut line),
            )
            .await??;
            if read == 0 {
                return Ok(());
            }
            if !line.ends_with('\n') || line.len() - 1 > limit || line.contains('\r') {
                return Err("invalid or oversized IPC frame".into());
            }
            let request = line.trim_end_matches('\n');
            if request.is_empty() {
                return Err("empty IPC frame".into());
            }
            if let Some(response) = dispatcher.handle_request(request).await {
                tokio::time::timeout(options.response_timeout, async {
                    writer.write_all(response.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await
                })
                .await??;
            }
            if session.lock().await.shutdown {
                return Ok(());
            }
        }
    }

    pub(super) struct Connections {
        permits: Arc<tokio::sync::Semaphore>,
        tasks: tokio::task::JoinSet<()>,
        options: super::IpcOptions,
    }

    impl Connections {
        pub(super) fn new(options: super::IpcOptions) -> Self {
            Self {
                permits: Arc::new(tokio::sync::Semaphore::new(options.max_connections)),
                tasks: tokio::task::JoinSet::new(),
                options,
            }
        }

        /// Serve `stream` if a connection slot is free. A rejected client is
        /// told why in one JSON-RPC error frame before its connection closes,
        /// so it can distinguish an overloaded daemon from a crashed one.
        pub(super) fn admit<PS, SS, S>(
            &mut self,
            repository: Arc<Repository<PS, SS>>,
            mut stream: S,
        ) -> bool
        where
            PS: BlobGc + 'static,
            SS: MetadataStore + 'static,
            S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        {
            while self.tasks.try_join_next().is_some() {}
            let options = self.options.clone();
            let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                self.tasks.spawn(async move {
                    let _ = tokio::time::timeout(options.response_timeout, async {
                        stream.write_all(REJECTED_FRAME.as_bytes()).await?;
                        stream.flush().await
                    })
                    .await;
                });
                return false;
            };
            self.tasks.spawn(async move {
                let _permit = permit;
                let _ = serve_connection_with_options(repository, stream, options).await;
            });
            true
        }

        /// Let admitted connections run to completion after the listener
        /// stops, instead of aborting them with the task set.
        pub(super) fn detach(&mut self) {
            self.tasks.detach_all();
        }
    }

    /// Sent to a client that arrives while every connection slot is taken.
    pub(super) const REJECTED_FRAME: &str = concat!(
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32000,"#,
        r#""message":"too many IPC connections; retry later"}}"#,
        "\n"
    );

    /// Pause before retrying `accept` after a transient error, so a burst that
    /// exhausted file descriptors can drain instead of spinning the listener.
    pub(super) const ACCEPT_RETRY_DELAY: std::time::Duration =
        std::time::Duration::from_millis(100);

    /// Whether the listener can keep accepting after this error. Resource
    /// exhaustion and aborted handshakes are per-connection conditions;
    /// anything else means the endpoint itself is gone.
    pub(super) fn accept_error_is_transient(error: &std::io::Error) -> bool {
        use std::io::ErrorKind;

        matches!(
            error.kind(),
            ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionReset
                | ErrorKind::Interrupted
                | ErrorKind::OutOfMemory
                | ErrorKind::WouldBlock
        ) || matches!(error.raw_os_error(), Some(code) if TRANSIENT_ACCEPT_ERRNOS.contains(&code))
    }

    // EMFILE, ENFILE and ENOBUFS have no stable `ErrorKind` on every platform.
    #[cfg(unix)]
    const TRANSIENT_ACCEPT_ERRNOS: &[i32] = &[libc::EMFILE, libc::ENFILE, libc::ENOBUFS];
    #[cfg(not(unix))]
    const TRANSIENT_ACCEPT_ERRNOS: &[i32] = &[];

    fn dispatcher<PS, SS>(
        repository: Arc<Repository<PS, SS>>,
        session: Arc<Mutex<Session>>,
    ) -> IoHandler
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        let initialize_session = Arc::clone(&session);
        let checkout_repository = repository.clone();
        let checkout_session = Arc::clone(&session);
        let restore_repository = repository.clone();
        let restore_session = Arc::clone(&session);
        let import_session = Arc::clone(&session);
        let shutdown_session = Arc::clone(&session);
        let mut dispatcher = IoHandler::new();

        dispatcher.add_method("rpc.initialize", move |params: Params| {
            let session = Arc::clone(&initialize_session);
            async move {
                let params: InitializeParams = params.parse()?;
                let mut session = session.lock().await;
                if session.initialized || !params.versions.contains(&VERSION) {
                    return Err(server_error(-32600, "unsupported or duplicate initialize"));
                }
                if !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&params.max_frame_bytes) {
                    return Err(RpcError::invalid_params("invalid max_frame_bytes"));
                }
                session.initialized = true;
                session.max_frame_bytes = params.max_frame_bytes;
                tracing::debug!(
                    protocol_version = VERSION,
                    max_frame_bytes = session.max_frame_bytes,
                    "IPC session initialized"
                );
                Ok(value(InitializeResult {
                    version: VERSION,
                    max_frame_bytes: session.max_frame_bytes,
                    capabilities: ["artifact.checkout", "artifact.import", "artifact.restore"],
                    importers: super::import::IMPORTERS,
                }))
            }
        });
        dispatcher.add_method("artifact.checkout", move |params: Params| {
            let repository = checkout_repository.clone();
            let session = Arc::clone(&checkout_session);
            async move {
                let params: ArtifactParams = params.parse()?;
                require_initialized(&session).await?;
                checkout(repository, params).await.map(value)
            }
        });
        dispatcher.add_method("artifact.restore", move |params: Params| {
            let repository = restore_repository.clone();
            let session = Arc::clone(&restore_session);
            async move {
                require_initialized(&session).await?;
                super::restore::run(repository, params).await
            }
        });
        dispatcher.add_method("artifact.import", move |params: Params| {
            let repository = repository.clone();
            let session = Arc::clone(&import_session);
            async move {
                let params = super::import::parse(params)?;
                require_initialized(&session).await?;
                super::import::run(repository, params).await
            }
        });
        dispatcher.add_method("rpc.shutdown", move |params: Params| {
            let session = Arc::clone(&shutdown_session);
            async move {
                params.parse::<()>()?;
                require_initialized(&session).await?;
                session.lock().await.shutdown = true;
                Ok(Value::Null)
            }
        });
        dispatcher.add_notification("rpc.cancel", |_params: Params| {});
        dispatcher
    }

    async fn require_initialized(session: &Mutex<Session>) -> Result<(), RpcError> {
        if session.lock().await.initialized {
            Ok(())
        } else {
            Err(server_error(-32600, "rpc.initialize must be called first"))
        }
    }

    #[tracing::instrument(name = "ipc.artifact.checkout", skip_all)]
    async fn checkout<PS, SS>(
        repository: Arc<Repository<PS, SS>>,
        params: ArtifactParams,
    ) -> Result<CheckoutResult, RpcError>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        let root = RootName::try_from(params.root.as_str())
            .map_err(|_| RpcError::invalid_params("invalid root"))?;
        let hold = repository
            .retention_hold()
            .await
            .map_err(|_| server_error(-32008, "could not read repository state"))?;
        let Some(object) = hold
            .snapshot()
            .root(&root)
            .await
            .map_err(|_| server_error(-32008, "could not read repository root"))?
        else {
            std::fs::create_dir(&params.path)
                .map_err(|_| server_error(-32008, "could not create checkout directory"))?;
            return Ok(CheckoutResult { present: false });
        };
        repository
            .checkout(&object, &params.path)
            .await
            .map_err(|_| server_error(-32008, "could not check out artifact root"))?;
        if let Err(error) = repository.touch_root(&root, &object).await {
            tracing::debug!(%error, "could not update root access time");
        }
        Ok(CheckoutResult { present: true })
    }

    fn value(value: impl Serialize) -> Value {
        serde_json::to_value(value).expect("IPC results are serializable")
    }

    fn server_error(code: i64, message: impl Into<String>) -> RpcError {
        RpcError {
            code: ErrorCode::ServerError(code),
            message: message.into(),
            data: None,
        }
    }
}

#[cfg(unix)]
mod platform {
    use std::path::Path;
    use std::sync::Arc;

    use tokio::net::UnixListener;

    use super::{endpoint, server};
    use casita::experimental::{BlobGc, MetadataStore, Repository};

    pub(super) async fn serve<PS, SS>(
        repository: Arc<Repository<PS, SS>>,
        repository_dir: &Path,
        options: super::IpcOptions,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        let socket = endpoint(repository_dir);
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
            restrict_directory_permissions(parent)?;
        }
        remove_stale_socket(&socket)?;
        let listener = UnixListener::bind(&socket)?;
        restrict_socket_permissions(&socket)?;

        let mut connections = server::Connections::new(options);
        let result: Result<(), std::io::Error> = async {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        connections.admit(repository.clone(), stream);
                    }
                    Err(error) if server::accept_error_is_transient(&error) => {
                        // One aborted handshake or a momentary descriptor
                        // shortage must not stop the listener.
                        tracing::warn!(%error, "IPC accept failed; continuing");
                        tokio::time::sleep(server::ACCEPT_RETRY_DELAY).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        .await;
        // Admitted clients finish their current requests; only new
        // connections are refused once the listener is gone.
        connections.detach();
        result.map_err(Into::into)
    }

    fn remove_stale_socket(socket: &Path) -> Result<(), std::io::Error> {
        if !socket.exists() {
            return Ok(());
        }
        match std::os::unix::net::UnixStream::connect(socket) {
            Ok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!(
                    "Casita IPC endpoint is already running at {}",
                    socket.display()
                ),
            )),
            Err(error) if matches!(error.kind(), std::io::ErrorKind::ConnectionRefused) => {
                std::fs::remove_file(socket)
            }
            Err(error) => Err(error),
        }
    }

    fn restrict_socket_permissions(socket: &Path) -> Result<(), std::io::Error> {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
    }

    fn restrict_directory_permissions(directory: &Path) -> Result<(), std::io::Error> {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))
    }
}

#[cfg(windows)]
mod platform {
    use std::path::Path;
    use std::sync::Arc;

    use tokio::net::windows::named_pipe::ServerOptions;

    use super::{endpoint, server};
    use casita::experimental::{BlobGc, MetadataStore, Repository};

    pub(super) async fn serve<PS, SS>(
        repository: Arc<Repository<PS, SS>>,
        repository_dir: &Path,
        options: super::IpcOptions,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        PS: BlobGc + 'static,
        SS: MetadataStore + 'static,
    {
        let endpoint = endpoint(repository_dir);
        let mut listener = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&endpoint)?;

        let mut connections = server::Connections::new(options);
        let result: Result<(), std::io::Error> = async {
            loop {
                if let Err(error) = listener.connect().await {
                    if server::accept_error_is_transient(&error) {
                        tracing::warn!(%error, "IPC connect failed; continuing");
                        tokio::time::sleep(server::ACCEPT_RETRY_DELAY).await;
                        // A failed connect leaves this instance unusable;
                        // the next client needs a fresh one.
                        listener = ServerOptions::new().create(&endpoint)?;
                        continue;
                    }
                    return Err(error);
                }
                let next = ServerOptions::new().create(&endpoint)?;
                let connection = std::mem::replace(&mut listener, next);
                connections.admit(repository.clone(), connection);
            }
        }
        .await;
        // Admitted clients finish their current requests; only new
        // connections are refused once the listener is gone.
        connections.detach();
        result.map_err(Into::into)
    }
}
