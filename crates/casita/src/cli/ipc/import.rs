//! Typed wire requests for the repository's local importers.

use std::{path::PathBuf, sync::Arc};

use jsonrpc_core::{Error, Params, Value};
use serde::Deserialize;
use serde_json::json;

use casita::RootName;
use casita::experimental::{BlobGc, MetadataStore, Repository};

pub(super) const IMPORTERS: &[&str] = &[
    "filesystem",
    "blob",
    "copy",
    "nar",
    "filesystem_nar",
    "tar",
    "casitar",
    #[cfg(feature = "git")]
    "git",
];

#[derive(Deserialize)]
#[serde(tag = "importer", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ImportParams {
    Blob {
        path: PathBuf,
        root: String,
        #[serde(default)]
        options: EmptyOptions,
    },
    Copy {
        path: PathBuf,
        source_root: String,
        root: String,
        #[serde(default)]
        options: EmptyOptions,
    },
    Nar {
        path: PathBuf,
        root: String,
        #[serde(default)]
        options: EmptyOptions,
    },
    FilesystemNar {
        path: PathBuf,
        root: String,
        #[serde(default)]
        options: NarOptions,
    },
    Filesystem {
        path: PathBuf,
        root: String,
        #[serde(default)]
        options: FilesystemOptions,
    },
    Tar {
        path: PathBuf,
        root: String,
        #[serde(default)]
        options: TarOptions,
    },
    Casitar {
        path: PathBuf,
        destinations: Vec<String>,
        #[serde(default)]
        options: CasitarOptions,
    },
    #[cfg(feature = "git")]
    Git {
        path: PathBuf,
        view: String,
        #[serde(default)]
        options: GitOptions,
    },
}

pub(super) fn error(
    category: &str,
    importer: Option<&str>,
    option: Option<&str>,
    message: impl Into<String>,
) -> Error {
    Error {
        code: if category == "execution_failure" {
            jsonrpc_core::ErrorCode::ServerError(-32008)
        } else {
            jsonrpc_core::ErrorCode::InvalidParams
        },
        message: message.into(),
        data: Some(json!({"category": category, "importer": importer, "option": option})),
    }
}

pub(super) fn parse(params: Params) -> Result<ImportParams, Error> {
    let Params::Map(mut fields) = params else {
        return Err(error(
            "invalid_parameters",
            None,
            None,
            "import requires named parameters",
        ));
    };
    fields
        .entry("importer")
        .or_insert_with(|| json!("filesystem"));
    let importer = fields["importer"]
        .as_str()
        .ok_or_else(|| {
            error(
                "invalid_parameters",
                None,
                None,
                "importer must be a string",
            )
        })?
        .to_owned();
    if !IMPORTERS.contains(&importer.as_str()) {
        return Err(error(
            "unsupported_importer",
            Some(&importer),
            None,
            "unsupported importer",
        ));
    }
    // Accept an explicit parameter envelope as well as the original flat shape.
    if let Some(parameters) = fields.remove("parameters") {
        let Value::Object(parameters) = parameters else {
            return Err(error(
                "invalid_parameters",
                Some(&importer),
                None,
                "parameters must be an object",
            ));
        };
        for (key, value) in parameters {
            if key == "importer" || key == "options" || fields.contains_key(&key) {
                return Err(error(
                    "invalid_parameters",
                    Some(&importer),
                    None,
                    "duplicate or reserved parameter",
                ));
            }
            fields.insert(key, value);
        }
    }
    let options = fields.get("options").cloned().unwrap_or(Value::Null);
    if importer == "tar"
        && let Some(compression) = options.get("compression").and_then(Value::as_str)
        && !["none", "gzip"].contains(&compression)
    {
        return Err(error(
            "unsupported_option",
            Some(&importer),
            Some("compression"),
            "unsupported compression",
        ));
    }
    if importer == "tar"
        && options.get("max_compressed_bytes").is_some()
        && options
            .get("compression")
            .and_then(Value::as_str)
            .unwrap_or("none")
            == "none"
    {
        return Err(error(
            "unsupported_option",
            Some(&importer),
            Some("max_compressed_bytes"),
            "compressed byte limit requires gzip",
        ));
    }
    if fields.contains_key("options") {
        match importer.as_str() {
            "filesystem" => validate_options::<FilesystemOptions>(&importer, &options)?,
            "tar" => validate_options::<TarOptions>(&importer, &options)?,
            "casitar" => validate_options::<CasitarOptions>(&importer, &options)?,
            "filesystem_nar" => validate_options::<NarOptions>(&importer, &options)?,
            #[cfg(feature = "git")]
            "git" => validate_options::<GitOptions>(&importer, &options)?,
            _ => validate_options::<EmptyOptions>(&importer, &options)?,
        }
    }
    serde_json::from_value(Value::Object(fields)).map_err(|cause| {
        error(
            "invalid_parameters",
            Some(&importer),
            None,
            cause.to_string(),
        )
    })
}

pub(super) enum ImportRequest {
    Single(ImportParams),
    Batch(Vec<ImportParams>),
}

pub(super) fn parse_request(params: Params) -> Result<ImportRequest, Error> {
    match super::request_items(params)? {
        super::RequestItems::Single(params) => parse(params).map(ImportRequest::Single),
        super::RequestItems::Batch(items) => items
            .into_iter()
            .map(parse)
            .collect::<Result<Vec<_>, _>>()
            .map(ImportRequest::Batch),
    }
}

pub(super) async fn run_request<PS: BlobGc + 'static, SS: MetadataStore + 'static>(
    repository: Arc<Repository<PS, SS>>,
    request: ImportRequest,
) -> Result<Value, Error> {
    let ImportRequest::Batch(items) = request else {
        let ImportRequest::Single(params) = request else {
            unreachable!()
        };
        return run(repository, params).await;
    };
    // Validate every request before opening sources or staging bytes.
    let mut names = std::collections::HashSet::new();
    for item in &items {
        let name = match item {
            ImportParams::Blob { root, .. }
            | ImportParams::Filesystem { root, .. }
            | ImportParams::Tar { root, .. } => root,
            _ => {
                return Err(error(
                    "unsupported_importer",
                    None,
                    None,
                    "batch import supports blob, filesystem, and tar",
                ));
            }
        };
        let name = root(name)?;
        if !names.insert(name) {
            return Err(Error::invalid_params("duplicate batch root"));
        }
    }
    if items.len() > repository.limits().max_root_changes {
        return Err(Error::invalid_params("batch exceeds mutation root limit"));
    }
    if items.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    let session = repository.mutation_session().await.map_err(failed)?;
    let mut objects = Vec::new();
    let mut changes = Vec::new();
    let mut results = Vec::new();
    for item in items {
        let (staged, result) = match item {
            ImportParams::Blob {
                path, root: name, ..
            } => {
                let reader = tokio::fs::File::open(path).await.map_err(failed)?;
                let staged = casita::import::BlobImport::new(reader, root(&name)?)
                    .stage(&session)
                    .await
                    .map_err(failed)?;
                let result = json!({"object": staged.report.to_string()});
                (staged, result)
            }
            ImportParams::Filesystem {
                path,
                root: name,
                options,
            } => {
                let mut request = casita::import::FilesystemImport::new(path, root(&name)?)
                    .reread(options.reread);
                if let Some(exclude) = options.exclude {
                    request = request.exclude(exclude);
                }
                let staged = request.stage(&session).await.map_err(failed)?;
                let result = json!({"object": staged.report.to_string()});
                (staged, result)
            }
            ImportParams::Tar {
                path,
                root: name,
                options,
            } => {
                let reader = tar_reader(path, &options).await?;
                let staged = casita::import::TarImport::new(reader, root(&name)?)
                    .with_limits(options.limits.resolve())
                    .stage(&session)
                    .await
                    .map_err(failed)?;
                let result = tar_result(&staged.report);
                let staged = casita::import::StagedImport {
                    report: staged.report.root,
                    objects: staged.objects,
                    root_change: staged.root_change,
                    metadata_changes: staged.metadata_changes,
                };
                (staged, result)
            }
            _ => unreachable!("batch importers were validated"),
        };
        objects.extend(staged.objects);
        if objects.len() > repository.limits().max_batch_objects {
            return Err(failed("batch exceeds mutation object limit"));
        }
        changes.push(staged.root_change);
        results.push(result);
    }
    if !changes.is_empty() {
        session.publish(objects, changes).await.map_err(failed)?;
    }
    Ok(Value::Array(results))
}

async fn tar_reader(
    path: PathBuf,
    options: &TarOptions,
) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, Error> {
    let reader = tokio::fs::File::open(path).await.map_err(failed)?;
    Ok(match options.compression {
        Compression::None => Box::new(reader),
        Compression::Gzip => {
            let limited = BoundedReader {
                inner: reader,
                remaining: options.max_compressed_bytes.unwrap_or(1 << 40),
            };
            let mut decoder = async_compression::tokio::bufread::GzipDecoder::new(
                tokio::io::BufReader::new(limited),
            );
            decoder.multiple_members(true);
            Box::new(decoder)
        }
    })
}

fn tar_result(report: &casita::TarImportReport) -> Value {
    json!({
        "object": report.root.to_string(), "archive_bytes": report.archive_bytes,
        "entries": report.entries, "files": report.files, "directories": report.directories,
        "symlinks": report.symlinks, "hardlinks": report.hardlinks,
        "file_bytes": report.file_bytes, "sparse_expansion_bytes": report.sparse_expansion_bytes,
    })
}

// Validate options independently of required parameters, so unsupported
// requests are distinguishable even when the source does not exist. Testing
// partial defaulted objects also identifies the offending typed option.
fn validate_options<T: serde::de::DeserializeOwned>(
    importer: &str,
    options: &Value,
) -> Result<(), Error> {
    let Err(cause) = serde_json::from_value::<T>(options.clone()) else {
        return Ok(());
    };
    let mut option = "options".to_owned();
    if let Some(fields) = options.as_object() {
        for (key, value) in fields {
            if serde_json::from_value::<T>(json!({key: value})).is_err() {
                option = key.clone();
                if key == "limits"
                    && let Some(limits) = value.as_object()
                {
                    for (limit, value) in limits {
                        if serde_json::from_value::<T>(json!({"limits": {limit: value}})).is_err() {
                            option = format!("limits.{limit}");
                            break;
                        }
                    }
                }
                break;
            }
        }
    }
    let message = cause.to_string();
    let category = if message.starts_with("unknown field") || message.starts_with("unknown variant")
    {
        "unsupported_option"
    } else {
        "invalid_parameters"
    };
    Err(error(category, Some(importer), Some(&option), message))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EmptyOptions {}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct NarOptions {
    reread: bool,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct FilesystemOptions {
    reread: bool,
    exclude: Option<PathBuf>,
}

impl Default for FilesystemOptions {
    fn default() -> Self {
        Self {
            reread: true,
            exclude: None,
        }
    }
}

// Wire limits are partial overrides of backend defaults, so those defaults
// have one owner and unknown options cannot silently disable a bound.
macro_rules! limit_options {
    ($name:ident, $backend:ty, { $($field:ident: $ty:ty),* $(,)? }) => {
        #[derive(Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        pub(super) struct $name {
            $($field: Option<$ty>,)*
        }

        impl $name {
            fn resolve(self) -> $backend {
                let mut limits = <$backend>::default();
                $(if let Some(value) = self.$field { limits.$field = value; })*
                limits
            }
        }
    };
}

limit_options!(TarLimits, casita::TarImportLimits, {
    max_in_flight_files: usize,
    max_archive_bytes: u64,
    max_entries: usize,
    max_path_bytes: usize,
    max_file_bytes: u64,
    max_total_file_bytes: u64,
    max_sparse_expansion_bytes: u64,
});

limit_options!(CasitarLimits, casita::CasitarStreamLimits, {
    max_header_bytes: usize,
    max_record_bytes: usize,
    max_payload_bytes: u64,
    max_total_payload_bytes: u64,
    max_archive_bytes: u64,
    max_payloads: usize,
    max_records: usize,
    read_buffer_bytes: usize,
});

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct TarOptions {
    limits: TarLimits,
    compression: Compression,
    max_compressed_bytes: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Compression {
    #[default]
    None,
    Gzip,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct CasitarOptions {
    conflict_policy: ConflictPolicy,
    limits: CasitarLimits,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConflictPolicy {
    #[default]
    RequireAbsent,
    ReplaceIfUnchanged,
}

#[cfg(feature = "git")]
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct GitOptions {
    refs: Vec<String>,
    revisions: Vec<String>,
    max_cached_pack_bytes: Option<u64>,
    concurrency: Option<std::num::NonZeroUsize>,
    max_buffered_bytes: Option<std::num::NonZeroU64>,
}

fn root(name: &str) -> Result<RootName, Error> {
    RootName::try_from(name).map_err(|_| Error::invalid_params("invalid root"))
}

fn failed(cause: impl std::fmt::Display) -> Error {
    error(
        "execution_failure",
        None,
        None,
        format!("import failed: {cause}"),
    )
}

#[tracing::instrument(name = "ipc.artifact.import", skip_all)]
async fn execute<PS, SS>(
    repository: Arc<Repository<PS, SS>>,
    params: ImportParams,
) -> Result<Value, Error>
where
    PS: BlobGc + 'static,
    SS: MetadataStore + 'static,
{
    match params {
        ImportParams::Blob {
            path,
            root: name,
            options: _options,
        } => {
            let name = root(&name)?;
            let reader = tokio::fs::File::open(path).await.map_err(failed)?;
            let object = repository
                .import(casita::import::BlobImport::new(reader, name))
                .await
                .map_err(failed)?;
            Ok(json!({"object": object.to_string()}))
        }
        ImportParams::Copy {
            path,
            source_root,
            root: name,
            options: _options,
        } => {
            let name = root(&name)?;
            let source_root = root(&source_root)?;
            // Opening an absent source must not initialize a new repository.
            if !path.join("casita.sqlite").exists() {
                return Err(failed("source repository does not exist"));
            }
            let source = casita::Repository::local(path).await.map_err(failed)?;
            let object = repository
                .import(casita::import::CopyImport::new(&source, source_root, name))
                .await
                .map_err(failed)?;
            Ok(json!({"object": object.to_string()}))
        }
        ImportParams::Nar {
            path,
            root: name,
            options: _options,
        } => {
            let name = root(&name)?;
            let reader = tokio::fs::File::open(path).await.map_err(failed)?;
            let report = repository
                .import(casita::import::NarImport::new(reader))
                .await
                .map_err(failed)?;
            publish_nar(&repository, name, report).await
        }
        ImportParams::FilesystemNar {
            path,
            root: name,
            options,
        } => {
            let name = root(&name)?;
            let report = repository
                .import(casita::import::FilesystemNarImport::new(path).reread(options.reread))
                .await
                .map_err(failed)?;
            publish_nar(&repository, name, report).await
        }
        ImportParams::Filesystem {
            path,
            root: name,
            options,
        } => {
            let mut request =
                casita::import::FilesystemImport::new(path, root(&name)?).reread(options.reread);
            if let Some(exclude) = options.exclude {
                request = request.exclude(exclude);
            }
            let object = repository.import(request).await.map_err(failed)?;
            Ok(json!({ "object": object.to_string() }))
        }
        ImportParams::Tar {
            path,
            root: name,
            options,
        } => {
            let name = root(&name)?;
            let reader = tar_reader(path, &options).await?;
            let request =
                casita::import::TarImport::new(reader, name).with_limits(options.limits.resolve());
            let report = repository.import(request).await.map_err(failed)?;
            Ok(tar_result(&report))
        }
        ImportParams::Casitar {
            path,
            destinations,
            options,
        } => {
            let destinations = destinations
                .iter()
                .map(|name| root(name))
                .collect::<Result<Vec<_>, _>>()?;
            if destinations.is_empty() {
                return Err(Error::invalid_params("destinations must not be empty"));
            }
            let reader = tokio::fs::File::open(path).await.map_err(failed)?;
            let request = casita::import::CasitarImport::with_limits(
                reader,
                destinations,
                options.limits.resolve(),
            )
            .with_conflict_policy(match options.conflict_policy {
                ConflictPolicy::RequireAbsent => casita::CasitarRootConflictPolicy::RequireAbsent,
                ConflictPolicy::ReplaceIfUnchanged => {
                    casita::CasitarRootConflictPolicy::ReplaceIfUnchanged
                }
            });
            let report = repository.import(request).await.map_err(failed)?;
            let mappings: Vec<_> = report
                .mappings
                .into_iter()
                .map(|mapping| {
                    json!({
                        "index": mapping.index,
                        "root": mapping.root.to_string(),
                        "name": mapping.name.to_string(),
                    })
                })
                .collect();
            Ok(json!({
                "mappings": mappings,
                "destination_revision": report.destination_revision.to_string(),
                "records_inserted": report.records_inserted,
                "records_reused": report.records_reused,
                "payloads_written": report.payloads_written,
                "payloads_reused": report.payloads_reused,
            }))
        }
        #[cfg(feature = "git")]
        ImportParams::Git {
            path,
            view,
            options,
        } => {
            casita::experimental::git_view_root_name(&view)
                .map_err(|e| Error::invalid_params(e.to_string()))?;
            let mut request = casita::import::GitImport::new(path, view)
                .with_revisions(options.revisions)
                .map_err(|e| Error::invalid_params(e.to_string()))?
                .with_refs(options.refs)
                .map_err(|error| Error::invalid_params(error.to_string()))?;
            if let Some(limit) = options.max_cached_pack_bytes {
                request = request.with_max_cached_pack_bytes(limit);
            }
            if let Some(limit) = options.concurrency {
                request = request.with_concurrency(limit);
            }
            if let Some(limit) = options.max_buffered_bytes {
                request = request.with_max_buffered_bytes(limit);
            }
            let report = repository.import(request).await.map_err(failed)?;
            Ok(json!({
                "view": report.view.to_string(),
                "objects": report.objects,
                "revision": report.revision.to_string(),
            }))
        }
    }
}

async fn publish_nar<PS: BlobGc + 'static, SS: MetadataStore + 'static>(
    repository: &Repository<PS, SS>,
    name: RootName,
    report: casita::VerifiedNarReport,
) -> Result<Value, Error> {
    // An envelope preserves root-file mode and inline symlinks, and gives every
    // NAR a normal rooted graph that copy, Casitar and GC understand.
    let mut directory = casita::Directory::new();
    directory
        .add(
            casita::PathComponent::try_from("root").unwrap(),
            report.root().clone(),
        )
        .map_err(failed)?;
    let session = repository.mutation_session().await.map_err(failed)?;
    let staged = session.stage_directory(&directory).await.map_err(failed)?;
    let object = staged.record().key().clone();
    session
        .publish_rooted(vec![staged], name, object.clone())
        .await
        .map_err(failed)?;
    Ok(json!({"object": object.to_string(), "nar_size": report.nar_size()}))
}

struct BoundedReader<R> {
    inner: R,
    remaining: u64,
}
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for BoundedReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::{pin::Pin, task::Poll};
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.remaining == 0 {
            let mut byte = [0];
            let mut probe = tokio::io::ReadBuf::new(&mut byte);
            return match Pin::new(&mut this.inner).poll_read(cx, &mut probe) {
                Poll::Ready(Ok(())) if !probe.filled().is_empty() => {
                    Poll::Ready(Err(std::io::Error::other("compressed input exceeds limit")))
                }
                result => result,
            };
        }
        let count = buf
            .remaining()
            .min(usize::try_from(this.remaining).unwrap_or(usize::MAX));
        let mut part = tokio::io::ReadBuf::new(buf.initialize_unfilled_to(count));
        match Pin::new(&mut this.inner).poll_read(cx, &mut part) {
            Poll::Ready(Ok(())) => {
                let count = part.filled().len();
                buf.advance(count);
                this.remaining -= count as u64;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

pub(super) async fn run<PS: BlobGc + 'static, SS: MetadataStore + 'static>(
    repository: Arc<Repository<PS, SS>>,
    params: ImportParams,
) -> Result<Value, Error> {
    let importer = match &params {
        ImportParams::Filesystem { .. } => "filesystem",
        ImportParams::Blob { .. } => "blob",
        ImportParams::Copy { .. } => "copy",
        ImportParams::Nar { .. } => "nar",
        ImportParams::FilesystemNar { .. } => "filesystem_nar",
        ImportParams::Tar { .. } => "tar",
        ImportParams::Casitar { .. } => "casitar",
        #[cfg(feature = "git")]
        ImportParams::Git { .. } => "git",
    };
    execute(repository, params).await.map_err(|cause| {
        error(
            if cause.code == jsonrpc_core::ErrorCode::InvalidParams {
                "invalid_parameters"
            } else {
                "execution_failure"
            },
            Some(importer),
            None,
            cause.message,
        )
    })
}
