//! Offline restoration into a staged sibling, published only on success.
use super::import::error;
use casita::RootName;
use casita::experimental::{BlobGc, MetadataStore, Repository};
use jsonrpc_core::{Error, Params, Value};
use serde::Deserialize;
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
#[cfg(feature = "git")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreParams {
    #[serde(default = "filesystem")]
    importer: String,
    root: String,
    path: PathBuf,
}
fn filesystem() -> String {
    "filesystem".into()
}

pub(super) async fn run<PS: BlobGc + 'static, SS: MetadataStore + 'static>(
    repository: Arc<Repository<PS, SS>>,
    params: Params,
) -> Result<Value, Error> {
    match super::request_items(params)? {
        super::RequestItems::Single(params) => run_one(repository, params).await,
        super::RequestItems::Batch(items) => {
            let requests = items
                .into_iter()
                .map(parse)
                .collect::<Result<Vec<_>, _>>()?;
            if requests.is_empty() {
                return Ok(Value::Array(Vec::new()));
            }
            let reader = repository
                .retained_reader()
                .await
                .map_err(|e| error("execution_failure", None, None, e.to_string()))?;
            let mut results = Vec::with_capacity(requests.len());
            for params in requests {
                let root = RootName::try_from(params.root.as_str()).unwrap();
                let result = restore(&repository, &reader, &params, root)
                    .await
                    .map_err(|e| {
                        error(
                            "execution_failure",
                            Some(&params.importer),
                            None,
                            e.to_string(),
                        )
                    })?;
                results.push(result);
            }
            Ok(Value::Array(results))
        }
    }
}

fn parse(params: Params) -> Result<RestoreParams, Error> {
    let params: RestoreParams = params.parse()?;
    if !super::import::IMPORTERS.contains(&params.importer.as_str()) {
        return Err(error(
            "unsupported_importer",
            Some(&params.importer),
            None,
            "unsupported restore importer",
        ));
    }
    RootName::try_from(params.root.as_str()).map_err(|e| {
        error(
            "invalid_parameters",
            Some(&params.importer),
            None,
            e.to_string(),
        )
    })?;
    Ok(params)
}

async fn run_one<PS: BlobGc + 'static, SS: MetadataStore + 'static>(
    repository: Arc<Repository<PS, SS>>,
    params: Params,
) -> Result<Value, Error> {
    let importer = match &params {
        Params::Map(v) => v
            .get("importer")
            .and_then(Value::as_str)
            .unwrap_or("filesystem"),
        _ => "filesystem",
    }
    .to_owned();
    let invalid = |e: String| error("invalid_parameters", Some(&importer), None, e);
    let params: RestoreParams = params.parse().map_err(|e| invalid(e.message))?;
    if !super::import::IMPORTERS.contains(&params.importer.as_str()) {
        return Err(error(
            "unsupported_importer",
            Some(&importer),
            None,
            "unsupported restore importer",
        ));
    }
    let root = RootName::try_from(params.root.as_str()).map_err(|e| invalid(e.to_string()))?;
    let failed = |e: Box<dyn std::error::Error + Send + Sync>| {
        error("execution_failure", Some(&importer), None, e.to_string())
    };
    let reader = repository
        .retained_reader()
        .await
        .map_err(|e| failed(Box::new(e)))?;
    restore(&repository, &reader, &params, root)
        .await
        .map_err(failed)
}

type Failure = Box<dyn std::error::Error + Send + Sync>;
async fn restore<PS: BlobGc + 'static, SS: MetadataStore + 'static>(
    repository: &Repository<PS, SS>,
    reader: &casita::RetainedReader,
    params: &RestoreParams,
    root: RootName,
) -> Result<Value, Failure> {
    let Some(object) = reader.root(&root).await? else {
        return Ok(json!({"present": false}));
    };
    match tokio::fs::symlink_metadata(&params.path).await {
        Ok(metadata) => {
            if !metadata.is_dir()
                || tokio::fs::read_dir(&params.path)
                    .await?
                    .next_entry()
                    .await?
                    .is_some()
            {
                return Err("destination must be absent or an empty real directory".into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => return Err(e.into()),
    }
    let parent = params
        .path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let stage = tempfile::Builder::new()
        .prefix(".casita-restore-")
        .tempdir_in(parent)?;
    let output = stage.path().join("output");
    let mut result_path = output.clone();
    match params.importer.as_str() {
        "blob" => {
            if object.namespace().as_str() != casita::experimental::BLOB_NAMESPACE {
                return Err("root is not a blob".into());
            }
            let mut source = reader.open_verified(&object).await?.ok_or("missing blob")?;
            let mut file = tokio::fs::File::create(&output).await?;
            tokio::io::copy(&mut source, &mut file).await?;
            file.sync_all().await?;
        }
        #[cfg(feature = "git")]
        "git" => restore_git(reader, &object, &output).await?,
        "nar" | "filesystem_nar" => {
            reader.checkout(&object, &output).await?;
            result_path = output.join("root");
            // Require the envelope shape documented by the NAR adapter.
            let mut entries = tokio::fs::read_dir(&output).await?;
            let first = entries.next_entry().await?.ok_or("empty NAR envelope")?;
            if first.file_name() != "root" || entries.next_entry().await?.is_some() {
                return Err("root is not a NAR envelope".into());
            }
        }
        _ => reader.checkout(&object, &output).await?,
    }
    if tokio::fs::symlink_metadata(&result_path).await?.is_dir() {
        // Rename cannot replace a nonempty directory, including one populated
        // concurrently after the preflight check.
        tokio::fs::rename(&result_path, &params.path).await?;
    } else {
        // Unlike rename, hard-link publication never overwrites an existing file.
        // File and symlink roots require an absent destination.
        tokio::fs::hard_link(&result_path, &params.path).await?;
    }
    if let Err(error) = repository.touch_root(&root, &object).await {
        tracing::debug!(%error, "could not update root access time");
    }
    Ok(json!({"present": true, "object": object.to_string()}))
}

#[cfg(feature = "git")]
async fn restore_git(
    reader: &casita::RetainedReader,
    object: &casita::ObjectKey,
    output: &Path,
) -> Result<(), Failure> {
    use casita::experimental::{GitObjectFormat, GitRefValue, GitViewBody, git_key_parts};
    if object.namespace().as_str() != casita::experimental::GIT_VIEW_NAMESPACE {
        return Err("root is not a Git view".into());
    }
    let mut source = reader
        .open_verified(object)
        .await?
        .ok_or("missing Git view")?;
    let mut bytes = Vec::new();
    source.read_to_end(&mut bytes).await?;
    let view = GitViewBody::decode(&bytes)?;
    tokio::fs::create_dir_all(output.join("objects")).await?;
    tokio::fs::create_dir_all(output.join("refs")).await?;
    let config = match view.object_format {
        GitObjectFormat::Sha1 => "[core]\nrepositoryformatversion = 0\nbare = true\n",
        GitObjectFormat::Sha256 => {
            "[core]\nrepositoryformatversion = 1\nbare = true\n[extensions]\nobjectFormat = sha256\n"
        }
    };
    tokio::fs::write(output.join("config"), config).await?;
    for object in view.objects() {
        let (_, kind, oid) = git_key_parts(object)?;
        let hex = data_encoding::HEXLOWER.encode(oid);
        let directory = output.join("objects").join(&hex[..2]);
        tokio::fs::create_dir_all(&directory).await?;
        let file = tokio::fs::File::create(directory.join(&hex[2..])).await?;
        let mut encoder = async_compression::tokio::write::ZlibEncoder::new(file);
        let mut body = reader
            .open_verified(object)
            .await?
            .ok_or("missing Git object")?;
        encoder
            .write_all(format!("{} {}\0", kind.as_str(), body.record().payload_size()).as_bytes())
            .await?;
        tokio::io::copy(&mut body, &mut encoder).await?;
        encoder.shutdown().await?;
    }
    for (name, value) in &view.refs {
        let path = output.join(name.as_str());
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        let text = match value {
            GitRefValue::Direct(key) => format!(
                "{}\n",
                data_encoding::HEXLOWER.encode(git_key_parts(key)?.2)
            ),
            GitRefValue::Symbolic(target) => format!("ref: {}\n", target.as_str()),
        };
        tokio::fs::write(path, text).await?;
    }
    let head = if let Some(name) = view.default_ref.as_ref() {
        format!("ref: {}\n", name.as_str())
    } else if let Some(key) = view.objects().iter().find(|key| {
        matches!(
            git_key_parts(key),
            Ok((_, casita::experimental::GitObjectKind::Commit, _))
        )
    }) {
        format!(
            "{}\n",
            data_encoding::HEXLOWER.encode(git_key_parts(key)?.2)
        )
    } else {
        "ref: refs/heads/main\n".into()
    };
    tokio::fs::write(output.join("HEAD"), head).await?;
    Ok(())
}
