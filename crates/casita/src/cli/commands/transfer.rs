//! Repository endpoint transfers and hold inspection.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casita::experimental::MetadataStore as _;
use casita::experimental::{Digest, Node, ObjectKey, RootName};

use super::{
    Error,
    stats::{print_pack_stats, print_pack_stats_with_prefix},
    usage_error,
};
#[cfg(feature = "ssh")]
use crate::cli::SshSourceArgs;
use crate::cli::SyncArgs;

enum SyncSelection {
    Request(casita::experimental::TransferRequest),
    Path {
        source_root: RootName,
        path: String,
        destination_root: Option<RootName>,
    },
}

pub(super) struct SyncSource {
    pub(super) source: Box<dyn casita::experimental::TransferSource>,
    payloads: Option<casita::experimental::ChunkedBlobStore>,
}

pub(super) async fn open_sync_source(
    endpoint: &str,
    writer: &str,
    pack_target_bytes: Option<u64>,
    pack_cache_bytes: Option<u64>,
) -> Result<SyncSource, Error> {
    #[cfg(not(feature = "s3"))]
    let _ = (writer, pack_cache_bytes);
    let source_payloads;
    let source: Box<dyn casita::experimental::TransferSource> =
        if let Some(location) = s3_location(endpoint)? {
            #[cfg(feature = "s3")]
            {
                let repository = open_s3_repository(
                    location,
                    writer.to_owned(),
                    pack_target_bytes,
                    pack_cache_bytes,
                )
                .await?;
                source_payloads = Some(repository.payloads().clone());
                Box::new(repository)
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = location;
                return Err(usage_error("S3 sync requires the `s3` Cargo feature"));
            }
        } else if endpoint.starts_with("ssh://") {
            #[cfg(feature = "ssh")]
            {
                source_payloads = None;
                let endpoint = endpoint.parse::<casita::experimental::SshEndpoint>()?;
                Box::new(casita::experimental::SshTransferSource::new(endpoint))
            }
            #[cfg(not(feature = "ssh"))]
            {
                return Err("SSH sync requires the `ssh` Cargo feature".into());
            }
        } else {
            let path = PathBuf::from(endpoint);
            let repository = match pack_target_bytes {
                Some(target) => {
                    casita::experimental::Repository::local_with_pack_options(
                        path,
                        casita::experimental::PackOptions {
                            target_size: target,
                            ..Default::default()
                        },
                    )
                    .await?
                }
                None => casita::experimental::Repository::local(path).await?,
            };
            source_payloads = Some(repository.payloads().clone());
            Box::new(repository)
        };
    Ok(SyncSource {
        source,
        payloads: source_payloads,
    })
}

pub(super) async fn generic_sync(
    args: SyncArgs,
    spill_limits: casita::experimental::SpillLimits,
    pack_target_bytes: Option<u64>,
    pack_cache_bytes: Option<u64>,
) -> Result<(), Error> {
    if args.path.is_some() && (!args.objects.is_empty() || args.roots.len() != 1 || args.shallow) {
        return Err(usage_error(
            "sync --path requires exactly one --root and cannot be combined with --object or --shallow",
        ));
    }
    if args.objects.is_empty() && args.roots.is_empty() {
        return Err(usage_error(
            "sync requires at least one --object or --root selector",
        ));
    }
    if (pack_cache_bytes.is_some())
        && !args.from.starts_with("s3://")
        && !args.to.starts_with("s3://")
        && !args
            .from_blobs
            .as_deref()
            .is_some_and(|endpoint| endpoint.starts_with("s3://"))
    {
        return Err(usage_error(
            "pack cache tuning requires an S3 sync endpoint",
        ));
    }
    let writer = sync_writer(args.writer.as_deref())?;
    let metadata_source =
        open_sync_source(&args.from, &writer, pack_target_bytes, pack_cache_bytes).await?;
    let blob_source = match args.from_blobs.as_deref() {
        Some(endpoint) => {
            Some(open_sync_source(endpoint, &writer, pack_target_bytes, pack_cache_bytes).await?)
        }
        None => None,
    };
    let transfer_selection = casita::experimental::TransferSelection::Selected {
        objects: args
            .objects
            .iter()
            .map(|value| value.parse())
            .collect::<Result<Vec<_>, _>>()?,
        roots: args
            .roots
            .iter()
            .map(|value| RootName::try_from(value.clone()))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let source = metadata_source
        .source
        .begin_transfer(transfer_selection)
        .await?;
    let source: Box<dyn casita::experimental::TransferReadSession + '_> = match &blob_source {
        Some(blobs) => Box::new(casita::experimental::SplitTransferSession::new(
            source,
            blobs
                .source
                .begin_transfer(casita::experimental::TransferSelection::Snapshot)
                .await?,
        )),
        None => source,
    };

    let selection = if let Some(path) = args.path {
        SyncSelection::Path {
            source_root: RootName::try_from(args.roots[0].clone())?,
            path,
            destination_root: args.destination_root.map(RootName::try_from).transpose()?,
        }
    } else {
        let mut selected = BTreeMap::<ObjectKey, bool>::new();
        for value in args.objects {
            let key: ObjectKey = value.parse()?;
            selected
                .entry(key)
                .and_modify(|recursive| *recursive |= !args.shallow)
                .or_insert(!args.shallow);
        }
        let mut roots = BTreeMap::<RootName, ObjectKey>::new();
        for value in args.roots {
            let name = RootName::try_from(value)?;
            let target = source.root(&name).await?.ok_or_else(|| {
                casita::experimental::RepositoryError::Absent(format!("root `{name}`"))
            })?;
            selected
                .entry(target.clone())
                .and_modify(|recursive| *recursive = true)
                .or_insert(true);
            roots.insert(name, target);
        }
        SyncSelection::Request(casita::experimental::TransferRequest {
            objects: selected
                .into_iter()
                .map(|(key, recursive)| casita::experimental::ObjectRequest { key, recursive })
                .collect(),
            roots: roots
                .into_iter()
                .map(|(name, target)| casita::experimental::DestinationRoot { name, target })
                .collect(),
        })
    };
    if let Some(location) = s3_location(&args.to)? {
        #[cfg(feature = "s3")]
        {
            let destination =
                open_s3_repository(location, writer, pack_target_bytes, pack_cache_bytes)
                    .await?
                    .with_spill_limits(spill_limits);
            let result =
                finish_sync(source.as_ref(), &destination, selection, args.incremental).await;
            print_sync_source_stats(&metadata_source, blob_source.as_ref());
            print_pack_stats(destination.payloads());
            return result;
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = location;
            return Err(usage_error("S3 sync requires the `s3` Cargo feature"));
        }
    }
    let path = PathBuf::from(args.to);
    let destination = match pack_target_bytes {
        Some(target) => {
            casita::experimental::Repository::local_with_pack_options(
                path,
                casita::experimental::PackOptions {
                    target_size: target,
                    ..Default::default()
                },
            )
            .await?
        }
        None => casita::experimental::Repository::local(path).await?,
    }
    .with_spill_limits(spill_limits);
    let result = finish_sync(source.as_ref(), &destination, selection, args.incremental).await;
    print_sync_source_stats(&metadata_source, blob_source.as_ref());
    print_pack_stats(destination.payloads());
    result
}

fn print_sync_source_stats(metadata: &SyncSource, blobs: Option<&SyncSource>) {
    if let Some(payloads) = &metadata.payloads {
        print_pack_stats_with_prefix(
            payloads,
            if blobs.is_some() {
                "source-repo-"
            } else {
                "source-"
            },
        );
    }
    if let Some(payloads) = blobs.and_then(|source| source.payloads.as_ref()) {
        print_pack_stats_with_prefix(payloads, "source-blobs-");
    }
}

async fn finish_sync<PS, SS>(
    source: &dyn casita::experimental::TransferReadSession,
    destination: &casita::experimental::Repository<PS, SS>,
    selection: SyncSelection,
    incremental: bool,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let discovery = if incremental {
        casita::experimental::TransferDiscovery::ReuseVerified
    } else {
        casita::experimental::TransferDiscovery::Exhaustive
    };
    match selection {
        SyncSelection::Request(request) => {
            let result = casita::experimental::transfer(
                &casita::experimental::HeldSession(source),
                destination,
                request,
                casita::experimental::TransferOptions::default().with_discovery(discovery),
            )
            .await?;
            print_transfer_progress(result.progress);
        }
        SyncSelection::Path {
            source_root,
            path,
            destination_root,
        } => {
            let outcome = casita::experimental::transfer_path(
                &casita::experimental::HeldSession(source),
                destination,
                &source_root,
                &path,
                destination_root.clone(),
                casita::experimental::TransferOptions::default().with_discovery(discovery),
            )
            .await?;
            let node = outcome.node.ok_or_else(|| {
                casita::experimental::RepositoryError::Absent(format!(
                    "path `{path}` beneath source root `{source_root}`"
                ))
            })?;
            match node {
                Node::Directory { digest, .. } => println!("selected directory {digest}"),
                Node::File { digest, .. } => println!("selected blob {digest}"),
                Node::Symlink { target } => println!("selected symlink -> {target}"),
            }
            if let Some(name) = destination_root {
                println!("root {name}");
            }
            if let Some(result) = outcome.transfer {
                print_transfer_progress(result.progress);
            }
        }
    }
    Ok(())
}

fn print_transfer_progress(progress: casita::experimental::TransferProgress) {
    println!("revision {}", progress.destination_revision);
    println!("published-objects {}", progress.published_objects);
    println!("payloads-sent {}", progress.payloads_sent);
    println!("payloads-reused {}", progress.payloads_reused);
    println!("chunks-sent {}", progress.chunks_sent);
    println!("chunks-reused {}", progress.chunks_reused);
    println!("slice-copy-bytes {}", progress.slice_copy_bytes);
    println!("slice-literal-bytes {}", progress.slice_literal_bytes);
    for status in progress.requested {
        println!("status {status:?}");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Location {
    bucket: String,
    prefix: String,
}

fn s3_location(value: &str) -> Result<Option<S3Location>, Error> {
    let Some(location) = value.strip_prefix("s3://") else {
        return Ok(None);
    };
    if location.is_empty() || location.contains(['?', '#']) {
        return Err(usage_error(
            "S3 repository URLs must be s3://BUCKET or s3://BUCKET/PREFIX",
        ));
    }
    let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
    if bucket.is_empty() {
        return Err(usage_error(
            "S3 repository URLs must be s3://BUCKET or s3://BUCKET/PREFIX",
        ));
    }
    Ok(Some(S3Location {
        bucket: bucket.to_owned(),
        prefix: prefix.trim_matches('/').to_owned(),
    }))
}

#[cfg(feature = "s3")]
async fn open_s3_repository(
    location: S3Location,
    writer: String,
    pack_target_bytes: Option<u64>,
    pack_cache_bytes: Option<u64>,
) -> Result<
    casita::experimental::Repository<
        casita::experimental::ChunkedBlobStore,
        casita::experimental::Wal3MetadataStore,
    >,
    Error,
> {
    Ok(casita::experimental::Repository::s3_with_pack_options(
        location.bucket,
        location.prefix,
        writer,
        casita::experimental::PackOptions {
            target_size: pack_target_bytes
                .unwrap_or(casita::experimental::DEFAULT_PACK_TARGET_SIZE),
            cache_capacity: pack_cache_bytes
                .unwrap_or(casita::experimental::DEFAULT_PACK_CACHE_CAPACITY),
        },
    )
    .await?)
}

fn sync_writer(explicit: Option<&str>) -> Result<String, Error> {
    if let Some(writer) = explicit.map(ToOwned::to_owned).or_else(|| {
        std::env::var("CASITA_WRITER")
            .ok()
            .filter(|name| !name.is_empty())
    }) {
        return Ok(writer);
    }
    let mut instance = [0; 8];
    getrandom::fill(&mut instance).map_err(|error| format!("runner identity: {error}"))?;
    Ok(format!(
        "casita-{}-{}",
        std::process::id(),
        data_encoding::HEXLOWER.encode(&instance)
    ))
}

fn pin_inventory_json(inventory: &casita::experimental::PinInventory) -> serde_json::Value {
    use casita::experimental::{PinResource, PinScope};
    let catalog = |bytes: &[u8]| {
        serde_json::json!({
            "digest": Digest::hash(bytes).to_string(), "bytes": bytes.len(),
        })
    };
    let resource = |resource: &PinResource| match resource {
        PinResource::Blob(id) => serde_json::json!({"kind": "blob", "id": id.to_string()}),
        PinResource::Chunk(id) => serde_json::json!({"kind": "chunk", "id": id.to_string()}),
        PinResource::StorageObject(path) => {
            serde_json::json!({"kind": "storage_object", "path": path})
        }
        PinResource::Object(key) => serde_json::json!({"kind": "object", "key": key.to_string()}),
        PinResource::Catalog(bytes) => {
            serde_json::json!({"kind": "catalog", "catalog": catalog(bytes)})
        }
        PinResource::MetadataObject(path) => {
            serde_json::json!({"kind": "metadata_object", "path": path})
        }
    };
    let pins = inventory.pins.iter().map(|(token, pin)| {
        let scope = match &pin.scope {
            PinScope::Snapshot { generation } => serde_json::json!({"kind": "snapshot", "generation": generation}),
            PinScope::Closures(roots) => serde_json::json!({"kind": "closures", "roots": roots.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            PinScope::Staging => serde_json::json!({"kind": "staging"}),
            PinScope::Metadata => serde_json::json!({"kind": "metadata"}),
        };
        serde_json::json!({
            "token": token.to_string(), "scope": scope,
            "released": inventory.retired.contains(token),
            "catalog": pin.catalog.as_deref().map(catalog),
            "resources": pin.resources.iter().map(resource).collect::<Vec<_>>(),
        })
    }).collect::<Vec<_>>();
    let deletions = inventory.deletions.iter().map(|(token, resources)| serde_json::json!({
        "token": token.to_string(), "resources": resources.iter().map(resource).collect::<Vec<_>>(),
    })).collect::<Vec<_>>();
    serde_json::json!({
        "revision": inventory.revision, "pins": pins, "deletions": deletions,
        "collector": inventory.collector.as_ref().map(ToString::to_string),
        "logical_prune": inventory.logical_prune.as_ref().map(ToString::to_string),
    })
}

pub(super) async fn list_holds(endpoint: &str, json: bool) -> Result<(), Error> {
    let (collectors, state, coordination) = if let Some(location) = s3_location(endpoint)? {
        #[cfg(not(feature = "s3"))]
        {
            let _ = location;
            return Err(usage_error(
                "S3 hold inspection requires the `s3` Cargo feature",
            ));
        }
        #[cfg(feature = "s3")]
        {
            let prefix = if location.prefix.is_empty() {
                "state".to_owned()
            } else {
                format!("{}/state", location.prefix)
            };
            let state = casita::experimental::Wal3MetadataStore::open_s3(
                location.bucket,
                prefix,
                sync_writer(None)?,
            )
            .await?;
            let collectors = state.repository_holds().await?.iter().map(|hold| serde_json::json!({
                "token": hold.token.as_str(), "writer": hold.writer, "exclusive": hold.exclusive,
            })).collect::<Vec<_>>();
            let pins = state.pin_store().await?.inventory().await?;
            let coordination = state
                .repository_coordination_pin_store()
                .await?
                .inventory()
                .await?;
            (collectors, pins, Some(coordination))
        }
    } else {
        let database = Path::new(endpoint).join("casita.sqlite");
        if !database.is_file() {
            return Err(usage_error(format!(
                "repository database {} is absent",
                database.display()
            )));
        }
        let state = casita::experimental::TursoMetadataStore::open(database).await?;
        (
            Vec::<serde_json::Value>::new(),
            state.pin_store().await?.inventory().await?,
            None,
        )
    };
    let state = pin_inventory_json(&state);
    let coordination = coordination.as_ref().map(pin_inventory_json);
    if json {
        println!(
            "{}",
            serde_json::json!({"collectors": collectors, "state": state, "coordination": coordination})
        );
    } else {
        for collector in collectors {
            println!("collector {}", serde_json::to_string(&collector)?);
        }
        for (name, inventory) in std::iter::once(("state", &state)).chain(
            coordination
                .as_ref()
                .map(|inventory| ("coordination", inventory)),
        ) {
            println!(
                "{name} ledger revision={} collector={} logical_prune={}",
                inventory["revision"], inventory["collector"], inventory["logical_prune"]
            );
            for pin in inventory["pins"].as_array().expect("encoded pins array") {
                println!("{name} pin {}", serde_json::to_string(pin)?);
            }
            for deletion in inventory["deletions"]
                .as_array()
                .expect("encoded deletion array")
            {
                println!("{name} deletion {}", serde_json::to_string(deletion)?);
            }
        }
    }
    Ok(())
}

#[cfg(feature = "ssh")]
pub(super) async fn serve_ssh_source(args: SshSourceArgs) -> Result<(), Error> {
    let encoded = data_encoding::BASE64URL_NOPAD
        .decode(args.repository_base64.as_bytes())
        .map_err(|error| format!("invalid encoded SSH repository path: {error}"))?;
    let path =
        String::from_utf8(encoded).map_err(|_| "encoded SSH repository path is not valid UTF-8")?;
    let repository = casita::experimental::Repository::local(PathBuf::from(path)).await?;
    casita::experimental::serve_transfer_stdio(
        &repository,
        tokio::io::stdin(),
        tokio::io::stdout(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_urls_preserve_the_bucket_and_normalize_edge_slashes() {
        assert_eq!(
            s3_location("s3://bucket/releases/current").unwrap(),
            Some(S3Location {
                bucket: "bucket".into(),
                prefix: "releases/current".into(),
            })
        );
        assert_eq!(
            s3_location("s3://bucket/").unwrap(),
            Some(S3Location {
                bucket: "bucket".into(),
                prefix: String::new(),
            })
        );
        assert!(s3_location("s3://bucket?prefix=one").is_err());
        assert!(s3_location("s3://").is_err());
    }
}
