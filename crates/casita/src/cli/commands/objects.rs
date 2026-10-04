//! Object inspection, tree listing, and payload output.

use casita::experimental::{ClosureStatus, Node, ObjectKey};
use tokio::io::AsyncWriteExt;

use super::{Error, parse_blob_or_exact_key, parse_directory_key};

fn require_complete(key: &ObjectKey, status: ClosureStatus) -> Result<(), Error> {
    if matches!(status, ClosureStatus::Complete { .. }) {
        Ok(())
    } else {
        Err(casita::experimental::RepositoryError::ObjectNotReadable {
            object: key.clone(),
            status,
        }
        .into())
    }
}

pub(super) async fn object_show<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let key: ObjectKey = value.parse()?;
    let hold = repository.retention_hold().await?;
    let record = hold
        .object(&key)
        .await?
        .ok_or_else(|| casita::experimental::RepositoryError::Absent(format!("object {key}")))?;
    println!("key {}", record.key());
    println!("payload {}", record.payload());
    println!("payload-size {}", record.payload_size());
    println!("links {}", record.links().len());
    for link in record.links() {
        println!("  {link}");
    }
    println!("closure {:?}", hold.verify_closure(&key).await?);
    Ok(())
}

pub(super) async fn tree_list<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let key = parse_directory_key(value)?;
    if key.namespace().as_str() != casita::experimental::DIRECTORY_NAMESPACE {
        return Err(casita::experimental::RepositoryError::InvalidInput(format!(
            "tree listing requires `{}`, got `{}`",
            casita::experimental::DIRECTORY_NAMESPACE,
            key.namespace()
        ))
        .into());
    }
    let hold = repository.retention_hold().await?;
    require_complete(&key, hold.verify_closure(&key).await?)?;
    let (record, mut reader) = hold
        .open_payload(&key)
        .await?
        .ok_or_else(|| casita::experimental::RepositoryError::Absent(format!("object {key}")))?;
    let limits = repository.limits();
    let directory = casita::experimental::read_directory_payload(
        &key,
        &record,
        &mut *reader,
        limits.max_metadata_bytes.min(limits.max_payload_bytes),
    )
    .await?;
    for (name, node) in directory.nodes() {
        match node {
            Node::Directory { digest, size } => {
                println!("d {size:>12}  {name}  {}", ObjectKey::directory(*digest));
            }
            Node::File {
                digest,
                size,
                executable,
            } => {
                let kind = if *executable { "x" } else { "f" };
                println!("{kind} {size:>12}  {name}  {}", ObjectKey::blob(*digest));
            }
            Node::Symlink { target } => println!("l {:>12}  {name} -> {target}", "-"),
        }
    }
    Ok(())
}

pub(super) async fn object_cat<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
    verified: bool,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let key = parse_blob_or_exact_key(value)?;
    let hold = repository.retention_hold().await?;
    require_complete(&key, hold.verify_closure(&key).await?)?;
    if verified {
        let record = hold.object(&key).await?.ok_or_else(|| {
            casita::experimental::RepositoryError::Absent(format!("object {key}"))
        })?;
        let mut reader = repository
            .payloads()
            .open_verified(&record.payload(), record.payload_size())
            .await?
            .ok_or_else(|| {
                casita::experimental::RepositoryError::MissingPayload(record.payload())
            })?;
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut reader, &mut stdout).await?;
        stdout.flush().await?;
        return Ok(());
    }
    let (_, mut reader) = hold
        .open_payload(&key)
        .await?
        .ok_or_else(|| casita::experimental::RepositoryError::Absent(format!("object {key}")))?;
    let mut stdout = tokio::io::stdout();
    tokio::io::copy(&mut reader, &mut stdout).await?;
    stdout.flush().await?;
    Ok(())
}
