//! Recursive comparison of held canonical filesystem trees.

use std::collections::{BTreeMap, btree_map};

use casita::experimental::{
    BlobStore, Directory, MetadataStore, Node, ObjectKey, PathComponent, Repository,
    RepositoryError, RetentionHold,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::{Error, objects::require_complete, parse_directory_key};

type EntryPair = (Option<Node>, Option<Node>);

struct Frame {
    path_len: usize,
    entries: btree_map::IntoIter<PathComponent, EntryPair>,
}

async fn read_directory<PS: BlobStore, SS: MetadataStore>(
    hold: &RetentionHold<'_, PS, SS>,
    key: Option<&ObjectKey>,
    limit: u64,
) -> Result<Directory, Error> {
    let Some(key) = key else {
        return Ok(Directory::new());
    };
    let (record, mut reader) = hold
        .open_payload(key)
        .await?
        .ok_or_else(|| RepositoryError::Absent(format!("object {key}")))?;
    Ok(casita::experimental::read_directory_payload(key, &record, &mut *reader, limit).await?)
}

async fn frame<PS: BlobStore, SS: MetadataStore>(
    hold: &RetentionHold<'_, PS, SS>,
    old: Option<&ObjectKey>,
    new: Option<&ObjectKey>,
    path_len: usize,
    limit: u64,
) -> Result<Frame, Error> {
    let mut entries = BTreeMap::<_, EntryPair>::new();
    for (name, node) in read_directory(hold, old, limit).await?.nodes() {
        entries.insert(name.clone(), (Some(node.clone()), None));
    }
    for (name, node) in read_directory(hold, new, limit).await?.nodes() {
        entries.entry(name.clone()).or_default().1 = Some(node.clone());
    }
    Ok(Frame {
        path_len,
        entries: entries.into_iter(),
    })
}

fn directory_key(node: Option<&Node>) -> Option<ObjectKey> {
    match node {
        Some(Node::Directory { digest, .. }) => Some(ObjectKey::directory(*digest)),
        _ => None,
    }
}

fn node_json(node: &Node) -> serde_json::Value {
    match node {
        Node::Directory { digest, size } => serde_json::json!({
            "kind": "directory", "key": ObjectKey::directory(*digest).to_string(), "size": size,
        }),
        Node::File {
            digest,
            size,
            executable,
        } => serde_json::json!({
            "kind": "file", "key": ObjectKey::blob(*digest).to_string(),
            "size": size, "executable": executable,
        }),
        Node::Symlink { target } => serde_json::json!({
            "kind": "symlink", "target": std::str::from_utf8(target.as_bytes()).ok(),
            "target_bytes": target.as_bytes(),
        }),
    }
}

async fn write_change(
    output: &mut (impl AsyncWrite + Unpin),
    json: bool,
    status: &str,
    path: &[u8],
    old: Option<&Node>,
    new: Option<&Node>,
) -> Result<(), Error> {
    let line = if json {
        serde_json::to_string(&serde_json::json!({
            "schema": "casita.tree-diff.v1",
            "status": status,
            "path": std::str::from_utf8(path).ok(),
            "path_bytes": path,
            "before": old.map(node_json),
            "after": new.map(node_json),
        }))?
    } else {
        // BStr's debug representation quotes and escapes raw bytes, including
        // line breaks and invalid UTF-8, so each change occupies exactly one line.
        format!("{status}\t{:?}", bstr::BStr::new(path))
    };
    output.write_all(line.as_bytes()).await?;
    output.write_all(b"\n").await?;
    Ok(())
}

pub(super) async fn write_diff<PS, SS>(
    repository: &Repository<PS, SS>,
    old: &str,
    new: &str,
    json: bool,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<(), Error>
where
    PS: BlobStore,
    SS: MetadataStore + 'static,
{
    let old = parse_directory_key(old)?;
    let new = parse_directory_key(new)?;
    for key in [&old, &new] {
        if key.namespace().as_str() != casita::experimental::DIRECTORY_NAMESPACE {
            return Err(RepositoryError::InvalidInput(format!(
                "tree diff requires `{}`, got `{}`",
                casita::experimental::DIRECTORY_NAMESPACE,
                key.namespace()
            ))
            .into());
        }
    }

    // Both sides use one immutable snapshot and stay protected from collection
    // throughout comparison. Validate even identical keys before returning.
    let hold = repository.retention_hold().await?;
    require_complete(&old, hold.verify_closure(&old).await?)?;
    if old != new {
        require_complete(&new, hold.verify_closure(&new).await?)?;
        let limits = repository.limits();
        let limit = limits.max_metadata_bytes.min(limits.max_payload_bytes);
        let mut stack = vec![frame(&hold, Some(&old), Some(&new), 0, limit).await?];
        let mut path = Vec::new();
        let mut visited = 0usize;
        while let Some(current) = stack.last_mut() {
            let Some((name, (old, new))) = current.entries.next() else {
                stack.pop();
                continue;
            };
            if visited >= limits.max_traversal_objects {
                return Err(RepositoryError::LimitExceeded(
                    "tree diff exceeds traversal entry limit".into(),
                )
                .into());
            }
            visited += 1;
            if old == new {
                continue;
            }
            path.truncate(current.path_len);
            if !path.is_empty() {
                path.push(b'/');
            }
            path.extend_from_slice(name.as_bytes());

            let old_dir = directory_key(old.as_ref());
            let new_dir = directory_key(new.as_ref());
            let status = match (&old, &new) {
                (None, _) => Some("A"),
                (_, None) => Some("D"),
                (Some(old), Some(new))
                    if std::mem::discriminant(old) != std::mem::discriminant(new) =>
                {
                    Some("T")
                }
                _ if old_dir.is_some() && new_dir.is_some() => None,
                _ => Some("M"),
            };
            if let Some(status) = status {
                write_change(output, json, status, &path, old.as_ref(), new.as_ref()).await?;
            }
            if old_dir != new_dir {
                stack.push(
                    frame(&hold, old_dir.as_ref(), new_dir.as_ref(), path.len(), limit).await?,
                );
            }
        }
    }
    output.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use casita::experimental::{
        FormatLimits, FormatRegistry, MemoryBlobStore, MemoryMetadataStore,
    };

    fn links(count: usize) -> Directory {
        Directory::try_from_iter((0..count).map(|index| {
            (
                PathComponent::try_from(format!("link-{index}")).unwrap(),
                Node::Symlink {
                    target: "../outside".try_into().unwrap(),
                },
            )
        }))
        .unwrap()
    }

    fn child(tree: &Directory) -> Node {
        Node::Directory {
            digest: tree.digest(),
            size: tree.size(),
        }
    }

    #[tokio::test]
    async fn repeated_dag_paths_obey_the_entry_limit() {
        let repository = Repository::with_formats(
            MemoryBlobStore::new(),
            MemoryMetadataStore::new().unwrap(),
            FormatRegistry::builtin(),
            FormatLimits {
                max_traversal_objects: 3,
                ..Default::default()
            },
        );
        let shared = links(3);
        let old = Directory::try_from_iter([("a".try_into().unwrap(), child(&shared))]).unwrap();
        let new = Directory::try_from_iter([
            ("a".try_into().unwrap(), child(&shared)),
            ("b".try_into().unwrap(), child(&shared)),
        ])
        .unwrap();
        let session = repository.mutation_session().await.unwrap();
        let mut staged = Vec::new();
        for tree in [&shared, &old, &new] {
            staged.push(session.stage_directory(tree).await.unwrap());
        }
        session.publish_unrooted(staged).await.unwrap();
        let error = write_diff(
            &repository,
            &old.digest().to_string(),
            &new.digest().to_string(),
            true,
            &mut Vec::new(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::LimitExceeded(message)) if message.contains("tree diff")
        ));
    }

    #[tokio::test]
    async fn deep_trees_are_walked_without_recursive_futures() {
        let repository = Repository::memory().unwrap();
        let session = repository.mutation_session().await.unwrap();
        let empty = Directory::new();
        let mut staged = vec![session.stage_directory(&empty).await.unwrap()];
        let mut tree = links(1);
        staged.push(session.stage_directory(&tree).await.unwrap());
        for _ in 0..256 {
            tree = Directory::try_from_iter([("d".try_into().unwrap(), child(&tree))]).unwrap();
            staged.push(session.stage_directory(&tree).await.unwrap());
        }
        session.publish_unrooted(staged).await.unwrap();
        let mut output = Vec::new();
        write_diff(
            &repository,
            &empty.digest().to_string(),
            &tree.digest().to_string(),
            false,
            &mut output,
        )
        .await
        .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output.lines().count(), 257);
        assert!(output.ends_with(&format!("A\t\"{}link-0\"\n", "d/".repeat(256))));
    }
}
