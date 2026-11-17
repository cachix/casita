//! Root publication, retention, removal, and listing.

use casita::experimental::{
    BlobId, ConditionalPublishResult, Digest, DirectoryId, ObjectKey, RootChange, RootName,
};
use futures::{StreamExt, TryStreamExt};

use super::{Error, scoped_root, workspace::Workspace};
use crate::cli::RootCommand;

async fn resolve_root_target<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    value: &str,
) -> Result<ObjectKey, Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    if value.contains(':') {
        return Ok(value.parse()?);
    }
    let digest: Digest = value.parse()?;
    let snapshot = repository.metadata().snapshot().await?;
    let directory = ObjectKey::directory(DirectoryId::new(digest));
    let blob = ObjectKey::blob(BlobId::new(digest));
    match (
        snapshot.object(&directory).await?.is_some(),
        snapshot.object(&blob).await?.is_some(),
    ) {
        (true, false) => Ok(directory),
        (false, true) => Ok(blob),
        (false, false) => Err(casita::experimental::RepositoryError::Absent(format!(
            "no blob or directory record has digest {digest}"
        ))
        .into()),
        (true, true) => Err(casita::experimental::RepositoryError::InvalidInput(format!(
            "digest {digest} is ambiguous; supply the full generic object key"
        ))
        .into()),
    }
}

pub(super) async fn generic_root<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    command: RootCommand,
    workspace: Option<&Workspace>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    match command {
        RootCommand::Set {
            name,
            target,
            retention,
            if_target,
            if_absent,
        } => {
            let name = scoped_root(workspace, name)?;
            let target = resolve_root_target(repository, &target).await?;
            if if_target.is_some() || if_absent {
                let expected = if_target.map(|value| value.parse()).transpose()?;
                match repository
                    .set_root_if_matches(name, target, expected, retention.map(Into::into))
                    .await?
                {
                    ConditionalPublishResult::Committed(_) => {}
                    ConditionalPublishResult::RootMismatch {
                        name,
                        expected,
                        actual,
                    } => {
                        let describe = |key: Option<ObjectKey>| {
                            key.map_or_else(|| "absent".to_owned(), |key| key.to_string())
                        };
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::AlreadyExists,
                            format!(
                                "root {name} changed: expected {}, found {}; nothing published",
                                describe(expected),
                                describe(actual),
                            ),
                        )
                        .into());
                    }
                }
            } else if let Some(retention) = retention {
                repository
                    .set_root_with_retention(name, target, retention.into())
                    .await?;
            } else {
                repository
                    .mutation_session()
                    .await?
                    .publish(Vec::new(), vec![RootChange::Set { name, target }])
                    .await?;
            }
        }
        RootCommand::Retention { name, policy } => {
            let name = scoped_root(workspace, name)?;
            repository.set_root_retention(&name, policy.into()).await?;
        }
        RootCommand::Rm { name, prefix } => {
            let snapshot = repository.metadata().snapshot().await?;
            let roots = snapshot.roots().try_collect::<Vec<_>>().await?;
            let selected = if let Some(prefix) = prefix {
                let prefix = match workspace {
                    Some(workspace) => workspace.scoped_prefix(&prefix)?,
                    None => RootName::try_from(prefix)?,
                };
                roots
                    .into_iter()
                    .filter(|root| root.name().is_under(&prefix))
                    .collect::<Vec<_>>()
            } else {
                let name = scoped_root(
                    workspace,
                    name.expect("clap requires a name when --prefix is absent"),
                )?;
                roots
                    .into_iter()
                    .filter(|root| root.name() == &name)
                    .collect::<Vec<_>>()
            };
            if selected.is_empty() {
                return Err(
                    casita::experimental::RepositoryError::Absent("root name".to_owned()).into(),
                );
            }
            let changes = selected
                .iter()
                .map(|root| RootChange::Remove {
                    name: root.name().clone(),
                })
                .collect();
            repository
                .mutation_session()
                .await?
                .publish(Vec::new(), changes)
                .await?;
            for root in selected {
                let name = workspace
                    .and_then(|workspace| workspace.display_name(root.name()))
                    .unwrap_or_else(|| root.name().as_str());
                println!("removed {name}");
            }
        }
        RootCommand::Ls { prefix, long } => {
            let prefix = match workspace {
                Some(workspace) => Some(workspace.scoped_prefix(&prefix)?),
                None if prefix.is_empty() => None,
                None => Some(RootName::try_from(prefix)?),
            };
            let snapshot = repository.metadata().snapshot().await?;
            let mut roots = snapshot.roots();
            while let Some(root) = roots.next().await {
                let root = root?;
                if prefix
                    .as_ref()
                    .is_none_or(|prefix| root.name().is_under(prefix))
                {
                    let name = workspace
                        .and_then(|workspace| workspace.display_name(root.name()))
                        .unwrap_or_else(|| root.name().as_str());
                    if long {
                        let retention = repository.root_retention(root.name()).await?;
                        let retention = match retention {
                            Some(casita::RootRetention::Evictable) => "evictable",
                            _ => "permanent",
                        };
                        println!("{}  {retention}  {name}", root.target());
                    } else {
                        println!("{}  {name}", root.target());
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use casita::experimental::{
        MemoryBlobStore, MemoryMetadataStore, MetadataStore as _, Repository,
    };

    #[tokio::test]
    async fn conditional_root_set_compares_only_the_scoped_workspace_name() {
        let repository =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let session = repository.mutation_session().await.unwrap();
        let object = session.stage_blob(b"build").await.unwrap();
        let key = object.record().key().clone();
        let raw_name = RootName::try_from("build/current").unwrap();
        session
            .publish_rooted(vec![object], raw_name.clone(), key.clone())
            .await
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let workspace = Workspace::create_at(directory.path()).unwrap();
        let command = || RootCommand::Set {
            name: raw_name.to_string(),
            target: key.to_string(),
            retention: None,
            if_target: None,
            if_absent: true,
        };
        generic_root(&repository, command(), Some(&workspace))
            .await
            .unwrap();
        let scoped = workspace.root_name(raw_name.as_str()).unwrap();
        let before = repository.metadata().snapshot().await.unwrap();
        assert_eq!(before.root(&raw_name).await.unwrap(), Some(key.clone()));
        assert_eq!(before.root(&scoped).await.unwrap(), Some(key.clone()));
        let error = generic_root(&repository, command(), Some(&workspace))
            .await
            .unwrap_err();
        assert_eq!(
            crate::cli::stable_error_category(error.as_ref()),
            casita::experimental::RepositoryErrorCategory::DestinationConflict
        );
        assert_eq!(
            repository.metadata().snapshot().await.unwrap().revision(),
            before.revision()
        );
    }
}
