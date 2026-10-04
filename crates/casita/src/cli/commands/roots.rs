//! Root publication, retention, removal, and listing.

use casita::experimental::{BlobId, Digest, DirectoryId, ObjectKey, RootChange, RootName};
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
        } => {
            let name = scoped_root(workspace, name)?;
            let target = resolve_root_target(repository, &target).await?;
            if let Some(retention) = retention {
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
