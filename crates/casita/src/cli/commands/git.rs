//! Native Git view inspection, checkout, and smart-HTTP serving.

use casita::experimental::ObjectKey;

use super::Error;
#[cfg(feature = "git-http")]
use super::usage_error;
use crate::cli::NativeGitCommand;

#[cfg(feature = "git-http")]
async fn serve_native_git<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    view: String,
    listen: String,
    max_pack_bytes: usize,
    pack_compression_level: u32,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore + Clone + Send + Sync + 'static,
    SS: casita::experimental::MetadataStore + Clone + Send + Sync + 'static,
{
    if max_pack_bytes == 0 {
        return Err(usage_error("--max-pack-bytes must be at least 1"));
    }
    let mut limits = casita::experimental::GitFetchLimits::default();
    limits.max_pack_bytes = max_pack_bytes;
    limits.compression_level = pack_compression_level;
    let service = casita::experimental::GitFetchService::bind(repository, &view, limits).await?;
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    let address = listener.local_addr()?;
    let route = format!("/{view}.git");
    println!("http://{address}{route}");
    casita::experimental::serve_git_smart_http_with_shutdown(
        listener,
        route,
        service,
        casita::experimental::GitHttpOptions::default(),
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await?;
    Ok(())
}

#[cfg(not(feature = "git-http"))]
async fn serve_native_git<PS, SS>(
    _repository: &casita::experimental::Repository<PS, SS>,
    _view: String,
    _listen: String,
    _max_pack_bytes: usize,
    _pack_compression_level: u32,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore + Clone + Send + Sync + 'static,
    SS: casita::experimental::MetadataStore + Clone + Send + Sync + 'static,
{
    Err("native Git smart-HTTP serving requires the 'git-http' cargo feature".into())
}

pub(super) async fn run_native_git<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    command: NativeGitCommand,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore + Clone + Send + Sync + 'static,
    SS: casita::experimental::MetadataStore + Clone + Send + Sync + 'static,
{
    match command {
        NativeGitCommand::Show { view } => {
            let (key, body) = casita::experimental::read_git_view(repository, &view)
                .await?
                .ok_or_else(|| {
                    casita::experimental::RepositoryError::Absent(format!("Git view `{view}`"))
                })?;
            println!("view {key}");
            println!("object-format {:?}", body.object_format);
            if let Some(default_ref) = body.default_ref {
                println!("default-ref {default_ref}");
            }
            for (name, value) in body.refs {
                match value {
                    casita::experimental::GitRefValue::Direct(target) => {
                        println!("ref {name} {target}");
                    }
                    casita::experimental::GitRefValue::Symbolic(target) => {
                        println!("ref {name} -> {target}");
                    }
                }
            }
        }
        NativeGitCommand::Checkout {
            tree,
            dir,
            skip_gitlinks,
        } => {
            let tree: ObjectKey = tree.parse()?;
            let policy = if skip_gitlinks {
                casita::experimental::GitlinkCheckoutPolicy::Skip
            } else {
                casita::experimental::GitlinkCheckoutPolicy::Error
            };
            casita::experimental::checkout_git_tree(repository, &tree, &dir, policy).await?;
            println!("checked out {tree} to {}", dir.display());
        }
        NativeGitCommand::Serve {
            view,
            listen,
            max_pack_bytes,
            pack_compression_level,
        } => {
            serve_native_git(
                repository,
                view,
                listen,
                max_pack_bytes,
                pack_compression_level,
            )
            .await?;
        }
    }
    Ok(())
}
