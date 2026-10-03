//! Importer detection and archive/image import workflows.

use std::io::Read as _;
use std::path::Path;

use casita::experimental::RootName;
use casita::import::Importer as _;

use super::{Error, archive::open_archive_input, usage_error};
#[cfg(feature = "oci")]
use crate::cli::OciImportArgs;
use crate::cli::{ImporterKind, TarImportArgs};

/// Choose a built-in importer without trusting a filename extension. Archive
/// inputs are reopened by their importer after this small, bounded probe.
pub(in crate::cli) async fn detect_importer(path: &Path) -> Result<ImporterKind, Error> {
    if path == Path::new("-") {
        return Err(usage_error(
            "automatic importer detection cannot replay standard input; select an importer with -i",
        ));
    }

    let metadata = std::fs::metadata(path)?;
    if metadata.is_dir() {
        return Ok(if is_git_repository(path) {
            ImporterKind::Git
        } else {
            ImporterKind::Filesystem
        });
    }
    if !metadata.is_file() {
        return Err(usage_error(format!(
            "cannot detect an importer for {}; select one with -i",
            path.display()
        )));
    }

    let mut header = [0_u8; 1024];
    let mut input = std::fs::File::open(path)?;
    let bytes = input.read(&mut header)?;
    let header = &header[..bytes];
    if header.starts_with(casita::experimental::CASITAR_MAGIC) {
        Ok(ImporterKind::Casitar)
    } else if is_tar_header(header) {
        Ok(ImporterKind::Tar)
    } else {
        Err(usage_error(format!(
            "cannot detect an importer for {}; select one with -i",
            path.display()
        )))
    }
}

fn is_git_repository(path: &Path) -> bool {
    let dot_git = path.join(".git");
    dot_git.is_dir()
        || dot_git.is_file()
        || (path.join("HEAD").is_file() && path.join("objects").is_dir())
}

fn is_tar_header(bytes: &[u8]) -> bool {
    let Some(header) = bytes.get(..512) else {
        return false;
    };
    if header.iter().all(|byte| *byte == 0) {
        return bytes
            .get(512..1024)
            .is_some_and(|block| block.iter().all(|byte| *byte == 0));
    }
    let Some(expected) = parse_tar_checksum(&header[148..156]) else {
        return false;
    };
    let actual = header
        .iter()
        .enumerate()
        .map(|(index, byte)| {
            if (148..156).contains(&index) {
                u32::from(b' ')
            } else {
                u32::from(*byte)
            }
        })
        .sum::<u32>();
    actual == expected
}

fn parse_tar_checksum(field: &[u8]) -> Option<u32> {
    let field = field
        .iter()
        .copied()
        .skip_while(|byte| *byte == b' ' || *byte == 0)
        .take_while(|byte| *byte != b' ' && *byte != 0)
        .collect::<Vec<_>>();
    if field.is_empty() || field.iter().any(|byte| !(b'0'..=b'7').contains(byte)) {
        return None;
    }
    field.into_iter().try_fold(0_u32, |value, byte| {
        value.checked_mul(8)?.checked_add(u32::from(byte - b'0'))
    })
}

#[cfg(feature = "git")]
pub(super) fn automatic_git_view(path: &Path) -> Result<String, Error> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| usage_error("cannot derive a Git view name; pass --git-view"))?;
    casita::experimental::git_view_root_name(name)?;
    Ok(name.into())
}

pub(super) async fn import_tar<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    input: &Path,
    name: RootName,
    args: TarImportArgs,
    retention: Option<casita::RootRetention>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    let input = open_archive_input(&input.to_string_lossy()).await?;
    let mut request =
        casita::import::TarImport::new(input, name.clone()).with_limits(args.limits());
    if let Some(retention) = retention {
        request = request.with_retention(retention);
    }
    let report = request.import(repository).await?;
    println!("root {}", report.root);
    println!("name {name}");
    println!("archive-bytes {}", report.archive_bytes);
    println!("entries {}", report.entries);
    println!("files {}; directories {}", report.files, report.directories);
    println!(
        "symlinks {}; hardlinks {}",
        report.symlinks, report.hardlinks
    );
    println!(
        "file-bytes {}; sparse-expansion-bytes {}",
        report.file_bytes, report.sparse_expansion_bytes
    );
    Ok(())
}

#[cfg(feature = "oci")]
pub(super) async fn import_oci<PS, SS>(
    repository: &casita::experimental::Repository<PS, SS>,
    reference: &Path,
    name: RootName,
    args: OciImportArgs,
    rootfs_name: Option<RootName>,
) -> Result<(), Error>
where
    PS: casita::experimental::BlobStore,
    SS: casita::experimental::MetadataStore + 'static,
{
    use oci_client::client::{ClientConfig, ClientProtocol};
    use oci_client::{Client, Reference};

    let reference: Reference = reference
        .to_str()
        .ok_or_else(|| usage_error("OCI image reference must be UTF-8"))?
        .parse()?;
    let mut config = ClientConfig::default();
    if args.http {
        config.protocol = ClientProtocol::Http;
    }
    if let Some(platform) = args.platform.as_deref() {
        let parts: Vec<_> = platform.split('/').collect();
        if !(2..=3).contains(&parts.len()) || parts.iter().any(|part| part.is_empty()) {
            return Err(usage_error("--oci-platform requires OS/ARCH[/VARIANT]"));
        }
    }
    let limits = casita::OciImportLimits {
        max_blob_bytes: args.max_blob_bytes,
        max_total_blob_bytes: args.max_total_blob_bytes,
        ..Default::default()
    };
    let mut request = casita::import::OciImport::new(reference, name.clone())
        .with_client(Client::new(config))
        .with_limits(limits);
    if let Some(platform) = args.platform {
        request = request.with_platform(platform);
    }
    if let Some(root) = rootfs_name.as_ref() {
        request = request
            .with_rootfs(root.clone())
            .with_rootfs_limits(casita::OciRootfsLimits {
                max_layer_bytes: args.rootfs_max_bytes,
                max_total_archive_bytes: args.rootfs_max_bytes,
                max_entries: args.rootfs_max_entries,
                max_tree_entries: args.rootfs_max_entries,
                ..Default::default()
            });
    }
    let report = request.import(repository).await?;
    println!("root {}", report.root);
    println!("name {name}");
    println!("manifest {}", report.manifest_digest);
    println!("layers {}; blob-bytes {}", report.layers, report.blob_bytes);
    if let Some(key) = report.rootfs {
        println!("rootfs {key}");
        println!(
            "rootfs-name {}",
            rootfs_name.expect("requested filesystem root")
        );
    }
    Ok(())
}
