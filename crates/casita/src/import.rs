//! Import requests consumed by [`crate::Repository::import`].
//!
//! All importers share the [`Importer`](crate::import::Importer) trait. Git imports require `git`;
//! `MultiRootFilesystemImport` and `UnrootedFilesystemImport` require
//! `experimental` and retain their experimental status.

#[cfg(feature = "oci")]
pub use crate::importers::OciImport;
pub use crate::importers::{
    BlobImport, CasitarImport, CopyImport, FilesystemImport, FilesystemNarImport, Importer,
    NarImport, TarImport,
};
#[cfg(feature = "git")]
pub use crate::importers::{GitClosureImport, GitImport};
#[cfg(feature = "experimental")]
pub use crate::importers::{MultiRootFilesystemImport, UnrootedFilesystemImport};

#[cfg(feature = "experimental")]
pub use crate::importers::StagedImport;
