//! Import requests and adapters for the shared repository publication lifecycle.

use async_trait::async_trait;

/// One external input that publishes a verified graph into a repository.
///
/// `R` defaults to the application [`crate::Repository`]. The same built-in
/// requests also support experimental repositories with custom storage, and
/// blob and filesystem requests support an existing mutation session. Each importer
/// preserves its own report and error types. Returned futures are `Send` so
/// imports can run in task-based services as well as embedded applications.
///
/// A caller can stay generic without knowing the input format:
///
/// ```no_run
/// use casita::{import::Importer, Repository};
///
/// async fn ingest<I: Importer>(repository: &Repository, input: I)
///     -> Result<I::Report, I::Error>
/// {
///     repository.import(input).await
/// }
/// ```
#[async_trait]
pub trait Importer<R = crate::Repository>: Send {
    /// The importer-specific report returned after publication.
    type Report: Send;
    /// The importer-specific typed failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Consume this input and publish its verified graph.
    async fn import(self, repository: &R) -> Result<Self::Report, Self::Error>;
}

// Keep engine errors and backend types behind the experimental boundary.
// The public adapters below use this one implementation for every profile.
pub(crate) trait BackendImporter<R>: Send {
    type Report: Send;
    type Error: std::error::Error + Send + Sync + 'static;

    fn import_into(
        self,
        repository: &R,
    ) -> impl std::future::Future<Output = Result<Self::Report, Self::Error>> + Send;
}

// Experimental repositories preserve typed backend errors. Only these public
// adapters depend on the feature; ingestion itself is shared with the facade.
#[cfg(feature = "experimental")]
macro_rules! repository_importer {
    ($request:ty, [$($reader:ident),*], $report:ty, $error:ty) => {
        #[async_trait]
        impl<PS, SS, $($reader),*> Importer<Repository<PS, SS>> for $request
        where
            PS: BlobStore,
            SS: MetadataStore,
            $($reader: AsyncRead + Unpin + Send,)*
        {
            type Report = $report;
            type Error = $error;

            async fn import(self, repository: &Repository<PS, SS>) -> Result<Self::Report, Self::Error> {
                self.import_into(repository).await
            }
        }
    };
}

mod blob;
mod casitar;
mod copy;
mod filesystem;
#[cfg(feature = "git")]
mod git;
#[cfg(feature = "git")]
mod git_closure;
mod nar;
#[cfg(feature = "oci")]
mod oci;
mod tar;

pub use blob::BlobImport;
pub use casitar::CasitarImport;
pub use copy::CopyImport;
pub use filesystem::FilesystemImport;
#[cfg(feature = "experimental")]
pub use filesystem::{MultiRootFilesystemImport, UnrootedFilesystemImport};
#[cfg(feature = "git")]
pub use git::GitImport;
#[cfg(feature = "git")]
pub use git_closure::{
    GitClosureImport, GitClosureImportError, GitClosureImportOutcome, GitClosureImportReport,
};
pub use nar::{FilesystemNarImport, NarImport};
#[cfg(feature = "oci")]
pub use oci::OciImport;
pub use tar::TarImport;
