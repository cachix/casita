//! Tar stream import requests.

use async_trait::async_trait;

use super::{BackendImporter, Importer};
use crate::blob::BlobStore;
use crate::metadata::MetadataStore;
use crate::repository::Repository;
use crate::tar::{TarImportError, TarImportLimits, TarImportReport};
use crate::{RootName, RootRetention};
use tokio::io::AsyncRead;

/// One already-decompressed tar-stream import request.
pub struct TarImport<R> {
    /// Stream carrying complete raw tar bytes.
    reader: R,
    /// Destination-owned root name.
    root: RootName,
    /// Hostile-input and resource limits.
    limits: TarImportLimits,
    retention: Option<RootRetention>,
}

impl<PS, SS, R> BackendImporter<Repository<PS, SS>> for TarImport<R>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin + Send,
{
    type Report = TarImportReport;
    type Error = TarImportError;

    async fn import_into(
        self,
        repository: &Repository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        match self.retention {
            Some(retention) => {
                repository
                    .import_tar_with_retention(self.reader, self.root, self.limits, Some(retention))
                    .await
            }
            None => {
                repository
                    .import_tar(self.reader, self.root, self.limits)
                    .await
            }
        }
    }
}

impl<R> TarImport<R> {
    /// Set hostile-input and resource limits for the tar stream.
    pub fn with_limits(mut self, limits: TarImportLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Set the root's retention policy in the same publication as the import.
    pub fn with_retention(mut self, retention: RootRetention) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Import an already-decompressed tar stream using bounded defaults.
    pub fn new(reader: R, root: RootName) -> Self {
        Self {
            reader,
            root,
            limits: TarImportLimits::default(),
            retention: None,
        }
    }
}

impl<R: AsyncRead + Unpin + Send> TarImport<R> {
    /// Stage a complete tar graph and root change without publishing checkpoints.
    #[cfg(feature = "experimental")]
    pub async fn stage<'hold, PS: BlobStore, SS: MetadataStore>(
        self,
        session: &'hold crate::MutationSession<'_, PS, SS>,
    ) -> Result<super::StagedImport<'hold, TarImportReport>, TarImportError> {
        let mut objects = Vec::new();
        let report = session
            .repository()
            .import_tar_in_session(
                session,
                self.reader,
                self.root.clone(),
                self.limits,
                super::ImportPublication {
                    retention: self.retention,
                    staging: Some(&mut objects),
                },
            )
            .await?;
        let metadata_changes = self
            .retention
            .map(|retention| crate::repository::root_policy::policy_change(&self.root, retention))
            .into_iter()
            .collect();
        Ok(super::StagedImport {
            root_change: crate::RootChange::Set {
                name: self.root,
                target: report.root.clone(),
            },
            report,
            objects,
            metadata_changes,
        })
    }
}

#[async_trait]
impl<R: AsyncRead + Unpin + Send> Importer for TarImport<R> {
    type Report = TarImportReport;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        self.import_into(&repository.inner)
            .await
            .map_err(|error| crate::api::Error::classified(error.category(), error))
    }
}

#[cfg(feature = "experimental")]
repository_importer!(TarImport<R>, [R], TarImportReport, TarImportError);
