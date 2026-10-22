//! Ordered imports sharing retention while publishing each request independently.

use async_trait::async_trait;

use super::Importer;

/// Import requests in order using one staging pin lifetime.
///
/// Each request publishes independently. An error stops the sequence, leaving
/// earlier successes published. Blob and filesystem requests support sequences;
/// use [`super::BlobImport::batch`] for an atomic group of blob roots instead.
/// Empty sequences acquire no pins. Keep sequences bounded to limit retained
/// staging state. Inputs are consumed lazily, so callers can open one reader at
/// a time.
/// A sequence supports only the targets supported by its inputs; session-only
/// unrooted requests still require an explicitly held session.
///
/// ```no_run
/// # async fn example() -> Result<(), casita::Error> {
/// use casita::{import::{BlobImport, ImportSequence}, Repository};
/// let repository = Repository::memory()?;
/// let inputs = ["first", "second"].map(|name| {
///     BlobImport::new(std::io::Cursor::new(name.as_bytes()), name.parse().unwrap())
/// });
/// let keys = repository.import(ImportSequence::new(inputs)).await?;
/// assert_eq!(keys.len(), 2);
/// # Ok(())
/// # }
/// ```
pub struct ImportSequence<Inputs> {
    inputs: Inputs,
}

impl<Inputs> ImportSequence<Inputs> {
    /// Group inputs into an ordered sequence with independent publications.
    pub fn new(inputs: Inputs) -> Self {
        Self { inputs }
    }
}

async fn run<Inputs, Target>(
    inputs: Inputs,
    target: &Target,
) -> Result<
    Vec<<Inputs::Item as Importer<Target>>::Report>,
    <Inputs::Item as Importer<Target>>::Error,
>
where
    Inputs: IntoIterator + Send,
    Inputs::IntoIter: Send,
    Inputs::Item: Importer<Target>,
    Target: Sync,
{
    let mut reports = Vec::new();
    for input in inputs {
        reports.push(input.import(target).await?);
    }
    Ok(reports)
}

#[async_trait]
impl<Inputs, Report> Importer for ImportSequence<Inputs>
where
    Inputs: IntoIterator + Send,
    Inputs::IntoIter: Send,
    Report: Send,
    Inputs::Item: Importer<crate::Repository, Report = Report, Error = crate::Error>,
    for<'session> Inputs::Item:
        Importer<crate::ImportSession<'session>, Report = Report, Error = crate::Error>,
{
    type Report = Vec<Report>;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        let mut inputs = self.inputs.into_iter().peekable();
        if inputs.peek().is_none() {
            return Ok(Vec::new());
        }
        let session = repository.import_session().await?;
        session.import(ImportSequence::new(inputs)).await
    }
}

#[async_trait]
impl<'session, Inputs> Importer<crate::ImportSession<'session>> for ImportSequence<Inputs>
where
    Inputs: IntoIterator + Send,
    Inputs::IntoIter: Send,
    Inputs::Item: Importer<crate::ImportSession<'session>>,
{
    type Report = Vec<<Inputs::Item as Importer<crate::ImportSession<'session>>>::Report>;
    type Error = <Inputs::Item as Importer<crate::ImportSession<'session>>>::Error;

    async fn import(
        self,
        session: &crate::ImportSession<'session>,
    ) -> Result<Self::Report, Self::Error> {
        run(self.inputs, session).await
    }
}

#[cfg(feature = "experimental")]
#[async_trait]
impl<PS, SS, Inputs, Report> Importer<crate::repository::Repository<PS, SS>>
    for ImportSequence<Inputs>
where
    PS: crate::blob::BlobStore,
    SS: crate::metadata::MetadataStore,
    Inputs: IntoIterator + Send,
    Inputs::IntoIter: Send,
    Report: Send,
    Inputs::Item: Importer<
            crate::repository::Repository<PS, SS>,
            Report = Report,
            Error = crate::RepositoryError,
        >,
    for<'session> Inputs::Item: Importer<
            crate::MutationSession<'session, PS, SS>,
            Report = Report,
            Error = crate::RepositoryError,
        >,
{
    type Report = Vec<Report>;
    type Error = crate::RepositoryError;

    async fn import(
        self,
        repository: &crate::repository::Repository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        let mut inputs = self.inputs.into_iter().peekable();
        if inputs.peek().is_none() {
            return Ok(Vec::new());
        }
        let session = repository.mutation_session().await?;
        session.import(ImportSequence::new(inputs)).await
    }
}

#[cfg(feature = "experimental")]
#[async_trait]
impl<'session, PS, SS, Inputs> Importer<crate::MutationSession<'session, PS, SS>>
    for ImportSequence<Inputs>
where
    PS: crate::blob::BlobStore,
    SS: crate::metadata::MetadataStore,
    Inputs: IntoIterator + Send,
    Inputs::IntoIter: Send,
    Inputs::Item: Importer<crate::MutationSession<'session, PS, SS>>,
{
    type Report = Vec<<Inputs::Item as Importer<crate::MutationSession<'session, PS, SS>>>::Report>;
    type Error = <Inputs::Item as Importer<crate::MutationSession<'session, PS, SS>>>::Error;

    async fn import(
        self,
        session: &crate::MutationSession<'session, PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        run(self.inputs, session).await
    }
}
