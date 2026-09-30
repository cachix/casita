//! Verified content-addressed storage with a small application API.
//!
//! `Repository` opens built-in storage and provides filesystem import,
//! checkout, named roots, streaming reads and writes, synchronization, archives,
//! collection, and integrity checks. Backend types stay private.
//!
//! ```no_run
//! # #[cfg(feature = "native")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! use casita::{Repository, RootName};
//! let repository = Repository::local("./data").await?;
//! let root = repository.import(casita::import::FilesystemImport::new("./project", RootName::try_from("project")?)).await?;
//! repository.checkout(&root, "./restored").await?;
//! # Ok(())
//! # }
//! ```
//!
//! With `default-features = false`, only identities and the filesystem data
//! model are public. The default `cli` feature includes `native`, which adds
//! repository workflows; `git` and `s3` add built-in Git import and shared S3
//! storage respectively.
//!
//! Enable `experimental` for `casita::experimental`: the generic repository,
//! custom backend and format traits, protocol services, and low-level tuning.
//! That namespace may change between releases. `cli` and `fuzzing` enable it
//! for the project's command-line and testing tools.
//!
//! Implementation modules are private:
//!
//! ```compile_fail
//! use casita::wire::decode_manifest;
//! ```
//!
//! Backend traits are not exported from the application namespace:
//!
//! ```compile_fail
//! use casita::BlobStore;
//! ```
//!
//! Catalog records are read-only in the application API. Construction and
//! binary encoding use free functions in `casita::experimental`, even when
//! that feature is enabled:
//!
//! ```compile_fail
//! fn encode(record: &casita::ObjectRecord) {
//!     record.encode();
//! }
//! ```
//!
//! ```compile_fail
//! fn construct(name: casita::RootName, key: casita::ObjectKey) {
//!     casita::RootRecord::new(name, key);
//! }
//! ```
//!
//! ```compile_fail
//! fn decode(bytes: &[u8]) {
//!     casita::ObjectKey::decode(bytes);
//! }
//! ```

#![deny(unsafe_code)]
// Every type reachable from the supported API must have a public name.
#![cfg_attr(not(feature = "experimental"), deny(unnameable_types))]
// Unexported experimental entry points are intentionally retained internally.
#![cfg_attr(not(feature = "experimental"), allow(dead_code))]
// Repository errors intentionally preserve exact object keys, closure status,
// and typed backend context at the public boundary. Boxing every propagation
// layer would obscure that API without reducing the bounded underlying data.
#![allow(clippy::result_large_err)]
// docs.rs builds with `--cfg docsrs` on nightly (see Cargo.toml) so the
// feature-gated APIs render with their cfg badges. `doc_auto_cfg` was merged
// into `doc_cfg` in Rust 1.92; enabling that feature turns auto-cfg on by
// default, and `doc(auto_cfg)` states it explicitly.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]

/// Compile-tests the README's code examples as doctests. The README shows the
/// local repository profile, so it only compiles with the native backends.
#[cfg(all(doctest, feature = "native"))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

/// Compile-check the Rust examples shown in the documentation site's library
/// guide against this crate's current public API.
#[cfg(all(doctest, feature = "native"))]
#[doc = include_str!("../../../docs/src/content/docs/library.md")]
struct LibraryGuideDoctests;

#[cfg(all(doctest, feature = "native"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/filesystem.md")]
struct FilesystemGuideDoctests;

#[cfg(all(doctest, feature = "native"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/tar.md")]
struct TarGuideDoctests;

#[cfg(all(doctest, feature = "native", feature = "git"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/git.md")]
struct GitGuideDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/reference/rust-api.md")]
struct RustApiReferenceDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/concepts/blob-storage.md")]
struct BlobStorageDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/adding-an-importer.md")]
struct AddingImporterDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/custom-formats.md")]
struct CustomFormatsGuideDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/casitar.md")]
struct CasitarGuideDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/design/casitar.md")]
struct CasitarDesignDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/reference/experimental-rust-api.md")]
struct ExperimentalRustApiDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/concepts/garbage-collection.md")]
struct GarbageCollectionDoctests;

#[cfg(all(doctest, feature = "native", feature = "experimental"))]
#[doc = include_str!("../../../docs/src/content/docs/concepts/shared-payload-services.md")]
struct SharedPayloadServicesDoctests;

#[cfg(all(doctest, feature = "native"))]
#[doc = include_str!("../../../docs/src/content/docs/guides/s3-multi-owner.md")]
struct S3MultiOwnerGuideDoctests;

#[cfg(feature = "native")]
mod blob;
#[cfg(feature = "native")]
mod byte_budget;
mod casitar;
#[cfg(feature = "native")]
mod collection;
#[cfg(feature = "native")]
mod compression;
#[cfg(feature = "native")]
mod conformance;
#[cfg(feature = "native")]
mod coordination;
mod digest;
mod directory;
mod encode;
mod error;
#[cfg(feature = "native")]
mod filesystem;
mod format;
mod git;
/// Import requests for built-in and experimental repository workflows.
#[cfg(feature = "native")]
pub mod import;
#[cfg(feature = "native")]
mod importers;
mod invariant;
mod ipld;
mod linked;
#[cfg(feature = "native")]
mod nar;
mod node;
mod object;
mod path;
#[cfg(feature = "native")]
mod repository;
#[cfg(feature = "native")]
mod spill;

#[cfg(feature = "native")]
mod metadata;
#[cfg(all(test, feature = "native"))]
mod object_read_tests;
#[cfg(all(test, feature = "native"))]
mod scale_benchmarks;
#[cfg(feature = "native")]
mod sqlite;
#[cfg(feature = "native")]
mod sync;
#[cfg(feature = "native")]
mod tar;
#[cfg(test)]
pub(crate) mod test_util;
#[cfg(feature = "native")]
mod verified;
#[cfg(any(feature = "native", test))]
mod wire;

/// Parser access for the repository's fuzz harnesses. This feature is tooling
/// infrastructure and is not part of the supported application API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    pub use crate::sync::sliced::fuzz_decode as fuzz_sliced_payload;
    pub use crate::sync::wire::{decode_presence, decode_records, encode_presence, encode_records};
    pub use crate::wire::decode_manifest;
}

// Advanced entry points are also used internally when their public export is
// disabled. Keeping one implementation avoids feature-dependent semantics.
#[cfg(feature = "experimental")]
pub mod experimental;
#[cfg(not(feature = "experimental"))]
mod experimental;
#[allow(unused_imports)]
pub(crate) use experimental::*;

pub use digest::{BlobId, Digest, DigestError, DirectoryId, ObjectId};
pub use directory::Directory;
pub use encode::DirectoryDecodeError;
pub use error::{DirectoryError, RetryDisposition};
pub use node::Node;
pub use object::{
    NamespaceId, NamespaceIdError, ObjectKey, ObjectKeyError, ObjectRecord, RepositoryGeneration,
    RepositoryRevision, RepositoryRevisionError, RootName, RootNameError, RootRecord,
};
pub use path::{PathComponent, PathComponentError, SymlinkTarget, SymlinkTargetError};

#[cfg(feature = "native")]
mod api;
#[cfg(feature = "native")]
pub use api::{
    CollectionReport, Error, IntegrityReport, MetadataReader, Reader, Repository, RetainedReader,
    VerifiedReader,
};
#[cfg(feature = "native")]
pub use metadata::{
    MetadataChange, MetadataCheck, MetadataCommitResult, MetadataCursor, MetadataKey, MetadataPage,
    MetadataRecord,
};
#[cfg(feature = "native")]
pub use repository::{
    FsckDisposition as IntegrityDisposition, FsckIssue as IntegrityIssue,
    FsckIssueKind as IntegrityIssueKind, RepositoryErrorCategory as ErrorKind, RootRetention,
};

#[cfg(feature = "native")]
pub use casitar::{
    CasitarImportReport, CasitarRootConflictPolicy, CasitarRootMapping, CasitarStats,
    CasitarStreamLimits,
};
#[cfg(feature = "git")]
pub use git::repository::NativeGitImportOutcome;
#[cfg(feature = "native")]
pub use spill::SpillMetrics;
#[cfg(feature = "native")]
pub use tar::{TarImportLimits, TarImportReport};

#[cfg(feature = "native")]
pub use nar::{
    NarAssociationCleanup, NarError, NarHashAlgorithm, NarHashMethod, NarRequirements,
    NarVerificationStats, VerifiedNarReport, ensure_nar, lookup_nar, prune_nar_associations,
    scrub_nar,
};
