//! Turso/SQLite implementation of revisioned logical repository state.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use turso::transaction::Transaction;
use turso::{Connection, params};

use super::{
    AddedObject, CollectedObjects, CommitChange, CommitResult, CommitState, MetadataCheck,
    MetadataError, MetadataKey, MetadataMutation, MetadataRecord, MetadataSnapshot, MetadataStore,
    ObjectChange, RETAINED_PAGE, RetainedObjects, RootChange, fresh_revision,
    validate_commit_transition,
};
use crate::digest::{BlobId, Digest};
use crate::error::Error as DatabaseError;
use crate::invariant;
use crate::object::{ObjectKey, ObjectRecord, RepositoryRevision, RootName, RootRecord};
use crate::sqlite::TursoDb;

/// Upper bound for one decoded canonical state record. The production format
/// limit permits at most one million links, whose worst-case key encoding is
/// below this ceiling. Bounding decompression keeps a corrupt database from
/// turning a point lookup into an unbounded allocation.
const MAX_ENCODED_RECORD_BYTES: usize = 256 * 1024 * 1024;
const COLLECTION_REFERENCE_BUFFER: usize = 65_536;
/// Small repositories validate collection directly in bounded memory; larger
/// repositories retain the spill-backed temporary-table path below.
const INLINE_COLLECTION_OBJECTS: usize = 65_536;

const OBJECT_BATCH_SIZE: usize = 256;

fn object_batch_sql(width: usize) -> &'static str {
    // Power-of-two widths bound the prepared cache to nine query shapes.
    // NULL padding cannot match the non-null composite primary key.
    static QUERIES: std::sync::LazyLock<Vec<String>> = std::sync::LazyLock::new(|| {
        (0..=8)
            .map(|power| {
                let values = (0..1usize << power)
                    .map(|index| format!("({index}, ?{}, ?{})", index * 2 + 1, index * 2 + 2))
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "WITH requested(ordinal, namespace, native_id) AS (VALUES {values}) \
                SELECT requested.ordinal, objects.record FROM requested \
                LEFT JOIN objects ON objects.namespace = requested.namespace \
                AND objects.native_id = requested.native_id"
                )
            })
            .collect()
    });
    &QUERIES[width.next_power_of_two().trailing_zeros() as usize]
}

/// How many objects closure validation walked.
///
/// The regression this guards is a cost, not a wrong answer, and cost must not
/// be asserted with a clock. Counting the objects one commit inspects pins it
/// exactly and deterministically instead. Outside tests the counter has no
/// fields and every method on it compiles away.
#[derive(Clone, Default)]
struct ValidationCounter(#[cfg(test)] Arc<std::sync::atomic::AtomicUsize>);

impl ValidationCounter {
    /// Count one object walked.
    fn record(&self) {
        #[cfg(test)]
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Read the count and reset it.
    #[cfg(test)]
    fn take(&self) -> usize {
        self.0.swap(0, std::sync::atomic::Ordering::Relaxed)
    }
}

/// Persistent revisioned logical state in Casita's SQLite-compatible database.
#[derive(Clone)]
pub struct TursoMetadataStore {
    db: Arc<TursoDb>,
    validated: ValidationCounter,
    pins: Arc<std::sync::OnceLock<Arc<super::FilePinStore>>>,
    /// Shared by clones so each deletion batch reaches the same database.
    commits: Option<crate::blob::CommitDurability>,
}

impl TursoMetadataStore {
    /// The database this store keeps its state in, which local accelerators
    /// such as the ingest cache share.
    pub(crate) fn database(&self) -> &Arc<TursoDb> {
        &self.db
    }

    /// Open the shared database and initialize its generic repository state.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, MetadataError> {
        Self::from_db(TursoDb::open(path).map_err(from_database_error)?).await
    }

    /// Attach to a shared database, creating its initial revision once.
    pub async fn from_db(db: Arc<TursoDb>) -> Result<Self, MetadataError> {
        let connection = db
            .begin_read_snapshot()
            .await
            .map_err(from_database_error)?;
        let mut rows = connection
            .query(
                "SELECT revision FROM repository_state WHERE singleton = 1",
                (),
            )
            .await?;
        let initialized = rows.next().await?.is_some();
        drop(rows);
        drop(connection);

        if !initialized {
            let initial = fresh_revision(None)?;
            db.write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    transaction
                        .execute(
                            "INSERT INTO repository_state (singleton, revision) VALUES (1, ?1)",
                            [initial.as_bytes().as_slice()],
                        )
                        .await?;
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await
            .map_err(from_database_error)?;
        }
        Ok(Self {
            commits: crate::blob::CommitDurability::for_database(db.path()),
            db,
            validated: ValidationCounter::default(),
            pins: Default::default(),
        })
    }
}

#[derive(Clone)]
struct TursoSnapshot {
    revision: RepositoryRevision,
    generation: u64,
    payload_catalog: Option<Vec<u8>>,
    connection: Arc<tokio::sync::Mutex<crate::sqlite::ReadConnection>>,
}

impl TursoSnapshot {
    async fn read<T, F>(&self, operation: F) -> Result<T, MetadataError>
    where
        F: for<'a> FnOnce(&'a Connection) -> BoxFuture<'a, Result<T, MetadataError>>
            + Send
            + 'static,
        T: Send + 'static,
    {
        let guard = self.connection.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || futures::executor::block_on(operation(&guard)))
            .await
            .map_err(|error| MetadataError::Backend(error.to_string()))?
    }
}

async fn read_snapshot_state(
    connection: &Connection,
) -> Result<(RepositoryRevision, Option<Vec<u8>>, u64), MetadataError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT revision, payload_catalog_ref, generation FROM repository_state WHERE singleton = 1",
        )
        .await?;
    let mut rows = statement.query(()).await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| MetadataError::Corruption("repository revision row is absent".to_owned()))?;
    let bytes: Vec<u8> = row.get(0)?;
    let generation = u64::try_from(row.get::<i64>(2)?)
        .map_err(|_| MetadataError::Corruption("negative metadata generation".into()))?;
    Ok((
        decode_revision(&bytes)?,
        row.get::<Option<Vec<u8>>>(1)?,
        generation,
    ))
}

async fn read_records(
    connection: &Connection,
    keys: &[MetadataKey],
) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
    let mut statement = connection
        .prepare_cached("SELECT value FROM metadata_records WHERE namespace = ?1 AND key = ?2")
        .await?;
    let mut values = Vec::with_capacity(keys.len());
    let mut bytes = 0;
    for key in keys {
        let mut rows = statement
            .query(params![key.namespace.as_str(), key.key.as_ref()])
            .await?;
        values.push(match rows.next().await? {
            Some(row) => Some(bytes::Bytes::from(row.get::<Vec<u8>>(0)?)),
            None => None,
        });
        bytes += values.last().unwrap().as_ref().map_or(0, bytes::Bytes::len);
        if bytes > super::records::MAX_BATCH_BYTES {
            return Err(super::records::invalid(
                "metadata get result exceeds 16 MiB; split the batch",
            ));
        }
    }
    Ok(values)
}

#[async_trait]
impl MetadataSnapshot for TursoSnapshot {
    async fn get(&self, keys: &[MetadataKey]) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        super::records::validate_get(keys)?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys.to_vec();
        self.read(move |connection| Box::pin(async move { read_records(connection, &keys).await }))
            .await
    }

    async fn scan(
        &self,
        prefix: &MetadataKey,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<MetadataRecord>, MetadataError> {
        super::records::validate_scan(prefix, after, limit)?;
        let prefix = prefix.clone();
        let after = after.map(Vec::from);
        self.read(move |connection| Box::pin(async move {
            // Separate seeks avoid OR predicates and OFFSET scans. At most
            // four cached query shapes, independent of keys and page widths.
            let upper = super::records::prefix_end(&prefix.key);
            let sql = match (after.is_some(), upper.is_some()) {
                (false, false) => "SELECT key, value FROM metadata_records WHERE namespace = ?1 AND key >= ?2 ORDER BY key LIMIT ?3",
                (true, false) => "SELECT key, value FROM metadata_records WHERE namespace = ?1 AND key > ?2 ORDER BY key LIMIT ?3",
                (false, true) => "SELECT key, value FROM metadata_records WHERE namespace = ?1 AND key >= ?2 AND key < ?4 ORDER BY key LIMIT ?3",
                (true, true) => "SELECT key, value FROM metadata_records WHERE namespace = ?1 AND key > ?2 AND key < ?4 ORDER BY key LIMIT ?3",
            };
            let lower = after.as_deref().unwrap_or(&prefix.key);
            let mut statement = connection.prepare_cached(sql).await?;
            let mut rows = if let Some(upper) = upper {
                statement.query(params![prefix.namespace.as_str(), lower, (limit + 1) as i64, upper]).await?
            } else {
                statement.query(params![prefix.namespace.as_str(), lower, (limit + 1) as i64]).await?
            };
            let mut records = Vec::new();
            let mut bytes = 0;
            while let Some(row) = rows.next().await? {
                records.push(MetadataRecord {
                    key: MetadataKey::new(prefix.namespace.clone(), row.get::<Vec<u8>>(0)?),
                    value: row.get::<Vec<u8>>(1)?.into(),
                });
                let last = records.last().unwrap();
                bytes += last.key.key.len() + last.value.len();
                if bytes > super::records::MAX_BATCH_BYTES { break; }
            }
            Ok(records)
        })).await
    }

    fn revision(&self) -> RepositoryRevision {
        self.revision
    }

    fn generation(&self) -> Result<u64, MetadataError> {
        Ok(self.generation)
    }

    fn payload_catalog(&self) -> Option<&[u8]> {
        self.payload_catalog.as_deref()
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, MetadataError> {
        let key = key.clone();
        self.read(move |connection| Box::pin(async move { read_record(connection, &key).await }))
            .await
    }

    async fn object_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, MetadataError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys.to_vec();
        self.read(move |connection| {
            Box::pin(async move {
                // The ordinal preserves duplicates and caller order without
                // sorting SQL output. Bound both parameters and decoded rows.
                let mut records = vec![None; keys.len()];
                for (chunk, output) in keys
                    .chunks(OBJECT_BATCH_SIZE)
                    .zip(records.chunks_mut(OBJECT_BATCH_SIZE))
                {
                    let width = chunk.len().next_power_of_two();
                    let mut values = Vec::with_capacity(width * 2);
                    for key in chunk {
                        values.push(turso::Value::Text(key.namespace().as_str().to_owned()));
                        values.push(turso::Value::Blob(key.native_id().to_vec()));
                    }
                    values.resize(width * 2, turso::Value::Null);
                    let mut statement = connection.prepare_cached(object_batch_sql(width)).await?;
                    let mut rows = statement.query(values).await?;
                    while let Some(row) = rows.next().await? {
                        let ordinal = row.get::<i64>(0)? as usize;
                        if ordinal < chunk.len() {
                            let encoded: Option<Vec<u8>> = row.get(1)?;
                            output[ordinal] = encoded
                                .map(|bytes| decode_stored_record(&chunk[ordinal], &bytes))
                                .transpose()?;
                        }
                    }
                }
                Ok(records)
            })
        })
        .await
    }

    async fn object_payload_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<(BlobId, u64)>>, MetadataError> {
        let keys = keys.to_vec();
        self.read(move |connection| {
            Box::pin(async move {
                let mut statement = connection
                    .prepare_cached(
                        "SELECT payload, payload_size FROM objects \
                         WHERE namespace = ?1 AND native_id = ?2",
                    )
                    .await?;
                let mut payloads = Vec::with_capacity(keys.len());
                for key in &keys {
                    let mut rows = statement
                        .query(params![key.namespace().as_str(), key.native_id()])
                        .await?;
                    payloads.push(match rows.next().await? {
                        Some(row) => Some(decode_payload_summary(key, row.get(0)?, row.get(1)?)?),
                        None => None,
                    });
                }
                Ok(payloads)
            })
        })
        .await
    }

    async fn validated_payload_batch(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<(BlobId, u64)>>, MetadataError> {
        let keys = keys.to_vec();
        self.read(move |connection| {
            Box::pin(async move {
                let mut statement = connection
                    .prepare_cached(
                        "SELECT payload, payload_size FROM objects \
                         WHERE namespace = ?1 AND native_id = ?2 AND validated != 0",
                    )
                    .await?;
                let mut payloads = Vec::with_capacity(keys.len());
                for key in &keys {
                    let mut rows = statement
                        .query(params![key.namespace().as_str(), key.native_id()])
                        .await?;
                    payloads.push(match rows.next().await? {
                        Some(row) => Some(decode_payload_summary(key, row.get(0)?, row.get(1)?)?),
                        None => None,
                    });
                }
                Ok(payloads)
            })
        })
        .await
    }

    async fn validated_closures(&self, keys: &[ObjectKey]) -> Result<Vec<bool>, MetadataError> {
        let keys = keys.to_vec();
        self.read(move |connection| {
            Box::pin(async move {
                let mut statement = connection
                    .prepare_cached(
                        "SELECT validated FROM objects \
                         WHERE namespace = ?1 AND native_id = ?2",
                    )
                    .await?;
                let mut known = Vec::with_capacity(keys.len());
                for key in &keys {
                    let mut rows = statement
                        .query(params![key.namespace().as_str(), key.native_id()])
                        .await?;
                    known.push(match rows.next().await? {
                        Some(row) => row.get::<i64>(0)? != 0,
                        None => false,
                    });
                }
                Ok(known)
            })
        })
        .await
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, MetadataError> {
        let name = name.clone();
        self.read(move |connection| {
            Box::pin(async move {
                let mut statement = connection
                    .prepare_cached("SELECT namespace, native_id FROM named_roots WHERE name = ?1")
                    .await?;
                let mut rows = statement.query([name.as_str()]).await?;
                rows.next()
                    .await?
                    .map(|row| decode_key(row.get(0)?, row.get(1)?))
                    .transpose()
            })
        })
        .await
    }

    fn objects(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let snapshot = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut after: Option<ObjectKey> = None;
            loop {
                let page_after = after.clone();
                let records: Vec<ObjectRecord> = snapshot
                    .read(move |connection| Box::pin(async move {
                        let mut rows = if let Some(after) = &page_after {
                            connection
                                .prepare_cached(
                                    "SELECT namespace, native_id, record FROM objects \
                                     WHERE namespace = ?1 AND native_id > ?2 \
                                     ORDER BY native_id LIMIT 256",
                                )
                                .await?
                                .query(params![after.namespace().as_str(), after.native_id()])
                                .await?
                        } else {
                            connection
                                .prepare_cached(
                                    "SELECT namespace, native_id, record FROM objects \
                                     ORDER BY namespace, native_id LIMIT 256",
                                )
                                .await?
                                .query(())
                                .await?
                        };
                        let mut records = Vec::new();
                        while let Some(row) = rows.next().await? {
                            let key = decode_key(row.get(0)?, row.get(1)?)?;
                            let stored: Vec<u8> = row.get(2)?;
                            records.push(decode_stored_record(&key, &stored)?);
                        }
                        // Both continuations seek into the compound index.
                        // Combining them with OR makes Turso sort all remaining
                        // candidates for every page. Exhaust the current
                        // namespace before advancing, preserving canonical order
                        // and including empty native IDs in later namespaces.
                        if records.is_empty() && let Some(after) = page_after {
                            let mut rows = connection
                                .prepare_cached(
                                    "SELECT namespace, native_id, record FROM objects \
                                     WHERE namespace > ?1 \
                                     ORDER BY namespace, native_id LIMIT 256",
                                )
                                .await?
                                .query([after.namespace().as_str()])
                                .await?;
                            while let Some(row) = rows.next().await? {
                                let key = decode_key(row.get(0)?, row.get(1)?)?;
                                let stored: Vec<u8> = row.get(2)?;
                                records.push(decode_stored_record(&key, &stored)?);
                            }
                        }
                        Ok(records)
                    }))
                    .await?;
                if records.is_empty() {
                    break;
                }
                after = records.last().map(|record| record.key().clone());
                // The ordered scan returns and decodes complete records in
                // the same bounded query. Fetching only keys and then issuing
                // one point query per row made large integrity scans and GC
                // pay tens of thousands of redundant SQL executions.
                for record in records {
                    yield record;
                }
            }
        })
    }

    fn objects_unordered(&self) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        self.objects_created_through(u64::MAX)
    }

    fn objects_created_through(
        &self,
        generation: u64,
    ) -> BoxStream<'static, Result<ObjectRecord, MetadataError>> {
        let snapshot = self.clone();
        let generation = i64::try_from(generation).unwrap_or(i64::MAX);
        Box::pin(async_stream::try_stream! {
            let mut after = 0i64;
            loop {
                let (next_after, records) = snapshot
                    .read(move |connection| Box::pin(async move {
                        let mut rows = connection
                            .prepare_cached(
                                "SELECT rowid, namespace, native_id, record FROM objects NOT INDEXED \
                                 WHERE rowid > ?1 AND created_generation <= ?2 \
                                 ORDER BY rowid LIMIT 256",
                            )
                            .await?
                            .query([after, generation])
                            .await?;
                        let mut next_after = after;
                        let mut records = Vec::new();
                        while let Some(row) = rows.next().await? {
                            next_after = row.get(0)?;
                            let key = decode_key(row.get(1)?, row.get(2)?)?;
                            let stored: Vec<u8> = row.get(3)?;
                            records.push(decode_stored_record(&key, &stored)?);
                        }
                        Ok((next_after, records))
                    }))
                    .await?;
                if records.is_empty() {
                    break;
                }
                after = next_after;
                for record in records {
                    yield record;
                }
            }
        })
    }

    fn roots(&self) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
        let snapshot = self.clone();
        Box::pin(async_stream::try_stream! {
            let mut after: Option<String> = None;
            loop {
                let page_after = after.clone();
                let roots = snapshot
                    .read(move |connection| Box::pin(async move {
                        let mut rows = if let Some(after) = page_after {
                            connection
                                .prepare_cached(
                                    "SELECT name, namespace, native_id FROM named_roots \
                                     WHERE name > ?1 ORDER BY name LIMIT 256",
                                )
                                .await?
                                .query([after])
                                .await?
                        } else {
                            connection
                                .prepare_cached(
                                    "SELECT name, namespace, native_id FROM named_roots \
                                     ORDER BY name LIMIT 256",
                                )
                                .await?
                                .query(())
                                .await?
                        };
                        let mut roots = Vec::new();
                        while let Some(row) = rows.next().await? {
                            let name: String = row.get(0)?;
                            let name = RootName::try_from(name).map_err(|error| {
                                MetadataError::Corruption(format!("invalid stored root name: {error}"))
                            })?;
                            roots.push(RootRecord::new(name, decode_key(row.get(1)?, row.get(2)?)?));
                        }
                        Ok(roots)
                    }))
                    .await?;
                if roots.is_empty() {
                    break;
                }
                after = roots.last().map(|root| root.name().as_str().to_owned());
                for root in roots {
                    yield root;
                }
            }
        })
    }

    fn roots_under(
        &self,
        prefix: &RootName,
    ) -> BoxStream<'static, Result<RootRecord, MetadataError>> {
        let snapshot = self.clone();
        let prefix = prefix.clone();
        Box::pin(async_stream::try_stream! {
            if let Some(target) = snapshot.root(&prefix).await? {
                yield RootRecord::new(prefix.clone(), target);
            }
            // Every descendant begins with a slash. The ASCII '0' is the
            // exclusive upper bound regardless of the root name's UTF-8 text.
            let lower = format!("{}/", prefix.as_str());
            let upper = format!("{}0", prefix.as_str());
            let mut after: Option<String> = None;
            loop {
                let start = after.clone().unwrap_or_else(|| lower.clone());
                let upper = upper.clone();
                let strict = after.is_some();
                let roots = snapshot
                    .read(move |connection| Box::pin(async move {
                        let statement = if strict {
                            "SELECT name, namespace, native_id FROM named_roots \
                             WHERE name > ?1 AND name < ?2 ORDER BY name LIMIT 256"
                        } else {
                            "SELECT name, namespace, native_id FROM named_roots \
                             WHERE name >= ?1 AND name < ?2 ORDER BY name LIMIT 256"
                        };
                        let mut rows = connection.prepare_cached(statement).await?.query([start, upper]).await?;
                        let mut roots = Vec::new();
                        while let Some(row) = rows.next().await? {
                            let name: String = row.get(0)?;
                            let name = RootName::try_from(name).map_err(|error| {
                                MetadataError::Corruption(format!("invalid stored root name: {error}"))
                            })?;
                            roots.push(RootRecord::new(name, decode_key(row.get(1)?, row.get(2)?)?));
                        }
                        Ok(roots)
                    }))
                    .await?;
                if roots.is_empty() {
                    break;
                }
                after = roots.last().map(|root| root.name().as_str().to_owned());
                for root in roots {
                    yield root;
                }
            }
        })
    }
}

/// Locally verified facts in the shared repository database, beside the
/// object records whose validated-closure mark they resemble.
struct TursoVerificationFacts {
    db: Arc<TursoDb>,
}

#[async_trait]
impl super::VerificationFacts for TursoVerificationFacts {
    fn scope(&self) -> Option<std::path::PathBuf> {
        Some(self.db.path().to_owned())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetadataError> {
        // A lookup never waits behind the serialized writer.
        let connection = self
            .db
            .begin_read_snapshot()
            .await
            .map_err(from_database_error)?;
        let mut rows = connection
            .query(
                "SELECT facts FROM verification_facts WHERE identity = ?1",
                [key.to_vec()],
            )
            .await?;
        Ok(match rows.next().await? {
            Some(row) => Some(row.get::<Vec<u8>>(0)?),
            None => None,
        })
    }

    async fn edit(&self, keys: Vec<Vec<u8>>, edit: super::FactsEdit) -> Result<(), MetadataError> {
        self.db
            .write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    let mut current = Vec::with_capacity(keys.len());
                    for key in &keys {
                        let mut rows = transaction
                            .query(
                                "SELECT facts FROM verification_facts WHERE identity = ?1",
                                [key.clone()],
                            )
                            .await?;
                        current.push(match rows.next().await? {
                            Some(row) => Some(row.get::<Vec<u8>>(0)?),
                            None => None,
                        });
                    }
                    for (key, value) in edit(current) {
                        match value {
                            Some(value) => {
                                transaction
                                    .execute(
                                        "INSERT OR REPLACE INTO verification_facts (identity, facts) VALUES (?1, ?2)",
                                        turso::params![key, value],
                                    )
                                    .await?;
                            }
                            None => {
                                transaction
                                    .execute(
                                        "DELETE FROM verification_facts WHERE identity = ?1",
                                        [key],
                                    )
                                    .await?;
                            }
                        }
                    }
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await
            .map_err(from_database_error)
    }

    async fn clear(&self, tombstone: Vec<u8>, generation: Vec<u8>) -> Result<(), MetadataError> {
        self.db
            .write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    let old = {
                        let mut rows = transaction.query(
                            "SELECT facts FROM verification_facts WHERE identity = ?1",
                            [generation.clone()],
                        ).await?;
                        match rows.next().await? {
                            Some(row) => Some(row.get::<Vec<u8>>(0)?),
                            None => None,
                        }
                    };
                    let next = super::next_facts_generation(old.as_deref())
                        .map_err(|error| crate::error::Error::Backend(Box::new(error)))?;
                    // Kept only to read back what the clear stores.
                    let checked_keys =
                        invariant::ENABLED.then(|| (tombstone.clone(), generation.clone()));
                    transaction
                        .execute(
                            "DELETE FROM verification_facts WHERE length(facts) > 0",
                            (),
                        )
                        .await?;
                    transaction
                        .execute(
                            "INSERT OR REPLACE INTO verification_facts (identity, facts) VALUES (?1, ?2)",
                            turso::params![tombstone, Vec::<u8>::new()],
                        )
                        .await?;
                    transaction.execute(
                        "INSERT OR REPLACE INTO verification_facts (identity, facts) VALUES (?1, ?2)",
                        turso::params![generation, next.to_le_bytes().to_vec()],
                    ).await?;
                    if let Some((tombstone, generation)) = checked_keys {
                        let next_generation =
                            read_fact_tx(&transaction, generation.clone()).await?;
                        let tombstone = read_fact_tx(&transaction, tombstone).await?;
                        // The generation row is the one non-empty row a clear keeps.
                        let facts_remain = transaction
                            .query(
                                "SELECT 1 FROM verification_facts \
                                 WHERE length(facts) > 0 AND identity != ?1 LIMIT 1",
                                [generation],
                            )
                            .await?
                            .next()
                            .await?
                            .is_some();
                        invariant::check(|| {
                            super::validate_facts_clear(
                                "turso.facts_clear",
                                &super::FactsClear {
                                    previous_generation: old.as_deref(),
                                    next_generation: next_generation.as_deref(),
                                    tombstone: tombstone.as_deref(),
                                    facts_remain,
                                },
                            )
                        })
                        .map_err(|violation| state_as_database(violation.into()))?;
                    }
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await
            .map_err(from_database_error)
    }

    async fn page(&self, after: Vec<u8>, limit: usize) -> Result<Vec<Vec<u8>>, MetadataError> {
        let connection = self
            .db
            .begin_read_snapshot()
            .await
            .map_err(from_database_error)?;
        let mut rows = connection
            .query(
                "SELECT identity FROM verification_facts \
                 WHERE identity > ?1 AND length(facts) > 0 ORDER BY identity LIMIT ?2",
                turso::params![after, limit as i64],
            )
            .await?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await? {
            keys.push(row.get::<Vec<u8>>(0)?);
        }
        Ok(keys)
    }
}

async fn read_fact_tx(
    transaction: &Transaction<'_>,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, turso::Error> {
    let mut rows = transaction
        .query(
            "SELECT facts FROM verification_facts WHERE identity = ?1",
            [key],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| row.get::<Vec<u8>>(0))
        .transpose()
}

#[async_trait]
impl MetadataStore for TursoMetadataStore {
    // Repository::get uses a short transaction; metadata_reader retains one across
    // calls. Both paths share read_snapshot_state and read_records so state
    // validation and record semantics stay aligned despite different lifetimes.
    async fn get_records(
        &self,
        keys: &[MetadataKey],
    ) -> Result<Vec<Option<bytes::Bytes>>, MetadataError> {
        // Avoid copying oversized requests, but preserve state-error precedence.
        let validation = super::records::validate_get(keys);
        let keys = if validation.is_ok() {
            keys.to_vec()
        } else {
            Vec::new()
        };
        self.db
            .read(move |connection| {
                Box::pin(async move {
                    Ok(async {
                        // Preserve snapshot-opening validation, even for empty requests.
                        read_snapshot_state(connection).await?;
                        validation?;
                        if keys.is_empty() {
                            return Ok(Vec::new());
                        }
                        read_records(connection, &keys).await
                    }
                    .await)
                })
            })
            .await
            .map_err(from_database_error)?
    }

    fn supports_metadata_records(&self) -> bool {
        true
    }
    fn supports_root_retention(&self) -> bool {
        true
    }
    fn verification_facts(&self) -> Option<Arc<dyn super::VerificationFacts>> {
        Some(Arc::new(TursoVerificationFacts {
            db: self.db.clone(),
        }))
    }
    fn commit_durability(&self) -> Option<crate::blob::CommitDurability> {
        self.commits.clone()
    }
    async fn commit_checked(
        &self,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        self.commit_impl(None, mutation).await
    }

    async fn pin_store(&self) -> Result<Arc<dyn super::PinStore>, MetadataError> {
        if let Some(pins) = self.pins.get() {
            return Ok(pins.clone());
        }
        let path = self
            .db
            .path()
            .canonicalize()
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let mut name = path
            .file_name()
            .expect("database has a filename")
            .to_os_string();
        name.push(".online-pins");
        let _ = self.pins.set(Arc::new(super::FilePinStore::new(
            path.with_file_name(name),
        )));
        Ok(self.pins.get().expect("pin store initialized").clone())
    }
    fn coordinates_payload_catalog(&self) -> bool {
        true
    }
    async fn try_collection_lease(&self) -> Result<Option<super::RepositoryLease>, MetadataError> {
        Ok(Some(super::RepositoryLease::process_local()))
    }

    #[tracing::instrument(
        name = "state.snapshot",
        level = "debug",
        skip_all,
        fields(backend = "turso")
    )]
    async fn snapshot(&self) -> Result<Arc<dyn MetadataSnapshot>, MetadataError> {
        let connection = self
            .db
            .begin_read_snapshot()
            .await
            .map_err(from_database_error)?;
        let snapshot = TursoSnapshot {
            revision: RepositoryRevision::from_bytes([0; 32]),
            generation: 0,
            payload_catalog: None,
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
        };
        let (revision, payload_catalog, generation) = snapshot
            .read(|connection| Box::pin(async move { read_snapshot_state(connection).await }))
            .await?;
        tracing::debug!(revision = %revision, "state snapshot opened");
        Ok(Arc::new(TursoSnapshot {
            revision,
            generation,
            payload_catalog,
            ..snapshot
        }))
    }

    #[tracing::instrument(
        name = "state.commit",
        skip_all,
        fields(
            backend = "turso",
            expected_revision = %expected,
            objects = mutation.objects.len(),
            root_changes = mutation.roots.len(),
            collection = mutation.retained_objects.is_some()
        )
    )]
    async fn commit(
        &self,
        expected: &RepositoryRevision,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        self.commit_impl(Some(*expected), mutation).await
    }

    #[tracing::instrument(name = "state.compact", skip_all, fields(backend = "turso"))]
    async fn compact_transient_state(&self) -> Result<(), MetadataError> {
        // Use the same writer gate as publication and collection. The owned
        // gate survives cancellation; SQLite's WAL locks coordinate other
        // handles/processes and prevent truncation past a live read snapshot.
        self.db
            .write(|connection| {
                Box::pin(async move { checkpoint_write_ahead_log(connection).await })
            })
            .await
            .map_err(from_database_error)
    }
}

/// Checkpoint pragmas return a result row (`busy`, `log`, `checkpointed`), so
/// they must be queried and consumed rather than sent through `execute_batch`.
async fn checkpoint_write_ahead_log(connection: &Connection) -> Result<(), DatabaseError> {
    // Maintenance must not spend the ordinary write retry budget waiting on
    // a reader owned by its caller. All callers run under db.write, which
    // completes this operation (including restoration) despite cancellation.
    connection.busy_timeout(std::time::Duration::ZERO)?;
    let result = checkpoint_status(connection).await;
    connection.busy_timeout(std::time::Duration::from_millis(
        crate::sqlite::BUSY_TIMEOUT_MS,
    ))?;
    result
}

async fn checkpoint_status(connection: &Connection) -> Result<(), DatabaseError> {
    let mut rows = connection
        .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
        .await?;
    let row = rows.next().await?.ok_or_else(|| {
        state_as_database(MetadataError::Corruption(
            "checkpoint returned no status".into(),
        ))
    })?;
    let busy: i64 = row.get(0)?;
    while rows.next().await?.is_some() {}
    if busy != 0 {
        return Err(state_as_database(MetadataError::Busy(
            "WAL checkpoint is blocked by active readers or writers".into(),
        )));
    }
    Ok(())
}

impl TursoMetadataStore {
    async fn commit_impl(
        &self,
        expected: Option<RepositoryRevision>,
        mutation: MetadataMutation,
    ) -> Result<CommitResult, MetadataError> {
        use crate::repository::CollectionPhase;

        let collecting = mutation.retained_objects.is_some();
        let writer_wait = collecting.then(|| CollectionPhase::new("prune_db_wait"));
        let validated = self.validated.clone();
        let result = self
            .db
            .write(move |connection| {
                drop(writer_wait);
                Box::pin(async move {
                    let begin = collecting.then(|| CollectionPhase::new("prune_db_begin"));
                    // Beginning the transaction also rolls back a previously
                    // abandoned transaction. prepare_cached alone does not
                    // perform that cleanup, so keep it inside this boundary.
                    let transaction = connection.transaction().await?;
                    drop(begin);
                    let prepare = collecting.then(|| CollectionPhase::new("prune_db_prepare"));
                    // The committed row, read again only to check the transition.
                    let previous_state = if invariant::ENABLED {
                        Some(
                            read_snapshot_state(&transaction)
                                .await
                                .map_err(state_as_database)?,
                        )
                    } else {
                        None
                    };
                    let actual = read_revision_tx(&transaction)
                        .await
                        .map_err(state_as_database)?;
                    if let Some(expected) = expected
                        && actual != expected
                    {
                        return Err(state_as_database(MetadataError::StaleRevision {
                            expected,
                            actual,
                        }));
                    }
                    check_metadata_tx(&transaction, &mutation.checks)
                        .await
                        .map_err(state_as_database)?;
                    if mutation.require_validated_roots {
                        require_validated_roots_tx(&transaction, mutation.root_changes())
                            .await
                            .map_err(state_as_database)?;
                    }
                    let generation = next_generation_tx(&transaction)
                        .await
                        .map_err(state_as_database)?;
                    mutation
                        .reject_mixed_collection()
                        .map_err(state_as_database)?;

                    let next_revision = fresh_revision(Some(actual)).map_err(state_as_database)?;
                    let root_policy_changes = mutation
                        .root_changes()
                        .map(|change| match change {
                            RootChange::Set { name, .. } => (name.clone(), true),
                            RootChange::Remove { name } => (name.clone(), false),
                        })
                        .collect::<Vec<_>>();
                    let mut result = CommitResult {
                        revision: next_revision,
                        objects_inserted: 0,
                        objects_removed: 0,
                        roots_changed: 0,
                    };
                    drop(prepare);
                    // Each addition's key and prior record, and a collection's
                    // object counts, kept only for the transition check.
                    let mut added = Vec::new();
                    let mut collected = None;
                    if let Some(retained) = mutation.retained_objects {
                        let _retained = CollectionPhase::new("prune_db_retained");
                        // Collection already scans every object, so counting
                        // them on both sides stays within its own cost.
                        let before = if invariant::ENABLED {
                            Some(
                                checked_object_count_tx(&transaction)
                                    .await
                                    .map_err(state_as_database)?,
                            )
                        } else {
                            None
                        };
                        result.objects_removed =
                            install_retained_tx(&transaction, retained.as_ref())
                                .await
                                .map_err(state_as_database)?;
                        if let Some(before) = before {
                            collected = Some(CollectedObjects {
                                retained: retained.len() as u64,
                                before,
                                after: checked_object_count_tx(&transaction)
                                    .await
                                    .map_err(state_as_database)?,
                                missing_root_target: missing_root_target_tx(&transaction)
                                    .await
                                    .map_err(state_as_database)?,
                            });
                        }
                    } else {
                        for verified in mutation.objects {
                            let record = verified.into_record();
                            let existing = read_record_tx(&transaction, record.key())
                                .await
                                .map_err(state_as_database)?;
                            if invariant::ENABLED {
                                added.push((record.key().clone(), existing.clone()));
                            }
                            match existing {
                                Some(existing) if existing == record => {}
                                Some(_) => {
                                    return Err(state_as_database(
                                        MetadataError::ImmutableConflict(record.key().clone()),
                                    ));
                                }
                                None => {
                                    insert_record_tx(&transaction, &record, generation)
                                        .await
                                        .map_err(state_as_database)?;
                                    result.objects_inserted += 1;
                                    #[cfg(test)]
                                    crate::blob::crash_tests::checkpoint("state-object-inserted");
                                }
                            }
                        }
                        let mut newly_named = Vec::new();
                        for change in mutation.roots.into_values() {
                            if let RootChange::Set { target, .. } = &change {
                                newly_named.push(target.clone());
                            }
                            result.roots_changed += apply_root_tx(&transaction, change)
                                .await
                                .map_err(state_as_database)?;
                            #[cfg(test)]
                            crate::blob::crash_tests::checkpoint("state-root-changed");
                        }
                        let newly_validated: BTreeSet<_> =
                            mutation.validated_closures.iter().cloned().collect();
                        if !mutation.require_validated_roots {
                            validate_root_closures_tx(
                                &transaction,
                                &newly_named,
                                &newly_validated,
                                &validated,
                            )
                            .await
                            .map_err(state_as_database)?;
                        }
                        if !newly_validated.is_empty() {
                            let mut remember = transaction
                                .prepare_cached(
                                    "UPDATE objects SET validated = 1 \
                                     WHERE namespace = ?1 AND native_id = ?2",
                                )
                                .await?;
                            for key in &newly_validated {
                                remember
                                    .execute(params![key.namespace().as_str(), key.native_id()])
                                    .await?;
                            }
                        }
                    }
                    let state = collecting.then(|| CollectionPhase::new("prune_db_state"));
                    apply_metadata_tx(&transaction, mutation.records)
                        .await
                        .map_err(state_as_database)?;
                    refresh_root_policies_tx(&transaction, root_policy_changes)
                        .await
                        .map_err(state_as_database)?;
                    let declared_catalog = if invariant::ENABLED {
                        mutation.payload_catalog.clone()
                    } else {
                        None
                    };
                    transaction
                        .prepare_cached(
                            "UPDATE repository_state SET revision = ?1, generation = ?3, \
                             payload_catalog_ref = COALESCE(?2, payload_catalog_ref) WHERE singleton = 1",
                        )
                        .await?
                        .execute(params![
                            next_revision.as_bytes().as_slice(),
                            mutation.payload_catalog,
                            generation
                        ])
                        .await?;
                    if let Some((previous_revision, previous_catalog, previous_generation)) =
                        &previous_state
                    {
                        // Read back what this transaction is about to commit,
                        // so the check covers the SQL rather than its inputs.
                        let (next_revision, next_catalog, next_generation) =
                            read_snapshot_state(&transaction)
                                .await
                                .map_err(state_as_database)?;
                        let mut stored = Vec::with_capacity(added.len());
                        for (key, _) in &added {
                            stored.push(
                                read_record_tx(&transaction, key)
                                    .await
                                    .map_err(state_as_database)?,
                            );
                        }
                        let objects = match collected {
                            Some(collected) => ObjectChange::Collected(collected),
                            None => ObjectChange::Added(
                                added
                                    .iter()
                                    .zip(&stored)
                                    .map(|((key, before), after)| AddedObject {
                                        key,
                                        before: before.as_ref(),
                                        after: after.as_ref(),
                                    })
                                    .collect(),
                            ),
                        };
                        invariant::check(|| {
                            validate_commit_transition(
                                "turso.commit",
                                &CommitState {
                                    revision: *previous_revision,
                                    generation: *previous_generation,
                                    payload_catalog: previous_catalog.as_deref(),
                                },
                                &CommitState {
                                    revision: next_revision,
                                    generation: next_generation,
                                    payload_catalog: next_catalog.as_deref(),
                                },
                                &CommitChange {
                                    payload_catalog: declared_catalog.as_deref(),
                                    objects,
                                    result: &result,
                                },
                            )
                        })
                        .map_err(|violation| state_as_database(violation.into()))?;
                    }
                    #[cfg(test)]
                    crate::blob::crash_tests::checkpoint("before-state-commit");
                    drop(state);
                    let commit = collecting.then(|| CollectionPhase::new("prune_db_commit"));
                    transaction.commit().await?;
                    drop(commit);
                    #[cfg(test)]
                    crate::blob::crash_tests::checkpoint("after-state-commit");
                    if collecting {
                        let _checkpoint = CollectionPhase::new("prune_db_checkpoint");
                        // Committed pages otherwise accumulate in the log for
                        // the life of the repository: the database file stays
                        // one page wide while the log grows with every commit,
                        // so every read resolves through an ever longer log.
                        // Collection is the operation whose whole purpose is to
                        // give space back, and it holds the repository
                        // exclusively, which is exactly what a truncating
                        // checkpoint needs. Never at the cost of the
                        // collection itself, though: this is maintenance, and
                        // a log that could not be folded back is not a failed
                        // collection.
                        let _ = checkpoint_write_ahead_log(connection).await;
                    }
                    Ok(result)
                })
            })
            .await
            .map_err(from_database_error);
        if matches!(result, Err(MetadataError::StorageFull)) {
            self.db.reset_writer().await.map_err(from_database_error)?;
        }
        if let Ok(committed) = &result {
            tracing::info!(
                revision = %committed.revision,
                objects_inserted = committed.objects_inserted,
                objects_removed = committed.objects_removed,
                roots_changed = committed.roots_changed,
                "state commit completed"
            );
        }
        result
    }
}

async fn read_record(
    connection: &Connection,
    key: &ObjectKey,
) -> Result<Option<ObjectRecord>, MetadataError> {
    let mut statement = connection
        .prepare_cached("SELECT record FROM objects WHERE namespace = ?1 AND native_id = ?2")
        .await?;
    let mut rows = statement
        .query(params![key.namespace().as_str(), key.native_id()])
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let record: Vec<u8> = row.get(0)?;
    decode_stored_record(key, &record).map(Some)
}

async fn read_record_tx(
    transaction: &Transaction<'_>,
    key: &ObjectKey,
) -> Result<Option<ObjectRecord>, MetadataError> {
    Ok(read_record_with_validation_tx(transaction, key)
        .await?
        .map(|(record, _)| record))
}

async fn read_record_with_validation_tx(
    transaction: &Transaction<'_>,
    key: &ObjectKey,
) -> Result<Option<(ObjectRecord, bool)>, MetadataError> {
    let mut statement = transaction
        .prepare_cached(
            "SELECT record, validated FROM objects \
             WHERE namespace = ?1 AND native_id = ?2",
        )
        .await?;
    let mut rows = statement
        .query(params![key.namespace().as_str(), key.native_id()])
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let stored: Vec<u8> = row.get(0)?;
    let validated = row.get::<i64>(1)? != 0;
    decode_stored_record(key, &stored).map(|record| Some((record, validated)))
}

fn encode_stored_record(record: &ObjectRecord) -> Result<Vec<u8>, MetadataError> {
    crate::compression::compress(&record.encode(), 1).map_err(|error| {
        MetadataError::Backend(format!("compressing state record failed: {error}"))
    })
}

fn decode_stored_record(key: &ObjectKey, stored: &[u8]) -> Result<ObjectRecord, MetadataError> {
    let encoded =
        crate::compression::decompress(stored, MAX_ENCODED_RECORD_BYTES).map_err(|error| {
            MetadataError::Corruption(format!(
                "invalid compressed state record for {key}: {error}"
            ))
        })?;
    let record = ObjectRecord::decode(&encoded).map_err(|error| {
        MetadataError::Corruption(format!("invalid stored object record {key}: {error}"))
    })?;
    if record.key() != key {
        return Err(MetadataError::Corruption(format!(
            "state row key {key} contains record for {}",
            record.key()
        )));
    }
    Ok(record)
}

fn decode_payload_summary(
    key: &ObjectKey,
    payload: Vec<u8>,
    size: Vec<u8>,
) -> Result<(BlobId, u64), MetadataError> {
    let payload = BlobId::new(Digest::try_from(payload.as_slice()).map_err(|error| {
        MetadataError::Corruption(format!("invalid payload digest for {key}: {error}"))
    })?);
    let size: [u8; 8] = size.try_into().map_err(|size: Vec<u8>| {
        MetadataError::Corruption(format!(
            "invalid payload-size width {} for {key}",
            size.len()
        ))
    })?;
    Ok((payload, u64::from_le_bytes(size)))
}

async fn insert_record_tx(
    transaction: &Transaction<'_>,
    record: &ObjectRecord,
    generation: i64,
) -> Result<(), MetadataError> {
    let encoded = encode_stored_record(record)?;
    let mut insert_object = transaction
        .prepare_cached(
            "INSERT INTO objects (namespace, native_id, payload, payload_size, record, created_generation) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .await?;
    insert_object
        .execute(params![
            record.key().namespace().as_str(),
            record.key().native_id(),
            record.payload().digest().as_bytes().as_slice(),
            record.payload_size().to_le_bytes().as_slice(),
            encoded,
            generation,
        ])
        .await?;
    Ok(())
}

async fn apply_root_tx(
    transaction: &Transaction<'_>,
    change: RootChange,
) -> Result<usize, MetadataError> {
    match change {
        RootChange::Set { name, target } => {
            if read_record_tx(transaction, &target).await?.is_none() {
                return Err(MetadataError::MissingObject {
                    missing: target,
                    from: None,
                });
            }
            let mut lookup = transaction
                .prepare_cached("SELECT namespace, native_id FROM named_roots WHERE name = ?1")
                .await?;
            let mut rows = lookup.query([name.as_str()]).await?;
            let existing = rows
                .next()
                .await?
                .map(|row| decode_key(row.get(0)?, row.get(1)?))
                .transpose()?;
            drop(rows);
            transaction
                .prepare_cached(
                    "INSERT OR REPLACE INTO named_roots (name, namespace, native_id) \
                     VALUES (?1, ?2, ?3)",
                )
                .await?
                .execute(params![
                    name.as_str(),
                    target.namespace().as_str(),
                    target.native_id()
                ])
                .await?;
            Ok(usize::from(existing.as_ref() != Some(&target)))
        }
        RootChange::Remove { name } => Ok(transaction
            .prepare_cached("DELETE FROM named_roots WHERE name = ?1")
            .await?
            .execute([name.as_str()])
            .await? as usize),
    }
}

/// Validate the closure of every name this commit points somewhere new.
///
/// Only [`RootChange::Set`] can leave a name resolving to an incomplete graph.
/// Records are immutable once inserted (a conflicting rewrite is rejected
/// above), objects are never deleted on this path (collection is a separate,
/// separately validated mutation), and unnaming cannot break another name's
/// closure. A name that survives this commit unchanged was therefore validated
/// when it was set and is still complete.
///
/// Walking every name instead would charge each mutation the cost of the
/// whole live repository, which is quadratic across the batched commits that
/// make up one import.
async fn validate_root_closures_tx(
    transaction: &Transaction<'_>,
    named: &[ObjectKey],
    newly_validated: &BTreeSet<ObjectKey>,
    validated: &ValidationCounter,
) -> Result<(), MetadataError> {
    let mut walk = super::closure::ClosureWalk::new(named, newly_validated);
    while let Some(key) = walk.next_key() {
        validated.record();
        let found = read_record_with_validation_tx(transaction, &key).await?;
        walk.visit(
            found
                .as_ref()
                .map(|(record, witnessed)| (record.links(), *witnessed)),
        )?;
    }
    Ok(())
}

async fn install_retained_tx(
    transaction: &Transaction<'_>,
    retained: &dyn RetainedObjects,
) -> Result<usize, MetadataError> {
    let before = object_count_tx(transaction).await?;
    if usize::try_from(before).is_ok_and(|count| count <= INLINE_COLLECTION_OBJECTS)
        && retained.len() <= INLINE_COLLECTION_OBJECTS
    {
        return install_retained_inline_tx(transaction, retained, before).await;
    }
    install_retained_streamed_tx(transaction, retained, before).await
}

async fn install_retained_streamed_tx(
    transaction: &Transaction<'_>,
    retained: &dyn RetainedObjects,
    before: i64,
) -> Result<usize, MetadataError> {
    transaction
        .execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS retained_objects (
                 namespace TEXT NOT NULL,
                 native_id BLOB NOT NULL,
                 PRIMARY KEY (namespace, native_id)
             );
             DELETE FROM retained_objects;",
        )
        .await?;
    let mut insert = transaction
        .prepare_cached("INSERT INTO retained_objects (namespace, native_id) VALUES (?1, ?2)")
        .await?;
    // The retained set arrives in canonical-order pages, so a marked graph
    // larger than memory reaches the scratch table without ever being one
    // allocation. Validation below stays pure SQL against that table.
    let mut after = None;
    loop {
        let page = retained.page(after.clone(), RETAINED_PAGE).await?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for key in &page {
            insert
                .execute(params![key.namespace().as_str(), key.native_id()])
                .await?;
        }
        after = Some(last);
    }

    let mut unknown = transaction
        .prepare_cached(
            "SELECT r.namespace, r.native_id FROM retained_objects r \
             LEFT JOIN objects o ON o.namespace = r.namespace AND o.native_id = r.native_id \
             WHERE o.namespace IS NULL LIMIT 1",
        )
        .await?
        .query(())
        .await?;
    if let Some(row) = unknown.next().await? {
        let key = decode_key(row.get(0)?, row.get(1)?)?;
        return Err(MetadataError::InvalidRetainedSet(format!(
            "unknown retained key {key}"
        )));
    }

    let mut omitted_root = transaction
        .prepare_cached(
            "SELECT n.name FROM named_roots n \
             LEFT JOIN retained_objects r \
             ON r.namespace = n.namespace AND r.native_id = n.native_id \
             WHERE r.namespace IS NULL LIMIT 1",
        )
        .await?
        .query(())
        .await?;
    if let Some(row) = omitted_root.next().await? {
        let name: String = row.get(0)?;
        return Err(MetadataError::InvalidRetainedSet(format!(
            "root {name} is not retained"
        )));
    }

    // Records keep their canonical links in one compressed value instead of
    // expanding every edge into a persistent SQL row. Build a temporary,
    // deduplicated target set only while validating collection. Deduplicating
    // one page in memory is important for versioned wide trees: thousands of
    // tree records often point at the same mostly-unchanged blob set.
    transaction
        .execute_batch(
            "CREATE TEMP TABLE IF NOT EXISTS referenced_objects (
                 target_namespace TEXT NOT NULL,
                 target_native_id BLOB NOT NULL,
                 source_namespace TEXT NOT NULL,
                 source_native_id BLOB NOT NULL,
                 PRIMARY KEY (target_namespace, target_native_id)
             );
             DELETE FROM referenced_objects;",
        )
        .await?;
    // Only bounded batches, not canonical order, are needed here. Paging by
    // the compound key's OR predicate makes Turso sort the remaining joined
    // records for every page. The physical rowid cursor seeks forward once;
    // this transaction does not delete objects until validation is complete.
    let mut after: Option<i64> = None;
    loop {
        let mut rows = if let Some(after) = after {
            transaction
                .prepare_cached(
                    "SELECT o.rowid, o.namespace, o.native_id, o.record
                       FROM objects o NOT INDEXED
                       JOIN retained_objects r
                         ON r.namespace = o.namespace AND r.native_id = o.native_id
                      WHERE o.rowid > ?1
                      ORDER BY o.rowid LIMIT 256",
                )
                .await?
                .query([after])
                .await?
        } else {
            transaction
                .prepare_cached(
                    "SELECT o.rowid, o.namespace, o.native_id, o.record
                       FROM objects o NOT INDEXED
                       JOIN retained_objects r
                         ON r.namespace = o.namespace AND r.native_id = o.native_id
                      ORDER BY o.rowid LIMIT 256",
                )
                .await?
                .query(())
                .await?
        };
        let mut page = Vec::new();
        while let Some(row) = rows.next().await? {
            after = Some(row.get(0)?);
            let source = decode_key(row.get(1)?, row.get(2)?)?;
            let stored: Vec<u8> = row.get(3)?;
            page.push((source, stored));
        }
        if page.is_empty() {
            break;
        }

        let mut insert_reference = transaction
            .prepare_cached(
                "INSERT OR IGNORE INTO referenced_objects
                 (target_namespace, target_native_id, source_namespace, source_native_id)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .await?;
        let mut targets = BTreeMap::<ObjectKey, ObjectKey>::new();
        for (source, stored) in page {
            let record = decode_stored_record(&source, &stored)?;
            for target in record.links() {
                targets
                    .entry(target.clone())
                    .or_insert_with(|| source.clone());
                if targets.len() >= COLLECTION_REFERENCE_BUFFER {
                    for (target, source) in std::mem::take(&mut targets) {
                        insert_reference
                            .execute(params![
                                target.namespace().as_str(),
                                target.native_id(),
                                source.namespace().as_str(),
                                source.native_id(),
                            ])
                            .await?;
                    }
                }
            }
        }
        for (target, source) in targets {
            insert_reference
                .execute(params![
                    target.namespace().as_str(),
                    target.native_id(),
                    source.namespace().as_str(),
                    source.native_id(),
                ])
                .await?;
        }
    }

    let mut omitted_link = transaction
        .prepare_cached(
            "SELECT l.source_namespace, l.source_native_id,
                    l.target_namespace, l.target_native_id
             FROM referenced_objects l
             LEFT JOIN retained_objects target \
               ON target.namespace = l.target_namespace \
              AND target.native_id = l.target_native_id \
             WHERE target.namespace IS NULL LIMIT 1",
        )
        .await?
        .query(())
        .await?;
    if let Some(row) = omitted_link.next().await? {
        let source = decode_key(row.get(0)?, row.get(1)?)?;
        let target = decode_key(row.get(2)?, row.get(3)?)?;
        return Err(MetadataError::InvalidRetainedSet(format!(
            "retained object {source} links to omitted {target}"
        )));
    }

    transaction
        .execute_batch(
            "DELETE FROM objects
               WHERE NOT EXISTS (
                 SELECT 1 FROM retained_objects r
                  WHERE r.namespace = objects.namespace
                    AND r.native_id = objects.native_id
               );
             DELETE FROM ingest_cache
               WHERE NOT EXISTS (
                 SELECT 1 FROM retained_objects r
                  WHERE r.namespace = 'casita.blob.v1'
                    AND r.native_id = ingest_cache.blob_digest
               );",
        )
        .await?;
    let after = object_count_tx(transaction).await?;
    usize::try_from(before - after)
        .map_err(|_| MetadataError::Backend("removed object count does not fit usize".to_owned()))
}

async fn install_retained_inline_tx(
    transaction: &Transaction<'_>,
    retained: &dyn RetainedObjects,
    before: i64,
) -> Result<usize, MetadataError> {
    let mut retained_keys = BTreeSet::new();
    let mut after = None;
    loop {
        let page = retained.page(after.clone(), RETAINED_PAGE).await?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for key in page {
            if !retained_keys.insert(key) {
                return Err(MetadataError::InvalidRetainedSet(
                    "retained source contains a duplicate key".to_owned(),
                ));
            }
        }
        after = Some(last);
    }
    if retained_keys.len() != retained.len() {
        return Err(MetadataError::InvalidRetainedSet(format!(
            "retained source declared {} keys but yielded {}",
            retained.len(),
            retained_keys.len()
        )));
    }

    let mut roots = transaction
        .prepare_cached("SELECT name, namespace, native_id FROM named_roots ORDER BY name")
        .await?
        .query(())
        .await?;
    while let Some(row) = roots.next().await? {
        let name: String = row.get(0)?;
        let target = decode_key(row.get(1)?, row.get(2)?)?;
        if !retained_keys.contains(&target) {
            return Err(MetadataError::InvalidRetainedSet(format!(
                "root {name} is not retained"
            )));
        }
    }
    drop(roots);

    let mut rows = transaction
        .prepare_cached("SELECT rowid, namespace, native_id, record FROM objects")
        .await?
        .query(())
        .await?;
    let mut unseen = retained_keys.clone();
    let mut stale = Vec::new();
    let mut live_blob_digests = BTreeSet::new();
    while let Some(row) = rows.next().await? {
        let key = decode_key(row.get(1)?, row.get(2)?)?;
        if !retained_keys.contains(&key) {
            // Physical IDs stay inside the transaction that scanned them;
            // all retained-set validation finishes before the first delete.
            stale.push(row.get::<i64>(0)?);
            continue;
        }
        unseen.remove(&key);
        if key.namespace().as_str() == "casita.blob.v1" {
            live_blob_digests.insert(key.native_id().to_vec());
        }
        let stored: Vec<u8> = row.get(3)?;
        let record = decode_stored_record(&key, &stored)?;
        if let Some(target) = record
            .links()
            .iter()
            .find(|target| !retained_keys.contains(*target))
        {
            return Err(MetadataError::InvalidRetainedSet(format!(
                "retained object {key} links to omitted {target}"
            )));
        }
    }
    drop(rows);
    if let Some(missing) = unseen.first() {
        return Err(MetadataError::InvalidRetainedSet(format!(
            "unknown retained key {missing}"
        )));
    }

    let mut delete_object = transaction
        .prepare_cached("DELETE FROM objects WHERE rowid = ?1")
        .await?;
    for rowid in &stale {
        delete_object.execute([*rowid]).await?;
    }

    let mut cache_rows = transaction
        .prepare_cached("SELECT blob_digest FROM ingest_cache")
        .await?
        .query(())
        .await?;
    let mut stale_cache = Vec::new();
    while let Some(row) = cache_rows.next().await? {
        let digest: Vec<u8> = row.get(0)?;
        if !live_blob_digests.contains(&digest) {
            stale_cache.push(digest);
        }
    }
    drop(cache_rows);
    let mut delete_cache = transaction
        .prepare_cached("DELETE FROM ingest_cache WHERE blob_digest = ?1")
        .await?;
    for digest in stale_cache {
        delete_cache.execute([digest]).await?;
    }

    let expected_after = before - i64::try_from(stale.len()).unwrap_or(i64::MAX);
    let after = object_count_tx(transaction).await?;
    if after != expected_after {
        return Err(MetadataError::Corruption(
            "inline collection deleted an unexpected number of objects".to_owned(),
        ));
    }
    Ok(stale.len())
}

async fn object_count_tx(transaction: &Transaction<'_>) -> Result<i64, MetadataError> {
    let mut rows = transaction
        .prepare_cached("SELECT COUNT(*) FROM objects")
        .await?
        .query(())
        .await?;
    Ok(rows
        .next()
        .await?
        .ok_or_else(|| MetadataError::Corruption("COUNT returned no row".to_owned()))?
        .get(0)?)
}

async fn checked_object_count_tx(transaction: &Transaction<'_>) -> Result<u64, MetadataError> {
    u64::try_from(object_count_tx(transaction).await?)
        .map_err(|_| MetadataError::Corruption("negative object count".to_owned()))
}

/// A root whose target has no object record, looked up per root through the
/// object primary key.
async fn missing_root_target_tx(
    transaction: &Transaction<'_>,
) -> Result<Option<ObjectKey>, MetadataError> {
    let mut rows = transaction
        .prepare_cached(
            "SELECT n.namespace, n.native_id FROM named_roots n \
             LEFT JOIN objects o ON o.namespace = n.namespace AND o.native_id = n.native_id \
             WHERE o.namespace IS NULL LIMIT 1",
        )
        .await?
        .query(())
        .await?;
    rows.next()
        .await?
        .map(|row| decode_key(row.get(0)?, row.get(1)?))
        .transpose()
}

async fn next_generation_tx(transaction: &Transaction<'_>) -> Result<i64, MetadataError> {
    let mut rows = transaction
        .prepare_cached("SELECT generation FROM repository_state WHERE singleton = 1")
        .await?
        .query(())
        .await?;
    let generation: i64 = rows
        .next()
        .await?
        .ok_or_else(|| MetadataError::Corruption("repository revision row is absent".into()))?
        .get(0)?;
    if generation < 0 {
        return Err(MetadataError::Corruption(
            "negative metadata generation".into(),
        ));
    }
    generation
        .checked_add(1)
        .ok_or_else(|| MetadataError::Backend("metadata generation exhausted".into()))
}

async fn read_revision_tx(
    transaction: &Transaction<'_>,
) -> Result<RepositoryRevision, MetadataError> {
    let mut statement = transaction
        .prepare_cached("SELECT revision FROM repository_state WHERE singleton = 1")
        .await?;
    let mut rows = statement.query(()).await?;
    let bytes: Vec<u8> = rows
        .next()
        .await?
        .ok_or_else(|| MetadataError::Corruption("repository revision row is absent".to_owned()))?
        .get(0)?;
    decode_revision(&bytes)
}

fn decode_revision(bytes: &[u8]) -> Result<RepositoryRevision, MetadataError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
        MetadataError::Corruption(format!(
            "repository revision has width {}, expected 32",
            bytes.len()
        ))
    })?;
    Ok(RepositoryRevision::from_bytes(bytes))
}

fn decode_key(namespace: String, native_id: Vec<u8>) -> Result<ObjectKey, MetadataError> {
    let namespace = namespace.parse().map_err(|error| {
        MetadataError::Corruption(format!("invalid stored namespace `{namespace}`: {error}"))
    })?;
    ObjectKey::new(namespace, native_id)
        .map_err(|error| MetadataError::Corruption(format!("invalid stored object key: {error}")))
}

fn state_as_database(error: MetadataError) -> DatabaseError {
    DatabaseError::Backend(Box::new(error))
}

fn from_database_error(error: DatabaseError) -> MetadataError {
    match error {
        DatabaseError::Backend(error) => match error.downcast::<MetadataError>() {
            Ok(error) => *error,
            Err(error) => match error.downcast::<turso::Error>() {
                Ok(error) => turso_state_error(*error),
                Err(error) => MetadataError::Backend(error.to_string()),
            },
        },
        DatabaseError::Io(error) if error.kind() == std::io::ErrorKind::StorageFull => {
            MetadataError::StorageFull
        }
        error => MetadataError::Backend(error.to_string()),
    }
}

impl From<turso::Error> for MetadataError {
    fn from(error: turso::Error) -> Self {
        turso_state_error(error)
    }
}

fn turso_state_error(error: turso::Error) -> MetadataError {
    match error {
        turso::Error::Busy(message) | turso::Error::BusySnapshot(message) => {
            MetadataError::Busy(message)
        }
        turso::Error::DatabaseFull(_) => MetadataError::StorageFull,
        turso::Error::IoError(std::io::ErrorKind::StorageFull, _) => MetadataError::StorageFull,
        error => MetadataError::Backend(error.to_string()),
    }
}

async fn check_metadata_tx(
    transaction: &Transaction<'_>,
    checks: &[MetadataCheck],
) -> Result<(), MetadataError> {
    if checks.is_empty() {
        return Ok(());
    }
    // Compare values in SQL, so large expected values do not require returning
    // and allocating the stored value merely to decide equality.
    let mut record = transaction
        .prepare_cached("SELECT value = ?3 FROM metadata_records WHERE namespace = ?1 AND key = ?2")
        .await?;
    let mut root = transaction
        .prepare_cached("SELECT namespace, native_id FROM named_roots WHERE name = ?1")
        .await?;
    for (index, check) in checks.iter().enumerate() {
        let matches = match check {
            MetadataCheck::Record { key, expected } => {
                let mut rows = record
                    .query(params![
                        key.namespace.as_str(),
                        key.key.as_ref(),
                        expected.as_ref().map(|v| v.to_vec())
                    ])
                    .await?;
                match rows.next().await? {
                    Some(row) => expected.is_some() && row.get::<i64>(0)? == 1,
                    None => expected.is_none(),
                }
            }
            MetadataCheck::Root { name, expected } => {
                let mut rows = root.query([name.as_str()]).await?;
                let actual = match rows.next().await? {
                    Some(row) => Some(decode_key(row.get(0)?, row.get(1)?)?),
                    None => None,
                };
                actual.as_ref() == expected.as_ref()
            }
        };
        if !matches {
            return Err(MetadataError::CheckFailed { index });
        }
    }
    Ok(())
}

async fn apply_metadata_tx(
    transaction: &Transaction<'_>,
    records: BTreeMap<MetadataKey, Option<bytes::Bytes>>,
) -> Result<(), MetadataError> {
    if records.is_empty() {
        return Ok(());
    }
    let mut set = transaction.prepare_cached(
        "INSERT INTO metadata_records (namespace, key, value) VALUES (?1, ?2, ?3) ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value"
    ).await?;
    let mut delete = transaction
        .prepare_cached("DELETE FROM metadata_records WHERE namespace = ?1 AND key = ?2")
        .await?;
    for (key, value) in records {
        match value {
            Some(value) => {
                set.execute(params![
                    key.namespace.as_str(),
                    key.key.as_ref(),
                    value.as_ref()
                ])
                .await?;
            }
            None => {
                delete
                    .execute(params![key.namespace.as_str(), key.key.as_ref()])
                    .await?;
            }
        }
        #[cfg(test)]
        crate::blob::crash_tests::checkpoint("state-metadata-changed");
    }
    Ok(())
}

async fn refresh_root_policies_tx(
    transaction: &Transaction<'_>,
    changes: Vec<(RootName, bool)>,
) -> Result<(), MetadataError> {
    for (name, set) in changes {
        let key = crate::repository::root_policy::policy_key(&name);
        if !set {
            transaction
                .prepare_cached("DELETE FROM metadata_records WHERE namespace = ?1 AND key = ?2")
                .await?
                .execute(params![key.namespace.as_str(), key.key.as_ref()])
                .await?;
            continue;
        }
        let mut statement = transaction
            .prepare_cached("SELECT value FROM metadata_records WHERE namespace = ?1 AND key = ?2")
            .await?;
        let mut rows = statement
            .query(params![key.namespace.as_str(), key.key.as_ref()])
            .await?;
        let value: Option<Vec<u8>> = rows.next().await?.map(|row| row.get(0)).transpose()?;
        drop(rows);
        if value
            .as_deref()
            .and_then(crate::repository::root_policy::last_use)
            .is_some()
        {
            let value = crate::repository::root_policy::policy_value();
            transaction
                .prepare_cached(
                    "UPDATE metadata_records SET value = ?3 WHERE namespace = ?1 AND key = ?2",
                )
                .await?
                .execute(params![
                    key.namespace.as_str(),
                    key.key.as_ref(),
                    value.as_ref()
                ])
                .await?;
        }
    }
    Ok(())
}

async fn require_validated_roots_tx<'a>(
    transaction: &Transaction<'_>,
    roots: impl Iterator<Item = &'a RootChange>,
) -> Result<(), MetadataError> {
    let mut lookup = transaction
        .prepare_cached("SELECT validated FROM objects WHERE namespace = ?1 AND native_id = ?2 AND EXISTS (SELECT 1 FROM named_roots WHERE namespace = ?1 AND native_id = ?2)")
        .await?;
    for change in roots {
        if let RootChange::Set { target, .. } = change {
            let mut rows = lookup
                .query(params![target.namespace().as_str(), target.native_id()])
                .await?;
            if !matches!(rows.next().await?, Some(row) if row.get::<i64>(0)? == 1) {
                return Err(MetadataError::RootVerificationRequired);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures::{StreamExt, TryStreamExt};

    use super::*;
    use crate::digest::{BlobId, Digest};
    use crate::format::{FormatLimits, FormatRegistry};
    use crate::{Directory, MetadataMutation, Node, PathComponent, PayloadReader};

    #[tokio::test]
    async fn current_reads_preserve_snapshot_state_corruption_errors() {
        for sql in [
            "DELETE FROM repository_state",
            "UPDATE repository_state SET revision = X'01'",
            "PRAGMA ignore_check_constraints = ON; UPDATE repository_state SET generation = -1",
            "UPDATE repository_state SET revision = 'invalid'",
            "UPDATE repository_state SET payload_catalog_ref = 'invalid'",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let store = TursoMetadataStore::open(directory.path().join("corrupt.sqlite"))
                .await
                .unwrap();
            store
                .db
                .write(move |connection| {
                    Box::pin(async move {
                        connection.execute_batch(sql).await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
            let expected = store
                .snapshot()
                .await
                .err()
                .expect("snapshot must reject corruption");
            let key = MetadataKey::new("test.records.v1".parse().unwrap(), b"missing".to_vec());
            for keys in [
                vec![],
                vec![key.clone()],
                vec![key; crate::metadata::records::MAX_BATCH + 1],
            ] {
                let actual = store.get_records(&keys).await.unwrap_err();
                assert_eq!(actual.to_string(), expected.to_string(), "{sql}");
                assert_eq!(
                    std::mem::discriminant(&actual),
                    std::mem::discriminant(&expected),
                    "{sql}"
                );
            }
        }
    }

    #[tokio::test]
    async fn object_batch_query_uses_composite_index() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("plans.sqlite"))
            .await
            .unwrap();
        store
            .db
            .write(|connection| {
                Box::pin(async move {
                    for width in [1, 2, 128, 256] {
                        let sql = format!("EXPLAIN QUERY PLAN {}", object_batch_sql(width));
                        let mut rows = connection
                            .query(sql, vec![turso::Value::Null; width * 2])
                            .await?;
                        let mut details = Vec::new();
                        while let Some(row) = rows.next().await? {
                            details.push(row.get::<String>(3)?);
                        }
                        println!("batch plan {width}: {details:?}");
                        assert!(
                            details
                                .iter()
                                .any(|detail| detail.contains("SEARCH objects USING INDEX")
                                    && detail.contains("namespace=? AND native_id=?")),
                            "{details:?}"
                        );
                        assert!(
                            !details.iter().any(|detail| detail.contains("SCAN objects")),
                            "{details:?}"
                        );
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn object_batches_preserve_order_duplicates_missing_keys_and_snapshots() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("batches.sqlite"))
            .await
            .unwrap();
        let mut expected = std::collections::BTreeMap::new();
        for namespace in ["test.z.v1", "test.a.v1", "test.aa.v1"] {
            for native in [vec![], vec![0], vec![0, 0], vec![1], vec![255]] {
                let key = ObjectKey::new(namespace.parse().unwrap(), native).unwrap();
                let record = ObjectRecord::new(
                    key.clone(),
                    BlobId::new(Digest::hash(&key.encode())),
                    0,
                    vec![],
                )
                .unwrap();
                expected.insert(key, record);
            }
        }
        let seed: Vec<_> = expected.values().cloned().collect();
        store
            .db
            .write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    for record in seed.iter().rev() {
                        insert_record_tx(&transaction, record, 0)
                            .await
                            .map_err(state_as_database)?;
                    }
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let added = blob(b"future batch record").await;
        let added_record = added.record().clone();
        let missing = added_record.key().clone();
        let old = store.snapshot().await.unwrap();
        assert!(old.object_batch(&[]).await.unwrap().is_empty());
        let mut keys: Vec<_> = expected
            .keys()
            .rev()
            .flat_map(|key| [key.clone(), missing.clone(), key.clone()])
            .cycle()
            .take(2051)
            .collect();
        for _ in 0..2 {
            let answers: Vec<_> = keys.iter().map(|key| expected.get(key).cloned()).collect();
            assert_eq!(old.object_batch(&keys).await.unwrap(), answers);
            for width in [1, 2, 3, 127, 128, 129, 255, 256, 257, 511, 512, 513] {
                assert_eq!(
                    old.object_batch(&keys[..width]).await.unwrap(),
                    answers[..width]
                );
            }
            keys.reverse();
        }

        let mut mutation = MetadataMutation::new();
        mutation.add_object(added);
        store.commit(&old.revision(), mutation).await.unwrap();
        assert_eq!(
            old.object_batch(std::slice::from_ref(&missing))
                .await
                .unwrap(),
            vec![None]
        );
        let current = store.snapshot().await.unwrap();
        assert_eq!(
            current
                .object_batch(&[missing.clone(), missing])
                .await
                .unwrap(),
            vec![Some(added_record.clone()), Some(added_record)]
        );
        assert!(current.object_batch(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn object_batches_reject_invalid_and_mismatched_stored_records() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("corrupt-batches.sqlite"))
            .await
            .unwrap();
        let first = blob(b"first batch record").await;
        let second = blob(b"second batch record").await;
        let first_key = first.record().key().clone();
        let second_key = second.record().key().clone();
        let other_stored = encode_stored_record(second.record()).unwrap();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(first);
        mutation.add_object(second);
        let revision = store.snapshot().await.unwrap().revision();
        store.commit(&revision, mutation).await.unwrap();
        for stored in [vec![255], other_stored] {
            let key = first_key.clone();
            store
                .db
                .write(move |connection| {
                    Box::pin(async move {
                        connection
                            .execute(
                                "UPDATE objects SET record = ?1 WHERE namespace = ?2 AND native_id = ?3",
                                params![stored, key.namespace().as_str(), key.native_id()],
                            )
                            .await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
            let snapshot = store.snapshot().await.unwrap();
            assert!(matches!(
                snapshot
                    .object_batch(&[second_key.clone(), first_key.clone(), first_key.clone()])
                    .await,
                Err(MetadataError::Corruption(_))
            ));
            let good = snapshot.object(&second_key).await.unwrap().unwrap();
            // Reuse the same cached query shape after the earlier batch exited
            // during decoding, with unread rows still in its result stream.
            assert_eq!(
                snapshot
                    .object_batch(&[second_key.clone(), second_key.clone(), second_key.clone()])
                    .await
                    .unwrap(),
                vec![Some(good); 3]
            );
        }
    }

    #[tokio::test]
    async fn turso_exact_collection_recovery_preserves_live_pins() {
        let directory = tempfile::tempdir().unwrap();
        let state = TursoMetadataStore::open(directory.path().join("recovery.sqlite"))
            .await
            .unwrap();
        super::super::tests::assert_exact_collection_recovery(state).await;
    }

    #[tokio::test]
    async fn turso_online_snapshot_excludes_future_garbage() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("holds.sqlite"))
            .await
            .unwrap();
        super::super::tests::assert_online_snapshot_excludes_future_garbage(store).await;
    }

    #[tokio::test]
    async fn turso_snapshot_generations_preserve_exact_births() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("births.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        super::super::tests::assert_snapshot_generations(&store).await;
        let snapshot = store.snapshot().await.unwrap();
        let generation = snapshot.generation().unwrap();
        let expected = snapshot
            .objects_created_through(generation)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        drop(snapshot);
        drop(store);
        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        let snapshot = reopened.snapshot().await.unwrap();
        assert_eq!(snapshot.generation().unwrap(), generation);
        assert_eq!(
            snapshot
                .objects_created_through(generation)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            expected
        );
        assert!(
            snapshot
                .objects_created_through(generation - 1)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }

    async fn blob(bytes: &[u8]) -> crate::VerifiedObject {
        let key = ObjectKey::blob(BlobId::new(Digest::hash(bytes)));
        let mut reader = Cursor::new(bytes.to_vec());
        FormatRegistry::builtin()
            .verify(
                &key,
                &mut reader as &mut dyn PayloadReader,
                &FormatLimits::default(),
            )
            .await
            .unwrap()
    }

    async fn directory_with_missing_child() -> crate::VerifiedObject {
        let missing = BlobId::new(Digest::from([0x7a; 32]));
        let directory = Directory::try_from_iter([(
            PathComponent::try_from("missing").unwrap(),
            Node::File {
                digest: missing,
                size: 4,
                executable: false,
            },
        )])
        .unwrap();
        verified_directory(directory).await
    }

    async fn verified_directory(directory: Directory) -> crate::VerifiedObject {
        let encoded = directory.encode();
        let key = ObjectKey::directory(directory.digest());
        let mut reader = Cursor::new(encoded);
        FormatRegistry::builtin()
            .verify(
                &key,
                &mut reader as &mut dyn PayloadReader,
                &FormatLimits::default(),
            )
            .await
            .unwrap()
    }

    // Exercise the large-repository algorithm with small, adversarial fixtures.
    // The end-to-end performance probe separately checks the actual cutoff.
    async fn collect_streamed(
        store: &TursoMetadataStore,
        retained: BTreeSet<ObjectKey>,
    ) -> Result<usize, MetadataError> {
        store
            .db
            .write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    let before = object_count_tx(&transaction)
                        .await
                        .map_err(state_as_database)?;
                    let removed = install_retained_streamed_tx(&transaction, &retained, before)
                        .await
                        .map_err(state_as_database)?;
                    transaction.commit().await?;
                    Ok(removed)
                })
            })
            .await
            .map_err(from_database_error)
    }

    #[tokio::test]
    async fn streamed_collection_pages_sparse_physical_rows_and_validates_late_links() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let mut mutation = MetadataMutation::new();
        let mut keys = Vec::new();
        for index in 0..600_u64 {
            let object = blob(&index.to_le_bytes()).await;
            keys.push(object.record().key().clone());
            mutation.add_object(object);
        }
        let parent = verified_directory(
            Directory::try_from_iter([(
                PathComponent::try_from("child").unwrap(),
                Node::File {
                    digest: BlobId::new(Digest::hash(&0_u64.to_le_bytes())),
                    size: 8,
                    executable: false,
                },
            )])
            .unwrap(),
        )
        .await;
        let parent_key = parent.record().key().clone();
        let root = RootName::try_from("live").unwrap();
        mutation
            .add_object(parent)
            .set_root(root.clone(), parent_key.clone());
        let revision = store.snapshot().await.unwrap().revision();
        let committed = store.commit(&revision, mutation).await.unwrap();
        // Physical IDs are not offsets or object-key order. Include negative
        // values and gaps so the first page cannot assume a cursor of zero.
        store
            .db
            .write(|connection| {
                Box::pin(async move {
                    connection
                        .execute("UPDATE objects SET rowid = -rowid * 3", ())
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let broken = directory_with_missing_child().await;
        let broken_key = broken.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(broken);
        store.commit(&committed.revision, mutation).await.unwrap();
        let before = store.snapshot().await.unwrap();
        let mut all: BTreeSet<_> = keys.iter().cloned().collect();
        all.extend([parent_key.clone(), broken_key.clone()]);
        // This directory is physically after more than two retained pages.
        // Stopping early must not silently approve its missing child.
        let error = collect_streamed(&store, all.clone()).await.unwrap_err();
        assert!(matches!(error, MetadataError::InvalidRetainedSet(message)
            if message.contains(&broken_key.to_string())));

        let mut retained: BTreeSet<_> = keys.iter().step_by(2).cloned().collect();
        retained.insert(parent_key.clone());
        assert_eq!(
            collect_streamed(&store, retained.clone()).await.unwrap(),
            all.len() - retained.len()
        );
        assert_eq!(
            before
                .objects()
                .map_ok(|r| r.key().clone())
                .try_collect::<BTreeSet<_>>()
                .await
                .unwrap(),
            all
        );
        drop(before);
        let snapshot = store.snapshot().await.unwrap();
        assert_eq!(
            snapshot.root(&root).await.unwrap(),
            Some(parent_key.clone())
        );
        assert_eq!(
            snapshot
                .objects()
                .map_ok(|r| r.key().clone())
                .try_collect::<BTreeSet<_>>()
                .await
                .unwrap(),
            retained
        );
        drop(snapshot);

        // Reuse the cached page statements and scratch tables with new keys.
        let mut smaller: BTreeSet<_> = keys.iter().step_by(4).cloned().collect();
        smaller.insert(parent_key);
        assert_eq!(
            collect_streamed(&store, smaller.clone()).await.unwrap(),
            retained.len() - smaller.len()
        );
        drop(store);
        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .await
                .unwrap()
                .objects()
                .map_ok(|r| r.key().clone())
                .try_collect::<BTreeSet<_>>()
                .await
                .unwrap(),
            smaller
        );
    }

    #[tokio::test]
    async fn streamed_collection_preserves_roots_and_rejects_unknown_keys() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("state.sqlite"))
            .await
            .unwrap();
        let live = blob(b"live").await;
        let live_key = live.record().key().clone();
        let orphan = blob(b"orphan").await;
        let orphan_key = orphan.record().key().clone();
        let root = RootName::try_from("live").unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_objects([live, orphan])
            .set_root(root.clone(), live_key.clone());
        let revision = store.snapshot().await.unwrap().revision();
        store.commit(&revision, mutation).await.unwrap();
        assert!(matches!(
            collect_streamed(&store, BTreeSet::from([orphan_key.clone()])).await,
            Err(MetadataError::InvalidRetainedSet(_))
        ));
        let unknown = blob(b"absent").await.record().key().clone();
        assert!(matches!(
            collect_streamed(&store, BTreeSet::from([live_key.clone(), unknown])).await,
            Err(MetadataError::InvalidRetainedSet(_))
        ));
        let snapshot = store.snapshot().await.unwrap();
        assert_eq!(snapshot.root(&root).await.unwrap(), Some(live_key.clone()));
        assert!(snapshot.object(&orphan_key).await.unwrap().is_some());
        drop(snapshot);
        assert_eq!(
            collect_streamed(&store, BTreeSet::from([live_key]))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn cached_statements_rebind_across_commits_and_collection() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let name = RootName::try_from("current").unwrap();
        let mut previous = None;
        for bytes in [b"first".as_slice(), b"second", b"third"] {
            let object = blob(bytes).await;
            let key = object.record().key().clone();
            let before = store.snapshot().await.unwrap();
            let mut mutation = MetadataMutation::new();
            mutation
                .add_object(object)
                .set_root(name.clone(), key.clone());
            let committed = store.commit(&before.revision(), mutation).await.unwrap();

            // A cached lookup must remain on its snapshot while the shared
            // writer reuses the same SQL with new parameter values.
            assert_eq!(before.root(&name).await.unwrap(), previous);
            assert!(before.object(&key).await.unwrap().is_none());
            let after = store.snapshot().await.unwrap();
            assert_eq!(after.root(&name).await.unwrap(), Some(key.clone()));
            assert!(after.object(&key).await.unwrap().is_some());
            drop(before);
            drop(after);

            let collected = store
                .commit(
                    &committed.revision,
                    MetadataMutation::install_retained_objects(BTreeSet::from([key.clone()])),
                )
                .await
                .unwrap();
            assert_eq!(collected.objects_removed, usize::from(previous.is_some()));
            let snapshot = store.snapshot().await.unwrap();
            assert_eq!(
                snapshot
                    .objects()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
                    .len(),
                1
            );
            if let Some(previous) = previous {
                assert!(snapshot.object(&previous).await.unwrap().is_none());
            }
            previous = Some(key);
        }
        drop(store);
        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        assert_eq!(
            reopened
                .snapshot()
                .await
                .unwrap()
                .root(&name)
                .await
                .unwrap(),
            previous
        );
    }

    #[tokio::test]
    async fn cached_statements_do_not_publish_an_abandoned_commit() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();
        let revision = store.snapshot().await.unwrap().revision();
        let broken = directory_with_missing_child().await;
        let broken_key = broken.record().key().clone();
        let name = RootName::try_from("current").unwrap();
        let mut failed = MetadataMutation::new();
        failed
            .add_object(broken)
            .set_root(name.clone(), broken_key.clone());
        assert!(matches!(
            store.commit(&revision, failed).await,
            Err(MetadataError::MissingObject { .. })
        ));

        // Reuse the writer immediately: no intervening uncached query may
        // accidentally provide the rollback that the next commit needs.
        let good = blob(b"committed").await;
        let good_key = good.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(good)
            .set_root(name.clone(), good_key.clone());
        store.commit(&revision, mutation).await.unwrap();
        let snapshot = store.snapshot().await.unwrap();
        assert!(snapshot.object(&broken_key).await.unwrap().is_none());
        assert_eq!(snapshot.root(&name).await.unwrap(), Some(good_key));
        assert_eq!(
            snapshot
                .objects()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn validated_payload_projection_preserves_order_duplicates_and_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("projection.sqlite"))
            .await
            .unwrap();
        let valid = blob(b"valid").await;
        let unvalidated = blob(b"not validated").await;
        let valid_key = valid.record().key().clone();
        let other_key = unvalidated.record().key().clone();
        let expected = (valid.record().payload(), valid.record().payload_size());
        let old = store.snapshot().await.unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_objects([valid, unvalidated])
            .mark_validated_closures([valid_key.clone()]);
        store.commit(&old.revision(), mutation).await.unwrap();
        let missing = ObjectKey::blob(BlobId::new(crate::Digest::hash(b"missing")));
        let keys = [other_key, valid_key.clone(), missing, valid_key];
        assert_eq!(
            old.validated_payload_batch(&keys).await.unwrap(),
            vec![None; 4]
        );
        assert_eq!(
            store
                .snapshot()
                .await
                .unwrap()
                .validated_payload_batch(&keys)
                .await
                .unwrap(),
            vec![None, Some(expected), None, Some(expected)]
        );
    }

    /// One mutation must cost the graph it names, not the graph already in
    /// the repository. Validating every name on every commit made an import's
    /// batched commits quadratic in repository size: adding one small object to
    /// a repository holding a large named graph re-walked the whole thing.
    #[tokio::test]
    async fn commit_validates_only_the_closures_it_names() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();

        // A named graph big enough that re-walking it would be unmistakable.
        let mut resident = Vec::new();
        let mut mutation = MetadataMutation::new();
        for index in 0..32u8 {
            let object = blob(&[index; 64]).await;
            resident.push(object.record().key().clone());
            mutation.add_object(object);
        }
        let anchor = blob(b"anchor").await;
        let anchor_key = anchor.record().key().clone();
        mutation.add_object(anchor);
        mutation.set_root(
            RootName::try_from("bench/resident").unwrap(),
            anchor_key.clone(),
        );
        let revision = store.snapshot().await.unwrap().revision();
        store.commit(&revision, mutation).await.unwrap();
        store.validated.take();

        // Publishing without naming anything cannot break any closure.
        let revision = store.snapshot().await.unwrap().revision();
        let mut unrooted = MetadataMutation::new();
        unrooted.add_object(blob(b"unrooted addition").await);
        store.commit(&revision, unrooted).await.unwrap();
        assert_eq!(store.validated.take(), 0);

        // Naming a new object walks that object alone, not the 33 already
        // named ones.
        let revision = store.snapshot().await.unwrap().revision();
        let named = blob(b"newly named").await;
        let named_key = named.record().key().clone();
        let mut rooted = MetadataMutation::new();
        rooted
            .add_object(named)
            .set_root(RootName::try_from("bench/named").unwrap(), named_key);
        store.commit(&revision, rooted).await.unwrap();
        assert_eq!(store.validated.take(), 1);
    }

    /// A repository validation witness replaces the state backend's duplicate
    /// graph walk, both in the mutation that records it and in later root
    /// changes. The target record itself is still read on every root change.
    #[tokio::test]
    async fn validated_witness_stops_root_validation_at_the_target() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();

        let mut objects = Vec::new();
        let mut entries = Vec::new();
        for index in 0..32u8 {
            let object = blob(&[index; 64]).await;
            let digest = BlobId::new(object.record().key().native_digest().unwrap());
            entries.push((
                PathComponent::try_from(format!("file-{index}").as_str()).unwrap(),
                Node::File {
                    digest,
                    size: 64,
                    executable: false,
                },
            ));
            objects.push(object);
        }
        let tree = Directory::try_from_iter(entries).unwrap();
        let encoded = tree.encode();
        let tree_key = ObjectKey::directory(tree.digest());
        let mut reader = Cursor::new(encoded);
        let tree_object = FormatRegistry::builtin()
            .verify(
                &tree_key,
                &mut reader as &mut dyn PayloadReader,
                &FormatLimits::default(),
            )
            .await
            .unwrap();
        objects.push(tree_object);

        let revision = store.snapshot().await.unwrap().revision();
        let mut publish = MetadataMutation::new();
        publish
            .add_objects(objects)
            .mark_validated_closures([tree_key.clone()])
            .set_root(
                RootName::try_from("bench/witnessed").unwrap(),
                tree_key.clone(),
            );
        store.commit(&revision, publish).await.unwrap();
        assert_eq!(store.validated.take(), 1);

        let revision = store.snapshot().await.unwrap().revision();
        let mut rename = MetadataMutation::new();
        rename.set_root(
            RootName::try_from("bench/witnessed-again").unwrap(),
            tree_key,
        );
        store.commit(&revision, rename).await.unwrap();
        assert_eq!(store.validated.take(), 1);
    }

    #[tokio::test]
    async fn a_witness_never_hides_a_missing_root_target() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();
        let missing = ObjectKey::blob(BlobId::new(Digest::from([0x91; 32])));
        let mut mutation = MetadataMutation::new();
        mutation
            .mark_validated_closures([missing.clone()])
            .set_root(RootName::try_from("bench/missing-proof").unwrap(), missing);
        let revision = store.snapshot().await.unwrap().revision();
        assert!(matches!(
            store.commit(&revision, mutation).await,
            Err(MetadataError::MissingObject { .. })
        ));
    }

    /// Scoping validation must not let an incomplete graph acquire a name.
    #[tokio::test]
    async fn commit_still_rejects_naming_an_incomplete_closure() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();
        let object = directory_with_missing_child().await;
        let key = object.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(object)
            .set_root(RootName::try_from("bench/broken").unwrap(), key);
        let revision = store.snapshot().await.unwrap().revision();
        assert!(matches!(
            store.commit(&revision, mutation).await,
            Err(MetadataError::MissingObject { .. })
        ));
    }

    #[tokio::test]
    async fn commit_reopen_and_snapshot_isolation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let before = store.snapshot().await.unwrap();
        let object = blob(b"persistent").await;
        let key = object.record().key().clone();
        let name = RootName::try_from("profiles/persistent").unwrap();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(object)
            .set_root(name.clone(), key.clone())
            .set_payload_catalog(b"first catalog".to_vec());
        store.commit(&before.revision(), mutation).await.unwrap();

        assert!(before.root(&name).await.unwrap().is_none());
        assert_eq!(before.payload_catalog(), None);
        drop(store);

        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        let after = reopened.snapshot().await.unwrap();
        assert_eq!(after.root(&name).await.unwrap(), Some(key.clone()));
        assert!(after.object(&key).await.unwrap().is_some());
        assert_eq!(after.objects().collect::<Vec<_>>().await.len(), 1);
        assert_eq!(after.roots().collect::<Vec<_>>().await.len(), 1);
        assert_eq!(after.payload_catalog(), Some(b"first catalog".as_slice()));

        let mut stale = MetadataMutation::new();
        stale.set_payload_catalog(b"stale catalog".to_vec());
        assert!(matches!(
            reopened.commit(&before.revision(), stale).await,
            Err(MetadataError::StaleRevision { .. })
        ));
        reopened
            .commit(&after.revision(), MetadataMutation::new())
            .await
            .unwrap();
        let unchanged = reopened.snapshot().await.unwrap();
        assert_eq!(unchanged.payload_catalog(), after.payload_catalog());
        let mut replacement = MetadataMutation::new();
        replacement.set_payload_catalog(b"next catalog".to_vec());
        reopened
            .commit(&unchanged.revision(), replacement)
            .await
            .unwrap();
        assert_eq!(after.payload_catalog(), Some(b"first catalog".as_slice()));
        assert_eq!(
            reopened.snapshot().await.unwrap().payload_catalog(),
            Some(b"next catalog".as_slice())
        );
    }

    #[tokio::test]
    #[ignore = "performance probe; run through benchmark run metadata-batch"]
    async fn benchmark_object_batch() {
        use std::time::Instant;

        let count: usize = std::env::var("CASITA_BATCH_COUNT")
            .unwrap()
            .parse()
            .unwrap();
        let iterations: usize = std::env::var("CASITA_BATCH_ITERATIONS")
            .unwrap()
            .parse()
            .unwrap();
        let requests: usize = std::env::var("CASITA_BATCH_REQUESTS")
            .unwrap()
            .parse()
            .unwrap();
        let widths: Vec<usize> = std::env::var("CASITA_BATCH_WIDTHS")
            .unwrap()
            .split(',')
            .map(|width| width.parse().unwrap())
            .collect();
        assert!(count > 0 && requests > 0 && iterations > 0);
        assert!(widths.iter().all(|width| *width > 0));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let mut expected = BTreeMap::new();
        for index in 0..count {
            let key = ObjectKey::new(
                format!("test.batch{}.v1", index % 3).parse().unwrap(),
                (index as u64).to_be_bytes().to_vec(),
            )
            .unwrap();
            let record = ObjectRecord::new(
                key.clone(),
                BlobId::new(Digest::hash(&key.encode())),
                index as u64,
                vec![],
            )
            .unwrap();
            expected.insert(key, record);
        }
        let seed: Vec<_> = expected.values().cloned().collect();
        store
            .db
            .write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    for record in seed.iter().rev() {
                        insert_record_tx(&transaction, record, 0)
                            .await
                            .map_err(state_as_database)?;
                    }
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let revision = store.snapshot().await.unwrap().revision();
        drop(store);
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let snapshot = TursoSnapshot {
            revision,
            generation: 0,
            payload_catalog: None,
            connection: Arc::new(tokio::sync::Mutex::new(
                store.db.begin_read_snapshot().await.unwrap(),
            )),
        };
        assert_eq!(
            snapshot.objects().try_collect::<Vec<_>>().await.unwrap(),
            expected.values().cloned().collect::<Vec<_>>()
        );
        let all_keys: Vec<_> = expected.keys().cloned().collect();
        let missing = blob(b"batch benchmark future object").await;
        let missing_key = missing.record().key().clone();
        let mut samples = Vec::new();
        for pattern in ["hits", "mixed"] {
            let mut random = 0xca517au64;
            let mut keys: Vec<ObjectKey> = Vec::new();
            for index in 0..requests {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let key = if pattern == "mixed" && index % 5 == 0 {
                    missing_key.clone()
                } else if pattern == "mixed" && index % 7 == 0 && index > 0 {
                    keys[index - 1].clone()
                } else {
                    all_keys[random as usize % count].clone()
                };
                keys.push(key);
            }
            let answers: Vec<_> = keys.iter().map(|key| expected.get(key).cloned()).collect();
            for &width in &widths {
                for iteration in 0..=iterations {
                    // Alternate both paths on the same read transaction and fixture.
                    // The reference reproduces a0a8b89: one task/lock per API
                    // batch, with a cached single-key query for every key.
                    for reference in if iteration % 2 == 0 {
                        [true, false]
                    } else {
                        [false, true]
                    } {
                        let started = Instant::now();
                        let mut actual = Vec::with_capacity(keys.len());
                        for chunk in keys.chunks(width) {
                            let batch = if reference {
                                let owned = chunk.to_vec();
                                snapshot
                                    .read(move |connection| {
                                        Box::pin(async move {
                                            let mut records = Vec::with_capacity(owned.len());
                                            for key in &owned {
                                                records.push(read_record(connection, key).await?);
                                            }
                                            Ok(records)
                                        })
                                    })
                                    .await
                                    .unwrap()
                            } else {
                                snapshot.object_batch(chunk).await.unwrap()
                            };
                            actual.extend(batch);
                        }
                        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap();
                        assert_eq!(actual, answers);
                        assert_eq!(snapshot.revision(), revision);
                        samples.push(serde_json::json!({
                            "pattern": pattern, "width": width, "iteration": iteration,
                            "warm": iteration > 0, "nanos": nanos,
                            "variant": if reference { "point-reference" } else { "current" }
                        }));
                    }
                }
            }
        }
        let mut mutation = MetadataMutation::new();
        mutation.add_object(missing);
        store.commit(&revision, mutation).await.unwrap();
        assert_eq!(
            snapshot
                .object_batch(std::slice::from_ref(&missing_key))
                .await
                .unwrap(),
            vec![None]
        );
        drop(snapshot);
        assert!(
            store
                .snapshot()
                .await
                .unwrap()
                .object(&missing_key)
                .await
                .unwrap()
                .is_some()
        );
        println!(
            "batch_sample {}",
            serde_json::json!({
                "count": count, "iterations": iterations, "requests": requests, "widths": widths,
                "samples": samples,
                "correctness": "exact ordered records, duplicates, misses and snapshot isolation"
            })
        );
    }

    #[tokio::test]
    #[ignore = "performance probe; run through benchmark run metadata-scan"]
    async fn benchmark_ordered_scan() {
        use std::time::Instant;

        let counts = std::env::var("CASITA_COLLECTION_COUNTS")
            .unwrap_or_else(|_| "256,257,8192,65536,65537".into());
        let iterations: usize = std::env::var("CASITA_COLLECTION_ITERATIONS")
            .unwrap_or_else(|_| "3".into())
            .parse()
            .unwrap();
        assert!(iterations > 0);
        for count in counts
            .split(',')
            .map(|value| value.parse::<usize>().unwrap())
        {
            assert!(count > 0);
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("metadata.sqlite");
            let store = TursoMetadataStore::open(&path).await.unwrap();
            let mut revision = store.snapshot().await.unwrap().revision();
            let mut expected = Vec::new();
            for first in (0..count).step_by(1024) {
                let mut mutation = MetadataMutation::new();
                for index in first..(first + 1024).min(count) {
                    let object = blob(&(index as u64).to_le_bytes()).await;
                    expected.push(object.record().clone());
                    mutation.add_object(object);
                }
                revision = store.commit(&revision, mutation).await.unwrap().revision;
            }
            expected.sort_by(|a, b| a.key().cmp(b.key()));
            drop(store);
            let reopened = TursoMetadataStore::open(&path).await.unwrap();
            let snapshot = reopened.snapshot().await.unwrap();
            assert_eq!(snapshot.revision(), revision);
            let mut samples = Vec::new();
            for iteration in 0..=iterations {
                let started = Instant::now();
                let actual = snapshot.objects().try_collect::<Vec<_>>().await.unwrap();
                let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap();
                assert_eq!(actual, expected);
                assert_eq!(snapshot.revision(), revision);
                samples.push(serde_json::json!({
                    "iteration": iteration, "warm": iteration > 0, "nanos": nanos
                }));
            }
            drop(snapshot);
            // Retain query plans alongside real API timings, including the old
            // OR and tuple alternatives and both implemented range searches.
            // Use the existing writer for EXPLAIN after releasing the measured
            // snapshot; diagnostics need no additional read transaction.
            let midpoint = expected[count / 2].key().clone();
            let plans = reopened.db.write(move |connection| Box::pin(async move {
            let mut plans = Vec::new();
            for (name, predicate, order) in [
                (
                    "or-continuation",
                    "namespace > ?1 OR (namespace = ?1 AND native_id > ?2)",
                    "namespace, native_id",
                ),
                (
                    "tuple",
                    "(namespace, native_id) > (?1, ?2)",
                    "namespace, native_id",
                ),
                (
                    "namespace-range",
                    "namespace = ?1 AND native_id > ?2",
                    "native_id",
                ),
                (
                    "namespace-after",
                    "namespace > ?1",
                    "namespace, native_id",
                ),
            ] {
                let sql = format!(
                    "EXPLAIN QUERY PLAN SELECT namespace, native_id, record FROM objects \
                     WHERE {predicate} ORDER BY {order} LIMIT 256"
                );
                let mut rows = if name == "namespace-after" {
                    connection.query(&sql, params![midpoint.namespace().as_str()]).await.unwrap()
                } else {
                    connection.query(&sql, params![midpoint.namespace().as_str(), midpoint.native_id()]).await.unwrap()
                };
                let mut details = Vec::new();
                while let Some(row) = rows.next().await.unwrap() {
                    details.push(row.get::<String>(3).unwrap());
                }
                plans.push(serde_json::json!({"name": name, "details": details}));
            }
            Ok(plans)
            })).await.unwrap();
            println!(
                "scan_sample {}",
                serde_json::json!({
                    "count": count, "iterations": iterations, "samples": samples,
                    "plans": plans,
                    "correctness": "exact ordered records and snapshot revision"
                })
            );
        }
    }

    #[tokio::test]
    async fn ordered_inventory_seeks_across_namespaces_and_preserves_snapshots() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        assert!(
            store
                .snapshot()
                .await
                .unwrap()
                .objects()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );

        // Structural fixtures cover IDs that built-in digest formats cannot
        // produce: empty bytes, NULs, prefixes, and non-UTF-8 bytes. Namespace
        // sizes put transitions before, on, and after a 256-row page boundary.
        let mut expected = Vec::new();
        for (namespace, count) in [
            ("test.a.v1", 255),
            ("test.aa.v1", 257),
            ("test.b.v1", 1),
            ("test.c.v1", 512),
            ("test.z.v1", 1),
        ] {
            for index in 0u16..count {
                let id = match index {
                    0 => vec![],
                    1 => vec![0],
                    2 => vec![0, 0],
                    3 => vec![255],
                    _ => [vec![1], index.to_be_bytes().to_vec()].concat(),
                };
                let key = ObjectKey::new(namespace.parse().unwrap(), id).unwrap();
                expected.push(
                    ObjectRecord::new(
                        key,
                        BlobId::new(Digest::hash(&index.to_le_bytes())),
                        u64::from(index),
                        vec![],
                    )
                    .unwrap(),
                );
            }
        }
        expected.sort_by(|a, b| a.key().cmp(b.key()));
        let seed = expected.clone();
        store
            .db
            .write(move |connection| {
                Box::pin(async move {
                    let transaction = connection.transaction().await?;
                    for record in seed.iter().rev() {
                        insert_record_tx(&transaction, record, 0)
                            .await
                            .map_err(state_as_database)?;
                    }
                    transaction
                        .execute("UPDATE objects SET rowid = -rowid * 3", ())
                        .await?;
                    transaction.commit().await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let old = store.snapshot().await.unwrap();
        let old_revision = old.revision();
        let mut stream = old.objects();
        let mut observed = Vec::new();
        for _ in 0..257 {
            observed.push(stream.try_next().await.unwrap().unwrap());
        }
        let retained_records: Vec<_> = expected.iter().step_by(3).cloned().collect();
        let retained: BTreeSet<_> = retained_records.iter().map(|r| r.key().clone()).collect();
        let committed = store
            .commit(
                &old_revision,
                MetadataMutation::install_retained_source(Arc::new(retained)),
            )
            .await
            .unwrap();
        assert_eq!(
            committed.objects_removed,
            expected.len() - retained_records.len()
        );
        let added = blob(b"new object before test namespaces").await;
        let added_record = added.record().clone();
        let mut mutation = MetadataMutation::new();
        mutation.add_object(added);
        let committed = store.commit(&committed.revision, mutation).await.unwrap();

        observed.extend(stream.try_collect::<Vec<_>>().await.unwrap());
        assert_eq!(observed, expected);
        assert_eq!(old.revision(), old_revision);
        assert_eq!(
            old.objects().try_collect::<Vec<_>>().await.unwrap(),
            expected
        );
        drop(old);

        let mut current = retained_records;
        current.push(added_record);
        current.sort_by(|a, b| a.key().cmp(b.key()));
        assert_eq!(
            store
                .snapshot()
                .await
                .unwrap()
                .objects()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            current
        );
        drop(store);
        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        let snapshot = reopened.snapshot().await.unwrap();
        assert_eq!(snapshot.revision(), committed.revision);
        assert_eq!(
            snapshot.objects().try_collect::<Vec<_>>().await.unwrap(),
            current
        );
    }

    #[tokio::test]
    async fn snapshot_inventory_streams_across_bounded_pages() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();
        let initial = store.snapshot().await.unwrap();
        let mut mutation = MetadataMutation::new();
        let mut expected_keys = Vec::new();
        for index in 0u16..300 {
            let object = blob(&index.to_le_bytes()).await;
            let key = object.record().key().clone();
            mutation.add_object(object).set_root(
                RootName::try_from(format!("inventory/{index:03}")).unwrap(),
                key.clone(),
            );
            expected_keys.push(key);
        }
        store.commit(&initial.revision(), mutation).await.unwrap();

        expected_keys.sort();
        let snapshot = store.snapshot().await.unwrap();
        let objects = snapshot.objects().try_collect::<Vec<_>>().await.unwrap();
        let unordered = snapshot
            .objects_unordered()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let roots = snapshot.roots().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            objects
                .iter()
                .map(|record| record.key().clone())
                .collect::<Vec<_>>(),
            expected_keys
        );
        assert_eq!(
            unordered
                .iter()
                .map(|record| record.key().clone())
                .collect::<BTreeSet<_>>(),
            expected_keys.iter().cloned().collect()
        );
        assert_eq!(roots.len(), 300);
        assert!(roots.windows(2).all(|pair| pair[0].name() < pair[1].name()));
    }

    #[tokio::test]
    async fn inline_collection_preserves_composite_keys_and_rejects_unknown_retained() {
        for garbage in [0, 1, 2, 3, 63, 64, 65, 127, 128, 129] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("collection.sqlite");
            let store = TursoMetadataStore::open(&path).await.unwrap();
            let mut expected = BTreeMap::new();
            let mut seed = Vec::new();
            for index in 0..garbage.max(1) {
                // Include empty and zero-containing native IDs, and identical
                // native IDs in live/dead namespaces. Physical row IDs must
                // not remove an unrelated live record.
                let native = if index == 0 {
                    vec![]
                } else {
                    (index as u64).to_le_bytes().to_vec()
                };
                for namespace in ["test.live.v1", "test.dead.v1"] {
                    if namespace == "test.dead.v1" && index >= garbage {
                        continue;
                    }
                    let key = ObjectKey::new(namespace.parse().unwrap(), native.clone()).unwrap();
                    let record = ObjectRecord::new(
                        key.clone(),
                        BlobId::new(Digest::hash(&key.encode())),
                        0,
                        vec![],
                    )
                    .unwrap();
                    if namespace == "test.live.v1" {
                        expected.insert(key, record.clone());
                    }
                    seed.push(record);
                }
            }
            let original = seed.clone();
            store
                .db
                .write(move |connection| {
                    Box::pin(async move {
                        let transaction = connection.transaction().await?;
                        for record in seed {
                            insert_record_tx(&transaction, &record, 0)
                                .await
                                .map_err(state_as_database)?;
                        }
                        transaction.commit().await?;
                        Ok(())
                    })
                })
                .await
                .unwrap();
            let revision = store.snapshot().await.unwrap().revision();
            let retained: BTreeSet<_> = expected.keys().cloned().collect();
            let mut invalid = retained.clone();
            invalid.insert(ObjectKey::new("test.missing.v1".parse().unwrap(), vec![]).unwrap());
            assert!(matches!(
                store
                    .commit(
                        &revision,
                        MetadataMutation::install_retained_objects(invalid)
                    )
                    .await,
                Err(MetadataError::InvalidRetainedSet(_))
            ));
            let snapshot = store.snapshot().await.unwrap();
            assert_eq!(snapshot.revision(), revision);
            let actual = snapshot
                .objects_unordered()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(actual.len(), original.len());
            for record in &original {
                assert_eq!(
                    snapshot.object(record.key()).await.unwrap().as_ref(),
                    Some(record)
                );
            }
            drop(snapshot);
            let committed = store
                .commit(
                    &revision,
                    MetadataMutation::install_retained_objects(retained),
                )
                .await
                .unwrap();
            assert_eq!(committed.objects_removed, garbage);
            drop(store);
            let reopened = TursoMetadataStore::open(&path).await.unwrap();
            let snapshot = reopened.snapshot().await.unwrap();
            assert_eq!(snapshot.revision(), committed.revision);
            let actual: BTreeMap<_, _> = snapshot
                .objects_unordered()
                .map_ok(|record| (record.key().clone(), record))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn stale_commit_and_atomic_collection() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();
        let initial = store.snapshot().await.unwrap();
        let live = blob(b"live").await;
        let live_key = live.record().key().clone();
        let orphan = blob(b"orphan").await;
        let orphan_key = orphan.record().key().clone();
        let mut publish = MetadataMutation::new();
        publish
            .add_objects([live, orphan])
            .set_root(RootName::try_from("live").unwrap(), live_key.clone());
        let committed = store.commit(&initial.revision(), publish).await.unwrap();
        assert!(matches!(
            store
                .commit(&initial.revision(), MetadataMutation::new())
                .await,
            Err(MetadataError::StaleRevision { .. })
        ));

        let retained = BTreeSet::from([live_key.clone()]);
        let outcome = store
            .commit(
                &committed.revision,
                MetadataMutation::install_retained_objects(retained),
            )
            .await
            .unwrap();
        assert_eq!(outcome.objects_removed, 1);
        let after = store.snapshot().await.unwrap();
        assert!(after.object(&live_key).await.unwrap().is_some());
        assert!(after.object(&orphan_key).await.unwrap().is_none());
    }

    /// Committed pages accumulate in the write-ahead log, and nothing else
    /// folds them back, so a long-lived repository would read through a log
    /// that only ever grows. Collection is the operation that exists to give
    /// space back and the one that holds the repository exclusively, so it is
    /// where the log is folded into the database.
    #[tokio::test]
    async fn collection_folds_the_write_ahead_log_back() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let log = directory.path().join("casita.sqlite-wal");
        let store = TursoMetadataStore::open(&path).await.unwrap();

        let mut publish = MetadataMutation::new();
        let live = blob(b"live through collection").await;
        let live_key = live.record().key().clone();
        publish
            .add_object(live)
            .set_root(RootName::try_from("live").unwrap(), live_key.clone());
        for index in 0..64u8 {
            publish.add_object(blob(&[index; 512]).await);
        }
        let revision = store.snapshot().await.unwrap().revision();
        let committed = store.commit(&revision, publish).await.unwrap();
        let logged = std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0);
        assert!(logged > 0, "the mutation should have written a log");

        // A truncating checkpoint needs the repository to itself, which is what
        // collection already holds; a live snapshot here would keep the log.
        store
            .commit(
                &committed.revision,
                MetadataMutation::install_retained_objects(BTreeSet::from([live_key.clone()])),
            )
            .await
            .unwrap();

        assert_eq!(
            std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0),
            0
        );
        let after = store.snapshot().await.unwrap();
        assert!(after.object(&live_key).await.unwrap().is_some());
    }

    /// Large bounded operations can explicitly compact their commit history
    /// without changing logical state or running a collection traversal.
    #[tokio::test]
    async fn transient_state_compaction_folds_the_write_ahead_log_back() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let log = directory.path().join("casita.sqlite-wal");
        let store = TursoMetadataStore::open(&path).await.unwrap();

        let mut expected = store.snapshot().await.unwrap().revision();
        let mut last_key = None;
        for index in 0..8u8 {
            let object = blob(&[index; 512]).await;
            last_key = Some(object.record().key().clone());
            let mut mutation = MetadataMutation::new();
            mutation.add_object(object);
            expected = store.commit(&expected, mutation).await.unwrap().revision;
        }
        assert!(
            std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0) > 0,
            "bounded commits should have written a log"
        );

        store.compact_transient_state().await.unwrap();

        assert_eq!(
            std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0),
            0
        );
        assert!(
            store
                .snapshot()
                .await
                .unwrap()
                .object(last_key.as_ref().unwrap())
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Collection forgets what the ingest cache recorded about content it
    /// removed. Without this the table would grow for the life of the
    /// repository, holding a row for every file ever imported.
    #[tokio::test]
    async fn collection_forgets_ingested_files_whose_content_is_gone() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();

        let live = blob(b"still referenced").await;
        let live_key = live.record().key().clone();
        let live_digest = live.record().payload().digest().as_bytes().to_vec();
        let orphan = blob(b"about to be collected").await;
        let orphan_digest = orphan.record().payload().digest().as_bytes().to_vec();

        let mut publish = MetadataMutation::new();
        publish
            .add_objects([live, orphan])
            .set_root(RootName::try_from("live").unwrap(), live_key.clone());
        let revision = store.snapshot().await.unwrap().revision();
        let committed = store.commit(&revision, publish).await.unwrap();

        // Two files were imported, one holding each blob's content.
        let ingested = vec![(1i64, live_digest.clone()), (2i64, orphan_digest)];
        store
            .db
            .write(move |connection| {
                Box::pin(async move {
                    for (inode, digest) in &ingested {
                        connection
                            .execute(
                                "INSERT INTO ingest_cache (device, inode, size, mtime_sec, \
                                 mtime_nsec, ctime_sec, ctime_nsec, blob_digest) \
                                 VALUES (7, ?1, 0, 0, 0, 0, 0, ?2)",
                                params![*inode, digest.as_slice()],
                            )
                            .await?;
                    }
                    Ok::<_, DatabaseError>(())
                })
            })
            .await
            .unwrap();

        store
            .commit(
                &committed.revision,
                MetadataMutation::install_retained_objects(BTreeSet::from([live_key])),
            )
            .await
            .unwrap();

        let remaining = store
            .db
            .write(|connection| {
                Box::pin(async move {
                    let mut rows = connection
                        .query("SELECT blob_digest FROM ingest_cache ORDER BY inode", ())
                        .await?;
                    let mut found: Vec<Vec<u8>> = Vec::new();
                    while let Some(row) = rows.next().await? {
                        found.push(row.get(0)?);
                    }
                    Ok::<_, DatabaseError>(found)
                })
            })
            .await
            .unwrap();
        assert_eq!(remaining, vec![live_digest]);
    }

    #[tokio::test]
    async fn rooted_forward_closure_must_exist_in_the_same_commit() {
        let directory = tempfile::tempdir().unwrap();
        let store = TursoMetadataStore::open(directory.path().join("casita.sqlite"))
            .await
            .unwrap();
        let initial = store.snapshot().await.unwrap();
        let parent = directory_with_missing_child().await;
        let parent_key = parent.record().key().clone();
        let mut mutation = MetadataMutation::new();
        mutation
            .add_object(parent)
            .set_root(RootName::try_from("broken").unwrap(), parent_key);
        assert!(matches!(
            store.commit(&initial.revision(), mutation).await,
            Err(MetadataError::MissingObject { from: Some(_), .. })
        ));
        assert_eq!(
            store.snapshot().await.unwrap().revision(),
            initial.revision()
        );
    }

    /// `from_db` decides whether to create the initial revision in a read
    /// snapshot and inserts it in a separate write transaction, so a second
    /// first opener's insert hits the singleton key instead of reusing the row.
    #[tokio::test]
    #[ignore = "F-34: concurrent first opens race on the initial repository_state row"]
    async fn concurrent_first_opens_of_one_database_both_succeed() {
        let directory = tempfile::tempdir().unwrap();
        let db = crate::sqlite::TursoDb::open(directory.path().join("race.sqlite")).unwrap();
        let (left, right) = tokio::join!(
            TursoMetadataStore::from_db(db.clone()),
            TursoMetadataStore::from_db(db.clone())
        );
        let (left, right) = (left.unwrap(), right.unwrap());
        assert_eq!(
            left.snapshot().await.unwrap().revision(),
            right.snapshot().await.unwrap().revision()
        );
    }
}
