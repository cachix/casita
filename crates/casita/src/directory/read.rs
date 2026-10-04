//! Bounded loading of canonical directory payloads selected by a stored record.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::directory::Directory;
use crate::metadata::MetadataError;
use crate::object::{DIRECTORY_NAMESPACE, ObjectKey, ObjectRecord};
use crate::repository::RepositoryError;
use crate::{BlobId, Digest};

/// Read a complete canonical directory for `key`, checking the record's key,
/// declared length and both logical and physical identities. Reject bytes above
/// `limit`. The caller must keep the record and payload protected from collection
/// and choose any required streaming authentication when opening `reader`.
pub async fn read_directory_payload<R: AsyncRead + Unpin + ?Sized>(
    key: &ObjectKey,
    record: &ObjectRecord,
    reader: &mut R,
    limit: u64,
) -> Result<Directory, RepositoryError> {
    if key.namespace().as_str() != DIRECTORY_NAMESPACE || key.native_digest().is_none() {
        return Err(RepositoryError::InvalidInput(format!(
            "directory loading requires a digest-width {DIRECTORY_NAMESPACE} key, got {key}"
        )));
    }
    if record.key() != key {
        return Err(MetadataError::Corruption(format!(
            "directory record {} does not match requested {key}",
            record.key()
        ))
        .into());
    }
    let oversized =
        || RepositoryError::LimitExceeded(format!("directory payload exceeds {limit} bytes"));
    if record.payload_size() > limit {
        return Err(oversized());
    }
    let mut encoded = Vec::new();
    // Keep scratch space off the enclosing future's stack and do not append
    // the overflow probe to the materialized payload.
    let mut buffer = vec![0; 8192];
    loop {
        let remaining = limit - encoded.len() as u64;
        let width = remaining.saturating_add(1).min(buffer.len() as u64) as usize;
        let read = reader.read(&mut buffer[..width]).await?;
        if read == 0 {
            break;
        }
        if read as u64 > remaining {
            return Err(oversized());
        }
        encoded.extend_from_slice(&buffer[..read]);
    }
    if encoded.len() as u64 != record.payload_size() {
        return Err(MetadataError::Corruption(format!(
            "object {key} declares {} bytes but {} were read",
            record.payload_size(),
            encoded.len()
        ))
        .into());
    }
    let directory = Directory::decode(&encoded).map_err(|error| {
        RepositoryError::Format(crate::format::FormatError::InvalidPayload {
            namespace: key.namespace().clone(),
            message: error.to_string(),
        })
    })?;
    let observed = Digest::hash(&encoded);
    if key.native_digest() != Some(observed) || record.payload() != BlobId::new(observed) {
        return Err(MetadataError::Corruption(format!(
            "directory payload for {key} does not match its record"
        ))
        .into());
    }
    Ok(directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Node, ObjectKey, PathComponent, SymlinkTarget};
    use std::io::Cursor;

    fn record(bytes: &[u8]) -> ObjectRecord {
        let digest = Digest::hash(bytes);
        ObjectRecord::new(
            ObjectKey::directory(crate::DirectoryId::new(digest)),
            BlobId::new(digest),
            bytes.len() as u64,
            Vec::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn reads_complete_directories_at_the_limit() {
        let large = Directory::try_from_iter((0..200).map(|i| {
            (
                PathComponent::try_from(format!("entry-{i:03}")).unwrap(),
                Node::Symlink {
                    target: SymlinkTarget::try_from("x".repeat(100)).unwrap(),
                },
            )
        }))
        .unwrap();
        assert!(large.encode().len() > 8192);
        for directory in [Directory::new(), large] {
            let bytes = directory.encode();
            let mut input = Cursor::new(&bytes);
            assert_eq!(
                read_directory_payload(
                    record(&bytes).key(),
                    &record(&bytes),
                    &mut input,
                    bytes.len() as u64
                )
                .await
                .unwrap(),
                directory
            );
            assert_eq!(input.position(), bytes.len() as u64);
        }
    }

    #[tokio::test]
    async fn rejects_declared_overflow_before_reading() {
        let bytes = Directory::new().encode();
        let mut input = Cursor::new(&bytes);
        assert!(matches!(
            read_directory_payload(
                record(&bytes).key(),
                &record(&bytes),
                &mut input,
                bytes.len() as u64 - 1
            )
            .await,
            Err(RepositoryError::LimitExceeded(_))
        ));
        assert_eq!(input.position(), 0);
    }

    #[tokio::test]
    async fn probes_only_one_byte_beyond_the_limit() {
        let bytes = Directory::new().encode();
        let mut input = Cursor::new(vec![0; 100]);
        assert!(matches!(
            read_directory_payload(
                record(&bytes).key(),
                &record(&bytes),
                &mut input,
                bytes.len() as u64
            )
            .await,
            Err(RepositoryError::LimitExceeded(_))
        ));
        assert_eq!(input.position(), bytes.len() as u64 + 1);
    }

    #[tokio::test]
    async fn rejects_short_and_long_payloads_against_declared_length() {
        let bytes = Directory::new().encode();
        for length in [bytes.len() - 1, bytes.len() + 1] {
            let mut input = Cursor::new(vec![0; length]);
            assert!(matches!(
                read_directory_payload(record(&bytes).key(), &record(&bytes), &mut input, 100)
                    .await,
                Err(RepositoryError::Metadata(MetadataError::Corruption(_)))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_malformed_canonical_encodings() {
        for bytes in [vec![0; 9], u64::MAX.to_le_bytes().to_vec()] {
            let error = read_directory_payload(
                record(&bytes).key(),
                &record(&bytes),
                &mut bytes.as_slice(),
                100,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                RepositoryError::Format(crate::format::FormatError::InvalidPayload { .. })
            ));
        }
    }

    #[tokio::test]
    async fn checks_both_logical_and_physical_identity() {
        let bytes = Directory::new().encode();
        let valid = record(&bytes);
        let other = Digest::hash(b"other");
        let records = [
            ObjectRecord::new(
                ObjectKey::directory(crate::DirectoryId::new(other)),
                valid.payload(),
                valid.payload_size(),
                Vec::new(),
            )
            .unwrap(),
            ObjectRecord::new(
                valid.key().clone(),
                BlobId::new(other),
                valid.payload_size(),
                Vec::new(),
            )
            .unwrap(),
        ];
        for record in records {
            assert!(matches!(
                read_directory_payload(record.key(), &record, &mut bytes.as_slice(), 100).await,
                Err(RepositoryError::Metadata(MetadataError::Corruption(_)))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_non_directory_keys() {
        let bytes = Directory::new().encode();
        let record = ObjectRecord::new(
            ObjectKey::blob(record(&bytes).payload()),
            record(&bytes).payload(),
            bytes.len() as u64,
            Vec::new(),
        )
        .unwrap();
        let mut input = Cursor::new(&bytes);
        assert!(matches!(
            read_directory_payload(record.key(), &record, &mut input, 100).await,
            Err(RepositoryError::InvalidInput(_))
        ));
        assert_eq!(input.position(), 0);
    }

    #[tokio::test]
    async fn rejects_records_for_another_key_before_reading() {
        let bytes = Directory::new().encode();
        let key = ObjectKey::directory(crate::DirectoryId::new(Digest::hash(b"another")));
        let mut input = Cursor::new(&bytes);
        assert!(matches!(
            read_directory_payload(&key, &record(&bytes), &mut input, 100).await,
            Err(RepositoryError::Metadata(MetadataError::Corruption(_)))
        ));
        assert_eq!(input.position(), 0);
    }
}
