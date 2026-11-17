//! Preserve WAL3's codec, offset, checksum, body and corruption contracts.
#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, BinaryArray, UInt64Array},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::ArrowWriter,
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    };
    use std::sync::Arc;
    use wal3::{Error, LogPosition, Setsum, interfaces::checksum_parquet};

    const BODIES: [&[u8]; 2] = [
        b"first immutable WAL record",
        b"second immutable WAL record",
    ];
    const EPOCH: u64 = 12345;
    const BASE: u64 = 41;

    fn fixture(relative: bool, codec: Compression) -> (Vec<u8>, Setsum) {
        let offsets = if relative {
            vec![0, 1]
        } else {
            vec![BASE, BASE + 1]
        };
        let mut checksum = Setsum::default();
        for (&offset, body) in offsets.iter().zip(BODIES) {
            checksum.insert_vectored(&[&offset.to_be_bytes(), &EPOCH.to_be_bytes(), body]);
        }
        let batch = RecordBatch::try_from_iter(vec![
            (
                if relative {
                    "relative_offset"
                } else {
                    "offset"
                },
                Arc::new(UInt64Array::from(offsets)) as ArrayRef,
            ),
            (
                "timestamp_us",
                Arc::new(UInt64Array::from(vec![EPOCH; 2])) as ArrayRef,
            ),
            (
                "body",
                Arc::new(BinaryArray::from(BODIES.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap();
        let properties = WriterProperties::builder().set_compression(codec).build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        (bytes, checksum)
    }

    fn verify(relative: bool, codec: Compression) {
        let (bytes, expected) = fixture(relative, codec);
        let start = relative.then_some(LogPosition::from_offset(BASE));
        let (checksum, records, actual_relative, epoch) =
            checksum_parquet(&bytes, true, start).unwrap();
        assert_eq!(checksum, expected);
        assert_eq!(actual_relative, relative);
        assert_eq!(epoch, EPOCH);
        assert_eq!(
            records,
            vec![
                (LogPosition::from_offset(BASE), BODIES[0].to_vec()),
                (LogPosition::from_offset(BASE + 1), BODIES[1].to_vec())
            ]
        );
        let skipped = checksum_parquet(&bytes, false, start).unwrap();
        assert_eq!(skipped.1, records);
        assert_eq!(skipped.2, relative);
        assert!(matches!(
            checksum_parquet(&bytes[..bytes.len() - 8], true, start),
            Err(Error::CorruptFragment(_))
        ));
        assert!(
            checksum_parquet(
                &bytes,
                true,
                if relative {
                    None
                } else {
                    Some(LogPosition::from_offset(BASE))
                }
            )
            .is_err()
        );
    }

    #[test]
    fn snappy_absolute_offsets() {
        verify(false, Compression::SNAPPY);
    }
    #[test]
    fn snappy_relative_offsets() {
        verify(true, Compression::SNAPPY);
    }
    #[test]
    fn zstd_absolute_offsets() {
        verify(false, Compression::ZSTD(ZstdLevel::default()));
    }
    #[test]
    fn zstd_relative_offsets() {
        verify(true, Compression::ZSTD(ZstdLevel::default()));
    }

    #[test]
    fn corrupt_zstd_page_preserves_corruption_classification() {
        let (mut bytes, _) = fixture(true, Compression::ZSTD(ZstdLevel::default()));
        let frame = bytes
            .windows(4)
            .position(|w| w == [0x28, 0xb5, 0x2f, 0xfd])
            .expect("the fixture must contain a compressed zstd page");
        bytes[frame] ^= 0xff;
        // The footer and metadata remain valid; the failure must occur when
        // the compressed page is decoded, rather than parsing the container.
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::copy_from_slice(&bytes),
        )
        .unwrap();
        assert!(matches!(
            checksum_parquet(&bytes, true, Some(LogPosition::from_offset(BASE))),
            Err(Error::CorruptFragment(_))
        ));
    }
}
