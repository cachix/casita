use arrow::{
    array::{ArrayRef, BinaryArray, UInt64Array},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{path::Path, sync::Arc};
const BASE: u64 = 987;
const EPOCH: u64 = 123456;
fn main() {
    let dir = std::env::args().nth(1).expect("output directory");
    let dir = Path::new(&dir);
    std::fs::create_dir_all(dir).unwrap();
    let bodies = vec![
        b"immutable WAL record".to_vec(),
        vec![],
        vec![0xff, 0, 0x80],
        (0..262_144).map(|i| (i % 251) as u8).collect(),
    ];
    for relative in [false, true] {
        for zstd in [false, true] {
            let offsets = (0..bodies.len() as u64)
                .map(|i| i + if relative { 0 } else { BASE })
                .collect::<Vec<_>>();
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
                    Arc::new(UInt64Array::from(vec![EPOCH; bodies.len()])) as ArrayRef,
                ),
                (
                    "body",
                    Arc::new(BinaryArray::from_iter_values(bodies.iter())) as ArrayRef,
                ),
            ])
            .unwrap();
            let codec = if zstd {
                Compression::ZSTD(ZstdLevel::default())
            } else {
                Compression::SNAPPY
            };
            let properties = WriterProperties::builder().set_compression(codec).build();
            let mut out = vec![];
            let mut writer =
                ArrowWriter::try_new(&mut out, batch.schema(), Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(&out))
                    .unwrap();
            assert!(reader
                .metadata()
                .row_groups()
                .iter()
                .all(|r| r.columns().iter().all(|c| c.compression() == codec)));
            assert_eq!(reader.metadata().file_metadata().num_rows(), 4);
            let path = dir.join(format!(
                "{}-{}.parquet",
                if relative { "relative" } else { "absolute" },
                if zstd { "zstd" } else { "snappy" }
            ));
            std::fs::write(&path, out).unwrap();
            println!(
                "{}: Arrow/Parquet55.2.0, 4 records, {:?}",
                path.file_name().unwrap().to_str().unwrap(),
                codec
            );
        }
    }
}
