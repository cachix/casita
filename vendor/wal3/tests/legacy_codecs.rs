//! Historical fragments must remain readable without workspace feature unification.
#![forbid(unsafe_code)]

use wal3::interfaces::checksum_parquet;
use wal3::{Error, LogPosition, Setsum};

const BASE: u64 = 987;
const EPOCH: u64 = 123456;
const ABSOLUTE_SNAPPY: &[u8] = include_bytes!("fixtures/legacy_codecs/absolute-snappy.parquet");
const RELATIVE_SNAPPY: &[u8] = include_bytes!("fixtures/legacy_codecs/relative-snappy.parquet");
const ABSOLUTE_ZSTD: &[u8] = include_bytes!("fixtures/legacy_codecs/absolute-zstd.parquet");
const RELATIVE_ZSTD: &[u8] = include_bytes!("fixtures/legacy_codecs/relative-zstd.parquet");

fn bodies() -> Vec<Vec<u8>> {
    vec![
        b"immutable WAL record".to_vec(),
        vec![],
        vec![0xff, 0, 0x80],
        (0..262_144).map(|i| (i % 251) as u8).collect(),
    ]
}

fn verify(bytes: &[u8], relative: bool) {
    let expected_bodies = bodies();
    let start = relative.then_some(LogPosition::from_offset(BASE));
    let (sum, records, actual_relative, epoch) = checksum_parquet(bytes, true, start)
        .expect("valid historical WAL fragments must remain readable");
    let mut expected_sum = Setsum::default();
    for (i, body) in expected_bodies.iter().enumerate() {
        let raw_offset = i as u64 + if relative { 0 } else { BASE };
        expected_sum.insert_vectored(&[&raw_offset.to_be_bytes(), &EPOCH.to_be_bytes(), body]);
    }
    assert_eq!(sum, expected_sum);
    assert_eq!(actual_relative, relative);
    assert_eq!(epoch, EPOCH);
    assert_eq!(records.len(), expected_bodies.len());
    for (i, ((position, body), expected)) in records.iter().zip(&expected_bodies).enumerate() {
        assert_eq!(position.offset(), BASE + i as u64);
        assert_eq!(body, expected);
    }
    let skipped = checksum_parquet(bytes, false, start).unwrap();
    assert_eq!(skipped.0, Setsum::default());
    assert_eq!(skipped.1, records);
    assert_eq!(skipped.2, relative);
    assert_eq!(skipped.3, EPOCH);
}

#[test]
fn parquet55_snappy_absolute() {
    verify(ABSOLUTE_SNAPPY, false);
}

#[test]
fn parquet55_snappy_relative() {
    verify(RELATIVE_SNAPPY, true);
}

#[test]
fn parquet55_zstd_absolute() {
    verify(ABSOLUTE_ZSTD, false);
}

#[test]
fn parquet55_zstd_relative() {
    verify(RELATIVE_ZSTD, true);
}

#[test]
fn truncated_legacy_fragments_are_corrupt() {
    for (bytes, relative) in [
        (ABSOLUTE_SNAPPY, false),
        (RELATIVE_SNAPPY, true),
        (ABSOLUTE_ZSTD, false),
        (RELATIVE_ZSTD, true),
    ] {
        assert!(matches!(
            checksum_parquet(
                &bytes[..bytes.len() - 8],
                true,
                relative.then_some(LogPosition::from_offset(BASE)),
            ),
            Err(Error::CorruptFragment(_))
        ));
    }
}

#[test]
fn corrupt_zstd_pages_are_corrupt_fragments() {
    for (bytes, relative) in [(ABSOLUTE_ZSTD, false), (RELATIVE_ZSTD, true)] {
        let mut corrupt = bytes.to_vec();
        let frame = corrupt
            .windows(4)
            .position(|w| w == [0x28, 0xb5, 0x2f, 0xfd])
            .expect("frozen fixture must contain a ZSTD frame");
        corrupt[frame] ^= 0xff;
        assert!(matches!(
            checksum_parquet(
                &corrupt,
                true,
                relative.then_some(LogPosition::from_offset(BASE)),
            ),
            Err(Error::CorruptFragment(_))
        ));
    }
}

#[test]
fn starting_position_contract_is_preserved() {
    for (bytes, relative) in [
        (ABSOLUTE_SNAPPY, false),
        (RELATIVE_SNAPPY, true),
        (ABSOLUTE_ZSTD, false),
        (RELATIVE_ZSTD, true),
    ] {
        let invalid_start = (!relative).then_some(LogPosition::from_offset(BASE));
        assert!(checksum_parquet(bytes, true, invalid_start).is_err());
    }
}

#[test]
fn relative_offset_overflow_is_rejected_for_both_codecs() {
    for bytes in [RELATIVE_SNAPPY, RELATIVE_ZSTD] {
        let error = checksum_parquet(bytes, true, Some(LogPosition::from_offset(u64::MAX)))
            .expect_err("relative offsets must not wrap");
        assert!(
            matches!(error, Error::CorruptFragment(ref message) if message.contains("offset overflow"))
        );
    }
}
