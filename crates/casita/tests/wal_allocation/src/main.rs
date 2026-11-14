#![forbid(unsafe_code)]
use std::io::Write;
use wal3::{Error, Setsum};

fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("a frozen probe mode is required");
    let bytes: &[u8] = match mode.as_str() {
        "valid" | "truncated" => include_bytes!("../fixtures/valid.parquet"),
        "page-size" => include_bytes!("../fixtures/unbacked-page-size.parquet"),
        "statistics-default" => include_bytes!("../fixtures/unbacked-statistics.parquet"),
        _ => panic!("unknown probe mode"),
    };
    println!("probe={mode} input_bytes={}", bytes.len());
    std::io::stdout().flush().unwrap();
    let bytes = if mode == "truncated" {
        &bytes[..bytes.len() - 8]
    } else {
        bytes
    };
    let result = wal3::interfaces::checksum_parquet(bytes, true, None);
    if mode == "valid" {
        let (sum, records, relative, epoch) = result.expect("valid SNAPPY WAL must read");
        assert!(!relative);
        assert_eq!(epoch, 12345);
        assert_eq!(records.len(), 4);
        let mut expected_sum = Setsum::default();
        for (row, (position, actual)) in records.iter().enumerate() {
            let body: Vec<u8> = (0..128).map(|i| ((i + row * 31) % 251) as u8).collect();
            let offset = 41 + row as u64;
            assert_eq!(position.offset(), offset);
            assert_eq!(*actual, body);
            expected_sum.insert_vectored(&[&offset.to_be_bytes(), &12345u64.to_be_bytes(), &body]);
        }
        assert_eq!(sum, expected_sum);
    } else {
        assert!(
            matches!(result, Err(Error::CorruptFragment(_))),
            "malformed data must produce CorruptFragment without aborting"
        );
    }
    println!("probe={mode} completed_without_abort");
}
