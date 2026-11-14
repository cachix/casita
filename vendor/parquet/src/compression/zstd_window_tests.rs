//! A valid historical ZSTD window must remain readable with bounded page output.

use super::*;

#[test]
fn large_window_with_small_output_remains_readable() {
    let source = vec![42u8; 1024 * 1024];
    // zstd CLI 1.5.7: 1 MiB of byte 42 on stdin, compressed with --long=28.
    // The 51-byte frame advertises a 256 MiB window and no content size.
    let encoded = include_bytes!("fixtures/one_mib_window28.zst");

    // The former bulk decoder accepted this valid frame.
    let bulk = zstd::bulk::decompress(encoded, source.len()).unwrap();
    assert_eq!(bulk, source);

    let options = CodecOptions::default();
    let mut codec = create_codec(CodecType::ZSTD(ZstdLevel::default()), &options)
        .unwrap()
        .unwrap();
    let mut output = vec![7, 8, 9];
    assert_eq!(
        codec
            .decompress(encoded, &mut output, Some(source.len()))
            .unwrap(),
        source.len()
    );
    assert_eq!(&output[..3], &[7, 8, 9]);
    assert_eq!(&output[3..], source);
}

#[test]
fn concatenated_and_skippable_frames_preserve_output() {
    let first = zstd::bulk::compress(b"first", 1).unwrap();
    let second = zstd::bulk::compress(b"second", 1).unwrap();
    let mut encoded = first;
    encoded.extend_from_slice(&[0x50, 0x2a, 0x4d, 0x18, 4, 0, 0, 0, 1, 2, 3, 4]);
    encoded.extend_from_slice(&second);

    let options = CodecOptions::default();
    let mut codec = create_codec(CodecType::ZSTD(ZstdLevel::default()), &options)
        .unwrap()
        .unwrap();
    let mut output = vec![7, 8, 9];
    assert_eq!(
        codec.decompress(&encoded, &mut output, Some(11)).unwrap(),
        11
    );
    assert_eq!(&output, b"\x07\x08\x09firstsecond");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "run the test binary under a 256 MiB address-space limit"]
fn large_window_with_128_mib_plus_one_output_stays_within_bound() {
    const LENGTH: usize = 128 * 1024 * 1024 + 1;
    let encoded = include_bytes!("fixtures/one_128m_plus1_window28.zst");
    let options = CodecOptions::default();
    let mut codec = create_codec(CodecType::ZSTD(ZstdLevel::default()), &options)
        .unwrap()
        .unwrap();
    let mut output = Vec::new();
    assert_eq!(
        codec
            .decompress(encoded, &mut output, Some(LENGTH))
            .unwrap(),
        LENGTH
    );
    assert_eq!(output.len(), LENGTH);
    assert!(output.iter().all(|&byte| byte == 42));
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "run the test binary under a 256 MiB address-space limit"]
fn large_window_with_page_prefix_stays_within_bound() {
    const LENGTH: usize = 128 * 1024 * 1024 + 1;
    let encoded = include_bytes!("fixtures/one_128m_plus1_window28.zst");
    let options = CodecOptions::default();
    let mut codec = create_codec(CodecType::ZSTD(ZstdLevel::default()), &options)
        .unwrap()
        .unwrap();
    let mut output = vec![7, 8, 9];
    assert_eq!(
        codec
            .decompress(encoded, &mut output, Some(LENGTH))
            .unwrap(),
        LENGTH
    );
    assert_eq!(&output[..3], &[7, 8, 9]);
    assert_eq!(output.len(), LENGTH + 3);
    assert!(output[3..].iter().all(|&byte| byte == 42));
}
