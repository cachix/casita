//! Dependency regressions for RUSTSEC-2026-0391 and RUSTSEC-2026-0403.
//! Exercise the native profile's actual zstd dependency, including its legacy
//! decoder. The error probe uses initialized storage and never exposes spare
//! Vec capacity, even when run against the affected dependency.
#![cfg(feature = "native")]

use std::io::{Read, Write};
use zstd::zstd_safe::{CCtx, DCtx, InBuffer, OutBuffer};

#[test]
fn abandoned_encoder_does_not_prepend_previous_plaintext() {
    let mut context = CCtx::create();
    zstd::stream::write::Encoder::with_context(Vec::new(), &mut context)
        .write_all(b"previous private job")
        .unwrap();
    let mut encoder = zstd::stream::write::Encoder::with_context(Vec::new(), &mut context);
    encoder.write_all(b"next independent job").unwrap();
    let frame = encoder.finish().unwrap();
    assert_eq!(
        zstd::decode_all(frame.as_slice()).unwrap(),
        b"next independent job"
    );
}

#[test]
fn abandoned_decoder_does_not_return_previous_plaintext() {
    let previous = zstd::encode_all(&b"previous private job"[..], 1).unwrap();
    let next = zstd::encode_all(&b"next independent job"[..], 1).unwrap();
    let mut context = DCtx::create();
    zstd::stream::read::Decoder::with_context(previous.as_slice(), &mut context)
        .read_exact(&mut [0; 3])
        .unwrap();
    let mut output = Vec::new();
    zstd::stream::read::Decoder::with_context(next.as_slice(), &mut context)
        .read_to_end(&mut output)
        .unwrap();
    assert_eq!(output, b"next independent job");
}

#[test]
fn failed_legacy_frame_does_not_commit_output_or_input() {
    // A legacy v0.7 header with a reserved frame-descriptor bit set.
    let corrupt = [0x27, 0xb5, 0x2f, 0xfd, 0x08, 0x00];
    let mut storage = [0xa5; 1024];
    let mut input = InBuffer::around(&corrupt);
    let mut output = OutBuffer::around(&mut storage[..]);
    assert!(
        DCtx::create()
            .decompress_stream(&mut output, &mut input)
            .is_err()
    );
    assert_eq!(output.pos(), 0, "an error committed untrusted output");
    assert_eq!(input.pos(), 0, "an error committed untrusted input");
}

#[test]
fn independent_frames_and_invalid_input_preserve_normal_decoding() {
    let mut frames = Vec::new();
    for body in [b"first frame".as_slice(), b"second frame".as_slice()] {
        frames.extend(zstd::encode_all(body, 1).unwrap());
    }
    assert_eq!(
        zstd::decode_all(frames.as_slice()).unwrap(),
        b"first framesecond frame"
    );
    assert!(zstd::decode_all(&b"invalid frame"[..]).is_err());
}
