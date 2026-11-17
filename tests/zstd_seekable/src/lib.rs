//! RUSTSEC-2026-0398: past-EOF offsets must produce no bytes for either API.
//! Initialized slices make the old-version negative safe: inspect the count
//! without extending a Vec over memory it does not own. The extreme offsets
//! are reached only after the ordinary past-EOF check succeeds.
#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use zstd_safe::{
        InBuffer, OutBuffer, SafeResult,
        seekable::{Seekable, SeekableCStream},
    };

    const INPUT: &[u8] = b"independent seekable frames preserve complete plaintext";

    fn archive() -> Vec<u8> {
        let mut encoder = SeekableCStream::create();
        encoder.init(1, true, 16).unwrap();
        let mut input = InBuffer::around(INPUT);
        let mut bytes = [0; 1024];
        let mut output = OutBuffer::around(&mut bytes[..]);
        for _ in 0..100 {
            if input.pos() == INPUT.len() {
                break;
            }
            encoder.compress_stream(&mut output, &mut input).unwrap();
        }
        assert_eq!(input.pos(), INPUT.len());
        let mut finished = false;
        for _ in 0..100 {
            if encoder.end_stream(&mut output).unwrap() == 0 {
                finished = true;
                break;
            }
        }
        assert!(finished);
        output.as_slice().to_vec()
    }

    fn check(mut decompress: impl FnMut(&mut [u8], u64) -> SafeResult) {
        let mut bytes = [0xa5; 256];
        let n = decompress(&mut bytes, 0).unwrap();
        assert!(n <= bytes.len());
        assert_eq!(&bytes[..n], INPUT);
        let end = INPUT.len() as u64;
        let n = decompress(&mut bytes, end - 5).unwrap();
        assert!(n <= bytes.len());
        assert_eq!(&bytes[..n], &INPUT[INPUT.len() - 5..]);
        for offset in [end + 4096, end, end + 1, u64::MAX - 9, u64::MAX] {
            assert_eq!(decompress(&mut bytes, offset), Ok(0));
            assert_eq!(decompress(&mut bytes[..1], offset), Ok(0));
        }
    }

    #[test]
    fn borrowed_archive_bounds_offsets() {
        let bytes = archive();
        let mut decoder = Seekable::create();
        decoder.init_buff(&bytes).unwrap();
        check(|out, offset| decoder.decompress(out, offset));
    }

    #[test]
    fn advanced_reader_bounds_offsets() {
        let mut decoder = Seekable::create()
            .init_advanced(Box::new(Cursor::new(archive())))
            .unwrap();
        check(|out, offset| decoder.decompress(out, offset));
    }
}
