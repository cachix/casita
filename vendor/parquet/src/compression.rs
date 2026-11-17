// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Modified for Casita: bound decompression of untrusted Parquet pages and
// preserve large-window ZSTD compatibility. See vendor/README.md.

//! Contains codec interface and supported codec implementations.
//!
//! See [`Compression`](crate::basic::Compression) enum for all available compression
//! algorithms.
//!
// NOTE: this must be an inner attribute so that the example is attached to the module (and
// therefore actually run as a doc test) rather than to the `use` statement below.
#![cfg_attr(
    feature = "experimental",
    doc = r"
# Example

```no_run
use parquet::{basic::Compression, compression::{create_codec, CodecOptionsBuilder}};

let codec_options = CodecOptionsBuilder::default()
    .set_backward_compatible_lz4(false)
    .build();
let mut codec = match create_codec(Compression::SNAPPY, &codec_options) {
 Ok(Some(codec)) => codec,
 _ => panic!(),
};

let data = vec![b'p', b'a', b'r', b'q', b'u', b'e', b't'];
let mut compressed = vec![];
codec.compress(&data[..], &mut compressed).unwrap();

let mut output = vec![];
codec.decompress(&compressed[..], &mut output, None).unwrap();

assert_eq!(output, data);
```
"
)]
use crate::basic::Compression as CodecType;
use crate::errors::{ParquetError, Result};

/// Parquet compression codec interface.
pub trait Codec: Send {
    /// Compresses data stored in slice `input_buf` and appends the compressed result
    /// to `output_buf`.
    ///
    /// Note that you'll need to call `clear()` before reusing the same `output_buf`
    /// across different `compress` calls.
    fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()>;

    /// Decompresses data stored in slice `input_buf` and appends output to `output_buf`.
    ///
    /// If `uncompress_size` is provided, codecs may use it to bound the output.
    /// Callers must still check that the decoded size matches their expected size.
    ///
    /// Returns the total number of bytes written.
    fn decompress(
        &mut self,
        input_buf: &[u8],
        output_buf: &mut Vec<u8>,
        uncompress_size: Option<usize>,
    ) -> Result<usize>;
}

#[cfg(any(
    feature = "brotli",
    feature = "flate2",
    feature = "zstd",
    feature = "lz4",
    test
))]
fn decompress_stream<R: std::io::Read>(
    mut reader: R,
    output_buf: &mut Vec<u8>,
    uncompress_size: Option<usize>,
) -> Result<usize> {
    let start = output_buf.len();
    let result = (|| {
        let mut chunk = [0u8; 8192];
        loop {
            let read = reader.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            let decoded = output_buf.len() - start;
            let next = decoded.checked_add(read).ok_or_else(|| {
                ParquetError::General("Decoded page length overflows usize".into())
            })?;
            if uncompress_size.is_some_and(|expected| next > expected) {
                return Err(ParquetError::General(
                    "Decoded page exceeds declared size".into(),
                ));
            }
            if output_buf.capacity() - output_buf.len() < read {
                let additional = uncompress_size
                    .map(|expected| {
                        let remaining = expected - decoded;
                        if remaining <= decoded.saturating_add(read) {
                            remaining
                        } else {
                            remaining.min(decoded.max(read))
                        }
                    })
                    .unwrap_or_else(|| decoded.max(read));
                output_buf.try_reserve_exact(additional).map_err(|e| {
                    ParquetError::General(format!("Cannot reserve decoded page output: {e}"))
                })?;
            }
            output_buf.extend_from_slice(&chunk[..read]);
        }
        Ok(output_buf.len() - start)
    })();
    if result.is_err() {
        output_buf.truncate(start);
    }
    result
}

/// Struct to hold `Codec` creation options.
#[derive(Debug, PartialEq, Eq)]
pub struct CodecOptions {
    /// Whether or not to fallback to other LZ4 older implementations on error in LZ4_HADOOP.
    backward_compatible_lz4: bool,
}

impl Default for CodecOptions {
    fn default() -> Self {
        CodecOptionsBuilder::default().build()
    }
}

pub struct CodecOptionsBuilder {
    /// Whether or not to fallback to other LZ4 older implementations on error in LZ4_HADOOP.
    backward_compatible_lz4: bool,
}

impl Default for CodecOptionsBuilder {
    fn default() -> Self {
        Self {
            backward_compatible_lz4: true,
        }
    }
}

impl CodecOptionsBuilder {
    /// Enable/disable backward compatible LZ4.
    ///
    /// If backward compatible LZ4 is enable, on LZ4_HADOOP error it will fallback
    /// to the older versions LZ4 algorithms. That is LZ4_FRAME, for backward compatibility
    /// with files generated by older versions of this library, and LZ4_RAW, for backward
    /// compatibility with files generated by older versions of parquet-cpp.
    ///
    /// If backward compatible LZ4 is disabled, on LZ4_HADOOP error it will return the error.
    pub fn set_backward_compatible_lz4(mut self, value: bool) -> CodecOptionsBuilder {
        self.backward_compatible_lz4 = value;
        self
    }

    pub fn build(self) -> CodecOptions {
        CodecOptions {
            backward_compatible_lz4: self.backward_compatible_lz4,
        }
    }
}

/// Defines valid compression levels.
pub(crate) trait CompressionLevel<T: std::fmt::Display + std::cmp::PartialOrd> {
    const MINIMUM_LEVEL: T;
    const MAXIMUM_LEVEL: T;

    /// Tests if the provided compression level is valid.
    fn is_valid_level(level: T) -> Result<()> {
        let compression_range = Self::MINIMUM_LEVEL..=Self::MAXIMUM_LEVEL;
        if compression_range.contains(&level) {
            Ok(())
        } else {
            Err(ParquetError::General(format!(
                "valid compression range {}..={} exceeded.",
                compression_range.start(),
                compression_range.end()
            )))
        }
    }
}

/// Given the compression type `codec`, returns a codec used to compress and decompress
/// bytes for the compression type.
/// This returns `None` if the codec type is `UNCOMPRESSED`.
#[cfg_attr(
    any(feature = "lz4", test),
    expect(
        clippy::used_underscore_binding,
        reason = "`_options` is only read when the `lz4` feature is on"
    )
)]
pub fn create_codec(codec: CodecType, _options: &CodecOptions) -> Result<Option<Box<dyn Codec>>> {
    #[cfg_attr(
        any(
            test,
            feature = "brotli",
            feature = "flate2",
            feature = "lz4",
            feature = "snap",
            feature = "zstd"
        ),
        expect(unreachable_code)
    )]
    #[cfg_attr(
        all(
            not(test),
            not(all(feature = "brotli", feature = "flate2", feature = "zstd"))
        ),
        expect(unused_variables)
    )]
    match codec {
        CodecType::BROTLI(level) => {
            #[cfg(any(feature = "brotli", test))]
            return Ok(Some(Box::new(BrotliCodec::new(level))));
            Err(ParquetError::General(
                "Disabled feature at compile time: brotli".into(),
            ))
        }
        CodecType::GZIP(level) => {
            #[cfg(any(feature = "flate2", test))]
            return Ok(Some(Box::new(GZipCodec::new(level))));
            Err(ParquetError::General(
                "Disabled feature at compile time: flate2".into(),
            ))
        }
        CodecType::SNAPPY => {
            #[cfg(any(feature = "snap", test))]
            return Ok(Some(Box::new(SnappyCodec::new())));
            Err(ParquetError::General(
                "Disabled feature at compile time: snap".into(),
            ))
        }
        CodecType::LZ4 => {
            #[cfg(any(feature = "lz4", test))]
            return Ok(Some(Box::new(LZ4HadoopCodec::new(
                _options.backward_compatible_lz4,
            ))));
            Err(ParquetError::General(
                "Disabled feature at compile time: lz4".into(),
            ))
        }
        CodecType::ZSTD(level) => {
            #[cfg(any(feature = "zstd", test))]
            return Ok(Some(Box::new(ZSTDCodec::new(level))));
            Err(ParquetError::General(
                "Disabled feature at compile time: zstd".into(),
            ))
        }
        CodecType::LZ4_RAW => {
            #[cfg(any(feature = "lz4", test))]
            return Ok(Some(Box::new(LZ4RawCodec::new())));
            Err(ParquetError::General(
                "Disabled feature at compile time: lz4".into(),
            ))
        }
        CodecType::UNCOMPRESSED => Ok(None),
        CodecType::LZO => Err(nyi_err!("The codec type {} is not supported yet", codec)),
    }
}

#[cfg(any(feature = "snap", test))]
mod snappy_codec {
    use snap::raw::{Decoder, Encoder, decompress_len, max_compress_len};

    use crate::compression::Codec;
    use crate::errors::{ParquetError, Result};

    /// Codec for Snappy compression format.
    pub struct SnappyCodec {
        decoder: Decoder,
        encoder: Encoder,
    }

    impl SnappyCodec {
        /// Creates new Snappy compression codec.
        pub(crate) fn new() -> Self {
            Self {
                decoder: Decoder::new(),
                encoder: Encoder::new(),
            }
        }
    }

    // Walk the raw Snappy tokens before trusting either size in the page.
    // A matching stream header can still advertise a huge output with only
    // padding after it; the decoder must not reserve that output first.
    fn check_encoded_size(input: &[u8], expected: usize) -> Result<()> {
        let invalid =
            || ParquetError::General("Invalid Snappy page encoding or output size".into());
        let mut pos = 0;
        loop {
            let byte = *input.get(pos).ok_or_else(invalid)?;
            pos += 1;
            if byte & 0x80 == 0 {
                break;
            }
            if pos >= 5 {
                return Err(invalid());
            }
        }
        let mut produced = 0usize;
        while pos < input.len() {
            let tag = input[pos];
            pos += 1;
            let kind = tag & 3;
            let length = match kind {
                0 => {
                    let mut length = (tag >> 2) as usize;
                    if length >= 60 {
                        let bytes = length - 59;
                        let end = pos.checked_add(bytes).ok_or_else(invalid)?;
                        let encoded = input.get(pos..end).ok_or_else(invalid)?;
                        length = encoded
                            .iter()
                            .enumerate()
                            .fold(0usize, |n, (i, b)| n | ((*b as usize) << (i * 8)));
                        pos = end;
                    }
                    length = length.checked_add(1).ok_or_else(invalid)?;
                    pos = pos.checked_add(length).ok_or_else(invalid)?;
                    if pos > input.len() {
                        return Err(invalid());
                    }
                    length
                }
                1 => {
                    let offset = (((tag as usize) & 0xe0) << 3)
                        | *input.get(pos).ok_or_else(invalid)? as usize;
                    pos += 1;
                    if offset == 0 || offset > produced {
                        return Err(invalid());
                    }
                    4 + ((tag as usize >> 2) & 7)
                }
                2 | 3 => {
                    let bytes = if kind == 2 { 2 } else { 4 };
                    let end = pos.checked_add(bytes).ok_or_else(invalid)?;
                    let encoded = input.get(pos..end).ok_or_else(invalid)?;
                    let offset = encoded
                        .iter()
                        .enumerate()
                        .fold(0usize, |n, (i, b)| n | ((*b as usize) << (i * 8)));
                    pos = end;
                    if offset == 0 || offset > produced {
                        return Err(invalid());
                    }
                    1 + (tag as usize >> 2)
                }
                _ => unreachable!(),
            };
            produced = produced.checked_add(length).ok_or_else(invalid)?;
            if produced > expected {
                return Err(invalid());
            }
        }
        if produced != expected {
            return Err(invalid());
        }
        Ok(())
    }

    impl Codec for SnappyCodec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            let len = decompress_len(input_buf)?;
            if uncompress_size.is_some_and(|expected| expected != len) {
                return Err(ParquetError::General(
                    "Snappy page size does not match compressed data".into(),
                ));
            }
            // The largest Snappy copy emits 64 bytes from a three-byte tag.
            // Literals and other copies expand less, so a valid stream cannot
            // produce more than 22 bytes per compressed input byte.
            if len > input_buf.len().saturating_mul(22) {
                return Err(ParquetError::General(
                    "Snappy page size exceeds the encoded input bound".into(),
                ));
            }
            check_encoded_size(input_buf, len)?;
            let offset = output_buf.len();
            let end = offset.checked_add(len).ok_or_else(|| {
                ParquetError::General("Snappy page size overflows output length".into())
            })?;
            output_buf.try_reserve_exact(len).map_err(|e| {
                ParquetError::General(format!("Cannot reserve Snappy page output: {e}"))
            })?;
            output_buf.resize(end, 0);
            self.decoder
                .decompress(input_buf, &mut output_buf[offset..])
                .map_err(|e| e.into())
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            let output_buf_len = output_buf.len();
            let required_len = max_compress_len(input_buf.len());
            output_buf.resize(output_buf_len + required_len, 0);
            let n = self
                .encoder
                .compress(input_buf, &mut output_buf[output_buf_len..])?;
            output_buf.truncate(output_buf_len + n);
            Ok(())
        }
    }
}
#[cfg(any(feature = "snap", test))]
pub use snappy_codec::*;

#[cfg(any(feature = "flate2", test))]
mod gzip_codec {

    use std::io::Write;

    use flate2::{Compression, read, write};

    use crate::compression::Codec;
    use crate::errors::Result;

    use super::GzipLevel;

    /// Codec for GZIP compression algorithm.
    pub struct GZipCodec {
        level: GzipLevel,
    }

    impl GZipCodec {
        /// Creates new GZIP compression codec.
        pub(crate) fn new(level: GzipLevel) -> Self {
            Self { level }
        }
    }

    impl Codec for GZipCodec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            let decoder = read::MultiGzDecoder::new(input_buf);
            super::decompress_stream(decoder, output_buf, uncompress_size)
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            let mut encoder = write::GzEncoder::new(output_buf, Compression::new(self.level.0));
            encoder.write_all(input_buf)?;
            encoder.try_finish().map_err(|e| e.into())
        }
    }
}
#[cfg(any(feature = "flate2", test))]
pub use gzip_codec::*;

/// Represents a valid gzip compression level.
///
/// Defaults to 6.
///
/// * 0: least compression
/// * 9: most compression (that other software can read)
/// * 10: most compression (incompatible with other software, see below)
/// #### WARNING:
/// Level 10 compression can offer smallest file size,
/// but Parquet files created with it will not be readable
/// by other "standard" paquet readers.
///
/// Do **NOT** use level 10 if you need other software to
/// be able to read the files. Read below for details.
///
/// ### IMPORTANT:
/// There's often confusion about the compression levels in `flate2` vs `arrow`
/// as highlighted in issue [#1011](https://github.com/apache/arrow-rs/issues/6282).
///
/// `flate2` supports two compression backends: `miniz_oxide` and `zlib`.
///
/// - `zlib` supports levels from 0 to 9.
/// - `miniz_oxide` supports levels from 0 to 10.
///
/// `arrow` uses `flate2` with `rust_backend` feature,
/// which provides `miniz_oxide` as the backend.
/// Therefore 0-10 levels are supported.
///
/// `flate2` documents this behavior properly with
/// [this commit](https://github.com/rust-lang/flate2-rs/pull/430).
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub struct GzipLevel(u32);

impl Default for GzipLevel {
    fn default() -> Self {
        // The default as of miniz_oxide 0.5.1 is 6 for compression level
        // (miniz_oxide::deflate::CompressionLevel::DefaultLevel)
        Self(6)
    }
}

impl CompressionLevel<u32> for GzipLevel {
    const MINIMUM_LEVEL: u32 = 0;
    const MAXIMUM_LEVEL: u32 = 9;
}

impl GzipLevel {
    /// Attempts to create a gzip compression level.
    ///
    /// Compression levels must be valid (i.e. be acceptable for [`flate2::Compression`]).
    pub fn try_new(level: u32) -> Result<Self> {
        Self::is_valid_level(level).map(|()| Self(level))
    }

    /// Returns the compression level.
    pub fn compression_level(&self) -> u32 {
        self.0
    }
}

#[cfg(any(feature = "brotli", test))]
mod brotli_codec {

    use std::io::Write;

    use crate::compression::Codec;
    use crate::errors::Result;

    use super::BrotliLevel;

    const BROTLI_DEFAULT_BUFFER_SIZE: usize = 4096;
    const BROTLI_DEFAULT_LG_WINDOW_SIZE: u32 = 22; // recommended between 20-22

    /// Codec for Brotli compression algorithm.
    pub struct BrotliCodec {
        level: BrotliLevel,
    }

    impl BrotliCodec {
        /// Creates new Brotli compression codec.
        pub(crate) fn new(level: BrotliLevel) -> Self {
            Self { level }
        }
    }

    impl Codec for BrotliCodec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            // The page's declared output size is not a safe decoder buffer size.
            let decoder = brotli::Decompressor::new(input_buf, BROTLI_DEFAULT_BUFFER_SIZE);
            super::decompress_stream(decoder, output_buf, uncompress_size)
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            let mut encoder = brotli::CompressorWriter::new(
                output_buf,
                BROTLI_DEFAULT_BUFFER_SIZE,
                self.level.0,
                BROTLI_DEFAULT_LG_WINDOW_SIZE,
            );
            encoder.write_all(input_buf)?;
            encoder.flush().map_err(|e| e.into())
        }
    }
}
#[cfg(any(feature = "brotli", test))]
pub use brotli_codec::*;

/// Represents a valid brotli compression level.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub struct BrotliLevel(u32);

impl Default for BrotliLevel {
    fn default() -> Self {
        Self(1)
    }
}

impl CompressionLevel<u32> for BrotliLevel {
    const MINIMUM_LEVEL: u32 = 0;
    const MAXIMUM_LEVEL: u32 = 11;
}

impl BrotliLevel {
    /// Attempts to create a brotli compression level.
    ///
    /// Compression levels must be valid.
    pub fn try_new(level: u32) -> Result<Self> {
        Self::is_valid_level(level).map(|()| Self(level))
    }

    /// Returns the compression level.
    pub fn compression_level(&self) -> u32 {
        self.0
    }
}

#[cfg(any(feature = "lz4", test))]
mod lz4_codec {
    use std::io::Write;

    use crate::compression::Codec;
    use crate::errors::{ParquetError, Result};

    const LZ4_BUFFER_SIZE: usize = 4096;

    /// Codec for LZ4 compression algorithm.
    pub struct LZ4Codec {}

    impl LZ4Codec {
        /// Creates new LZ4 compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for LZ4Codec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            let decoder = lz4_flex::frame::FrameDecoder::new(input_buf);
            super::decompress_stream(decoder, output_buf, uncompress_size)
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(output_buf);
            let mut from = 0;
            loop {
                let to = std::cmp::min(from + LZ4_BUFFER_SIZE, input_buf.len());
                encoder.write_all(&input_buf[from..to])?;
                from += LZ4_BUFFER_SIZE;
                if from >= input_buf.len() {
                    break;
                }
            }
            match encoder.finish() {
                Ok(_) => Ok(()),
                Err(e) => Err(ParquetError::External(Box::new(e))),
            }
        }
    }
}

#[cfg(all(feature = "experimental", any(feature = "lz4", test)))]
pub use lz4_codec::*;

#[cfg(any(feature = "zstd", test))]
mod zstd_codec {
    use crate::compression::{Codec, ZstdLevel};
    use crate::errors::Result;
    use std::io::Cursor;

    /// Codec for Zstandard compression algorithm.
    ///
    /// Uses a reusable bulk compressor for writes and bounded bulk
    /// decompression for reads.
    pub struct ZSTDCodec {
        compressor: zstd::bulk::Compressor<'static>,
    }

    impl ZSTDCodec {
        /// Creates new Zstandard compression codec.
        pub(crate) fn new(level: ZstdLevel) -> Self {
            Self {
                compressor: zstd::bulk::Compressor::new(level.compression_level())
                    .expect("valid zstd compression level"),
            }
        }
    }

    impl Codec for ZSTDCodec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            // Keep the existing streaming behavior when no page size is known.
            let Some(expected_size) = uncompress_size else {
                let decoder = zstd::Decoder::new(input_buf)?;
                return super::decompress_stream(decoder, output_buf, None);
            };
            let start = output_buf.len();
            let result = (|| {
                let mut decoder = zstd::zstd_safe::DCtx::try_create().ok_or_else(|| {
                    crate::errors::ParquetError::General("Cannot create ZSTD decoder".into())
                })?;
                let too_small =
                    (zstd::zstd_safe::zstd_sys::ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall
                        as usize)
                        .wrapping_neg();
                let mut input = input_buf;
                let mut first_frame = true;
                while !input.is_empty() {
                    let frame_len =
                        zstd::zstd_safe::find_frame_compressed_size(input).map_err(|code| {
                            crate::errors::ParquetError::General(format!(
                                "Invalid ZSTD frame: {}",
                                zstd::zstd_safe::get_error_name(code)
                            ))
                        })?;
                    if frame_len == 0 || frame_len > input.len() {
                        return Err(crate::errors::ParquetError::General(
                            "Invalid ZSTD frame length".into(),
                        ));
                    }
                    let frame = &input[..frame_len];
                    let produced = output_buf.len() - start;
                    let remaining = expected_size.checked_sub(produced).ok_or_else(|| {
                        crate::errors::ParquetError::General(
                            "Decoded page exceeds declared size".into(),
                        )
                    })?;
                    // A frame size is untrusted, so use it only up to a small
                    // initial cap. Unknown-size frames begin from encoded bytes.
                    let known_size = zstd::zstd_safe::get_frame_content_size(frame)
                        .ok()
                        .flatten()
                        .and_then(|size| usize::try_from(size).ok());
                    if known_size.is_some_and(|size| size > remaining) {
                        return Err(crate::errors::ParquetError::General(
                            "Decoded page exceeds declared size".into(),
                        ));
                    }
                    let mut capacity = known_size
                        .map(|size| size.min(1024 * 1024))
                        .unwrap_or_else(|| frame.len().saturating_mul(8).clamp(256, 64 * 1024))
                        .min(remaining);
                    loop {
                        let mut frame_output = Vec::new();
                        let prefix_len = if first_frame { output_buf.len() } else { 0 };
                        let reserve = prefix_len.checked_add(capacity).ok_or_else(|| {
                            crate::errors::ParquetError::General(
                                "Decoded page length overflows usize".into(),
                            )
                        })?;
                        frame_output.try_reserve_exact(reserve).map_err(|e| {
                            crate::errors::ParquetError::General(format!(
                                "Cannot reserve decoded page output: {e}"
                            ))
                        })?;
                        match decoder.decompress(&mut frame_output, frame) {
                            Ok(written) => {
                                if written > remaining {
                                    return Err(crate::errors::ParquetError::General(
                                        "Decoded page exceeds declared size".into(),
                                    ));
                                }
                                if first_frame {
                                    if prefix_len > 0 {
                                        let total =
                                            written.checked_add(prefix_len).ok_or_else(|| {
                                                crate::errors::ParquetError::General(
                                                    "Decoded page length overflows usize".into(),
                                                )
                                            })?;
                                        frame_output.resize(total, 0);
                                        frame_output.copy_within(0..written, prefix_len);
                                        frame_output[..prefix_len].copy_from_slice(output_buf);
                                    }
                                    *output_buf = frame_output;
                                } else {
                                    output_buf.try_reserve(written).map_err(|e| {
                                        crate::errors::ParquetError::General(format!(
                                            "Cannot reserve decoded page output: {e}"
                                        ))
                                    })?;
                                    output_buf.extend_from_slice(&frame_output);
                                }
                                break;
                            }
                            Err(code) if code == too_small => {
                                if capacity == remaining {
                                    return Err(crate::errors::ParquetError::General(
                                        "Decoded page exceeds declared size".into(),
                                    ));
                                }
                                let next = capacity.saturating_mul(2).max(1).min(remaining);
                                if next == capacity {
                                    return Err(crate::errors::ParquetError::General(
                                        "Decoded page output is too large".into(),
                                    ));
                                }
                                capacity = next;
                            }
                            Err(code) => {
                                return Err(crate::errors::ParquetError::General(format!(
                                    "ZSTD decompression failed: {}",
                                    zstd::zstd_safe::get_error_name(code)
                                )));
                            }
                        }
                    }
                    input = &input[frame_len..];
                    first_frame = false;
                }
                Ok(output_buf.len() - start)
            })();
            if result.is_err() {
                output_buf.truncate(start);
            }
            result
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            let offset = output_buf.len();
            let len = zstd::zstd_safe::compress_bound(input_buf.len());
            output_buf.reserve(len);

            let mut cursor = Cursor::new(output_buf);
            cursor.set_position(offset as u64);
            let _written = self.compressor.compress_to_buffer(input_buf, &mut cursor)?;
            Ok(())
        }
    }
}
#[cfg(any(feature = "zstd", test))]
pub use zstd_codec::*;

/// Represents a valid zstd compression level.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub struct ZstdLevel(i32);

impl CompressionLevel<i32> for ZstdLevel {
    // zstd binds to C, and hence zstd::compression_level_range() is not const as this calls the
    // underlying C library.
    const MINIMUM_LEVEL: i32 = -131072;
    const MAXIMUM_LEVEL: i32 = 22;
}

impl ZstdLevel {
    /// Attempts to create a zstd compression level from a given compression level.
    ///
    /// Compression levels must be valid (i.e. be acceptable for [`zstd::compression_level_range`]).
    pub fn try_new(level: i32) -> Result<Self> {
        Self::is_valid_level(level).map(|()| Self(level))
    }

    /// Returns the compression level.
    pub fn compression_level(&self) -> i32 {
        self.0
    }
}

impl Default for ZstdLevel {
    fn default() -> Self {
        Self(1)
    }
}

#[cfg(any(feature = "lz4", test))]
fn check_lz4_block_size(input: &[u8], expected: usize) -> crate::errors::Result<()> {
    use crate::errors::ParquetError;

    let invalid = || ParquetError::General("Invalid LZ4 block or output size".into());
    let mut pos = 0usize;
    let mut produced = 0usize;
    while pos < input.len() {
        let token = input[pos];
        pos += 1;
        let mut literal_len = (token >> 4) as usize;
        if literal_len == 15 {
            loop {
                let extra = *input.get(pos).ok_or_else(invalid)? as usize;
                pos += 1;
                literal_len = literal_len.checked_add(extra).ok_or_else(invalid)?;
                if extra != 255 {
                    break;
                }
            }
        }
        pos = pos.checked_add(literal_len).ok_or_else(invalid)?;
        if pos > input.len() {
            return Err(invalid());
        }
        produced = produced.checked_add(literal_len).ok_or_else(invalid)?;
        if produced > expected {
            return Err(invalid());
        }
        if pos == input.len() {
            break;
        }
        let offset_end = pos.checked_add(2).ok_or_else(invalid)?;
        let offset_bytes = input.get(pos..offset_end).ok_or_else(invalid)?;
        let offset = u16::from_le_bytes([offset_bytes[0], offset_bytes[1]]) as usize;
        pos = offset_end;
        if offset == 0 || offset > produced {
            return Err(invalid());
        }
        let mut match_len = (token & 15) as usize + 4;
        if token & 15 == 15 {
            loop {
                let extra = *input.get(pos).ok_or_else(invalid)? as usize;
                pos += 1;
                match_len = match_len.checked_add(extra).ok_or_else(invalid)?;
                if extra != 255 {
                    break;
                }
            }
        }
        produced = produced.checked_add(match_len).ok_or_else(invalid)?;
        if produced > expected {
            return Err(invalid());
        }
    }
    if produced != expected {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(any(feature = "lz4", test))]
mod lz4_raw_codec {
    use crate::compression::Codec;
    use crate::errors::ParquetError;
    use crate::errors::Result;

    /// Codec for LZ4 Raw compression algorithm.
    pub struct LZ4RawCodec {}

    impl LZ4RawCodec {
        /// Creates new LZ4 Raw compression codec.
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    impl Codec for LZ4RawCodec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            let offset = output_buf.len();
            let Some(required_len) = uncompress_size else {
                return Err(ParquetError::General(
                    "LZ4RawCodec unsupported without uncompress_size".into(),
                ));
            };
            // LZ4 block matches add at most 255 bytes per encoded length byte.
            if required_len > input_buf.len().saturating_mul(256) {
                return Err(ParquetError::General(
                    "LZ4 page size exceeds the encoded input bound".into(),
                ));
            }
            super::check_lz4_block_size(input_buf, required_len)?;
            let end = offset.checked_add(required_len).ok_or_else(|| {
                ParquetError::General("LZ4 page size overflows output length".into())
            })?;
            output_buf.try_reserve_exact(required_len).map_err(|e| {
                ParquetError::General(format!("Cannot reserve LZ4 page output: {e}"))
            })?;
            output_buf.resize(end, 0);
            match lz4_flex::block::decompress_into(input_buf, &mut output_buf[offset..]) {
                Ok(n) => {
                    if n != required_len {
                        return Err(ParquetError::General(
                            "LZ4RawCodec uncompress_size is not the expected one".into(),
                        ));
                    }
                    Ok(n)
                }
                Err(e) => Err(ParquetError::External(Box::new(e))),
            }
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            let offset = output_buf.len();
            let required_len = lz4_flex::block::get_maximum_output_size(input_buf.len());
            output_buf.resize(offset + required_len, 0);
            match lz4_flex::block::compress_into(input_buf, &mut output_buf[offset..]) {
                Ok(n) => {
                    output_buf.truncate(offset + n);
                    Ok(())
                }
                Err(e) => Err(ParquetError::External(Box::new(e))),
            }
        }
    }
}
#[cfg(any(feature = "lz4", test))]
pub use lz4_raw_codec::*;

#[cfg(any(feature = "lz4", test))]
mod lz4_hadoop_codec {
    use crate::compression::Codec;
    use crate::compression::lz4_codec::LZ4Codec;
    use crate::compression::lz4_raw_codec::LZ4RawCodec;
    use crate::errors::{ParquetError, Result};
    use std::io;

    /// Size of u32 type.
    const SIZE_U32: usize = std::mem::size_of::<u32>();

    /// Length of the LZ4_HADOOP prefix.
    const PREFIX_LEN: usize = SIZE_U32 * 2;

    /// Codec for LZ4 Hadoop compression algorithm.
    pub struct LZ4HadoopCodec {
        /// Whether or not to fallback to other LZ4 implementations on error.
        /// Fallback is done to be backward compatible with older versions of this
        /// library and older versions parquet-cpp.
        backward_compatible_lz4: bool,
    }

    impl LZ4HadoopCodec {
        /// Creates new LZ4 Hadoop compression codec.
        pub(crate) fn new(backward_compatible_lz4: bool) -> Self {
            Self {
                backward_compatible_lz4,
            }
        }
    }

    /// Try to decompress the buffer as if it was compressed with the Hadoop Lz4Codec.
    /// Adapted from pola-rs [compression.rs:try_decompress_hadoop](https://pola-rs.github.io/polars/src/parquet2/compression.rs.html#225)
    /// Translated from the apache arrow c++ function [TryDecompressHadoop](https://github.com/apache/arrow/blob/bf18e6e4b5bb6180706b1ba0d597a65a4ce5ca48/cpp/src/arrow/util/compression_lz4.cc#L474).
    /// Returns error if decompression failed.
    fn try_decompress_hadoop(input_buf: &[u8], output_buf: &mut [u8]) -> io::Result<usize> {
        // Parquet files written with the Hadoop Lz4Codec use their own framing.
        // The input buffer can contain an arbitrary number of "frames", each
        // with the following structure:
        // - bytes 0..3: big-endian uint32_t representing the frame decompressed size
        // - bytes 4..7: big-endian uint32_t representing the frame compressed size
        // - bytes 8...: frame compressed data
        //
        // The Hadoop Lz4Codec source code can be found here:
        // https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
        let mut input_len = input_buf.len();
        let mut input = input_buf;
        let mut read_bytes = 0;
        let mut output_len = output_buf.len();
        let mut output: &mut [u8] = output_buf;
        while input_len >= PREFIX_LEN {
            let mut bytes = [0; SIZE_U32];
            bytes.copy_from_slice(&input[0..4]);
            let expected_decompressed_size = u32::from_be_bytes(bytes);
            let mut bytes = [0; SIZE_U32];
            bytes.copy_from_slice(&input[4..8]);
            let expected_compressed_size = u32::from_be_bytes(bytes);
            input = &input[PREFIX_LEN..];
            input_len -= PREFIX_LEN;

            if input_len < expected_compressed_size as usize {
                return Err(io::Error::other("Not enough bytes for Hadoop frame"));
            }

            if output_len < expected_decompressed_size as usize {
                return Err(io::Error::other(
                    "Not enough bytes to hold advertised output",
                ));
            }
            let decompressed_size =
                lz4_flex::decompress_into(&input[..expected_compressed_size as usize], output)
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
            if decompressed_size != expected_decompressed_size as usize {
                return Err(io::Error::other("Unexpected decompressed size"));
            }
            input_len -= expected_compressed_size as usize;
            output_len -= expected_decompressed_size as usize;
            read_bytes += expected_decompressed_size as usize;
            if input_len > expected_compressed_size as usize {
                input = &input[expected_compressed_size as usize..];
                output = &mut output[expected_decompressed_size as usize..];
            } else {
                break;
            }
        }
        if input_len == 0 {
            Ok(read_bytes)
        } else {
            Err(io::Error::other("Not all input are consumed"))
        }
    }

    fn check_hadoop_size(input: &[u8], expected: usize) -> Result<()> {
        let invalid = || ParquetError::General("Invalid LZ4 Hadoop page or output size".into());
        let mut pos = 0usize;
        let mut produced = 0usize;
        while pos < input.len() {
            let header_end = pos.checked_add(PREFIX_LEN).ok_or_else(invalid)?;
            let header = input.get(pos..header_end).ok_or_else(invalid)?;
            let decoded = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
            let encoded = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
            pos = header_end;
            let block_end = pos.checked_add(encoded).ok_or_else(invalid)?;
            let block = input.get(pos..block_end).ok_or_else(invalid)?;
            super::check_lz4_block_size(block, decoded)?;
            produced = produced.checked_add(decoded).ok_or_else(invalid)?;
            if produced > expected {
                return Err(invalid());
            }
            pos = block_end;
        }
        if produced != expected {
            return Err(invalid());
        }
        Ok(())
    }

    impl Codec for LZ4HadoopCodec {
        fn decompress(
            &mut self,
            input_buf: &[u8],
            output_buf: &mut Vec<u8>,
            uncompress_size: Option<usize>,
        ) -> Result<usize> {
            let output_len = output_buf.len();
            let Some(required_len) = uncompress_size else {
                return Err(ParquetError::General(
                    "LZ4HadoopCodec unsupported without uncompress_size".into(),
                ));
            };
            if required_len > input_buf.len().saturating_mul(256) {
                return Err(ParquetError::General(
                    "LZ4 page size exceeds the encoded input bound".into(),
                ));
            }
            if let Err(e) = check_hadoop_size(input_buf, required_len) {
                if !self.backward_compatible_lz4 {
                    return Err(e);
                }
                return match LZ4Codec::new().decompress(input_buf, output_buf, uncompress_size) {
                    Ok(n) => Ok(n),
                    Err(_) => {
                        output_buf.truncate(output_len);
                        let result =
                            LZ4RawCodec::new().decompress(input_buf, output_buf, uncompress_size);
                        if result.is_err() {
                            output_buf.truncate(output_len);
                        }
                        result
                    }
                };
            }
            let end = output_len.checked_add(required_len).ok_or_else(|| {
                ParquetError::General("LZ4 page size overflows output length".into())
            })?;
            output_buf.try_reserve_exact(required_len).map_err(|e| {
                ParquetError::General(format!("Cannot reserve LZ4 page output: {e}"))
            })?;
            output_buf.resize(end, 0);
            match try_decompress_hadoop(input_buf, &mut output_buf[output_len..]) {
                Ok(n) => {
                    if n != required_len {
                        return Err(ParquetError::General(
                            "LZ4HadoopCodec uncompress_size is not the expected one".into(),
                        ));
                    }
                    Ok(n)
                }
                Err(e) if !self.backward_compatible_lz4 => Err(e.into()),
                // Fallback done to be backward compatible with older versions of this
                // library and older versions of parquet-cpp.
                Err(_) => {
                    // Truncate any inserted element before tryingg next algorithm.
                    output_buf.truncate(output_len);
                    match LZ4Codec::new().decompress(input_buf, output_buf, uncompress_size) {
                        Ok(n) => Ok(n),
                        Err(_) => {
                            // Truncate any inserted element before tryingg next algorithm.
                            output_buf.truncate(output_len);
                            LZ4RawCodec::new().decompress(input_buf, output_buf, uncompress_size)
                        }
                    }
                }
            }
        }

        fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
            // Allocate memory to store the LZ4_HADOOP prefix.
            let offset = output_buf.len();
            output_buf.resize(offset + PREFIX_LEN, 0);

            // Append LZ4_RAW compressed bytes after prefix.
            LZ4RawCodec::new().compress(input_buf, output_buf)?;

            // Prepend decompressed size and compressed size in big endian to be compatible
            // with LZ4_HADOOP.
            let output_buf = &mut output_buf[offset..];
            let compressed_size = output_buf.len() - PREFIX_LEN;
            let compressed_size = compressed_size as u32;
            let uncompressed_size = input_buf.len() as u32;
            output_buf[..SIZE_U32].copy_from_slice(&uncompressed_size.to_be_bytes());
            output_buf[SIZE_U32..PREFIX_LEN].copy_from_slice(&compressed_size.to_be_bytes());

            Ok(())
        }
    }
}
#[cfg(any(feature = "lz4", test))]
pub use lz4_hadoop_codec::*;

#[cfg(test)]
mod zstd_window_tests;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::test_common::rand_gen::random_bytes;

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "run the test binary under a 256 MiB address-space limit"]
    fn stream_keeps_valid_large_page_within_bound() {
        use std::io::Read;

        let length = 128 * 1024 * 1024 + 1;
        let input = std::io::repeat(42).take(length as u64);
        let mut output = Vec::new();
        assert_eq!(
            decompress_stream(input, &mut output, Some(length)).unwrap(),
            length
        );
        assert_eq!(output.len(), length);
        assert_eq!(output[0], 42);
        assert_eq!(output[length - 1], 42);
    }

    #[test]
    fn snappy_rejects_unbacked_output_sizes() {
        let options = CodecOptions::default();
        let mut codec = create_codec(CodecType::SNAPPY, &options).unwrap().unwrap();
        let mut output = Vec::new();

        let encoded = snap::raw::Encoder::new().compress_vec(&[42]).unwrap();
        assert!(
            codec
                .decompress(&encoded, &mut output, Some(1 << 30))
                .is_err()
        );
        assert!(output.is_empty());

        // A matching in-band Snappy length is also file-controlled. A tiny
        // stream cannot back the claimed output even when both lengths agree.
        let forged = [0x80, 0x80, 0x80, 0x80, 0x04, 0x00, 42];
        assert_eq!(snap::raw::decompress_len(&forged).unwrap(), 1 << 30);
        assert!(
            codec
                .decompress(&forged, &mut output, Some(1 << 30))
                .is_err()
        );
        assert!(output.is_empty());

        // Padding makes a false claim fit a ratio-only bound. The compressed
        // tokens still cannot produce the advertised output.
        let mut padded = vec![0x80, 0x80, 0x40]; // 1 MiB Snappy length
        padded.resize(50_000, 0);
        assert!(
            codec
                .decompress(&padded, &mut output, Some(1 << 20))
                .is_err()
        );
        assert!(output.is_empty());
    }

    #[test]
    fn snappy_keeps_valid_high_compression_and_output_prefix() {
        let options = CodecOptions::default();
        let mut codec = create_codec(CodecType::SNAPPY, &options).unwrap().unwrap();
        let source = vec![42; 1024 * 1024];
        let encoded = snap::raw::Encoder::new().compress_vec(&source).unwrap();
        let mut output = vec![7, 8, 9];

        assert_eq!(
            codec
                .decompress(&encoded, &mut output, Some(source.len()))
                .unwrap(),
            source.len()
        );
        assert_eq!(&output[..3], &[7, 8, 9]);
        assert_eq!(&output[3..], source);
    }

    #[test]
    fn snappy_accepts_valid_four_byte_copy_offset() {
        // Five output bytes: literal 'a', then a four-byte copy of offset 1.
        // A copy-4 token carries four offset bytes even when high bytes are 0.
        let encoded = [5, 0, b'a', 0x0f, 1, 0, 0, 0];
        assert_eq!(
            snap::raw::Decoder::new().decompress_vec(&encoded).unwrap(),
            b"aaaaa"
        );
        let options = CodecOptions::default();
        let mut codec = create_codec(CodecType::SNAPPY, &options).unwrap().unwrap();
        let mut output = Vec::new();
        assert_eq!(codec.decompress(&encoded, &mut output, Some(5)).unwrap(), 5);
        assert_eq!(output, b"aaaaa");
    }

    #[test]
    fn lz4_codecs_reject_unbacked_output_sizes() {
        let options = CodecOptions::default();
        for kind in [CodecType::LZ4_RAW, CodecType::LZ4] {
            let mut codec = create_codec(kind, &options).unwrap().unwrap();
            let mut output = Vec::new();
            assert!(codec.decompress(&[0], &mut output, Some(1 << 30)).is_err());
            assert!(output.is_empty());

            let padded = vec![0; 4096];
            assert!(
                codec
                    .decompress(&padded, &mut output, Some(1 << 20))
                    .is_err()
            );
            assert!(output.is_empty());
        }
    }

    #[test]
    fn lz4_codecs_keep_valid_high_compression_and_output_prefix() {
        let options = CodecOptions::default();
        let source = vec![42; 1024 * 1024];
        for kind in [CodecType::LZ4_RAW, CodecType::LZ4] {
            let mut encoder = create_codec(kind, &options).unwrap().unwrap();
            let mut decoder = create_codec(kind, &options).unwrap().unwrap();
            let mut encoded = Vec::new();
            encoder.compress(&source, &mut encoded).unwrap();
            let mut output = vec![7, 8, 9];
            assert_eq!(
                decoder
                    .decompress(&encoded, &mut output, Some(source.len()))
                    .unwrap(),
                source.len()
            );
            assert_eq!(&output[..3], &[7, 8, 9]);
            assert_eq!(&output[3..], source);
        }
    }

    #[test]
    fn lz4_compatibility_fallback_restores_output_on_frame_error() {
        let source = random_bytes(128 * 1024);
        let mut frame = Vec::new();
        lz4_codec::LZ4Codec::new()
            .compress(&source, &mut frame)
            .unwrap();
        assert!(frame.len() > 100_000);
        frame.truncate(frame.len() - 10_000); // cut the second encoded block

        let options = CodecOptions::default();
        let mut codec = create_codec(CodecType::LZ4, &options).unwrap().unwrap();
        let mut output = vec![7, 8, 9];
        assert!(
            codec
                .decompress(&frame, &mut output, Some(source.len()))
                .is_err()
        );
        assert_eq!(output, [7, 8, 9]);
    }

    #[test]
    fn streaming_codecs_reject_output_beyond_page_size() {
        let options = CodecOptions::default();
        let source = vec![42; 1024 * 1024];
        for kind in [
            CodecType::BROTLI(BrotliLevel::default()),
            CodecType::GZIP(GzipLevel::default()),
            CodecType::ZSTD(ZstdLevel::default()),
        ] {
            let mut encoder = create_codec(kind, &options).unwrap().unwrap();
            let mut decoder = create_codec(kind, &options).unwrap().unwrap();
            let mut encoded = Vec::new();
            encoder.compress(&source, &mut encoded).unwrap();
            let mut output = vec![7, 8, 9];
            assert!(decoder.decompress(&encoded, &mut output, Some(1)).is_err());
            assert!(output.len() <= 4);
        }

        let mut encoded_frame = Vec::new();
        lz4_codec::LZ4Codec::new()
            .compress(&source, &mut encoded_frame)
            .unwrap();
        let mut decoder = create_codec(CodecType::LZ4, &options).unwrap().unwrap();
        let mut output = vec![7, 8, 9];
        assert!(
            decoder
                .decompress(&encoded_frame, &mut output, Some(1))
                .is_err()
        );
        assert!(output.len() <= 4);
    }

    #[test]
    fn streaming_codecs_keep_valid_high_compression_and_prefix() {
        let options = CodecOptions::default();
        let source = vec![42; 1024 * 1024];
        for kind in [
            CodecType::BROTLI(BrotliLevel::default()),
            CodecType::GZIP(GzipLevel::default()),
            CodecType::ZSTD(ZstdLevel::default()),
        ] {
            let mut encoder = create_codec(kind, &options).unwrap().unwrap();
            let mut decoder = create_codec(kind, &options).unwrap().unwrap();
            let mut encoded = Vec::new();
            encoder.compress(&source, &mut encoded).unwrap();
            let mut output = vec![7, 8, 9];
            assert_eq!(
                decoder
                    .decompress(&encoded, &mut output, Some(source.len()))
                    .unwrap(),
                source.len()
            );
            assert_eq!(&output[..3], &[7, 8, 9]);
            assert_eq!(&output[3..], source);
        }

        let mut encoded_frame = Vec::new();
        lz4_codec::LZ4Codec::new()
            .compress(&source, &mut encoded_frame)
            .unwrap();
        let mut decoder = create_codec(CodecType::LZ4, &options).unwrap().unwrap();
        let mut output = vec![7, 8, 9];
        assert_eq!(
            decoder
                .decompress(&encoded_frame, &mut output, Some(source.len()))
                .unwrap(),
            source.len()
        );
        assert_eq!(&output[..3], &[7, 8, 9]);
        assert_eq!(&output[3..], source);
    }

    fn test_roundtrip(c: CodecType, data: &[u8], uncompress_size: Option<usize>) {
        let codec_options = CodecOptionsBuilder::default()
            .set_backward_compatible_lz4(false)
            .build();
        let mut c1 = create_codec(c, &codec_options).unwrap().unwrap();
        let mut c2 = create_codec(c, &codec_options).unwrap().unwrap();

        // Compress with c1
        let mut compressed = Vec::new();
        let mut decompressed = Vec::new();
        c1.compress(data, &mut compressed)
            .expect("Error when compressing");

        // Decompress with c2
        let decompressed_size = c2
            .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
            .expect("Error when decompressing");
        assert_eq!(data.len(), decompressed_size);
        assert_eq!(data, decompressed.as_slice());

        decompressed.clear();
        compressed.clear();

        // Compress with c2
        c2.compress(data, &mut compressed)
            .expect("Error when compressing");

        // Decompress with c1
        let decompressed_size = c1
            .decompress(compressed.as_slice(), &mut decompressed, uncompress_size)
            .expect("Error when decompressing");
        assert_eq!(data.len(), decompressed_size);
        assert_eq!(data, decompressed.as_slice());

        decompressed.clear();
        compressed.clear();

        // Test does not trample existing data in output buffers
        let prefix = &[0xDE, 0xAD, 0xBE, 0xEF];
        decompressed.extend_from_slice(prefix);
        compressed.extend_from_slice(prefix);

        c2.compress(data, &mut compressed)
            .expect("Error when compressing");

        assert_eq!(&compressed[..4], prefix);

        let decompressed_size = c2
            .decompress(&compressed[4..], &mut decompressed, uncompress_size)
            .expect("Error when decompressing");

        assert_eq!(data.len(), decompressed_size);
        assert_eq!(data, &decompressed[4..]);
        assert_eq!(&decompressed[..4], prefix);
    }

    fn test_codec_with_size(c: CodecType) {
        let sizes = vec![100, 10000, 100000];
        for size in sizes {
            let data = random_bytes(size);
            test_roundtrip(c, &data, Some(data.len()));
        }
    }

    fn test_codec_without_size(c: CodecType) {
        let sizes = vec![100, 10000, 100000];
        for size in sizes {
            let data = random_bytes(size);
            test_roundtrip(c, &data, None);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_codec_snappy() {
        test_codec_with_size(CodecType::SNAPPY);
        test_codec_without_size(CodecType::SNAPPY);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_codec_gzip() {
        for level in GzipLevel::MINIMUM_LEVEL..=GzipLevel::MAXIMUM_LEVEL {
            let level = GzipLevel::try_new(level).unwrap();
            test_codec_with_size(CodecType::GZIP(level));
            test_codec_without_size(CodecType::GZIP(level));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_codec_brotli() {
        for level in BrotliLevel::MINIMUM_LEVEL..=BrotliLevel::MAXIMUM_LEVEL {
            let level = BrotliLevel::try_new(level).unwrap();
            test_codec_with_size(CodecType::BROTLI(level));
            test_codec_without_size(CodecType::BROTLI(level));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_codec_lz4() {
        test_codec_with_size(CodecType::LZ4);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Zstd calls native C functions unsupported by Miri
    fn test_codec_zstd() {
        // since ZstdLevel::MINIMUM_LEVEL is a large negative number, we test a smaller range
        for level in
            std::iter::once(ZstdLevel::MINIMUM_LEVEL).chain(-100..=ZstdLevel::MAXIMUM_LEVEL)
        {
            let level = ZstdLevel::try_new(level).unwrap();
            test_codec_with_size(CodecType::ZSTD(level));
            test_codec_without_size(CodecType::ZSTD(level));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Takes too long
    fn test_codec_lz4_raw() {
        test_codec_with_size(CodecType::LZ4_RAW);
    }
}
