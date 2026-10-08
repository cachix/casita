//! Portable bounded metadata verification, with the previous helper as a control.
use casita::ObjectKey;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Bounded metadata materialization uses the same path as directory verification.
/// Include empty payloads, both sides of its 64 KiB scratch-buffer boundary, and
/// a one-byte hint that understates larger payloads and forces scratch growth.
fn metadata_verification_buffers(c: &mut Criterion) {
    use casita::experimental::{BlobId, FormatError, PayloadReader, VerificationContext};

    // A rejected alternative: read known payloads up to 64 KiB straight into
    // the output, then probe EOF, saving the copy from scratch space.
    async fn hybrid_metadata(
        context: &mut VerificationContext<'_>,
        limit: u64,
    ) -> Result<Vec<u8>, FormatError> {
        // lib.rs allows this lint crate-wide; benches compile as separate crates.
        #[allow(clippy::result_large_err)]
        fn append(output: &mut Vec<u8>, bytes: &[u8], limit: u64) -> Result<(), FormatError> {
            let length = output
                .len()
                .checked_add(bytes.len())
                .ok_or(FormatError::PayloadSizeOverflow)?;
            if length as u64 > limit {
                return Err(FormatError::MetadataLimit { limit });
            }
            if length > output.capacity() {
                let capacity = output
                    .capacity()
                    .saturating_mul(2)
                    .max(length)
                    .min(usize::try_from(limit).unwrap_or(usize::MAX));
                output.reserve_exact(capacity - output.len());
            }
            output.extend_from_slice(bytes);
            Ok(())
        }
        const MAX_READ: u64 = 64 * 1024;
        let mut output = Vec::new();
        if let Some(length) = context.exact_len().filter(|length| *length <= MAX_READ) {
            output = vec![0; length.min(limit) as usize];
            let mut used = 0;
            while used < output.len() {
                let read = context.read(&mut output[used..]).await?;
                if read == 0 {
                    output.truncate(used);
                    return Ok(output);
                }
                used += read;
            }
            let mut probe = [0];
            if context.read(&mut probe).await? == 0 {
                return Ok(output);
            }
            append(&mut output, &probe, limit)?;
        }
        let scratch = MAX_READ.min(limit.saturating_sub(output.len() as u64).saturating_add(1));
        let mut buffer = vec![0; scratch as usize];
        loop {
            let read = context.read(&mut buffer).await?;
            if read == 0 {
                return Ok(output);
            }
            append(&mut output, &buffer[..read], limit)?;
        }
    }

    // The former production helper, retained as a same-binary control.
    async fn scratch_metadata(
        context: &mut VerificationContext<'_>,
        limit: u64,
    ) -> Result<Vec<u8>, FormatError> {
        let mut output = Vec::new();
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = context.read(&mut buffer).await?;
            if read == 0 {
                return Ok(output);
            }
            let new_len = (output.len() as u64)
                .checked_add(read as u64)
                .ok_or(FormatError::PayloadSizeOverflow)?;
            if new_len > limit {
                return Err(FormatError::MetadataLimit { limit });
            }
            output.extend_from_slice(&buffer[..read]);
        }
    }

    struct Reader<'a> {
        bytes: &'a [u8],
        length: Option<u64>,
        largest_buffer: usize,
        eof: bool,
    }

    #[async_trait::async_trait]
    impl PayloadReader for Reader<'_> {
        fn exact_len(&self) -> Option<u64> {
            self.length
        }

        async fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            assert!(!buffer.is_empty());
            self.largest_buffer = self.largest_buffer.max(buffer.len());
            let count = std::io::Read::read(&mut self.bytes, buffer)?;
            self.eof |= count == 0;
            Ok(count)
        }
    }

    const LIMIT: u64 = 256 * 1024 * 1024;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("metadata_verification_buffers");
    group.sample_size(10);
    for size in [0, 1, 128, 1024, 16384, 65535, 65536, 65537, 262144] {
        let mut data = vec![0; size];
        blake3::Hasher::new()
            .update(b"metadata-verification-benchmark")
            .finalize_xof()
            .fill(&mut data);
        let digest = blake3::hash(&data).into();
        let key = ObjectKey::blob(BlobId::new(digest));
        group.throughput(Throughput::Bytes(size as u64));
        let mut hints = vec![("known", Some(size as u64)), ("unknown", None)];
        if size > 1 {
            hints.push(("understated", Some(1)));
        }
        for (kind, length) in hints {
            let mut implementations = ["scratch_64k", "hybrid", "production"];
            if std::env::var_os("CASITA_METADATA_READ_REVERSE").is_some() {
                implementations.reverse();
            }
            for implementation in implementations {
                let read = || async {
                    let mut reader = Reader {
                        bytes: &data,
                        length,
                        largest_buffer: 0,
                        eof: false,
                    };
                    let mut context = VerificationContext::new(&key, &mut reader);
                    let decoded = match implementation {
                        "scratch_64k" => scratch_metadata(&mut context, LIMIT).await,
                        "hybrid" => hybrid_metadata(&mut context, LIMIT).await,
                        _ => context.read_to_end_bounded(LIMIT).await,
                    }
                    .unwrap();
                    assert_eq!(decoded, data);
                    assert_eq!(context.observed_digest(), digest);
                    assert_eq!(
                        context.finish(Vec::new()).unwrap().record().payload_size(),
                        size as u64
                    );
                    assert!(reader.eof);
                    assert!(reader.largest_buffer <= 65536);
                    if implementation == "production" && kind == "known" {
                        // An accurate hint sizes scratch space to the payload.
                        assert_eq!(reader.largest_buffer, size.clamp(1, 65536));
                    }
                    reader.largest_buffer
                };
                let largest_buffer = runtime.block_on(read());
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "case": format!("metadata_verification_buffers/{kind}/{size}/{implementation}"),
                        "largest_read_buffer_bytes": largest_buffer, "correctness": "passed",
                    })
                );
                group.bench_function(
                    BenchmarkId::new(format!("{kind}/{size}"), implementation),
                    |b| b.to_async(&runtime).iter(read),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, metadata_verification_buffers);
criterion_main!(benches);
