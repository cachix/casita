//! An [`AsyncRead`] wrapper that derives the ordinary BLAKE3 root and Bao
//! outboard during chunking. EOF publishes the root for whole-chunk hash reuse.

use std::io;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use crate::digest::BlobId;
use crate::verified::ingest::{IngestHasher, OutboardData};

/// Published only after a read with spare buffer capacity observes EOF. The chunker can
/// inspect this while it still holds a mutable borrow of the hashing reader.
#[derive(Default)]
pub(crate) struct HashCompletion(OnceLock<(BlobId, u64, u64)>);

impl HashCompletion {
    /// The blob identity and outboard length are known together at EOF.
    pub(crate) fn single_blob(&self, offset: u64, length: usize) -> Option<(BlobId, u64)> {
        let &(digest, size, outboard_len) = self.0.get()?;
        (offset == 0 && length as u64 == size).then_some((digest, outboard_len))
    }
}

pub(crate) struct HashingReader<'a, R> {
    inner: R,
    hasher: Option<IngestHasher>,
    finished: Option<(BlobId, OutboardData)>,
    size: u64,
    completion: &'a HashCompletion,
}

impl<'a, R> HashingReader<'a, R> {
    pub(crate) fn new(inner: R, completion: &'a HashCompletion) -> Self {
        Self {
            inner,
            hasher: Some(IngestHasher::default()),
            finished: None,
            size: 0,
            completion,
        }
    }

    /// Bytes read from the source so far.
    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    /// Consume the accumulated hash and metadata, reusing EOF finalization.
    pub(crate) fn finish(self) -> io::Result<(BlobId, OutboardData)> {
        if let Some(finished) = self.finished {
            Ok(finished)
        } else {
            self.hasher
                .ok_or_else(|| io::Error::other("ingestion hash failed"))?
                .finish()
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for HashingReader<'_, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Zero-capacity reads do not prove EOF. A completed source stays fused.
        if buf.remaining() == 0 || this.finished.is_some() {
            return Poll::Ready(Ok(()));
        }
        let Some(hasher) = this.hasher.as_mut() else {
            return Poll::Ready(Err(io::Error::other("ingestion hash failed")));
        };
        let before = buf.filled().len();
        let res = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &res {
            let bytes = &buf.filled()[before..];
            if bytes.is_empty() {
                let result = this.hasher.take().expect("hasher is present").finish();
                match result {
                    Ok((digest, outboard)) => {
                        this.completion
                            .0
                            .get_or_init(|| (digest, this.size, outboard.len));
                        this.finished = Some((digest, outboard));
                    }
                    Err(error) => return Poll::Ready(Err(error)),
                }
            } else {
                let result = hasher.update(bytes).and_then(|()| {
                    this.size = this
                        .size
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| io::Error::other("BLAKE3 input exceeds size limit"))?;
                    Ok(())
                });
                if let Err(error) = result {
                    this.hasher = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn eof_hash_reuse_retains_the_reference_outboard() {
        for size in [0, 1, 16383, 16384, 16385, 131071, 131072, 131073, 524289] {
            let bytes: Vec<_> = (0..size).map(|i| (i * 17) as u8).collect();
            let completion = HashCompletion::default();
            let mut reader = HashingReader::new(bytes.as_slice(), &completion);
            let mut output = Vec::new();
            reader.read_to_end(&mut output).await.unwrap();
            assert_eq!(output, bytes);
            assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
            let expected = blake3::hash(&bytes);
            assert_eq!(
                completion.single_blob(0, size).map(|(blob, _)| blob),
                Some(BlobId::new(expected.into()))
            );
            let (digest, mut outboard) = reader.finish().unwrap();
            assert_eq!(digest, BlobId::new(expected.into()));
            assert_eq!(
                completion.single_blob(0, size),
                Some((digest, outboard.len))
            );
            let (reference, _) = crate::verified::build_outboard(bytes::Bytes::from(bytes))
                .await
                .unwrap();
            let mut encoded = Vec::new();
            std::io::Read::read_to_end(&mut outboard.file, &mut encoded).unwrap();
            assert_eq!(encoded, reference, "size={size}");
        }
    }

    #[tokio::test]
    async fn only_eof_proves_a_whole_chunk() {
        let bytes = b"a complete payload";
        let completion = HashCompletion::default();
        let mut reader = HashingReader::new(bytes.as_slice(), &completion);
        let mut output = vec![0; bytes.len()];

        reader.read_exact(&mut []).await.unwrap();
        assert_eq!(completion.single_blob(0, 0), None);
        reader.read_exact(&mut output).await.unwrap();
        assert_eq!(&output, bytes);
        // Consuming exactly the known input length still does not observe EOF.
        assert_eq!(completion.single_blob(0, bytes.len()), None);
        assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
        let expected = BlobId::new(blake3::hash(bytes).into());
        assert_eq!(
            completion.single_blob(0, bytes.len()).map(|(blob, _)| blob),
            Some(expected)
        );
        assert_eq!(completion.single_blob(1, bytes.len()), None);
        assert_eq!(completion.single_blob(0, bytes.len() - 1), None);
        assert_eq!(completion.single_blob(0, bytes.len() + 1), None);
        assert_eq!(reader.finish().unwrap().0.digest(), expected.digest());
    }

    #[tokio::test]
    async fn pending_and_full_read_buffers_do_not_publish_completion() {
        use tokio::io::AsyncWriteExt;
        let completion = HashCompletion::default();
        let (mut writer, source) = tokio::io::duplex(64);
        let mut reader = HashingReader::new(source, &completion);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut bytes = [0; 16];
        let mut buffer = ReadBuf::new(&mut bytes);
        buffer.put_slice(b"existing prefix");
        assert!(
            Pin::new(&mut reader)
                .poll_read(&mut cx, &mut buffer)
                .is_pending()
        );
        assert_eq!(completion.single_blob(0, 0), None);
        buffer.put_slice(b"!");
        assert!(matches!(
            Pin::new(&mut reader).poll_read(&mut cx, &mut buffer),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(completion.single_blob(0, 0), None);
        writer.write_all(b"x").await.unwrap();
        drop(writer);
        // Only newly read bytes belong in the hash, even with a filled prefix.
        let mut bytes = [0; 4];
        let mut buffer = ReadBuf::new(&mut bytes);
        buffer.put_slice(b"old");
        std::future::poll_fn(|cx| Pin::new(&mut reader).poll_read(cx, &mut buffer))
            .await
            .unwrap();
        assert_eq!(buffer.filled(), b"oldx");
        assert_eq!(reader.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(
            completion.single_blob(0, 1).map(|(blob, _)| blob),
            Some(BlobId::new(blake3::hash(b"x").into()))
        );
    }

    struct ReadError;

    impl AsyncRead for ReadError {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("injected read failure")))
        }
    }

    #[tokio::test]
    async fn read_failure_does_not_certify_a_partial_payload() {
        let completion = HashCompletion::default();
        let source = b"partial".as_slice().chain(ReadError);
        let mut reader = HashingReader::new(source, &completion);
        let mut bytes = Vec::new();
        assert!(reader.read_to_end(&mut bytes).await.is_err());
        assert_eq!(bytes, b"partial");
        assert_eq!(completion.single_blob(0, bytes.len()), None);
    }

    #[tokio::test]
    async fn chunker_reuses_only_a_complete_first_chunk() {
        use futures::StreamExt;
        let (min, avg, max) = (512, 1024, 2048);
        for size in [0, 511, 512, 513, 1024, 2047, 2048, 2049, 10_000] {
            for seed in [0u64, 83] {
                let mut state = seed;
                let bytes: Vec<_> = (0..size)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        state as u8
                    })
                    .collect();
                let expected: Vec<_> =
                    fastcdc::v2020::FastCDC::new(&bytes, min, avg, max).collect();
                let completion = HashCompletion::default();
                let mut reader = HashingReader::new(bytes.as_slice(), &completion);
                let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(&mut reader, min, avg, max);
                let stream = chunker.as_stream();
                futures::pin_mut!(stream);
                let mut count = 0;
                let mut reused = 0;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.unwrap();
                    assert_eq!(chunk.offset, expected[count].offset as u64);
                    assert_eq!(chunk.length, expected[count].length);
                    if let Some(digest) = completion
                        .single_blob(chunk.offset, chunk.length)
                        .map(|(blob, _)| blob)
                    {
                        assert_eq!(digest, BlobId::new(blake3::hash(&chunk.data).into()));
                        assert_eq!(chunk.data, bytes);
                        reused += 1;
                    }
                    count += 1;
                }
                assert_eq!(count, expected.len());
                assert_eq!(
                    reused,
                    usize::from(count == 1 && size < max),
                    "size={size}, seed={seed}"
                );
            }
        }
    }
}
