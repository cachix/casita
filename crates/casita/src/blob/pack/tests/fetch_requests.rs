use super::*;

#[tokio::test]
async fn planned_requests_preserve_consumer_order_duplicates_and_cached_frames() {
    use object_store::memory::InMemory;

    let objects = Arc::new(InMemory::new());
    let packed = PackedChunks::open_with_cache(objects.clone(), Path::default(), 256 * 1024, 128)
        .await
        .unwrap();
    let packs = [9, 3, 7].map(|n| PackId::new(Digest::from([n; 32])));
    for (pack, bytes) in packs
        .iter()
        .zip([&b"aaaabbbb"[..], &b"cccc"[..], &b"xxddddxx"[..]])
    {
        objects
            .put(
                &pack_path(&Path::default(), pack),
                Bytes::copy_from_slice(bytes).into(),
            )
            .await
            .unwrap();
    }
    let chunks: Vec<_> = [1, 2, 3, 4, 1, 5]
        .into_iter()
        .map(|n| ChunkMeta {
            digest: ChunkId::new(Digest::from([n; 32])),
            size: 4,
        })
        .collect();
    let frozen = [
        (1, 0, 4, 8),
        (2, 1, 0, 4),
        (3, 0, 0, 8),
        (4, 2, 2, 8),
        (5, 1, 0, 4),
    ]
    .into_iter()
    .map(|(n, pack, offset, pack_len)| {
        (
            ChunkId::new(Digest::from([n; 32])),
            FrozenChunk(Location {
                pack: packs[pack],
                pack_len,
                offset,
                framed_len: 4,
                uncompressed_len: 4,
            }),
        )
    })
    .collect();
    packed
        .fetch
        .cache
        .lock()
        .unwrap()
        .insert(chunks[5].digest, b"warm");
    let context = Arc::new(Context {
        packed: packed.reader(),
        plan: ReadPlan {
            chunks: chunks.clone(),
            frozen,
            _pin: None,
        },
        decode: ByteBudget::new(BUFFER_BYTES),
    });
    let frames = context.demand(0..chunks.len()).await.unwrap();
    assert_eq!(frames.len(), chunks.len());
    for ((frame, chunk), expected) in frames
        .iter()
        .zip(&chunks)
        .zip([b"bbbb", b"cccc", b"aaaa", b"dddd", b"bbbb", b"warm"])
    {
        assert_eq!(frame.chunk.digest, chunk.digest);
        assert_eq!(frame.bytes.as_ref(), expected);
    }
    assert!(frames[..5].iter().all(|frame| frame.missed));
    assert!(!frames[5].missed);
    let stats = packed.read_stats();
    assert_eq!(stats.chunk_range_requests, 3);
    assert_eq!(stats.chunk_range_bytes, 16);
    assert_eq!(packed.fetch.requests.available_permits(), 4);
    drop(frames);
    assert!(packed.fetch.buffers.try_reserve(BUFFER_BYTES).is_some());
}
