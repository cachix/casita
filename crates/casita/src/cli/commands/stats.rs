//! Physical payload-store statistics printed by command workflows.

pub(super) fn print_pack_stats(payloads: &casita::experimental::ChunkedBlobStore) {
    print_pack_stats_with_prefix(payloads, "");
}

pub(super) fn print_pack_stats_with_prefix(
    payloads: &casita::experimental::ChunkedBlobStore,
    prefix: &str,
) {
    if std::env::var_os("CASITA_PACK_STATS").is_none() {
        return;
    }
    let Some(stats) = payloads.pack_read_stats() else {
        return;
    };
    println!("{prefix}pack-list-requests {}", stats.list_requests);
    println!(
        "{prefix}pack-gc-manifest-list-requests {}",
        stats.gc_manifest_list_requests
    );
    println!(
        "{prefix}pack-gc-loose-chunk-list-requests {}",
        stats.gc_loose_chunk_list_requests
    );
    println!(
        "{prefix}pack-footer-range-requests {}",
        stats.footer_range_requests
    );
    println!(
        "{prefix}pack-footer-range-bytes {}",
        stats.footer_range_bytes
    );
    println!(
        "{prefix}pack-chunk-range-requests {}",
        stats.chunk_range_requests
    );
    println!("{prefix}pack-chunk-range-bytes {}", stats.chunk_range_bytes);
    println!("{prefix}pack-whole-requests {}", stats.whole_pack_requests);
    println!("{prefix}pack-whole-bytes {}", stats.whole_pack_bytes);
    println!("{prefix}pack-cache-hits {}", stats.cache_hits);
    println!("{prefix}pack-cache-promotions {}", stats.cache_promotions);
    println!("{prefix}pack-cache-evictions {}", stats.cache_evictions);
    println!(
        "{prefix}pack-gc-replacement-put-requests {}",
        stats.gc_replacement_put_requests
    );
    println!(
        "{prefix}pack-gc-replacement-put-bytes {}",
        stats.gc_replacement_put_bytes
    );
    println!(
        "{prefix}pack-gc-marker-put-requests {}",
        stats.gc_marker_put_requests
    );
    println!(
        "{prefix}pack-gc-marker-put-bytes {}",
        stats.gc_marker_put_bytes
    );
    println!(
        "{prefix}pack-gc-delete-requests {}",
        stats.gc_pack_delete_requests
    );
    println!(
        "{prefix}pack-gc-manifest-delete-requests {}",
        stats.gc_manifest_delete_requests
    );
    println!(
        "{prefix}pack-gc-outboard-delete-requests {}",
        stats.gc_outboard_delete_requests
    );
    println!(
        "{prefix}pack-gc-loose-chunk-delete-requests {}",
        stats.gc_loose_chunk_delete_requests
    );
    println!(
        "{prefix}pack-gc-tombstone-put-requests {}",
        stats.gc_tombstone_put_requests
    );
    println!(
        "{prefix}pack-gc-tombstone-put-bytes {}",
        stats.gc_tombstone_put_bytes
    );
    println!(
        "{prefix}pack-gc-tombstone-delete-requests {}",
        stats.gc_tombstone_delete_requests
    );
    println!("{prefix}pack-gc-deferred-packs {}", stats.gc_deferred_packs);
    println!(
        "{prefix}pack-index-pointer-requests {}",
        stats.index_pointer_requests
    );
    println!("{prefix}pack-index-requests {}", stats.index_requests);
    println!("{prefix}pack-index-bytes {}", stats.index_bytes);
    println!("{prefix}pack-index-hash-nanos {}", stats.index_hash_nanos);
    println!(
        "{prefix}pack-index-decode-nanos {}",
        stats.index_decode_nanos
    );
    println!("{prefix}pack-index-hits {}", stats.index_hits);
    println!("{prefix}pack-index-fallbacks {}", stats.index_fallbacks);
    println!(
        "{prefix}pack-index-put-requests {}",
        stats.index_put_requests
    );
    println!("{prefix}pack-index-put-bytes {}", stats.index_put_bytes);
}
