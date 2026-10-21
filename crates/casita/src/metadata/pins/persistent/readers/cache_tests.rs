//! Cache correctness does not depend on filesystem timestamp granularity.
use super::*;

fn snapshot() -> DataPin {
    DataPin {
        scope: PinScope::Snapshot { generation: 42 },
        catalog: None,
        resources: BTreeSet::new(),
    }
}

// Model an aliased stat result without a global clock mock or sleeping. The
// actual file bytes, kernel owner locks, ledger and public inventory API remain
// real. Only the cached stat witness is aligned with the current descriptor.
fn alias_cached_identity(store: &FilePinStore) {
    let file = std::fs::File::open(store.reader_path()).unwrap();
    let identity = ReaderFileIdentity::of(&file).unwrap();
    let local = store.local().unwrap();
    let mut cache = local.reader_cache.lock().unwrap();
    let cached = cache.as_mut().expect("warm reader cache");
    assert_eq!(cached.identity.device, identity.device);
    assert_eq!(cached.identity.inode, identity.inode);
    assert_eq!(cached.identity.length, identity.length);
    cached.identity = identity;
}

#[tokio::test]
async fn aliased_stamp_corruption_fails_closed_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let token = store.register_reader(snapshot()).await.unwrap().unwrap();
    let revision = store.inventory().await.unwrap().revision;
    let path = store.reader_path();
    let valid = std::fs::read(&path).unwrap();
    let mut corrupted = valid.clone();
    corrupted[8] ^= 1;
    assert_eq!(
        &valid[valid.len() - 32..],
        &corrupted[corrupted.len() - 32..]
    );
    std::fs::write(&path, corrupted).unwrap();
    alias_cached_identity(&store);
    assert!(
        store.inventory().await.is_err(),
        "same-stamp corrupt bytes must not reuse decoded state"
    );
    assert!(store.begin_collection(revision, None).await.is_err());
    std::fs::write(&path, valid).unwrap();
    alias_cached_identity(&store);
    assert!(store.inventory().await.unwrap().pins.contains_key(&token));
    std::fs::remove_file(path).unwrap();
    assert!(store.inventory().await.is_err());
}

#[tokio::test]
async fn aliased_stamp_valid_replacement_is_decoded() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let token = store.register_reader(snapshot()).await.unwrap().unwrap();
    assert!(store.inventory().await.unwrap().pins.contains_key(&token));
    let path = store.reader_path();
    let valid = std::fs::read(&path).unwrap();
    let mut replacement = ReaderState::decode(&valid).unwrap();
    replacement.revision += 1;
    for owner in replacement.owners.values_mut() {
        owner.pins.get_mut(&token).unwrap().scope = PinScope::Snapshot { generation: 43 };
    }
    let bytes = replacement.encode().unwrap();
    assert_eq!(valid.len(), bytes.len());
    std::fs::write(path, bytes).unwrap();
    alias_cached_identity(&store);
    let inventory = store.inventory().await.unwrap();
    assert_eq!(
        inventory.pins[&token].scope,
        PinScope::Snapshot { generation: 43 }
    );
    assert_eq!(inventory.revision, replacement.revision);
}

#[tokio::test]
async fn aliased_stamp_cannot_bypass_revision_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    store.register_reader(snapshot()).await.unwrap().unwrap();
    store.inventory().await.unwrap();
    let path = store.reader_path();
    let mut replacement = ReaderState::decode(&std::fs::read(&path).unwrap()).unwrap();
    replacement.revision = store.read_locked().unwrap().reader_revision_ceiling + 1;
    std::fs::write(path, replacement.encode().unwrap()).unwrap();
    alias_cached_identity(&store);
    assert!(
        store.inventory().await.is_err(),
        "checksum-valid revision still needs its durable fence"
    );
}

#[tokio::test]
async fn warm_cache_rejects_checksum_valid_recovery_records_and_oversize() {
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let token = store.register_reader(snapshot()).await.unwrap().unwrap();
    store.inventory().await.unwrap();
    let path = store.reader_path();
    let valid = std::fs::read(&path).unwrap();
    let mut replacement = ReaderState::decode(&valid).unwrap();
    replacement
        .owners
        .values_mut()
        .next()
        .unwrap()
        .reader_revision_ceiling = 1;
    std::fs::write(&path, replacement.encode().unwrap()).unwrap();
    assert!(store.inventory().await.is_err());
    std::fs::write(&path, &valid).unwrap();
    assert!(store.inventory().await.unwrap().pins.contains_key(&token));
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(codec::MAX_BYTES as u64 + 1)
        .unwrap();
    assert!(store.inventory().await.is_err());
}

#[tokio::test]
async fn rapid_in_place_corruption_never_reuses_stale_inventory() {
    use std::io::{Seek, SeekFrom};
    // Fixed work bound, not retries until green. Every iteration must fail closed.
    let dir = tempfile::tempdir().unwrap();
    let store = FilePinStore::new(dir.path().join("pins"));
    let token = store.register_reader(snapshot()).await.unwrap().unwrap();
    let path = store.reader_path();
    let valid = std::fs::read(&path).unwrap();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    let mut aliases = 0;
    for iteration in 0..256 {
        assert!(store.inventory().await.unwrap().pins.contains_key(&token));
        let before = ReaderFileIdentity::of(&file).unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&[valid[8] ^ 1]).unwrap();
        let after = ReaderFileIdentity::of(&file).unwrap();
        aliases += usize::from(before == after);
        assert!(
            store.inventory().await.is_err(),
            "corrupt inventory accepted at iteration {iteration}; actual stat alias={}",
            before == after
        );
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&valid).unwrap();
    }
    eprintln!("reader_cache_probe iterations=256 actual_stat_aliases={aliases}");
}
