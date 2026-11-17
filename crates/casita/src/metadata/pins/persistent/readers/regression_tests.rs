use super::*;

struct Fixture {
    store: FilePinStore,
    local: Arc<super::super::group::Local>,
    token: PinToken,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = FilePinStore::new(directory.path().join("pins"));
        let local = store.local().unwrap();
        let token = store.register_reader(snapshot()).await.unwrap().unwrap();
        assert!(store.inventory().await.unwrap().pins.contains_key(&token));
        assert!(local.reader_cache.lock().unwrap().is_some());
        Self {
            store,
            local,
            token,
            _directory: directory,
        }
    }

    async fn populate_by_read(&self) {
        *self.local.reader_cache.lock().unwrap() = None;
        assert!(
            self.store
                .inventory()
                .await
                .unwrap()
                .pins
                .contains_key(&self.token)
        );
        assert!(self.local.reader_cache.lock().unwrap().is_some());
    }

    fn bytes(&self) -> Vec<u8> {
        std::fs::read(self.store.reader_path()).unwrap()
    }

    fn overwrite(&self, bytes: &[u8]) {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(self.store.reader_path())
            .unwrap();
        file.write_all(bytes).unwrap();
    }

    fn corrupt(&self, offset: usize) -> Vec<u8> {
        let mut bytes = self.bytes();
        bytes[offset] ^= 1;
        self.overwrite(&bytes);
        assert!(ReaderState::decode(&bytes).is_err());
        assert_eq!(self.bytes(), bytes);
        bytes
    }

    fn identity(&self) -> ReaderFileIdentity {
        ReaderFileIdentity::of(&std::fs::File::open(self.store.reader_path()).unwrap()).unwrap()
    }

    fn model_metadata_collision(&self) {
        // Filesystem clocks need not distinguish two same-inode, same-length
        // writes. Model that collision without changing clocks or checksums.
        let identity = self.identity();
        let mut cache = self.local.reader_cache.lock().unwrap();
        cache.as_mut().unwrap().identity = identity;
    }
}

fn snapshot() -> DataPin {
    DataPin {
        scope: PinScope::Snapshot { generation: 42 },
        catalog: None,
        resources: BTreeSet::new(),
    }
}

#[tokio::test]
async fn reader_inventory_detects_corruption_after_metadata_identity_collision() {
    for populate_by_read in [false, true] {
        for part in 0..3 {
            let fixture = Fixture::new().await;
            if populate_by_read {
                fixture.populate_by_read().await;
            }
            let length = fixture.bytes().len();
            let offset = [8, length - 33, length - 1][part];
            let corrupt = fixture.corrupt(offset);
            fixture.model_metadata_collision();

            assert!(
                fixture.store.inventory().await.is_err(),
                "cached inventory accepted corrupt bytes after an identity collision"
            );
            assert_eq!(fixture.bytes(), corrupt);
        }
    }
}

enum Mutation {
    Register,
    Protect,
    Release,
}

async fn rejects_corrupt_mutation(mutation: Mutation) {
    for populate_by_read in [false, true] {
        let fixture = Fixture::new().await;
        if populate_by_read {
            fixture.populate_by_read().await;
        }
        let durable = std::fs::read(&fixture.store.path).unwrap();
        let corrupt = fixture.corrupt(8);
        fixture.model_metadata_collision();
        let result = match mutation {
            Mutation::Register => fixture.store.register_reader(snapshot()).await.map(|_| ()),
            Mutation::Protect => fixture
                .store
                .protect(
                    &fixture.token,
                    BTreeSet::from([PinResource::StorageObject("pack".into())]),
                )
                .await
                .map(|_| ()),
            Mutation::Release => fixture.store.release(&fixture.token).await,
        };

        assert!(
            result.is_err(),
            "mutation republished corrupt cached inventory"
        );
        assert_eq!(fixture.bytes(), corrupt);
        assert_eq!(std::fs::read(&fixture.store.path).unwrap(), durable);
    }
}

#[tokio::test]
async fn reader_registration_does_not_republish_corrupt_cached_inventory() {
    rejects_corrupt_mutation(Mutation::Register).await;
}

#[tokio::test]
async fn reader_protection_does_not_republish_corrupt_cached_inventory() {
    rejects_corrupt_mutation(Mutation::Protect).await;
}

#[tokio::test]
async fn reader_release_does_not_republish_corrupt_cached_inventory() {
    rejects_corrupt_mutation(Mutation::Release).await;
}

#[tokio::test]
async fn reader_inventory_observes_valid_in_place_change_after_identity_collision() {
    let fixture = Fixture::new().await;
    let original = fixture.bytes();
    let mut state = ReaderState::decode(&original).unwrap();
    state.revision += 1;
    for owner in state.owners.values_mut() {
        owner.pins.get_mut(&fixture.token).unwrap().scope = PinScope::Snapshot { generation: 43 };
    }
    let replacement = state.encode().unwrap();
    assert_eq!(replacement.len(), original.len());
    fixture.overwrite(&replacement);
    fixture.model_metadata_collision();

    let inventory = fixture.store.inventory().await.unwrap();
    assert_eq!(inventory.revision, state.revision);
    assert_eq!(
        inventory.pins[&fixture.token].scope,
        PinScope::Snapshot { generation: 43 }
    );
}

#[tokio::test]
async fn uncached_reader_inventory_rejects_the_same_corruption() {
    let fixture = Fixture::new().await;
    let corrupt = fixture.corrupt(8);
    *fixture.local.reader_cache.lock().unwrap() = None;

    assert!(fixture.store.inventory().await.is_err());
    assert_eq!(fixture.bytes(), corrupt);
}

#[tokio::test]
async fn reader_inventory_does_not_accept_rapid_in_place_corruption() {
    let mut equal_identity = 0;
    let mut accepted = 0;
    let mut accepted_without_equal_identity = 0;
    for _ in 0..32 {
        let fixture = Fixture::new().await;
        let before = fixture.identity();
        fixture.corrupt(8);
        let equal = fixture.identity() == before;
        equal_identity += usize::from(equal);
        if fixture.store.inventory().await.is_ok() {
            accepted += 1;
            accepted_without_equal_identity += usize::from(!equal);
        }
    }
    println!(
        "reader_cache_corruption_observations {}",
        serde_json::json!({
            "samples": 32,
            "equal_file_identity": equal_identity,
            "accepted_corruption": accepted,
            "accepted_without_equal_identity": accepted_without_equal_identity,
        })
    );
    assert_eq!(
        accepted, 0,
        "rapid in-place corruption was accepted by the inventory cache"
    );
}
