//! Immutable, independently authenticated Bao root packs and a copy-on-write index.
use super::*;

const PACK_MAGIC: &[u8; 8] = b"casitab1";
const LEAF_MAGIC: &[u8; 8] = b"casitabl";
const BRANCH_MAGIC: &[u8; 8] = b"casitabb";
const PACK_LIMIT: usize = 64 * 1024;
const ROOT_LIMIT: usize = 4096;
const LEAF_LIMIT: usize = 128;
const ROW: usize = 72;
const PACK_ROW: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Location {
    pack: Digest,
    offset: u32,
    length: u32,
}

#[derive(Clone)]
pub(super) struct Storage {
    objects: Arc<dyn ObjectStore>,
    base: Path,
}

impl Storage {
    pub(super) fn new(objects: Arc<dyn ObjectStore>, base: Path) -> Self {
        Self { objects, base }
    }
    fn path(&self, kind: &str, digest: &Digest) -> Path {
        sharded_path(&self.base, kind, digest)
    }
    async fn read(&self, kind: &str, digest: Digest, limit: usize) -> io::Result<Bytes> {
        let result =
            self.objects
                .get(&self.path(kind, &digest))
                .await
                .map_err(|error| match error {
                    object_store::Error::NotFound { .. } => {
                        io::Error::other(crate::blob::BlobIntegrityError::BaoMetadata {
                            reason: format!("missing referenced {kind} object {digest}"),
                        })
                    }
                    error => io::Error::other(error),
                })?;
        if result.meta.size > limit as u64 {
            return Err(invalid());
        }
        let bytes = result.bytes().await.map_err(io::Error::other)?;
        if bytes.len() > limit || Digest::from(blake3::hash(&bytes)) != digest {
            return Err(invalid());
        }
        Ok(bytes)
    }
    async fn write(&self, kind: &str, bytes: Bytes) -> io::Result<Digest> {
        let digest = Digest::from(blake3::hash(&bytes));
        put_object(&self.objects, &self.path(kind, &digest), bytes, false)
            .await
            .map_err(io::Error::other)?;
        Ok(digest)
    }
    pub(super) async fn pack(
        &self,
        roots: &BTreeMap<BlobId, Bytes>,
    ) -> io::Result<BTreeMap<BlobId, Location>> {
        let (bytes, rows) = encode_pack(roots)?;
        let pack = self.write("bao-packs", bytes).await?;
        Ok(rows
            .into_iter()
            .map(|(blob, (offset, length))| {
                (
                    blob,
                    Location {
                        pack,
                        offset,
                        length,
                    },
                )
            })
            .collect())
    }
    pub(super) async fn get(&self, blob: BlobId, location: Location) -> io::Result<Bytes> {
        let bytes = self.read("bao-packs", location.pack, PACK_LIMIT).await?;
        let rows = decode_pack(&bytes)?;
        if rows.get(&blob) != Some(&(location.offset, location.length)) {
            return Err(invalid());
        }
        Ok(bytes.slice(location.offset as usize..(location.offset + location.length) as usize))
    }
    async fn node(&self, root: Digest, depth: usize) -> io::Result<Node> {
        decode_node(
            &self
                .read("bao-indexes", root, 16 + LEAF_LIMIT * ROW)
                .await?,
            depth,
        )
    }
    pub(super) async fn lookup(
        &self,
        mut root: Option<Digest>,
        blob: BlobId,
    ) -> io::Result<Option<Location>> {
        for depth in 0..=64 {
            let Some(digest) = root else { return Ok(None) };
            match self.node(digest, depth).await? {
                Node::Leaf(entries) => {
                    for key in entries.keys() {
                        for at in 0..depth {
                            if nibble(*key, at)? != nibble(blob, at)? {
                                return Err(invalid());
                            }
                        }
                    }
                    return Ok(entries.get(&blob).copied());
                }
                Node::Branch(children) => root = children[nibble(blob, depth)?],
            }
        }
        Err(invalid())
    }
    pub(super) async fn update(
        &self,
        root: Option<Digest>,
        changes: BTreeMap<BlobId, Option<Location>>,
        depth: usize,
    ) -> io::Result<Option<Digest>> {
        // Resolve child identities before uploading. All changed immutable
        // nodes can then share pin admission and bounded concurrent I/O; the
        // caller publishes the catalog only after every write is durable.
        let mut writes = Vec::new();
        let updated = self.plan_update(root, changes, depth, &mut writes).await?;
        if updated != root {
            futures::stream::iter(writes)
                .map(|bytes| self.write("bao-indexes", bytes))
                .buffer_unordered(16)
                .try_collect::<Vec<_>>()
                .await?;
        }
        Ok(updated)
    }
    fn plan_update<'a>(
        &'a self,
        root: Option<Digest>,
        changes: BTreeMap<BlobId, Option<Location>>,
        depth: usize,
        writes: &'a mut Vec<Bytes>,
    ) -> futures::future::BoxFuture<'a, io::Result<Option<Digest>>> {
        Box::pin(async move {
            if changes.is_empty() {
                return Ok(root);
            }
            let node = match root {
                Some(root) => self.node(root, depth).await?,
                None => Node::Leaf(BTreeMap::new()),
            };
            let (mut children, changes) = match node {
                Node::Leaf(mut entries) => {
                    let query = *changes.first_key_value().ok_or_else(invalid)?.0;
                    for key in entries.keys() {
                        for at in 0..depth {
                            if nibble(*key, at)? != nibble(query, at)? {
                                return Err(invalid());
                            }
                        }
                    }
                    for (blob, location) in changes {
                        match location {
                            Some(location) => {
                                entries.insert(blob, location);
                            }
                            None => {
                                entries.remove(&blob);
                            }
                        }
                    }
                    if entries.is_empty() {
                        return Ok(None);
                    }
                    if entries.len() <= LEAF_LIMIT {
                        return Ok(Some(queue_node(
                            encode_node(&Node::Leaf(entries), depth)?,
                            writes,
                        )));
                    }
                    (
                        [None; 16],
                        entries.into_iter().map(|(k, v)| (k, Some(v))).collect(),
                    )
                }
                Node::Branch(children) => (*children, changes),
            };
            let mut partitions: [BTreeMap<BlobId, Option<Location>>; 16] = Default::default();
            for (blob, location) in changes {
                partitions[nibble(blob, depth)?].insert(blob, location);
            }
            for (at, changes) in partitions.into_iter().enumerate() {
                if !changes.is_empty() {
                    children[at] = self
                        .plan_update(children[at], changes, depth + 1, writes)
                        .await?;
                }
            }
            if children.iter().all(Option::is_none) {
                return Ok(None);
            }
            Ok(Some(queue_node(
                encode_node(&Node::Branch(Box::new(children)), depth)?,
                writes,
            )))
        })
    }
    pub(super) async fn inventory(
        &self,
        root: Option<Digest>,
    ) -> io::Result<(BTreeMap<BlobId, Location>, HashSet<Path>)> {
        let mut pending: Vec<_> = root
            .into_iter()
            .map(|root| (root, Vec::<usize>::new()))
            .collect();
        let mut entries = BTreeMap::new();
        let mut paths = HashSet::new();
        while let Some((root, prefix)) = pending.pop() {
            let depth = prefix.len();
            if !paths.insert(self.path("bao-indexes", &root)) {
                return Err(invalid());
            }
            match self.node(root, depth).await? {
                Node::Leaf(rows) => {
                    for (blob, location) in rows {
                        for (depth, expected) in prefix.iter().enumerate() {
                            if nibble(blob, depth)? != *expected {
                                return Err(invalid());
                            }
                        }
                        paths.insert(self.path("bao-packs", &location.pack));
                        if entries.insert(blob, location).is_some() {
                            return Err(invalid());
                        }
                    }
                }
                Node::Branch(children) => pending.extend(
                    (*children)
                        .into_iter()
                        .enumerate()
                        .filter_map(|(at, child)| {
                            child.map(|child| {
                                let mut path = prefix.clone();
                                path.push(at);
                                (child, path)
                            })
                        }),
                ),
            }
        }
        Ok((entries, paths))
    }
}

fn queue_node(bytes: Bytes, writes: &mut Vec<Bytes>) -> Digest {
    let digest = blake3::hash(&bytes).into();
    writes.push(bytes);
    digest
}

fn invalid() -> io::Error {
    io::Error::other(crate::blob::BlobIntegrityError::BaoMetadata {
        reason: "invalid packed Bao root or index".into(),
    })
}
fn nibble(blob: BlobId, depth: usize) -> io::Result<usize> {
    let byte = *blob
        .as_digest()
        .as_bytes()
        .get(depth / 2)
        .ok_or_else(invalid)?;
    Ok(usize::from(if depth.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 15
    }))
}
fn u32_at(bytes: &[u8]) -> io::Result<u32> {
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| invalid())?))
}
fn digest(bytes: &[u8]) -> io::Result<Digest> {
    Digest::try_from(bytes).map_err(|_| invalid())
}

type PackRows = BTreeMap<BlobId, (u32, u32)>;
fn encode_pack(roots: &BTreeMap<BlobId, Bytes>) -> io::Result<(Bytes, PackRows)> {
    if roots.is_empty() {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    let mut rows = BTreeMap::new();
    for (blob, data) in roots {
        if data.len() > ROOT_LIMIT {
            return Err(invalid());
        }
        let offset = u32::try_from(bytes.len()).map_err(|_| invalid())?;
        rows.insert(*blob, (offset, data.len() as u32));
        bytes.extend_from_slice(data);
        if bytes.len() > PACK_LIMIT {
            return Err(invalid());
        }
    }
    for (blob, (offset, length)) in &rows {
        bytes.extend_from_slice(blob.as_digest().as_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
    }
    bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    bytes.extend_from_slice(PACK_MAGIC);
    if bytes.len() > PACK_LIMIT {
        return Err(invalid());
    }
    Ok((bytes.into(), rows))
}
fn decode_pack(bytes: &[u8]) -> io::Result<PackRows> {
    if bytes.len() < 12 || bytes.len() > PACK_LIMIT || &bytes[bytes.len() - 8..] != PACK_MAGIC {
        return Err(invalid());
    }
    let count = u32_at(&bytes[bytes.len() - 12..bytes.len() - 8])? as usize;
    let footer = count
        .checked_mul(PACK_ROW)
        .and_then(|n| n.checked_add(12))
        .ok_or_else(invalid)?;
    let start = bytes.len().checked_sub(footer).ok_or_else(invalid)?;
    if count == 0 {
        return Err(invalid());
    }
    let mut rows = BTreeMap::new();
    let mut end = 0;
    let mut previous = None;
    for row in bytes[start..bytes.len() - 12].chunks_exact(PACK_ROW) {
        let blob = BlobId::new(digest(&row[..32])?);
        let offset = u32_at(&row[32..36])?;
        let length = u32_at(&row[36..40])?;
        if previous.is_some_and(|previous| previous >= blob)
            || offset != end
            || length as usize > ROOT_LIMIT
        {
            return Err(invalid());
        }
        end = offset.checked_add(length).ok_or_else(invalid)?;
        if end as usize > start {
            return Err(invalid());
        }
        rows.insert(blob, (offset, length));
        previous = Some(blob);
    }
    if end as usize != start {
        return Err(invalid());
    }
    Ok(rows)
}

enum Node {
    Leaf(BTreeMap<BlobId, Location>),
    Branch(Box<[Option<Digest>; 16]>),
}
fn encode_node(node: &Node, depth: usize) -> io::Result<Bytes> {
    if depth > 64 {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    match node {
        Node::Leaf(rows) => {
            if rows.is_empty() || rows.len() > LEAF_LIMIT {
                return Err(invalid());
            }
            bytes.extend_from_slice(LEAF_MAGIC);
            bytes.extend_from_slice(&(depth as u32).to_le_bytes());
            bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
            for (blob, location) in rows {
                bytes.extend_from_slice(blob.as_digest().as_bytes());
                bytes.extend_from_slice(location.pack.as_bytes());
                bytes.extend_from_slice(&location.offset.to_le_bytes());
                bytes.extend_from_slice(&location.length.to_le_bytes());
            }
        }
        Node::Branch(children) => {
            if depth == 64 || children.iter().all(Option::is_none) {
                return Err(invalid());
            }
            bytes.extend_from_slice(BRANCH_MAGIC);
            bytes.extend_from_slice(&(depth as u32).to_le_bytes());
            let mask = children
                .iter()
                .enumerate()
                .fold(0_u32, |mask, (at, child)| {
                    mask | ((child.is_some() as u32) << at)
                });
            bytes.extend_from_slice(&mask.to_le_bytes());
            for child in children.iter().flatten() {
                bytes.extend_from_slice(child.as_bytes());
            }
        }
    }
    Ok(bytes.into())
}
fn decode_node(bytes: &[u8], depth: usize) -> io::Result<Node> {
    if bytes.len() < 16 || depth > 64 || u32_at(&bytes[8..12])? as usize != depth {
        return Err(invalid());
    }
    let count = u32_at(&bytes[12..16])? as usize;
    if &bytes[..8] == LEAF_MAGIC {
        if count == 0 || count > LEAF_LIMIT || bytes.len() != 16 + count * ROW {
            return Err(invalid());
        }
        let mut rows = BTreeMap::new();
        let mut previous = None;
        for row in bytes[16..].chunks_exact(ROW) {
            let blob = BlobId::new(digest(&row[..32])?);
            let location = Location {
                pack: digest(&row[32..64])?,
                offset: u32_at(&row[64..68])?,
                length: u32_at(&row[68..72])?,
            };
            if previous.is_some_and(|previous| previous >= blob)
                || location.length as usize > ROOT_LIMIT
                || location
                    .offset
                    .checked_add(location.length)
                    .is_none_or(|end| end as usize > PACK_LIMIT)
            {
                return Err(invalid());
            }
            rows.insert(blob, location);
            previous = Some(blob);
        }
        Ok(Node::Leaf(rows))
    } else if &bytes[..8] == BRANCH_MAGIC {
        if depth == 64
            || count == 0
            || count > u16::MAX as usize
            || bytes.len() != 16 + count.count_ones() as usize * 32
        {
            return Err(invalid());
        }
        let mut children = [None; 16];
        let mut at = 16;
        for (slot, child) in children.iter_mut().enumerate() {
            if count & (1 << slot) != 0 {
                *child = Some(digest(&bytes[at..at + 32])?);
                at += 32;
            }
        }
        Ok(Node::Branch(Box::new(children)))
    } else {
        Err(invalid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(index: usize) -> BlobId {
        BlobId::new(blake3::hash(&index.to_le_bytes()).into())
    }
    fn storage() -> (Storage, Arc<object_store::memory::InMemory>) {
        let objects = Arc::new(object_store::memory::InMemory::new());
        (
            Storage::new(objects.clone(), Path::from("payloads")),
            objects,
        )
    }

    #[tokio::test]
    async fn root_updates_preserve_old_snapshots_and_remove_only_selected_entries() {
        let (store, _) = storage();
        for count in [1, 127, 128, 129, 257] {
            let roots: BTreeMap<_, _> = (0..count)
                .map(|i| (blob(i), Bytes::from(vec![i as u8; 64])))
                .collect();
            let locations = store.pack(&roots).await.unwrap();
            let original = store
                .update(
                    None,
                    locations.iter().map(|(k, v)| (*k, Some(*v))).collect(),
                    0,
                )
                .await
                .unwrap();
            let (inventory, paths) = store.inventory(original).await.unwrap();
            assert_eq!(inventory, locations);
            assert!(!paths.is_empty());
            for (key, data) in &roots {
                let location = store.lookup(original, *key).await.unwrap().unwrap();
                assert_eq!(store.get(*key, location).await.unwrap(), *data);
            }
            let removed: BTreeMap<_, _> = roots
                .keys()
                .copied()
                .take(count.div_ceil(2))
                .map(|key| (key, None))
                .collect();
            let updated = store.update(original, removed.clone(), 0).await.unwrap();
            for key in roots.keys() {
                assert!(store.lookup(original, *key).await.unwrap().is_some());
                assert_eq!(
                    store.lookup(updated, *key).await.unwrap().is_none(),
                    removed.contains_key(key)
                );
            }
            let empty = store
                .update(updated, roots.keys().map(|key| (*key, None)).collect(), 0)
                .await
                .unwrap();
            assert!(empty.is_none());
            assert_eq!(store.inventory(original).await.unwrap().0, locations);
        }
    }

    #[test]
    fn pack_limits_include_footer_and_allow_empty_root_objects() {
        for size in [0, 64, ROOT_LIMIT] {
            let max = (PACK_LIMIT - 12) / (size + PACK_ROW);
            for count in [max - 1, max, max + 1] {
                let roots = (0..count)
                    .map(|i| (blob(i), Bytes::from(vec![42; size])))
                    .collect();
                let encoded = encode_pack(&roots);
                assert_eq!(encoded.is_ok(), count <= max);
                if let Ok((bytes, rows)) = encoded {
                    assert_eq!(decode_pack(&bytes).unwrap(), rows);
                }
            }
        }
        assert!(encode_pack(&BTreeMap::new()).is_err());
        assert!(
            encode_pack(&BTreeMap::from([(
                blob(0),
                Bytes::from(vec![0; ROOT_LIMIT + 1])
            )]))
            .is_err()
        );
    }

    #[test]
    fn malformed_pack_counts_offsets_order_and_truncation_are_rejected() {
        let (bytes, _) = encode_pack(&BTreeMap::from([
            (blob(0), Bytes::from_static(b"one")),
            (blob(1), Bytes::from_static(b"two")),
        ]))
        .unwrap();
        for end in 0..bytes.len() {
            assert!(decode_pack(&bytes[..end]).is_err());
        }
        let mut invalid = bytes.to_vec();
        let count = invalid.len() - 12;
        invalid[count..count + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_pack(&invalid).is_err());
        for at in [6 + 32, 6 + 36, 6 + PACK_ROW + 32, 6 + PACK_ROW + 36] {
            let mut invalid = bytes.to_vec();
            invalid[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(decode_pack(&invalid).is_err());
        }
        let mut invalid = bytes.to_vec();
        invalid[6..6 + 2 * PACK_ROW].rotate_left(PACK_ROW);
        assert!(decode_pack(&invalid).is_err());
        let mut invalid = bytes.to_vec();
        invalid.push(0);
        assert!(decode_pack(&invalid).is_err());
    }

    #[tokio::test]
    async fn corrupt_or_missing_selected_objects_never_become_lookup_misses() {
        use object_store::ObjectStoreExt;
        let (store, objects) = storage();
        let locations = store
            .pack(&BTreeMap::from([(
                blob(0),
                Bytes::from_static(b"outboard"),
            )]))
            .await
            .unwrap();
        let location = locations[&blob(0)];
        let root = store
            .update(None, BTreeMap::from([(blob(0), Some(location))]), 0)
            .await
            .unwrap()
            .unwrap();
        objects
            .put(
                &store.path("bao-packs", &location.pack),
                b"corrupt".to_vec().into(),
            )
            .await
            .unwrap();
        assert!(store.get(blob(0), location).await.is_err());
        objects
            .delete(&store.path("bao-packs", &location.pack))
            .await
            .unwrap();
        assert!(store.get(blob(0), location).await.is_err());
        objects
            .put(
                &store.path("bao-indexes", &root),
                b"corrupt".to_vec().into(),
            )
            .await
            .unwrap();
        assert!(store.lookup(Some(root), blob(0)).await.is_err());
        objects
            .delete(&store.path("bao-indexes", &root))
            .await
            .unwrap();
        assert!(store.lookup(Some(root), blob(0)).await.is_err());
    }

    #[tokio::test]
    async fn catalog_location_must_match_authenticated_pack_footer() {
        let (store, _) = storage();
        let rows = store
            .pack(&BTreeMap::from([
                (blob(0), Bytes::from_static(b"one")),
                (blob(1), Bytes::from_static(b"two")),
            ]))
            .await
            .unwrap();
        assert!(store.get(blob(0), rows[&blob(1)]).await.is_err());
        let mut bad = rows[&blob(0)];
        bad.offset = u32::MAX;
        assert!(store.get(blob(0), bad).await.is_err());
    }

    #[test]
    fn malformed_nodes_reject_oversized_counts_and_depths() {
        let location = Location {
            pack: Digest::from([3; 32]),
            offset: 0,
            length: 64,
        };
        let bytes = encode_node(&Node::Leaf(BTreeMap::from([(blob(0), location)])), 0).unwrap();
        for end in 0..bytes.len() {
            assert!(decode_node(&bytes[..end], 0).is_err());
        }
        assert!(decode_node(&bytes, 1).is_err());
        let mut bad = bytes.to_vec();
        bad[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_node(&bad, 0).is_err());
        assert!(
            encode_node(
                &Node::Branch(Box::new([Some(Digest::from([1; 32])); 16])),
                64
            )
            .is_err()
        );
    }
}

#[derive(Default)]
pub(super) struct Staging {
    serial: u64,
    roots: BTreeMap<BlobId, (u64, Bytes)>,
    pending: BTreeMap<BlobId, (u64, Option<Location>)>,
}

impl Staging {
    fn next(&mut self) -> io::Result<u64> {
        self.serial = self.serial.checked_add(1).ok_or_else(invalid)?;
        Ok(self.serial)
    }
    fn bytes(&self) -> usize {
        12 + self
            .roots
            .values()
            .map(|(_, data)| PACK_ROW + data.len())
            .sum::<usize>()
    }
}

impl PackedChunks {
    fn sidecar_storage(&self) -> Storage {
        Storage::new(self.object_store.clone(), self.base.clone())
    }
    fn sidecar_root(&self) -> Option<Digest> {
        self.index_catalog
            .lock()
            .unwrap()
            .root
            .as_ref()
            .and_then(|root| root.sidecars)
    }
    pub(crate) async fn put_sidecar(&self, blob: BlobId, bytes: Bytes) -> io::Result<()> {
        if bytes.len() > ROOT_LIMIT || !self.uses_state_catalog() {
            return Err(invalid());
        }
        let _flush = self.flush_lock.lock().await;
        let full = {
            let staging = self.sidecars.lock().unwrap();
            staging.bytes() + PACK_ROW + bytes.len() > PACK_LIMIT
        };
        if full {
            self.flush_sidecars().await?;
        }
        let mut staging = self.sidecars.lock().unwrap();
        let serial = staging.next()?;
        staging.roots.insert(blob, (serial, bytes));
        self.index_dirty.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) async fn flush_sidecars(&self) -> io::Result<()> {
        let roots = self.sidecars.lock().unwrap().roots.clone();
        if roots.is_empty() {
            return Ok(());
        }
        let values = roots
            .iter()
            .map(|(blob, (_, bytes))| (*blob, bytes.clone()))
            .collect();
        let locations = self.sidecar_storage().pack(&values).await?;
        let mut staging = self.sidecars.lock().unwrap();
        for (blob, (serial, _)) in roots {
            if staging
                .roots
                .get(&blob)
                .is_some_and(|(current, _)| *current == serial)
            {
                staging.roots.remove(&blob);
                staging
                    .pending
                    .insert(blob, (serial, Some(locations[&blob])));
            }
        }
        Ok(())
    }
    pub(crate) async fn sidecar(
        &self,
        blob: BlobId,
        catalog: Option<&[u8]>,
    ) -> io::Result<Option<Bytes>> {
        let root = if let Some(catalog) = catalog {
            decode_delta_catalog(&self.resolve_state_catalog(catalog).await?)?.sidecars
        } else {
            let pending = {
                let staging = self.sidecars.lock().unwrap();
                if let Some((_, bytes)) = staging.roots.get(&blob) {
                    return Ok(Some(bytes.clone()));
                }
                staging.pending.get(&blob).copied()
            };
            if let Some((_, location)) = pending {
                return match location {
                    Some(location) => Ok(Some(self.sidecar_storage().get(blob, location).await?)),
                    None => Ok(None),
                };
            }
            self.sidecar_root()
        };
        match self.sidecar_storage().lookup(root, blob).await? {
            Some(location) => Ok(Some(self.sidecar_storage().get(blob, location).await?)),
            None => Ok(None),
        }
    }
    pub(crate) fn remove_sidecars(
        &self,
        blobs: impl IntoIterator<Item = BlobId>,
    ) -> io::Result<()> {
        let mut staging = self.sidecars.lock().unwrap();
        for blob in blobs {
            self.index_dirty.store(true, Ordering::Release);
            let serial = staging.next()?;
            staging.roots.remove(&blob);
            staging.pending.insert(blob, (serial, None));
        }
        Ok(())
    }
    pub(super) fn has_pending_sidecars(&self) -> bool {
        let staging = self.sidecars.lock().unwrap();
        !staging.pending.is_empty() || !staging.roots.is_empty()
    }
    pub(super) async fn prepare_sidecars(
        &self,
        previous: Option<Digest>,
    ) -> io::Result<(Option<Digest>, BTreeMap<BlobId, u64>)> {
        self.compact_sidecars(previous).await?;
        let changes = self.sidecars.lock().unwrap().pending.clone();
        let root = self
            .sidecar_storage()
            .update(
                previous,
                changes
                    .iter()
                    .map(|(blob, (_, location))| (*blob, *location))
                    .collect(),
                0,
            )
            .await?;
        Ok((
            root,
            changes
                .into_iter()
                .map(|(blob, (serial, _))| (blob, serial))
                .collect(),
        ))
    }
    pub(super) fn finish_sidecars(&self, captured: BTreeMap<BlobId, u64>) {
        let mut staging = self.sidecars.lock().unwrap();
        for (blob, serial) in captured {
            if staging
                .pending
                .get(&blob)
                .is_some_and(|(current, _)| *current == serial)
            {
                staging.pending.remove(&blob);
            }
        }
    }
    pub(crate) async fn sidecar_blobs(&self) -> io::Result<Vec<BlobId>> {
        let (mut entries, _) = self
            .sidecar_storage()
            .inventory(self.sidecar_root())
            .await?;
        let staging = self.sidecars.lock().unwrap();
        for (blob, (_, location)) in &staging.pending {
            match location {
                Some(location) => {
                    entries.insert(*blob, *location);
                }
                None => {
                    entries.remove(blob);
                }
            }
        }
        let mut ids: BTreeSet<_> = entries.into_keys().collect();
        ids.extend(staging.roots.keys().copied());
        Ok(ids.into_iter().collect())
    }
    pub(super) async fn mark_sidecars(
        &self,
        root: Option<Digest>,
        retained: &mut HashSet<Path>,
    ) -> io::Result<()> {
        retained.extend(self.sidecar_storage().inventory(root).await?.1);
        Ok(())
    }
}

impl PackedChunks {
    /// Stream descriptor-sized roots so page collection does not buffer every
    /// outboard or fetch packs containing only flat roots.
    pub(crate) fn sidecar_roots<'a>(
        &'a self,
        catalog: Option<&'a [u8]>,
        size: usize,
    ) -> futures::stream::BoxStream<'a, io::Result<Bytes>> {
        Box::pin(async_stream::try_stream! {
            let root = match catalog {
                Some(catalog) => decode_delta_catalog(&self.resolve_state_catalog(catalog).await?)?.sidecars,
                None => self.sidecar_root(),
            };
            let storage = self.sidecar_storage();
            let mut pending: Vec<_> = root.into_iter().map(|root| (root, Vec::<usize>::new())).collect();
            while let Some((root, prefix)) = pending.pop() {
                match storage.node(root, prefix.len()).await? {
                    Node::Leaf(rows) => {
                        for (blob, location) in rows {
                            for (depth, expected) in prefix.iter().enumerate() {
                                if nibble(blob, depth)? != *expected { Err(invalid())?; }
                            }
                            if location.length as usize == size {
                                yield storage.get(blob, location).await?;
                            }
                        }
                    }
                    Node::Branch(children) => {
                        for (at, child) in (*children).into_iter().enumerate() {
                            if let Some(child) = child {
                                let mut path = prefix.clone();
                                path.push(at);
                                pending.push((child, path));
                            }
                        }
                    }
                }
            }
            if catalog.is_none() {
                let (staged, pending) = {
                    let staging = self.sidecars.lock().unwrap();
                    (staging.roots.values().filter(|(_, bytes)| bytes.len() == size)
                        .map(|(_, bytes)| bytes.clone()).collect::<Vec<_>>(),
                     staging.pending.iter().filter_map(|(blob, (_, location))|
                        location.filter(|location| location.length as usize == size).map(|location| (*blob, location)))
                        .collect::<Vec<_>>())
                };
                for bytes in staged { yield bytes; }
                for (blob, location) in pending { yield storage.get(blob, location).await?; }
            }
        })
    }
    pub(super) async fn prune_orphan_sidecars(&self) -> io::Result<()> {
        if !self.uses_state_catalog() {
            return Ok(());
        }
        for blob in self.sidecar_blobs().await? {
            if !self.catalog_contains_manifest(blob).await?
                && self
                    .metadata(&ChunkId::new(*blob.as_digest()))
                    .await?
                    .is_none()
            {
                self.remove_sidecars([blob])?;
            }
        }
        Ok(())
    }
    pub(super) async fn reclaim_sidecars(&self, mark: Option<&PayloadPinMark>) -> io::Result<()> {
        let mut retained = HashSet::new();
        self.mark_sidecars(self.sidecar_root(), &mut retained)
            .await?;
        if let Some(mark) = mark {
            retained.extend(mark.retained.iter().cloned());
        }
        for kind in ["bao-packs", "bao-indexes"] {
            let mut listed = self.object_store.list(Some(&self.base.clone().join(kind)));
            let mut deletes = Vec::new();
            while let Some(object) = listed.try_next().await.map_err(io::Error::other)? {
                if !retained.contains(&object.location) {
                    deletes.push(object.location);
                }
                if deletes.len() == 256 {
                    self.delete_payload_batch(std::mem::take(&mut deletes), mark)
                        .await?;
                }
            }
            if !deletes.is_empty() {
                self.delete_payload_batch(deletes, mark).await?;
            }
        }
        Ok(())
    }
    pub(super) async fn compact_sidecars(&self, previous: Option<Digest>) -> io::Result<()> {
        let pending = self.sidecars.lock().unwrap().pending.clone();
        let mut affected = HashSet::new();
        let storage = self.sidecar_storage();
        let changes: Vec<_> = pending
            .iter()
            .map(|(blob, (_, location))| (*blob, *location))
            .collect();
        let mut changes = futures::stream::iter(changes)
            .map(|(blob, location)| {
                let storage = storage.clone();
                async move {
                    Ok::<_, io::Error>(
                        storage
                            .lookup(previous, blob)
                            .await?
                            .filter(|old| Some(*old) != location)
                            .map(|old| old.pack),
                    )
                }
            })
            .buffer_unordered(16);
        while let Some(pack) = changes.try_next().await? {
            if let Some(pack) = pack {
                affected.insert(pack);
            }
        }
        if affected.is_empty() {
            return Ok(());
        }
        let (entries, _) = self.sidecar_storage().inventory(previous).await?;
        for pack in affected {
            let all: Vec<_> = entries
                .iter()
                .filter(|(_, location)| location.pack == pack)
                .collect();
            let live: Vec<_> = all
                .iter()
                .copied()
                .filter(|(blob, location)| {
                    pending
                        .get(blob)
                        .is_none_or(|(_, next)| *next == Some(**location))
                })
                .collect();
            if live.is_empty() {
                continue;
            }
            let bytes = storage.read("bao-packs", pack, PACK_LIMIT).await?;
            let original = decode_pack(&bytes)?;
            // Compare against the physical pack, not the progressively shrinking
            // index. Otherwise one-at-a-time removals never reach the threshold.
            if live.len() * 2 > original.len() {
                continue;
            }
            let mut roots = BTreeMap::new();
            for (blob, location) in live {
                if original.get(blob) != Some(&(location.offset, location.length)) {
                    return Err(invalid());
                }
                roots.insert(
                    *blob,
                    bytes.slice(
                        location.offset as usize..(location.offset + location.length) as usize,
                    ),
                );
            }
            let locations = self.sidecar_storage().pack(&roots).await?;
            let mut staging = self.sidecars.lock().unwrap();
            for (blob, location) in locations {
                if staging.pending.get(&blob) == pending.get(&blob)
                    && !staging.roots.contains_key(&blob)
                {
                    let serial = staging.next()?;
                    staging.pending.insert(blob, (serial, Some(location)));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[tokio::test]
    async fn prepared_roots_abort_retry_and_preserve_pinned_catalog_view() {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let store =
            PackedChunks::open_with_state_catalog(objects, Path::default(), 1024 * 1024, 0, &empty)
                .await
                .unwrap();
        let blob = BlobId::new(blake3::hash(b"blob").into());
        store
            .put_sidecar(blob, Bytes::from_static(b"first"))
            .await
            .unwrap();
        let aborted = store.prepare_catalog().await.unwrap();
        assert!(aborted.catalog().is_some());
        drop(aborted);
        assert_eq!(
            store.sidecar(blob, None).await.unwrap().unwrap(),
            b"first"[..]
        );
        assert!(store.sidecar(blob, Some(&empty)).await.unwrap().is_none());
        let first = store.prepare_catalog().await.unwrap();
        let catalog = first.catalog().unwrap().to_vec();
        first.commit().unwrap();
        store
            .put_sidecar(blob, Bytes::from_static(b"second"))
            .await
            .unwrap();
        let next = store.prepare_catalog().await.unwrap();
        assert_eq!(
            store.sidecar(blob, Some(&catalog)).await.unwrap().unwrap(),
            b"first"[..]
        );
        next.commit().unwrap();
        assert_eq!(
            store.sidecar(blob, None).await.unwrap().unwrap(),
            b"second"[..]
        );
        assert_eq!(
            store.sidecar(blob, Some(&catalog)).await.unwrap().unwrap(),
            b"first"[..]
        );
        store.remove_sidecars([blob]).unwrap();
        store.prepare_catalog().await.unwrap().commit().unwrap();
        assert!(store.sidecar(blob, None).await.unwrap().is_none());
        assert_eq!(
            store.sidecar(blob, Some(&catalog)).await.unwrap().unwrap(),
            b"first"[..]
        );
    }
    #[tokio::test]
    async fn sparse_packs_repack_live_roots_and_reclaim_only_after_publication() {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let store = PackedChunks::open_with_state_catalog(
            objects.clone(),
            Path::default(),
            1024 * 1024,
            0,
            &empty,
        )
        .await
        .unwrap();
        let blobs: Vec<_> = (0..4u8)
            .map(|i| BlobId::new(blake3::hash(&[i]).into()))
            .collect();
        for blob in &blobs {
            store
                .put_sidecar(*blob, Bytes::from_static(b"root"))
                .await
                .unwrap();
        }
        let prepared = store.prepare_catalog().await.unwrap();
        let old = prepared.catalog().unwrap().to_vec();
        prepared.commit().unwrap();
        let old_location = store
            .sidecar_storage()
            .lookup(store.sidecar_root(), blobs[0])
            .await
            .unwrap()
            .unwrap();
        store.remove_sidecars(blobs[2..].iter().copied()).unwrap();
        let prepared = store.prepare_catalog().await.unwrap();
        store.reclaim_payloads(true, None).await.unwrap();
        assert!(store.sidecar(blobs[3], Some(&old)).await.unwrap().is_some());
        prepared.commit().unwrap();
        let next = store
            .sidecar_storage()
            .lookup(store.sidecar_root(), blobs[0])
            .await
            .unwrap()
            .unwrap();
        assert_ne!(old_location.pack, next.pack);
        store.reclaim_sidecars(None).await.unwrap();
        assert!(
            objects
                .head(
                    &store
                        .sidecar_storage()
                        .path("bao-packs", &old_location.pack)
                )
                .await
                .is_err()
        );
        for blob in &blobs[..2] {
            assert_eq!(
                store.sidecar(*blob, None).await.unwrap().unwrap(),
                b"root"[..]
            );
        }
        store.remove_sidecars(blobs[..2].iter().copied()).unwrap();
        store.prepare_catalog().await.unwrap().commit().unwrap();
        store.reclaim_sidecars(None).await.unwrap();
        assert!(
            objects
                .list(Some(&Path::from("bao-packs")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            objects
                .list(Some(&Path::from("bao-indexes")))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
    #[tokio::test]
    async fn gradual_deletion_and_replacement_compact_against_physical_capacity() {
        for replace in [false, true] {
            let objects = Arc::new(object_store::memory::InMemory::new());
            let empty = PackedChunks::empty_state_catalog().unwrap();
            let store = PackedChunks::open_with_state_catalog(
                objects,
                Path::default(),
                1024 * 1024,
                0,
                &empty,
            )
            .await
            .unwrap();
            let blobs: Vec<_> = (0..8u8)
                .map(|i| BlobId::new(blake3::hash(&[i]).into()))
                .collect();
            for blob in &blobs {
                store
                    .put_sidecar(*blob, Bytes::from_static(b"original"))
                    .await
                    .unwrap();
            }
            store.prepare_catalog().await.unwrap().commit().unwrap();
            let physical = store
                .sidecar_storage()
                .lookup(store.sidecar_root(), blobs[7])
                .await
                .unwrap()
                .unwrap()
                .pack;
            for (at, blob) in blobs[..4].iter().enumerate() {
                if replace {
                    store
                        .put_sidecar(*blob, Bytes::from_static(b"replacement"))
                        .await
                        .unwrap();
                } else {
                    store.remove_sidecars([*blob]).unwrap();
                }
                store.prepare_catalog().await.unwrap().commit().unwrap();
                let current = store
                    .sidecar_storage()
                    .lookup(store.sidecar_root(), blobs[7])
                    .await
                    .unwrap()
                    .unwrap()
                    .pack;
                assert_eq!(
                    current == physical,
                    at < 3,
                    "replace={replace}, removal={at}"
                );
            }
            for blob in &blobs[4..] {
                assert_eq!(
                    store.sidecar(*blob, None).await.unwrap().unwrap(),
                    b"original"[..]
                );
            }
        }
    }

    #[tokio::test]
    async fn abandoned_outboard_without_payload_is_pruned() {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let empty = PackedChunks::empty_state_catalog().unwrap();
        let store =
            PackedChunks::open_with_state_catalog(objects, Path::default(), 1024 * 1024, 0, &empty)
                .await
                .unwrap();
        let blob = BlobId::new(blake3::hash(b"abandoned").into());
        store
            .put_sidecar(blob, Bytes::from_static(b"root"))
            .await
            .unwrap();
        store.prepare_catalog().await.unwrap().commit().unwrap();
        store.prune_orphan_sidecars().await.unwrap();
        store.prepare_catalog().await.unwrap().commit().unwrap();
        assert!(store.sidecar(blob, None).await.unwrap().is_none());
    }
}

#[cfg(test)]
mod routing_tests {
    use super::*;
    #[tokio::test]
    async fn authenticated_but_misrouted_leaf_is_rejected() {
        let objects = Arc::new(object_store::memory::InMemory::new());
        let storage = Storage::new(objects, Path::default());
        let blob = BlobId::new(Digest::from([0; 32]));
        let location = Location {
            pack: Digest::from([1; 32]),
            offset: 0,
            length: 64,
        };
        let leaf = storage
            .write(
                "bao-indexes",
                encode_node(&Node::Leaf(BTreeMap::from([(blob, location)])), 1).unwrap(),
            )
            .await
            .unwrap();
        let mut children = [None; 16];
        children[1] = Some(leaf);
        let root = storage
            .write(
                "bao-indexes",
                encode_node(&Node::Branch(Box::new(children)), 0).unwrap(),
            )
            .await
            .unwrap();
        let query = BlobId::new(Digest::from([0x10; 32]));
        assert!(storage.lookup(Some(root), query).await.is_err());
        assert!(storage.inventory(Some(root)).await.is_err());
        assert!(
            storage
                .update(Some(root), BTreeMap::from([(query, None)]), 0)
                .await
                .is_err()
        );
    }
}
