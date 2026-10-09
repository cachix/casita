use super::*;
use crate::blob::BlobGc;
use crate::metadata::MetadataStore;
use futures::{FutureExt, StreamExt};

const MAX_DEPTH: usize = 64;
const MAX_NODES: u64 = 1_000_000;
const BUFFER: usize = 64 * 1024;
// At most this many active events and this many queued events retain payload
// pipes, plus one in the decoder. Every pipe is bounded by BUFFER.
const FILE_CONCURRENCY: usize = 16;
// Limit canonical directory bytes retained for concurrent staging. A directory
// larger than this is staged alone; open traversal frames are unchanged.
const DIRECTORY_BATCH_BYTES: usize = 256 * 1024;

enum Hasher {
    Md5(md5::Md5),
    Sha1(Box<sha1_checked::Sha1>),
    Sha256(Sha256),
    Sha512(Sha512),
}
impl Hasher {
    fn new(algorithm: NarHashAlgorithm) -> Self {
        match algorithm {
            NarHashAlgorithm::Md5 => Self::Md5(md5::Digest::new()),
            NarHashAlgorithm::Sha1 => Self::Sha1(Box::new(sha1_checked::Sha1::new())),
            NarHashAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
            NarHashAlgorithm::Sha512 => Self::Sha512(Sha512::new()),
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Md5(h) => md5::Digest::update(h, bytes),
            Self::Sha1(h) => h.update(bytes),
            Self::Sha256(h) => h.update(bytes),
            Self::Sha512(h) => h.update(bytes),
        }
    }
    fn finish(self) -> Result<Vec<u8>, NarError> {
        Ok(match self {
            Self::Md5(h) => md5::Digest::finalize(h).to_vec(),
            Self::Sha1(h) => {
                let result = (*h).try_finalize();
                if result.has_collision() {
                    return Err(NarError::invalid("SHA-1 collision"));
                }
                result.hash().to_vec()
            }
            Self::Sha256(h) => h.finalize().to_vec(),
            Self::Sha512(h) => h.finalize().to_vec(),
        })
    }
}
type Hashers = BTreeMap<NarHashAlgorithm, Hasher>;
type GitHashes = BTreeMap<NarHashAlgorithm, Vec<u8>>;
type GitChildren = Vec<(Vec<u8>, Node, GitHashes)>;
struct EncodeDirectory {
    entries: std::vec::IntoIter<(PathComponent, Node)>,
    pending: Option<(Vec<u8>, Node)>,
    children: GitChildren,
}
fn finish(hashers: Hashers) -> Result<GitHashes, NarError> {
    hashers
        .into_iter()
        .map(|(a, h)| Ok((a, h.finish()?)))
        .collect()
}
fn update(hashers: &mut Hashers, bytes: &[u8]) {
    for h in hashers.values_mut() {
        h.update(bytes);
    }
}

impl std::io::Write for Hasher {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Feed one byte stream to every hasher of a set.
struct HasherSink<'a>(&'a mut Hashers);
impl std::io::Write for HasherSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        update(self.0, bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Nix's whole-NAR reference scan over canonical bytes. nix-archive matches
/// 32-byte base32 hash parts exactly as Nix does, retains a fixed boundary
/// tail, and allocates nothing per fed buffer.
struct Scanner(nix_archive::nar::ReferenceScanner);
impl Scanner {
    fn new(needles: &[Vec<u8>]) -> Result<Option<Self>, NarError> {
        if needles.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self(reference_pattern(needles)?.scanner())))
    }
    fn feed(&mut self, bytes: &[u8]) {
        self.0.scan(bytes);
    }
}
pub(super) fn reference_pattern(
    needles: &[Vec<u8>],
) -> Result<nix_archive::nar::ReferencePattern, NarError> {
    nix_archive::nar::ReferencePattern::new(needles)
        .map_err(|error| NarError::invalid(format!("reference needles: {error}")))
}

struct Measurement {
    hashes: Hashers,
    size: u64,
    scanner: Option<Scanner>,
}
impl Measurement {
    fn new(request: &NarRequirements, canonical: bool) -> Result<Self, NarError> {
        let mut hashes = Hashers::new();
        if canonical {
            hashes.insert(
                NarHashAlgorithm::Sha256,
                Hasher::new(NarHashAlgorithm::Sha256),
            );
        }
        for (method, algorithm) in &request.hashes {
            if *method == NarHashMethod::Nar {
                hashes.insert(*algorithm, Hasher::new(*algorithm));
            }
        }
        Ok(Self {
            hashes,
            size: 0,
            scanner: Scanner::new(&request.needles)?,
        })
    }
    fn feed(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.size = self
            .size
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("NAR size overflow"))?;
        update(&mut self.hashes, bytes);
        if let Some(scanner) = &mut self.scanner {
            scanner.feed(bytes);
        }
        Ok(())
    }
    fn complete(
        self,
        request: &NarRequirements,
        values: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Result<u64, NarError> {
        for (algorithm, hash) in finish(self.hashes)? {
            values.insert(vec![0, algorithm as u8], hash);
        }
        if let Some(key) = request.keys().into_iter().find(|k| k[0] == 4) {
            values.insert(
                key,
                self.scanner
                    .map(|scanner| scanner.0.into_matches())
                    .unwrap_or_default()
                    .into_iter()
                    .flat_map(|i| (i as u64).to_le_bytes())
                    .collect(),
            );
        }
        Ok(self.size)
    }
}
impl std::io::Write for Measurement {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.feed(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct MeasuredReader<R> {
    inner: R,
    measurement: Measurement,
}
impl<R: AsyncRead + Unpin> AsyncRead for MeasuredReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => Poll::Ready(self.measurement.feed(&buf.filled()[before..])),
            other => other,
        }
    }
}
struct FileReader<'a, R> {
    inner: R,
    hashes: &'a mut Hashers,
    flat: &'a mut Hashers,
}
impl<R: AsyncRead + Unpin> AsyncRead for FileReader<'_, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                update(self.hashes, &buf.filled()[before..]);
                update(self.flat, &buf.filled()[before..]);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
// nix-archive owns wire validation. Bounded pipes adapt its synchronous
// visitor to async storage without buffering an archive or a whole file.
enum ImportEvent {
    Directory(Option<Vec<u8>>),
    End,
    File(Option<Vec<u8>>, bool, u64, tokio::io::DuplexStream),
    Symlink(Option<Vec<u8>>, Vec<u8>),
}
fn archive_error(error: nix_archive::nar::Error) -> NarError {
    match error {
        nix_archive::nar::Error::Io(error) => NarError::Io(error),
        other => NarError::invalid(other.to_string()),
    }
}
async fn decode<R: AsyncRead + Unpin + Send>(
    reader: R,
    request: &NarRequirements,
    session: &MutationSession<'_, std::sync::Arc<dyn BlobGc>, std::sync::Arc<dyn MetadataStore>>,
    batch_limit: usize,
) -> Result<(Node, GitHashes, Facts, u64), NarError> {
    let (mut input, wire) = tokio::io::duplex(BUFFER);
    let (events, receiver) = tokio::sync::mpsc::channel(FILE_CONCURRENCY);
    let mut receiver = tokio_stream::wrappers::ReceiverStream::new(receiver);
    // The first stage to fail names the cause. The others then see a closed
    // pipe or a missing root, which are consequences; each stage records its
    // error before dropping the pipe end the next one is waiting on.
    let first = std::sync::Arc::new(std::sync::Mutex::new(None::<NarError>));
    let record = |error: NarError| record_first(&first, error);
    let decoder = super::decoder::run({
        let first = first.clone();
        move || {
            let mut wire = tokio_util::io::SyncIoBridge::new(wire);
            let result = nix_archive::nar::decode_events_reader(&mut wire, |event| {
                use nix_archive::nar::Event;
                let send = |event| {
                    events.blocking_send(event).map_err(|_| {
                        nix_archive::nar::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "NAR intake cancelled",
                        ))
                    })
                };
                match event {
                    Event::DirectoryStart { name } => {
                        send(ImportEvent::Directory(name.map(<[u8]>::to_vec)))
                    }
                    Event::DirectoryEnd { .. } => send(ImportEvent::End),
                    Event::Symlink { name, target } => send(ImportEvent::Symlink(
                        name.map(<[u8]>::to_vec),
                        target.to_vec(),
                    )),
                    Event::Regular {
                        name,
                        executable,
                        mut contents,
                    } => {
                        let (output, payload) = tokio::io::duplex(BUFFER);
                        send(ImportEvent::File(
                            name.map(<[u8]>::to_vec),
                            executable,
                            contents.size(),
                            payload,
                        ))?;
                        contents.copy_to(&mut tokio_util::io::SyncIoBridge::new(output))?;
                        Ok(())
                    }
                }
            });
            if let Err(error) = result {
                record_first(&first, archive_error(error));
            }
        }
    });
    let pump = async move {
        let measurement = pump_input(reader, request, &mut input)
            .await
            .map_err(record)
            .ok();
        drop(input);
        measurement
    };
    let consume = async move {
        let consumed = consume_events(&mut receiver, request, session, batch_limit)
            .await
            .map_err(record)
            .ok();
        drop(receiver);
        consumed
    };
    let (consumed, decoded, pumped) = tokio::join!(consume, decoder, pump);
    decoded?;
    if let Some(error) = first.lock().unwrap().take() {
        return Err(error);
    }
    let (Some((root, git, mut values, payload_bytes)), Some(measurement)) = (consumed, pumped)
    else {
        return Err(NarError::invalid("NAR intake ended without a result"));
    };
    let size = measurement.complete(request, &mut values)?;
    Ok((root, git, Facts { size, values }, payload_bytes))
}

/// A broken pipe is never a cause: the decoder sees one only after the
/// consumer went away and the pump only after the decoder did, and whichever
/// went away recorded why before releasing its pipe end.
fn record_first(first: &std::sync::Mutex<Option<NarError>>, error: NarError) {
    if matches!(&error, NarError::Io(error) if error.kind() == std::io::ErrorKind::BrokenPipe) {
        return;
    }
    first.lock().unwrap().get_or_insert(error);
}

async fn pump_input<R: AsyncRead + Unpin + Send>(
    reader: R,
    request: &NarRequirements,
    input: &mut tokio::io::DuplexStream,
) -> Result<Measurement, NarError> {
    let mut reader = MeasuredReader {
        inner: reader,
        measurement: Measurement::new(request, true)?,
    };
    tokio::io::copy(&mut reader, input).await?;
    Ok(reader.measurement)
}

async fn consume_events(
    receiver: &mut tokio_stream::wrappers::ReceiverStream<ImportEvent>,
    request: &NarRequirements,
    session: &MutationSession<'_, std::sync::Arc<dyn BlobGc>, std::sync::Arc<dyn MetadataStore>>,
    batch_limit: usize,
) -> Result<(Node, GitHashes, BTreeMap<Vec<u8>, Vec<u8>>, u64), NarError> {
    let mut frames: Vec<(Option<Vec<u8>>, Directory, GitChildren)> = Vec::new();
    let mut root = None;
    let mut values = BTreeMap::new();
    let mut pending = Vec::new();
    let mut payload_bytes = 0u64;
    let mut nodes = 0u64;
    // Poll file stages together so the durable pin ledger can group admissions.
    // Ordered completion keeps directory construction and publication post-order.
    // Futures borrow the session: cancellation drops every in-flight stage.
    loop {
        let completed = {
            let mut stage = |event| {
                let valid = (|| {
                    if !matches!(event, ImportEvent::End) {
                        nodes += 1;
                        if nodes > MAX_NODES {
                            return Err(NarError::invalid("NAR traversal limit exceeded"));
                        }
                        if nodes == 1 {
                            // The root kind is known before anything is staged.
                            request.validate_root(match &event {
                                ImportEvent::File(_, executable, ..) => Some(*executable),
                                _ => None,
                            })?;
                        }
                    }
                    Ok::<_, NarError>(())
                })();
                async move {
                    let mut event = event;
                    // Keep the pipe alive even on error until ordered consumption
                    // records the cause, rather than waking the decoder early.
                    let staged = async {
                        valid?;
                        if let ImportEvent::File(name, _, size, payload) = &mut event {
                            let mut hashes = git_hashers(request, *size);
                            let mut flat = flat_hashers(request, name.is_none());
                            let mut file = FileReader {
                                inner: payload,
                                hashes: &mut hashes,
                                flat: &mut flat,
                            };
                            let object = session
                                .stage_blob_reader(&mut file)
                                .await
                                .map_err(NarError::storage)?;
                            if object.record().payload_size() != *size {
                                return Err(NarError::invalid("truncated file contents"));
                            }
                            Ok(Some((object, hashes, flat)))
                        } else {
                            Ok(None)
                        }
                    }
                    .await;
                    (event, staged)
                }
                .boxed()
            };
            let mut stages = futures::stream::FuturesOrdered::new();
            let mut completed = Vec::new();
            let mut admitted = 0;
            loop {
                // Fill the ready window before polling its first writer. The
                // pin layer can then see sibling requests in the same turn.
                // Never wait for more input here: a large file may need its
                // stage polled before the decoder can send the next event.
                while !stages.is_empty() && admitted < FILE_CONCURRENCY {
                    let Some(Some(event)) = receiver.next().now_or_never() else {
                        break;
                    };
                    stages.push_back(stage(event));
                    admitted += 1;
                }
                if stages.is_empty() {
                    if admitted == FILE_CONCURRENCY {
                        break;
                    }
                    // Do not wait for more input after completing the admitted
                    // stages: publication must progress even if the sender pauses.
                    let event = if completed.is_empty() {
                        receiver.next().await
                    } else {
                        receiver.next().now_or_never().flatten()
                    };
                    let Some(event) = event else { break };
                    stages.push_back(stage(event));
                    admitted += 1;
                } else if admitted == FILE_CONCURRENCY {
                    let (event, staged) = stages.next().await.expect("nonempty staging queue");
                    completed.push((event, staged?));
                } else {
                    tokio::select! {
                        event = receiver.next() => {
                            if let Some(event) = event {
                                stages.push_back(stage(event));
                                admitted += 1;
                            } else {
                                admitted = FILE_CONCURRENCY;
                            }
                        }
                        item = stages.next() => {
                            let (event, staged) = item.expect("nonempty staging queue");
                            completed.push((event, staged?));
                        }
                    }
                }
            }
            completed
        };
        if completed.is_empty() {
            break;
        }
        // Staging may hold the shared pin admission gate across an await. Drain
        // every stage before directory writes or publication can reacquire it;
        // leaving a buffered future suspended here can deadlock those operations.
        let mut writes = Vec::new();
        let mut directory_bytes = 0;
        for (event, staged) in completed {
            let (name, node, git) = match event {
                ImportEvent::Directory(name) => {
                    frames.push((name, Directory::new(), Vec::new()));
                    continue;
                }
                ImportEvent::End => {
                    let (name, directory, children) =
                        frames.pop().expect("validated directory events");
                    let encoded = directory.encode();
                    let bytes = encoded.len();
                    let node = Node::Directory {
                        digest: crate::DirectoryId::new(blake3::hash(&encoded).into()),
                        size: directory.size(),
                    };
                    drop(encoded);
                    if bytes > DIRECTORY_BATCH_BYTES.saturating_sub(directory_bytes) {
                        finish_writes(session, &mut writes, &mut pending, batch_limit).await?;
                        directory_bytes = 0;
                    }
                    writes.push(
                        async move {
                            session
                                .stage_directory(&directory)
                                .await
                                .map_err(NarError::storage)
                        }
                        .boxed(),
                    );
                    directory_bytes += bytes;
                    (name, node, git_tree(request, children)?)
                }
                ImportEvent::Symlink(name, target) => {
                    let mut hashes = git_hashers(request, target.len() as u64);
                    update(&mut hashes, &target);
                    let node = Node::Symlink {
                        target: SymlinkTarget::try_from(Bytes::from(target))
                            .map_err(NarError::storage)?,
                    };
                    (name, node, finish(hashes)?)
                }
                ImportEvent::File(name, executable, size, _) => {
                    let (object, hashes, flat) = staged.expect("file staging completed");
                    let node = Node::File {
                        digest: crate::BlobId::new(object.record().key().native_digest().unwrap()),
                        size,
                        executable,
                    };
                    writes.push(async move { Ok(object) }.boxed());
                    payload_bytes = payload_bytes
                        .checked_add(size)
                        .ok_or_else(|| NarError::invalid("payload size overflow"))?;
                    save_flat(request, flat, &mut values)?;
                    (name, node, finish(hashes)?)
                }
            };
            if directory_bytes >= DIRECTORY_BATCH_BYTES || writes.len() >= FILE_CONCURRENCY {
                finish_writes(session, &mut writes, &mut pending, batch_limit).await?;
                directory_bytes = 0;
            }
            if let Some((_, directory, children)) = frames.last_mut() {
                let name = name.expect("validated child name");
                if !git.is_empty() {
                    children.push((name.clone(), node.clone(), git));
                }
                directory
                    .add(
                        PathComponent::try_from(Bytes::from(name)).map_err(NarError::storage)?,
                        node,
                    )
                    .map_err(NarError::storage)?;
            } else {
                root = Some((node, git));
            }
        }
        // Do not wait for more input with completed directories queued. Also
        // drain all writes before publication can acquire their admission gate.
        finish_writes(session, &mut writes, &mut pending, batch_limit).await?;
    }
    let (root, git) = root.ok_or_else(|| NarError::invalid("missing NAR root"))?;
    publish_full(session, &mut pending, 1).await?;
    Ok::<_, NarError>((root, git, values, payload_bytes))
}

async fn finish_writes<'s>(
    session: &'s MutationSession<'_, std::sync::Arc<dyn BlobGc>, std::sync::Arc<dyn MetadataStore>>,
    writes: &mut Vec<
        futures::future::BoxFuture<'s, Result<crate::repository::StagedObject<'s>, NarError>>,
    >,
    pending: &mut Vec<crate::repository::StagedObject<'s>>,
    batch_limit: usize,
) -> Result<(), NarError> {
    use futures::TryStreamExt;
    let completed: Vec<_> = std::mem::take(writes)
        .into_iter()
        .collect::<futures::stream::FuturesOrdered<_>>()
        .try_collect()
        .await?;
    // Staging may finish in any order, but publication remains post-order.
    // Never publish while an unpolled sibling could hold the admission gate.
    for object in completed {
        pending.push(object);
        publish_full(session, pending, batch_limit).await?;
    }
    Ok(())
}

/// Publish staged objects once `limit` are pending. Children are staged before
/// their directory, so every batch is a prefix of the post-order sequence and
/// link verification finds each target already published or in the batch.
/// That same order is the construction proof a validated closure needs.
async fn publish_full<'s>(
    session: &'s MutationSession<'_, std::sync::Arc<dyn BlobGc>, std::sync::Arc<dyn MetadataStore>>,
    pending: &mut Vec<crate::repository::StagedObject<'s>>,
    limit: usize,
) -> Result<(), NarError> {
    if !pending.is_empty() && pending.len() >= limit {
        session
            .publish_filesystem_constructed(std::mem::take(pending), Vec::new())
            .await
            .map_err(NarError::storage)?;
    }
    Ok(())
}

fn flat_hashers(request: &NarRequirements, root: bool) -> Hashers {
    request
        .hashes
        .iter()
        .filter(|(method, _)| root && matches!(method, NarHashMethod::Flat | NarHashMethod::Text))
        .map(|(_, algorithm)| (*algorithm, Hasher::new(*algorithm)))
        .collect()
}
fn save_flat(
    request: &NarRequirements,
    hashes: Hashers,
    values: &mut BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<(), NarError> {
    for (algorithm, digest) in finish(hashes)? {
        for method in [NarHashMethod::Flat, NarHashMethod::Text] {
            if request.hashes.contains(&(method, algorithm)) {
                values.insert(vec![method as u8, algorithm as u8], digest.clone());
            }
        }
    }
    Ok(())
}
/// Git blob hashers for `size` bytes of file contents or symlink target,
/// one per requested algorithm, with the object header already fed.
fn git_hashers(request: &NarRequirements, size: u64) -> Hashers {
    let mut hashers: Hashers = request
        .hashes
        .iter()
        .filter(|(m, _)| *m == NarHashMethod::Git)
        .map(|(_, a)| (*a, Hasher::new(*a)))
        .collect();
    nix_archive::git::blob_header(&mut HasherSink(&mut hashers), size).expect("hashing succeeds");
    hashers
}
fn git_mode(node: &Node) -> nix_archive::git::Mode {
    use nix_archive::git::Mode;
    match node {
        Node::Directory { .. } => Mode::Directory,
        Node::Symlink { .. } => Mode::Symlink,
        Node::File {
            executable: true, ..
        } => Mode::Executable,
        Node::File { .. } => Mode::Regular,
    }
}
fn git_error(error: nix_archive::git::Error) -> NarError {
    match error {
        nix_archive::git::Error::Io(error) => NarError::Io(error),
        other => NarError::invalid(other.to_string()),
    }
}
/// Git tree hashes over children collected in NAR order. nix-archive owns
/// the entry order, which differs from NAR order, and the tree encoding.
fn git_tree(request: &NarRequirements, mut children: GitChildren) -> Result<GitHashes, NarError> {
    children.sort_by(|(a, a_node, _), (b, b_node, _)| {
        nix_archive::git::compare_names(a, git_mode(a_node), b, git_mode(b_node))
    });
    let mut result = BTreeMap::new();
    for (method, algorithm) in &request.hashes {
        if *method != NarHashMethod::Git {
            continue;
        }
        let entries = children
            .iter()
            .map(|(name, node, hashes)| {
                Ok(nix_archive::git::TreeEntry {
                    name,
                    mode: git_mode(node),
                    hash: hashes
                        .get(algorithm)
                        .ok_or_else(|| NarError::invalid("missing Git child hash"))?,
                })
            })
            .collect::<Result<Vec<_>, NarError>>()?;
        let mut hash = Hasher::new(*algorithm);
        nix_archive::git::encode_tree(&mut hash, &entries).map_err(git_error)?;
        result.insert(*algorithm, hash.finish()?);
    }
    Ok(result)
}

pub(crate) async fn import<R: AsyncRead + Unpin + Send>(
    repository: &Repository,
    reader: R,
    request: NarRequirements,
) -> Result<VerifiedNarReport, NarError> {
    let start = Instant::now();
    request.validate(None)?;
    let session = repository
        .inner
        .mutation_session()
        .await
        .map_err(NarError::storage)?;
    // One serialized metadata commit per node would stall every other writer
    // behind a large archive; publish in the same bounded batches as tar intake.
    let batch_limit = repository.inner.limits().max_batch_objects.max(1);
    let (root, git, mut facts, payload_bytes) =
        decode(reader, &request, &session, batch_limit).await?;
    request.validate(Some(&root))?;
    for (algorithm, digest) in git {
        facts.values.insert(vec![3, algorithm as u8], digest);
    }
    // All native publications are durable. Acquire the outgoing hold while
    // the mutation session still pins every imported object.
    let reader = repository.retained_reader().await?;
    #[cfg(test)]
    super::tests::crash_checkpoint("content");
    let mut audit_stats = NarVerificationStats::default();
    if let Some(store) = &repository.inner.nar_store {
        facts = loop {
            let generation = store.generation().await.map_err(NarError::storage)?;
            // Ingestion may deduplicate against a preexisting damaged physical
            // representation. After a known failure, do not mint new trust from
            // an input stream until the stored representation verifies again.
            if store.requires_native_audit().await? && store.get(&identity(&root)).await?.is_none()
            {
                let (stored, stats) = measure_tree(&reader, &root, &request, None, true).await?;
                if stored.encode() != facts.encode() {
                    store.quarantine(&identity(&root)).await?;
                    return Err(NarError::Conflict);
                }
                audit_stats.encoding_passes += stats.encoding_passes;
                audit_stats.hash_payload_bytes += stats.hash_payload_bytes;
            }
            // An invalidation between that check and the association write
            // means the stored representation must verify again.
            if let Some(merged) = store.merge(&identity(&root), &facts, generation).await? {
                break merged;
            }
        };
    }
    #[cfg(test)]
    super::tests::crash_checkpoint("association");
    let result = report(
        reader,
        root,
        facts,
        &request,
        NarVerificationStats {
            encoding_passes: audit_stats.encoding_passes,
            hash_payload_bytes: payload_bytes + audit_stats.hash_payload_bytes,
            verification_time: start.elapsed(),
            ..Default::default()
        },
    );
    drop(session);
    result
}

pub(crate) async fn read_directory(
    reader: &RetainedReader,
    key: &ObjectKey,
) -> Result<Directory, NarError> {
    let mut payload = reader.open_verified(key).await?.ok_or_else(|| {
        NarError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "missing directory",
        ))
    })?;
    let limits = reader.hold.repository().limits();
    let record = payload.record().clone();
    crate::directory::read::read_directory_payload(
        key,
        &record,
        &mut payload,
        limits.max_metadata_bytes.min(limits.max_payload_bytes),
    )
    .await
    .map_err(NarError::storage)
}
struct Encoder<'a> {
    reader: &'a RetainedReader,
    request: &'a NarRequirements,
    wire: Option<nix_archive::nar::Encoder<std::io::BufWriter<Measurement>>>,
    values: BTreeMap<Vec<u8>, Vec<u8>>,
    payload_bytes: u64,
    nodes: u64,
}
impl Encoder<'_> {
    async fn node(&mut self, root: &Node) -> Result<GitHashes, NarError> {
        let mut frames: Vec<EncodeDirectory> = Vec::new();
        let mut current = Some((None, root.clone()));
        let mut completed = None;
        loop {
            if let Some(git) = completed.take() {
                let Some(parent) = frames.last_mut() else {
                    return Ok(git);
                };
                let (name, node) = parent.pending.take().expect("entry precedes child");
                if !git.is_empty() {
                    parent.children.push((name, node, git));
                }
            } else {
                let (name, node) = current.take().expect("next node selected");
                self.nodes += 1;
                if frames.len() >= MAX_DEPTH || self.nodes > MAX_NODES {
                    return Err(NarError::invalid("NAR traversal limit exceeded"));
                }
                match node {
                    Node::File {
                        digest,
                        size,
                        executable,
                    } => {
                        let mut output = self
                            .wire
                            .as_mut()
                            .map(|wire| wire.start_regular(name.as_deref(), executable, size))
                            .transpose()
                            .map_err(archive_error)?;
                        let mut git = git_hashers(self.request, size);
                        let mut flat = flat_hashers(self.request, frames.is_empty());
                        let mut payload = self
                            .reader
                            .open_verified(&ObjectKey::blob(digest))
                            .await?
                            .ok_or_else(|| {
                                NarError::Io(std::io::Error::new(
                                    std::io::ErrorKind::NotFound,
                                    "missing file",
                                ))
                            })?;
                        if payload.record().payload_size() != size {
                            return Err(NarError::invalid("file size mismatch"));
                        }
                        let mut buffer = vec![0; BUFFER];
                        let mut read = 0;
                        loop {
                            let n = payload.read(&mut buffer).await?;
                            if n == 0 {
                                break;
                            }
                            read += n as u64;
                            update(&mut git, &buffer[..n]);
                            update(&mut flat, &buffer[..n]);
                            if let Some(output) = &mut output {
                                std::io::Write::write_all(output, &buffer[..n])?;
                            }
                        }
                        if read != size {
                            return Err(NarError::invalid("file length mismatch"));
                        }
                        self.payload_bytes += read;
                        if let Some(output) = output {
                            output.finish().map_err(archive_error)?;
                        }
                        save_flat(self.request, flat, &mut self.values)?;
                        completed = Some(finish(git)?);
                        continue;
                    }
                    Node::Symlink { target } => {
                        if let Some(wire) = &mut self.wire {
                            wire.symlink(name.as_deref(), target.as_bytes())
                                .map_err(archive_error)?;
                        }
                        let mut git = git_hashers(self.request, target.as_bytes().len() as u64);
                        update(&mut git, target.as_bytes());
                        completed = Some(finish(git)?);
                        continue;
                    }
                    Node::Directory { digest, size } => {
                        if let Some(wire) = &mut self.wire {
                            wire.start_directory(name.as_deref())
                                .map_err(archive_error)?;
                        }
                        let directory =
                            read_directory(self.reader, &ObjectKey::directory(digest)).await?;
                        if directory.size() != size {
                            return Err(NarError::invalid("directory descendant count mismatch"));
                        }
                        frames.push(EncodeDirectory {
                            entries: directory
                                .nodes()
                                .map(|(name, node)| (name.clone(), node.clone()))
                                .collect::<Vec<_>>()
                                .into_iter(),
                            pending: None,
                            children: Vec::new(),
                        });
                    }
                }
            }
            let frame = frames.last_mut().expect("open directory");
            if let Some((name, node)) = frame.entries.next() {
                let name = name.as_bytes().to_vec();
                frame.pending = Some((name.clone(), node.clone()));
                current = Some((Some(name), node));
            } else {
                let frame = frames.pop().expect("open directory");
                if let Some(wire) = &mut self.wire {
                    wire.end_directory().map_err(archive_error)?;
                }
                completed = Some(git_tree(self.request, frame.children)?);
            }
        }
    }
}
pub(super) async fn measure_tree(
    reader: &RetainedReader,
    root: &Node,
    request: &NarRequirements,
    old: Option<&Facts>,
    scrub: bool,
) -> Result<(Facts, NarVerificationStats), NarError> {
    let encode = scrub
        || old.is_none()
        || !request.needles.is_empty()
        || request.hashes.iter().any(|(m, _)| *m == NarHashMethod::Nar);
    let mut encoder = Encoder {
        reader,
        request,
        wire: if encode {
            // The encoder emits a few bytes per token; buffering keeps the
            // hashing and needle scanning passes per 64 KiB, not per token.
            Some(
                nix_archive::nar::Encoder::new(std::io::BufWriter::with_capacity(
                    BUFFER,
                    Measurement::new(request, old.is_none() || scrub)?,
                ))
                .map_err(archive_error)?,
            )
        } else {
            None
        },
        values: BTreeMap::new(),
        payload_bytes: 0,
        nodes: 0,
    };
    let git = encoder.node(root).await?;
    for (algorithm, digest) in git {
        encoder.values.insert(vec![3, algorithm as u8], digest);
    }
    let size = match encoder.wire {
        Some(wire) => {
            let mut sink = wire.finish().map_err(archive_error)?;
            std::io::Write::flush(&mut sink)?;
            sink.into_inner()
                .map_err(|error| NarError::Io(error.into_error()))?
                .complete(request, &mut encoder.values)?
        }
        None => old.unwrap().size,
    };
    let mut facts = Facts {
        size: if encode { size } else { old.unwrap().size },
        values: encoder.values,
    };
    if let Some(old) = old {
        if encode && size != old.size {
            return Err(NarError::Conflict);
        }
        for (k, v) in &old.values {
            facts.values.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    Ok((
        facts,
        NarVerificationStats {
            encoding_passes: u64::from(encode),
            hash_payload_bytes: encoder.payload_bytes,
            ..Default::default()
        },
    ))
}
