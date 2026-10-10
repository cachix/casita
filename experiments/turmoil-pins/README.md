# Turmoil repository simulation spike

The contribution uses Turmoil with explicit entropy injection. The earlier optional
symbol-interception comparison was removed from this reviewable version; no MadSim
or interception runtime is included. Historical findings below refer to the
original research worktree.

This standalone crate exercises Casita's production `ObjectPinStore`, repository
publication, graph verification, and garbage collection. Turmoil controls the
TCP pin ledger, host ordering, network latency, and Tokio timers. The simulated
storage server models atomic conditional writes, not S3's HTTP protocol. This
is a correctness investigation, not a performance benchmark.

The production changes introduce an experimental `EntropySource` composition
point for memory metadata revisions, memory/object pin tokens, and publication
retry jitter. Ordinary constructors still use OS entropy. Turmoil and the
simulation dependencies stay outside the production workspace.

## Focused CI harness

`src/harness.rs` centralizes seeded host ordering, simulated latency and time
budgets. It also provides a strict replay checker that compares complete reports,
including ownership identities and event timing. Each attempt writes a `started`
record followed by its report or error to a flushed JSONL artifact. A timeout or
panic leaves the active seed and case identifiable; divergent reports are both
retained. Successful reports contribute to a stable corpus digest.

`dst-ci` is a small entrypoint for a bounded change gate. It requires
`explicit-entropy` and accepts 1 through 64 seeds, defaulting to four. Every seed
runs 63 cases twice: four pin-ledger cases, eight repository cases, nine atomic
marker cases, six shared-chunk cases, four partial-restore cases, and 32 restarted
save-outage cases covering both publication boundaries, all read faults and all
save stages. The exhaustive partial-write matrices, negative controls and native
process-crash probes remain in the standalone test suite.

```console
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml --bin dst-ci -- 4 target/dst/reports-1.jsonl
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml --bin dst-ci -- 4 target/dst/reports-2.jsonl
cmp target/dst/reports-1.jsonl target/dst/reports-2.jsonl
```

`.github/workflows/deterministic-simulation.yml` runs on PRs, main pushes and
manual dispatch. Its Linux job uses the repository's pinned Rust 1.96.0, a
standalone Cargo cache, formatting, strict Clippy and the full test suite. It then
runs the four-seed corpus in two independent processes and compares their complete
JSONL artifacts. Logs and reports are uploaded even on failure. The check and replay steps have
30- and ten-minute limits within the 45-minute job budget, leaving time for
artifact upload after a step timeout. The contribution uses explicit entropy injection with no interception shim.
This workflow becomes active when the contribution is pushed.

## Reproduce

Run from the checkout containing the spike:

```console
cargo test --locked --manifest-path experiments/turmoil-pins/Cargo.toml
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- chunk-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- writer-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-writer-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-read-corpus 64
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-read-save-corpus 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-read-save-crash-corpus 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-read-save-outage-corpus 4
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-read-save-outage-crash-corpus 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-restarted-outage-corpus 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- native-fencing 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- native-retention 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- partial-restore-corpus 32
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- cancel-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- network-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- restart-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- marker-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- partition-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-restart-corpus 256
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-failure-corpus 32
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- journal-error-crash-corpus 32
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- partial-journal-corpus 32
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- persistent-journal-corpus 16
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal-persistent-crash 4
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal-read-failure 4
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal-partial-crash 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal-error-crash 8
cargo test --locked --manifest-path experiments/turmoil-pins/Cargo.toml --lib intent_journal::
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 marker-client-restart-before-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 marker-client-restart-after-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 marker-partition-before-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 marker-partition-after-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 marker-restart-advanced
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 marker-writers
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- backend-marker 32
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- backend-crash 32
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal 16
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal-overlap 8
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- client-journal-submission 8
cargo test --locked --manifest-path experiments/turmoil-pins/Cargo.toml --test client_journal
cargo test --locked -p casita --no-default-features --features native,experimental --lib operation_marker_ -- --nocapture
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 network-restart-before-cache
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 network-lost-ack
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 chunk-cancel-writer-before-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 chunk-writers-before-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 chunk-delete-before-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 cancel-before-apply
cargo run --locked --manifest-path experiments/turmoil-pins/Cargo.toml -- seed 7 stale-mark
cargo test --locked --manifest-path experiments/turmoil-pins/Cargo.toml --no-default-features -- --test-threads=1
cargo fmt --manifest-path experiments/turmoil-pins/Cargo.toml --check
cargo clippy --locked --manifest-path experiments/turmoil-pins/Cargo.toml --all-targets --all-features -- -D warnings
```

After fetching dependencies, commands also support `--offline`. Each corpus
case runs twice. Default runs require identical reports, including raw ledger
bytes for the pin cases and revision IDs and event timestamps for repository
cases. The CLI prints replayable traces for individual seeds. Replays are scoped
to a fixed binary, lockfile, platform, scenario, and seed; framework seeds do
not promise identical schedules after code or dependency changes.

## Cases and independent checks

### Real backend operation markers

`backend-marker COUNT` runs two native cases per iteration using a fresh on-disk
`TursoMetadataStore` and the production `publish_with_metadata` API. It is a
transaction probe outside Turmoil and is excluded from the seeded corpus.

- Discard an applied publication's acknowledgement, then attempt another request
  with the same operation ID and a different payload.
- Publish concurrently from two mutation sessions sharing an operation ID, with
  different payloads and root targets. Exactly one writer may commit.

Each publication combines an expected-absent record check, a marker containing
the operation ID and request identity, the verified object, and the root change.
The checker requires one successful application and one `CheckFailed` rejection.
It drops all database handles and reopens the file, then checks the winning
marker, root, logical object, exact payload bytes, and unchanged repository
revision. A fresh caller attempts another conflicting publication after reopening;
its object insertion, marker replacement, and root change must all be rejected,
with the original revision intact. A negative control omits the marker from
publication and must fail the independent recovery checker.

This establishes a persisted logical outcome and protection against reuse of an
operation ID. It does not reconstruct the original full `CommitResult`: the
transaction generates its revision internally, and the marker cannot record
that revision through this API. A remote protocol should distinguish recovery
of the requested effect from replay of the original response. Opaque markers do
not retain objects after their root is removed, so their lifecycle also needs
an explicit policy.

The acknowledgement loss is injected by discarding the publication result at
the probe boundary. Database reopening is graceful; no process kill, power loss,
or fsync failure is injected. Payloads use `MemoryBlobStore` and remain in this
process, so this probe tests metadata persistence rather than payload durability.
The existing seeded TCP cases separately exercise actual connection loss and
daemon restart against their modeled journal.

### Restart while persistent save storage remains faulty

`journal-restarted-outage-corpus COUNT` keeps the persistent fault active when
client epoch 6 starts. It reloads the original current intent, queries the original
effect read-only, then exhausts its own three-attempt save batch. The live client
must remain idle for 25ms with the fault still active before explicit repair
allows one healthy local save. The expected batch outcomes are
`[(1, true), (1, true), (3, false), (3, false), (1, true)]`: six save errors,
four local retries and two exhausted budgets. Both surviving current phases,
all partial-write cutoffs, all read faults and both application boundaries remain
covered. Negative controls add a fourth attempt in the fresh client's batch,
start another batch before repair, or resubmit the original publication.

### Lock-file replacement and authoritative publication fencing

The Unix lock-replacement test holds the old `recovery.lock`, renames it and
creates a replacement at the same path. A second owner can lock the new inode
while the first owner still holds its lock. The old owner can then overwrite the
current local journal. This reproduces a limitation of the spike-only advisory
protocol; it does not establish a production journal bug. Checking path identity
alone cannot make the check and the later local write atomic.

`native-fencing COUNT` uses real Turso metadata and the production
`publish_with_metadata` API to exercise an authoritative alternative for repository
publication. A generation record advances from 1 to 2 before the old owner tries
to publish. The generation check, object insertion, operation marker and root
change share one atomic transaction. The old owner must be rejected with no
logical object, marker, root or revision changes; generation 2 publishes normally.
After advancing to 3, closing and reopening the database, both earlier generations
must remain rejected even with fresh operation markers. Exact winning payload
bytes, root, marker, generation and unchanged revision are independently audited.
A negative control omits the generation check and must fail the stale-publication
checker.

This fences repository publication, not local `intent.json` writes. The probe
assumes a trusted generation authority never reuses tokens and does not add a
lease allocator, expiration policy, distributed coordination service or production
journal API. Native fencing cases are repeated database checks, not seeded replays;
payload storage remains in memory across graceful database reopening.

### Marker retention and operation-ID reuse

`native-retention COUNT` exercises an application policy against native Turso
metadata. An operation publishes a root and marker atomically. Retiring it removes
the root and replaces the marker with a tombstone. Production GC must remove the
logical object and its physical payload while preserving that tombstone. Both an
identical retry and a different request with the same full operation ID must fail
without changing metadata or recreating a root.

Pruning the tombstone and advancing the authoritative generation share one atomic
transaction. Recovery reads generation and marker from one snapshot. An absent
marker from an expired generation reports `Expired`, never `Unknown`. Publication
checks the generation atomically, so an old request cannot republish after pruning.
A new generation can reuse the short suffix `operation7` under a different full ID.
The new payload, marker, root and expired recovery result are checked after a
graceful database reopen. Negative controls remove the publication generation
check or ignore the recovery generation, and must fail their respective checkers.

This is a spike policy, not automatic Casita marker retention. Generations must
never be reused; allocation, retirement authorization and a retention duration
remain application responsibilities. The native cases use in-memory payloads
and do not claim payload crash durability or seeded replay.

### Cancellation during partial object restore

`partial-restore-corpus COUNT` uses the existing production FastCDC/zstd fixture,
three independent staging leases, and both DELETE application boundaries. Writer
0 alone restores either one or three of the four missing unique B chunks. The
two surviving writers wait before restoration while retaining pins over every B
resource. The coordinator cancels writer 0 before `stage_existing` or publication
and verifies that only its physical token is released.

GC then runs with the surviving pins. The checker compares the exact restored
wire bytes with the fixture before and after collection, verifies a strictly
partial restore, checks root A's shared chunks remain readable, and requires B/0
to remain absent. The two survivors then restore the remaining objects and publish
the full B graph. Fresh handles verify exact payloads and roots before and after
final GC, with no leaked ownership. Releasing the survivors' pins early is a
negative control and must destroy the partial chunks and fail the checker.
Complete reports, including traces, physical paths and timing, replay twice per
case and are hashed for independent-process comparison. This models cancellation
after a completed prefix of object writes, not a power loss within one PUT.

### Crash after persistent recovery-save budget exhaustion

`journal-read-save-outage-crash-corpus COUNT` combines all four read faults and
eight save faults at both publication boundaries, for 64 cases per seed. The
repaired fifth client exhausts exactly three persistent save attempts and crashes
before storage repair. Its current intent must retain the original identity:
Unknown before rename, or the complete Recovered revision after directory-sync
failure. Partial temporary bytes remain independently checked.

The supervisor keeps the failed client down and the save fault active for another
25ms of virtual time. Journal bytes, retry batches, error counts, remote requests
and repository revision must remain unchanged during that interval. This is a
client-down check; the separate live-client outage corpus verifies idleness while
the client is still running. The supervisor then explicitly repairs save storage
and boots client epoch 6, which reloads current intent, performs exactly one
read-only recovery query and completes one healthy local save.

Expected batch outcomes are two initial one-attempt successes, a failed
three-attempt recovery batch and one final one-attempt success. Final auditing
requires one publication application, matching marker and root, exact payload
bytes, unchanged repository revision during fresh recovery, and persisted
Recovered intent. Negative controls exceed the retry budget, resubmit after
repair, or promote either an alternate valid temporary intent or a truncated
recovery record.

Each seeded case runs twice and independent processes compare complete-report
digests. The supervisor repairs storage before restarting the final client. The separate
`journal-restarted-outage-corpus` restarts before save repair.

### Read repair followed by persistent recovery-save outage

`journal-read-save-outage-corpus COUNT` combines four journal read faults, eight
recovery-save faults, both application boundaries and two repair timings, for 128
cases per seed. Three failed reader hosts are discarded before read repair. Save 3
then encounters a persistent replacement-stage or partial-write fault.

Early repair occurs 1ms after the first save error, before the 5ms retry delay:
the next attempt succeeds within the three-attempt budget. Late repair waits for
all three attempts to fail. The live client must then remain idle for another
25ms of virtual time while the fault stays active. The independent observer
requires unchanged retry batches, error counts, journal bytes, request count,
application count and repository revision during that interval. It also checks
the surviving current identity and phase: Unknown before rename, or the original
Recovered revision after a directory-sync error.

Only explicit storage repair releases the parked client to perform another local
save. The batch checker requires successful batches with attempt counts
`[1, 1, 2]` for early repair,
or two initial successful saves, a failed three-attempt recovery batch and one
healthy attempt after late repair. Final checks retain read-only effect recovery,
one publication application, the original marker and root, exact payload bytes,
and saved Recovered intent. Negative controls start a new batch before repair,
attempt a fourth retry, or dispatch a remote operation during local recovery.

These are complete-report seeded replays. The persistent fault is modeled by the
shared storage adapter; the late-repair client remains alive, so idle-state checks
are not satisfied merely by killing the client. The separate
`journal-read-save-outage-crash-corpus` adds a crash after exhaustion.

### Crash after failed recovery save, before retry

`journal-read-save-crash-corpus COUNT` extends the combined read/save matrix with
client-host death immediately after the Recovered save error, before journal
reload or local retry. It covers all four read faults, all four replacement-stage
errors, all four partial-write cutoffs and both application boundaries, for 64
cases per seed. Three failed reader hosts are discarded first; the repaired fifth
client recovers the original publication, attempts save 3 and is then crashed.

While that saver is down, the independent observer loads only current intent and
requires the original fingerprint. Errors before rename must leave Unknown;
directory-sync errors after replacement must expose the same complete Recovered
revision. Partial writes must retain the expected incomplete temporary prefix.
The observer requires one save failure, one save-error crash and zero local
retries, then boots client epoch 6.

The final client reloads current intent and makes exactly one additional read-only
query for the original effect. Previously persisted Recovered revision cannot
change. Its healthy local save replaces any abandoned temporary bytes. Final
auditing requires unchanged repository revision, exactly one application, the
original marker and root, exact payload bytes and saved Recovered intent.
Negative controls attempt resubmission after the crash or promote either a valid
alternate temporary intent or a truncated recovery record; the independent
query-count, identity and decoding gates must reject them.

These are seeded host-crash cases with complete-report replay and process digests.
They model a one-shot save fault; persistent save outages and native process death
remain separate boundaries.

### Read repair followed by a failed recovery save

`journal-read-save-corpus COUNT` combines each of the four read faults with each
of four replacement-stage errors and four partial-write cutoffs on save 3, which
persists Recovered intent. Both before-apply and after-apply restart scenarios
are covered, for 64 cases per seed. Three failed reader hosts must remain
quiescent before read repair. The fresh healthy reader then recovers the original
operation, encounters the selected save failure, reloads current state and retries
only the local replacement.

The save checker requires unchanged remote request count, publication application
count and repository revision across the failed save and retry. Existing storage
checks distinguish failures before rename from directory-sync errors after the
complete replacement becomes visible, and verify partial temporary bytes before
retry. Final auditing requires the original request fingerprint, Recovered intent,
matching marker and root, and exact payload bytes. The valid alternate temporary
intent from the read outage may not be promoted when the next save fails before
writing its own temporary record.

A negative control dispatches the original operation after the failed recovery
save. The independent request-count checker must reject it even when atomic
markers prevent a second application. The targeted test covers every save fault
in both application scenarios. A second negative control promotes the valid
alternate temporary intent after read repair and a pre-write recovery-save
failure; the durable-state checker must reject it for every read fault in both
application scenarios. Full report replay and independent-process digests
use the same convention as the other seeded corpora.

This combines read outages and one-shot save faults with a local retry. The
separate `journal-read-save-crash-corpus` adds host death before that retry.
The separate `journal-read-save-outage-corpus` adds persistent save faults.

### Seeded journal reads across fresh client hosts

`journal-read-corpus COUNT` injects I/O errors, permission errors, truncated JSON
and mismatched fingerprints at validated journal loading, in both before-apply
and after-apply client restart scenarios. Three failed readers are each crashed
without cleanup, and each replacement host must independently stop at the same
read fault. The observer requires the exact fault diagnostic, zero recovery
admission or extra network requests, unchanged repository revision, and unchanged
current and temporary journal bytes. A valid temporary record with a different
request identity is present throughout the outage and must never substitute for
the failed current read.

After three failures, the observer repairs reads and boots a fifth client epoch.
The healthy client reloads the original intent and performs read-only effect
recovery. Existing publication checks still require exactly one application,
matching atomic marker and root, protected payload bytes during GC, and a saved
Recovered intent. The before-apply scenario must still query an unresolved
publication before its original handler is allowed to settle. A negative control
ignores the failed read and must fail the independent recovery-admission checker
for all four faults in both application scenarios.

Each seeded case runs twice and compares complete reports, including raw intent
bytes, revisions, diagnostics, event order and virtual timestamps. The CLI emits
a digest for independent-process comparison. Read failures are injected by the
shared journal storage adapter; this extension models client-host death and
repair, not native disk or process failure. The separate native read-failure and
SIGKILL cases remain in the regression suite.

### Submission racing recovery

`client-journal-submission COUNT` runs nine native cases per iteration. A submitting
process holds the same spike-only advisory lock as recovery while it persists
Submitted intent, applies the real production publication with its atomic marker,
and persists Unknown outcome. It parks at each of those three boundaries; three
fresh recovery contenders must reject ownership contention without dispatching or
changing current or temporary intent bytes. After SIGKILL, a fresh recovery process
resolves the original operation and independently audits its marker, root, payload
and terminal journal. Killing before publication leaves the request Unknown;
killing after publication preserves its recoverable effect.

The other six cases hold recovery ownership before loading or after temporary-file
sync, in Submitted, Unknown and Recovered phases. Three fresh submission contenders
must stop at ownership acquisition. After owner death, another submission attempt
must still refuse the existing durable intent without replacing either current or
temporary bytes; recovery then resolves that original identity. A negative control
bypasses submission ownership and must fail the independent dispatch checker.
Repository revision must stay unchanged during final recovery; the submitting
owner's legitimate publication is allowed to advance it beforehand.

Submission here is a bounded admission probe for an existing journal, not a new
public submission API. The real owner uses a fresh directory per native case.
These checks add no production locking or distributed fencing. All participants
must cooperate with the advisory lock. Cases inject SIGKILL and repeat native
process runs; they are not deterministic Turmoil replays.

### Overlapping recovery processes for one intent

`client-journal-overlap COUNT` covers six native cases per iteration: Submitted,
Unknown and Recovered intents, with an owner parked either before validated
loading or after syncing a recovery temporary file. Recovery uses a spike-only
nonblocking OS file lock held across intent loading, repository reads and journal
replacement. Three independently launched contenders must fail with ownership
contention before repository dispatch and leave both current and temporary journal
bytes unchanged. The owner receives SIGKILL; a fresh process must acquire the
same persistent lock file, recover the original request, and pass an independent
journal and payload audit. Repository revision must remain unchanged.

A negative control omits ownership in the contender and must fail the independent
dispatch checker. This exercises real overlapping processes and kernel lock release
after death, rather than a modeled lock. The lock is advisory: every recovery
participant must cooperate. It does not coordinate with arbitrary writers or
provide distributed fencing, and it adds no production journal API. Native runs
are repeated crash checks, not seeded deterministic replays.

### Concurrent journal path ownership

`journal-writer-corpus COUNT` uses Turmoil to generate two writers' storage-stage
orders and virtual timestamps. Each schedule is replayed against separate modeled
journals, abandoning writer 0 after each of Write, SyncFile, Rename and
SyncDirectory. The independent checker requires the original request identity
and the expected current phase for each writer, then reloads current records and
persists recovery without borrowing the other writer's intent. Complete reports
include stage order, timestamps and exact surviving and recovered journal bytes.
Each seeded case runs twice; the CLI prints a digest for cross-process comparison.

The native test additionally enumerates all 70 order-preserving interleavings of
two four-stage replacements at all four abandonment boundaries. Its 280 cases
must produce identical reports with modeled storage and real files in separate
writer directories. A negative control deliberately shares one journal path:
writer 0 successfully syncs and renames writer 1's valid temporary record. The
identity checker must reject that apparent success against both storage adapters.

This tests the spike journal's exclusive-path assumption. It does not implement
locking, fencing, or shared-path coordination, and does not establish a new
production Casita bug. Abandonment stops subsequent storage operations without
cleanup; this extension does not inject OS process death, power loss or fsync
latency. Existing native SIGKILL cases cover process death separately. Turmoil
chooses schedules here; native filesystem operations replay those schedules
outside the simulation.

### Shared intent journal and injected persistence failures

`intent_journal` supplies the generic intent record, fingerprint validation,
Submitted/Unknown/Recovered transitions, storage interface and replacement
sequence used by both the native journal probe and Turmoil client. Native
storage uses actual files; modeled storage retains current and temporary bytes
outside the client host. A recovered intent cannot regress to Unknown, and
recovery must match the saved request fingerprint.

`journal-failure-corpus COUNT` injects one failure before each of write,
file sync, rename and directory sync, at each of the three client saves, in
both before-commit and after-commit restart scenarios. This is 24 cases per
seed, each replayed. The CLI prints a digest over all complete reports so
separate processes can compare full traces without dumping their contents.
The ordinary `corpus` still covers its 27 scenarios; use this dedicated command
or the automatically discovered marker test for the persistence failure matrix.

A failure before rename must preserve the previous current record. A directory
sync error occurs after replacement and may expose the new complete record;
callers reload before retrying. Submission waits for successful persistence,
and retries affect only the local journal. Each simulation still requires one
publication application, read-only recovery, protected payload bytes during GC,
and a saved recovered intent. Native and modeled storage tests exercise all
four errors on the initial save and both later transitions. These four failure stages are injected before I/O operations; the separate
partial-write matrix below covers incomplete temporary writes. Neither family
models power loss, persistent disk failure or storage latency.

### Journal read failures before recovery dispatch

`client-journal-read-failure COUNT` exercises 12 native combinations per
iteration: I/O errors, permission errors, truncated JSON and mismatched request
fingerprints, in Submitted, Unknown and Recovered states. A writer receives
SIGKILL at the selected boundary. Three fresh recovery processes each encounter
the same read fault and must exit with the specific diagnostic before opening
the repository. An instrumentation gate placed immediately after validated
loading proves no repository access occurs. The supervisor independently checks
that current journal bytes and the repository revision remain unchanged after
every failed process. A subsequent healthy reader recovers from the original
current file, and a separate audit verifies persisted state and exact published
payload bytes. Submitted intent remains Unknown when no marker exists.

The shared memory and filesystem tests also leave a valid temporary record
available: a failing current read must never fall back to it. They repeat failed
loads, then remove the injected fault and require the same original identity and
phase. Missing intent remains covered by the existing separate negative test.
Read faults are injected at the storage seam; the filesystem test does not rely
on chmod behavior or damage real files. This is immediate failure followed by
explicit fresh-process recovery, not a new automatic read retry policy. These
native cases are outside the seeded Turmoil corpus. Device stalls and genuine
power loss remain unmodeled.

### Persistent journal outages and bounded retry

`intent_journal::persist_bounded` retries only the local replacement. The spike
policy allows three attempts, with a 5 ms pause between attempts. Every attempt
must complete write, file sync, rename and directory sync. Seeing a complete
replacement after a directory-sync error does not establish success. Exhaustion
returns an explicit unsuccessful result; the caller stops until explicit
recovery rather than spinning or reporting a saved intent.

`persistent-journal-corpus COUNT` covers all four operation-boundary errors and
all four partial-write cutoffs, at each of the three saves, before and after
server commit. The outage remains active across attempts. In one mode, storage
is repaired after the first failure and before the next attempt; local retry
succeeds without retransmitting a request. In the other, all three attempts fail,
the client host crashes, and the observer checks quiescence for 20 ms of virtual
time before repairing storage and starting a fresh client. Recovery uses only
the surviving current intent and read-only marker queries. An initial failure
before rename leaves no durable intent, so the fresh client stops without
reconstructing or sending the request. Pending accepted bytes survive GC while
the client is down. This gives 96 cases per seed, each fully replayed.

Report fields record each retry batch, its attempt count, success or exhaustion,
errors, storage repair and observed quiescence. A negative control adds a fourth
attempt; the independent checker must reject it. Shared modeled and native
storage tests require exhaustion even when directory-sync failures leave a
readable replacement, then require a single successful attempt after repair.

`client-journal-persistent-crash COUNT` covers 24 native combinations per
iteration. A worker exhausts the same shared three-attempt policy, then receives
SIGKILL without retrying again. The supervisor checks the saved retry diagnostics;
separate recovery and audit processes use healthy filesystem storage and the
previous current intent, with exact published bytes and unchanged metadata.
Initial missing intent stays absent. Healthy recovery also uses the shared
bounded replacement and must complete in one attempt. These native repetitions
remain outside the seeded corpus; use this dedicated command or the automatically
discovered tests for the outage matrix.

The limit bounds attempts and retry pauses, not the duration of an individual
blocking filesystem call. Device stalls, power loss, automatic error
classification and concurrent journal writers remain outside this prototype's
coverage. The read-failure matrix below checks a separate fail-closed policy. The fixed retry policy is
an experiment, not a selected production default.

### Partial temporary-file writes

`partial-journal-corpus COUNT` writes an incomplete prefix of `intent.tmp`, then
returns an I/O error. Four cutoffs leave zero bytes, one byte, half the serialized
record or every byte except the last. Each cutoff is exercised at the Submitted,
Unknown and Recovered saves, before and after server commit, with either a local
retry or an immediate client-host crash. That gives 48 cases per seed, each
replayed, with a digest over complete reports for cross-process comparison.

The observer inspects actual temporary bytes and requires the exact expected
prefix, incomplete JSON and an unchanged current record. Local retry must
rewrite the temporary file completely before rename. On immediate restart,
recovery loads only the previous current record; an incomplete first save has
no current record and stops without dispatch. Later saves retain Submitted or
Unknown and resolve through read-only queries against the original operation.
The existing error-crash checks still require zero local retries before crash,
unchanged metadata during recovery, payload protection during GC and no duplicate
publication. A negative control renames the incomplete prefix over the current
journal and must fail decoding before recovery sends a request.

The shared storage tests check exact prefixes and complete replacement on retry
against both real files and modeled storage. `client-journal-partial-crash COUNT`
runs 12 fresh native cases per iteration: each cutoff and each save, followed
by SIGKILL before retry. The supervisor checks the actual temporary file's
length and invalid JSON; separate recovery and audit processes verify the prior
current intent or its absence, exact published payload bytes and unchanged
repository metadata. These native cases remain outside the seeded corpus.
The original `corpus` command still runs its 27 seeded scenarios; use the
partial-write command or the automatically discovered tests for this matrix.

Partial-write injection occurs in the temporary file before file sync or rename.
This models a short write followed by an error, not a torn replacement during
power loss. The journal remains a single-writer prototype with no concurrency locking or
production API integration. Its experimental retry policy is described above.

### Crash immediately after a journal error

`journal-error-crash-corpus COUNT` covers the same 24 failure combinations per
seed but actually crashes the Turmoil client host immediately after the save
returns an I/O error. It does not reload or retry the failed write first. A
fresh host loads only the current serialized intent, never the fixture request
or a temporary record. The observer independently checks which phase survived.

| Failed save | Failure before rename | Directory sync failure after rename |
| --- | --- | --- |
| Initial Submitted intent | No intent, stop without sending | Submitted, query and keep Unknown |
| Unknown status | Submitted | Unknown |
| Recovered status | Unknown | Recovered |

For later saves, the original request has already been accepted. The before-commit
case runs GC with its staged payload protected while the client is down, then
resumes that original handler. The fresh client uses read-only marker queries
and persists the recovered effect; no request is resubmitted. For the Recovered
save, the first host has already queried the effect and the fresh host queries
it again, even if a complete Recovered record survived. Every successful run
requires zero local journal retries, one error followed by one client crash,
an unchanged repository revision during recovery, exact request identity and
payload bytes, and removal of unrelated garbage. A missing initial intent
requires zero publication or query requests and no published object or root.

Negative controls reconstruct a missing request from fixture defaults or
resubmit a surviving intent. Both must fail the checker. A resubmission may
return the existing marker without applying again, so request counts and
read-only query counts are checked in addition to application count.

`client-journal-error-crash COUNT` covers 12 combinations per iteration against
the real local repository and filesystem journal. An isolated native worker
reports the injected I/O error, then the supervisor SIGKILLs it before any retry.
Separate recovery and audit processes load the surviving current journal or
verify its absence. Initial missing intent leaves no root or marker; later
intent recovery requires exact payload bytes and an unchanged repository
revision. These native repetitions are outside the seeded corpus. These four I/O error stages occur before operations start; the separate
partial-write cases cover temporary prefixes. Neither family models power loss,
persistent disk errors or storage latency.

### Client intent journal across native process death

`client-journal COUNT` runs six fresh native cases per iteration. A supervisor
kills a worker with SIGKILL at an explicit checkpoint, starts an independent
recovery process, then starts a third process to audit its persisted result.
The spike-only journal saves the full operation identity, root, payload and
request fingerprint before publication. Updates write a temporary JSON file,
sync that file, rename it over the journal, then sync the parent directory.

The six boundaries are after the submitted intent is durable but before
publication; after server commit but before saving Unknown; after Unknown is
durable; after writing and syncing the temporary Recovered record; after
renaming that record but before syncing the directory; and after the entire
Recovered update is durable. The temporary-file crash must retain Unknown;
the post-rename process-death case must expose a complete Recovered record.

Recovery loads only the saved intent and queries the real local repository's
atomic operation marker and root. A matching marker must lead to exact payload
bytes; no marker leaves the intent Unknown even when its old status was
Submitted. Recovery never publishes or retries. The repository revision must
remain unchanged, and the third process must observe the saved terminal status.
Negative controls delete the journal or change its operation identity without
updating its fingerprint; recovery must reject both.

This tests native process death, not power loss or a storage device's durability
guarantees. The initial worker drives client and server boundaries in one
process; actual TCP faults and independently restarted simulated hosts remain
covered by Turmoil. The journal is a single-writer spike without file locking,
retention policy or production API integration. An absent marker does not prove
that a delayed request cannot still commit, so it remains unresolved. This
native probe is excluded from the deterministic seeded corpus.

### Process death with persistent payloads

`backend-crash COUNT` runs two cases per iteration using the standard
`Repository::local` profile, which combines Turso metadata with the packed
filesystem payload store and process coordination. These native runs are outside
Turmoil and are excluded from the seeded corpus.

The supervisor launches a writer in a fresh temporary repository. One case has
a single publication; the other has two concurrent mutation sessions with
different payloads competing for one operation ID. Each fixture is 768 KiB plus
nine bytes, with a shared 256 KiB prefix and distinct remaining bytes, so reads
exercise the packed chunk path. The competing writer must observe exactly one
successful commit and one marker conflict.

After publication returns, the writer calls `process::exit(73)` while its
repository and staging session handles remain alive. It sends no response or
saved outcome to the supervisor and performs no explicit flush or Rust handle
cleanup after commit. The supervisor checks that exit boundary and starts a
separate reader process against the same directory. Only the persisted
operation marker identifies the winning request. The reader requires:

- The marker, root, and logical object agree on the expected payload.
- The entire payload matches independently generated fixture bytes, and
  pack I/O counters prove packed chunks were read.
- A conflicting retry cannot insert an object, replace the marker or root, or
  advance the recovered revision; the losing writer's logical object is absent.
- Integrity checks find no corruption or unchecked objects. Collectible staging
  residue is permitted and counted in the report, because rejected publications
  can leave unreferenced physical bytes.

A negative control publishes without a marker, exits at the same boundary,
and requires the independent reader to reject recovery. Each child has a
30-second deadline and is killed and reaped on timeout. This tests process exit
after a completed publication, with metadata and payload recovery in another
process. It does not inject death inside the transaction, SIGKILL, power loss,
or filesystem faults, and does not replay the original full `CommitResult`.

### SIGKILL around the metadata transaction

The native library's existing test-only crash harness now has an
`operation-marker` scenario, run by the `operation_marker_` test filter above.
It reuses the existing transaction checkpoints; no production failpoint or
simulator dependency is added. A control run discovers the checkpoints before
the supervisor starts fresh repositories for six exact kill points:

| Checkpoint | Required recovery outcome |
| --- | --- |
| `state-object-inserted:1` | Roll back the new object, root, and marker |
| `state-root-changed:1` | Roll back the new object, root, and marker |
| `state-metadata-changed:1` | Roll back the new object, root, and marker |
| `before-state-commit:1` | Preserve the baseline revision and root; marker remains absent |
| `after-state-commit:1` | Recover the winning marker, object, root, and packed payload |
| `publication-acknowledged:1` | Recover the winning publication and reject a conflicting retry |

The writer stages two distinct packed payloads and races two mutation sessions
for the same operation ID. An older acknowledged payload remains protected by
a separate root throughout. At a selected checkpoint the worker writes a
handshake and parks while holding the recorder mutex; the supervisor sends
SIGKILL on Unix, reaps it, and starts an independent verifier. A polling delay
only waits for the handshake and does not choose the kill boundary.

The verifier reads the marker and graph independently, checks exact bytes, and
requires rolled-back insertions to remain absent before commit. After rollback
a fresh publication with the absent ID must succeed. After commit a conflicting
retry must leave the marker, root, objects, and revision unchanged. In both
branches GC must preserve the newly published graph, the previously acknowledged
payload, and the marker, with a healthy integrity report. A negative control
tells a reader killed before commit to expect a committed publication; its
graph audit must reject that false outcome.

These are native filesystem and transaction tests with precisely selected
process-death boundaries, not seeded deterministic runtime replays. They do
not simulate power loss, torn writes, or failed fsync calls. The baseline
revision is saved before arming the writer solely as an independent rollback
oracle; no publication result or winner is saved for the recovery process.

### Seeded simulation cases

The marker protocol sends the complete tiny-blob request over simulated TCP:
operation ID, root name, payload bytes, and fixture actor identity. The server
stages and verifies those bytes through a production mutation session, then uses
`publish_with_metadata` to commit an expected-absent marker, verified object,
and root together. The marker stores a BLAKE3 fingerprint of the complete
serialized request. It does not store a reply or the generated commit revision.

On a retry, the server reads the application record. A matching fingerprint
returns `RecoveredEffect { request_fingerprint, observed_revision }`; a mismatch
returns `RejectedReuse`. Fresh commits return `Applied { revision }`. This
protocol has no daemon result cache, separate journal, or opaque-command
registry. `RecoveredEffect` is a domain outcome and is never converted to a
fabricated `CommitResult`. The earlier TCP result-journal scenarios remain as
comparisons for full original-response replay under that stronger modeled
durability assumption.

The new seeded cases are included in `corpus` and can be run separately through
`marker-corpus`. They require exactly one target application, consistent marker
and root identity, exact payload reads through the repository after GC, and
actual removal of unreferenced garbage. The advanced restart case independently
checks the observed revision against a backend snapshot, and both the original
publication and intervening publication must survive GC. Competing writers
both stage before publication is admitted; distinct seed-derived delays choose
both winning actors across the corpus without simultaneous wake-order races.

The memory metadata backend represents durable logical state outside the
simulated host. Host restart is modeled with `Sim::crash` and `Sim::bounce`;
it is not a real filesystem restart. The native backend and SIGKILL tests above
separately exercise the same atomic publication API against persistent storage.
Operation IDs and markers are retained for the entire fixture; expiration,
namespace authorization, and a production transport format are outside this
spike. An existing marker proves the original effect committed, rather than
promising that later writers have never changed its root.

Publication and recovery-query messages now use an envelope with an explicit
`query_only` flag. The request fingerprint covers the publication intent, so
switching to query mode does not change the operation identity. A read-only
query returns the existing effect or `MarkerAbsent` and never stages payloads
or commits metadata. The bounded client maps transport failure, a deadline,
or an absent marker to `Unknown { operation, request_fingerprint }`; absence
does not prove that an already accepted request will never apply.

Two partition cases pause an accepted handler with its staging session alive,
before application or after commit but before acknowledgement. The client
calls Turmoil's actual `partition` control, waits for its 100 ms publication
deadline, and attempts a read-only recovery query under the same deadline.
The query can fail immediately when connecting across the partition; timeouts
and transport failures are counted separately. Both underlying application
states must produce the same unknown outcome with the original identity.

An independent fixture observer runs GC while the client remains uncertain.
The paused staging pin must preserve unpublished bytes before application;
the root must preserve published bytes after application. Unreferenced garbage
must be removed, proving the collector ran. After `repair`, a query made while
the original handler is still paused must return an absent marker and leave
the client uncertain, with no new application. Releasing the original handler
then lets another read-only query recover the effect. Exactly one publication
may apply, with the marker, root, and exact payload intact through final GC.
The before-application case therefore checks that queries cannot secretly
resubmit a pending publication. Negative controls separately classify a
deadline as definitive failure and make a query republish; both must be caught.

Client-restart cases add a serialized client intent journal outside the
simulated client host. Before sending, the client saves the complete request,
its fingerprint, and `Submitted` status. At the 100 ms reply deadline it saves
`Unknown`, then asks the driver to crash its host. The server remains paused
before application or after commit and continues to own the accepted request.

A separate observer runs GC while the client is down. It requires unpublished
or committed bytes to survive and unreferenced garbage to disappear, then asks
the driver to bounce the client. The new client epoch constructs no request
from fixture defaults: it must deserialize the saved bytes, validate the
fingerprint and unknown status, and perform read-only recovery. Before server
application, an absent marker keeps the client uncertain until the original
handler resumes. After commit, the first query resolves the existing effect.
The client saves a terminal `Recovered` status with the observed revision.

The oracle requires two client epochs, one actual host crash and bounce, three
intent writes, one intent reload, and exactly one server application. It checks
the persisted terminal identity and revision independently, plus the original
marker, root, and exact bytes after final GC. Negative controls erase the saved
intent at crash or make the restarted client resubmit the pending request; both
must fail their specific recovery check. The intent journal is modeled as
serialized bytes retained outside the host, not a tested filesystem outbox.
Client host crash runs Turmoil's teardown behavior, not a native process kill;
disk durability of this client journal remains a separate next step.

| Scenario | Injection | Required outcome |
| --- | --- | --- |
| `concurrent-pins` | Synchronize two initial GETs | A CAS conflict occurs; both acknowledged tokens remain |
| `lost-response` | Commit the first pin PUT and discard its response | Recover the original token; exactly one write and pin |
| `pin-vs-deletion` | Race admission with varied TCP latency | Exactly one side succeeds and its token remains |
| `crashed-owner` | Crash a host after acknowledged registration | Durable pin remains and prevents deletion after time advances |
| `cancel-before-apply` | Submit an owned metadata request, cancel publication, run GC, then apply it | Staged bytes survive; eventual root and its graph remain readable |
| `cancel-after-apply` | Apply metadata, pause acknowledgement, cancel publication and run GC | Committed graph survives both GC passes |
| `lost-commit-response` | Apply metadata, cancel caller, run GC and deliver a transport error | Unknown outcome preserves the committed graph |
| `chunk-delete-before-apply` | Pause an owned chunk DELETE before application, cancel collector caller, attempt conflicting recreation | Writer waits for delete settlement; shared chunks survive and both roots read correctly |
| `chunk-delete-after-apply` | Apply DELETE, pause acknowledgement, cancel collector caller, attempt recreation | Physical ownership remains until acknowledgement; restoration follows settlement |
| `chunk-writers-before-apply` | Three independent writers race with a DELETE paused before application and force conflicting metadata commits | All wait for settlement, hold separate overlapping pins, retry stale revisions and retain every acknowledged root |
| `chunk-writers-after-apply` | Three writers attempt recreation while DELETE acknowledgement is paused | No early pin admission; all roots and shared chunks survive final GC |
| `chunk-cancel-writer-before-apply` | Three writers wait for a DELETE paused before application; abort one after all stage B, then collect while the other two are paused | Cancelled pin drains and root remains absent; the surviving pins preserve unpublished B through GC and both writers publish safely |
| `chunk-cancel-writer-after-apply` | Repeat cancellation with DELETE acknowledgement initially paused | Both surviving roots and A remain readable after final GC; all ownership drains |
| `network-cancel-before-apply` | Adopt a TCP commit request on a separate metadata host, pause application, cancel caller and collect | Owned request and staging protection survive cancellation; the eventual rooted graph remains readable |
| `network-lost-ack` | Apply a TCP commit, cancel caller while response is held, collect, then close the socket without its acknowledgement | Real EOF causes a retry of the same operation ID on a new connection; server returns cached result without applying twice |
| `network-restart-before-cache` | Apply metadata and model its result as jointly durable, pause before populating daemon cache, cancel caller, run GC, then crash and bounce the metadata host | Clear the daemon cache; retry through a new listener must deserialize the original journal reply without reapplication or revision changes |
| `network-restart-after-cache` | Crash after cache population but before acknowledgement and discard the cache | Recovery must use journal bytes, preserve the original revision and counters, and leave the committed graph readable |
| `marker-lost-ack` | Atomically commit the marker and publication, then close TCP without a reply | Retry reads the marker and returns an explicit recovered effect without applying again |
| `marker-restart` | Hold the applied reply, crash the server, and connect to its new listener | Recovery reads the marker from logical state without a result cache or journal |
| `marker-restart-advanced` | Commit an unrelated rooted payload while the first reply is held, then restart | Recover the first effect at the newer observed revision; both publications survive GC |
| `marker-id-reuse` | Reuse an operation ID with another payload and fingerprint | Reject the second request and preserve the first publication |
| `marker-writers` | Two requests stage distinct payloads concurrently for one operation ID | Admit exactly one publication, reject the conflicting request, and preserve the winner through GC |
| `marker-partition-before-apply` | Partition after staging, expire the publication deadline, attempt recovery, and collect before application | Retain an unknown outcome and staged bytes; an absent-marker query cannot publish; after repair the original handler commits exactly once |
| `marker-partition-after-apply` | Partition after commit with acknowledgement held, expire the deadline, and collect | Retain the same unknown outcome and the committed graph; a read-only query after repair resolves the original effect |
| `marker-client-restart-before-apply` | Save the unknown intent and crash the client while the accepted server handler is paused before commit | GC retains staged bytes; a fresh client reloads the original identity, keeps absence unknown, and resolves the original handler's publication without resubmitting |
| `marker-client-restart-after-apply` | Crash the client after the server commits but before acknowledgement | Reload intent bytes and recover the existing marker through a read-only query; persist the recovered outcome and preserve the graph through GC |
| `stale-mark` | Complete old logical mark, publish another graph, resume old collection | Reject stale revision before physical sweep; fresh GC preserves new graph |

Repository cases start with a rooted shared blob, stage the same bytes again
plus a new blob and a directory linking both, and repoint the root. The stale
mark case also includes an unreachable logical record to force a metadata prune. This models
shared **payloads**, not shared physical chunks: `MemoryBlobStore` has no chunk
layer. Unreferenced garbage must disappear, so checks also establish that GC
actually ran. The oracle checks root identity, required records, expected blob
bytes, and root access through a production retention hold. Spill metrics must
report zero files for these bounded graphs.

The metadata worker owns submitted mutations independently of the caller and
its acknowledgement future. Application and delivery are separate events with
seed-derived delays and explicit phase gates. Caller cancellation does not
cancel a submitted request. Casita's production tracked publication task keeps
its staging pin while this request settles. The stale-mark pause occurs when the collector starts physical inventory,
after completing its logical mark. An unreachable logical record forces the
prune path. Pausing before pin inventory capture would permit the collector to
observe publisher protection and conservatively retain everything instead.

Network metadata cases run the commit worker on a separate Turmoil host.
Every commit, including initial publication and collection mutations, sends a
JSON operation ID and expected revision over TCP. The receiving handler adopts
the submitted mutation and owns it independently of the connection's result.
Application and acknowledgement have separate gates. After application, a result
cache records the expected revision and reply. The lost-ack case closes the
socket before writing its response, so the client observes real EOF and retries
the same operation ID through a fresh connection. The server returns its cached
result. Checks require exactly one target application, one lost acknowledgement,
one transport failure, one duplicate reply, no server errors and no unadopted
commands. Existing graph, pin-drain and garbage-removal oracles also apply.

This is a commit RPC model, not a deployable metadata protocol. Verified Casita
mutations do not expose serialization; a test-only registry transfers their
opaque values when the TCP envelope arrives. Snapshots, collection leases and
entropy sources remain direct in-process interfaces. The result journal is a
model of persistent storage, held outside host runtimes.
It has no filesystem or eviction policy. Simultaneous duplicate requests,
partitions and multi-client recovery are not modeled. The retry is
safe within this model because its operation ID survives the failed response
and the server retains the matching result. It is not a general retry policy
for an arbitrary production metadata backend.

Restart cases drive actual `Sim::crash` and `Sim::bounce` calls after caller
cancellation and GC while acknowledgement is pending. Host handlers and sockets
are destroyed; the daemon result cache is cleared. The cases pause before and
after cache population, discarding one or two entries respectively. A new
listener increments the server epoch. The original client operation retries
only after that listener is ready. With no cache entry available, the restarted
worker deserializes its result from the journal and returns it over TCP.

The persistence boundary is deliberately a model: memory metadata and a map
of serialized journal entries survive outside the host runtime. Successful
metadata application and result journaling have no suspension between them,
representing joint durability for the bounded memory backend. This is an
assumption that a production backend must implement transactionally; it does
not validate an on-disk atomic commit, fsync, torn writes, or SIGKILL. The reply
contains the original revision and every `CommitResult` counter. It must match
the original reply byte for byte, its revision must match committed state, and
exactly one application, restart, journal recovery, duplicate reply and lost
acknowledgement must occur. The server must reach epoch two. Existing graph,
garbage-removal and ownership-drain checks remain required.

A negative control deliberately persists metadata without journaling the target
result, then crashes at the same boundary. Root data survives, but the old
operation's reply cannot be recovered. The checker must report loss of the
original commit result. This demonstrates the application/result atomicity
requirement and guards against passing recovery by keeping an old daemon cache.

Chunk cases use production `ChunkedBlobStore` manifests, FastCDC chunks, compressed
objects, pin admission and physical GC. Two distinct 12 KiB payloads share an
8 KiB prefix. A remains rooted while unrooted B is collected. An independent
DELETE worker pauses before application or before acknowledgement. Cancelling
the collector caller leaves production collection ownership alive. A competing
writer acquires real `DataPinLease` protection, recreates B's missing objects,
and publishes B through a mutation session. Every unique B chunk must be deleted
and restored after DELETE acknowledgement. Shared chunks must be reused without
deletion or restoration. Fresh readers verify both roots and exact payload bytes
before and after a final GC; settled ownership must be empty.

Competing-writer cases use three independently constructed TCP pin clients and
separate repository handles. All restore the same B payload and publish distinct
roots `B/0`, `B/1` and `B/2`. The fixture contains five chunks shared with A and
four unique B chunks. Before any restoration, an inventory must show three
distinct staging tokens protecting the overlapping physical resources. All
writers must attempt admission while DELETE is held, and none may be admitted.
A second gate captures all three initial commits at the same revision. Seeded
application delays vary which writer wins. At least two stale revisions must
exercise production publication retry; every acknowledged root must remain
readable before and after final GC. Concurrent duplicate puts contain identical
verified bytes. The early delete-claim release negative control also runs with
three writers and must detect a late DELETE destroying their publications.

Writer-cancellation cases pause all three writers after `stage_existing`, before
any metadata submission. The coordinator aborts writer 0 and awaits cancellation,
then drains its cleanup. The original physical token must disappear while the
two surviving tokens remain registered. A real GC runs while both survivors are
still paused and B has no root. A fresh chunk reader must read B's exact bytes,
and both surviving tokens must remain owned. Only then can writers 1 and 2
publish their distinct roots, exercising a forced stale-revision retry. Root
`B/0` must remain absent, roots A, `B/1`, and `B/2` must read correctly before
and after final GC, and settled ownership must be empty.

A separate negative control removes the remaining staging pins while the two
writer tasks still hold their leases, then performs the same GC. The checker
must specifically report lost staged chunks. This establishes that survival
requires the remaining ownership, rather than an accidentally retained root or
cached payload. Cancellation here is before submission; the repository cases
separately cover cancellation of submitted metadata requests.

These gates use individually owned oneshot channels and ordered releases. An
initial experiment with simultaneous Tokio barrier wakeups produced different
publication orders for identical seeds. The final harness controls those wakeups
and first-commit delays explicitly rather than normalizing the divergent traces.
This remains bounded exploration of deliberately forced contention windows.

The chunk fixture is generated with production chunking and compression before
simulation, then cached. These CPU operations use `spawn_blocking`, so their
execution is outside the simulated schedule. Recreation restores those verified
wire objects under production leases and uses `stage_existing`; the ordinary
chunk upload pipeline is not covered. Object reads, range reads and canonical
inventory use an in-memory object store. Submitted deletes outlive their caller
and have separate application and acknowledgement events.

Five semantic negative controls must fail for specific semantic reasons: ignoring storage
CAS loses an acknowledged token, and releasing a pending staging pin before GC
loses unpublished payloads. Releasing a physical delete claim before settlement
allows a late DELETE to destroy a newly published chunk. Transport failure alone
does not satisfy these controls. Releasing surviving staging ownership after
writer cancellation must expose loss of the unpublished chunk graph. Applying
metadata without durably modeling its result must expose lost reply recovery
after host restart.

## Determinism boundary

Default `explicit-entropy` uses isolated, domain-separated seeded sources for
pin actors and memory metadata. Clones share their source's sequence. These
sources are for tests only and must never provide production ownership IDs.
The payload wrapper forwards production safety hooks and sorts memory blob
inventories to remove randomized HashMap traversal from observable GC ordering.
No global entropy override is required for default replay.

`--no-default-features` retains the earlier experiment: pin tokens use OS
entropy, so pin traces and outcomes replay but raw ledger bytes differ. Memory
metadata still uses its explicit seeded source in repository cases. The original
research compared S2's `mad-turmoil` symbol interception too. Its global RNG needed
serialized runs and warming a per-thread HashMap entropy cache before reseeding.
That comparison remains in the original research worktree; its feature, dependency
and symbol overrides have been removed from this contribution.

### Runtime isolation for background lease completion

The concurrent cancellation replay regression found a production runtime
isolation bug in `metadata/lease.rs`: task counters were keyed by Tokio runtime
ID, but completion notified every runtime through a process-global `Notify`.
An unrelated OS thread could therefore wake a simulated drain at a different
virtual instant, changing writer admission order and complete replay reports.
This was a scheduling defect; the failing reports still passed their data
correctness checks.

Each runtime entry now owns its completion notification. Drain waiters register
under the tracking lock before awaiting, avoiding lost wakeups, and reacquire
the current entry after each notification so concurrent drains can remove idle
entries safely. Cancellation, cleanup errors and panic reporting retain their
existing behavior.

The production regression directly counts wakes across two Tokio runtimes.
The spike regression starts four OS threads together and requires two complete
cancellation reports per thread to equal a sequential baseline on both sides of
the DELETE boundary. It fails on the original implementation and passes after
the fix. The full default spike suite is also checked with parallel test threads.
The historical symbol-interposition comparison required serialization.

## Scope and remaining gaps

- Ordinary metadata cases use an in-process independent worker. Network cases
  send commit envelopes to a separate simulated TCP host; verified commands
  use a shared registry and snapshots and leases remain in process. Pin
  operations also use simulated TCP.
- Payloads and metadata are memory implementations. S3 HTTP, AWS SDK retries,
  WAL3, Turso, filesystem locks, fsync, packed catalogs and the chunk upload CPU pipeline are
  outside this harness. Existing real-backend tests remain necessary.
- Explicit entropy covers the composed memory and object-ledger paths. File
  ledgers, persistent metadata engines and other dependency randomness are
  not claimed to be controlled.
- Gates force critical windows; seed variation changes latency and host order.
  This is not exhaustive task-poll exploration or arbitrary fault generation.
  The `select!` gates have only one ready branch by construction; general
  competing selects would need separate Tokio scheduler/RNG evaluation.
- Turmoil host crash drops runtime futures and runs Rust destructors. The crash
  case uses an explicit durable token and waits for admission. It does not
  model SIGKILL or a request landing after the requesting process dies.
- The TCP pin server implements full GET and conditional PUT. Unused operations fail.
  Chunk storage uses in-process workers with range reads and sorted inventories.
  Server crash persistence requires a separate durable-storage model.

## Verification

Verified on x86_64 Linux with Rust 1.97.1 and the included lockfile:

- Metadata-restart extension: four repository tests passed, including the
  missing-journal control and the 32-seed eight-scenario replay corpus. The CLI
  additionally checks the recovered reply revision against committed state.
  A 256-seed restart corpus passed 512 cases with identical full replay (1,024
  simulations). Both restart cases also replayed identically across independent
  processes at seed 7: server epoch two, one host crash and bounce, one target
  application, one journal recovery, one transport failure, the exact original
  reply recovered and the committed graph readable. One and two volatile cache
  entries were discarded in the before-cache and after-cache cases respectively.
  The combined eighteen-scenario CLI passed a two-seed smoke corpus (36 cases,
  each replayed). Spike Clippy across all targets/features, both formatting
  checks and `git diff --check` passed. Durability remains a serialized-journal
  model; this extension changes only the standalone spike and documentation.
- Network metadata extension: all three repository tests passed, including the
  network early pin-release control and the 32-seed six-scenario replay corpus.
  A 256-seed network corpus passed 512 cases with identical full replay (1,024
  simulations). Both network cases also replayed identically across independent
  processes at seed 7. The lost-ack trace showed one target application, one
  dropped acknowledgement, one transport failure, one cached duplicate reply,
  no server errors, and a readable committed graph. The combined sixteen-scenario
  CLI passed a two-seed smoke corpus (32 cases, each replayed). Spike Clippy across
  all targets/features, both formatting checks and `git diff --check` passed.
  This extension changes only the standalone spike and documentation.
- Writer-cancellation extension: all nine default tests passed across the saved
  and resumed runs. The chunk tests separately completed together, including
  the control that removes surviving staging protection. The interrupted full
  run recorded eight test passes; only its unfinished ninth test was resumed.
  A 256-seed cancellation corpus passed 512 cases with identical full reports
  on replay (1,024 simulations). Both cancellation cases also replayed
  identically across independent processes at seed 7: the cancelled physical
  token disappeared, two surviving tokens protected unpublished B through GC,
  both surviving roots published after a stale-revision retry, and the cancelled
  root remained absent. The combined fourteen-scenario CLI passed a two-seed
  smoke corpus (28 cases, each replayed). Spike Clippy across all targets/features,
  both formatting checks and `git diff --check` passed. This extension changes
  only the spike and its documentation.
- Competing-writer extension: all eight default tests passed, including the
  three-writer early delete-claim release negative control. A 256-seed writer
  corpus passed 512 cases with identical full replay (1,024 simulations).
  Both writer scenarios also replayed identically across independent processes
  at seed 7, with three distinct overlapping staging tokens, two stale revision
  conflicts, and every root readable after GC. The combined twelve-scenario CLI
  passed a two-seed smoke corpus (24 cases, each replayed). Spike Clippy across
  all targets/features, both formatting checks and `git diff --check` passed.
  This extension changes only the standalone spike and its documentation.
- Before competing writers, the shared-chunk extension passed all seven default
  tests, including all three negative controls. A 256-seed chunk corpus passed
  512 cases, each replayed
  with identical reports (1,024 simulation runs). The combined ten-scenario
  CLI passed a two-seed smoke corpus (20 cases, each replayed). Spike Clippy
  across all targets/features, both formatting checks and `git diff --check`
  passed. No additional production changes were made for this extension.
- Before the chunk extension, default explicit-entropy configuration: all five
  tests passed. A 256-seed CLI corpus passed 2,048 cases (all eight scenarios), each replayed with
  identical reports. Pin/deletion races admitted the writer on 230 seeds and
  deletion on 26 seeds.
- Before the chunk extension, OS-entropy comparison: all five tests passed,
  including the assertion that pin bytes differ while normalized outcomes replay.
- Before the chunk extension, optional symbol-shim comparison: all five tests
  passed.
- The all-features library compatibility check passed.
- Both negative controls failed specifically for missing acknowledged ownership
  or missing pending-publication protection.
- Native regression checks passed: 137 metadata tests (17 existing benchmarks
  and special-environment cases ignored), eight publication retry tests, and
  19 integration tests across closure publication, online collection,
  publication cancellation, and publication snapshot release.
- Portable experimental configuration passed 62 unit tests and six doctests.
- Native library Clippy and spike Clippy across all targets/features passed
  with warnings denied. Root and spike formatting
  checks and `git diff --check` passed.

Production regression commands (run in addition to the spike commands above):

```console
cargo test --offline -p casita --no-default-features --features native,experimental --lib metadata::
cargo test --offline -p casita --no-default-features --features native,experimental --lib publication_retry_tests::
cargo test --offline -p casita --no-default-features --features native,experimental --test publication_cancellation --test online_collection --test closure_publication --test publication_snapshot
cargo test --offline -p casita --no-default-features --features experimental
cargo clippy --offline -p casita --no-default-features --features native,experimental --lib -- -D warnings
cargo check --offline -p casita --all-features --lib
```

The native test profile emits an existing unused `directory_bytes` helper warning;
the portable profile emits an existing unused `RepositoryGeneration::new` warning.
These files were not changed. No live S3 or full-disk integration suite was run.
The earlier production and optional comparison check output is retained locally
in `target/final-verification.log`. Chunk extension checks are recorded separately
in `target/chunk-verification.log`. Competing-writer checks are recorded in
`target/multiwriter-verification.log`. Independent-process comparison results
are in `target/multiwriter-cross-process.log`; the full seed-7 reports are in
`target/chunk-writers-before-apply-seed-7.json` and
`target/chunk-writers-after-apply-seed-7.json`. These local artifacts are ignored;
the CLI commands reproduce them.

Writer-cancellation logs are retained in `target/cancellation-initial.log`,
`target/cancellation-verification.log` (the interrupted run), and
`target/cancellation-resumed-verification.log` (the completed remaining checks).
Independent-process results are in `target/cancellation-cross-process.log`;
full seed-7 reports are in `target/chunk-cancel-writer-before-apply-seed-7.json`
and `target/chunk-cancel-writer-after-apply-seed-7.json`.

Network check output is retained in `target/network-initial.log` and
`target/network-verification.log`. Independent-process results are in
`target/network-cross-process.log`, with seed-7 reports in
`target/network-cancel-before-apply-seed-7.json` and
`target/network-lost-ack-seed-7.json`.

Restart check output is retained in `target/restart-initial.log` and
`target/restart-verification.log`. Independent-process results are in
`target/restart-cross-process.log`, with seed-7 reports in
`target/network-restart-before-cache-seed-7.json` and
`target/network-restart-after-cache-seed-7.json`.

Real backend marker checks are retained in `target/backend-marker-initial.log`
and `target/backend-marker-verification.log`. Three targeted tests cover lost
acknowledgement recovery, competing writers, and the missing-marker negative
control. The `backend-marker 32` command exercises 64 fresh database cases;
these are repeated native runs, not deterministic replays.

Process-death output is retained in `target/backend-crash-initial.log`,
`target/backend-crash-diagnosis.log`, and `target/backend-crash-verification.log`.
The three integration tests cover single-writer recovery, competing-writer
recovery, and a missing-marker negative control. `backend-crash 32` exercises
64 fresh repositories, each with separate writer and recovery processes.
Clippy output is retained in `target/backend-crash-clippy.log`.

Transaction-boundary checks are retained in `target/transaction-crash-initial.log`
and `target/transaction-crash-verification.log`. The targeted native tests cover
six SIGKILL boundaries plus a wrong-outcome negative control and a control run.

Marker-protocol output is retained in `target/marker-rpc-initial.log` and
`target/marker-rpc-verification.log`. Before the partition extension, three targeted tests covered the replay
corpus and negative controls for omitted atomic markers and ignored request
fingerprints. `marker-corpus 256` then covered 1,280 cases with two runs each;
`corpus 2` covered 46 cases across the original 23 seeded scenarios. Independent-process
comparisons and representative reports are retained in
`target/marker-rpc-cross-process.log` and the corresponding marker seed JSON
files. These are local ignored artifacts; the commands above reproduce them.

Partition extension output is retained in `target/marker-partition-initial.log`,
`target/marker-partition-diagnosis.log`, and
`target/marker-partition-verification.log`. Before client restart was added, five marker-protocol tests covered
all seven scenarios at 32 seeds with full replay plus four negative controls.
The partition extension's `marker-corpus 256` covered 1,792 cases with two runs each, including
512 partition cases; `partition-corpus 2` checks its dedicated CLI route and
`corpus 2` covered 50 cases across its 25 seeded scenarios. Representative seed-7
reports and independent-process comparisons are retained in
`target/marker-partition-cross-process.log` and corresponding seed JSON files.

Client-restart output is retained in `target/marker-client-restart-initial.log`
and `target/marker-client-restart-verification.log`. Seven marker-protocol tests
cover all nine scenarios at 32 seeds with full replay plus six negative controls.
`marker-corpus 256` now covers 2,304 cases with two runs each, including 512 client
restart cases. `client-restart-corpus 2` checks its dedicated CLI route, and
`corpus 2` covers 54 cases across all 27 seeded scenarios. Independent-process
comparisons and representative seed-7 client-restart reports are retained in
`target/marker-client-restart-cross-process.log` and corresponding seed JSON files.

Native client-journal checks are retained in `target/client-journal-verification.log`,
`target/client-journal-regression.log`, and `target/client-journal-clippy.log`.
Three targeted tests cover six SIGKILL boundaries and two identity negative
controls. At the native journal stage, the standalone suite passed 27 tests, and Clippy
with all targets and features passes with warnings denied. `client-journal 16`
covers 96 fresh cases; machine-readable reports are
retained in `target/client-journal-corpus.jsonl`. These are repeated native
checks, not deterministic replays.

Shared-journal verification is retained in `target/shared-journal-verification.log`,
`target/shared-journal-regression.log`, and `target/shared-journal-clippy.log`.
At the shared-journal stage, all 32 tests and Clippy with all targets and features
passed. The new
tests cover shared state transitions, native and modeled replacement failures,
and 384 seeded failure cases with full replay. The extended CLI run
covers 768 seeded cases per process with full replay; independent processes
compare complete-report digests in `target/shared-journal-cross-process.log`.
The 48 fresh native crash reports using the shared implementation are retained
in `target/shared-journal-native-corpus.jsonl`. Formatting and diff checks pass.

Journal error-crash checks are retained in
`target/journal-error-crash-verification.log`,
`target/journal-error-crash-regression.log`,
`target/journal-error-crash-final-marker.log`, and
`target/journal-error-crash-clippy.log`. At that stage, the full standalone suite
passed 36 tests. The new marker test covers 384 seeded error-crash cases with full replay
and two negative controls; the new native test covers all 12 error/SIGKILL
combinations. Extended runs cover 768 seeded cases per process, each replayed,
with complete-report digests compared in
`target/journal-error-crash-cross-process.log`. The 96 fresh native error-crash
reports are in `target/journal-error-crash-native-corpus.jsonl`.

Partial-write verification is retained in `target/partial-journal-verification.log`,
`target/partial-journal-regression.log`, and `target/partial-journal-clippy.log`.
All 41 standalone tests and Clippy with all targets and features pass, along
with formatting and diff checks. Two new shared storage tests cover all cutoffs
and saves with real and modeled files. The new marker test covers 768 seeded partial-write cases with full replay,
including both retry and crash paths; a separate negative control promotes a
truncated record. The new native test covers all 12 cutoff/SIGKILL combinations.
Extended CLI runs cover 1,536 seeded cases per process, each replayed, with
matching complete-report digests in `target/partial-journal-cross-process.log`.
The 96 fresh native partial-write/SIGKILL reports are retained in
`target/partial-journal-native-corpus.jsonl`.

Persistent-outage checks are retained in `target/persistent-journal-verification.log`,
`target/persistent-journal-regression.log`, and `target/persistent-journal-clippy.log`.
All 46 standalone tests, Clippy with all targets and features, formatting and
diff checks pass. Two shared tests cover exhaustion and repair against modeled
and real storage.
The new marker test covers 1,536 seeded cases with full replay; a negative
control attempts a fourth save. The native test covers all 24 failure/SIGKILL
combinations. Extended CLI runs use 16 seeds for 1,536 cases per process, each
replayed, with complete-report digests compared in
`target/persistent-journal-cross-process.log`. The 96 fresh native outage/SIGKILL reports are retained in
`target/persistent-journal-native-corpus.jsonl`.

All 49 standalone tests pass with the documented serial invocation.
Journal-read checks are retained in `target/read-journal-clippy.log`,
`target/read-journal-regression-serial.log`, and
`target/read-journal-native-corpus.jsonl`. Two shared tests cover all four read
faults in three intent phases against memory and filesystem storage. The native
test and dedicated CLI each cover 12 fresh writer/SIGKILL combinations, 36
failed recovery processes, and 12 subsequent healthy recovery/audit pairs.
Clippy with all targets and features, formatting and diff checks pass.
An initial regression invocation omitted the documented `--test-threads=1`
flag and failed shared-chunk full replay on writer admission order and virtual
timing; that output remains in `target/read-journal-regression.log`. The same
chunk corpus passes under the documented serial invocation.

Runtime-isolation regression logs are retained in
`target/runtime-notify-spike-red.log`, `target/runtime-notify-spike-green.log`,
`target/runtime-notify-parallel.log`, `target/runtime-notify-lease-tests.log`,
and `target/runtime-notify-spike-clippy.log`. All 50 standalone tests pass with
parallel test threads, and spike Clippy with all targets and features passes.
The five production lease tests and strict production-library Clippy also pass.
Production test Clippy allows the existing unused `directory_bytes` benchmark
helper (`-A dead_code`); its strict invocation is retained in
`target/runtime-notify-production-clippy.log`. The earlier journal-read parallel
failure above led to this fix; default explicit-entropy runs now use per-runtime
completion notifications.

All 53 standalone tests pass with parallel test threads after the concurrent
journal extension. All-target/all-feature Clippy with warnings denied, spike
formatting and diff checks pass. Concurrent journal ownership checks are retained in
`target/journal-writers-regression.log` and `target/journal-writers-clippy.log`.
The three new tests cover 280 exhaustive native/model comparisons, 256 seeded
cases with full replay, and the wrong-identity shared-path negative control.
`journal-writer-corpus 256` covers 1,024 cases per process, each replayed;
the two independent process runs produced matching complete-report digests in
`target/journal-writers-process-1.log` and `target/journal-writers-process-2.log`.

All 55 standalone tests pass with parallel test threads after the overlapping
recovery extension. All-target/all-feature Clippy with warnings denied, spike
formatting and diff checks pass. Overlapping recovery verification is retained in
`target/journal-overlap-initial.log`, `target/journal-overlap-regression.log`
and `target/journal-overlap-clippy.log`. The two new tests cover all six ownership
and SIGKILL combinations plus a bypassed-ownership negative control.
`client-journal-overlap 8` passed 48 fresh cases and 144 rejected contenders;
complete reports are retained in `target/journal-overlap-native-corpus.jsonl`.

All 57 standalone tests pass with parallel test threads after the submission
and recovery extension. All-target/all-feature Clippy with warnings denied,
spike formatting and diff checks pass. Submission/recovery verification is retained in
`target/journal-submission-initial.log`, `target/journal-submission-regression.log`
and `target/journal-submission-clippy.log`. The two new tests cover nine ownership
and SIGKILL combinations plus a bypassed-submission negative control.
`client-journal-submission 8` passed 72 fresh cases and 216 rejected contenders:
24 cases hold submission ownership, and 48 hold recovery ownership. Reports are
retained in `target/journal-submission-native-corpus.jsonl`.

All 59 standalone tests pass with parallel test threads after the seeded read
extension. All-target/all-feature Clippy with warnings denied, spike formatting
and diff checks pass. Seeded journal read checks are retained in
`target/seeded-journal-read-targeted.log`, `target/seeded-journal-read-regression.log`
and `target/seeded-journal-read-clippy.log`. The two new tests cover 256 seeded
cases with full replay plus eight ignored-read negative controls.
`journal-read-corpus 64` passed 512 cases per independent process, each replayed;
complete-report digests matched in `target/seeded-journal-read-process-1.log` and
`target/seeded-journal-read-process-2.log`.

All 62 standalone tests pass with parallel test threads after the combined
read/save extension. All-target/all-feature Clippy with warnings denied, spike
formatting and diff checks pass. Combined read/save verification is retained in
`target/journal-read-save-targeted.log`, `target/journal-read-save-negative.log`,
`target/journal-read-save-alternate.log`, `target/journal-read-save-regression.log`
and `target/journal-read-save-clippy.log`. Three new tests cover 256 seeded cases
with full replay, 16 forbidden remote-dispatch controls and eight alternate-intent
promotion controls. `journal-read-save-corpus 8` passed 512 cases per independent
process, each replayed; complete-report digests matched in
`target/journal-read-save-process-1.log` and `target/journal-read-save-process-2.log`.

All 65 standalone tests pass with parallel test threads after the combined
save-crash extension. All-target/all-feature Clippy with warnings denied, spike
formatting and diff checks pass. Combined read/save-crash verification is retained in
`target/journal-read-save-crash-targeted-final.log`,
`target/journal-read-save-crash-regression.log` and
`target/journal-read-save-crash-clippy.log`. Three new tests cover 256 seeded cases
with full replay, 16 resubmission controls and four temporary-promotion controls.
`journal-read-save-crash-corpus 8` passed 512 cases per independent process, each
replayed; complete-report digests matched in
`target/journal-read-save-crash-process-1.log` and
`target/journal-read-save-crash-process-2.log`. The existing local-retry CLI still
passes its 64-case smoke run in `target/journal-read-save-crash-cli-compatibility.log`.

All 69 standalone tests pass with parallel test threads after the combined
persistent-save extension. All-target/all-feature Clippy with warnings denied,
spike formatting and diff checks pass. Combined persistent recovery-save verification is retained in
`target/journal-read-save-outage-targeted.log`,
`target/journal-read-save-outage-regression.log` and
`target/journal-read-save-outage-clippy.log`. Four new tests cover 256 seeded cases
with full replay plus controls for a new batch before repair, a fourth attempt,
and remote dispatch. `journal-read-save-outage-corpus 4` passed 512 cases per
independent process, each replayed; complete-report digests matched in
`target/journal-read-save-outage-process-1.log` and
`target/journal-read-save-outage-process-2.log`.

All 73 standalone tests pass with parallel test threads after the persistent
save-exhaustion crash extension. All-target/all-feature Clippy with warnings
denied, spike formatting and diff checks pass. Persistent save-exhaustion crash verification is retained in
`target/journal-outage-crash-targeted.log`, `target/journal-outage-crash-regression.log`
and `target/journal-outage-crash-clippy.log`. Four new tests cover 256 seeded cases
with full replay and controls for a fourth attempt, resubmission after repair and
temporary promotion. `journal-read-save-outage-crash-corpus 8` passed 512 cases per
independent process, each replayed; complete-report digests matched in
`target/journal-outage-crash-process-1.log` and `target/journal-outage-crash-process-2.log`.
The existing local-retry and one-shot crash CLI commands also passed 64-case smoke
runs in `target/journal-outage-crash-cli-retry.log` and
`target/journal-outage-crash-cli-oneshot.log`.

All 84 standalone tests pass after the remaining-coverage extension: 70 library
tests, three backend process-crash tests and 11 client-journal integration tests.
Strict all-target/all-feature Clippy, spike formatting and diff checks pass.
Results are retained in `target/remaining-coverage-regression.log` and
`target/remaining-coverage-clippy.log`. The additions include the restarted
persistent-outage matrix, native stale-generation fencing and lock replacement,
marker retirement and generation-scoped reuse, and partial restore cancellation
at both prefix cutoffs and DELETE boundaries, with negative controls.

Independent-process verification also passes. `journal-restarted-outage-corpus 8`
passed 512 cases per process, each replayed, with matching complete-report digest
`blake3-28immJ0zf2Rr9kGaxjmVzt8RlQ8jwxBcXYg4AJugWoA`.
`partial-restore-corpus 32` passed 128 cases per process, each replayed, with matching
digest `blake3-c9xDFAW6SQP7wodpmjUilRJs2zD-HugExPffmuVZqoY`.
Reports are retained in `target/remaining-restarted-process-{1,2}.log` and
`target/remaining-partial-process-{1,2}.log`. Both `native-fencing 8` and
`native-retention 8` passed eight fresh-database runs, in
`target/remaining-fencing-process-1.log` and `target/remaining-retention-process-1.log`.

The shared-harness and CI extension passes all 86 standalone tests on the pinned
Rust 1.96.0: 72 library tests, three backend process-crash tests and 11 native
client-journal tests. Strict default-feature/all-target Clippy and formatting
also pass on 1.96.0. All-target/all-feature Clippy passes on the current 1.97.1
toolchain. Actionlint 1.7.12 validates the new workflow.

Two independent `dst-ci 4` processes each passed 252 cases with complete replay.
Their 1,008-line JSONL artifacts match byte for byte, and both summaries report
`blake3-i9IxdOFoad0_HDQQb_3AAiCWpOPzMxMCNUFJ0cfnIkA`. Manual CLI checks reject zero
seeds, 65 seeds and surplus arguments before creating an artifact. Local evidence
is retained in `target/dst-harness-regression-1.96.log`,
`target/dst-harness-clippy-1.96.log`, `target/dst-workflow-lint.log` and
`target/dst/{reports-1.jsonl,reports-2.jsonl,replay-1.log,replay-2.log,argument-validation.log}`.
The GitHub workflow has been validated locally; it has not run remotely yet.

## Contribution verification on current main

The reviewed contribution baseline is `9d7a2f42`, which already contains the
runtime lease-notification fix from PR #67. Scoped entropy is isolated in
[PR #70](https://github.com/cachix/casita/pull/70); this corpus depends on that
contribution. The native operation-marker SIGKILL regressions are kept as a
separate contribution. The original research worktree remains available.

On Rust 1.96.0, the entropy contribution passes 834 native/experimental library
tests, with 49 existing ignored worker fixtures and benchmark cases. All three
entropy regressions and the jitter/fallback regression execute. A preexisting
S3-only benchmark size helper is gated to S3 so native-only test Clippy can run
with warnings denied. The contribution corpus passes all 86 standalone tests,
strict all-target/all-feature Clippy, formatting and Actionlint. No symbol
interception or MadSim dependency remains in the manifest or lockfile.

Two independent `dst-ci 4` processes each pass 252 cases with full replay. Their
1,008-line reports match byte for byte and retain the digest
`blake3-i9IxdOFoad0_HDQQb_3AAiCWpOPzMxMCNUFJ0cfnIkA`. Zero seeds, 65 seeds and
surplus arguments fail before creating a report file.

Verification uses a private `target/build` directory per worktree. Sharing a
Cargo target directory between worktrees with the same package and relative
source paths can select another worktree's test binary; that produced a zero-test
entropy rerun during preparation. All required checks were rerun with isolated
local artifacts. Dependency artifacts were reused, but local Casita artifacts
were excluded from the copied cache. Current evidence is retained under
`target/dst-contributions`: `entropy-native-tests-isolated.log`,
`entropy-native-clippy-isolated.log`, `corpus-tests-isolated.log`,
`corpus-clippy-isolated.log` and `replay-isolated/`.

## Next steps

The remaining spike coverage now includes restarted save outages, stale publication
fencing, marker retention with operation-ID reuse, and cancellation during partial
restore. The shared harness and a bounded CI workflow are now extracted. Prefer
reviewing the separate entropy, native marker-crash and simulation-corpus
contributions next. An alternative is implementing authoritative local journal fencing if
lock-file replacement must be supported; the current local
protocol requires a stable lock inode.
Keep Turmoil as the simulator; Stateright remains an optional separate model checker.

## Sources

- [Celld deterministic simulation testing](https://celld.dev/blog/deterministic-simulation-testing/)
- [Turmoil documentation](https://docs.rs/turmoil/0.7.2/turmoil/)
- [Turmoil builder and seeded host ordering](https://docs.rs/turmoil/0.7.2/turmoil/struct.Builder.html)
- [S2's Turmoil/MadSim integration writeup](https://s2.dev/blog/dst)
- [Pinned mad-turmoil source](https://github.com/s2-streamstore/mad-turmoil/tree/f2dc7556f60497dacb54dfb23c00af7eac487069)
