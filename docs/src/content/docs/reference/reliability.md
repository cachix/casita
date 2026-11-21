---
title: Reliability Contract
description: What Casita guarantees across process death, power loss, corruption, and operator error, where each guarantee applies, and which tests would catch a violation.
---

This page states what Casita promises about crashes, faults, and concurrent
use, where each promise applies, and what would show it broken. It describes
the current `main` branch. Casita is pre-release: where current behavior falls
short of a clause, the clause's known limits say so, and the
[findings register](https://github.com/cachix/casita/blob/main/docs/reliability/findings.md)
tracks the open defect.

## Reading a clause

Every clause has the same parts:

- **Statement**: the guarantee.
- **Scope**: the profiles, platforms, and crash classes it covers.
- **Arbiter**: the mechanism that decides whether it holds.
- **Falsifiers**: tests that fail when it is violated. Library tests are named
  by module path under `crates/casita/src` and run with
  `devenv shell cargo test --all-features --lib <path>`. Integration tests are
  named by file under `crates/casita/tests`.
- **Confidence**, per scope:
  - `tested`: a falsifier for that scope runs in the Linux and macOS CI jobs;
  - `audited`: the mechanism was traced in source, but no test would catch a
    regression;
  - `claimed`: design intent that neither a test nor an audit has confirmed.
- **Known limits**: where current behavior does not meet the statement.

The profiles are:

- **local**: `Repository::local`. A Turso database (`casita.sqlite`) holds
  records, roots, and the payload catalog; packed payloads live under `blobs/`;
  a file pin ledger and `gc.lock` coordinate processes. See
  [Local repository](../local-repository/).
- **S3**: `Repository::s3` with the `s3` feature. A wal3 log holds logical
  state; packed payloads and object pin ledgers share the bucket and prefix.
  See [Maintain an S3 Repository](../../guides/s3-maintenance/).

Memory repositories promise nothing across a crash. Custom compositions under
`casita::experimental` inherit the guarantees of the backends they use.

## Crash classes

| Class | Event | What it destroys |
|---|---|---|
| K1 | Process death: `SIGKILL`, abort, panic | The process's memory. The kernel and its page cache survive |
| K2 | OS crash or power loss | Every write not yet synced to stable storage |
| K3 | Torn write | Part of a page or sector that was being written |
| K4 | Media corruption | Bytes at rest: bit rot, lost or misdirected writes |
| K5 | Operator error | Partial copies, mixed binary versions, edited internal files |

## Environment axioms

What Casita relies on from each substrate, and what it must survive without.

| Substrate | Given | Not given; the design must survive it |
|---|---|---|
| Linux filesystems (ext4, XFS, Btrfs) | `fsync` of a file makes its data durable, and `fsync` of a directory makes its entries durable; same-directory `rename` is atomic; `RENAME_EXCHANGE` swaps two names atomically; `File::lock` locks are released when their process dies | Durability or ordering of unsynced writes; atomic multi-sector writes; lock semantics on network filesystems |
| macOS APFS | As Linux, with `RENAME_SWAP`. `File::sync_all` issues `F_FULLFSYNC`, which flushes the drive's cache | A plain `fsync` reaches only the drive's volatile cache, so it does not survive power loss |
| Windows NTFS | `File::sync_all` flushes file data; a same-volume `rename` replaces its target atomically; `File::lock` locks are released when their process dies | Durability of directory entries: Casita issues no directory flush on Windows, so renames, new files, and deletions are not ordered against power loss |
| Turso (`cachix/turso` `dca55133`, pre-release) | One write transaction commits atomically; `synchronous = FULL` syncs the WAL before a commit returns; the experimental multi-process WAL coordinates writers across processes | Engine maturity: every Turso revision change is treated as a durability change. Casita does not enable `fullfsync`, so on macOS a commit reaches only the drive cache. A commit has no identity that can be looked up after an ambiguous error |
| `object_store` `LocalFileSystem` (0.14, `with_fsync(true)`) | A written object's file is synced, then renamed into place, and its parent directory is synced on Unix | Durable standalone deletes: a deleted object can reappear after power loss. Directory syncs on Windows |
| S3-compatible stores (S3, RustFS) and wal3 | A single-object `PUT` is atomic; reads after a write observe it; conditional create and conditional update (ETag or version) are honored. Casita refuses to publish a packed catalog to a store without conditional writes | Multi-object atomicity; an unambiguous outcome for a conditional write whose response is lost or retried by the client; exactly-once delivery (a delayed request can land late); ETag ordering; `LIST` as a complete or current view; synchronized clocks |
| OpenSSH | An authenticated, encrypted, ordered byte stream to the remote `casita`; `ConnectTimeout=30`, `ServerAliveInterval=15`, and `ServerAliveCountMax=3` detect an unreachable or silent host | Liveness of the remote `casita` process behind a live `sshd`; delivery of a final frame on disconnect |

## Durability by platform

For the local profile:

| Class | Linux | macOS | Windows |
|---|---|---|---|
| K1 | Acknowledged state survives; roots are all-old or all-new (`tested`) | Same (`tested`) | Not tested: the Windows CI job does not complete on `main` |
| K2 | Acknowledged state survives (`audited`) | The repository reopens consistent at an earlier revision; the most recent acknowledged commits may be lost (`audited`) | Not promised |
| K3 | Pin-journal frames and payload chunks are checksummed and fail closed; other files are not promised (`claimed`) | Same | Not promised |
| K4 | Detected on read and by `fsck`; repairable from a verified replica (`tested`) | Same | Same (`claimed`) |
| K5 | Mixed binary versions are refused by the database schema check (`tested`); see C13 for the pin ledger | Same | Same |

## Clauses

### C1. Acknowledged publications are durable

**Statement.** Once a publication, root change, or metadata commit is
acknowledged to its caller, every later open observes that state or a later
one.

- **Scope:** local and S3 profiles. K1 on Linux, macOS, and Windows; K2 on
  Linux and on S3. Not K2 on macOS or Windows.
- **Arbiter:** the metadata commit is the commit point. Local: the Turso write
  transaction in `metadata::sqlite::TursoMetadataStore::commit_impl`, whose WAL
  is synced under `synchronous = FULL`; payloads are written and synced before
  it by `blob::local_durability` and `LocalFileSystem::with_fsync(true)`. S3:
  the wal3 manifest append in `metadata::wal3::Wal3MetadataStore::commit_loaded`,
  after payload uploads complete. Shared catalog pointers move only by
  conditional write (`blob::pack::PackedChunks::publish_index_catalog`).
- **Falsifiers:**
  - `blob::crash_tests::repository_publication_survives_every_process_crash_boundary`
    kills a child process at every recorded publication step and requires
    every commit that reached `after-state-commit` or
    `publication-acknowledged` to survive. The same harness covers catalog
    rebase, batch, streamed, catalog-migration, metadata, and paged-overwrite
    publication in the other `blob::crash_tests` tests.
  - `metadata::pins::persistent::journal::tests::killed_publishers_preserve_acknowledged_pins_and_deletion_claims`
    (Linux and macOS).
  - `blob::pack::tests::concurrent_catalog_publishers_merge_without_losing_packs`
    and `blob::pack::tests::catalog_pointer_refuses_stores_without_conditional_updates`.
- **Confidence:** `tested` for K1 on the local profile. `audited` for K2 on
  Linux. `claimed` for S3, which rests on the store's durability and on wal3.
- **Known limits:**
  - On macOS, commits sync with a plain `fsync`, so a power loss may lose the
    most recent acknowledged commits. C2 and C9 still hold there: every
    payload deletion batch first flushes the drive cache holding the database
    (`blob::deletion_barrier`), so the repository reopens consistent at an
    earlier revision.
  - No test simulates K2 on any platform. The crash harnesses kill processes
    while the page cache survives, and Turso's own file I/O is not
    instrumented.
  - Casita issues no directory flush on Windows. On current `main` the local
    pin ledger's parent-directory flush fails there, so the Windows CI job
    does not complete and no clause is tested on Windows.
  - Checkout, Casitar export, IPC restore, and the `casita init` workspace
    marker do not sync their parent directory after publishing a file.
  - On S3, a commit whose log position was taken by another handle's WAL
    collection is retried with a record built on the old log. Every fresh
    handle's open then fails with `Corruption`, although both commits were
    acknowledged.
  - A packed payload store used without a metadata-coordinated catalog
    (`ChunkedBlobStore::packed` or `local_packed` on its own) can lose a write
    whose pack is identical to one it collected earlier.

### C2. Nothing is visible before its commit point

**Statement.** Readers observe records, roots, and payload catalog changes
only after the metadata commit that contains them. One commit applies all of
its root changes or none. A root is set only on an object whose closure is
complete: every reachable record and payload is present and verifies.

- **Scope:** all profiles; K1. On macOS also K2, by deletion ordering.
- **Arbiter:** records, roots, and the payload catalog commit in one metadata
  transaction or wal3 append, conditional on the expected revision
  (`StaleRevision` otherwise). Mutation sessions verify closures before they
  publish; `metadata::sqlite::validate_root_closures_tx` re-checks inside the
  Turso commit, and a fast path without proof fails with
  `RootVerificationRequired`. Deletions wait for the commits that allow them
  (`blob::deletion_barrier::DeletionBarrier::before_deletion`).
- **Falsifiers:**
  - The `blob::crash_tests` audit, after every kill: roots are all-old or
    all-new, `verify_closure` is `Complete`, and `fsck` is healthy.
  - `sync::tests::several_destination_roots_appear_in_one_final_revision`.
  - `sync::tests::split_source_missing_or_wrong_blobs_do_not_fallback_or_move_roots`.
  - `tests/application_api.rs` `root_updates_validate_closures_and_compare_before_removing`.
  - `metadata::record_tests::metadata_records_rootless_witness_cannot_bypass_emergency_fence`.
  - `metadata::wal3::tests::high_fanout_cloned_commits_have_one_durable_winner` (S3).
  - `repository::tests::collection_deletes_only_after_the_commits_that_allow_it_are_durable`
    and `blob::pinned_store::tests::a_deletion_that_cannot_flush_committed_state_never_reaches_storage`
    (Unix).
- **Confidence:** `tested` for K1. `audited` for macOS K2: the ordering tests
  count flushes but do not simulate a power loss.
- **Known limits:** no test drives the commit-time closure check itself with an
  incomplete closure; it is covered only through mutation sessions.

### C3. Unknown outcomes are resolved by identity

**Statement.** When a commit or conditional write fails so that its outcome is
unknown, Casita decides whether it landed by finding its own identity (token,
revision, or content) in durable state. It never assumes the operation failed
and never repeats it blindly.

- **Scope:** all profiles; lost responses, client-internal retries, and
  cancelled requests.
- **Arbiter:** S3 hold acquisition succeeds when it finds its own token already
  recorded, and release always performs its compare-and-swap so that an
  ambiguous earlier admission is fenced (`metadata::wal3::coordination`). wal3
  commit contention reloads the manifest and compares its head with the
  candidate revision (`metadata::wal3::reconcile_contention`); an unresolved
  outcome is reported as a backend error rather than retried. Publication does
  not resubmit a commit whose outcome is unknown.
- **Falsifiers:**
  - `repository::publication_retry_tests::ambiguous_commit_is_not_replayed`: a
    commit that lands and then reports a transient error is not replayed; the
    caller receives the error and the object is present.
  - `metadata::wal3::tests::remote_cancelled_admission_releases_only_after_its_cas_completes`
    (S3).
- **Confidence:** `tested` for publication replay. `audited` for S3 hold
  acquisition.
- **Known limits:** this clause is not met everywhere today.
  - S3 pin-ledger edits mint a fresh token on every attempt. A retry after a
    lost response can leave an extra pin, or an ownerless claim that blocks
    collection.
  - The wal3 check compares only the head revision. If another writer commits
    on top of a landed commit, the landed commit is reported as
    `StaleRevision`.
  - A Turso commit generates its revision inside the transaction, so nothing
    identifies it after an ambiguous error.
  - A publication whose commit outcome is unknown aborts its prepared payload
    catalog as if the commit had been rejected.
  - Casita configures no `object_store` client or retry options, so a
    client-internal retry can report a successful conditional write as a
    precondition failure.
  - No test injects an indeterminate wal3 append or a lost hold-acquisition
    response.

### C4. Retries start from fresh state and are bounded

**Statement.** A retry re-reads state, re-derives its change, and re-checks the
caller's expectations. Every retry loop has an attempt or time bound.
Cancelling a caller never cancels a submitted commit or leaks ownership.

- **Scope:** all profiles.
- **Arbiter:** `repository::publication::PublicationRetry` allows at most 32
  attempts within 30 seconds, with jittered delays of at most 250 ms, and
  checks its deadline only between attempts. Turso writes run on a blocking
  worker that completes after its caller is dropped (`sqlite::TursoDb::write`).
- **Falsifiers:**
  - `repository::publication_retry_tests::maintenance_retries_keep_staging_and_exact_revision`,
    `maintenance_and_stale_revision_share_one_attempt_budget`,
    `retry_window_does_not_start_an_attempt_after_its_deadline`,
    `maintenance_retry_rechecks_original_root_expectation`, and
    `cancelled_maintenance_attempt_keeps_protection_until_it_settles` in the
    same module.
  - `repository::tests::filesystem_construction_proof_survives_a_stale_revision_retry`.
  - `tests/publication_cancellation.rs`
    `cancelled_publication_keeps_its_pin_through_online_collection_and_shutdown`
    and `cancelled_logical_prune_keeps_admission_fenced_until_commit_settles`.
  - `tests/publication_snapshot.rs` `releasing_the_snapshot_preserves_exact_revision_conflicts`.
  - `object_read_tests::cancelled_open_waiting_for_admission_leaks_no_pin`.
- **Confidence:** `tested` for publication and catalog maintenance.
- **Known limits:** several loops have neither a bound nor a backoff:
  conditional root commits and `remove_root_if_matches` retry
  `StaleRevision` indefinitely; mutation admission loops while state keeps
  moving; local data-pin waits poll every 25 ms; S3 WAL collection polls for
  its lease every 25 ms; wal3 barrier initialization and pinned WAL deletion
  loop until they succeed.

### C5. Deletion needs proof, not time

**Statement.** Casita deletes a record or payload only with durable evidence
that no current or later reader or writer can need it: reachability from roots
and active pins in one snapshot, collector exclusivity, and deletion claims
that exclude later pins. Elapsed time never proves a deletion safe. Not
collecting is always safe.

- **Scope:** all profiles; concurrent processes and hosts.
- **Arbiter:** logical prune before physical deletion
  (`repository::collection`); durable data pins, prune fences, and deletion
  claims (`metadata::pins`); `gc.lock` locally and the exclusive collector hold
  on S3; local reader liveness from kernel locks on owner files
  (`metadata::pins::persistent::readers`).
- **Falsifiers:**
  - `repository::tests::generic_collection_matches_an_independent_reachability_model`
    (property test against a model).
  - `repository::tests::collection_progress_survives_duplicate_readers_after_marking`
    and `collection_progress_survives_idle_writers_admitted_after_marking`.
  - `blob::pack::tests::catalog_deletion_rejects_a_pin_registered_after_its_mark`.
  - `tests/online_collection.rs` `deletion_claims_cover_cancelled_io_and_deferred_collection_finish`.
  - `tests/repository_workflows.rs` `local_collection_reclaims_unrelated_data_while_another_process_stages`.
  - `object_read_tests::killed_reader_releases_only_read_protection_after_kernel_owner_exit`.
  - `metadata::wal3::tests::gc_fences_collection_shards_uploaded_before_publication`
    and `tests/s3_multi_owner_recovery.rs` (S3).
- **Confidence:** `tested` for local collection and S3 payload collection.
- **Known limits:** S3 WAL collection (`Wal3MetadataStore::collect_wal` and
  `collect_repository_coordination`) deletes obsolete log files after sleeping
  for a caller-chosen `reader_grace_period`. Nothing verifies that readers are
  gone, so safety there rests on elapsed time.

### C6. Ownership never expires

**Statement.** Pins, holds, prune fences, deletion claims, and collector
ownership last until their exact token is released. No timeout ends them.
Only evidence does: an explicit release naming the exact token, or, for local
readers and collectors, the kernel releasing the owner's lock when its
process exits.

- **Scope:** all profiles.
- **Arbiter:** no ownership record has an expiry. An S3 hold
  (`metadata::wal3::coordination::Wal3RepositoryHold`) records only its token,
  a diagnostic writer name, and whether it is exclusive. Recovery APIs
  (`release_abandoned_repository_hold`, `recover_s3_collection`,
  `recover_collection`, pin `release`) take exact tokens.
- **Falsifiers:**
  - `tests/s3_multi_owner_recovery.rs` `s3_multi_owner_recovery_across_process_failures`:
    a killed reader's pin keeps its data until its token is released, and a
    replayed release cannot remove another owner's protection.
  - `tests/s3_collection_recovery.rs` `separate_process_recovers_s3_catalog_through_an_abandoned_prune_fence`.
  - `metadata::wal3::tests::remote_abandoned_holds_survive_reopen_and_recovery_is_token_specific`.
  - `tests/retained_process_pins.rs` `process_crashes_release_retained_protection_but_preserve_durable_writes`.
  - `tests/online_pin_ledger.rs` `local_pins_and_deletion_claims_survive_process_exit`.
  - `metadata::pins::tests::recovery_prune_requires_every_exact_claim_and_keeps_admission_closed`.
  - `metadata::pins::persistent::readers::tests::missing_reader_inventory_fails_closed_until_owner_stops_then_fences_old_revisions`.
- **Confidence:** `tested`.
- **Known limits:** on S3, the evidence that an owner has stopped comes from the
  operator. Hold records carry no owner incarnation or liveness evidence, and
  recovery follows a manual procedure. The S3 abandoned-hold and
  abandoned-collector tests end the owner with a normal exit after leaking its
  handle, not with process death.

### C7. Reads verify; corruption is typed; repair uses verified bytes

**Statement.** Every payload byte returned to a caller has been verified
against its BLAKE3 identity: per chunk, at EOF for a sequential read, or by a
Bao proof for a range. Corruption surfaces as a typed error or a `Corrupt`
`fsck` finding, never as wrong bytes. Repair writes only bytes from an
independent replica that verified against the expected identity, and never
changes logical records.

- **Scope:** all profiles; K3 and K4, detected when data is read or audited.
- **Arbiter:** verified readers in `blob::chunked` and `verified`;
  `repository::integrity` (`Repository::fsck` and its repair);
  `blob::repairing` for near/far stores.
- **Falsifiers:**
  - `blob::chunked::tests::corrupted_chunk_fails_read`,
    `sequential_stream_reports_large_chunk_corruption_at_eof`,
    `scoped_verified_reader_rejects_corrupt_proof_before_returning_bytes`, and
    `verified_reads_reject_substitution_and_repair_missing_outboards`.
  - `blob::chunked::tests::fsck_repair_repairs_a_corrupt_chunk_from_a_verified_replica`,
    `fsck_repair_chaos_refuses_a_missing_or_corrupt_replica`,
    `repairing_tier_rejects_an_unverified_source_with_both_diagnostics`, and
    `repairing_tier_does_not_mistake_backend_io_for_corruption`.
  - `blob::pack::tests::a_corrupt_packed_chunk_is_reported_as_integrity_failure`.
  - `repository::tests::fsck_distinguishes_corruption_from_collectible_staging`.
  - `tests/hostile_inputs.rs` `portable_decoders_reject_truncation_and_hostile_lengths`,
    and the fuzz targets in `fuzz/`.
- **Confidence:** `tested`.
- **Known limits:** corruption is found only when data is read or audited;
  nothing scrubs in the background. Fuzzing covers decoders of untrusted
  bytes, not a repository whose on-disk state was mutated. Torn writes inside
  the metadata database are left to the database engine and are not tested.

### C8. Recovery is loud

**Statement.** Opening or maintaining a repository discards only state that no
acknowledged operation depends on, and reports everything it discards or
reclaims.

- **Scope:** all profiles, after K1 and K2.
- **Arbiter:** pin-journal replay ignores only an incomplete tail and fails
  closed on a complete corrupt frame; the spill sweep spares files whose lock
  is held; a prepared file that is never committed removes its temporary when
  dropped.
- **Falsifiers:**
  - `metadata::pins::persistent::journal::tests::corrupt_frame_epoch_does_not_silently_discard_acknowledged_protection`
    and `appends_replay_exactly_and_reject_complete_corruption` (Linux and
    macOS).
  - `spill::tests::sweeping_removes_abandoned_state_and_spares_live_state` and
    `tests/spillable_traversal.rs` `spill_state_from_a_dead_process_is_swept_at_open`.
  - `blob::local_durability::tests::prepared_group_is_invisible_until_commit_and_cleans_abandoned_temps`.
- **Confidence:** `tested` for discarding only unacknowledged state. The
  reporting half is not provided.
- **Known limits:** nothing is reported today. Replay that stops at an
  incomplete journal tail, the spill sweep at open, reclamation of unpublished
  payloads and retired packs, removal of abandoned temporary files, and
  removal of a stale IPC socket all happen without a log event or report.
  `fsck` and `gc` have no machine-readable output, and `runs/` leftovers are
  never swept.

### C9. A crash leaves only collectible garbage

**Statement.** After a crash at any point, the repository reopens and accepts
new work. Whatever the crash left behind (unreferenced payloads, partial
uploads, temporary files, retired packs) is collectible, and after the next
collection `fsck` is clean.

- **Scope:** all profiles; K1. On macOS also K2, by deletion ordering.
- **Arbiter:** publication writes payloads before the commit that references
  them; collection and vacuum reclaim what no commit references; replacement
  markers track retired packs until their deletion finishes.
- **Falsifiers:**
  - The `blob::crash_tests` verifier: after every kill it publishes again,
    collects, requires `fsck().is_clean()`, and reopens.
  - `blob::pack::tests::interrupted_catalog_sweep_keeps_marker_and_retries_from_published_root`,
    `deferred_pack_cleanup_preserves_historical_catalogs_and_reclaims_unrelated_uploads`,
    and `state_catalog_vacuum_recovers_retired_packs_without_deleting_reintroduced_content`.
  - `repository::tests::reopen_and_fsck_recover_after_publication_commit_failure`,
    `reopen_and_fsck_recover_after_post_prune_sweep_failure`, and
    `interrupted_emergency_sweep_is_collectible_and_retryable`.
- **Confidence:** `tested` for K1 on the local profile. `audited` for S3.
- **Known limits:** K2 is not tested. `LocalFileSystem` does not sync the
  directory after a standalone delete, so a collected object can reappear
  after a power loss; the committed state no longer references it, so it is
  garbage again. `runs/` leftovers from an interrupted `casita run` are not
  collected. The NAR intake crash test exits its process instead of killing
  it.

### C10. Library code does not panic on failures

**Statement.** Library code reports I/O failures, contention, corrupt input or
state, and poisoned locks as typed errors with a retry disposition. It does
not panic on them.

- **Scope:** all profiles and platforms.
- **Arbiter:** `RepositoryError::category()`, `RetryDisposition`, and the
  non-exhaustive `MetadataError` (including `Corruption` and `Poisoned`);
  bounded decoders for untrusted input.
- **Falsifiers:**
  - `tests/hostile_inputs.rs` `portable_decoders_reject_truncation_and_hostile_lengths`
    (also run under Miri on schedule).
  - The fuzz targets `logical_records`, `frozen_formats`, `casitar_stream`,
    `git_owned_parsers`, and `input_boundaries` (smoke run on every pull
    request).
  - `wire::tests::hostile_counts_and_sizes_are_rejected`.
  - `repository::tests::storage_full_detection_preserves_typed_nested_errors`.
- **Confidence:** `tested` for untrusted input. `claimed` elsewhere.
- **Known limits:** many library paths still panic on a poisoned mutex (the
  packed payload store alone has over a hundred) and on internal invariants
  through `expect` and `unreachable!`. No lint enforces the rule, and no test
  poisons a lock.

### C11. Sync is idempotent and resumable; the destination verifies

**Statement.** Repeating or resuming an interrupted transfer converges to the
same destination state and does not redo work already verified there. The
destination verifies every record and payload it receives against its own
rules before rooting it, so a source cannot make it root an incomplete or
substituted graph.

- **Scope:** local and SSH transfer sources; any destination profile; K1 on
  either side.
- **Arbiter:** staged batches publish unrooted (`sync::Receiver::publish_staged`);
  all requested roots publish together through a closure-verifying mutation
  (`sync::Receiver::finish`); path proofs are checked before use
  (`sync::resolve_verified_path_proof`).
- **Falsifiers:**
  - `sync::tests::interrupted_bounded_batches_leave_unrooted_progress_and_retry_completes`
    and `interrupted_payload_copy_keeps_roots_unchanged_and_retry_completes`.
  - `sync::tests::incremental_discovery_reuses_only_complete_destination_closures`.
  - `sync::tests::recursive_transfer_reverifies_and_roots_an_ipld_graph`,
    `read_only_chunk_source_corruption_preserves_the_destination_root`,
    `path_proof_verification_rejects_every_untrusted_boundary`, and
    `missing_source_boundary_never_changes_requested_roots`.
  - `sync::ssh::tests::receiver_rejects_payload_substitution_from_remote_source`
    and `invalid_or_cancelled_payload_batches_poison_the_connection`.
- **Confidence:** `tested`.
- **Known limits:** SSH tests run the protocol over an in-process stream, not
  a real `ssh` connection. Transport liveness belongs to C12.

### C12. Every wait is bounded, cancellable, or reported

**Statement.** An operation waiting on another party (a peer, a lock holder,
another collector) gives up at a bound, reports what it is waiting for while
it waits, or stops without leaking ownership when its caller drops it.

- **Scope:** all profiles; local IPC, SSH, and Git smart-HTTP servers.
- **Arbiter:** IPC frame and response deadlines (`IpcOptions`, 60 and 30
  seconds by default); the OpenSSH options above; `GitHttpOptions` timeouts;
  S3 admission reports its blockers at most every 5 seconds; `try_collect`
  returns `Busy` instead of waiting; Turso waits at most 30 seconds for a
  database lock.
- **Falsifiers:**
  - `cli::ipc::tests::frame_deadline_bounds_idle_and_trickling_clients` and
    `response_deadline_bounds_clients_that_stop_reading`.
  - `sync::ssh::tests::endpoint_parser_and_ssh_arguments_are_injection_safe`,
    which pins the OpenSSH options.
  - `tests/application_api.rs` `flush_waits_or_reports_busy_until_a_live_snapshot_is_released`.
  - `tests/s3_hold_diagnostics.rs` `cli_inspects_collector_ownership_without_blocking_read_open`.
  - `git::fetch::read_ahead_tests::stalled_reads_overlap_are_bounded_and_cancel_with_the_pack`.
- **Confidence:** `tested` for IPC deadlines and `Busy`. `audited` for SSH:
  the options are pinned, but their effect is not exercised.
- **Known limits:** once an SSH connection is up, nothing bounds a remote
  `casita` that stops responding behind a live `sshd`, and the serving side
  may keep its retention hold for the whole stuck session. The unbounded loops
  listed under C4 wait without a report. No test exercises SSH keepalive or
  the Git smart-HTTP timeouts.

### C13. Formats are versioned and old binaries refuse them

**Statement.** Every durable format carries a version or magic. A binary that
does not understand a format rejects it before writing anything. Frozen object
encodings never change their bytes.

- **Scope:** all profiles; K5 (mixed binary versions).
- **Arbiter:** the local database's `PRAGMA user_version` (schema 6), checked
  at open without migration (`sqlite::TursoDb::open`); pin-ledger and journal
  magics (`CASPIN04`, `CASPJL01`, `CASDLT01`); the Casitar v1 magic; the SSH
  protocol magic `casita-ssh-source-v3`; golden vectors.
- **Falsifiers:**
  - `sqlite::tests::existing_nonrelease_schema_versions_are_rejected`,
    `rejected_older_schemas_preserve_their_schema_and_data`, and
    `unversioned_database_with_foreign_schema_is_rejected_untouched`.
  - `tests/generic_encodings.rs`, including
    `blob_payload_identity_vectors_are_frozen`,
    `directory_with_every_entry_kind_has_literal_layout`,
    `casitar_v1_literal_vectors`, and `v0_4_literal_vectors`.
  - `sync::wire::tests::request_and_progress_roundtrip_and_reject_trailing_bytes`.
- **Confidence:** `tested` for the local database and the frozen encodings.
- **Known limits:** the S3 profile has no format fence; operators must stop
  older binaries before upgrading. Processes sharing one local pin ledger must
  all be upgraded before any of them makes a durable mutation. No test covers
  a mismatched SSH protocol version or a wrong Casitar magic.

### C14. Caches are derived

**Statement.** Deleting a cache or accelerator (the verified-closure records,
the ingest cache, spill files, or Bao outboards) costs only performance.
Results stay the same.

- **Scope:** local profile, and outboards on every packed store.
- **Arbiter:** accelerators are not logical state, and any digest they
  produce is confirmed against committed state before use (see
  [local accelerators](../local-repository/#local-accelerators)). Outboards are
  rebuilt from verified bytes. Spill files are swept at open.
- **Falsifiers:**
  - `tests/spillable_traversal.rs` `spill_state_from_a_dead_process_is_swept_at_open`
    and `a_spilled_traversal_agrees_with_an_in_memory_one`.
  - `blob::chunked::tests::verified_reads_reject_substitution_and_repair_missing_outboards`
    and `repairing_tier_rebuilds_bao_state_from_verified_local_bytes`.
  - `repository::tests::ingest_cache_requires_a_validated_closure_witness`.
  - `blob::chunked_reader::tests::failures_are_never_cached`.
- **Confidence:** `tested` for spill files and outboards. `audited` for the
  verified-closure records and the ingest cache.
- **Known limits:** no test deletes the verified-closure records, the ingest
  cache, or every outboard and then checks that results are unchanged.

### C15. Running out of resources never corrupts

**Statement.** Exhausting disk space or a configured budget fails the
operation with a typed error (`StorageFull` for space) and leaves committed
state intact. On the local profile, collection still runs on a full disk.

- **Scope:** all profiles for the typed failure; local profile for
  full-disk collection.
- **Arbiter:** `repository::error::is_storage_full` classifies nested errors.
  When a local logical prune fails for lack of space, collection deletes only
  the already-marked stale payloads, reopens the writer, and retries. The pin
  ledger preallocates space for its own transitions.
- **Falsifiers:**
  - `tests/full_disk_gc.rs` `local_gc_succeeds_after_filesystem_returns_enospc`,
    `local_gc_preserves_roots_when_garbage_cannot_release_space`, and
    `local_gc_fails_cleanly_when_traversal_state_cannot_spill` (Linux; real
    `ENOSPC` on a private tmpfs).
  - `repository::tests::full_disk_crash::local_emergency_sweep_recovers_after_abort_without_a_mount`
    and `local_emergency_sweep_recovers_after_real_process_abort` (Linux).
  - `repository::tests::local_emergency_collection_uses_stale_payload_as_commit_space`,
    `generic_storage_full_does_not_assume_colocated_payload_capacity`, and
    `storage_full_detection_preserves_typed_nested_errors`.
  - `metadata::pins::persistent::journal::tests::checkpoint_and_gc_reuse_capacity_when_growth_is_denied`,
    `full_legacy_ledger_can_collect_before_journal_upgrade`, and
    `failed_append_invalidates_tentative_cache_and_retains_all_acknowledged_pins`.
  - `tests/spillable_traversal.rs` `a_traversal_that_outgrows_its_temporary_budget_fails`.
- **Confidence:** `tested` on Linux with a real full filesystem; `tested` on
  macOS with injected errors only.
- **Known limits:** the real-`ENOSPC` tests run only where unprivileged user
  and mount namespaces are available; elsewhere they skip, and CI reports the
  skip as a warning. The full-volume pin-journal test is ignored by default
  because it needs a small dedicated volume. Emergency collection applies
  only to the local profile; other compositions surface `StorageFull`.

## Changing this contract

A change that affects a guarantee updates the clause, its falsifier tests,
and the code together. Report a suspected violation of a clause the same way
as any other bug, or privately if it has security impact (see
[`SECURITY.md`](https://github.com/cachix/casita/blob/main/SECURITY.md)).
