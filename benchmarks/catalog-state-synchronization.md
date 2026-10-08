# Catalog synchronization correctness

Catalog synchronization must preserve local unpublished changes while selecting
an incoming catalog. Readers must observe index entries and deferred catalog
state from the same version.

## Defects and invariants

1. **A concurrent flush could disappear during synchronization.** A clean dirty
   flag observed before acquiring the index write lock missed a later flush.
   The dirty flag now only schedules publication: synchronization captures and
   replays actual pending changes under the index writer lock.
2. **Prepared changes could disappear before commit or abort.** Preparation
   removes changes from the pending collection, but they must remain replayable.
   The encoded prepared delta is retained from capture through candidate
   resolution, including catalog-building awaits. It is replayed before newer
   pending changes. Failure and cancellation restore pending ownership before
   clearing replay.
3. **Index and deferred state could describe different catalog versions.**
   Installation now updates the index and lazy overlay together. Readers capture
   both under one index read guard and release it before I/O. Materialization
   rechecks the selected runs; hydration discards results from a replaced base
   and retries the current catalog while preserving intervening local changes.
   Successful and failed loads are both checked against the selected catalog.
   Storage and decoding failures propagate only while that catalog is current.

If root synchronization occurs while a candidate is outstanding, resolving the
candidate must not reinstall its old witness, map, or rebase. Its mutation
descriptors are restored ahead of newer pending work for a merged publication.
A successful metadata commit may release committed retirements; an abort keeps
those retirements unpublished. Sidecars remain pending until included in the
merged publication. Protected scoped readers retain their immutable selected
catalog, independently of the writer's mutable view.

## Locking rules

- Synchronization takes `rebuild_lock`, then `checkpoint_lock`. Incoming catalog
  decoding and object reads finish before synchronous guards are acquired.
- Preparation takes `flush_lock`, flushes, then takes `checkpoint_lock`. It
  excludes root synchronization until returning the candidate. Run
  materialization does not take that lock, so prepared replay ownership starts
  atomically when pending changes are captured, before any await.
- Installation, materialization, and candidate resolution take
  `catalog_transition` first. Index replacement then takes `index`,
  `pending_catalog`, and `lazy_catalog`, in that order. The synchronous transition
  guard is never held across I/O or an async wait.
- Ordinary pack and manifest writers start at `index` and never acquire
  `catalog_transition`. They retain index ownership through pending/lazy updates.
  Manifest writers release their lazy guard before recording pending changes.
  Background job guards are not held while waiting for index ownership.
- Readers hold one index read guard while selecting local results and the lazy
  overlay. Enumeration and reclamation recheck deferred runs after
  materialization. Shard hydration rechecks the selected base and local changes
  before installing loaded entries.

`index_dirty` remains a scheduling hint. Mutation paths mark it while protecting
index changes; background rebase completion also requests publication.
Preparation/publication clear it before capturing their snapshot. Rollback and
candidate resolution restore descriptors and request publication when needed.
A clean flag never authorizes discarding pending changes.

## Regression coverage

The overlap tests use explicit channels and notifications to force the relevant
interleavings. Synchronous capture-boundary checks verify that readers retain
the index guard until lazy state is selected. Bounded waits detect a stalled
test; they do not select the race by chance. The tests include merged publication
and fresh-reader checks.

| Area | Regression coverage |
| --- | --- |
| Concurrent flush | `catalog_sync_preserves_concurrent_flush`, `catalog_sync_preserves_concurrent_flush_into_sharded_catalog` |
| Prepared candidate and newer mutations | `catalog_sync_preserves_prepared_changes_on_abort`, `catalog_sync_preserves_owned_candidate_and_later_changes` |
| Preparation failure and cancellation | `catalog_sync_after_failed_or_cancelled_preparation_preserves_retry` |
| Materialization during preparation | `catalog_sync_materialization_preserves_in_progress_preparation` |
| Resolution during incoming decode | `catalog_sync_preserves_resolution_during_decode` |
| Repository commit failure, retry, admission, and retained snapshots | `catalog_sync_during_repository_commit_preserves_retry_and_admission` |
| Index/overlay installation, removals, and rebase | `catalog_sync_installs_index_and_overlay_together`, `catalog_sync_preserves_pack_removals`, `catalog_sync_invalidates_a_prepared_background_rebase` |
| Coherent lookups, listings, and reclamation | The six `catalog_sync_*_uses_one_catalog_view` tests |
| Newly selected deferred runs | `catalog_sync_readers_materialize_newly_selected_runs` |
| Empty replacement, cancellation during retry, and storage failures | `catalog_sync_materialization_retries_an_empty_replacement`, `catalog_sync_cancelled_materialization_retry_preserves_pending`, `catalog_sync_materialization_propagates_run_failures` |
| Failed load from a replaced catalog and errors in its replacement | `catalog_sync_materialization_retries_obsolete_run_failures`, `catalog_sync_materialization_reports_replacement_run_failure` |
| Stale hydration | `catalog_sync_does_not_install_stale_pack_hydration`, `catalog_sync_retries_run_hydration_from_a_replaced_base` |

Run from the repository's configured development environment:

```sh
cargo test --lib catalog_sync_
cargo test --lib blob::pack::
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --all -- --check
```

The failed-load tests pause after an attempt to load a missing or corrupt run,
replace the catalog, and then let the reader handle the result. They check valid
and empty replacements, errors from the replacement itself, and cancellation
while retrying. Repairing a current run must allow a fresh attempt with pending
local changes intact.

## Permanent benchmark

`catalog-synchronization` is registered in `manifest.json` and `benchmark all`.
It covers changing and unchanged roots, materialized and sharded bases, 16 and
65,536 base manifests, and 0/1/64 pending changes. Sharded cases also cover legacy
and queryable deferred runs. Each deferred case materializes a run containing
one additional manifest after every changed-root selection and reports that
listing/materialization time separately. Every case checks exact membership and
publication/fresh reopen. It reports latency, catalog requests/bytes, and process
resource usage using an in-memory object store.

```sh
benchmark run catalog-synchronization --entries 65536 --iterations 100 --repetitions 5 --output /tmp/catalog-synchronization.json
benchmark all --suites catalog-synchronization --profile smoke --repetitions 1 --output /tmp/catalog-synchronization-smoke
```

See the [before/after measurement report](reports/2026-10-05-catalog-synchronization/README.md)
for results, retained samples, and baseline reproduction instructions.
