//! Bug arms for the wal3 checkpoint and append validators.

use super::*;

fn record(index: u8) -> ObjectRecord {
    let key = ObjectKey::new(
        crate::NamespaceId::try_from("test.wal3.transition.v1").unwrap(),
        vec![index],
    )
    .unwrap();
    ObjectRecord::new(
        key,
        crate::BlobId::new(crate::Digest::hash(&[index])),
        1,
        Vec::new(),
    )
    .unwrap()
}

fn revision(byte: u8) -> RepositoryRevision {
    RepositoryRevision::from_bytes([byte; 32])
}

fn position(offset: u64) -> wal3::LogPosition {
    wal3::LogPosition::from_offset(offset)
}

/// A state with one overlay object and its faithful compaction.
fn compaction() -> (StateData, StateData) {
    let object = record(1);
    let mut before = StateData::empty().unwrap();
    before.births.insert(object.key().clone(), 1);
    before.validated.insert(object.key().clone());
    before.objects.insert(object.key().clone(), object);
    let mut after = before.clone();
    after.objects.clear();
    after.births.clear();
    after.validated.clear();
    after.base_objects = Arc::new(StateShardMap {
        object_count: 1,
        validated_count: 1,
        ..StateShardMap::default()
    });
    (before, after)
}

#[test]
fn compaction_must_keep_the_logical_state() {
    let (before, after) = compaction();
    assert_eq!(validate_compaction("test", &before, &after), Ok(()));

    let mut moved = after.clone();
    moved.revision = revision(9);
    assert!(validate_compaction("test", &before, &moved).is_err());

    let mut advanced = after.clone();
    advanced.generation += 1;
    assert!(validate_compaction("test", &before, &advanced).is_err());

    let mut recataloged = after.clone();
    recataloged.payload_catalog = b"other".to_vec();
    assert!(validate_compaction("test", &before, &recataloged).is_err());

    let mut unmerged = after.clone();
    unmerged
        .roots
        .insert(RootName::try_from("pending").unwrap(), None);
    assert!(validate_compaction("test", &before, &unmerged).is_err());

    let mut dropped = after.clone();
    dropped.base_objects = Arc::new(StateShardMap {
        validated_count: 1,
        ..StateShardMap::default()
    });
    assert!(validate_compaction("test", &before, &dropped).is_err());

    let mut unvalidated = after.clone();
    unvalidated.base_objects = Arc::new(StateShardMap {
        object_count: 1,
        ..StateShardMap::default()
    });
    assert!(validate_compaction("test", &before, &unvalidated).is_err());

    let mut rooted = after;
    rooted.base_objects = Arc::new(StateShardMap {
        object_count: 1,
        validated_count: 1,
        root_count: 1,
        ..StateShardMap::default()
    });
    assert!(validate_compaction("test", &before, &rooted).is_err());
}

fn delta(expected: RepositoryRevision, revision: RepositoryRevision) -> StateDelta {
    StateDelta {
        expected,
        revision,
        objects: Vec::new(),
        roots: Vec::new(),
        validated: Vec::new(),
        payload_catalog: None,
    }
}

#[test]
fn a_checkpoint_append_caches_exactly_the_checkpoint_it_writes() {
    let state = StateData::empty().unwrap();
    let result = CommitResult {
        revision: state.revision,
        objects_inserted: 0,
        objects_removed: 0,
        roots_changed: 0,
    };
    let record = encode_state(&state).unwrap();
    let legal = AppendPlan {
        expected: revision(1),
        result: &result,
        state: &state,
        record: &record,
        appended: position(5),
        base: Some(position(2)),
        next_base: position(5),
        next_tail: &[],
    };
    assert_eq!(validate_append(&legal), Ok(()));

    // A retry that lands after another checkpoint must not cache the
    // position it first aimed at.
    let stale_base = AppendPlan {
        appended: position(6),
        ..legal
    };
    assert!(validate_append(&stale_base).is_err());
    let tail = [delta(revision(1), state.revision)];
    let with_tail = AppendPlan {
        next_tail: &tail,
        ..legal
    };
    assert!(validate_append(&with_tail).is_err());
    let mut advanced = state.clone();
    advanced.generation += 1;
    let differs = AppendPlan {
        state: &advanced,
        ..legal
    };
    assert!(validate_append(&differs).is_err());
    let misreported = CommitResult {
        revision: revision(9),
        ..result.clone()
    };
    let wrong_result = AppendPlan {
        result: &misreported,
        ..legal
    };
    assert!(validate_append(&wrong_result).is_err());
    let unknown = AppendPlan {
        record: b"not a wal3 record",
        ..legal
    };
    assert!(validate_append(&unknown).is_err());
}

#[test]
fn a_delta_append_builds_on_the_checkpoint_of_the_log_it_follows() {
    let state = StateData::empty().unwrap();
    let result = CommitResult {
        revision: state.revision,
        objects_inserted: 0,
        objects_removed: 0,
        roots_changed: 0,
    };
    let expected = revision(1);
    let tail = [
        delta(revision(0), expected),
        delta(expected, state.revision),
    ];
    let record = encode_delta_record(position(2), &tail);
    let legal = AppendPlan {
        expected,
        result: &result,
        state: &state,
        record: &record,
        appended: position(5),
        base: Some(position(2)),
        next_base: position(2),
        next_tail: &tail,
    };
    assert_eq!(validate_append(&legal), Ok(()));

    // Another writer checkpointed the same revision at 5 before this retry:
    // the delta still names checkpoint 2, which collection may delete.
    let rebased = AppendPlan {
        appended: position(6),
        base: Some(position(5)),
        ..legal
    };
    assert!(validate_append(&rebased).is_err());
    let unbased = AppendPlan {
        base: None,
        ..legal
    };
    assert!(validate_append(&unbased).is_err());
    let cached_short = AppendPlan {
        next_tail: &tail[1..],
        ..legal
    };
    assert!(validate_append(&cached_short).is_err());

    let broken = [
        delta(revision(0), expected),
        delta(revision(7), state.revision),
    ];
    let broken_record = encode_delta_record(position(2), &broken);
    let unchained = AppendPlan {
        record: &broken_record,
        next_tail: &broken,
        ..legal
    };
    assert!(validate_append(&unchained).is_err());

    let stale = [delta(revision(0), expected), delta(expected, revision(8))];
    let stale_record = encode_delta_record(position(2), &stale);
    let unreported = AppendPlan {
        record: &stale_record,
        next_tail: &stale,
        ..legal
    };
    assert!(validate_append(&unreported).is_err());
}
