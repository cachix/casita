//! Bug arms for the commit-transition validators every metadata store shares.

use super::*;
use crate::{BlobId, Digest};

fn record(index: u8) -> ObjectRecord {
    let key = ObjectKey::new(
        crate::NamespaceId::try_from("test.transition.v1").unwrap(),
        vec![index],
    )
    .unwrap();
    ObjectRecord::new(key, BlobId::new(Digest::hash(&[index])), 1, Vec::new()).unwrap()
}

fn revision(byte: u8) -> RepositoryRevision {
    RepositoryRevision::from_bytes([byte; 32])
}

fn result(revision: RepositoryRevision) -> CommitResult {
    CommitResult {
        revision,
        objects_inserted: 0,
        objects_removed: 0,
        roots_changed: 0,
    }
}

const PREVIOUS: CommitState<'static> = CommitState {
    revision: RepositoryRevision::from_bytes([1; 32]),
    generation: 4,
    payload_catalog: Some(b"old catalog"),
};

const NEXT: CommitState<'static> = CommitState {
    revision: RepositoryRevision::from_bytes([2; 32]),
    generation: 5,
    payload_catalog: Some(b"old catalog"),
};

fn unchanged(result: &CommitResult) -> CommitChange<'_> {
    CommitChange {
        payload_catalog: None,
        objects: ObjectChange::Added(Vec::new()),
        result,
    }
}

#[test]
fn a_commit_must_advance_generation_by_one_and_change_revision() {
    let committed = result(NEXT.revision);
    let check = |next: &CommitState<'_>, change: &CommitChange<'_>| {
        validate_commit_transition("test", &PREVIOUS, next, change)
    };
    assert_eq!(check(&NEXT, &unchanged(&committed)), Ok(()));

    for generation in [4, 6, 0] {
        let next = CommitState { generation, ..NEXT };
        assert!(
            check(&next, &unchanged(&committed)).is_err(),
            "generation 4 -> {generation} must be refused"
        );
    }

    let reused = result(PREVIOUS.revision);
    let next = CommitState {
        revision: PREVIOUS.revision,
        ..NEXT
    };
    assert!(check(&next, &unchanged(&reused)).is_err());

    let misreported = result(revision(3));
    assert!(check(&NEXT, &unchanged(&misreported)).is_err());
}

#[test]
fn the_payload_catalog_moves_only_to_the_one_a_mutation_sets() {
    let committed = result(NEXT.revision);
    let replaced = CommitState {
        payload_catalog: Some(b"new catalog"),
        ..NEXT
    };
    let setting = CommitChange {
        payload_catalog: Some(b"new catalog"),
        ..unchanged(&committed)
    };
    assert_eq!(
        validate_commit_transition("test", &PREVIOUS, &replaced, &setting),
        Ok(())
    );

    assert!(
        validate_commit_transition("test", &PREVIOUS, &replaced, &unchanged(&committed)).is_err(),
        "an unset catalog changed"
    );
    let dropped = CommitState {
        payload_catalog: None,
        ..NEXT
    };
    assert!(
        validate_commit_transition("test", &PREVIOUS, &dropped, &unchanged(&committed)).is_err(),
        "an unset catalog disappeared"
    );
    let other = CommitChange {
        payload_catalog: Some(b"other catalog"),
        ..unchanged(&committed)
    };
    assert!(
        validate_commit_transition("test", &PREVIOUS, &replaced, &other).is_err(),
        "the stored catalog is not the one set"
    );
}

#[test]
fn an_addition_never_rewrites_drops_or_miscounts_an_object() {
    let (new, existing, other) = (record(1), record(2), record(3));
    let legal = vec![
        AddedObject {
            key: new.key(),
            before: None,
            after: Some(&new),
        },
        AddedObject {
            key: existing.key(),
            before: Some(&existing),
            after: Some(&existing),
        },
        // A repeated key observes its own first insertion.
        AddedObject {
            key: new.key(),
            before: Some(&new),
            after: Some(&new),
        },
    ];
    let inserted_one = CommitResult {
        objects_inserted: 1,
        ..result(NEXT.revision)
    };
    let added = |objects: Vec<AddedObject<'_>>, result| {
        validate_commit_transition(
            "test",
            &PREVIOUS,
            &NEXT,
            &CommitChange {
                payload_catalog: None,
                objects: ObjectChange::Added(objects),
                result,
            },
        )
    };
    assert_eq!(added(legal.clone(), &inserted_one), Ok(()));

    let rewritten = vec![AddedObject {
        key: existing.key(),
        before: Some(&existing),
        after: Some(&other),
    }];
    let nothing_new = result(NEXT.revision);
    assert!(added(rewritten, &nothing_new).is_err());

    let dropped = vec![AddedObject {
        key: new.key(),
        before: None,
        after: None,
    }];
    assert!(added(dropped, &inserted_one).is_err());

    let counted_twice = CommitResult {
        objects_inserted: 2,
        ..inserted_one.clone()
    };
    assert!(added(legal.clone(), &counted_twice).is_err());
    let removing = CommitResult {
        objects_removed: 1,
        ..inserted_one
    };
    assert!(added(legal, &removing).is_err());
}

#[test]
fn a_collection_keeps_exactly_its_retained_set_and_every_root_target() {
    let legal = CollectedObjects {
        retained: 3,
        before: 5,
        after: 3,
        missing_root_target: None,
    };
    let removed_two = CommitResult {
        objects_removed: 2,
        ..result(NEXT.revision)
    };
    let collected = |objects: CollectedObjects, result| {
        validate_commit_transition(
            "test",
            &PREVIOUS,
            &NEXT,
            &CommitChange {
                payload_catalog: None,
                objects: ObjectChange::Collected(objects),
                result,
            },
        )
    };
    assert_eq!(collected(legal.clone(), &removed_two), Ok(()));

    let kept_extra = CollectedObjects {
        after: 4,
        ..legal.clone()
    };
    let removed_one = CommitResult {
        objects_removed: 1,
        ..removed_two.clone()
    };
    assert!(collected(kept_extra, &removed_one).is_err());
    assert!(collected(legal.clone(), &removed_one).is_err());
    let grew = CollectedObjects {
        before: 2,
        ..legal.clone()
    };
    let nothing_removed = result(NEXT.revision);
    assert!(collected(grew, &nothing_removed).is_err());
    let unrooted = CollectedObjects {
        missing_root_target: Some(record(1).key().clone()),
        ..legal.clone()
    };
    assert!(collected(unrooted, &removed_two).is_err());
    let inserting = CommitResult {
        objects_inserted: 1,
        ..removed_two.clone()
    };
    assert!(collected(legal.clone(), &inserting).is_err());
    let renaming = CommitResult {
        roots_changed: 1,
        ..removed_two
    };
    assert!(collected(legal, &renaming).is_err());
}

#[test]
fn a_facts_clear_advances_the_generation_once_and_keeps_only_tombstones() {
    let one = 1_u64.to_le_bytes();
    let five = 5_u64.to_le_bytes();
    let six = 6_u64.to_le_bytes();
    let seven = 7_u64.to_le_bytes();
    let first = FactsClear {
        previous_generation: None,
        next_generation: Some(&one),
        tombstone: Some(&[]),
        facts_remain: false,
    };
    assert_eq!(validate_facts_clear("test", &first), Ok(()));
    let later = FactsClear {
        previous_generation: Some(&five),
        next_generation: Some(&six),
        ..first
    };
    assert_eq!(validate_facts_clear("test", &later), Ok(()));

    for next_generation in [Some(five.as_slice()), Some(&seven), None] {
        let clear = FactsClear {
            next_generation,
            ..later
        };
        assert!(validate_facts_clear("test", &clear).is_err());
    }
    let corrupt = FactsClear {
        previous_generation: Some(b"bad"),
        next_generation: None,
        ..first
    };
    assert!(validate_facts_clear("test", &corrupt).is_err());
    for tombstone in [None, Some(b"fact".as_slice())] {
        let clear = FactsClear { tombstone, ..later };
        assert!(validate_facts_clear("test", &clear).is_err());
    }
    let surviving = FactsClear {
        facts_remain: true,
        ..later
    };
    assert!(validate_facts_clear("test", &surviving).is_err());
}
