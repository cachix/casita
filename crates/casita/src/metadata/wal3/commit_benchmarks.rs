//! CPU and copying costs of preparing the cumulative commit tail, without I/O.

use super::*;
use std::hint::black_box;

struct Case {
    name: &'static str,
    prior: Vec<StateDelta>,
    delta: Option<StateDelta>,
}

fn deltas(count: usize, catalog_bytes: usize) -> Vec<StateDelta> {
    (0..count)
        .map(|index| {
            let mut objects = (0..16_u32)
                .map(|object| {
                    let digest = crate::Digest::from(
                        *blake3::hash(&(index as u32 * 16 + object).to_le_bytes()).as_bytes(),
                    );
                    let payload = crate::BlobId::new(digest);
                    ObjectRecord::new(ObjectKey::blob(payload), payload, 4, Vec::new()).unwrap()
                })
                .collect::<Vec<_>>();
            objects.sort_by(|left, right| left.key().cmp(right.key()));
            StateDelta {
                expected: RepositoryRevision::from_bytes([index as u8; 32]),
                revision: RepositoryRevision::from_bytes([index as u8 + 1; 32]),
                roots: vec![RootChange::Set {
                    name: RootName::try_from(format!("benchmark/root-{index}")).unwrap(),
                    target: objects[0].key().clone(),
                }],
                validated: vec![objects[0].key().clone()],
                objects,
                payload_catalog: Some(vec![0x5a; catalog_bytes]),
            }
        })
        .collect()
}

fn case(name: &'static str, mut deltas: Vec<StateDelta>) -> Case {
    let delta = deltas.pop();
    Case {
        name,
        prior: deltas,
        delta,
    }
}

fn cases() -> Vec<Case> {
    let base = wal3::LogPosition::from_offset(7);
    let overhead = legacy_encode_record(base, &deltas(1, 0)).len();
    vec![
        Case {
            name: "collection",
            prior: deltas(8, 64 * 1024),
            delta: None,
        },
        case("tail_8", deltas(8, 64 * 1024)),
        case("tail_9", deltas(9, 64 * 1024)),
        case(
            "bytes_below",
            deltas(1, MAX_DELTA_RECORD_BYTES - overhead - 1),
        ),
        case("bytes_at", deltas(1, MAX_DELTA_RECORD_BYTES - overhead)),
        case(
            "bytes_above",
            deltas(1, MAX_DELTA_RECORD_BYTES - overhead + 1),
        ),
    ]
}

// Retain the previous serializer and cloning behavior for paired measurements.
fn legacy_put_entries(out: &mut Vec<u8>, entries: impl Iterator<Item = Vec<u8>>) {
    let entries = entries.collect::<Vec<_>>();
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&(entry.len() as u64).to_le_bytes());
        out.extend_from_slice(&entry);
    }
}

fn legacy_encode_delta(delta: &StateDelta) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(delta.expected.as_bytes());
    out.extend_from_slice(delta.revision.as_bytes());
    legacy_put_entries(&mut out, delta.objects.iter().map(ObjectRecord::encode));
    legacy_put_entries(&mut out, delta.roots.iter().map(encode_root_change));
    legacy_put_entries(&mut out, delta.validated.iter().map(ObjectKey::encode));
    match &delta.payload_catalog {
        Some(catalog) => {
            out.push(1);
            out.extend_from_slice(&(catalog.len() as u64).to_le_bytes());
            out.extend_from_slice(catalog);
        }
        None => out.push(0),
    }
    out
}

fn legacy_encode_record(base: wal3::LogPosition, tail: &[StateDelta]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(DELTA_MAGIC_V1);
    out.extend_from_slice(&base.offset().to_le_bytes());
    legacy_put_entries(&mut out, tail.iter().map(legacy_encode_delta));
    out
}

fn legacy_prepare(
    base: wal3::LogPosition,
    prior: &[StateDelta],
    delta: Option<StateDelta>,
) -> (Vec<StateDelta>, Option<Vec<u8>>) {
    let mut tail = if delta.is_some() {
        prior.to_vec()
    } else {
        Vec::new()
    };
    if let Some(delta) = delta {
        tail.push(delta);
    }
    let record = legacy_encode_record(base, &tail);
    let checkpoint =
        tail.is_empty() || tail.len() > MAX_TAIL_DELTAS || record.len() > MAX_DELTA_RECORD_BYTES;
    (tail, (!checkpoint).then_some(record))
}

#[test]
fn commit_preparation_preserves_wire_format_and_checkpoint_boundaries() {
    let base = wal3::LogPosition::from_offset(7);
    for case in cases() {
        let mut prior = case.prior.clone();
        let expected = legacy_prepare(base, &prior, case.delta.clone());
        let tail = take_commit_tail(&mut prior, case.delta.clone());
        let record = encode_commit_delta(base, &tail);
        assert_eq!(
            (&tail, &record),
            (&expected.0, &expected.1),
            "{}",
            case.name
        );
        assert_eq!(
            record.is_none(),
            matches!(case.name, "collection" | "tail_9" | "bytes_above")
        );
        if case.delta.is_some() {
            assert!(
                prior.is_empty(),
                "ordinary commits must move their input tail"
            );
        } else {
            assert_eq!(
                prior, case.prior,
                "collection must discard the old cumulative tail"
            );
        }
        if let Some(record) = record {
            let (decoded_base, decoded_tail) = decode_delta_record(&record).unwrap();
            assert_eq!(decoded_base, base);
            assert_eq!(decoded_tail, tail);
        }
    }
}

#[test]
#[ignore = "CPU benchmark, run via benchmark run wal3-commit-preparation"]
fn benchmark_commit_preparation() {
    let iterations = std::env::var("CASITA_STATE_BENCH_ITERATIONS")
        .unwrap_or_else(|_| "100".into())
        .parse::<usize>()
        .unwrap();
    assert!(iterations > 0);
    let base = wal3::LogPosition::from_offset(7);
    for case in cases() {
        let expected = legacy_prepare(base, &case.prior, case.delta.clone());
        let mut elapsed = [0_u128; 2];
        for iteration in 0..iterations {
            // Alternate order; constructing the owned input is outside timing.
            for mode in [iteration % 2, 1 - iteration % 2] {
                let mut prior = case.prior.clone();
                let delta = case.delta.clone();
                let started = Instant::now();
                let actual = black_box(if mode == 0 {
                    legacy_prepare(black_box(base), black_box(&prior), black_box(delta))
                } else {
                    let tail = take_commit_tail(black_box(&mut prior), black_box(delta));
                    let record = encode_commit_delta(black_box(base), black_box(&tail));
                    (tail, record)
                });
                elapsed[mode] += started.elapsed().as_nanos();
                assert_eq!(actual, expected, "{}", case.name);
                if let Some(record) = &actual.1 {
                    assert_eq!(
                        decode_delta_record(record).unwrap(),
                        (base, actual.0.clone())
                    );
                }
            }
        }
        println!("{}_legacy_nanos {}", case.name, elapsed[0]);
        println!("{}_candidate_nanos {}", case.name, elapsed[1]);
        println!(
            "{}_checkpoint {}",
            case.name,
            usize::from(expected.1.is_none())
        );
        println!("{}_tail_entries {}", case.name, expected.0.len());
        println!(
            "{}_encoded_bytes {}",
            case.name,
            expected.1.as_ref().map_or(0, Vec::len)
        );
    }
    println!("iterations {iterations}");
    println!("correctness_cases 6");
}
