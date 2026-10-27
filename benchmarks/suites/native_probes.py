"""Discoverable, fresh-process state and catalog maintenance probes."""
from __future__ import annotations
import argparse
import json
import os
import pathlib
import re
import tempfile
from benchmarks.suites import repository as common
from benchmarks.suites.pack.catalog import build_probe_binary

PROBES = {
    "state-publication": "metadata::benchmarks::benchmark_state_publication",
    "metadata-durability": "metadata::benchmarks::benchmark_metadata_commit_durability",
    "deletion-ordering": "repository::collection_benchmark::benchmark_collection_deletion_ordering",
    "catalog-maintenance": "blob::pack::benchmarks::benchmark_catalog_reclaim_marker_probe",
    "catalog-durability": "blob::pack::benchmarks::benchmark_local_catalog_durable_publication",
    "logical-state": "metadata::wal3_shard::tests::benchmark_logical_state_shards_scale",
    "wal3-commit-preparation": "metadata::wal3::commit_benchmarks::benchmark_commit_preparation",
    "raw-blob-closures": "repository::closure_benchmarks::benchmark_raw_blob_closures",
    "wal3-publication-checkpoints": "repository::wal3_publication_benchmark::benchmark_wal3_publication_checkpoints",
    "concurrent-publication": "repository::closure_benchmarks::benchmark_concurrent_publication",
}

# Batch sizes of the raw-blob-closures probe (BATCHES in closure_benchmarks.rs).
# Its --blobs must cover the largest, or that batch would run short.
RAW_BLOB_BATCHES = (512, 1024, 4096)

# Batch sizes of the wal3-publication-checkpoints probe's single-batch
# checkpoints (benchmark_wal3_publication_checkpoints): both sides of a block.
WAL3_CHECKPOINT_BATCHES = (511, 512, 513)

# Probes outside the default suites of their name prefix.
SUITES = {"deletion-ordering": "collection-and-fsck"}

def checkpoint_corpora(value):
    try:
        corpora = tuple(int(corpus) for corpus in value.split(","))
    except ValueError:
        raise argparse.ArgumentTypeError("expected comma-separated object counts") from None
    if not corpora or min(corpora) < 1 or len(set(corpora)) != len(corpora):
        raise argparse.ArgumentTypeError("expected distinct positive object counts")
    return corpora

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", choices=PROBES, default="state-publication")
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--entries", type=int, default=65536)
    parser.add_argument("--blobs", type=int, default=8192)
    parser.add_argument("--depth", type=int, default=64)
    parser.add_argument("--files", type=int, default=16)
    parser.add_argument("--checkpoint-corpora", type=checkpoint_corpora, default=(8192, 131072),
                        help="comma-separated object counts the wal3-publication-checkpoints probe "
                             "checkpoints before a 512-object publication")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if min(args.iterations, args.entries, args.blobs, args.depth, args.files, args.repetitions) < 1 or args.writers < 2:
        parser.error("positive counts and at least two writers are required")
    if args.probe == "raw-blob-closures" and args.blobs < max(RAW_BLOB_BATCHES):
        parser.error(f"--blobs must be at least {max(RAW_BLOB_BATCHES)} to fill every "
                     f"raw-blob-closures batch ({', '.join(map(str, RAW_BLOB_BATCHES))})")
    binary = (args.probe_binary or build_probe_binary()).resolve()
    env = {**os.environ, "CASITA_STATE_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_STATE_BENCH_WRITERS": str(args.writers),
        "CASITA_METADATA_DURABILITY_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_DELETION_ORDERING_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_CATALOG_MARKER_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_CATALOG_DURABILITY_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_LOGICAL_STATE_BENCH_ENTRIES": str(args.entries),
        "CASITA_RAW_BLOB_BENCH_BLOBS": str(args.blobs),
        "CASITA_PUBLICATION_BENCH_DEPTH": str(args.depth),
        "CASITA_PUBLICATION_BENCH_FILES": str(args.files),
        "CASITA_WAL3_CHECKPOINT_CORPORA": ",".join(map(str, args.checkpoint_corpora))}
    samples = []
    with tempfile.TemporaryDirectory(prefix="casita-native-probes-") as temporary:
        root = pathlib.Path(temporary)
        environment = common.environment_metadata(root)
        for repetition in range(1, args.repetitions + 1):
            stdout, stderr = root / "stdout", root / "stderr"
            timing = common.measured_command(common.CommandSpec(
                [[str(binary), PROBES[args.probe], "--exact", "--ignored", "--nocapture"]], root, env), stdout, stderr)
            output = stdout.read_text()
            if "1 passed" not in output:
                raise common.BenchmarkError("probe did not execute exactly one passing test")
            metrics = {key: int(value) for key, value in re.findall(r"(?:^|\s)([a-z][a-z0-9_]+) (\d+)(?=\s|$)", output)}
            if not metrics:
                raise common.BenchmarkError("probe emitted no metrics")
            # The WAL3 cases, where the tail limits live, need the s3 feature.
            if args.probe == "raw-blob-closures" and not any(key.startswith("wal3_") for key in metrics):
                raise common.BenchmarkError("raw-blob-closures probe binary lacks the s3 feature: "
                                            "no WAL3 case ran")
            if args.probe == "wal3-publication-checkpoints":
                cases = [(f"b{batch}", ("before", "after")) for batch in WAL3_CHECKPOINT_BATCHES]
                cases += [(f"c{corpus}", ("after",)) for corpus in args.checkpoint_corpora]
                for case, phases in cases:
                    for temperature in ("warm", "reopened"):
                        prefix = f"wal3_{case}_{temperature}"
                        if (metrics.get(prefix + "_validated") != 1 or
                                prefix + "_checkpoint_shards" not in metrics or
                                any(prefix + "_" + phase + "_nanos" not in metrics for phase in phases)):
                            raise common.BenchmarkError(f"missing checkpoint case: {prefix}")
            samples.append({"status": "ok", "implementation": "casita", "operation": args.probe,
                "repetition": repetition, **timing, "metrics": metrics, "stdout": output})
    common.write_atomic(args.output, json.dumps({"schema_version": 1,
        "result_schema": "casita.native-probes.v1",
        "suite_id": SUITES.get(args.probe, "blob-backends" if args.probe.startswith("catalog-") else "state-and-publication"),
        "environment": environment,
        "configuration": vars(args) | {"probe_binary": str(binary), "output": str(args.output)},
        "samples": samples}, indent=2) + "\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
