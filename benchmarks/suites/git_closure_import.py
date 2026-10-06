"""Inventory-free cold, warm, cross-root and wide-delta Git closure imports."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import pathlib
import subprocess
import statistics
import tempfile

from benchmarks import cli
from benchmarks.affinity import cpu_affinity, cpu_list
from benchmarks.suites import git_witness_policy
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import positive_csv

PROBE = "benchmark_git_closure_import"
CORRECTNESS = "exact imported/reused counts and exhaustive closure verification"
# Blobs each operation adds beyond the cold tree's files: one new leaf per delta.
ADDED_BLOBS = {"cold": 0, "warm": 0, "subtree-delta": 1, "wide-delta": 2}
# Every configuration that makes one sample's workload differ from another's.
WORKLOAD = ("backend", "files", "file_bytes", "content", "packed", "concurrency", "max_buffered_bytes")
# Per-import resource measurements. Probes that predate them, or platforms
# without /proc, report none; such pairs are summarized by time alone.
RESOURCES = ("written_bytes", "peak_rss_bytes")


def rotation_expectations(value):
    """Parse FILES=ROTATIONS pairs for the candidate's cold imports."""
    expected = {}
    for part in value.split(","):
        files, separator, rotations = part.partition("=")
        try:
            files, rotations = int(files), int(rotations)
        except ValueError as error:
            raise argparse.ArgumentTypeError("expected comma-separated FILES=ROTATIONS pairs") from error
        if not separator or files < 1 or rotations < 0 or files in expected:
            raise argparse.ArgumentTypeError("expected distinct positive file counts and non-negative rotations")
        expected[files] = rotations
    return expected


def expected_blob_witnesses(operation, files, policy):
    """Stored blob witnesses after `operation`: every imported blob, or none where presence suffices."""
    if not git_witness_policy.STORES_BLOB_WITNESSES[policy]:
        return 0
    return files + ADDED_BLOBS[operation]


def summarize_pairs(samples):
    """Pair identical workloads by repetition; report effects without hiding spread."""
    dimensions = ("operation", *WORKLOAD)
    groups = {}
    for sample in samples:
        if not isinstance(sample.get("root"), str) or not sample["root"]:
            raise common.BenchmarkError("missing or invalid benchmark root identity")
        key = tuple(sample[name] for name in dimensions)
        repetitions = groups.setdefault(key, {})
        pair = repetitions.setdefault(sample["repetition"], {})
        if sample["variant"] in pair:
            raise common.BenchmarkError("duplicate variant in paired measurement")
        pair[sample["variant"]] = sample
    summaries = []
    for key, repetitions in groups.items():
        baseline, candidate, reductions = [], [], []
        for pair in repetitions.values():
            if set(pair) != {"baseline", "candidate"}:
                raise common.BenchmarkError("incomplete paired measurement")
            before, after = pair["baseline"], pair["candidate"]
            if before.get("root") != after.get("root"):
                raise common.BenchmarkError("paired imports produced different root identities")
            baseline.append(before["wall_seconds"])
            candidate.append(after["wall_seconds"])
            reductions.append(100 * (1 - after["wall_seconds"] / before["wall_seconds"]))
        summary = dict(zip(dimensions, key), pairs=len(baseline),
            enough_samples=len(baseline) >= 5,
            baseline_median_seconds=statistics.median(baseline),
            candidate_median_seconds=statistics.median(candidate),
            median_paired_reduction_percent=statistics.median(reductions),
            minimum_paired_reduction_percent=min(reductions),
            maximum_paired_reduction_percent=max(reductions))
        for resource in RESOURCES:
            values = [(pair["baseline"].get(resource), pair["candidate"].get(resource))
                      for pair in repetitions.values()]
            # A zero baseline, such as a warm import that writes nothing, has no ratio.
            if all(before is not None and after is not None and before > 0 for before, after in values):
                summary[f"baseline_median_{resource}"] = statistics.median(before for before, _ in values)
                summary[f"candidate_median_{resource}"] = statistics.median(after for _, after in values)
                summary[f"median_paired_{resource}_reduction_percent"] = statistics.median(
                    100 * (1 - after / before) for before, after in values)
        summaries.append(summary)
    return summaries


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--counts", type=positive_csv)
    parser.add_argument("--max-buffered-bytes", type=positive_csv, default=[1023, 1024, 1025, 2047, 2048, 2049])
    parser.add_argument("--layout", choices=("loose", "packed", "both"), default="both")
    parser.add_argument("--backend", choices=("memory", "local", "both"), default="both")
    parser.add_argument("--file-bytes", type=positive_csv, default=[1024])
    parser.add_argument("--concurrency", type=positive_csv, default=[16])
    parser.add_argument("--content", choices=("repeated", "random", "mixed"), default="repeated")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--expected-rotations", type=rotation_expectations,
                        help="FILES=ROTATIONS writer rotations each candidate cold import must perform")
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--baseline-binary", type=pathlib.Path)
    parser.add_argument("--cpu-affinity", type=cpu_list)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a probe binary with --no-build are required")
    if min(args.file_bytes) < 8:
        parser.error("file sizes must be at least 8 bytes to encode distinct objects")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--no-default-features", "--features", "native,git,experimental",
                                "--test", "git_closure_import", "--no-run", "--message-format=json"],
                               cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
        paths = [item["executable"] for item in artifacts if item.get("reason") == "compiler-artifact"
                 and item.get("target", {}).get("name") == "git_closure_import" and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one Git closure benchmark executable")
        binary = pathlib.Path(paths[0])
    binary = binary.resolve()
    variants = [("candidate", binary)]
    if args.baseline_binary:
        variants.insert(0, ("baseline", args.baseline_binary.resolve()))
    artifacts = []
    for variant, executable in variants:
        with executable.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        artifact = dict(variant=variant, path=str(executable), sha256=digest)
        manifest = pathlib.Path(str(executable) + ".build.json")
        if manifest.exists():
            build = json.loads(manifest.read_text())
            if build.get("executable_sha256") != digest:
                raise common.BenchmarkError("build manifest fingerprint does not match executable")
            if not build.get("lockfile_sha256"):
                raise common.BenchmarkError("build manifest is missing its dependency lockfile fingerprint")
            artifact["build"] = build
        artifacts.append(artifact)
    if len(artifacts) == 2 and all("build" in artifact for artifact in artifacts):
        for field in ("lockfile_sha256", "features", "default_features", "rustc_version", "rustflags"):
            if artifacts[0]["build"].get(field) != artifacts[1]["build"].get(field):
                raise common.BenchmarkError(f"paired build manifests differ in {field}")
    if len(artifacts) == 2 and artifacts[0]["sha256"] == artifacts[1]["sha256"]:
        parser.error("baseline and candidate executables must have distinct hashes")
    by_variant = {artifact["variant"]: artifact for artifact in artifacts}
    counts = args.counts or ([63, 64, 65] if args.profile == "smoke" else
                            [63, 64, 65, 255, 256, 257, 509, 510, 511, 1022, 1023, 10000])
    if args.expected_rotations is not None and set(args.expected_rotations) != set(counts):
        parser.error("--expected-rotations must name exactly the file counts")
    layouts = [False, True] if args.layout == "both" else [args.layout == "packed"]
    backends = ["memory", "local"] if args.backend == "both" else [args.backend]
    result = dict(schema_version=1, result_schema="casita.git-closure-import.v1", suite_id="native-git",
                  complete=False, artifacts=artifacts, samples=[], processes=[], configuration=dict(
                      profile=args.profile, counts=counts, byte_budgets=args.max_buffered_bytes, layouts=layouts,
                      backends=backends, file_bytes=args.file_bytes, concurrency=args.concurrency,
                      content=args.content, paired=bool(args.baseline_binary), cpu_affinity=args.cpu_affinity,
                      memory_measurement="whole-process peak RSS includes fixture creation and audits",
                      import_memory_measurement="peak_rss_bytes is the resident high-water mark during each import, reset just before it",
                      expected_rotations=args.expected_rotations,
                      witness_policy="each probe declares its own; blob witnesses must match it exactly",
                      spill_memory_objects=64, metadata_frontier=256,
                      repetitions=args.repetitions, timing="import only; fixture generation and exhaustive audits excluded"))
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-git-closure-benchmark-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        if hasattr(os, "sched_getaffinity"):
            allowed = sorted(os.sched_getaffinity(0))
            result["environment"]["cpu_affinity"] = allowed
            frequencies = {}
            for cpu in allowed:
                frequency = pathlib.Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/cpuinfo_max_freq")
                if frequency.exists():
                    frequencies[str(cpu)] = int(frequency.read_text().strip())
            result["environment"]["cpu_max_frequencies_khz"] = frequencies
        try:
            matrix = itertools.product(counts, args.max_buffered_bytes, layouts, backends,
                                       args.file_bytes, args.concurrency, range(args.repetitions))
            for count, budget, packed, backend, file_bytes, concurrency, repetition in matrix:
                ordered = variants if repetition % 2 == 0 else list(reversed(variants))
                for variant, executable in ordered:
                    env = {**os.environ, "CASITA_GIT_CLOSURE_FILES": str(count), "CASITA_GIT_CLOSURE_BYTES": str(budget),
                           "CASITA_GIT_CLOSURE_PACKED": str(int(packed)),
                           "CASITA_GIT_CLOSURE_BACKEND": backend, "CASITA_GIT_CLOSURE_FILE_BYTES": str(file_bytes),
                           "CASITA_GIT_CLOSURE_CONCURRENCY": str(concurrency), "CASITA_GIT_CLOSURE_CONTENT": args.content}
                    timing = common.measured_command(common.CommandSpec(
                        [[str(executable), PROBE, "--exact", "--ignored", "--nocapture"]], work, env),
                        work / "stdout", work / "stderr", check=False)
                    stdout, stderr = (work / "stdout").read_text(), (work / "stderr").read_text()
                    result["processes"].append(dict(**timing, variant=variant, files=count, budget=budget, packed=packed,
                                                    backend=backend, file_bytes=file_bytes, concurrency=concurrency, content=args.content,
                                                    repetition=repetition, stdout=stdout, stderr=stderr))
                    if timing["exit_code"] != 0:
                        raise common.BenchmarkError(f"Git closure benchmark failed: {stdout}\n{stderr}")
                    rows = [json.loads(line.removeprefix("git_closure_sample ")) for raw in stdout.splitlines()
                            if (line := raw.removeprefix(f"test {PROBE} ... ")).startswith("git_closure_sample ")]
                    if ([row.get("operation") for row in rows] != ["cold", "warm", "subtree-delta", "wide-delta"]
                            or "test result: ok. 1 passed; 0 failed;" not in stdout):
                        raise common.BenchmarkError("missing benchmark operations or passing correctness gate")
                    for row in rows:
                        if not isinstance(row.get("root"), str) or not row["root"]:
                            raise common.BenchmarkError("missing or invalid benchmark root identity")
                        if (row.get("correctness") != CORRECTNESS or row.get("files") != count
                                or row.get("packed") != packed or row.get("max_buffered_bytes") != budget
                                or row.get("backend") != backend or row.get("file_bytes") != file_bytes
                                or row.get("concurrency") != concurrency or row.get("content") != args.content
                                or not isinstance(row.get("wall_nanos"), int) or row["wall_nanos"] <= 0
                                or any(row.get(resource) is not None
                                       and (type(row[resource]) is not int or row[resource] < 0)
                                       for resource in (*RESOURCES, "writer_rotations"))):
                            raise common.BenchmarkError("wrong benchmark configuration or correctness gate")
                        if (args.expected_rotations is not None and variant == "candidate"
                                and row["operation"] == "cold"
                                and row.get("writer_rotations") != args.expected_rotations[count]):
                            raise common.BenchmarkError(
                                f"cold import of {count} files rotated its writer {row.get('writer_rotations')!r} "
                                f"times, expected {args.expected_rotations[count]}")
                        expected = {"cold": (count + 2, 0), "warm": (0, 1),
                                    "subtree-delta": (2, 1), "wide-delta": (2, count)}[row["operation"]]
                        if ((row.get("imported_objects"), row.get("reused_objects")) != expected
                                or (row["operation"] == "warm" and row.get("source_bytes") != 0)):
                            raise common.BenchmarkError("incorrect import/reuse counters")
                        policy = git_witness_policy.declared(by_variant[variant], row)
                        blob_witnesses = expected_blob_witnesses(row["operation"], count, policy)
                        if row.get("blob_witnesses") != blob_witnesses or type(row["blob_witnesses"]) is not int:
                            raise common.BenchmarkError(
                                f"{row['operation']} import under witness policy {policy!r} stored "
                                f"{row.get('blob_witnesses')!r} blob witnesses, expected {blob_witnesses}")
                        result["samples"].append(dict(status="ok", implementation="casita", variant=variant, entries=count,
                            repetition=repetition, wall_seconds=row["wall_nanos"] / 1e9,
                            max_rss_bytes=timing["max_rss_bytes"], **row))
                    common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
            if args.baseline_binary:
                result["paired_summary"] = summarize_pairs(result["samples"])
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
