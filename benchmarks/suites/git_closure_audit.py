"""Git closure imports of linear histories with built-in and custom format registries."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import pathlib
import statistics
import subprocess
import tempfile

from benchmarks import cli
from benchmarks.affinity import cpu_affinity, cpu_list
from benchmarks.suites import git_witness_policy
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import positive_csv

PROBE = "benchmark_git_closure_audit"
SAMPLE = "git_closure_audit_sample "
CORRECTNESS = "exact import counts, exhaustive closure verification and source-free warm reuse"
WITNESS_FIELDS = ("witnesses", "witness_commits", "max_witness_batch")
# Every configuration that makes one sample's workload differ from another's.
WORKLOAD = ("commits", "registry", "publication_batch_objects")


def build_probe():
    command = ["cargo", "test", "--release", "-p", "casita", "--no-default-features",
               "--features", "native,git,experimental", "--test", "git_closure_custom_formats",
               "--no-run", "--message-format=json"]
    built = subprocess.run(command, cwd=cli.ROOT, capture_output=True, text=True)
    if built.returncode:
        raise common.BenchmarkError(built.stderr or built.stdout)
    rows = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
    paths = [row["executable"] for row in rows if row.get("reason") == "compiler-artifact"
             and row.get("target", {}).get("name") == "git_closure_custom_formats" and row.get("executable")]
    if len(paths) != 1:
        raise common.BenchmarkError("expected one Git closure audit probe executable")
    return pathlib.Path(paths[0])


def expected_witnesses(commits, registry, policy):
    """Witnesses for a history whose commits each add a commit, a tree and a blob.

    Custom registries witness every object they audited. Built-in registries
    witness each commit and tree, and each blob only under a storing policy.
    """
    if registry == "custom" or git_witness_policy.STORES_BLOB_WITNESSES[policy]:
        return 3 * commits
    return 2 * commits


def check_row(row, commits, registry, batch, policy, variant="candidate"):
    """Reject a sample unless it imported, audited, witnessed and reused exactly the expected graph.

    Only a paired baseline may audit more than once per object: comparing
    against a slower audit is what a baseline is for.
    """
    objects = 3 * commits
    if (row.get("correctness") != CORRECTNESS or row.get("commits") != commits
            or row.get("registry") != registry
            or row.get("publication_batch_objects") != batch or row.get("objects") != objects
            or not isinstance(row.get("root"), str) or not row["root"]
            or not isinstance(row.get("wall_nanos"), int) or row["wall_nanos"] <= 0):
        raise common.BenchmarkError("wrong Git closure audit configuration or correctness gate")
    if ((row.get("imported_objects"), row.get("reused_objects")) != (objects, 0)
            or (row.get("warm_imported_objects"), row.get("warm_source_bytes")) != (0, 0)):
        raise common.BenchmarkError("incorrect Git closure audit import or reuse counters")
    # Built-in registries trust construction, which no verifier call observes.
    # A custom registry audits each object once: repeating shared history in
    # every closure walk would make audits grow quadratically.
    audits = row.get("link_audits")
    if registry == "builtin":
        audited = audits is None
        expected_audits = "none"
    else:
        audited = type(audits) is int and (audits >= objects if variant == "baseline" else audits == objects)
        expected_audits = f"at least {objects}" if variant == "baseline" else objects
    if not audited:
        raise common.BenchmarkError(
            f"{registry} registry recorded {audits!r} link audits, expected {expected_audits}")
    # Witnesses stream from a spilling proof set in full batches: the largest
    # commit, and so the in-memory witness inventory, never exceeds one batch.
    witnesses = expected_witnesses(commits, registry, policy)
    expected = dict(witnesses=witnesses, witness_commits=-(-witnesses // batch),
                    max_witness_batch=min(batch, witnesses))
    recorded = {field: row.get(field) for field in WITNESS_FIELDS}
    if recorded != expected or any(type(value) is not int for value in recorded.values()):
        raise common.BenchmarkError(
            f"{registry} registry under witness policy {policy!r} recorded {recorded}, expected {expected}")


COUNTS = ("link_audits", "witnesses", "max_witness_batch")


def summarize_pairs(samples):
    """Pair identical workloads by repetition; report effects without hiding spread."""
    groups = {}
    for sample in samples:
        key = tuple(sample[name] for name in WORKLOAD)
        pair = groups.setdefault(key, {}).setdefault(sample["repetition"], {})
        if sample["variant"] in pair:
            raise common.BenchmarkError("duplicate variant in paired measurement")
        pair[sample["variant"]] = sample
    summaries = []
    for key, repetitions in groups.items():
        before, after, reductions, counts = [], [], [], set()
        for pair in repetitions.values():
            if set(pair) != {"baseline", "candidate"}:
                raise common.BenchmarkError("incomplete paired measurement")
            if pair["baseline"]["root"] != pair["candidate"]["root"]:
                raise common.BenchmarkError("paired imports produced different root identities")
            before.append(pair["baseline"]["wall_seconds"])
            after.append(pair["candidate"]["wall_seconds"])
            reductions.append(100 * (1 - after[-1] / before[-1]))
            counts.add(tuple(pair[variant][field] for variant in ("baseline", "candidate")
                             for field in COUNTS))
        if len(counts) != 1:
            raise common.BenchmarkError("deterministic audit or witness counts differ between repetitions")
        counts, = counts
        summaries.append(dict(zip(WORKLOAD, key), pairs=len(before), enough_samples=len(before) >= 5,
            **{f"{variant}_{field}": count for (variant, field), count
               in zip(itertools.product(("baseline", "candidate"), COUNTS), counts)},
            baseline_median_seconds=statistics.median(before),
            candidate_median_seconds=statistics.median(after),
            median_paired_reduction_percent=statistics.median(reductions),
            minimum_paired_reduction_percent=min(reductions),
            maximum_paired_reduction_percent=max(reductions)))
    return summaries


def fingerprint(variant, executable):
    with executable.open("rb") as stream:
        artifact = dict(variant=variant, path=str(executable), sha256=hashlib.file_digest(stream, "sha256").hexdigest())
    manifest = pathlib.Path(str(executable) + ".build.json")
    if manifest.exists():
        build = json.loads(manifest.read_text())
        if build.get("executable_sha256") != artifact["sha256"]:
            raise common.BenchmarkError("build manifest fingerprint does not match executable")
        if not build.get("lockfile_sha256"):
            raise common.BenchmarkError("build manifest is missing its dependency lockfile fingerprint")
        artifact["build"] = build
    return artifact


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--commits", type=positive_csv)
    parser.add_argument("--registry", choices=("builtin", "custom", "both"), default="both")
    parser.add_argument("--publication-batch-objects", type=positive_csv, default=[64, 4096])
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--baseline-binary", type=pathlib.Path)
    parser.add_argument("--cpu-affinity", type=cpu_list)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a probe binary with --no-build are required")
    variants = [("candidate", (args.probe_binary or build_probe()).resolve())]
    if args.baseline_binary:
        variants.insert(0, ("baseline", args.baseline_binary.resolve()))
    artifacts = [fingerprint(variant, executable) for variant, executable in variants]
    by_variant = {artifact["variant"]: artifact for artifact in artifacts}
    if len(artifacts) == 2:
        if artifacts[0]["sha256"] == artifacts[1]["sha256"]:
            parser.error("baseline and candidate executables must have distinct hashes")
        if all("build" in artifact for artifact in artifacts):
            for field in ("lockfile_sha256", "features", "default_features", "rustc_version", "rustflags"):
                if artifacts[0]["build"].get(field) != artifacts[1]["build"].get(field):
                    raise common.BenchmarkError(f"paired build manifests differ in {field}")
    # Each commit adds three objects. Custom registries witness all three, and
    # built-in ones two under derived-blobs. With 64-object witness batches, 16
    # commits fit in one batch while 64 span three or two. The default
    # 4096-object batch holds 1024 commits in one batch, while 4096 commits span
    # three or two.
    commits = args.commits or ([16, 64] if args.profile == "smoke" else [16, 64, 256, 1024, 4096])
    registries = ["builtin", "custom"] if args.registry == "both" else [args.registry]
    batches = args.publication_batch_objects
    result = dict(schema_version=1, result_schema="casita.git-closure-audit.v1", suite_id="native-git",
        complete=False, artifacts=artifacts, samples=[], processes=[], configuration=dict(
            profile=args.profile, commits=commits, registries=registries, publication_batch_objects=batches,
            backend="memory: custom format registries are configurable only with in-memory stores",
            objects_per_commit=3, repetitions=args.repetitions, paired=bool(args.baseline_binary),
            cpu_affinity=args.cpu_affinity, source="one packed linear history written by git fast-import",
            timing="cold import only; fixture generation, warm import and exhaustive audit excluded",
            witness_measurement="witnesses recorded by each metadata commit of the cold import",
            witness_policy="each probe declares its own; samples must match it exactly",
            memory_measurement="whole-process peak RSS includes fixture creation and audits"))
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-git-closure-audit-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        if hasattr(os, "sched_getaffinity"):
            result["environment"]["cpu_affinity"] = sorted(os.sched_getaffinity(0))
        try:
            matrix = itertools.product(commits, registries, batches, range(args.repetitions))
            for count, registry, batch, repetition in matrix:
                ordered = variants if repetition % 2 == 0 else list(reversed(variants))
                for variant, executable in ordered:
                    env = {**os.environ, "CASITA_GIT_AUDIT_COMMITS": str(count),
                           "CASITA_GIT_AUDIT_REGISTRY": registry,
                           "CASITA_GIT_AUDIT_BATCH_OBJECTS": str(batch)}
                    timing = common.measured_command(common.CommandSpec(
                        [[str(executable), PROBE, "--exact", "--ignored", "--nocapture"]], work, env),
                        work / "stdout", work / "stderr", check=False)
                    stdout, stderr = (work / "stdout").read_text(), (work / "stderr").read_text()
                    result["processes"].append(dict(**timing, variant=variant, commits=count, registry=registry,
                        publication_batch_objects=batch, repetition=repetition,
                        stdout=stdout, stderr=stderr))
                    lines = (line.removeprefix(f"test {PROBE} ... ") for line in stdout.splitlines())
                    rows = [json.loads(line.removeprefix(SAMPLE)) for line in lines if line.startswith(SAMPLE)]
                    if (timing["exit_code"] != 0 or len(rows) != 1
                            or "test result: ok. 1 passed; 0 failed;" not in stdout):
                        raise common.BenchmarkError(f"Git closure audit probe failed: {stdout}\n{stderr}")
                    row = rows[0]
                    policy = git_witness_policy.declared(by_variant[variant], row)
                    check_row(row, count, registry, batch, policy, variant)
                    audits = row["link_audits"]
                    result["samples"].append(dict(status="ok", operation="cold-import",
                        implementation="casita", variant=variant,
                        repetition=repetition, wall_seconds=row["wall_nanos"] / 1e9,
                        link_audits_per_object=None if audits is None else audits / row["objects"],
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
