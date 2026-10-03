"""Bounded chunk hash jobs: cold/duplicate writes and admission boundaries."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import pathlib
import statistics
import subprocess
import tempfile
from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.affinity import cpu_affinity, cpu_list

PROBE = "benchmark_chunk_hash_batch"
CORRECTNESS = "independent BLAKE3 and FastCDC, exhaustive verified readback, duplicate chunk-write count"
# file bytes, average chunk bytes, upload concurrency, shared budget, content
CASES = {
    **{f"many-{n}": (4194304, 1024, n, 4194304, "random") for n in (1, 3, 4, 5, 16)},
    "many-periodic": (4194304, 1024, 4, 4194304, "periodic"),
    "one-permit": (1048576, 1024, 16, 65536, "random"),
    "three-permits": (1048576, 1024, 16, 196608, "random"),
    "four-permits": (1048576, 1024, 16, 262144, "random"),
    "default-4": (16777216, 262144, 4, 4194304, "random"),
    "default-16": (16777216, 262144, 16, 8388608, "random"),
    "default-periodic": (16777216, 262144, 4, 4194304, "periodic"),
    # Exact production settings, including the default 64 MiB shared budget.
    "production-default": (16777216, 262144, 32, 67108864, "random"),
    "production-default-large": (67108864, 262144, 32, 67108864, "random"),
    "production-default-periodic": (16777216, 262144, 32, 67108864, "periodic"),
    # Probe both sides of a 64 KiB hash-job cap (maximum chunk = 2 * average).
    **{f"small-job-cap-{delta:+d}": (8388608, 32768 + delta, 4, 8388608, "random") for delta in (-2, 0, 2)},
    **{f"small-{delta:+d}": (131072 + delta, 262144, 4, 4194304, "random") for delta in (-1, 0, 1)},
    **{f"byte-cap-{delta:+d}": (8388608, 524288 + delta, 4, 8388608, "random") for delta in (-2, 0, 2)},
    "oversized": (16777216, 1048576, 4, 16777216, "random"),
    # At concurrency 1 every group flushes alone. Chunks span half to twice the
    # average: all within the inclusive 4 KiB lone-chunk inline bound at 2 KiB,
    # straddling it at 4 KiB, and all above it at 8 KiB+4 (minimum 4 KiB+2).
    **{f"inline-{average}": (4194304, average, 1, 8388608, "random") for average in (2048, 4096, 8196)},
}
SMOKE = ("many-3", "many-4", "many-5", "one-permit", "small--1", "default-4", "production-default")


def validate(row, case, backend):
    size, avg, concurrency, budget, content = case
    expected = dict(file_bytes=size, average_chunk_bytes=avg, upload_concurrency=concurrency,
                    memory_budget_bytes=budget, content=content, backend=backend)
    if any(row.get(key) != value for key, value in expected.items()):
        raise common.BenchmarkError("incorrect hash probe configuration")
    if (row.get("correctness") != CORRECTNESS or not row.get("root") or not row.get("manifest_hash")
        or type(row.get("wall_nanos")) is not int or row["wall_nanos"] <= 0
        or type(row.get("chunks")) is not int or row["chunks"] < 1
        or type(row.get("chunk_puts")) is not int or row["chunk_puts"] < 0):
        raise common.BenchmarkError("missing hash probe correctness audit")
    if row.get("phase") == "duplicate":
        if row["chunk_puts"] != 0:
            raise common.BenchmarkError("duplicate wrote chunks")
    elif row.get("phase") != "cold" or row["chunk_puts"] < 1:
        raise common.BenchmarkError("missing cold chunk writes")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--cases", help="comma-separated permanent case names")
    parser.add_argument("--backend", choices=("memory", "local", "both"), default="both")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--baseline-binary", type=pathlib.Path)
    parser.add_argument("--cpu-affinity", type=cpu_list)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    cases = args.cases.split(",") if args.cases else list(SMOKE if args.profile == "smoke" else CASES)
    if not cases or any(name not in CASES for name in cases) or len(set(cases)) != len(cases):
        parser.error("unknown or duplicate case")
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a probe binary with --no-build are required")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--no-default-features", "--features", "native,experimental", "--test", "chunk_hash_batch", "--no-run", "--message-format=json"], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        paths = [item["executable"] for line in built.stdout.splitlines() if line.startswith("{")
                 and (item := json.loads(line)).get("reason") == "compiler-artifact"
                 and item.get("target", {}).get("name") == "chunk_hash_batch" and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one chunk hash probe")
        binary = pathlib.Path(paths[0])
    variants = [("candidate", binary.resolve())]
    if args.baseline_binary:
        variants.insert(0, ("baseline", args.baseline_binary.resolve()))
    artifacts = []
    for variant, path in variants:
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        artifact = dict(variant=variant, path=str(path), sha256=digest)
        manifest = pathlib.Path(str(path) + ".build.json")
        if manifest.exists():
            build = json.loads(manifest.read_text())
            if build.get("executable_sha256") != digest or not build.get("lockfile_sha256"):
                raise common.BenchmarkError("invalid build fingerprint")
            artifact["build"] = build
        artifacts.append(artifact)
    if len(artifacts) == 2:
        if artifacts[0]["sha256"] == artifacts[1]["sha256"]:
            raise common.BenchmarkError("paired executables must differ")
        if all("build" in artifact for artifact in artifacts):
            for field in ("fixture_sha256", "lockfile_sha256", "features", "default_features", "rustc_version", "rustflags"):
                if artifacts[0]["build"].get(field) != artifacts[1]["build"].get(field):
                    raise common.BenchmarkError(f"paired builds differ in {field}")
    backends = ["memory", "local"] if args.backend == "both" else [args.backend]
    result = dict(schema_version=1, result_schema="casita.chunk-hash-batch.v1", suite_id="native-git", complete=False,
        artifacts=artifacts, configuration=dict(cases={name: CASES[name] for name in cases}, backends=backends,
        repetitions=args.repetitions, cpu_affinity=args.cpu_affinity,
        timing="put_slice only; deterministic whole-payload fixture, independent hashing and full verified audit excluded; duplicate follows cold audit"),
        samples=[], processes=[], paired_summary=[])
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-chunk-hash-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        try:
            for name in cases:
                size, avg, concurrency, budget, content = CASES[name]
                for backend in backends:
                    reductions = {phase: [] for phase in ("cold", "duplicate")}
                    for repetition in range(args.repetitions):
                        pair = {}
                        for variant, path in (variants if repetition % 2 == 0 else list(reversed(variants))):
                            env = {**os.environ, "CASITA_HASH_BYTES": str(size), "CASITA_HASH_AVERAGE": str(avg),
                                "CASITA_HASH_CONCURRENCY": str(concurrency), "CASITA_HASH_BUDGET": str(budget),
                                "CASITA_HASH_BACKEND": backend, "CASITA_HASH_CONTENT": content}
                            timing = common.measured_command(common.CommandSpec([[str(path), PROBE, "--exact", "--ignored", "--nocapture"]], work, env), work/"stdout", work/"stderr", check=False)
                            stdout, stderr = (work/"stdout").read_text(), (work/"stderr").read_text()
                            result["processes"].append(dict(**timing, variant=variant, repetition=repetition, case=name, backend=backend, stdout=stdout, stderr=stderr))
                            rows = [json.loads(line.removeprefix("chunk_hash_sample ")) for raw in stdout.splitlines()
                                if (line := raw.removeprefix(f"test {PROBE} ... ")).startswith("chunk_hash_sample ")]
                            if timing["exit_code"] != 0 or len(rows) != 2 or "test result: ok. 1 passed; 0 failed;" not in stdout:
                                raise common.BenchmarkError(f"chunk hash probe failed: {stdout}\n{stderr}")
                            if [row.get("phase") for row in rows] != ["cold", "duplicate"]:
                                raise common.BenchmarkError("missing cold/duplicate phases")
                            for row in rows:
                                validate(row, CASES[name], backend)
                                result["samples"].append(dict(status="ok", implementation="casita", variant=variant, repetition=repetition,
                                    case=name, wall_seconds=row["wall_nanos"] / 1e9, **row))
                            if any(rows[0][key] != rows[1][key] for key in ("root", "manifest_hash", "chunks")):
                                raise common.BenchmarkError("duplicate identity changed")
                            pair[variant] = {row["phase"]: row for row in rows}
                            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
                        if "baseline" in pair:
                            for phase in reductions:
                                before, after = (pair[v][phase] for v in ("baseline", "candidate"))
                                if any(before[field] != after[field] for field in ("root", "manifest_hash", "chunks")):
                                    raise common.BenchmarkError("paired chunk identity changed")
                                reductions[phase].append(100 * (1 - after["wall_nanos"] / before["wall_nanos"]))
                    for phase, values in reductions.items():
                        if values:
                            result["paired_summary"].append(dict(case=name, backend=backend, phase=phase, pairs=len(values), enough_samples=len(values)>=5,
                                positive_pairs=sum(value > 0 for value in values), median_paired_reduction_percent=statistics.median(values),
                                minimum_paired_reduction_percent=min(values), maximum_paired_reduction_percent=max(values)))
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
