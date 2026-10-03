"""Measure chunk completion scheduling with deterministic upload stragglers."""
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
from benchmarks.suites import repository as common
from benchmarks.affinity import cpu_affinity, cpu_list
from benchmarks.suites.metadata_collection import positive_csv
from benchmarks.suites.git_ingest_scheduling import delays_csv

PROBE = "benchmark_chunk_upload_completion"
CORRECTNESS = "reference FastCDC chunks and verified full readback; all uploads completed"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--file-bytes", type=positive_csv)
    parser.add_argument("--budgets", type=positive_csv)
    parser.add_argument("--delays-ms", type=delays_csv)
    parser.add_argument("--concurrency", type=positive_csv)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--baseline-binary", type=pathlib.Path)
    parser.add_argument("--cpu-affinity", type=cpu_list)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a probe binary with --no-build are required")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--no-default-features", "--features", "native,experimental", "--test", "chunk_upload_completion", "--no-run", "--message-format=json"], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        paths = [item["executable"] for line in built.stdout.splitlines() if line.startswith("{")
                 and (item := json.loads(line)).get("reason") == "compiler-artifact"
                 and item.get("target", {}).get("name") == "chunk_upload_completion" and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one chunk completion probe")
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
        if all("build" in a for a in artifacts):
            for field in ("fixture_sha256", "lockfile_sha256", "features", "default_features", "rustc_version", "rustflags"):
                if artifacts[0]["build"].get(field) != artifacts[1]["build"].get(field):
                    raise common.BenchmarkError(f"paired builds differ in {field}")
    sizes = args.file_bytes or ([65536] if args.profile == "smoke" else [511, 512, 513, 2047, 2048, 2049, 65536, 1048576])
    budgets = args.budgets or ([196607, 196608, 196609] if args.profile == "smoke" else [65535, 65536, 65537, 196607, 196608, 196609, 262143, 262144, 262145, 1048576, 4194304])
    # The reorder window is max(64, 16 * concurrency): 2 is below its 64-entry
    # floor, 4 meets it, and 32, which the 64 KiB units of a 4 MiB budget
    # fully admit, scales past it.
    concurrencies = args.concurrency or ([4] if args.profile == "smoke" else [2, 4, 32])
    # An 8 ms straggler stays within the reorder window of 32 uploads; a
    # 50 ms straggler fills the window the writer buffers behind it.
    delays = args.delays_ms or ([0, 8] if args.profile == "smoke" else [0, 8, 50])
    result = dict(schema_version=1, result_schema="casita.chunk-upload-completion.v1", suite_id="native-git", complete=False, artifacts=artifacts,
        configuration=dict(file_bytes=sizes, budgets=budgets, delays_ms=delays, repetitions=args.repetitions,
            cpu_affinity=args.cpu_affinity, upload_concurrency=concurrencies, average_chunk_bytes=1024,
            timing="payload write only; deterministic source generation and exhaustive chunk/readback audits excluded",
            memory_measurement="whole-process RSS includes fixture and audit buffers"), samples=[], processes=[], paired_summary=[])
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-chunk-completion-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        try:
            for size, budget, delay, concurrency in itertools.product(sizes, budgets, delays, concurrencies):
                reductions = []
                for repetition in range(args.repetitions):
                    pair = {}
                    for variant, path in (variants if repetition % 2 == 0 else list(reversed(variants))):
                        env = {**os.environ, "CASITA_CHUNK_COMPLETION_BYTES": str(size), "CASITA_CHUNK_COMPLETION_BUDGET": str(budget), "CASITA_CHUNK_COMPLETION_DELAY_MS": str(delay), "CASITA_CHUNK_COMPLETION_CONCURRENCY": str(concurrency)}
                        timing = common.measured_command(common.CommandSpec([[str(path), PROBE, "--exact", "--ignored", "--nocapture"]], work, env), work/"stdout", work/"stderr", check=False)
                        stdout, stderr = (work/"stdout").read_text(), (work/"stderr").read_text()
                        result["processes"].append(dict(**timing, variant=variant, repetition=repetition, file_bytes=size, budget=budget, delay_ms=delay, concurrency=concurrency, stdout=stdout, stderr=stderr))
                        rows = [json.loads(line.removeprefix("chunk_upload_completion_sample ")) for raw in stdout.splitlines()
                                if (line := raw.removeprefix(f"test {PROBE} ... ")).startswith("chunk_upload_completion_sample ")]
                        if (timing["exit_code"] != 0 or len(rows) != 1 or "test result: ok. 1 passed; 0 failed;" not in stdout):
                            raise common.BenchmarkError(f"chunk completion probe failed: {stdout}\n{stderr}")
                        row = rows[0]
                        if (row.get("file_bytes") != size or row.get("budget") != budget or row.get("delay_ms") != delay or row.get("concurrency") != concurrency
                            or row.get("correctness") != CORRECTNESS or not row.get("root") or type(row.get("wall_nanos")) is not int or row["wall_nanos"] <= 0):
                            raise common.BenchmarkError("incorrect chunk completion configuration or missing audit")
                        pair[variant] = row
                        result["samples"].append(dict(status="ok", implementation="casita", variant=variant, repetition=repetition,
                            wall_seconds=row["wall_nanos"] / 1e9, **row))
                        common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
                    if "baseline" in pair:
                        if pair["baseline"]["root"] != pair["candidate"]["root"]:
                            raise common.BenchmarkError("completion order changed identity")
                        reductions.append(100 * (1 - pair["candidate"]["wall_nanos"] / pair["baseline"]["wall_nanos"]))
                if reductions:
                    result["paired_summary"].append(dict(file_bytes=size, budget=budget, delay_ms=delay, concurrency=concurrency, pairs=len(reductions), enough_samples=len(reductions)>=5,
                        median_paired_reduction_percent=statistics.median(reductions), minimum_paired_reduction_percent=min(reductions), maximum_paired_reduction_percent=max(reductions)))
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
