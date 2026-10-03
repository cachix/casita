"""Measure streaming chunk-manifest construction with bounded fixture buffers."""
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

PROBE = "benchmark_chunk_manifest_stream"
CORRECTNESS = "independent streaming BLAKE3 and exhaustive verified readback"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--file-bytes", type=positive_csv)
    parser.add_argument("--backend", choices=("memory", "local", "both"), default="both")
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
        built = subprocess.run(["cargo", "test", "--release", "-p", "casita", "--no-default-features", "--features", "native,experimental", "--test", "chunk_manifest_stream", "--no-run", "--message-format=json"], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        paths = [item["executable"] for line in built.stdout.splitlines() if line.startswith("{")
                 and (item := json.loads(line)).get("reason") == "compiler-artifact"
                 and item.get("target", {}).get("name") == "chunk_manifest_stream" and item.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one chunk manifest probe")
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
    sizes = args.file_bytes or ([65536, 1048576] if args.profile == "smoke" else [65536, 16777216, 67108864, 268435456])
    if min(sizes) < 65536:
        parser.error("streaming fixture sizes must be at least 65536 bytes")
    backends = ["memory", "local"] if args.backend == "both" else [args.backend]
    result = dict(schema_version=1, result_schema="casita.chunk-manifest-stream.v1", suite_id="native-git", complete=False, artifacts=artifacts,
        configuration=dict(file_bytes=sizes, backends=backends, repetitions=args.repetitions, cpu_affinity=args.cpu_affinity,
            upload_concurrency=4, average_chunk_bytes=1024, fixture_buffer_bytes=65536,
            timing="payload stream write and close only; independent prehash and verified readback excluded",
            memory_measurement="Linux process high-water RSS captured immediately after close, before bounded-buffer readback; includes runtime and backend"),
        samples=[], processes=[], paired_summary=[])
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-chunk-manifest-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        try:
            for size, backend in itertools.product(sizes, backends):
                reductions, memory_reductions = [], []
                for repetition in range(args.repetitions):
                    pair = {}
                    for variant, path in (variants if repetition % 2 == 0 else list(reversed(variants))):
                        env = {**os.environ, "CASITA_MANIFEST_BYTES": str(size), "CASITA_MANIFEST_BACKEND": backend}
                        timing = common.measured_command(common.CommandSpec([[str(path), PROBE, "--exact", "--ignored", "--nocapture"]], work, env), work/"stdout", work/"stderr", check=False)
                        stdout, stderr = (work/"stdout").read_text(), (work/"stderr").read_text()
                        result["processes"].append(dict(**timing, variant=variant, repetition=repetition, file_bytes=size, backend=backend, stdout=stdout, stderr=stderr))
                        rows = [json.loads(line.removeprefix("chunk_manifest_sample ")) for raw in stdout.splitlines()
                                if (line := raw.removeprefix(f"test {PROBE} ... ")).startswith("chunk_manifest_sample ")]
                        if timing["exit_code"] != 0 or len(rows) != 1 or "test result: ok. 1 passed; 0 failed;" not in stdout:
                            raise common.BenchmarkError(f"chunk manifest probe failed: {stdout}\n{stderr}")
                        row = rows[0]
                        if (row.get("file_bytes") != size or row.get("backend") != backend or row.get("correctness") != CORRECTNESS
                            or not row.get("root") or not row.get("manifest_hash") or type(row.get("wall_nanos")) is not int or row["wall_nanos"] <= 0):
                            raise common.BenchmarkError("incorrect streaming configuration or missing audit")
                        for field in ("before_rss_bytes", "write_peak_rss_bytes"):
                            if row.get(field) is not None and (type(row[field]) is not int or row[field] <= 0):
                                raise common.BenchmarkError("invalid memory observation")
                        pair[variant] = row
                        result["samples"].append(dict(status="ok", implementation="casita", variant=variant, repetition=repetition,
                            wall_seconds=row["wall_nanos"] / 1e9, **row))
                        common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
                    if "baseline" in pair:
                        if any(pair["baseline"][field] != pair["candidate"][field] for field in ("root", "manifest_hash")):
                            raise common.BenchmarkError("streaming manifest changed identity")
                        reductions.append(100 * (1 - pair["candidate"]["wall_nanos"] / pair["baseline"]["wall_nanos"]))
                        before, after = (pair[v].get("write_peak_rss_bytes") for v in ("baseline", "candidate"))
                        if before is not None and after is not None:
                            memory_reductions.append(before - after)
                if reductions:
                    result["paired_summary"].append(dict(file_bytes=size, backend=backend, pairs=len(reductions), enough_samples=len(reductions)>=5,
                        median_paired_reduction_percent=statistics.median(reductions), minimum_paired_reduction_percent=min(reductions), maximum_paired_reduction_percent=max(reductions),
                        median_paired_write_peak_rss_reduction_bytes=statistics.median(memory_reductions) if memory_reductions else None,
                        minimum_paired_write_peak_rss_reduction_bytes=min(memory_reductions) if memory_reductions else None,
                        maximum_paired_write_peak_rss_reduction_bytes=max(memory_reductions) if memory_reductions else None))
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
