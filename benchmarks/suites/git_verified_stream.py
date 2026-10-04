"""Compare one-pass verification with write-then-reread ingestion."""
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

PROBE = "benchmark_git_verified_stream"
CORRECTNESS = "exact identity, length, closure and byte-for-byte readback"


def sizes(value):
    try:
        result = [int(part) for part in value.split(",")]
        if not result or min(result) < 0:
            raise ValueError()
        return result
    except ValueError:
        raise argparse.ArgumentTypeError("expected comma-separated nonnegative byte counts")


# Both sides of 64 KiB, and of 4 MiB where the profile affords it.
PROFILE_BYTES = {"smoke": [0, 1, 65535, 65536, 65537],
                 "standard": [0, 1, 65535, 65536, 65537, 4194303, 4194304, 4194305]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--file-bytes", type=sizes)
    parser.add_argument("--backend", choices=("memory", "local", "both"), default="both")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--cpu-affinity", type=cpu_list)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a probe binary with --no-build are required")
    binary = args.probe_binary
    if binary is None:
        command = ["cargo", "test", "--release", "-p", "casita", "--no-default-features",
                   "--features", "native,git,experimental", "--test", "verified_stream",
                   "--no-run", "--message-format=json"]
        built = subprocess.run(command, cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        rows = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
        paths = [r["executable"] for r in rows if r.get("target", {}).get("name") == "verified_stream"
                 and r.get("executable")]
        if len(paths) != 1:
            raise common.BenchmarkError("expected one verified-stream probe executable")
        binary = pathlib.Path(paths[0])
    binary = binary.resolve()
    with binary.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    byte_counts = args.file_bytes or PROFILE_BYTES[args.profile]
    backends = ["memory", "local"] if args.backend == "both" else [args.backend]
    result = dict(schema_version=1, result_schema="casita.git-verified-stream.v1", suite_id="native-git",
        complete=False, artifact=dict(path=str(binary), sha256=digest), samples=[], processes=[],
        configuration=dict(profile=args.profile, file_bytes=byte_counts, backends=backends, repetitions=args.repetitions,
            cpu_affinity=args.cpu_affinity, timing="staging only; source fixture, publication and audits excluded",
            comparison="same binary; write then reread versus verify while writing",
            memory_measurement="process RSS includes fixture creation and correctness audits"))
    with cpu_affinity(args.cpu_affinity), tempfile.TemporaryDirectory(prefix="casita-git-stream-") as temporary:
        work = pathlib.Path(temporary)
        result["environment"] = common.environment_metadata(work)
        if hasattr(os, "sched_getaffinity"):
            result["environment"]["cpu_affinity"] = sorted(os.sched_getaffinity(0))
        try:
            summaries = []
            for backend, size in itertools.product(backends, byte_counts):
                reductions, before, after = [], [], []
                for repetition in range(args.repetitions):
                    pair = {}
                    strategies = ["reread", "stream"] if repetition % 2 == 0 else ["stream", "reread"]
                    for strategy in strategies:
                        env = {**os.environ, "CASITA_GIT_STREAM_BACKEND": backend,
                               "CASITA_GIT_STREAM_BYTES": str(size), "CASITA_GIT_STREAM_STRATEGY": strategy}
                        timing = common.measured_command(common.CommandSpec(
                            [[str(binary), PROBE, "--exact", "--ignored", "--nocapture"]], work, env),
                            work / "stdout", work / "stderr", check=False)
                        stdout, stderr = (work / "stdout").read_text(), (work / "stderr").read_text()
                        result["processes"].append(dict(**timing, strategy=strategy, backend=backend,
                            file_bytes=size, repetition=repetition, stdout=stdout, stderr=stderr))
                        lines = (line.removeprefix(f"test {PROBE} ... ") for line in stdout.splitlines())
                        rows = [json.loads(line.removeprefix("git_verified_stream_sample "))
                                for line in lines if line.startswith("git_verified_stream_sample ")]
                        if (timing["exit_code"] != 0 or len(rows) != 1
                                or "test result: ok. 1 passed; 0 failed;" not in stdout):
                            raise common.BenchmarkError(f"verified-stream probe failed: {stdout}\n{stderr}")
                        row = rows[0]
                        if (row.get("strategy") != strategy or row.get("backend") != backend
                                or row.get("file_bytes") != size or row.get("correctness") != CORRECTNESS
                                or not row.get("root") or not isinstance(row.get("wall_nanos"), int)
                                or row["wall_nanos"] <= 0):
                            raise common.BenchmarkError("wrong verified-stream configuration or correctness gate")
                        pair[strategy] = row
                        result["samples"].append(dict(status="ok", implementation="casita",
                            repetition=repetition, wall_seconds=row["wall_nanos"] / 1e9, **row))
                    if pair["stream"]["root"] != pair["reread"]["root"]:
                        raise common.BenchmarkError("strategies produced different file identities")
                    baseline, candidate = pair["reread"]["wall_nanos"], pair["stream"]["wall_nanos"]
                    before.append(baseline / 1e9)
                    after.append(candidate / 1e9)
                    reductions.append(100 * (1 - candidate / baseline))
                    common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
                summaries.append(dict(backend=backend, file_bytes=size, pairs=len(reductions),
                    enough_samples=len(reductions) >= 5, reread_median_seconds=statistics.median(before),
                    stream_median_seconds=statistics.median(after),
                    median_paired_reduction_percent=statistics.median(reductions),
                    minimum_paired_reduction_percent=min(reductions),
                    maximum_paired_reduction_percent=max(reductions)))
            result["paired_summary"] = summaries
            result["complete"] = True
        except Exception as error:
            result["error"] = str(error)
            raise
        finally:
            common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
