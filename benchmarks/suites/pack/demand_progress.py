"""Demand progress under blocked speculative I/O, not storage throughput."""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess

from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import CARGO_ARGUMENTS
from benchmarks.suites.pack.catalog import parse_probe_binary

PROBE = "blob::pack::fetch::tests::speculative_requests_leave_room_for_demand"
CORRECTNESS = "exact bytes, verified digest, demand progress, permits released"


def parse_samples(stdout):
    samples = [json.loads(line.removeprefix("demand_progress_sample "))
               for line in stdout.splitlines() if line.startswith("demand_progress_sample ")]
    if (sorted(row.get("speculative_requests", -1) for row in samples) != [2, 3, 4, 8]
            or "test result: ok. 1 passed; 0 failed;" not in stdout):
        raise common.BenchmarkError("incomplete demand-progress matrix")
    for row in samples:
        if (row.get("correctness") != CORRECTNESS
                or type(row.get("demand_nanos")) is not int or row["demand_nanos"] < 0):
            raise common.BenchmarkError("unverified demand-progress sample")
    return samples


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="smoke")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path,
                        default=cli.ROOT / "benchmarks/results/pack-demand-progress.json")
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a binary for --no-build are required")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT,
                               capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    digest = hashlib.sha256()
    with binary.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    result = dict(schema_version=1, result_schema="casita.pack-demand-progress.v1",
                  suite_id="blob-backends", complete=False,
                  environment=common.environment_metadata(cli.ROOT),
                  configuration=dict(profile=args.profile, repetitions=args.repetitions,
                                     origin="throttled in-memory", speculative_requests=[2, 3, 4, 8]),
                  artifacts=[dict(path=str(binary), sha256=digest.hexdigest())],
                  samples=[], processes=[])

    def save():
        common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")

    save()
    try:
        for repetition in range(1, args.repetitions + 1):
            process = subprocess.run([str(binary), PROBE, "--exact", "--nocapture"],
                                     capture_output=True, text=True, timeout=60)
            result["processes"].append(dict(repetition=repetition, exit_code=process.returncode,
                                           stdout=process.stdout, stderr=process.stderr))
            if process.returncode:
                raise common.BenchmarkError("demand-progress correctness gate failed")
            result["samples"].extend(dict(row, repetition=repetition, status="ok")
                                     for row in parse_samples(process.stdout))
            save()
        result["complete"] = True
    except Exception as error:
        result["error"] = str(error)
        raise
    finally:
        save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
