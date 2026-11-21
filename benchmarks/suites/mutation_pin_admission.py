"""Measure mutation admission with empty and populated catalog witnesses."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import platform
import subprocess

from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import CARGO_ARGUMENTS
from benchmarks.suites.pack.catalog import parse_probe_binary

PROBE = "repository::mutation_catalog_tests::benchmark_mutation_pin_admission"
CORRECTNESS = "current catalog protected and empty released inventory"


def catalog_bytes(value):
    try:
        sizes = [int(part) for part in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated byte counts") from error
    if not sizes or min(sizes) < 0 or len(set(sizes)) != len(sizes):
        raise argparse.ArgumentTypeError("byte counts must be nonnegative and unique")
    return sizes


def parse_sample(stdout, size, iterations):
    try:
        samples = [json.loads(line.removeprefix("mutation_pin_admission_sample "))
                   for line in stdout.splitlines()
                   if line.startswith("mutation_pin_admission_sample ")]
    except json.JSONDecodeError as error:
        raise common.BenchmarkError("invalid mutation pin admission sample") from error
    if len(samples) != 1 or "test result: ok. 1 passed; 0 failed;" not in stdout:
        raise common.BenchmarkError("expected one passing mutation pin admission probe")
    sample = samples[0]
    if (sample.get("catalog_bytes") != size or sample.get("iterations") != iterations
            or sample.get("correctness") != CORRECTNESS
            or type(sample.get("nanos")) is not int or sample["nanos"] <= 0
            or type(sample.get("journal_syncs")) is not int
            or type(sample.get("admission_operations")) is not int):
        raise common.BenchmarkError("missing admission metric or correctness gate")
    if platform.system() in ("Linux", "Darwin"):
        if sample["admission_operations"] != iterations:
            raise common.BenchmarkError("admission added a separate pin protection write")
        if not iterations <= sample["journal_syncs"] <= 2 * iterations:
            raise common.BenchmarkError("unexpected durable sync count")
        if size <= 56 and sample["journal_syncs"] != iterations:
            raise common.BenchmarkError("small-catalog admission used more than one sync")
    return sample


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--catalog-bytes", type=catalog_bytes)
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args(argv)
    sizes = args.catalog_bytes or ([0, 56] if args.profile == "smoke" else [0, 56, 512 * 1024, 1024 * 1024])
    iterations = args.iterations if args.iterations is not None else (4 if args.profile == "smoke" else 32)
    if min(iterations, args.repetitions) < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive iterations and repetitions and a binary for --no-build are required")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT,
                               capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    with binary.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    result = dict(schema_version=1, suite_id="state-and-publication", complete=False,
                  environment=common.environment_metadata(cli.ROOT),
                  configuration=dict(profile=args.profile, catalog_bytes=sizes,
                                     iterations=iterations, repetitions=args.repetitions),
                  artifacts=[dict(path=str(binary), sha256=digest)], samples=[], processes=[])

    def save():
        common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
        if args.report:
            lines = ["# Mutation pin admission", "", f"Complete: {result['complete']}", "",
                     "Admission time excludes ledger warmup, release, and inventory verification. Each session protects the current catalog and releases its pin.",
                     "0 and 56 bytes cover empty and populated admission. The standard profile adds 512 KiB and 1 MiB witnesses around the journal checkpoint window.",
                     "", "| Catalog bytes | Repetition | Admissions | Mean ms | Admission operations | Journal syncs |",
                     "|---:|---:|---:|---:|---:|---:|"]
            for sample in result["samples"]:
                lines.append(f"| {sample['entries']} | {sample['repetition']} | {iterations} | {sample['wall_seconds'] * 1000 / iterations:.3f} | {sample['admission_operations']} | {sample['journal_syncs']} |")
            if "error" in result:
                lines += ["", result["error"]]
            common.write_atomic(args.report, "\n".join(lines) + "\n")

    save()
    try:
        for size in sizes:
            for repetition in range(1, args.repetitions + 1):
                process = subprocess.run(
                    [str(binary), PROBE, "--exact", "--ignored", "--nocapture"],
                    env={**os.environ, "CASITA_BENCH_PIN_CATALOG_BYTES": str(size),
                         "CASITA_BENCH_PIN_ITERATIONS": str(iterations)},
                    capture_output=True, text=True)
                result["processes"].append(dict(catalog_bytes=size, repetition=repetition,
                                                exit_code=process.returncode,
                                                stdout=process.stdout, stderr=process.stderr))
                if process.returncode:
                    raise common.BenchmarkError("mutation pin admission probe failed")
                sample = parse_sample(process.stdout, size, iterations)
                result["samples"].append(dict(status="ok", operation="mutation-pin-admission",
                                              entries=size, repetition=repetition,
                                              wall_seconds=sample["nanos"] / 1e9,
                                              journal_syncs=sample["journal_syncs"],
                                              admission_operations=sample["admission_operations"],
                                              correctness=CORRECTNESS))
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
