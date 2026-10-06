"""Writer resource bounds and maintenance admission across eight-publication groups."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import subprocess

from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import CARGO_ARGUMENTS, positive_csv
from benchmarks.suites.pack.catalog import parse_probe_binary

PROBE = "repository::mutation_rotation_tests::benchmark_mutation_rotation"
CORRECTNESS = "exact readback after collection; bounded writer resources; exact admissions; released pins"
MODES = ("long", "sessions", "rotate")


def parse_sample(stdout, count, mode, eligible):
    # Single-threaded libtest may put this probe's status on the same line.
    lines = [line.removeprefix(f"test {PROBE} ... ") for line in stdout.splitlines()]
    try:
        samples = [json.loads(line.removeprefix("mutation_rotation_sample "))
                   for line in lines if line.startswith("mutation_rotation_sample ")]
    except json.JSONDecodeError as error:
        raise common.BenchmarkError("invalid rotation JSON") from error
    if len(samples) != 1 or "test result: ok. 1 passed; 0 failed;" not in stdout:
        raise common.BenchmarkError("expected one passing rotation probe")
    sample = samples[0]
    admissions = (count + 7) // 8 if mode == "sessions" else 1
    expected = dict(count=count, mode=mode, eligible=eligible, batch_size=64,
                    group_size=8, admissions=admissions,
                    collections=admissions if eligible else 0,
                    peak_writer_objects=64 * (count if mode == "long" else min(count, 8)),
                    correctness=CORRECTNESS)
    if (not isinstance(sample, dict)
            or any(type(sample.get(key)) is not type(value) or sample[key] != value
                   for key, value in expected.items())
            or type(sample.get("nanos")) is not int or sample["nanos"] <= 0):
        raise common.BenchmarkError("invalid rotation configuration or correctness gate")
    return sample


def profile_defaults(profile):
    """Smoke covers both sides of the first boundary once; standard also the second."""
    return ([7, 8, 9], 1) if profile == "smoke" else ([7, 8, 9, 16, 17], 4)


def fingerprint(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--counts", type=positive_csv)
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args(argv)
    counts, repetitions = profile_defaults(args.profile)
    args.counts = args.counts or counts
    args.repetitions = repetitions if args.repetitions is None else args.repetitions
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a binary for --no-build are required")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    digest = fingerprint(binary)
    result = dict(schema_version=1, suite_id="state-and-publication", complete=False,
                  environment=common.environment_metadata(cli.ROOT),
                  configuration=dict(counts=args.counts, repetitions=args.repetitions,
                                     batch_size=64, group_size=8, profile=args.profile,
                                     maintenance="injected ineligible/eligible, with real collection; no wall-clock cooldown"),
                  artifacts=[dict(path=str(binary), sha256=digest)], samples=[], processes=[])

    def save():
        common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
        if args.report:
            lines = ["# Mutation rotation", "", f"Complete: {result['complete']}", "",
                     "Memory-backed publication with long, independently admitted, and rotated writers.",
                     "The injected hook deterministically models maintenance eligibility; it does not measure the local 60-second cooldown.",
                     "Timing includes staging, publication, retention handoff, admission and release drain; inventory inspection and final GC/readback are excluded.",
                     "Counts straddle the first and second eight-publication boundaries. Per-writer resource counts are not aggregate memory or RSS.",
                     "", "| Publications | Mode | Eligible | Repetition | Seconds | Peak writer objects |",
                     "|---:|---|---|---:|---:|---:|"]
            for sample in result["samples"]:
                lines.append(f"| {sample['entries']} | {sample['variant']} | {sample['eligible']} | {sample['repetition']} | {sample['wall_seconds']:.6f} | {sample['peak_writer_objects']} |")
            if "error" in result:
                lines += ["", result["error"]]
            common.write_atomic(args.report, "\n".join(lines) + "\n")

    save()
    try:
        for repetition in range(1, args.repetitions + 1):
            modes = MODES if repetition % 2 else tuple(reversed(MODES))
            for count in args.counts:
                for eligible in (False, True):
                    for mode in modes:
                        process = subprocess.run(
                            [str(binary), PROBE, "--exact", "--ignored", "--nocapture"],
                            env={**os.environ, "CASITA_ROTATION_COUNT": str(count),
                                 "CASITA_ROTATION_MODE": mode, "CASITA_ROTATION_ELIGIBLE": str(int(eligible))},
                            capture_output=True, text=True)
                        result["processes"].append(dict(count=count, mode=mode, eligible=eligible,
                            repetition=repetition, exit_code=process.returncode,
                            stdout=process.stdout, stderr=process.stderr))
                        if process.returncode:
                            raise common.BenchmarkError("rotation probe failed")
                        sample = parse_sample(process.stdout, count, mode, eligible)
                        result["samples"].append(dict(status="ok", operation="mutation-publication",
                            entries=count, variant=f"{mode}/{'eligible' if eligible else 'ineligible'}",
                            mode=mode, eligible=eligible, repetition=repetition,
                            wall_seconds=sample["nanos"] / 1e9,
                            peak_writer_objects=sample["peak_writer_objects"],
                            admissions=sample["admissions"], collections=sample["collections"],
                            correctness=CORRECTNESS))
                        save()
        if fingerprint(binary) != digest:
            raise common.BenchmarkError("probe binary changed during measurement")
        result["complete"] = True
    except Exception as error:
        result["error"] = str(error)
        raise
    finally:
        save()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
