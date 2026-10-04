"""Production packed reads over an in-memory origin with calibrated GET delay."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import pathlib
import statistics
import subprocess
import time

from benchmarks import cli
from benchmarks.suites import repository as common

CARGO_ARGUMENTS = ("build", "--release", "-p", "casita", "--no-default-features",
                   "--features", "native,experimental", "--example", "packed_read_latency",
                   "--message-format=json")


def integers(value):
    numbers = [int(item) for item in value.split(",")]
    if not numbers or min(numbers) < 0 or len(set(numbers)) != len(numbers):
        raise argparse.ArgumentTypeError("expected distinct nonnegative integers")
    return numbers


def parse_samples(stdout, size, delay, cache, pack):
    rows = [json.loads(line.removeprefix("latency_sample ")) for line in stdout.splitlines()
            if line.startswith("latency_sample ")]
    if [row.get("phase") for row in rows] != ["cold", "warm"]:
        raise common.BenchmarkError("missing cold/warm verified read")
    for row in rows:
        if (row.get("file_bytes") != size or row.get("get_delay_ms") != delay
                or row.get("cache_bytes") != cache or row.get("pack_bytes") != pack
                or row.get("correctness") != "exact bytes and independent BLAKE3"
                or type(row.get("nanos")) is not int or row["nanos"] <= 0
                or len(row.get("digest", "")) != 64
                or row.get("calibration_nanos", -1) < delay * 900000):
            raise common.BenchmarkError("wrong configuration or unverified read")
        for key in ("pack_requests", "pack_read_bytes", "cache_hits", "readahead_deferrals", "buffer_bypasses"):
            if type(row.get(key)) is not int or row[key] < 0:
                raise common.BenchmarkError("invalid I/O counters")
        if row["phase"] == "cold" and not row["pack_requests"]:
            raise common.BenchmarkError("cold read performed no pack I/O")
        if row["phase"] == "warm" and cache >= size * 2 and row["pack_requests"]:
            raise common.BenchmarkError("fitting warm cache performed pack I/O")
    if rows[0]["digest"] != rows[1]["digest"]:
        raise common.BenchmarkError("cold/warm data differs")
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="smoke")
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--baseline-probe-binary", type=pathlib.Path)
    parser.add_argument("--original-probe-binary", type=pathlib.Path)
    parser.add_argument("--variant-binary", action="append", default=[], metavar="LABEL=PATH")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--sizes-mib", type=integers)
    parser.add_argument("--delay-ms", type=integers, default=[0, 20, 80])
    parser.add_argument("--cache-mib", type=integers, default=[0, 192])
    parser.add_argument("--pack-mib", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--output", type=pathlib.Path, default=cli.ROOT / "benchmarks/results/pack-demand-latency.json")
    args = parser.parse_args(argv)
    sizes = args.sizes_mib or ([1, 17, 65] if args.profile == "smoke" else [15, 17, 63, 65])
    if min(sizes) <= 0 or args.pack_mib <= 0 or args.repetitions <= 0 or (args.no_build and not args.probe_binary):
        parser.error("positive sizes, pack size, repetitions, and a binary for --no-build required")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr)
        for line in built.stdout.splitlines():
            try:
                artifact = json.loads(line)
            except json.JSONDecodeError:
                continue
            if artifact.get("target", {}).get("name") == "packed_read_latency" and artifact.get("executable"):
                binary = pathlib.Path(artifact["executable"])
        if binary is None:
            raise common.BenchmarkError("missing release example")
    binaries = {"candidate": binary.resolve()}
    if args.baseline_probe_binary:
        binaries["serial"] = args.baseline_probe_binary.resolve()
    if args.original_probe_binary:
        binaries["original"] = args.original_probe_binary.resolve()
    for value in args.variant_binary:
        try:
            label, path = value.split("=", 1)
        except ValueError:
            parser.error("variant binary must be LABEL=PATH")
        if not label or label in binaries:
            parser.error("variant labels must be unique and nonempty")
        binaries[label] = pathlib.Path(path).resolve()
    if len(binaries) > 3:
        parser.error("at most three binaries are supported for balanced process order")
    artifacts = []
    for label, path in binaries.items():
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        artifacts.append(dict(label=label, path=str(path), sha256=digest))
    result = dict(schema_version=1, result_schema="casita.pack-demand-latency.v1",
                  suite_id="blob-backends", complete=False, finished=False,
                  environment=common.environment_metadata(cli.ROOT), artifacts=artifacts,
                  configuration=dict(sizes_mib=sizes, get_delay_ms=args.delay_ms, cache_mib=args.cache_mib,
                                     pack_mib=args.pack_mib, repetitions=args.repetitions,
                                     origin="in-memory, delay per GET, unlimited bandwidth",
                                     fixture="BLAKE3 XOF packed-demand-latency-v1",
                                     exclusions=["TCP", "shared bandwidth", "disk", "FUSE"]),
                  samples=[], processes=[], summary=[])
    cases = list(itertools.product(sizes, args.delay_ms, args.cache_mib, range(1, args.repetitions+1)))
    result["expected_processes"] = len(cases)*len(binaries)
    result["expected_samples"] = result["expected_processes"]*2
    digests = {}

    def save():
        common.write_atomic(args.output, json.dumps(result, indent=2)+"\n")

    save()
    try:
        for index, (size, delay, cache, repetition) in enumerate(cases):
            labels = list(itertools.permutations(binaries))[index % 6] if len(binaries) == 3 else list(binaries)
            if len(binaries) == 2 and index % 2:
                labels.reverse()
            print(f"case {index+1}/{len(cases)} size={size}MiB delay={delay}ms cache={cache}MiB rep={repetition}", flush=True)
            for label in labels:
                command = [str(binaries[label]), str(size*2**20), str(delay), str(cache*2**20), str(args.pack_mib*2**20)]
                started = time.monotonic()
                process = dict(label=label, size_mib=size, delay_ms=delay, cache_mib=cache, repetition=repetition,
                               command=command, status="failed")
                try:
                    completed = subprocess.run(command, capture_output=True, text=True, timeout=90)
                    process.update(exit_code=completed.returncode, stdout=completed.stdout, stderr=completed.stderr)
                    if completed.returncode:
                        raise common.BenchmarkError("read probe failed")
                    samples = parse_samples(completed.stdout, size*2**20, delay, cache*2**20, args.pack_mib*2**20)
                    for row in samples:
                        if digests.setdefault(size, row["digest"]) != row["digest"]:
                            raise common.BenchmarkError("variant reconstructed different fixture")
                    result["samples"].extend(dict(row, label=label, repetition=repetition) for row in samples)
                    process["status"] = "ok"
                except subprocess.TimeoutExpired as error:
                    process.update(status="timeout", stdout=(error.stdout or b"").decode(errors="replace"),
                                   stderr=(error.stderr or b"").decode(errors="replace"))
                except common.BenchmarkError as error:
                    process["error"] = str(error)
                process["seconds"] = time.monotonic()-started
                result["processes"].append(process)
                save()
        for label, size, delay, cache, phase in itertools.product(binaries, sizes, args.delay_ms, args.cache_mib, ["cold", "warm"]):
            rows = [row for row in result["samples"] if row["label"] == label and row["file_bytes"] == size*2**20
                    and row["get_delay_ms"] == delay and row["cache_bytes"] == cache*2**20 and row["phase"] == phase]
            result["summary"].append(dict(label=label, size_mib=size, delay_ms=delay, cache_mib=cache, phase=phase,
                                          passed=len(rows), expected=args.repetitions,
                                          median_ms=statistics.median(row["nanos"]/1e6 for row in rows) if rows else None))
        result["finished"] = True
        result["complete"] = len(result["samples"]) == result["expected_samples"] and all(p["status"] == "ok" for p in result["processes"])
    finally:
        save()
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
