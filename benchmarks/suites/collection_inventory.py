"""Full logical and physical collection planning across spill boundaries."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import os
import pathlib
import random
import subprocess
import tempfile

from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import CARGO_ARGUMENTS, positive_csv
from benchmarks.suites.pack.catalog import parse_probe_binary

PROBE = "repository::collection_inventory_tests::benchmark_collection_inventory"
CORRECTNESS = "exact preview and sweep; all live reads; shared edges; manifest presence and elision; unchanged preview revision; spill cleanup"


def default_limits(count):
    return [17, count, count + 1, count + 2, count * 16 + 128]


def parse_sample(stdout, count, memory_limit):
    lines = [line.removeprefix(f"test {PROBE} ... ") for line in stdout.splitlines()]
    try:
        cases = [json.loads(line.removeprefix("collection_inventory_sample "))
                 for line in lines if line.startswith("collection_inventory_sample ")]
    except json.JSONDecodeError as error:
        raise common.BenchmarkError("invalid collection inventory JSON") from error
    if "test result: ok. 1 passed; 0 failed;" not in stdout or len(cases) != 1:
        raise common.BenchmarkError("inventory probe must execute one passing test and emit one case")
    case = cases[0]
    expected = dict(count=count, memory_limit=memory_limit, logical_removed=5,
                    payloads_removed=5, correctness=CORRECTNESS)
    if not isinstance(case, dict) or any(type(case.get(k)) is not type(v) or case[k] != v
                                         for k, v in expected.items()):
        raise common.BenchmarkError("wrong inventory configuration or correctness gate")
    if any(type(case.get(k)) is not int or case[k] < 0
           for k in ("spill_files", "spill_peak_bytes", "chunks_removed")):
        raise common.BenchmarkError("invalid inventory counters")
    seconds = case.get("seconds")
    if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
        raise common.BenchmarkError("invalid inventory duration")
    if (case["chunks_removed"] == 0
            or (memory_limit <= count and case["spill_files"] == 0)
            or (memory_limit >= count * 16 + 128 and case["spill_files"] != 0)):
        raise common.BenchmarkError("inventory spill or garbage gate failed")
    return case


def save(args, result):
    common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    lines = ["# Collection inventory", "",
             "Memory metadata and chunked payloads; one live directory, shared edges and five orphan objects.",
             "Timing includes collection lock acquisition, logical marking and full physical planning.",
             "Setup, plan disposal, a separate collecting pass and exhaustive retained-data audits are outside timing.",
             "Process RSS includes setup and audits. These are local measurements, not a controlled revision comparison.",
             "", f"Complete: {result['complete']}", "",
             "| Files | Memory keys | Repetition | ms | Spill files |",
             "|---:|---:|---:|---:|---:|"]
    for sample in result["samples"]:
        lines.append(f"| {sample['entries']} | {sample['spill_memory_objects']} | {sample['repetition']} | {sample['wall_seconds'] * 1000:.3f} | {sample['metrics']['spill_files']} |")
    if "error" in result:
        lines += ["", f"Error: {result['error']}"]
    common.write_atomic(args.report or args.output.with_suffix(".md"), "\n".join(lines) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--counts", type=positive_csv)
    parser.add_argument("--memory-limits", type=positive_csv)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.repetitions < 1:
        parser.error("repetitions must be positive")
    if args.no_build and args.probe_binary is None:
        parser.error("--no-build requires --probe-binary")
    counts = args.counts or ([255, 256, 257] if args.profile == "smoke" else [255, 256, 257, 8192])
    limits = {count: args.memory_limits or sorted(set(default_limits(count))) for count in counts}
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    with binary.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    schedule = [(repetition, count, limit)
                for repetition, count in itertools.product(range(1, args.repetitions + 1), counts)
                for limit in limits[count]]
    random.Random(0xCA517A).shuffle(schedule)
    with tempfile.TemporaryDirectory(prefix="casita-collection-inventory-") as temporary:
        work = pathlib.Path(temporary)
        result = dict(schema_version=1, result_schema="casita.collection-inventory.v1",
                      suite_id="collection-and-fsck", complete=False,
                      environment=common.environment_metadata(work),
                      configuration=dict(profile=args.profile, counts=counts,
                                         memory_limits=limits, repetitions=args.repetitions),
                      schedule=schedule, artifacts=[dict(path=str(binary), sha256=digest)],
                      samples=[], processes=[])
        save(args, result)
        try:
            for repetition, count, limit in schedule:
                print(f"collection-inventory: count={count}, memory={limit}, repetition={repetition}", flush=True)
                env = {**os.environ, "CASITA_COLLECTION_INVENTORY_COUNT": str(count),
                       "CASITA_COLLECTION_INVENTORY_MEMORY": str(limit)}
                stdout, stderr = work / "stdout", work / "stderr"
                timing = common.measured_command(common.CommandSpec(
                    [[str(binary), PROBE, "--exact", "--ignored", "--nocapture"]], work, env),
                    stdout, stderr, check=False)
                captured = stdout.read_text()
                result["processes"].append(dict(**timing, count=count, memory_limit=limit,
                                               repetition=repetition, stdout=captured, stderr=stderr.read_text()))
                if timing["exit_code"] != 0:
                    raise common.BenchmarkError(f"inventory probe failed ({timing['exit_code']}): {stderr.read_text()}")
                case = parse_sample(captured, count, limit)
                result["samples"].append(dict(status="ok", implementation="casita", operation="collection-plan",
                    entries=count, spill_memory_objects=limit, repetition=repetition,
                    wall_seconds=case["seconds"], max_rss_bytes=timing["max_rss_bytes"], correctness=CORRECTNESS,
                    metrics={k: case[k] for k in ("spill_files", "spill_peak_bytes", "chunks_removed")}, case=case))
                save(args, result)
        except Exception as error:
            result["error"] = str(error)
            save(args, result)
            raise
        result["complete"] = True
        save(args, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
