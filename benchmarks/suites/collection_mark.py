"""Collection marking with shared edges and bounded traversal storage."""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import pathlib
import random
import statistics
import subprocess
import tempfile

from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import CARGO_ARGUMENTS, positive_csv
from benchmarks.suites.pack.catalog import parse_probe_binary

PROBE = "repository::collection_mark_tests::benchmark_collection_mark"
MODES = ("named", "pins", "snapshot-full", "snapshot-partial", "snapshot-sparse", "snapshot-forward")
CORRECTNESS = "exact marked keys, cardinality, spill boundary and reopened revision"


def parse_sample(stdout, parents, shape, memory_limit, iterations, strategy="current", mode="named"):
    # With one available CPU, libtest prints the test name before uncaptured
    # output on the same line. Accept only this probe's exact status prefix.
    lines = [line.removeprefix(f"test {PROBE} ... ") for line in stdout.splitlines()]
    try:
        cases = [json.loads(line.removeprefix("mark_sample "))
                 for line in lines if line.startswith("mark_sample ")]
    except json.JSONDecodeError as error:
        raise common.BenchmarkError("invalid mark JSON") from error
    if "test result: ok. 1 passed; 0 failed;" not in stdout or len(cases) != 1:
        raise common.BenchmarkError("mark probe must execute one passing test and emit one case")
    case = cases[0]
    required = dict(parents=parents, shape=shape, memory_limit=memory_limit,
                    objects=2 * parents if shape == "distinct" else parents + 1, edges=parents,
                    iterations=iterations, strategy=strategy, mode=mode, correctness=CORRECTNESS)
    if not isinstance(case, dict) or any(type(case.get(k)) is not type(v) or case[k] != v for k, v in required.items()):
        raise common.BenchmarkError("wrong mark configuration or correctness gate")
    cutoff = {"snapshot-full": parents, "snapshot-partial": max(1, parents // 2), "snapshot-sparse": 1}.get(mode, 0)
    scanned = parents if mode == "snapshot-forward" else ((2 * cutoff if shape == "distinct" else cutoff + 1) if cutoff else 0)
    samples = case.get("samples")
    if not isinstance(samples, list) or len(samples) != iterations + 1:
        raise common.BenchmarkError("missing or duplicate mark iterations")
    for sample, iteration in zip(samples, range(iterations + 1)):
        if (not isinstance(sample, dict) or type(sample.get("iteration")) is not int
                or sample["iteration"] != iteration or sample.get("mode") != mode
                or sample.get("warm") is not (iteration > 0)
                or any(type(sample.get(k)) is not int or sample[k] < 0
                       for k in ("nanos", "record_reads", "scanned_records", "spill_files", "spill_peak_bytes"))
                or sample["scanned_records"] != scanned
                or sample["record_reads"] + scanned < required["objects"]):
            raise common.BenchmarkError("invalid mark sample")
    return case


def save(args, result):
    common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    lines = ["# Collection marking", "",
             "Turso metadata; shared leaves, distinct leaves or a single-root chain. Exact marked sets are checked.",
             "Timing includes root/pin traversal, spill writes and queue cleanup; fixture setup, audit and returned mark-set cleanup are excluded.",
             "Each process measures one mode and strategy; pins never follows named marking. First/warm label iterations, not a cold OS cache.",
             "Legacy/current use the same executable and fixture. Matching strategies are adjacent, with reversed order on even repetitions.",
             "Snapshot modes cover all, half, or one parent generation, or old parents with newer leaves; closure-only pins call the same unchanged function under both strategy labels.",
             "Process RSS includes setup and audits. This is a traversal probe, not full collection or ingestion timing.",
             "", f"Complete: {result['complete']}", "",
             "| Parents | Shape | Memory keys | Strategy | Operation | Repetition | ms | Record reads | Scanned records |",
             "|---:|---|---:|---|---|---:|---:|---:|---:|"]
    for sample in result["samples"]:
        lines.append(f"| {sample['entries']} | {sample['shape']} | {sample['spill_memory_objects']} | {sample['variant']} | {sample['operation']} | {sample['repetition']} | {sample['wall_seconds'] * 1000:.3f} | {sample['metrics']['record_reads']:.0f} | {sample['metrics']['scanned_records']:.0f} |")
    if "error" in result:
        lines += ["", f"Error: {result['error']}"]
    common.write_atomic(args.report or args.output.with_suffix(".md"), "\n".join(lines) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--parents", type=positive_csv)
    parser.add_argument("--shape", choices=("shared", "distinct", "chain"), action="append", help="repeat to select shapes; default: all")
    parser.add_argument("--memory-limits", type=positive_csv, default=[256, 250000])
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--strategy", choices=("legacy", "current"), help="default: both, in adjacent alternating pairs")
    parser.add_argument("--mode", choices=MODES, action="append", help="repeat to select modes; default: all, in independent processes")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args(argv)
    boundaries = [127, 128, 255, 256, 257, 511, 512, 513]
    parents = args.parents or (boundaries if args.profile == "smoke" else [*boundaries, 8192])
    iterations = args.iterations if args.iterations is not None else (1 if args.profile == "smoke" else 3)
    shapes = args.shape or ["shared", "distinct", "chain"]
    if len(shapes) != len(set(shapes)):
        parser.error("shapes must not be repeated")
    strategies = [args.strategy] if args.strategy else ["legacy", "current"]
    modes = args.mode or list(MODES)
    if len(modes) != len(set(modes)):
        parser.error("modes must not be repeated")
    if iterations < 1 or args.repetitions < 1:
        parser.error("iterations and repetitions must be positive")
    if args.no_build and args.probe_binary is None:
        parser.error("--no-build requires --probe-binary")
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    with binary.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    with tempfile.TemporaryDirectory(prefix="casita-collection-mark-") as temporary:
        work = pathlib.Path(temporary)
        result = dict(schema_version=1, result_schema="casita.collection-mark.v1",
                      suite_id="collection-and-fsck", complete=False,
                      environment=common.environment_metadata(work),
                      configuration=dict(profile=args.profile, parents=parents, shapes=shapes,
                                         memory_limits=args.memory_limits, iterations=iterations,
                                         strategies=strategies, modes=modes,
                                         repetitions=args.repetitions, seed_batch_size=512),
                      artifacts=[dict(path=str(binary), sha256=digest)], samples=[], processes=[])
        save(args, result)
        cases = list(itertools.product(range(1, args.repetitions + 1), parents, shapes, args.memory_limits, modes))
        random.Random(0xCA517A).shuffle(cases)
        schedule = [(*case, strategy) for case in cases
                    for strategy in (strategies[::-1] if case[0] % 2 == 0 else strategies)]
        try:
            for repetition, count, shape, memory_limit, mode, strategy in schedule:
                print(f"collection-mark: parents={count}, shape={shape}, memory={memory_limit}, mode={mode}, strategy={strategy}, repetition={repetition}", flush=True)
                env = {**os.environ, "CASITA_MARK_PARENTS": str(count), "CASITA_MARK_SHAPE": shape,
                       "CASITA_MARK_MEMORY_LIMIT": str(memory_limit), "CASITA_MARK_ITERATIONS": str(iterations),
                       "CASITA_MARK_MODE": mode, "CASITA_MARK_STRATEGY": strategy}
                stdout, stderr = work / "stdout", work / "stderr"
                timing = common.measured_command(common.CommandSpec(
                    [[str(binary), PROBE, "--exact", "--ignored", "--nocapture"]], work, env), stdout, stderr, check=False)
                captured = stdout.read_text()
                result["processes"].append(dict(**timing, parents=count, shape=shape, memory_limit=memory_limit,
                                                mode=mode, strategy=strategy, repetition=repetition, stdout=captured, stderr=stderr.read_text()))
                if timing["exit_code"] != 0:
                    raise common.BenchmarkError(f"mark probe failed ({timing['exit_code']}): {stderr.read_text()}")
                case = parse_sample(captured, count, shape, memory_limit, iterations, strategy, mode)
                for phase in ("first", "warm"):
                    samples = [s for s in case["samples"] if s["warm"] == (phase == "warm")]
                    result["samples"].append(dict(status="ok", implementation="casita", operation=f"mark-{mode}-{phase}",
                        entries=count, shape=shape, spill_memory_objects=memory_limit,
                        variant=strategy,
                        objects=case["objects"], repetition=repetition, wall_seconds=statistics.mean(s["nanos"] for s in samples) / 1e9,
                        max_rss_bytes=timing["max_rss_bytes"], marks=samples, correctness=CORRECTNESS,
                        metrics={k: statistics.mean(s[k] for s in samples) for k in ("record_reads", "scanned_records", "spill_files", "spill_peak_bytes")}))
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
