"""Casita blob-output staging with per-output sessions, shared sessions and publication batching."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import random
import subprocess
import tempfile

from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import positive_csv
from benchmarks.suites.pack.catalog import parse_probe_binary


PROBE = "scale_benchmarks::benchmark_output_import"
CORRECTNESS = "exact roots, byte-for-byte payload reads, clean fsck"
MODES = ("per-output", "shared-session", "batch-api", "atomic-api", "batched")
CARGO_ARGUMENTS = ("test", "--release", "--features", "cli", "--lib", "--no-run", "--message-format=json")


def parse_sample(stdout: str, count: int, size: int, mode: str) -> dict:
    prefix = "output_import_sample "
    try:
        samples = [json.loads(line.removeprefix(prefix)) for line in stdout.splitlines() if line.startswith(prefix)]
    except json.JSONDecodeError as error:
        raise common.BenchmarkError("invalid output-import JSON") from error
    if "test result: ok. 1 passed; 0 failed;" not in stdout or len(samples) != 1:
        raise common.BenchmarkError("output-import probe must execute one passing test and emit one sample")
    sample = samples[0]
    if not isinstance(sample, dict) or sample.get("operation") != "output-import":
        raise common.BenchmarkError("output-import sample has the wrong operation")
    if sample.get("mode") != mode or sample.get("outputs") != count or sample.get("output_bytes") != size:
        raise common.BenchmarkError("output-import sample has the wrong configuration")
    if sample.get("correctness") != CORRECTNESS:
        raise common.BenchmarkError("output-import correctness gate is missing")
    for field in ("nanos", "session_nanos", "stage_nanos", "publish_nanos", "import_nanos"):
        if type(sample.get(field)) is not int or sample[field] < 0:
            raise common.BenchmarkError(f"invalid output-import timing field: {field}")
    ledger = sample.get("ledger")
    if not isinstance(ledger, dict) or "journal_append_sync" not in ledger:
        raise common.BenchmarkError("output-import journal sync accounting is missing")
    for phase in ledger.values():
        if not isinstance(phase, dict) or type(phase.get("count")) is not int or phase["count"] < 1:
            raise common.BenchmarkError("invalid output-import ledger phase count")
    if sample["nanos"] <= 0 or sample["logical_bytes"] != count * size:
        raise common.BenchmarkError("invalid output-import timing or byte count")
    return sample


def save(args, result):
    common.write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    if args.report:
        lines = [
            "# Casita output import",
            "",
            "Stages identical raw blobs with per-output sessions, shared sessions keeping per-output publications, and shared publications.",
            "The batch-api and atomic-api modes call Repository.import with ImportSequence and BlobImport.batch respectively and report combined import time; their individual phase times are zero because they are not exposed by the API.",
            "The 128 KiB cases bracket the local chunker threshold. Fixture setup, repository opening, payload audits and fsck are outside timing.",
            "",
            f"Complete: {result['complete']}",
            "",
            "| Outputs | Bytes | Mode | Repetition | Total ms | Session ms | Stage ms | Publish ms | Import ms | Journal syncs |",
            "|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for sample in result["samples"]:
            scale = 1e-6
            lines.append(
                f"| {sample['outputs']} | {sample['output_bytes']} | {sample['mode']} | {sample['repetition']} | "
                f"{sample['nanos'] * scale:.3f} | {sample['session_nanos'] * scale:.3f} | "
                f"{sample['stage_nanos'] * scale:.3f} | {sample['publish_nanos'] * scale:.3f} | "
                f"{sample['import_nanos'] * scale:.3f} | {sample['ledger']['journal_append_sync']['count']} |"
            )
        common.write_atomic(args.report, "\n".join(lines) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("smoke", "standard"), default="standard")
    parser.add_argument("--counts", type=positive_csv)
    parser.add_argument("--sizes", type=positive_csv)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or (args.no_build and args.probe_binary is None):
        parser.error("positive repetitions and a binary for --no-build are required")
    counts = args.counts or ([1, 8, 15, 16, 17, 48] if args.profile == "smoke" else [1, 8, 15, 16, 17, 32, 48])
    sizes = args.sizes or ([6, 4096, 131071, 131072, 131073] if args.profile == "smoke" else [6, 4096, 131071, 131072, 131073, 1048576])
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(["cargo", *CARGO_ARGUMENTS], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError(built.stderr or built.stdout)
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    with binary.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    result = {
        "schema_version": 1,
        "result_schema": "casita.output-import.v1",
        "suite_id": "state-and-publication",
        "complete": False,
        "environment": common.environment_metadata(cli.ROOT),
        "configuration": {"profile": args.profile, "counts": counts, "sizes": sizes, "modes": list(MODES), "repetitions": args.repetitions},
        "artifacts": [{"path": str(binary), "sha256": digest}],
        "samples": [],
        "processes": [],
    }
    save(args, result)
    jobs = [(repetition, count, size, mode) for repetition in range(1, args.repetitions + 1) for count in counts for size in sizes for mode in MODES]
    random.Random(0xCA517A).shuffle(jobs)
    try:
        with tempfile.TemporaryDirectory(prefix="casita-output-import-") as temporary:
            work = pathlib.Path(temporary)
            for repetition, count, size, mode in jobs:
                print(f"output-import: outputs={count}, bytes={size}, mode={mode}, repetition={repetition}", flush=True)
                stdout, stderr = work / "stdout", work / "stderr"
                env = {**os.environ, "CASITA_OUTPUT_IMPORT_COUNT": str(count), "CASITA_OUTPUT_IMPORT_SIZE": str(size), "CASITA_OUTPUT_IMPORT_MODE": mode}
                timing = common.measured_command(common.CommandSpec([[str(binary), PROBE, "--exact", "--ignored", "--nocapture"]], work, env), stdout, stderr, check=False)
                captured = stdout.read_text(errors="replace")
                process = {**timing, "outputs": count, "output_bytes": size, "mode": mode, "repetition": repetition, "stdout": captured, "stderr": stderr.read_text(errors="replace")}
                result["processes"].append(process)
                if timing["exit_code"] != 0:
                    raise common.BenchmarkError(f"output-import probe failed ({timing['exit_code']}): {process['stderr'][-2000:]}")
                sample = parse_sample(captured, count, size, mode)
                result["samples"].append({**sample, "status": "ok", "repetition": repetition, "wall_seconds": sample["nanos"] / 1e9, "max_rss_bytes": timing["max_rss_bytes"]})
                save(args, result)
        result["complete"] = True
    except Exception as error:
        result["error"] = str(error)
        save(args, result)
        raise
    save(args, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
