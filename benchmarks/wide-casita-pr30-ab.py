#!/usr/bin/env python3
"""Compare two release Casita revisions in fresh persistent Obrador wide graphs."""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def activity():
    result = {"loadavg": os.getloadavg()}
    for kind in ("cpu", "io"):
        path = Path("/proc/pressure") / kind
        if path.exists():
            result[kind + "_pressure"] = path.read_text().strip()
    return result


def phase(binary, name, root, nodes, sandbox, env):
    start = time.perf_counter()
    process = subprocess.run(
        [str(binary), name, str(root), str(nodes), "4", "wide", sandbox, "4"],
        env=env, capture_output=True, text=True, timeout=300,
    )
    elapsed = (time.perf_counter() - start) * 1000
    if process.returncode:
        raise RuntimeError(f"{name} failed for {binary}: {process.stderr[-4000:]}")
    return json.loads(process.stdout), elapsed, process.stderr


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--helper", type=Path, required=True, help="helper embedded in both binaries")
    parser.add_argument("--shell", type=Path, required=True, help="shell embedded in both binaries")
    parser.add_argument("--nodes", type=int, default=16)
    parser.add_argument("--rounds", type=int, default=8)
    parser.add_argument("--sandbox", choices=("on", "off", "both"), default="both")
    parser.add_argument("--base-revision", required=True)
    parser.add_argument("--head-revision", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if min(args.nodes, args.rounds) < 1:
        parser.error("nodes and rounds must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    binaries = {"A": args.baseline.resolve(), "B": args.candidate.resolve()}
    helper = args.helper.absolute()
    for binary in binaries.values():
        contents = binary.read_bytes()
        for embedded in (helper, args.shell.absolute()):
            if str(embedded).encode() not in contents:
                raise RuntimeError(f"{binary} does not embed the expected path {embedded}")
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("CASITA_BENCH_") or key in (
            "RUST_LOG", "OBRADOR_BENCH_REGISTRATION_BATCH", "OBRADOR_WORKER_PROFILE",
            "OBRADOR_COMPLETION_PROFILE",
        ):
            env.pop(key)
    metadata = {
        "complete": False, "nodes": args.nodes, "builds": args.nodes * 2,
        "jobs": 4, "runtime_threads": 4, "rounds": args.rounds,
        "base_revision": args.base_revision, "head_revision": args.head_revision,
        "binaries": {key: {"path": str(path), "sha256": digest(path)}
                     for key, path in binaries.items()},
        "helper": {"path": str(helper), "resolved_path": str(helper.resolve()), "sha256": digest(helper)},
        "shell": {"path": str(args.shell.absolute()), "sha256": digest(args.shell)},
        "warmup": [], "event_logging": False,
        "tracing": "filtered build and prepared-dispatch span counter only",
    }

    def save():
        (args.output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    def trial(variant, sandbox, label):
        before = activity()
        with tempfile.TemporaryDirectory(prefix="obrador-pr30-") as temporary:
            root = Path(temporary) / "fixture"
            phase(binaries["A"], "prepare", root, args.nodes, sandbox, env)
            build, wall_ms, stderr = phase(binaries[variant], "build", root,
                                           args.nodes, sandbox, env)
            verify, _, verify_stderr = phase(binaries["A"], "verify", root,
                                             args.nodes, sandbox, env)
            if build["builds"] != args.nodes * 2 or verify["builds"] != 0:
                raise RuntimeError("incorrect build or reopen build count")
            if build["outputs"] != verify["outputs"]:
                raise RuntimeError("reopened outputs differ")
            if "casita::pin_" in stderr or "online pin acquired" in stderr:
                raise RuntimeError("timed binary emitted pin tracing")
            (args.output_dir / f"{label}.stderr").write_text(stderr + verify_stderr)
            return {
                "variant": variant, "sandbox": sandbox, "nodes": args.nodes,
                "builds": build["builds"], "reopen_builds": verify["builds"],
                "wall_ms": wall_ms, "graph_ms": build["graph_ms"],
                "inside_process_ms": build["inside_process_ms"],
                "outputs": build["outputs"], "activity_before": before,
                "activity_after": activity(),
            }

    save()
    modes = ["on", "off"] if args.sandbox == "both" else [args.sandbox]
    try:
        for sandbox in modes:
            for variant in ("A", "B"):
                metadata["warmup"].append(trial(variant, sandbox,
                                                  f"warmup-{sandbox}-{variant}"))
                save()
        with (args.output_dir / "samples.jsonl").open("w") as output:
            for repetition in range(args.rounds):
                for sandbox in modes:
                    order = ("A", "B") if (repetition + (sandbox == "off")) % 2 == 0 else ("B", "A")
                    pair = []
                    for position, variant in enumerate(order):
                        row = trial(variant, sandbox, f"{repetition}-{sandbox}-{variant}")
                        row.update(round=repetition, position=position)
                        pair.append(row)
                        output.write(json.dumps(row) + "\n")
                        output.flush()
                        print(json.dumps({key: row[key] for key in (
                            "round", "sandbox", "variant", "wall_ms", "graph_ms"
                        )}), flush=True)
                    if pair[0]["outputs"] != pair[1]["outputs"]:
                        raise RuntimeError("baseline and candidate outputs differ")
        metadata["complete"] = True
    except Exception as error:
        metadata["error"] = str(error)
        raise
    finally:
        save()


if __name__ == "__main__":
    main()
