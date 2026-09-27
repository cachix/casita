"""Sequential all-suite execution with immutable binaries and a completion ledger."""
from __future__ import annotations
import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time
from benchmarks import cli
from benchmarks import storage
from benchmarks.suites import repository as common

CORE_BENCHES = ("write_path", "hash_inputs", "tar_import", "filesystem_import", "dedup", "repairing", "optimization", "compression_handoff", "git_fetch_fairness", "verified_io", "overwrite_pages", "manifest_reads", "nar_associations", "nar_import", "bao_packing", "cdcs", "sliced_transfer")

# Bounded defaults. Frontier sizes remain explicit opt-in suite arguments.
SMOKE = {
    "filesystem-reuse": ["--profile", "standard"],
    "scoped-catalog": ["--profile", "smoke"],
    "catalog-wal": ["--profile", "smoke"],
    "s3-read-planning": ["--profile", "smoke", "--rtt-ms", "0,80", "--caches", "below-largest-pack", "above-largest-pack", "default", "ample"],
    "s3-fetch-pipeline": ["--profile", "smoke", "--rtt-ms", "0,80", "--caches", "below-largest-pack", "above-largest-pack", "default", "ample"],
    "s3-fetch-lookahead": ["--profile", "smoke", "--rtt-ms", "0,80", "--caches", "below-largest-pack", "above-largest-pack", "default", "ample"],
    "pack-read-planning": ["--profile", "smoke"],
    "s3-fragmentation": ["--profile", "smoke", "--read-bytes", "0,1", "--include-small-buffer-control"],
    "pack-fragmentation": ["--profile", "smoke"],
    "memory-publication": ["--profile", "smoke"],
    "memory-index-lifecycle": ["--profile", "smoke"],
    "mutation-catalog": ["--profile", "smoke"],
    "filesystem-outputs": ["--profile", "smoke"],
    "output-import": ["--profile", "smoke"],
    "git-import-profile": ["--profile", "smoke"],
    "git-ingest-scheduling": ["--profile", "smoke"],
    "git-ingest-concurrency": ["--profile", "smoke"],
    "git-fetch-s3": ["--profile", "smoke", "--diagnostics"],
    "git-fetch-local": ["--profile", "smoke", "--diagnostics"],
    "git-pack-cached": ["--profile", "smoke"],
    "git-pack-delayed": ["--profile", "smoke"],
    "git-pack-boundary": ["--profile", "smoke"],
    "ingest-scheduling": ["--profile", "smoke"],
    "ingest-concurrency": ["--profile", "smoke"],
    "snapshot-connections": ["--profile", "smoke"],
    "erofs-transports": ["--profile", "smoke"],
    "filesystem-transports": ["--profile", "smoke"],
    "native-fskit": ["--profile", "smoke"],
    "native-fskit-portable": ["--profile", "smoke"],
    "native-fskit-repository": ["--profile", "smoke"],
    "native-fskit-launch": ["--profile", "smoke"],
    "native-fskit-launch-uncached": ["--profile", "smoke"],
    "native-fskit-launch-density-enumeration-uncached": ["--profile", "smoke"],
    "native-fskit-launch-enumeration-uncached": ["--profile", "smoke"],
    "native-fskit-launch-eager": ["--profile", "smoke"],
    "native-fskit-launch-profile": ["--profile", "smoke"],
    "native-fskit-launch-density-filename-bytes": ["--profile", "smoke"],
    "native-fskit-launch-capabilities": ["--profile", "smoke"],
    "native-fskit-launch-zero-times": ["--profile", "smoke"],
    "native-fskit-first-launch": ["--profile", "smoke"],
    "native-fskit-workloads": ["--profile", "smoke", "--repetitions", "1"],
    "native-fskit-workloads-readers-16": ["--profile", "smoke", "--repetitions", "1"],
    "native-fskit-workloads-read-trace": ["--profile", "smoke", "--repetitions", "1"],
    "native-fskit-workloads-uncached": ["--profile", "smoke", "--repetitions", "1"],
    "native-fskit-first-launch-uncached": ["--profile", "smoke"],
    "native-fskit-launch-density-phases": ["--profile", "smoke"],
    "native-fskit-launch-density": ["--profile", "smoke"],
    "native-fskit-launch-explicit-xattrs": ["--profile", "smoke"],
    "native-fskit-launch-density-explicit-xattrs": ["--profile", "smoke"],
    "memory-snapshots": ["--profile", "smoke"],
    "obrador-reads": ["--profile", "smoke"],
    "ledger-boundaries": ["--profile", "smoke"],
    "pin-protocol": ["--profile", "smoke"],
    "pin-protocol-s3": ["--profile", "smoke"],
    "pin-http": ["--profile", "smoke"],
    "small-blob-pins": ["--profile", "smoke"],
    "durable-ledger": ["--profile", "smoke", "--counts", "64,4096", "--contexts", "quiet,readers,claims"],
    "pin-growth": ["--profile", "smoke"],
    "reader-coordination": ["--profile", "smoke"],
    "decoded-seek-replay": ["--profile", "smoke"],
    "object-reads": ["--profile", "smoke"],
    "metadata-kv": ["--profile", "smoke"],
    "metadata-primitives": ["--profile", "smoke"],
    "metadata-batch": ["--profile", "smoke"],
    "fsck": ["--profile", "smoke"],
    "metadata-scan": ["--profile", "smoke"],
    "metadata-collection": ["--profile", "smoke"],
    "held-catalog-gc": ["--profile", "smoke"],
    "catalog-marking": ["--profile", "smoke"],
    "cleanup-batches": ["--profile", "smoke"],
    "repository": ["--profile", "smoke", "--implementations", "casita,git,tar-zstd", "--cache-policies", "warm"],
    "nixpkgs": ["--cache-policies", "warm"],
    "cdcs-corpus": [],
    "pack-limits": ["--profile", "smoke", "--targets-mib", "1,4"],
    "pack-index": ["--files", "128"],
    "s3-catalog-index": ["--entries", "65536", "--lookups", "10000", "--manifest-percents", "0,100"],
    "catalog-index": ["--entries", "65536", "--lookups", "10000", "--manifest-percents", "0,100"],
    "pack-gc": ["--targets-mib", "1", "--dead-percent", "10,100"],
    "s3-pack": ["--profile", "smoke", "--targets-mib", "1", "--cache-mib", "0,1"],
    "s3-pack-index": ["--files", "128", "--targets-mib", "1"],
    "s3-pack-gc": ["--targets-mib", "1", "--dead-percent", "10,100"],
    "s3-path-transfer": ["--depths", "0,4", "--subtree-files", "1,64", "--cache-mib", "0,64", "--rtt-ms", "0,20", "--transports", "direct-s3,atomic-rpc", "--max-rpc-requests", "5"],
    "graph-traversal": ["--profile", "smoke", "--spill-thresholds", "4,1024,250000"],
    "git-scale": ["--profile", "smoke"],
    "gix-odb": ["--profile", "smoke"],
    "process-contention": [],
    "state-publication": ["--iterations", "10"],
    "catalog-maintenance": ["--iterations", "100"],
    "catalog-durability": ["--iterations", "10"],
    "logical-state": ["--entries", "4096"],
    "casitar": ["--profile", "smoke"],
    "casitar-scaling": ["--profile", "smoke"],
    "casitar-import-profile": ["--profile", "smoke"],
    "casitar-pin-profile": ["--profile", "smoke"],
    "casitar-quiet-import": ["--profile", "smoke"],
    "fault-and-recovery": ["--profile", "smoke"],
    "history-scale": ["--profile", "smoke"],
    "pack-cache-scale": ["--profile", "smoke"],
    "pack-cache-network": ["--profile", "smoke", "--patterns", "random", "--phases", "warm", "--concurrency", "1", "--rtt-ms", "0,20", "--bandwidths-kib", "8192"],
    "network-scale": ["--profile", "smoke", "--rtt-ms", "0,20", "--bandwidths-kib", "0,1024"],
    "generations": ["--profile", "smoke", "--generations", "3"],
}


def save(path, value):
    common.write_atomic(path, json.dumps(value, indent=2) + "\n")


def fingerprint(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def build_commands(selected, build_dir):
    """Request only needed targets; Cargo owns source/configuration freshness."""
    from benchmarks.revisions import SUITE_BUILD_SPECS
    names = set()
    for suite in selected:
        if suite == "core-primitives":
            names.update(CORE_BENCHES)
        elif suite in {"online-holds", "retained-readers", "transfer-holds", "root-prefix"}:
            names.add(suite.replace("-", "_"))
        elif suite in SUITE_BUILD_SPECS:
            names.add(SUITE_BUILD_SPECS[suite].artifact_name)
        elif suite in {"state-publication", "catalog-maintenance", "catalog-durability", "logical-state", "s3-catalog-index"}:
            names.add("casita-lib-test")
        elif suite in {"casitar", "casitar-scaling", "casitar-import-profile", "casitar-pin-profile", "casitar-quiet-import", "fault-and-recovery", "generations", "process-contention"}:
            names.add("casita")
    if "fsck" in selected:
        names.add("casita-lib-test")
    prefix = ["cargo", "--config", f'build.build-dir="{build_dir}"']
    commands = []
    benches = sorted(names & {*CORE_BENCHES, "gix_odb", "online_holds", "retained_readers", "transfer_holds", "root_prefix"})
    examples = sorted(names - {*benches, "casita", "casita-lib-test"})
    if benches:
        commands.append(prefix + ["bench", "--all-features", "--no-run", "--message-format=json"] +
                        [arg for name in benches for arg in ("--bench", name)])
    if "casita-lib-test" in names:
        commands.append(prefix + ["test", "-p", "casita", "--release", "--all-features", "--lib", "--no-run", "--message-format=json"])
    if examples or "casita" in names:
        commands.append(prefix + ["build", "--release", "--all-features", "--message-format=json"] +
                        (["--bin", "casita"] if "casita" in names else []) +
                        [arg for name in examples for arg in ("--example", name)])
    return commands


def build_binaries(output, build_dir, selected=None):
    destination = output / "bin"
    destination.mkdir()
    artifacts = {}
    commands = build_commands(selected if selected is not None else [entry["id"] for entry in cli.entrypoints()], build_dir)
    save(output / "build-commands.json", commands)
    for index, command in enumerate(commands):
        with (output / f"build-{index}.jsonl").open("w+") as messages, (output / f"build-{index}.log").open("w") as errors:
            print("building benchmark artifacts", flush=True)
            subprocess.run(command, cwd=cli.ROOT, stdout=messages, stderr=errors, check=True)
            messages.seek(0)
            for line in messages:
                try:
                    artifact = json.loads(line)
                except json.JSONDecodeError:
                    continue
                executable = artifact.get("executable")
                if artifact.get("reason") != "compiler-artifact" or not executable:
                    continue
                target = artifact["target"]
                name = "casita-lib-test" if target["kind"] == ["lib"] else target["name"]
                if name not in {"casita", "casita-lib-test", *CORE_BENCHES, "online_holds", "retained_readers", "transfer_holds", "root_prefix", "gix_odb", "pack_index_rustfs", "pack_gc_rustfs", "s3_path_transfer", "git_fetch_s3", "git_pack_cached", "pack_cache_network"}:
                    continue
                path = destination / name
                if name in artifacts:
                    path.unlink()
                artifacts[name] = storage.retain_binary(pathlib.Path(executable), path)
    save(output / "artifacts.json", artifacts)
    return destination


def suite_arguments(identifier, binary_dir, profile, repetitions):
    from benchmarks.revisions import SUITE_BUILD_SPECS
    if identifier in {"pin-protocol", "pin-protocol-s3", "pin-http"}:
        return ["--profile", profile, "--repetitions", str(repetitions)]
    if identifier in {"obrador-reads", "filesystem-transports", "erofs-transports", "pack-read-planning", "native-fskit", "native-fskit-portable", "native-fskit-repository", "native-fskit-launch", "native-fskit-launch-uncached", "native-fskit-launch-eager", "native-fskit-launch-enumeration-uncached", "native-fskit-launch-density-enumeration-uncached", "native-fskit-launch-density", "native-fskit-launch-density-phases", "native-fskit-launch-capabilities", "native-fskit-launch-zero-times", "native-fskit-first-launch", "native-fskit-workloads", "native-fskit-workloads-readers-16", "native-fskit-workloads-uncached", "native-fskit-workloads-read-trace", "native-fskit-first-launch-uncached", "native-fskit-launch-density-filename-bytes", "native-fskit-launch-profile", "native-fskit-launch-explicit-xattrs", "native-fskit-launch-density-explicit-xattrs"}:
        return ["--profile", profile, "--repetitions", str(repetitions)]
    arguments = list(SMOKE.get(identifier, [])) if profile == "smoke" else []
    if identifier == "s3-fragmentation" and profile == "standard":
        arguments += ["--include-small-buffer-control"]
    if identifier == "fsck":
        arguments += ["--seed-probe", str(binary_dir / "casita-lib-test")]
    if identifier in {"state-publication", "catalog-maintenance", "catalog-durability", "logical-state", "s3-catalog-index"}:
        arguments += ["--probe-binary", str(binary_dir / "casita-lib-test")]
    elif identifier in {"casitar", "casitar-scaling", "casitar-import-profile", "casitar-pin-profile", "casitar-quiet-import", "fault-and-recovery", "generations", "process-contention"}:
        arguments += ["--casita-bin", str(binary_dir / "casita")]
        if identifier.startswith("casitar-") and profile == "standard":
            arguments += ["--profile", "standard"]
    else:
        spec = SUITE_BUILD_SPECS[identifier]
        arguments += [spec.artifact_option, str(binary_dir / spec.artifact_name)]
        if spec.supports_no_build:
            arguments += ["--no-build"]
    if identifier != "pack-index":
        arguments += ["--repetitions", str(repetitions)]
    return arguments


def execute(command, log, timeout, environment=None):
    started = time.monotonic()
    with log.open("w") as handle:
        process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT, env=environment, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
            status = "passed" if code == 0 else "failed"
        except subprocess.TimeoutExpired:
            import signal
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            code, status = None, "timeout"
    return {"command": command, "exit_code": code, "status": status, "elapsed_seconds": time.monotonic() - started, "log": str(log)}


def pin_protocol_s3_matrix(path):
    result = json.loads(path.read_text())
    config = result["configuration"]
    expected = (len(config["writers"]) * len(config["objects"]) * len(config["faults"])
                * config["repetitions"] * 3)
    samples = result["samples"]
    return {
        "expected_cases": expected,
        "recorded_cases": len(samples),
        "attempted_all": result.get("attempted_all") is True and len(samples) == expected,
        "failed_cases": sum(row.get("status") != "ok" for row in samples),
        "unaudited_cases": sum(row.get("fresh_readback") is not True for row in samples),
    }


def collect_criterion(output, criterion_home):
    # Match executed IDs so an imported Criterion directory cannot silently
    # contribute stale cases from an older benchmark binary.
    identifiers = set()
    for name in CORE_BENCHES:
        for line in (output / f"{name}.log").read_text().splitlines():
            if line.startswith("Benchmarking "):
                identifiers.add(line.removeprefix("Benchmarking ").split(": ", 1)[0])
    samples = []
    for path in sorted(criterion_home.rglob("new/benchmark.json")):
        benchmark = json.loads(path.read_text())
        if benchmark["full_id"] not in identifiers:
            continue
        estimates = json.loads(path.with_name("estimates.json").read_text())
        samples.append({"status": "ok", "implementation": "casita", "operation": benchmark["full_id"],
            "wall_seconds": estimates["median"]["point_estimate"] / 1e9,
            "criterion_estimates_nanos": estimates, "benchmark": benchmark,
            "raw_directory": str(path.parent)})
    if not samples or {sample["operation"] for sample in samples} != identifiers:
        raise common.BenchmarkError("Criterion estimates are missing for executed cases")
    save(output / "core-primitives.json", {"schema_version": 1, "result_schema": "casita.core-primitives.v1",
        "suite_id": "core-primitives", "environment": json.loads((output / "environment.json").read_text()),
        "configuration": {"profile": "criterion", "statistic": "per-process Criterion median"}, "samples": samples})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, help="fresh result directory; default: benchmarks/results/<UTC time>-<unique ID>")
    parser.add_argument("--profile", choices=("smoke", "standard"), default="smoke")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--suites", help="comma-separated subset; defaults to every registered suite")
    parser.add_argument("--groups", help="comma-separated manifest groups; see benchmark list --groups")
    parser.add_argument("--bin-dir", type=pathlib.Path, help="retain prebuilt binaries in the shared artifact store instead of compiling")
    parser.add_argument("--build-dir", type=pathlib.Path, default=cli.ROOT / "target" / "benchmark-build")
    parser.add_argument("--nixpkgs", type=pathlib.Path)
    parser.add_argument("--cdcs-store-names", help="comma-separated Nix store names with rebuilt copies for the cdcs-corpus suite")
    parser.add_argument("--obrador-source", type=pathlib.Path, default=os.environ.get("CASITA_OBRADOR_SOURCE"))
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args(argv)
    entries = cli.entrypoints()
    if args.groups and args.suites:
        parser.error("choose --groups or --suites")
    selected = args.suites.split(",") if args.suites else [entry["id"] for entry in entries]
    if args.groups:
        groups = args.groups.split(",")
        if set(groups) - {entry["suite_id"] for entry in entries}:
            parser.error("unknown group; see benchmark list --groups")
        selected = [entry["id"] for entry in entries if entry["suite_id"] in groups]
    if len(set(selected)) != len(selected) or set(selected) - {entry["id"] for entry in entries}:
        parser.error("suites must be unique registered identifiers")
    if min(args.repetitions, args.timeout) < 1:
        parser.error("repetitions and timeout must be positive")
    output = (args.output or storage.new_output()).resolve()
    output.mkdir(parents=True, exist_ok=False)
    print(f"results: {output}", flush=True)
    work = storage.prepare_work(output)
    run_environment = {**os.environ, "TMPDIR": str(work), "TMP": str(work), "TEMP": str(work)}
    ledger = {"schema_version": 1, "profile": args.profile, "complete": False,
        "work_directory": str(work), "finished": False,
        "revision": common.run_checked(["git", "rev-parse", "HEAD"]).strip(),
        "diff": common.run_checked(["git", "diff", "--stat"]),
        "entries": [{"suite": suite, "status": "pending"} for suite in selected]}
    save(output / "execution.json", ledger)
    save(output / "environment.json", common.environment_metadata(output))
    try:
        if args.bin_dir:
            binary_dir = output / "bin"
            binary_dir.mkdir()
            save(output / "artifacts.json", {path.name: storage.retain_binary(path, binary_dir / path.name)
                 for path in args.bin_dir.resolve().iterdir() if path.is_file()})
        elif set(selected) <= {"filesystem-transports", "erofs-transports", "pin-protocol", "pin-protocol-s3", "pin-http", "remote-pin-cost", "native-fskit", "native-fskit-portable", "native-fskit-repository", "native-fskit-launch", "native-fskit-launch-uncached", "native-fskit-launch-eager", "native-fskit-launch-enumeration-uncached", "native-fskit-launch-density-enumeration-uncached", "native-fskit-launch-density", "native-fskit-launch-density-phases", "native-fskit-launch-capabilities", "native-fskit-launch-zero-times", "native-fskit-first-launch", "native-fskit-workloads", "native-fskit-workloads-readers-16", "native-fskit-workloads-uncached", "native-fskit-workloads-read-trace", "native-fskit-first-launch-uncached", "native-fskit-launch-density-filename-bytes", "native-fskit-launch-profile", "native-fskit-launch-explicit-xattrs", "native-fskit-launch-density-explicit-xattrs"}:
            # These suites need no shared build or build their own probe.
            binary_dir = output / "bin"
            binary_dir.mkdir()
            save(output / "artifacts.json", {})
        else:
            binary_dir = build_binaries(output, args.build_dir.resolve(), selected)
    except Exception as error:
        ledger["build_error"] = str(error)
        save(output / "execution.json", ledger)
        raise
    for record in ledger["entries"]:
        suite = record["suite"]
        print(f"running {suite}", flush=True)
        record["status"] = "running"
        save(output / "execution.json", ledger)
        try:
            if suite in {"native-fskit", "native-fskit-portable", "native-fskit-repository", "native-fskit-launch", "native-fskit-launch-uncached", "native-fskit-launch-eager", "native-fskit-launch-enumeration-uncached", "native-fskit-launch-density-enumeration-uncached", "native-fskit-launch-density", "native-fskit-launch-density-phases", "native-fskit-launch-capabilities", "native-fskit-launch-zero-times", "native-fskit-first-launch", "native-fskit-workloads", "native-fskit-workloads-readers-16", "native-fskit-workloads-uncached", "native-fskit-workloads-read-trace", "native-fskit-first-launch-uncached", "native-fskit-launch-density-filename-bytes", "native-fskit-launch-profile", "native-fskit-launch-explicit-xattrs", "native-fskit-launch-density-explicit-xattrs"} and sys.platform != "darwin":
                record.update(status="skipped", reason="native FSKit requires macOS 15.4+; no native result collected")
                continue
            if suite in {"pin-protocol", "pin-protocol-s3", "pin-http"}:
                log = output / f"{suite}.log"
                module = "benchmarks.suites." + suite.replace("-", "_")
                run = execute([sys.executable, "-m", module,
                               "--profile", args.profile, "--repetitions", str(args.repetitions),
                               "--output", str(output / f"{suite}.json")], log, args.timeout, run_environment)
                record.update(run)
                if suite == "pin-protocol-s3":
                    matrix = pin_protocol_s3_matrix(output / f"{suite}.json")
                    record["matrix"] = matrix
                    if not matrix["attempted_all"] or matrix["failed_cases"] or matrix["unaudited_cases"]:
                        record["status"] = "failed"
                if suite == "pin-http":
                    control = execute([sys.executable, "-m", module, "--writers", "32",
                                       "--operations", "8", "--modes", "hot-put",
                                       "--lock-timeout-seconds", "2", "--output",
                                       str(output / "pin-http-lock2.json")],
                                      output / "pin-http-lock2.log", args.timeout, run_environment)
                    record.update(runs=[run, control], status="passed" if
                                  all(r["status"] == "passed" for r in (run, control)) else "failed")
                continue
            if suite == "remote-pin-cost":
                entry = next(entry for entry in entries if entry["id"] == suite)
                log = output / "remote-pin-cost.log"
                run = execute(entry["target"], log, args.timeout, run_environment)
                rows = [json.loads(line.removeprefix("remote_pin_cost_sample "))
                        for line in log.read_text().splitlines()
                        if line.startswith("remote_pin_cost_sample ")]
                if run["status"] == "passed" and len(rows) != 40:
                    raise common.BenchmarkError("remote pin cost probe must emit all 40 cases")
                save(output / "remote-pin-cost.json", {"samples": rows})
                record.update(run)
                continue
            if suite == "obrador-reads" and args.obrador_source is None:
                record.update(status="skipped", reason="--obrador-source is required for the external application workload")
                continue
            if suite == "nixpkgs" and args.nixpkgs is None:
                record.update(status="skipped", reason="--nixpkgs is required for the committed external corpus")
                continue
            if suite == "cdcs-corpus" and not args.cdcs_store_names:
                record.update(status="skipped", reason="--cdcs-store-names is required for the rebuilt store path corpus")
                continue
            if suite == "transfer-holds":
                runs, samples = [], []
                for repetition in range(args.repetitions):
                    environment = dict(run_environment)
                    environment.pop("CASITA_BENCH_TRANSFER_REVERSE", None)
                    if repetition % 2:
                        environment["CASITA_BENCH_TRANSFER_REVERSE"] = "1"
                    log = output / f"transfer-holds-{repetition}.log"
                    run = execute([str(binary_dir / "transfer_holds")], log, args.timeout, environment)
                    runs.append(run)
                    rows = [json.loads(line) for line in log.read_text().splitlines() if line.startswith("{")]
                    expected = {(transport, scope, size, gc)
                                for transport in ("local", "ssh-stdio")
                                for scope in ("snapshot", "selected")
                                for size in (4096, 4194304) for gc in (False, True)}
                    observed = {(row["transport"], row["scope"], row["payload_bytes"], row["gc"]) for row in rows}
                    samples.extend({**row, "repetition": repetition} for row in rows)
                    save(output / "transfer-holds.json", {"schema_version": 1, "suite_id": "transfer", "samples": samples})
                    if run["status"] == "passed" and (len(rows) != 16 or observed != expected
                            or any(row["correctness"] != "passed" for row in rows)):
                        raise common.BenchmarkError("transfer holds must pass all sixteen transport/scope/size/GC cases")
                record.update(status="passed" if all(run["status"] == "passed" for run in runs) else "failed", runs=runs)
                continue
            if suite == "root-prefix":
                environment = {**run_environment, "CASITA_BENCH_ROOT_PREFIX_ITERATIONS": "1" if args.profile == "smoke" else "5"}
                runs, samples = [], []
                for repetition in range(args.repetitions):
                    log = output / f"root-prefix-{repetition}.log"
                    run = execute([str(binary_dir / "root_prefix")], log, args.timeout, environment)
                    runs.append(run)
                    rows = [json.loads(line) for line in log.read_text().splitlines() if line.startswith("{")]
                    expected = {(256, 255), (258, 257), (4097, 8), (4097, 257)}
                    observed = {(row["total_roots"], row["matched_roots"]) for row in rows}
                    if run["status"] == "passed" and (len(rows) != 4 or observed != expected
                            or any(row["correctness"] != "passed" for row in rows)):
                        raise common.BenchmarkError("root prefix must pass all four size and density cases")
                    samples.extend({**row, "repetition": repetition} for row in rows)
                record.update(status="passed" if all(run["status"] == "passed" for run in runs) else "failed", runs=runs)
                save(output / "root-prefix.json", {"schema_version": 1, "samples": samples})
                continue
            if suite == "retained-readers":
                environment = {**run_environment, "CASITA_BENCH_RETAINED_ITERATIONS": "3" if args.profile == "smoke" else "30"}
                runs, samples = [], []
                for repetition in range(args.repetitions):
                    log = output / f"retained-readers-{repetition}.log"
                    run = execute([str(binary_dir / "retained_readers")], log, args.timeout, environment)
                    runs.append(run)
                    rows = [json.loads(line) for line in log.read_text().splitlines() if line.startswith("{")]
                    expected = {(owner, fanout, gc) for owner in ("durable", "process") for fanout in (1, 32) for gc in (False, True)}
                    observed = {(row["ownership"], row["readers_per_session"], row["concurrent_gc"]) for row in rows}
                    if run["status"] == "passed" and (len(rows) != 8 or observed != expected or any(row["correctness"] != "passed" for row in rows)):
                        raise common.BenchmarkError("retained readers must pass all eight ownership/fanout/GC cases")
                    samples.extend({**row, "repetition": repetition} for row in rows)
                record.update(status="passed" if all(run["status"] == "passed" for run in runs) else "failed", runs=runs)
                save(output / "retained-readers.json", {"schema_version": 1, "samples": samples})
                continue
            if suite == "online-holds":
                entry = next(entry for entry in entries if entry["id"] == suite)
                environment = {**run_environment, "CASITA_BENCH_READER_SCOPE": "application",
                               "CASITA_BENCH_FILES": "2" if args.profile == "smoke" else "16"}
                environment.pop("CASITA_BENCH_SCENARIO", None)
                runs = []
                samples = []
                for imports in entry["import_counts"][args.profile]:
                    environment["CASITA_BENCH_IMPORTS"] = str(imports)
                    for repetition in range(args.repetitions):
                        log = output / f"online-holds-{imports}-{repetition}.log"
                        run = execute([str(binary_dir / "online_holds")], log, args.timeout, environment)
                        runs.append({**run, "imports": imports, "repetition": repetition})
                        rows = [json.loads(line) for line in log.read_text().splitlines() if line.startswith("{")]
                        if run["status"] == "passed" and len(rows) != 4:
                            raise common.BenchmarkError("online holds must report four scenarios")
                        samples.extend({**row, "repetition": repetition} for row in rows)
                record.update(status="passed" if all(run["status"] == "passed" for run in runs) else "failed", runs=runs)
                save(output / "online-holds.json", {"schema_version": 1, "samples": samples})
                continue
            if suite == "core-primitives":
                environment = {**run_environment, "CRITERION_HOME": str(output / "criterion")}
                # Keep the all-suite identity bounded and independent of an
                # interactive corpus investigation's ambient environment.
                environment.pop("CASITA_HASH_REPOSITORY", None)
                environment.pop("CASITA_TAR_REVERSE", None)
                environment.pop("CASITA_CHUNK_DECODE_REVERSE", None)
                environment.pop("CASITA_BENCH_PERF_CONTROL", None)
                environment.pop("CASITA_BENCH_PERF_ACK", None)
                environment["CASITA_HASH_REPORT"] = str(output / "hash-inputs.json")
                runs = [execute([str(binary_dir / name), "--bench", "--warm-up-time", "1", "--measurement-time", "3", "--noplot"], output / f"{name}.log", args.timeout, environment) for name in CORE_BENCHES]
                record.update(status="passed" if all(run["status"] == "passed" for run in runs) else "failed", runs=runs)
                if record["status"] == "passed":
                    collect_criterion(output, output / "criterion")
                continue
            arguments = suite_arguments(suite, binary_dir, args.profile, args.repetitions)
            if suite == "nixpkgs":
                arguments += ["--nixpkgs", str(args.nixpkgs.resolve())]
            if suite in ("git-fetch-s3", "git-fetch-local", "git-pack-cached") and args.nixpkgs is not None:
                arguments += ["--nixpkgs", str(args.nixpkgs.resolve())]
            if suite == "cdcs-corpus":
                for name in args.cdcs_store_names.split(","):
                    arguments += ["--store-name", name]
            command = [sys.executable, "-m", "benchmarks.cli", "run", suite, *arguments, "--output", str(output / f"{suite}.json")]
            if suite == "obrador-reads":
                command += ["--obrador-source", str(args.obrador_source.resolve())]
            with contextlib.ExitStack() as stack:
                environment = dict(run_environment)
                if suite == "s3-pack":
                    from benchmarks.suites.pack.s3_gc import Rustfs
                    from benchmarks.suites.transfer.s3_path import create_rustfs_bucket
                    server = Rustfs(work / "rustfs")
                    stack.callback(server.close)
                    create_rustfs_bucket(server.endpoint, "casita-bench")
                    environment.update(AWS_ACCESS_KEY_ID="minio", AWS_SECRET_ACCESS_KEY="minio123", AWS_REGION="us-east-1", AWS_ENDPOINT_URL=server.endpoint, AWS_ENDPOINT=server.endpoint, AWS_ALLOW_HTTP="true", AWS_EC2_METADATA_DISABLED="true")
                    environment.pop("AWS_SESSION_TOKEN", None)
                    environment.pop("AWS_PROFILE", None)
                    command += ["--s3-url", "s3://casita-bench/all", "--latency-label", "loopback-rustfs"]
                record.update(execute(command, output / f"{suite}.log", args.timeout, environment))
        except Exception as error:
            record.update(status="failed", error=str(error))
        finally:
            save(output / "execution.json", ledger)
    artifacts = json.loads((output / "artifacts.json").read_text())
    ledger["artifacts_unchanged"] = all(fingerprint(pathlib.Path(artifact["path"])) == artifact["sha256"] for artifact in artifacts.values())
    ledger["complete"] = ledger["artifacts_unchanged"] and all(record["status"] == "passed" for record in ledger["entries"])
    ledger["finished"] = True
    save(output / "execution.json", ledger)
    print(f"completion ledger: {output / 'execution.json'}")
    return 0 if ledger["complete"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
