#!/usr/bin/env python3
"""Build and benchmark two or more Git revisions with one stable harness."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Sequence
from typing import Any

from benchmarks import comparison, dashboard, metrics
from benchmarks.suites import repository as common


ROOT = pathlib.Path(__file__).resolve().parents[1]


@dataclasses.dataclass(frozen=True)
class SuiteBuildSpec:
    cargo_arguments: tuple[str, ...]
    artifact_option: str
    artifact_name: str
    relative_artifact: str | None = None
    cargo_json_bench: str | None = None
    cargo_json_test: str | None = None
    supports_repetitions: bool = True
    supports_no_build: bool = True
    supports_report: bool = True
    records_revision: bool = False


SUITE_BUILD_SPECS = {
    "git-verified-stream": SuiteBuildSpec(
        ("test", "--release", "--no-default-features", "--features", "native,git,experimental", "--test", "verified_stream", "--no-run", "--message-format=json"),
        "--probe-binary", "verified_stream", cargo_json_test="verified_stream", supports_report=False,
    ),
    "git-blob-file": SuiteBuildSpec(
        ("test", "--release", "--no-default-features", "--features", "native,git,experimental", "--test", "git_blob_file", "--no-run", "--message-format=json"),
        "--probe-binary", "git_blob_file", cargo_json_test="git_blob_file", supports_report=False,
    ),
    "metadata-collection": SuiteBuildSpec(
        ("test", "--release", "--features", "cli", "--lib", "--no-run", "--message-format=json"),
        "--probe-binary", "casita-lib-test", cargo_json_test="casita",
    ),
    "repository": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
        records_revision=True,
    ),
    "nixpkgs": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
        records_revision=True,
    ),
    "pack-limits": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
    ),
    "pack-index": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
        supports_repetitions=False,
    ),
    "catalog-index": SuiteBuildSpec(
        (
            "test",
            "--release",
            "--features",
            "s3",
            "--lib",
            "--no-run",
            "--message-format=json",
        ),
        "--probe-binary",
        "casita-lib-test",
        cargo_json_test="casita",
        supports_no_build=False,
    ),
    "pack-gc": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
    ),
    "s3-pack": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git,s3", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
    ),
    "s3-pack-index": SuiteBuildSpec(
        ("build", "--release", "--example", "pack_index_rustfs", "--features", "s3"),
        "--helper",
        "pack_index_rustfs",
        relative_artifact="release/examples/pack_index_rustfs",
    ),
    "s3-pack-gc": SuiteBuildSpec(
        ("build", "--release", "--example", "pack_gc_rustfs", "--features", "s3"),
        "--helper",
        "pack_gc_rustfs",
        relative_artifact="release/examples/pack_gc_rustfs",
    ),
    "s3-path-transfer": SuiteBuildSpec(
        ("build", "--release", "--example", "s3_path_transfer", "--features", "s3,ssh"),
        "--helper",
        "s3_path_transfer",
        relative_artifact="release/examples/s3_path_transfer",
    ),
    "gix-odb": SuiteBuildSpec(
        (
            "bench",
            "--features",
            "git",
            "--bench",
            "gix_odb",
            "--no-run",
            "--message-format=json",
        ),
        "--benchmark-bin",
        "gix_odb",
        cargo_json_bench="gix_odb",
    ),
    "graph-traversal": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git", "--bin", "casita"),
        "--casita",
        "casita",
        relative_artifact="release/casita",
        supports_no_build=False,
        supports_report=False,
    ),
    "git-scale": SuiteBuildSpec(
        ("build", "--release", "--features", "cli,git-http", "--bin", "casita"),
        "--casita-bin",
        "casita",
        relative_artifact="release/casita",
    ),
}
SUITE_BUILD_SPECS["cdcs-corpus"] = SuiteBuildSpec(
    ("bench", "--features", "experimental", "--bench", "cdcs", "--no-run", "--message-format=json"),
    "--benchmark-bin", "cdcs", cargo_json_bench="cdcs",
)
SUITE_BUILD_SPECS["chunk-upload-completion"] = SuiteBuildSpec(
    ("test", "--release", "--no-default-features", "--features", "native,experimental", "--test", "chunk_upload_completion", "--no-run", "--message-format=json"),
    "--probe-binary", "chunk_upload_completion", cargo_json_test="chunk_upload_completion", supports_report=False,
)
SUITE_BUILD_SPECS["chunk-manifest-stream"] = SuiteBuildSpec(
    ("test", "--release", "--no-default-features", "--features", "native,experimental", "--test", "chunk_manifest_stream", "--no-run", "--message-format=json"),
    "--probe-binary", "chunk_manifest_stream", cargo_json_test="chunk_manifest_stream", supports_report=False,
)
SUITE_BUILD_SPECS["chunk-hash-batch"] = SuiteBuildSpec(
    ("test", "--release", "--no-default-features", "--features", "native,experimental", "--test", "chunk_hash_batch", "--no-run", "--message-format=json"),
    "--probe-binary", "chunk_hash_batch", cargo_json_test="chunk_hash_batch", supports_report=False,
)
SUITE_BUILD_SPECS["git-ingest-scheduling"] = SuiteBuildSpec(
    ("test", "--release", "--features", "cli,git", "--lib", "--no-run", "--message-format=json"),
    "--probe-binary", "casita-lib-test", cargo_json_test="casita",
)
SUITE_BUILD_SPECS["git-import-profile"] = SUITE_BUILD_SPECS["git-ingest-scheduling"]
SUITE_BUILD_SPECS["git-ingest-concurrency"] = SuiteBuildSpec(
    ("build", "--release", "--features", "cli,git", "--bin", "casita"),
    "--casita-bin", "casita", relative_artifact="release/casita",
)
SUITE_BUILD_SPECS["metadata-scan"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["memory-snapshots"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["memory-publication"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["memory-index-lifecycle"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["metadata-primitives"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["metadata-kv"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["metadata-batch"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["ledger-boundaries"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["small-blob-pins"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["durable-ledger"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["pin-growth"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["reader-coordination"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["decoded-seek-replay"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["object-reads"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["snapshot-connections"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["held-catalog-gc"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["catalog-marking"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["cleanup-batches"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["mutation-catalog"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["filesystem-outputs"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["filesystem-reuse"] = dataclasses.replace(SUITE_BUILD_SPECS["repository"], records_revision=False)
SUITE_BUILD_SPECS["output-import"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["catalog-wal"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["scoped-catalog"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["pack-fragmentation"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["ingest-concurrency"] = SUITE_BUILD_SPECS["pack-gc"]
SUITE_BUILD_SPECS["ingest-scheduling"] = SUITE_BUILD_SPECS["metadata-collection"]
SUITE_BUILD_SPECS["fsck"] = SuiteBuildSpec(
    ("build", "--release", "--features", "cli", "--bin", "casita"),
    "--casita", "casita", relative_artifact="release/casita",
    supports_no_build=False, supports_report=False,
)
for identifier in ("history-scale", "pack-cache-scale"):
    SUITE_BUILD_SPECS[identifier] = SuiteBuildSpec(
        ("test", "--release", "--all-features", "--lib", "--no-run", "--message-format=json"),
        "--probe-binary", "casita-lib-test", cargo_json_test="casita",
    )
SUITE_BUILD_SPECS["network-scale"] = SUITE_BUILD_SPECS["s3-path-transfer"]
SUITE_BUILD_SPECS["pack-cache-network"] = SuiteBuildSpec(
    ("build", "--release", "--example", "pack_cache_network", "--features", "s3,ssh"),
    "--helper",
    "pack_cache_network",
    relative_artifact="release/examples/pack_cache_network",
)
SUITE_BUILD_SPECS["s3-fragmentation"] = SuiteBuildSpec(
    ("test", "--release", "--features", "cli,s3", "--lib", "--no-run", "--message-format=json"),
    "--probe-binary", "casita-lib-test", cargo_json_test="casita",
)
SUITE_BUILD_SPECS["s3-read-planning"] = SUITE_BUILD_SPECS["s3-fragmentation"]
SUITE_BUILD_SPECS["s3-fetch-pipeline"] = SUITE_BUILD_SPECS["s3-fragmentation"]
SUITE_BUILD_SPECS["s3-fetch-lookahead"] = SUITE_BUILD_SPECS["s3-fragmentation"]
SUITE_BUILD_SPECS["git-fetch-s3"] = SuiteBuildSpec(
    ("build", "--release", "--example", "git_fetch_s3", "--features", "s3,git-http,experimental"),
    "--probe-binary", "git_fetch_s3", relative_artifact="release/examples/git_fetch_s3",
)
SUITE_BUILD_SPECS["git-fetch-local"] = SUITE_BUILD_SPECS["git-fetch-s3"]
SUITE_BUILD_SPECS["git-pack-cached"] = SuiteBuildSpec(
    ("build", "--offline", "--release", "--example", "git_pack_cached", "--features", "git,git-fetch,experimental"),
    "--probe-binary", "git_pack_cached", relative_artifact="release/examples/git_pack_cached",
)
SUITE_BUILD_SPECS["git-pack-delayed"] = SuiteBuildSpec(
    ("test", "--offline", "--release", "--all-features", "--lib", "--no-run", "--message-format=json"),
    "--probe-binary", "casita-lib-test", cargo_json_test="casita",
)
SUITE_BUILD_SPECS["git-pack-boundary"] = SUITE_BUILD_SPECS["git-pack-delayed"]
SUPPORTED_SUITES = tuple(SUITE_BUILD_SPECS)
CONTROLLED_SUITE_OPTIONS = {
    "--benchmark-bin",
    "--casita-bin",
    "--casita",
    "--casita-revision",
    "--helper",
    "--html",
    "--baseline-helper",
    "--keep-work",
    "--no-build",
    "--output",
    "--render-existing",
    "--repetitions",
    "--report",
    "--require-clean",
    "--probe-binary",
}


class RevisionBenchmarkError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True)
class RevisionSpec:
    argument: str
    reference: str
    label: str
    commit: str

    @property
    def short(self) -> str:
        return self.commit[:12]


def git_output(arguments: Sequence[str]) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RevisionBenchmarkError(f"git {' '.join(arguments)} failed: {detail}")
    return completed.stdout.strip()


def resolve_commit(reference: str) -> str:
    if not reference or reference.startswith("-"):
        raise RevisionBenchmarkError(f"invalid revision {reference!r}")
    commit = git_output(["rev-parse", "--verify", "--end-of-options", f"{reference}^{{commit}}"])
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RevisionBenchmarkError(f"Git returned an invalid commit for {reference!r}: {commit!r}")
    return commit


def split_revision_argument(argument: str) -> tuple[str | None, str]:
    if "=" not in argument:
        return None, argument
    label, reference = argument.split("=", 1)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", label) or not reference:
        raise RevisionBenchmarkError(
            f"invalid labeled revision {argument!r}; expected LABEL=GIT_REVISION"
        )
    return label, reference


def default_label(reference: str, commit: str) -> str:
    label = re.sub(r"[^A-Za-z0-9._-]+", "-", reference).strip("-.")
    return (label or commit[:12])[:48]


def resolve_revisions(arguments: Sequence[str]) -> list[RevisionSpec]:
    if len(arguments) < 2:
        raise RevisionBenchmarkError("at least two revisions are required")
    revisions = []
    explicit_labels: set[str] = set()
    commits: set[str] = set()
    for argument in arguments:
        supplied_label, reference = split_revision_argument(argument)
        commit = resolve_commit(reference)
        if commit in commits:
            raise RevisionBenchmarkError(
                f"revision {argument!r} resolves to duplicate commit {commit[:12]}"
            )
        label = supplied_label or default_label(reference, commit)
        if label in explicit_labels:
            if supplied_label:
                raise RevisionBenchmarkError(f"duplicate revision label {label!r}")
            label = f"{label}-{commit[:8]}"
        if label in explicit_labels:
            raise RevisionBenchmarkError(f"duplicate revision label {label!r}")
        revisions.append(RevisionSpec(argument, reference, label, commit))
        explicit_labels.add(label)
        commits.add(commit)
    return revisions


def select_baseline(revisions: Sequence[RevisionSpec], requested: str | None) -> str:
    if requested is None:
        return revisions[0].label
    for revision in revisions:
        if requested in {revision.label, revision.argument, revision.reference, revision.commit}:
            return revision.label
    try:
        commit = resolve_commit(requested)
    except RevisionBenchmarkError as error:
        raise RevisionBenchmarkError(
            f"baseline {requested!r} is not one of the supplied revisions"
        ) from error
    for revision in revisions:
        if commit == revision.commit:
            return revision.label
    raise RevisionBenchmarkError(f"baseline {requested!r} is not one of the supplied revisions")


def rotated_order(revisions: Sequence[RevisionSpec], round_index: int) -> list[RevisionSpec]:
    offset = round_index % len(revisions)
    return [*revisions[offset:], *revisions[:offset]]


def validate_suite_arguments(arguments: Sequence[str]) -> None:
    for argument in arguments:
        option = argument.split("=", 1)[0]
        if option in CONTROLLED_SUITE_OPTIONS:
            raise RevisionBenchmarkError(
                f"{option} is controlled by `benchmark revisions`; use its top-level option"
            )


def run_checked(command: Sequence[str], cwd: pathlib.Path, environment: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(command), flush=True)
    completed = subprocess.run(command, cwd=cwd, env=environment, check=False)
    if completed.returncode:
        raise RevisionBenchmarkError(
            f"command failed with status {completed.returncode}: {' '.join(command)}"
        )


def create_worktree(root: pathlib.Path, revision: RevisionSpec) -> pathlib.Path:
    path = root / "source"
    if path.exists():
        raise RevisionBenchmarkError(f"worktree path already exists: {path}")
    run_checked(["git", "worktree", "add", "--detach", str(path), revision.commit], ROOT)
    return path


def checkout_worktree(path: pathlib.Path, revision: RevisionSpec) -> None:
    run_checked(["git", "checkout", "--detach", revision.commit], path)


def cargo_build_command(worktree: pathlib.Path, spec: SuiteBuildSpec) -> list[str]:
    command = ["cargo", *spec.cargo_arguments]
    # Target feature gates can change between revisions. Read each checkout's
    # declaration so old revisions do not receive newly introduced features.
    manifest = worktree / "crates/casita/Cargo.toml"
    if not manifest.is_file():
        manifest = worktree / "Cargo.toml"
    if not manifest.is_file():
        return command
    cargo = tomllib.loads(manifest.read_text())
    for kind in ("bench", "example"):
        selector = f"--{kind}"
        if selector not in command:
            continue
        name = command[command.index(selector) + 1]
        target = next((target for target in cargo.get(kind, []) if target.get("name") == name), {})
        required = target.get("required-features", [])
        if not required:
            continue
        if "--features" not in command:
            command.extend(["--features", ",".join(required)])
        else:
            position = command.index("--features") + 1
            features = command[position].split(",")
            features.extend(feature for feature in required if feature not in features)
            command[position] = ",".join(features)
    return command


def build_artifact(
    worktree: pathlib.Path,
    target: pathlib.Path,
    destination: pathlib.Path,
    spec: SuiteBuildSpec,
) -> pathlib.Path:
    environment = {**os.environ, "CARGO_TARGET_DIR": str(target)}
    command = cargo_build_command(worktree, spec)
    if spec.cargo_json_bench:
        from benchmarks.suites import gix_odb

        print("+ " + " ".join(command), flush=True)
        completed = subprocess.run(
            command,
            cwd=worktree,
            env=environment,
            stdout=subprocess.PIPE,
            text=True,
            check=False,
        )
        if completed.returncode:
            raise RevisionBenchmarkError(
                f"command failed with status {completed.returncode}: {' '.join(command)}"
            )
        try:
            built = gix_odb.parse_benchmark_binary(completed.stdout)
        except gix_odb.GixOdbBenchmarkError as error:
            raise RevisionBenchmarkError(str(error)) from error
    elif spec.cargo_json_test:
        from benchmarks.suites.pack import catalog

        print("+ " + " ".join(command), flush=True)
        completed = subprocess.run(
            command,
            cwd=worktree,
            env=environment,
            stdout=subprocess.PIPE,
            text=True,
            check=False,
        )
        if completed.returncode:
            raise RevisionBenchmarkError(
                f"command failed with status {completed.returncode}: {' '.join(command)}"
            )
        try:
            built = catalog.parse_probe_binary(completed.stdout)
        except RuntimeError as error:
            raise RevisionBenchmarkError(str(error)) from error
    else:
        run_checked(command, worktree, environment)
        assert spec.relative_artifact is not None
        built = target / spec.relative_artifact
    if not built.is_file():
        raise RevisionBenchmarkError(f"build did not create {built}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(built, destination)
    return destination.resolve()


def remove_worktree(path: pathlib.Path) -> None:
    completed = subprocess.run(
        ["git", "worktree", "remove", "--force", str(path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode:
        print(
            f"warning: could not remove temporary worktree {path}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}",
            file=sys.stderr,
        )


def write_document(path: pathlib.Path, value: dict[str, Any]) -> None:
    common.write_atomic(path, json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def stamp_result_revision(
    path: pathlib.Path,
    revision: RevisionSpec,
    harness_revision: str,
    harness_dirty: bool,
) -> None:
    result = json.loads(path.read_text())
    if not isinstance(result, dict):
        raise RevisionBenchmarkError(f"benchmark result is not a JSON object: {path}")
    environment = result.setdefault("environment", {})
    if not isinstance(environment, dict):
        raise RevisionBenchmarkError(f"benchmark result has invalid environment metadata: {path}")
    recorded_revision = environment.get("casita_revision")
    if recorded_revision and recorded_revision != revision.commit:
        environment.setdefault("harness_revision", recorded_revision)
        environment.setdefault(
            "harness_worktree_dirty", environment.get("casita_worktree_dirty")
        )
    environment.setdefault("harness_revision", harness_revision)
    environment.setdefault("harness_worktree_dirty", harness_dirty)
    environment["casita_revision"] = revision.commit
    environment["casita_worktree_dirty"] = False
    write_document(path, result)


def invoke_suite(
    suite: str,
    spec: SuiteBuildSpec,
    forwarded: Sequence[str],
    revision: RevisionSpec,
    binary: pathlib.Path,
    output: pathlib.Path,
) -> int:
    from benchmarks import cli

    entry = next((entry for entry in cli.entrypoints() if entry["id"] == suite), None)
    if entry is None:
        raise RevisionBenchmarkError(f"benchmark manifest has no suite {suite!r}")
    arguments = [*forwarded]
    if spec.supports_repetitions:
        arguments.extend(["--repetitions", "1"])
    arguments.extend([spec.artifact_option, str(binary)])
    if spec.supports_no_build:
        arguments.append("--no-build")
    arguments.extend(["--output", str(output)])
    if spec.supports_report:
        arguments.extend(["--report", str(output.with_suffix(".md"))])
    if spec.records_revision:
        arguments.extend(["--casita-revision", revision.commit])
    try:
        return cli.run_entrypoint(entry, arguments)
    except SystemExit as error:
        if isinstance(error.code, int):
            return error.code
        raise RevisionBenchmarkError(str(error.code or "benchmark suite exited")) from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="benchmark revisions",
        description=__doc__,
        epilog="Pass options for the selected suite after `--`.",
    )
    parser.add_argument("revisions", nargs="+", metavar="REVISION")
    parser.add_argument("--suite", choices=SUPPORTED_SUITES, default="repository")
    parser.add_argument("--repetitions", type=int, default=3, help="interleaved rounds per revision")
    parser.add_argument("--baseline", help="supplied label or revision used for percentage changes")
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--keep-worktrees", type=pathlib.Path)
    parser.add_argument(
        "--artifact",
        action="append",
        default=[],
        metavar="LABEL=PATH",
        help="reuse one prebuilt artifact for each revision label instead of building",
    )
    parser.add_argument("--require-clean", action="store_true", help="require the stable harness worktree to be clean")
    return parser


def split_arguments(argv: Sequence[str]) -> tuple[list[str], list[str]]:
    arguments = list(argv)
    if "--" not in arguments:
        return arguments, []
    separator = arguments.index("--")
    return arguments[:separator], arguments[separator + 1 :]


def supplied_artifacts(values: Sequence[str], revisions: Sequence[RevisionSpec]) -> dict[str, pathlib.Path]:
    artifacts: dict[str, pathlib.Path] = {}
    labels = {revision.label for revision in revisions}
    for value in values:
        if "=" not in value:
            raise RevisionBenchmarkError("--artifact must be LABEL=PATH")
        label, raw_path = value.split("=", 1)
        if label not in labels:
            raise RevisionBenchmarkError(f"--artifact has unknown revision label {label!r}")
        if label in artifacts:
            raise RevisionBenchmarkError(f"duplicate --artifact for revision label {label!r}")
        path = pathlib.Path(raw_path).resolve()
        if not path.is_file():
            raise RevisionBenchmarkError(f"supplied artifact is not a file: {path}")
        artifacts[label] = path
    if artifacts and set(artifacts) != labels:
        missing = ", ".join(sorted(labels - set(artifacts)))
        raise RevisionBenchmarkError(f"missing --artifact for revision label(s): {missing}")
    return artifacts


def main(argv: Sequence[str] | None = None) -> int:
    runner_arguments, suite_arguments = split_arguments(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(runner_arguments)
    temporary: tempfile.TemporaryDirectory[str] | None = None
    worktrees: list[pathlib.Path] = []
    execution: dict[str, Any] | None = None
    execution_path: pathlib.Path | None = None
    try:
        if args.repetitions < 1:
            raise RevisionBenchmarkError("--repetitions must be positive")
        validate_suite_arguments(suite_arguments)
        if args.require_clean and git_output(["status", "--porcelain"]):
            raise RevisionBenchmarkError("the stable benchmark harness worktree is dirty")

        revisions = resolve_revisions(args.revisions)
        baseline = select_baseline(revisions, args.baseline)
        build_spec = SUITE_BUILD_SPECS[args.suite]
        provided_binaries = supplied_artifacts(args.artifact, revisions)
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = (
            args.output_dir or pathlib.Path("benchmarks/results") / f"revisions-{timestamp}"
        ).resolve()
        if output_dir.exists() and any(output_dir.iterdir()):
            raise RevisionBenchmarkError(f"output directory is not empty: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        artifact_dir = output_dir / "artifacts"
        bencher_dir = output_dir / "bencher"
        artifact_dir.mkdir()
        bencher_dir.mkdir()

        if args.keep_worktrees:
            worktree_root = args.keep_worktrees.resolve()
            worktree_root.mkdir(parents=True, exist_ok=True)
        else:
            temporary = tempfile.TemporaryDirectory(prefix="casita-benchmark-revisions-")
            worktree_root = pathlib.Path(temporary.name)

        harness_revision = git_output(["rev-parse", "HEAD"])
        schedule = [
            [revision.label for revision in rotated_order(revisions, round_index)]
            for round_index in range(args.repetitions)
        ]
        execution = {
            "result_schema": "casita.benchmark-revision-execution.v1",
            "status": "building",
            "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "harness_revision": harness_revision,
            "harness_dirty": bool(git_output(["status", "--porcelain"])),
            "suite": args.suite,
            "build": {
                "cargo_arguments": list(build_spec.cargo_arguments),
                "artifact_option": build_spec.artifact_option,
                "artifact_name": build_spec.artifact_name,
            },
            "suite_arguments": list(suite_arguments),
            "rounds": args.repetitions,
            "build_cache": (
                "explicit-prebuilt-revision-artifacts"
                if provided_binaries
                else "single-worktree-cargo-target-with-copied-revision-artifacts"
            ),
            "baseline_label": baseline,
            "schedule": schedule,
            "revisions": [dataclasses.asdict(revision) for revision in revisions],
            "artifacts": {revision.label: [] for revision in revisions},
            "builds": {},
        }
        execution_path = output_dir / "execution.json"
        write_document(execution_path, execution)

        binaries: dict[str, pathlib.Path] = {}
        if provided_binaries:
            binaries.update(provided_binaries)
        else:
            shared_target = worktree_root / "cargo-target"
            binary_root = worktree_root / "revision-binaries"
            worktree = create_worktree(worktree_root, revisions[0])
            worktrees.append(worktree)
            for revision_index, revision in enumerate(revisions):
                print(f"building {revision.label} ({revision.short})", flush=True)
                if revision_index:
                    checkout_worktree(worktree, revision)
                binaries[revision.label] = build_artifact(
                    worktree,
                    shared_target,
                    binary_root / revision.label / build_spec.artifact_name,
                    build_spec,
                )
        for revision in revisions:
            execution["builds"][revision.label] = {
                "bytes": binaries[revision.label].stat().st_size,
                "sha256": file_sha256(binaries[revision.label]),
                "source": "provided" if provided_binaries else "built",
            }
            write_document(execution_path, execution)

        execution["status"] = "running"
        write_document(execution_path, execution)
        had_failures = False
        for round_index in range(args.repetitions):
            for revision in rotated_order(revisions, round_index):
                print(
                    f"round {round_index + 1}/{args.repetitions}: "
                    f"{revision.label} ({revision.short})",
                    flush=True,
                )
                output = artifact_dir / f"{revision.label}-round-{round_index + 1:03d}.json"
                exit_code = invoke_suite(
                    args.suite,
                    build_spec,
                    suite_arguments,
                    revision,
                    binaries[revision.label],
                    output,
                )
                if not output.is_file():
                    raise RevisionBenchmarkError(
                        f"suite {args.suite} exited {exit_code} without writing {output}"
                    )
                stamp_result_revision(
                    output,
                    revision,
                    harness_revision,
                    bool(execution["harness_dirty"]),
                )
                execution["artifacts"][revision.label].append(str(output.relative_to(output_dir)))
                write_document(execution_path, execution)
                had_failures = had_failures or exit_code != 0

        manifest = dashboard.load_manifest(ROOT / "benchmarks" / "manifest.json")
        registry = metrics.load_metric_registry(manifest)
        normalized_sets = []
        for revision in revisions:
            paths = [output_dir / path for path in execution["artifacts"][revision.label]]
            normalized_sets.append((revision.label, comparison.normalize_many(paths)))
        execution["status"] = "completed-with-failures" if had_failures else "completed"
        execution["series"] = "series.json"
        execution["report"] = "series.md"
        series = comparison.revision_series(normalized_sets, registry, baseline)
        series["execution"] = execution
        for revision_result in series["revisions"]:
            label = revision_result["label"]
            bencher_path = bencher_dir / f"{label}.bmf.json"
            comparison.write_json(
                bencher_path,
                metrics.bmf_document([revision_result["normalized"]], registry),
            )
            revision_result["artifacts"] = execution["artifacts"][label]
            revision_result["bencher_output"] = str(bencher_path.relative_to(output_dir))

        series_path = output_dir / "series.json"
        report_path = output_dir / "series.md"
        write_document(series_path, series)
        common.write_atomic(report_path, comparison.render_series_report(series))
        write_document(execution_path, execution)
        print(f"revision series: {series_path}")
        print(f"report: {report_path}")
        return 1 if had_failures else 0
    except (
        RevisionBenchmarkError,
        common.BenchmarkError,
        comparison.ComparisonError,
        dashboard.DashboardError,
        metrics.MetricRegistryError,
        subprocess.CalledProcessError,
        OSError,
        json.JSONDecodeError,
    ) as error:
        if execution is not None and execution_path is not None:
            execution["status"] = "failed"
            execution["error"] = str(error)
            try:
                write_document(execution_path, execution)
            except OSError:
                pass
        print(f"error: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        if execution is not None and execution_path is not None:
            execution["status"] = "interrupted"
            try:
                write_document(execution_path, execution)
            except OSError:
                pass
        print("error: benchmark interrupted", file=sys.stderr)
        return 130
    finally:
        if temporary is not None:
            for worktree in reversed(worktrees):
                remove_worktree(worktree)
            temporary.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
