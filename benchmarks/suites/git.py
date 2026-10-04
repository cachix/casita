#!/usr/bin/env python3
"""Native-Git scale benchmarks for Casita.

The general benchmark suite measures filesystem-shaped snapshots.  This
runner instead creates deterministic Git histories through ``fast-import`` and
measures the native Git path end to end: import, immutable-view bind, clone,
incremental import, and incremental fetch.  Setup and validation are outside
the timed regions.

The large profiles are intentionally opt-in.  They write substantial data and
exist to find memory, object-count, pack-size, and full-history traversal
cliffs rather than to make a quick comparison chart.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pathlib
import random
import selectors
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from typing import BinaryIO, Sequence

from benchmarks.suites import repository as common


SCHEMA_VERSION = 1
SOURCE_DATE_EPOCH = 1_700_000_000
OPERATIONS = (
    "cold-import",
    "unchanged-import",
    "bind",
    "full-clone",
    "git-full-clone",
    "incremental-import",
    "incremental-bind",
    "incremental-fetch",
    "git-incremental-fetch",
)


@dataclasses.dataclass(frozen=True)
class GitScale:
    commits: int
    files: int
    blob_bytes: int
    changes_per_commit: int
    incremental_commits: int
    delta_friendly: bool
    incompressible: bool = False

    @property
    def base_logical_blob_bytes(self) -> int:
        return self.files * self.blob_bytes + max(0, self.commits - 1) * self.changes_per_commit * self.blob_bytes

    @property
    def logical_blob_bytes(self) -> int:
        incremental = self.incremental_commits * self.changes_per_commit * self.blob_bytes
        return self.base_logical_blob_bytes + incremental


SCALES: dict[str, dict[str, GitScale]] = {
    "smoke": {
        "many-objects": GitScale(32, 128, 1024, 8, 2, False),
        "delta-heavy": GitScale(48, 8, 64 * 1024, 2, 3, True),
        "pack-heavy": GitScale(16, 16, 64 * 1024, 8, 2, False, True),
        "wide-tree": GitScale(2, 2_048, 512, 64, 1, False),
    },
    "standard": {
        "many-objects": GitScale(2_000, 10_000, 1024, 16, 20, False),
        "delta-heavy": GitScale(2_000, 64, 256 * 1024, 4, 20, True),
        "pack-heavy": GitScale(256, 256, 256 * 1024, 32, 4, False, True),
        "wide-tree": GitScale(4, 100_000, 1024, 1024, 1, False),
    },
    # The delta-heavy shape exercises roughly 30 GiB of logical versions that
    # pack efficiently. The independent pack-heavy shape uses deterministic
    # incompressible bytes and targets more than 30 GiB of physical pack data.
    "huge": {
        "many-objects": GitScale(300_000, 1_000_000, 2048, 32, 100, False),
        "delta-heavy": GitScale(7_680, 64, 1024 * 1024, 4, 40, True),
        "pack-heavy": GitScale(256, 1024, 1024 * 1024, 128, 4, False, True),
        "wide-tree": GitScale(4, 5_000_000, 256, 4096, 1, False),
    },
}


class GitScaleError(RuntimeError):
    pass


def git_env() -> dict[str, str]:
    return {
        **os.environ,
        "TZ": "UTC",
        "LC_ALL": "C",
        # Prevent detached auto-maintenance from racing the immediate fsck
        # validation after fetch. The race can transiently leave a multi-pack
        # index naming a pack that a background rewrite has just replaced.
        "GIT_CONFIG_COUNT": "3",
        "GIT_CONFIG_KEY_0": "gc.auto",
        "GIT_CONFIG_VALUE_0": "0",
        "GIT_CONFIG_KEY_1": "maintenance.auto",
        "GIT_CONFIG_VALUE_1": "false",
        "GIT_CONFIG_KEY_2": "fetch.writeCommitGraph",
        "GIT_CONFIG_VALUE_2": "false",
        "GIT_CONFIG_NOSYSTEM": "1",
    }


def git(git_bin: str, repository: pathlib.Path, *args: str) -> list[str]:
    return [git_bin, f"--git-dir={repository}", *args]


def write_fast_import_data(stream: BinaryIO, payload: bytes) -> None:
    stream.write(f"data {len(payload)}\n".encode())
    stream.write(payload)
    stream.write(b"\n")


def blob_payload(
    file_index: int,
    version: int,
    size: int,
    delta_friendly: bool,
    incompressible: bool = False,
) -> bytes:
    header = f"casita-git-scale file={file_index} version={version}\n".encode()
    if incompressible:
        seed = int.from_bytes(
            hashlib.sha256(f"random:{file_index}:{version}".encode()).digest()[:8],
            "big",
        )
        body = random.Random(seed).randbytes(size)
    elif delta_friendly:
        stable = hashlib.sha256(f"stable:{file_index}".encode()).hexdigest().encode() + b"\n"
        body = (stable * (size // len(stable) + 1))[:size]
    else:
        block = hashlib.sha256(f"blob:{file_index}:{version}".encode()).digest()
        body = (block * (size // len(block) + 1))[:size]
    return (header + body[len(header) :])[:size]


def changed_files(scale: GitScale, commit_index: int) -> range | list[int]:
    if commit_index == 0:
        return range(scale.files)
    count = min(scale.files, scale.changes_per_commit)
    start = (commit_index * 104_729) % scale.files
    return [(start + offset * 65_537) % scale.files for offset in range(count)]


def append_history(
    git_bin: str,
    repository: pathlib.Path,
    scale: GitScale,
    *,
    first_commit: int,
    commit_count: int,
) -> None:
    if commit_count < 1:
        return
    parent = None
    if first_commit:
        parent = common.run_checked(git(git_bin, repository, "rev-parse", "refs/heads/main"), env=git_env()).strip()
    process = subprocess.Popen(
        git(git_bin, repository, "fast-import", "--quiet", "--date-format=raw"),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=git_env(),
    )
    assert process.stdin is not None
    stream = process.stdin
    mark = 1
    previous_commit_mark: int | None = None
    try:
        for commit_index in range(first_commit, first_commit + commit_count):
            modifications: list[tuple[int, int]] = []
            for file_index in changed_files(scale, commit_index):
                blob_mark = mark
                mark += 1
                stream.write(b"blob\n")
                stream.write(f"mark :{blob_mark}\n".encode())
                write_fast_import_data(
                    stream,
                    blob_payload(
                        file_index,
                        commit_index,
                        scale.blob_bytes,
                        scale.delta_friendly,
                        scale.incompressible,
                    ),
                )
                modifications.append((file_index, blob_mark))

            commit_mark = mark
            mark += 1
            timestamp = SOURCE_DATE_EPOCH + commit_index
            stream.write(b"commit refs/heads/main\n")
            stream.write(f"mark :{commit_mark}\n".encode())
            stream.write(
                f"author Casita Benchmark <benchmark@invalid> {timestamp} +0000\n"
                f"committer Casita Benchmark <benchmark@invalid> {timestamp} +0000\n".encode()
            )
            write_fast_import_data(stream, f"deterministic commit {commit_index}\n".encode())
            if previous_commit_mark is not None:
                stream.write(f"from :{previous_commit_mark}\n".encode())
            elif parent is not None:
                stream.write(f"from {parent}\n".encode())
            for file_index, blob_mark in modifications:
                stream.write(f"M 100644 :{blob_mark} files/{file_index:08d}.dat\n".encode())
            stream.write(b"\n")
            previous_commit_mark = commit_mark
        stream.write(b"done\n")
        stream.close()
        process.stdin = None
        stdout, stderr = process.communicate()
    except BaseException:
        process.kill()
        process.wait()
        raise
    if process.returncode != 0:
        raise GitScaleError(
            f"git fast-import failed ({process.returncode})\n"
            f"stdout:\n{stdout.decode(errors='replace')[-4000:]}\n"
            f"stderr:\n{stderr.decode(errors='replace')[-4000:]}"
        )


def create_source(git_bin: str, repository: pathlib.Path, scale: GitScale) -> None:
    common.run_checked(
        [git_bin, "init", "--quiet", "--bare", "--initial-branch", "main", str(repository)],
        env=git_env(),
    )
    for key, value in (
        ("gc.auto", "0"),
        ("gc.autoDetach", "false"),
        ("maintenance.auto", "false"),
        ("maintenance.autoDetach", "false"),
        ("core.logAllRefUpdates", "false"),
    ):
        common.run_checked(git(git_bin, repository, "config", key, value), env=git_env())
    append_history(git_bin, repository, scale, first_commit=0, commit_count=scale.commits)
    common.run_checked(git(git_bin, repository, "fsck", "--full", "--strict", "--no-progress"), env=git_env())


def source_metrics(git_bin: str, repository: pathlib.Path) -> dict[str, int]:
    objects = int(
        common.run_checked(git(git_bin, repository, "rev-list", "--objects", "--all", "--count"), env=git_env()).strip()
    )
    packs = list((repository / "objects" / "pack").glob("*.pack"))
    indexes = list((repository / "objects" / "pack").glob("*.idx"))
    count_objects: dict[str, int] = {}
    for line in common.run_checked(git(git_bin, repository, "count-objects", "-v"), env=git_env()).splitlines():
        name, separator, value = line.partition(": ")
        if separator and value.isdigit():
            count_objects[name] = int(value)
    return {
        "reachable_objects": objects,
        "pack_count": len(packs),
        "pack_bytes": sum(path.stat().st_size for path in packs),
        "index_bytes": sum(path.stat().st_size for path in indexes),
        "loose_objects": count_objects.get("count", 0),
        "loose_allocated_bytes": count_objects.get("size", 0) * 1024,
    }


def parse_import_metrics(stdout: str) -> dict[str, int | str]:
    metrics: dict[str, int | str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition(" ")
        if not separator or key not in {"view", "objects", "revision"}:
            continue
        metrics[key] = int(value) if key == "objects" else value
    return metrics


def process_peak_rss(pid: int) -> int | None:
    status = pathlib.Path(f"/proc/{pid}/status")
    if not status.exists():
        return None
    for line in status.read_text(errors="replace").splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    return None


@dataclasses.dataclass
class GitServer:
    process: subprocess.Popen[str]
    url: str
    bind_seconds: float

    def stop(self) -> dict[str, int | None]:
        peak = process_peak_rss(self.process.pid)
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        return {"server_peak_rss_bytes": peak, "server_exit_code": self.process.returncode}


def start_server(
    casita_bin: pathlib.Path,
    repository: pathlib.Path,
    timeout: float,
    max_pack_bytes: int,
    pack_compression_level: int,
) -> GitServer:
    argv = [
        str(casita_bin),
        "--repository",
        str(repository),
        "git",
        "serve",
        "scale",
        "--listen",
        "127.0.0.1:0",
        "--max-pack-bytes",
        str(max_pack_bytes),
        "--pack-compression-level",
        str(pack_compression_level),
    ]
    started = time.perf_counter_ns()
    process = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=git_env(),
        bufsize=1,
    )
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    events = selector.select(timeout)
    selector.close()
    if not events:
        process.kill()
        _, stderr = process.communicate()
        raise GitScaleError(f"Casita Git service did not bind within {timeout}s: {stderr[-4000:]}")
    url = process.stdout.readline().strip()
    elapsed = (time.perf_counter_ns() - started) / 1_000_000_000
    if process.poll() is not None or not url.startswith("http://"):
        _, stderr = process.communicate()
        raise GitScaleError(f"Casita Git service failed to bind: stdout={url!r} stderr={stderr[-4000:]}")
    return GitServer(process, url, elapsed)


def measured(
    operation: str,
    command: common.CommandSpec,
    workspace: pathlib.Path,
    repository_paths: Sequence[pathlib.Path],
) -> dict[str, object]:
    stdout_path = workspace / f"{operation}.stdout"
    stderr_path = workspace / f"{operation}.stderr"
    result: dict[str, object] = {
        "operation": operation,
        "command": command.display(),
    }
    result.update(common.measured_command(command, stdout_path, stderr_path))
    result["stdout"] = common.captured_output(stdout_path)
    result["stderr"] = common.captured_output(stderr_path)
    result["repository_usage"] = common.filesystem_usage(repository_paths)
    return result


def failed_sample(
    operation: str,
    command: str,
    error: Exception | str,
    repository_paths: Sequence[pathlib.Path],
) -> dict[str, object]:
    return {
        "operation": operation,
        "command": command,
        "status": "failed",
        "error": str(error),
        "repository_usage": common.filesystem_usage(repository_paths),
    }


def casita_import_command(
    casita_bin: pathlib.Path,
    repository: pathlib.Path,
    source: pathlib.Path,
    max_cached_pack_bytes: int | None,
) -> common.CommandSpec:
    arguments = [
        str(casita_bin),
        "--repository",
        str(repository),
        "import",
        "--importer",
        "git",
        str(source),
        "--git-view",
        "scale",
    ]
    if max_cached_pack_bytes is not None:
        arguments.extend(("--git-max-cached-pack-bytes", str(max_cached_pack_bytes)))
    return common.CommandSpec(
        [arguments],
        repository.parent,
        git_env(),
    )


def check_tip(git_bin: str, repository: pathlib.Path, revision: str, expected: str) -> None:
    actual = common.run_checked(git(git_bin, repository, "rev-parse", revision), env=git_env()).strip()
    if actual != expected:
        raise GitScaleError(f"{repository}: {revision} is {actual}, expected {expected}")
    common.run_checked(git(git_bin, repository, "fsck", "--full", "--strict", "--no-progress"), env=git_env())


def run_repetition(
    args: argparse.Namespace,
    root: pathlib.Path,
    scale: GitScale,
    repetition: int,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    workspace = root / f"repetition-{repetition:02d}"
    workspace.mkdir(parents=True)
    source = workspace / "source.git"
    create_source(args.git_bin, source, scale)
    base_tip = common.run_checked(git(args.git_bin, source, "rev-parse", "refs/heads/main"), env=git_env()).strip()
    casita_repository = workspace / "casita"
    samples: list[dict[str, object]] = []

    def condition(paths: Sequence[pathlib.Path]) -> None:
        common.apply_cache_policy(args.cache_policy, paths)

    condition([source])
    cold = measured(
        "cold-import",
        casita_import_command(
            args.casita_bin,
            casita_repository,
            source,
            None if args.omit_max_cached_pack_bytes else args.max_cached_pack_bytes,
        ),
        workspace,
        [casita_repository],
    )
    cold_stdout = cold["stdout"]
    assert isinstance(cold_stdout, dict)
    cold["metrics"] = parse_import_metrics(str(cold_stdout["text"]))
    samples.append(cold)

    condition([source, casita_repository])
    unchanged = measured(
        "unchanged-import",
        casita_import_command(
            args.casita_bin,
            casita_repository,
            source,
            None if args.omit_max_cached_pack_bytes else args.max_cached_pack_bytes,
        ),
        workspace,
        [casita_repository],
    )
    unchanged_stdout = unchanged["stdout"]
    assert isinstance(unchanged_stdout, dict)
    unchanged["metrics"] = parse_import_metrics(str(unchanged_stdout["text"]))
    samples.append(unchanged)

    condition([casita_repository])
    server = start_server(
        args.casita_bin,
        casita_repository,
        args.bind_timeout,
        args.max_pack_bytes,
        args.pack_compression_level,
    )
    bind_metrics = server.stop()
    samples.append(
        {
            "operation": "bind",
            "command": f"{args.casita_bin} --repository {casita_repository} git serve scale --listen 127.0.0.1:0 --max-pack-bytes {args.max_pack_bytes} --pack-compression-level {args.pack_compression_level}",
            "wall_seconds": server.bind_seconds,
            "metrics": bind_metrics,
            "repository_usage": common.filesystem_usage([casita_repository]),
        }
    )

    clone = workspace / "casita-clone.git"
    condition([casita_repository])
    server = start_server(
        args.casita_bin,
        casita_repository,
        args.bind_timeout,
        args.max_pack_bytes,
        args.pack_compression_level,
    )
    clone_command = common.CommandSpec(
        [[args.git_bin, "clone", "--quiet", "--bare", server.url, str(clone)]],
        workspace,
        git_env(),
    )
    clone_succeeded = False
    try:
        full_clone = measured("full-clone", clone_command, workspace, [clone])
        check_tip(args.git_bin, clone, "refs/heads/main", base_tip)
        clone_succeeded = True
    except (common.BenchmarkError, GitScaleError, OSError) as error:
        full_clone = failed_sample("full-clone", clone_command.display(), error, [clone])
    full_clone["metrics"] = {"bind_seconds": server.bind_seconds, **server.stop()}
    samples.append(full_clone)

    native_clone = workspace / "git-clone.git"
    condition([source])
    native_command = common.CommandSpec(
        [[args.git_bin, "clone", "--quiet", "--bare", "--no-local", str(source), str(native_clone)]],
        workspace,
        git_env(),
    )
    native = measured("git-full-clone", native_command, workspace, [native_clone])
    check_tip(args.git_bin, native_clone, "refs/heads/main", base_tip)
    samples.append(native)

    incremental = workspace / "incremental.git"
    common.run_checked(
        [args.git_bin, "clone", "--quiet", "--mirror", "--local", str(source), str(incremental)],
        env=git_env(),
    )
    append_history(
        args.git_bin,
        incremental,
        scale,
        first_commit=scale.commits,
        commit_count=scale.incremental_commits,
    )
    incremental_tip = common.run_checked(
        git(args.git_bin, incremental, "rev-parse", "refs/heads/main"), env=git_env()
    ).strip()
    condition([incremental, casita_repository])
    incremental_import = measured(
        "incremental-import",
        casita_import_command(
            args.casita_bin,
            casita_repository,
            incremental,
            None if args.omit_max_cached_pack_bytes else args.max_cached_pack_bytes,
        ),
        workspace,
        [casita_repository],
    )
    incremental_stdout = incremental_import["stdout"]
    assert isinstance(incremental_stdout, dict)
    incremental_import["metrics"] = parse_import_metrics(str(incremental_stdout["text"]))
    samples.append(incremental_import)

    condition([casita_repository])
    server = start_server(
        args.casita_bin,
        casita_repository,
        args.bind_timeout,
        args.max_pack_bytes,
        args.pack_compression_level,
    )
    incremental_bind_metrics = server.stop()
    samples.append(
        {
            "operation": "incremental-bind",
            "command": f"{args.casita_bin} --repository {casita_repository} git serve scale --listen 127.0.0.1:0 --max-pack-bytes {args.max_pack_bytes} --pack-compression-level {args.pack_compression_level}",
            "wall_seconds": server.bind_seconds,
            "metrics": incremental_bind_metrics,
            "repository_usage": common.filesystem_usage([casita_repository]),
        }
    )

    if clone_succeeded:
        condition([casita_repository, clone])
        server = start_server(
            args.casita_bin,
            casita_repository,
            args.bind_timeout,
            args.max_pack_bytes,
            args.pack_compression_level,
        )
        fetch_command = common.CommandSpec(
            [[args.git_bin, f"--git-dir={clone}", "fetch", "--quiet", server.url, "+refs/heads/main:refs/heads/main"]],
            workspace,
            git_env(),
        )
        try:
            incremental_fetch = measured("incremental-fetch", fetch_command, workspace, [clone])
            check_tip(args.git_bin, clone, "refs/heads/main", incremental_tip)
        except (common.BenchmarkError, GitScaleError, OSError) as error:
            incremental_fetch = failed_sample(
                "incremental-fetch", fetch_command.display(), error, [clone]
            )
        incremental_fetch["metrics"] = {
            "bind_seconds": server.bind_seconds,
            **server.stop(),
        }
    else:
        incremental_fetch = failed_sample(
            "incremental-fetch",
            "not run",
            "full-clone prerequisite failed",
            [clone],
        )
    samples.append(incremental_fetch)

    condition([incremental, native_clone])
    native_fetch_command = common.CommandSpec(
        [[args.git_bin, f"--git-dir={native_clone}", "fetch", "--quiet", str(incremental), "+refs/heads/main:refs/heads/main"]],
        workspace,
        git_env(),
    )
    native_fetch = measured("git-incremental-fetch", native_fetch_command, workspace, [native_clone])
    check_tip(args.git_bin, native_clone, "refs/heads/main", incremental_tip)
    samples.append(native_fetch)

    for sample in samples:
        sample["repetition"] = repetition
        sample.setdefault("status", "ok")
    corpus = {
        "base": source_metrics(args.git_bin, source),
        "incremental": source_metrics(args.git_bin, incremental),
        "base_tip": base_tip,
        "incremental_tip": incremental_tip,
        "base_logical_blob_bytes": scale.base_logical_blob_bytes,
        "logical_blob_bytes": scale.logical_blob_bytes,
    }
    return samples, corpus


def render_report(result: dict[str, object]) -> str:
    configuration = result["configuration"]
    assert isinstance(configuration, dict)
    samples = result["samples"]
    assert isinstance(samples, list)
    corpora = result["corpora"]
    assert isinstance(corpora, list) and corpora
    corpus = corpora[-1]
    assert isinstance(corpus, dict)
    base = corpus["base"]
    assert isinstance(base, dict)
    lines = [
        "# Casita native-Git scale benchmark",
        "",
        f"Profile: `{configuration['profile']}`; shape: `{configuration['shape']}`; cache: `{configuration['cache_policy']}`.",
        "",
        "## Corpus",
        "",
        "| Base logical blob bytes | Base Git pack bytes | Reachable objects |",
        "|---:|---:|---:|",
        "| {logical} | {pack} | {objects:,} |".format(
            logical=common.human_bytes(int(corpus.get("base_logical_blob_bytes", corpus["logical_blob_bytes"]))),
            pack=common.human_bytes(int(base.get("pack_bytes", 0))),
            objects=int(base.get("reachable_objects", 0)),
        ),
        "",
        "## Operations",
        "",
        "| Operation | Status | Repetition | Wall | Peak RSS | Server peak RSS | Repository allocated |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for sample in samples:
        assert isinstance(sample, dict)
        metrics = sample.get("metrics", {})
        assert isinstance(metrics, dict)
        usage = sample.get("repository_usage", {})
        assert isinstance(usage, dict)
        lines.append(
            "| {operation} | {status} | {repetition} | {wall} | {rss} | {server_rss} | {allocated} |".format(
                operation=sample["operation"],
                status=sample["status"],
                repetition=sample["repetition"],
                wall=f"{float(sample['wall_seconds']):.4f} s" if "wall_seconds" in sample else "—",
                rss=common.human_bytes(int(sample.get("max_rss_bytes", 0))) if sample.get("max_rss_bytes") else "—",
                server_rss=common.human_bytes(int(metrics["server_peak_rss_bytes"]))
                if metrics.get("server_peak_rss_bytes")
                else "—",
                allocated=common.human_bytes(int(usage.get("allocated_bytes", 0))),
            )
        )
    lines.extend(
        [
            "",
            "`git-*` rows are native Git baselines. Service bind is separated from clone/fetch, but the server peak RSS spans both bind and request handling.",
            "The huge profile is a cliff-finding workload, not a CI benchmark. Logical blob bytes count every generated Git blob version; base Git pack bytes are the measured physical-pack axis.",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(SCALES), default="smoke")
    parser.add_argument("--shape", choices=sorted(SCALES["smoke"]), default="many-objects")
    parser.add_argument("--cache-policy", choices=("warm", "cold"), default="warm")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--casita-bin", type=pathlib.Path, default=pathlib.Path("target/release/casita"))
    parser.add_argument("--git-bin", default=shutil.which("git") or "git")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--bind-timeout", type=float, default=300.0)
    parser.add_argument("--max-pack-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--max-cached-pack-bytes", type=int, default=8 * 1024**3)
    parser.add_argument(
        "--omit-max-cached-pack-bytes",
        action="store_true",
        help="use each revision's built-in cache limit (for historical binaries without the flag)",
    )
    parser.add_argument("--pack-compression-level", type=int, choices=range(10), default=6)
    parser.add_argument("--keep-work", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--report", type=pathlib.Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.repetitions < 1:
            raise GitScaleError("repetitions must be positive")
        if args.max_pack_bytes < 1:
            raise GitScaleError("--max-pack-bytes must be positive")
        if args.max_cached_pack_bytes < 0:
            raise GitScaleError("--max-cached-pack-bytes must be non-negative")
        if not args.no_build:
            profile = "debug" if "debug" in args.casita_bin.parts else "release"
            command = ["cargo", "build", "--features", "cli,git-http", "--bin", "casita"]
            if profile == "release":
                command.insert(2, "--release")
            print(f"building Casita: {shlex.join(command)}", flush=True)
            common.run_checked(command)
        args.casita_bin = args.casita_bin.resolve()
        if not args.casita_bin.exists():
            raise GitScaleError(f"Casita binary does not exist: {args.casita_bin}")
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        args.output = args.output or pathlib.Path("benchmarks/results") / f"git-scale-{timestamp}.json"
        args.report = args.report or args.output.with_suffix(".md")

        temporary: tempfile.TemporaryDirectory[str] | None = None
        if args.keep_work:
            work_root = args.keep_work.resolve()
            work_root.mkdir(parents=True, exist_ok=True)
        else:
            temporary = tempfile.TemporaryDirectory(prefix="casita-git-scale-")
            work_root = pathlib.Path(temporary.name)

        scale = SCALES[args.profile][args.shape]
        all_samples: list[dict[str, object]] = []
        corpora: list[dict[str, object]] = []
        for repetition in range(1, args.repetitions + 1):
            print(
                f"[{repetition}/{args.repetitions}] {args.profile}/{args.shape} "
                f"commits={scale.commits} files={scale.files} logical={common.human_bytes(scale.logical_blob_bytes)}",
                flush=True,
            )
            samples, corpus = run_repetition(args, work_root, scale, repetition)
            all_samples.extend(samples)
            corpora.append(corpus)

        result: dict[str, object] = {
            "result_schema": "casita.native-git.v1",
            "suite_id": "native-git",
            "schema_version": SCHEMA_VERSION,
            "environment": common.environment_metadata(work_root),
            "configuration": {
                "profile": args.profile,
                "shape": args.shape,
                "cache_policy": args.cache_policy,
                "repetitions": args.repetitions,
                "max_pack_bytes": args.max_pack_bytes,
                "max_cached_pack_bytes": args.max_cached_pack_bytes,
                "max_cached_pack_bytes_explicit": not args.omit_max_cached_pack_bytes,
                "pack_compression_level": args.pack_compression_level,
                "scale": dataclasses.asdict(scale),
                "argv": list(sys.argv if argv is None else [sys.argv[0], *argv]),
            },
            "tools": {
                "git": common.run_checked([args.git_bin, "--version"]).strip(),
                "casita": str(args.casita_bin),
            },
            "corpora": corpora,
            "samples": all_samples,
        }
        common.write_atomic(args.output, json.dumps(result, indent=2, sort_keys=True) + "\n")
        common.write_atomic(args.report, render_report(result))
        print(f"raw results: {args.output}")
        print(f"report: {args.report}")
        if temporary is not None:
            temporary.cleanup()
        return 1 if any(sample["status"] != "ok" for sample in all_samples) else 0
    except (GitScaleError, common.BenchmarkError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
