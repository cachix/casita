import json
import pathlib
import tempfile
import unittest
from unittest import mock

from benchmarks import revisions


def spec(label: str, digit: str) -> revisions.RevisionSpec:
    commit = digit * 40
    return revisions.RevisionSpec(label, label, label, commit)


def raw_result(revision: str) -> dict:
    return {
        "result_schema": "casita.repository-e2e.v1",
        "suite_id": "repository-e2e",
        "schema_version": 1,
        "environment": {"casita_revision": revision, "casita_worktree_dirty": False},
        "configuration": {"profile": "smoke"},
        "corpora": {"small-files": {"base_bytes": 100}},
        "samples": [],
        "aggregates": [
            {
                "corpus": "small-files",
                "cache_policy": "warm",
                "operation": "checkout",
                "implementation": "casita",
                "samples": 1,
                "median_wall_seconds": int(revision[0], 16),
                "p95_wall_seconds": int(revision[0], 16),
                "median_max_rss_bytes": 1024,
                "median_throughput_bytes_per_second": 100,
                "median_repository_allocated_bytes": 2048,
                "median_storage_metrics": {},
            }
        ],
    }


class RevisionArgumentTests(unittest.TestCase):
    def test_build_features_follow_each_revisions_target_declaration(self):
        with tempfile.TemporaryDirectory() as directory:
            worktree = pathlib.Path(directory)
            manifest = worktree / "Cargo.toml"
            for suite, kind, target, base in [
                ("gix-odb", "bench", "gix_odb", "git"),
                ("s3-path-transfer", "example", "s3_path_transfer", "s3,ssh"),
                ("pack-cache-network", "example", "pack_cache_network", "s3,ssh"),
            ]:
                build = revisions.SUITE_BUILD_SPECS[suite]
                for required, expected in [([], base), (["experimental"], base + ",experimental")]:
                    manifest.write_text(f'[[{kind}]]\nname = "{target}"\nrequired-features = {json.dumps(required)}\n')
                    command = revisions.cargo_build_command(worktree, build)
                    self.assertEqual(command[command.index("--features") + 1], expected)

    def test_every_supported_suite_has_an_explicit_build_contract(self):
        self.assertEqual(
            set(revisions.SUPPORTED_SUITES),
            {
                "metadata-collection", "output-import", "filesystem-outputs", "filesystem-reuse",
                "mutation-catalog", "mutation-rotation", "catalog-wal", "cleanup-batches", "catalog-marking", "held-catalog-gc",
                "memory-snapshots",
                "memory-publication",
                "memory-index-lifecycle",
                "metadata-scan",
                "metadata-primitives",
                "metadata-kv",
                "metadata-batch",
                "decoded-seek-replay", "object-reads", "snapshot-connections", "reader-coordination", "durable-ledger", "ledger-boundaries",
                "fsck",
                "repository",
                "ingest-concurrency",
                "ingest-scheduling",
                "git-ingest-concurrency",
                "git-ingest-scheduling",
                "chunk-upload-completion", "chunk-manifest-stream", "chunk-hash-batch",
                "git-import-profile", "git-closure-import", "git-closure-import-small-files", "git-closure-audit", "git-blob-file", "git-verified-stream", "pin-growth", "small-blob-pins",
                "git-fetch-s3", "git-fetch-local", "git-pack-cached",
                "git-pack-delayed", "git-pack-boundary",
                "nixpkgs",
                "cdcs-corpus",
                "pack-limits",
                "pack-index",
                "catalog-index",
                "scoped-catalog",
                "pack-gc",
                "s3-pack",
                "s3-pack-index",
                "s3-pack-gc",
                "s3-path-transfer",
                "graph-traversal",
                "git-scale",
                "gix-odb",
                "history-scale",
                "pack-cache-scale",
                "pack-cache-network",
                "pack-fragmentation",
                "s3-fragmentation",
                "s3-read-planning",
                "s3-fetch-pipeline",
                "s3-fetch-lookahead",
                "network-scale",
            },
        )
        for name, build in revisions.SUITE_BUILD_SPECS.items():
            with self.subTest(name=name):
                self.assertTrue(build.cargo_arguments)
                self.assertIn(build.artifact_option, revisions.CONTROLLED_SUITE_OPTIONS)
                discovery_modes = sum(
                    value is not None
                    for value in (
                        build.relative_artifact,
                        build.cargo_json_bench,
                        build.cargo_json_test,
                    )
                )
                self.assertEqual(discovery_modes, 1)

    def test_git_scale_build_includes_smart_http_server(self):
        self.assertIn(
            "cli,git-http",
            revisions.SUITE_BUILD_SPECS["git-scale"].cargo_arguments,
        )

    def test_labeled_revision_and_invalid_label(self):
        self.assertEqual(revisions.split_revision_argument("before=HEAD~2"), ("before", "HEAD~2"))
        self.assertEqual(revisions.split_revision_argument("HEAD"), (None, "HEAD"))
        with self.assertRaisesRegex(revisions.RevisionBenchmarkError, "LABEL=GIT_REVISION"):
            revisions.split_revision_argument("bad label=HEAD")

    def test_revisions_must_resolve_to_distinct_commits(self):
        with mock.patch.object(revisions, "resolve_commit", return_value="a" * 40):
            with self.assertRaisesRegex(revisions.RevisionBenchmarkError, "duplicate commit"):
                revisions.resolve_revisions(["a", "b"])

    def test_rotation_balances_every_revision_across_rounds(self):
        values = [spec("a", "a"), spec("b", "b"), spec("c", "c")]
        self.assertEqual(
            [[item.label for item in revisions.rotated_order(values, index)] for index in range(3)],
            [["a", "b", "c"], ["b", "c", "a"], ["c", "a", "b"]],
        )

    def test_suite_cannot_override_or_mislabel_controlled_outputs(self):
        for argument in (
            "--repetitions=10",
            "--output",
            "--casita-bin=/tmp/other",
            "--helper=/tmp/other",
            "--benchmark-bin=/tmp/other",
            "--probe-binary=/tmp/other",
            "--casita=/tmp/other",
        ):
            with self.assertRaisesRegex(revisions.RevisionBenchmarkError, "controlled"):
                revisions.validate_suite_arguments([argument])

    def test_argument_separator_preserves_suite_options(self):
        runner, suite = revisions.split_arguments(
            ["a", "b", "--repetitions", "4", "--", "--profile", "standard"]
        )
        self.assertEqual(runner, ["a", "b", "--repetitions", "4"])
        self.assertEqual(suite, ["--profile", "standard"])

    def test_supplied_artifacts_require_one_existing_path_per_label(self):
        selected = [spec("before", "a"), spec("after", "b")]
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            before = root / "before"
            after = root / "after"
            before.write_bytes(b"before")
            after.write_bytes(b"after")
            artifacts = revisions.supplied_artifacts(
                [f"before={before}", f"after={after}"], selected
            )
            self.assertEqual(artifacts, {"before": before.resolve(), "after": after.resolve()})
            with self.assertRaisesRegex(revisions.RevisionBenchmarkError, "missing"):
                revisions.supplied_artifacts([f"before={before}"], selected)


class RevisionRunnerTests(unittest.TestCase):
    def test_shared_target_build_copies_an_immutable_revision_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            target = root / "shared-target"
            destination = root / "binaries" / "before" / "casita"

            build_spec = revisions.SUITE_BUILD_SPECS["repository"]

            def build(_command, _cwd, _environment):
                binary = target / "release" / "casita"
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b"revision binary")

            with mock.patch.object(revisions, "run_checked", side_effect=build):
                built = revisions.build_artifact(
                    root / "worktree", target, destination, build_spec
                )

            self.assertEqual(built, destination.resolve())
            self.assertEqual(destination.read_bytes(), b"revision binary")

    def test_interrupted_run_records_terminal_status(self):
        selected = [spec("before", "a"), spec("after", "b")]
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            before = root / "before"
            after = root / "after"
            before.write_bytes(b"before")
            after.write_bytes(b"after")
            output = root / "output"
            with (
                mock.patch.object(revisions, "resolve_revisions", return_value=selected),
                mock.patch.object(
                    revisions,
                    "git_output",
                    side_effect=lambda arguments: "" if arguments[0] == "status" else "f" * 40,
                ),
                mock.patch.object(revisions, "invoke_suite", side_effect=KeyboardInterrupt),
            ):
                code = revisions.main(
                    [
                        "before", "after",
                        "--output-dir", str(output),
                        "--artifact", f"before={before}",
                        "--artifact", f"after={after}",
                    ]
                )
            self.assertEqual(code, 130)
            self.assertEqual(json.loads((output / "execution.json").read_text())["status"], "interrupted")

    def test_gix_build_discovers_and_copies_hashed_bench_executable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "target/release/deps/gix_odb-deadbeef"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"gix benchmark")
            destination = root / "binaries/gix_odb"
            artifact = json.dumps(
                {
                    "reason": "compiler-artifact",
                    "target": {"name": "gix_odb", "kind": ["bench"]},
                    "executable": str(source),
                }
            )
            completed = mock.Mock(returncode=0, stdout=artifact, stderr="")
            with mock.patch.object(
                revisions.subprocess, "run", return_value=completed
            ) as cargo:
                built = revisions.build_artifact(
                    root / "worktree",
                    root / "target",
                    destination,
                    revisions.SUITE_BUILD_SPECS["gix-odb"],
                )
            self.assertEqual(built, destination.resolve())
            self.assertEqual(destination.read_bytes(), b"gix benchmark")
            self.assertEqual(cargo.call_args.kwargs["stdout"], revisions.subprocess.PIPE)
            self.assertNotIn("stderr", cargo.call_args.kwargs)

    def test_result_stamping_adds_revision_to_legacy_s3_schema(self):
        revision = spec("after", "b")
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "result.json"
            path.write_text(json.dumps({"result_schema": "casita.s3-pack-index.v5"}))
            revisions.stamp_result_revision(path, revision, "a" * 40, True)
            environment = json.loads(path.read_text())["environment"]
        self.assertEqual(environment["casita_revision"], revision.commit)
        self.assertFalse(environment["casita_worktree_dirty"])
        self.assertEqual(environment["harness_revision"], "a" * 40)
        self.assertTrue(environment["harness_worktree_dirty"])

    def test_specialized_suite_receives_its_artifact_without_repository_flags(self):
        from benchmarks import cli

        revision = spec("after", "b")
        entry = {"id": "s3-pack-index", "kind": "module", "target": "unused"}
        with (
            mock.patch.object(cli, "entrypoints", return_value=[entry]),
            mock.patch.object(cli, "run_entrypoint", return_value=0) as run,
        ):
            exit_code = revisions.invoke_suite(
                "s3-pack-index",
                revisions.SUITE_BUILD_SPECS["s3-pack-index"],
                ["--files", "8"],
                revision,
                pathlib.Path("/tmp/pack-index-helper"),
                pathlib.Path("/tmp/result.json"),
            )
        self.assertEqual(exit_code, 0)
        arguments = run.call_args.args[1]
        self.assertIn("--helper", arguments)
        self.assertNotIn("--casita-bin", arguments)
        self.assertNotIn("--casita-revision", arguments)

    def test_suite_capabilities_control_injected_arguments(self):
        from benchmarks import cli

        revision = spec("after", "b")
        entry = {"id": "graph-traversal", "kind": "module", "target": "unused"}
        with (
            mock.patch.object(cli, "entrypoints", return_value=[entry]),
            mock.patch.object(cli, "run_entrypoint", return_value=0) as run,
        ):
            revisions.invoke_suite(
                "graph-traversal",
                revisions.SUITE_BUILD_SPECS["graph-traversal"],
                ["--profile", "smoke"],
                revision,
                pathlib.Path("/tmp/casita"),
                pathlib.Path("/tmp/result.json"),
            )
        arguments = run.call_args.args[1]
        self.assertIn("--repetitions", arguments)
        self.assertIn("--casita", arguments)
        self.assertNotIn("--no-build", arguments)
        self.assertNotIn("--report", arguments)

    def test_runner_writes_interleaved_series_and_one_bmf_per_revision(self):
        selected = [spec("a", "a"), spec("b", "b"), spec("c", "c")]

        def build(_worktree, _target, destination, _spec):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"test binary")
            return destination

        def invoke(_suite, _spec, _forwarded, revision, _binary, output):
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(raw_result(revision.commit)))
            output.with_suffix(".md").write_text("report\n")
            return 0

        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "output"
            with (
                mock.patch.object(revisions, "resolve_revisions", return_value=selected),
                mock.patch.object(
                    revisions,
                    "git_output",
                    side_effect=lambda arguments: "" if arguments[0] == "status" else "f" * 40,
                ),
                mock.patch.object(
                    revisions,
                    "create_worktree",
                    side_effect=lambda root, revision: root / revision.label,
                ) as create_worktree,
                mock.patch.object(revisions, "checkout_worktree") as checkout_worktree,
                mock.patch.object(
                    revisions,
                    "build_artifact",
                    side_effect=build,
                ),
                mock.patch.object(revisions, "invoke_suite", side_effect=invoke),
                mock.patch.object(revisions, "remove_worktree"),
            ):
                exit_code = revisions.main(
                    [
                        "a",
                        "b",
                        "c",
                        "--repetitions",
                        "3",
                        "--output-dir",
                        str(output),
                        "--",
                        "--profile",
                        "smoke",
                    ]
                )

            self.assertEqual(exit_code, 0)
            create_worktree.assert_called_once_with(mock.ANY, selected[0])
            self.assertEqual(
                [call.args[1] for call in checkout_worktree.call_args_list],
                selected[1:],
            )
            execution = json.loads((output / "execution.json").read_text())
            series = json.loads((output / "series.json").read_text())
            self.assertEqual(execution["status"], "completed")
            self.assertEqual(set(execution["builds"]), {"a", "b", "c"})
            self.assertEqual({row["bytes"] for row in execution["builds"].values()}, {11})
            self.assertEqual(
                execution["schedule"],
                [["a", "b", "c"], ["b", "c", "a"], ["c", "a", "b"]],
            )
            self.assertEqual([entry["rounds"] for entry in series["revisions"]], [3, 3, 3])
            self.assertEqual(len(list((output / "artifacts").glob("*.json"))), 9)
            self.assertEqual(len(list((output / "bencher").glob("*.bmf.json"))), 3)
            self.assertTrue((output / "series.md").is_file())


if __name__ == "__main__":
    unittest.main()
