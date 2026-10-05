import json
import os
import pathlib
import sys
import tempfile
import unittest

from benchmarks.suites import git_closure_import as suite

FAKE_PROBE = r'''import json, os
count = int(os.environ["CASITA_GIT_CLOSURE_FILES"])
for operation in ["cold", "warm", "subtree-delta", "wide-delta"]:
    new, reused = {"cold": (count + 2, 0), "warm": (0, 1), "subtree-delta": (2, 1), "wide-delta": (2, count)}[operation]
    print(("test benchmark_git_closure_import ... " if operation == "cold" else "") + "git_closure_sample " + json.dumps(dict(operation=operation, files=count,
        packed=os.environ["CASITA_GIT_CLOSURE_PACKED"] == "1",
        max_buffered_bytes=int(os.environ["CASITA_GIT_CLOSURE_BYTES"]),
        backend=os.environ["CASITA_GIT_CLOSURE_BACKEND"],
        file_bytes=int(os.environ["CASITA_GIT_CLOSURE_FILE_BYTES"]),
        content=os.environ["CASITA_GIT_CLOSURE_CONTENT"],
        concurrency=int(os.environ["CASITA_GIT_CLOSURE_CONCURRENCY"]),
        root="fixture-root", wall_nanos=1000, imported_objects=new, reused_objects=reused,
        source_bytes=0 if operation == "warm" else 1,
        witness_policy=POLICY, blob_witnesses=BLOB_WITNESSES,
        correctness="exact imported/reused counts and exhaustive closure verification")))
print("test result: ok. 1 passed; 0 failed;")
'''
# Every imported blob keeps a witness: the cold tree's files plus one new
# leaf per delta.
STORED = 'count + {"cold": 0, "warm": 0, "subtree-delta": 1, "wide-delta": 2}[operation]'


def write_probe(path, policy='"derived-blobs"', blob_witnesses="0"):
    """A fake probe; its defaults reproduce a correct derived-blobs probe."""
    path.write_text("#!" + sys.executable + "\n"
                    + FAKE_PROBE.replace("POLICY", policy).replace("BLOB_WITNESSES", blob_witnesses))
    path.chmod(0o755)


class GitClosureBenchmarkTests(unittest.TestCase):
    def test_rejects_stale_build_manifests_and_mismatched_dependency_locks(self):
        import hashlib
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binaries = [root / "baseline", root / "candidate"]
            for i, binary in enumerate(binaries):
                binary.write_bytes(bytes([i]))
                pathlib.Path(str(binary) + ".build.json").write_text(json.dumps(dict(
                    executable_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    lockfile_sha256=str(i), features="native,git,experimental", default_features=False)))
            args = ["--probe-binary", str(binaries[1]), "--baseline-binary", str(binaries[0]),
                    "--no-build", "--output", str(root / "report.json")]
            with mock.patch.object(suite.common, "measured_command", side_effect=AssertionError("incompatible builds must not run")):
                with self.assertRaisesRegex(suite.common.BenchmarkError, "lockfile"):
                    suite.main(args)
                binaries[0].write_bytes(b"replaced executable")
                with self.assertRaisesRegex(suite.common.BenchmarkError, "fingerprint"):
                    suite.main(args)

    def test_automatic_build_allows_a_fresh_checkout_to_resolve_dependencies(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(suite.subprocess, "run", side_effect=RuntimeError("build reached")) as build:
                with self.assertRaisesRegex(RuntimeError, "build reached"):
                    suite.main(["--output", str(pathlib.Path(directory) / "result.json")])
            command = build.call_args.args[0]
            self.assertEqual(command[:2], ["cargo", "test"])
            self.assertNotIn("--offline", command)
            self.assertNotIn("--locked", command)
            self.assertIn("--no-default-features", command)
            self.assertEqual(command[command.index("--features") + 1], "native,git,experimental")

    def test_revision_build_matches_the_evaluator_library_features(self):
        from benchmarks import revisions
        spec = revisions.SUITE_BUILD_SPECS["git-closure-import"]
        arguments = spec.cargo_arguments
        self.assertIn("--no-default-features", arguments)
        self.assertEqual(arguments[arguments.index("--features") + 1], "native,git,experimental")

    def test_paired_summary_uses_matching_repetitions_and_rejects_different_roots(self):
        rows = []
        for repetition, seconds in enumerate([10, 20, 30, 40, 50]):
            for variant, scale in [("baseline", 1), ("candidate", 0.5)]:
                rows.append(dict(operation="cold", backend="local", files=3,
                    file_bytes=1024, content="random", packed=True, concurrency=4,
                    max_buffered_bytes=4096, repetition=repetition, variant=variant,
                    wall_seconds=seconds * scale, root="same root"))
        summary, = suite.summarize_pairs(rows)
        self.assertEqual(summary["pairs"], 5)
        self.assertEqual(summary["baseline_median_seconds"], 30)
        self.assertEqual(summary["candidate_median_seconds"], 15)
        self.assertEqual(summary["median_paired_reduction_percent"], 50)
        self.assertTrue(summary["enough_samples"])
        rows[-1]["root"] = "different root"
        with self.assertRaisesRegex(suite.common.BenchmarkError, "root"):
            suite.summarize_pairs(rows)

    def test_paired_summary_requires_nonempty_root_identities(self):
        rows = [dict(operation="cold", backend="local", files=3,
                     file_bytes=1024, content="random", packed=True, concurrency=4,
                     max_buffered_bytes=4096, repetition=0, variant=variant,
                     wall_seconds=seconds)
                for variant, seconds in [("baseline", 10), ("candidate", 5)]]
        for invalid in [None, "", 0]:
            with self.subTest(root=invalid):
                for row in rows:
                    row["root"] = invalid
                with self.assertRaisesRegex(suite.common.BenchmarkError, "root"):
                    suite.summarize_pairs(rows)
        for row in rows:
            del row["root"]
        with self.assertRaisesRegex(suite.common.BenchmarkError, "root"):
            suite.summarize_pairs(rows)

    def test_runs_requested_matrix_with_serial_libtest_sample_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            write_probe(probe)
            output = root / "result.json"
            self.assertEqual(suite.main([
                "--probe-binary", str(probe), "--no-build", "--output", str(output),
                "--counts", "3", "--max-buffered-bytes", "1024", "--layout", "packed",
                "--backend", "both", "--file-bytes", "1024,262144",
                "--concurrency", "1,4", "--content", "random",
            ]), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(len(report["samples"]), 32)
            self.assertEqual({(r["backend"], r["file_bytes"], r["concurrency"])
                              for r in report["samples"]},
                             {(backend, size, workers) for backend in ["memory", "local"]
                              for size in [1024, 262144] for workers in [1, 4]})
            baseline = root / "baseline"
            baseline.write_text(probe.read_text() + "\n# separate baseline executable\n")
            baseline.chmod(0o755)
            paired = root / "paired.json"
            original_affinity = os.sched_getaffinity(0) if hasattr(os, "sched_getaffinity") else None
            affinity_args = (["--cpu-affinity", str(min(original_affinity))]
                             if original_affinity else [])
            self.assertEqual(suite.main([
                "--probe-binary", str(probe), "--baseline-binary", str(baseline),
                "--no-build", "--output", str(paired), "--counts", "3",
                "--max-buffered-bytes", "1024", "--layout", "loose", "--backend", "local",
                "--file-bytes", "1024", "--concurrency", "1", "--repetitions", "2",
                *affinity_args,
            ]), 0)
            report = json.loads(paired.read_text())
            self.assertEqual([p["variant"] for p in report["processes"]],
                             ["baseline", "candidate", "candidate", "baseline"])
            self.assertEqual(len({a["sha256"] for a in report["artifacts"]}), 2)
            self.assertEqual(len(report["samples"]), 16)
            if original_affinity:
                self.assertEqual(report["environment"]["cpu_affinity"], [min(original_affinity)])
                self.assertEqual(os.sched_getaffinity(0), original_affinity)
            valid_probe = probe.read_text()
            for root_field in ["", "root=None, ", 'root="", ']:
                with self.subTest(root_field=root_field):
                    probe.write_text(valid_probe.replace('root="fixture-root", ', root_field))
                    with self.assertRaisesRegex(suite.common.BenchmarkError, "root"):
                        suite.main(["--probe-binary", str(probe), "--no-build",
                                    "--output", str(root / "invalid.json"),
                                    "--counts", "3", "--max-buffered-bytes", "1024",
                                    "--backend", "local", "--layout", "loose"])

    def test_blob_witnesses_must_match_the_probes_declared_witness_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            args = ["--probe-binary", str(probe), "--no-build", "--output", str(root / "r.json"),
                    "--counts", "3", "--max-buffered-bytes", "1024", "--backend", "memory", "--layout", "loose"]
            for declared, blob_witnesses, expected in [("derived-blobs", "0", [0, 0, 0, 0]),
                                                       ("stored-blobs", STORED, [3, 3, 4, 5])]:
                with self.subTest(declared=declared):
                    write_probe(probe, policy=f'"{declared}"', blob_witnesses=blob_witnesses)
                    self.assertEqual(suite.main(args), 0)
                    report = json.loads((root / "r.json").read_text())
                    self.assertEqual([row["blob_witnesses"] for row in report["samples"]], expected)
                    self.assertEqual(report["artifacts"][0]["witness_policy"], declared)
            # Witnessed blobs under derived-blobs; a delta blob left unwitnessed
            # under stored-blobs; or no count at all.
            for declared, blob_witnesses in [("derived-blobs", STORED), ("derived-blobs", "1"),
                                             ("stored-blobs", "count"), ("derived-blobs", "None")]:
                with self.subTest(declared=declared, blob_witnesses=blob_witnesses):
                    write_probe(probe, policy=f'"{declared}"', blob_witnesses=blob_witnesses)
                    with self.assertRaisesRegex(suite.common.BenchmarkError,
                                                f"under witness policy '{declared}' stored"):
                        suite.main(args)
            for policy in ['"unknown"', "None"]:
                with self.subTest(policy=policy):
                    write_probe(probe, policy=policy)
                    with self.assertRaisesRegex(suite.common.BenchmarkError, "unknown witness policy"):
                        suite.main(args)


if __name__ == "__main__":
    unittest.main()
