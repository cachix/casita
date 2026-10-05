import json
import pathlib
import sys
import tempfile
import unittest

from benchmarks.suites import git_closure_audit as suite

FAKE_PROBE = r'''import json, os
commits = int(os.environ["CASITA_GIT_AUDIT_COMMITS"])
registry = os.environ["CASITA_GIT_AUDIT_REGISTRY"]
objects = 3 * commits
batch = int(os.environ["CASITA_GIT_AUDIT_BATCH_OBJECTS"])
audits = None if registry == "builtin" else AUDITS
witnesses = objects if registry == "custom" else BUILTIN * commits
print("test benchmark_git_closure_audit ... git_closure_audit_sample " + json.dumps(dict(
    commits=commits, registry=registry,
    publication_batch_objects=batch,
    objects=objects, imported_objects=objects, reused_objects=0, warm_imported_objects=0,
    warm_source_bytes=0, link_audits=audits, root="fixture-root", wall_nanos=1000,
    witnesses=witnesses, witness_commits=-(-witnesses // batch), max_witness_batch=PEAK,
    witness_policy=POLICY,
    correctness="exact import counts, exhaustive closure verification and source-free warm reuse")))
print("test result: ok. 1 passed; 0 failed;")
'''


def write_probe(path, audits="objects", peak="min(batch, witnesses)", builtin="2", policy='"derived-blobs"'):
    """A fake probe; its defaults reproduce a correct derived-blobs probe."""
    path.write_text("#!" + sys.executable + "\n"
                    + FAKE_PROBE.replace("AUDITS", audits).replace("PEAK", peak)
                    .replace("BUILTIN", builtin).replace("POLICY", policy))
    path.chmod(0o755)


def arguments(probe, output, registry="custom", commits="4", batch="64"):
    return ["--probe-binary", str(probe), "--no-build", "--output", str(output),
            "--commits", commits, "--registry", registry, "--publication-batch-objects", batch]


class GitClosureAuditBenchmarkTests(unittest.TestCase):
    def test_revision_build_uses_the_custom_format_probe_and_evaluator_features(self):
        from benchmarks import revisions
        arguments = revisions.SUITE_BUILD_SPECS["git-closure-audit"].cargo_arguments
        self.assertIn("--no-default-features", arguments)
        self.assertEqual(arguments[arguments.index("--features") + 1], "native,git,experimental")
        self.assertEqual(arguments[arguments.index("--test") + 1], "git_closure_custom_formats")

    def test_smoke_profile_straddles_one_publication_batch(self):
        from unittest import mock
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            write_probe(probe)
            output = root / "smoke.json"
            with mock.patch.object(suite, "build_probe", side_effect=AssertionError("must not build")):
                self.assertEqual(suite.main(["--profile", "smoke", "--probe-binary", str(probe),
                                             "--no-build", "--output", str(output)]), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            artifact, = report["artifacts"]
            self.assertEqual(artifact["witness_policy"], "derived-blobs")
            self.assertEqual(report["configuration"]["commits"], [16, 64])
            self.assertEqual(report["configuration"]["publication_batch_objects"], [64, 4096])
            # 48 objects fit one 64-object witness batch; 192 objects need three.
            self.assertEqual(len(report["samples"]), 2 * 2 * 2)
            custom = [row for row in report["samples"] if row["registry"] == "custom"]
            self.assertTrue(all(row["link_audits_per_object"] == 1 for row in custom))
            builtin = [row for row in report["samples"] if row["registry"] == "builtin"]
            self.assertTrue(all(row["link_audits_per_object"] is None for row in builtin))
            self.assertTrue(all(row["max_witness_batch"] <= row["publication_batch_objects"]
                                for row in report["samples"]))
            spanning, = [row for row in custom if row["commits"] == 64
                         and row["publication_batch_objects"] == 64]
            self.assertEqual((spanning["witnesses"], spanning["witness_commits"]), (192, 3))

    def test_candidates_audit_each_object_exactly_once_and_builtins_report_no_audits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            write_probe(probe)
            self.assertEqual(suite.main(arguments(probe, root / "r.json")), 0)
            # Skipped objects and repeated walks over shared history both fail.
            for audits in ["objects - 1", "objects + 1", "objects * objects"]:
                with self.subTest(audits=audits):
                    write_probe(probe, audits)
                    with self.assertRaisesRegex(suite.common.BenchmarkError, "link audits, expected 12"):
                        suite.main(arguments(probe, root / "r.json"))
            row = dict(commits=4, registry="builtin", publication_batch_objects=64,
                       objects=12, imported_objects=12, reused_objects=0, warm_imported_objects=0,
                       warm_source_bytes=0, link_audits=None, root="r", wall_nanos=1,
                       witnesses=8, witness_commits=1, max_witness_batch=8,
                       correctness=suite.CORRECTNESS)
            suite.check_row(row, 4, "builtin", 64, "derived-blobs")
            suite.check_row({**row, "witnesses": 12, "max_witness_batch": 12}, 4, "builtin", 64, "stored-blobs")
            for audits in [0, 12]:
                with self.assertRaisesRegex(suite.common.BenchmarkError, "builtin registry recorded"):
                    suite.check_row({**row, "link_audits": audits}, 4, "builtin", 64, "derived-blobs")
            custom = {**row, "registry": "custom", "link_audits": 144, "witnesses": 12, "max_witness_batch": 12}
            with self.assertRaisesRegex(suite.common.BenchmarkError, "expected 12"):
                suite.check_row(custom, 4, "custom", 64, "stored-blobs")
            suite.check_row(custom, 4, "custom", 64, "stored-blobs", "baseline")
            with self.assertRaisesRegex(suite.common.BenchmarkError, "expected at least 12"):
                suite.check_row({**custom, "link_audits": 11}, 4, "custom", 64, "stored-blobs", "baseline")
            for field, value in [("warm_source_bytes", 1), ("imported_objects", 11), ("root", ""),
                                 ("witnesses", 7), ("witness_commits", 2), ("max_witness_batch", None),
                                 ("witnesses", True)]:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(suite.common.BenchmarkError):
                        suite.check_row({**row, field: value}, 4, "builtin", 64, "derived-blobs")

    def test_rejects_witness_commits_beyond_the_publication_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            # The reviewed regression: 16 commits of witnesses spill into one
            # oversized commit instead of 16-object batches.
            write_probe(probe, peak="witnesses")
            with self.assertRaisesRegex(suite.common.BenchmarkError,
                                        "'max_witness_batch': 48}, expected .*'max_witness_batch': 16}"):
                suite.main(arguments(probe, root / "r.json", commits="16", batch="16"))

    def test_samples_must_match_the_probes_declared_witness_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            # Each policy holds a probe to its own built-in witness count: a
            # derived-blobs probe that still witnesses blobs fails, as does a
            # stored-blobs probe that skips them.
            write_probe(probe, builtin="3", policy='"stored-blobs"')
            self.assertEqual(suite.main(arguments(probe, root / "r.json", registry="builtin")), 0)
            for declared, builtin in [("derived-blobs", "3"), ("stored-blobs", "2")]:
                with self.subTest(declared=declared):
                    write_probe(probe, builtin=builtin, policy=f'"{declared}"')
                    with self.assertRaisesRegex(suite.common.BenchmarkError,
                                                f"witness policy '{declared}' recorded"):
                        suite.main(arguments(probe, root / "r.json", registry="builtin"))
            for policy in ['"unknown"', "None"]:
                with self.subTest(policy=policy):
                    write_probe(probe, policy=policy)
                    with self.assertRaisesRegex(suite.common.BenchmarkError, "unknown witness policy"):
                        suite.main(arguments(probe, root / "r.json"))

    def test_paired_runs_alternate_order_and_summarize_deterministic_audit_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe, baseline = root / "probe", root / "baseline"
            write_probe(probe)
            write_probe(baseline, "objects * objects")
            output = root / "paired.json"
            self.assertEqual(suite.main(["--probe-binary", str(probe), "--baseline-binary", str(baseline),
                                         "--no-build", "--output", str(output), "--commits", "4",
                                         "--registry", "custom",
                                         "--publication-batch-objects", "64", "--repetitions", "2"]), 0)
            report = json.loads(output.read_text())
            self.assertEqual([p["variant"] for p in report["processes"]],
                             ["baseline", "candidate", "candidate", "baseline"])
            self.assertEqual([(a["variant"], a["witness_policy"]) for a in report["artifacts"]],
                             [("baseline", "derived-blobs"), ("candidate", "derived-blobs")])
            summary, = report["paired_summary"]
            self.assertEqual((summary["baseline_link_audits"], summary["candidate_link_audits"]), (144, 12))
            self.assertEqual(summary["candidate_max_witness_batch"], 12)
            self.assertEqual((summary["baseline_witnesses"], summary["candidate_witnesses"]), (12, 12))
            self.assertEqual(summary["pairs"], 2)
            self.assertFalse(summary["enough_samples"])
            rows = report["samples"]
            rows[-1]["root"] = "different root"
            with self.assertRaisesRegex(suite.common.BenchmarkError, "root"):
                suite.summarize_pairs(rows)

    def test_paired_runs_hold_each_artifact_to_its_own_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe, baseline = root / "probe", root / "baseline"
            write_probe(probe)
            # The preceding revision: built-in imports witnessed every blob.
            write_probe(baseline, builtin="3", policy='"stored-blobs"')
            output = root / "paired.json"
            self.assertEqual(suite.main(["--probe-binary", str(probe), "--baseline-binary", str(baseline),
                                         "--no-build", "--output", str(output), "--commits", "16",
                                         "--registry", "builtin", "--publication-batch-objects", "16"]), 0)
            report = json.loads(output.read_text())
            self.assertEqual([(a["variant"], a["witness_policy"]) for a in report["artifacts"]],
                             [("baseline", "stored-blobs"), ("candidate", "derived-blobs")])
            summary, = report["paired_summary"]
            self.assertEqual((summary["baseline_witnesses"], summary["candidate_witnesses"]), (48, 32))
            self.assertEqual((summary["baseline_max_witness_batch"], summary["candidate_max_witness_batch"]),
                             (16, 16))


if __name__ == "__main__":
    unittest.main()
