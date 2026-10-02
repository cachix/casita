import itertools
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from benchmarks import dashboard, revisions
from benchmarks.suites import git_closure_audit, git_closure_import
from benchmarks.tests import test_git_closure_audit as audit
from benchmarks.tests import test_git_closure_import as closure_import

# Each suite's forwarded arguments select two values of every dimension they
# vary, so a report merging any two configurations would be caught.
SUITES = [
    ("git-closure-audit", audit.write_probe,
     ["--commits", "4,8", "--registry", "both", "--publication-batch-objects", "4,64"],
     ["cold-import"],
     {"commits": [4, 8], "registry": ["builtin", "custom"], "publication_batch_objects": [4, 64]}),
    ("git-closure-import", closure_import.write_probe,
     ["--counts", "3,4", "--max-buffered-bytes", "1024,2048", "--backend", "both", "--layout", "both"],
     ["cold", "subtree-delta", "warm", "wide-delta"],
     {"files": [3, 4], "max_buffered_bytes": [1024, 2048], "backend": ["memory", "local"],
      "packed": [False, True]}),
]
ROUNDS = 2


def configurations(dimensions):
    return {tuple(sorted(zip(dimensions, values)))
            for values in itertools.product(*dimensions.values())}


def selected(workload, dimensions):
    """The configuration a normalized workload identity names, restricted to `dimensions`."""
    _entrypoint, scale = workload.split(":", 1)
    scale = json.loads(scale)
    return tuple(sorted((name, scale[name]) for name in dimensions))


class GitClosureReportTests(unittest.TestCase):
    def test_revision_reports_keep_every_closure_workload_distinct(self):
        for suite_id, write_probe, forwarded, operations, dimensions in SUITES:
            with self.subTest(suite=suite_id), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                series = [revisions.RevisionSpec(label, label, label, digit * 40)
                          for label, digit in [("before", "a"), ("after", "b")]]
                for revision in series:
                    write_probe(root / revision.label)
                output = root / "output"
                with (
                    mock.patch.object(revisions, "resolve_revisions", return_value=series),
                    mock.patch.object(revisions, "git_output", side_effect=lambda arguments:
                                      "" if arguments[0] == "status" else "f" * 40),
                ):
                    code = revisions.main([
                        "before", "after", "--suite", suite_id, "--repetitions", str(ROUNDS),
                        "--output-dir", str(output),
                        *[f"--artifact={revision.label}={root / revision.label}" for revision in series],
                        "--", *forwarded])
                self.assertEqual(code, 0)
                self.assertEqual(json.loads((output / "execution.json").read_text())["status"], "completed")
                self.assertTrue((output / "series.md").is_file())
                report = json.loads((output / "series.json").read_text())
                expected = {(operation, configuration) for operation in operations
                            for configuration in configurations(dimensions)}
                for revision in report["revisions"]:
                    observations = revision["normalized"]["observations"]
                    self.assertEqual(len(observations), len(expected))
                    self.assertEqual({(row["operation"], selected(row["workload"], dimensions))
                                      for row in observations}, expected)
                    for row in observations:
                        self.assertEqual((row["status"], row["rounds"], row["samples"]), ("ok", ROUNDS, ROUNDS))
                        self.assertEqual(row["implementation"], "casita")
                        self.assertEqual(row["cache_policy"], "cold" if row["operation"].startswith("cold") else "warm")
                walls = [row for row in report["rows"] if row["metric"] == "wall_seconds"]
                self.assertEqual(len(walls), len(expected))
                for row in walls:
                    self.assertEqual([value["status"] for value in row["values"]], ["ok", "ok"])
                    self.assertTrue(all(value["value"] > 0 for value in row["values"]))

    def test_paired_reports_keep_variants_and_reject_incomplete_or_ambiguous_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for variant in ("baseline", "candidate"):
                closure_import.write_probe(root / variant)
            with (root / "baseline").open("a") as probe:
                probe.write("# baseline\n")
            output = root / "paired.json"
            # The dimensions the revision series above leaves fixed.
            varied = {"max_buffered_bytes": [1024, 2048], "file_bytes": [8, 16], "concurrency": [1, 16]}
            self.assertEqual(git_closure_import.main([
                "--counts", "3", "--max-buffered-bytes", "1024,2048", "--file-bytes", "8,16",
                "--concurrency", "1,16", "--content", "random", "--backend", "memory",
                "--layout", "loose", "--probe-binary", str(root / "candidate"),
                "--baseline-binary", str(root / "baseline"), "--no-build", "--output", str(output)]), 0)
            normalized = dashboard.normalize_result(output)
            self.assertEqual(normalized["suite_id"], "native-git")
            observations = normalized["observations"]
            expected = {(implementation, operation, configuration)
                        for implementation in ("casita-baseline", "casita-candidate")
                        for operation in ("cold", "subtree-delta", "warm", "wide-delta")
                        for configuration in configurations(varied)}
            self.assertEqual(len(observations), len(expected))
            self.assertEqual({(row["implementation"], row["operation"], selected(row["workload"], varied))
                              for row in observations}, expected)
            for row in observations:
                self.assertEqual(set(row["scale"]), set(git_closure_import.WORKLOAD))
                self.assertEqual(row["scale"]["content"], "random")
                self.assertEqual((row["profile"], row["status"]), ("standard", "ok"))
            result = json.loads(output.read_text())
            sample = next(row for row in result["samples"] if row["operation"] == "cold")
            cold = next(row for row in observations if row["operation"] == "cold")
            self.assertEqual(cold["metrics"]["imported_objects"], 5)
            self.assertEqual(cold["metrics"]["blob_witnesses"], sample["blob_witnesses"])
            del result["samples"][0]["packed"]
            output.write_text(json.dumps(result))
            with self.assertRaisesRegex(dashboard.DashboardError, r"lacks \['packed'\]"):
                dashboard.normalize_result(output)
            result["complete"] = False
            output.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, "incomplete Git closure"):
                dashboard.normalize_result(output)

    def test_audit_reports_require_a_complete_scalar_workload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            audit.write_probe(root / "probe")
            output = root / "audit.json"
            self.assertEqual(git_closure_audit.main(audit.arguments(
                root / "probe", output, registry="both", commits="4,8")), 0)
            result = json.loads(output.read_text())
            self.assertEqual(len(dashboard.normalize_result(output)["observations"]), 4)
            # Values of differing types are distinct workloads, never an ordering
            # failure, even where they compare equal. The first and third samples
            # share a registry and batch.
            for first, third in [("4", 8), (True, 1)]:
                with self.subTest(first=first, third=third):
                    samples = [dict(sample) for sample in result["samples"]]
                    samples[0]["commits"], samples[2]["commits"] = first, third
                    output.write_text(json.dumps({**result, "samples": samples}))
                    observations = dashboard.normalize_result(output)["observations"]
                    self.assertEqual(len(observations), 4)
                    self.assertEqual(len({row["workload"] for row in observations}), 4)
                    self.assertEqual({row["status"] for row in observations}, {"ok"})
            for field, value in [("registry", ["builtin"]), ("commits", None), ("operation", 1)]:
                with self.subTest(field=field, value=value):
                    output.write_text(json.dumps({**result, "samples": [
                        {**result["samples"][0], field: value}, *result["samples"][1:]]}))
                    with self.assertRaisesRegex(dashboard.DashboardError, "invalid workload"):
                        dashboard.normalize_result(output)
            del result["samples"][0]["operation"]
            output.write_text(json.dumps(result))
            with self.assertRaisesRegex(dashboard.DashboardError, r"lacks \['operation'\]"):
                dashboard.normalize_result(output)
            # A sample that neither succeeded nor recorded an error still fails its observation.
            result["samples"][1]["status"] = "skipped"
            del result["samples"][0]
            output.write_text(json.dumps(result))
            failed, = [row for row in dashboard.normalize_result(output)["observations"]
                       if row["status"] != "ok"]
            self.assertEqual((failed["samples"], failed["successful_samples"]), (1, 0))


if __name__ == "__main__":
    unittest.main()
