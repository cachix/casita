import json
import pathlib
import tempfile
import unittest
from unittest import mock

from benchmarks import revisions
from benchmarks.suites import git_witness_policy as policy
from benchmarks.tests import test_git_closure_audit as audit
from benchmarks.tests import test_git_closure_import as closure_import


def row(declared):
    return {"witness_policy": declared}


class GitWitnessPolicyTests(unittest.TestCase):
    def test_each_artifact_declares_one_known_policy(self):
        artifact = {}
        self.assertEqual(policy.declared(artifact, row("stored-blobs")), "stored-blobs")
        self.assertEqual(policy.declared(artifact, row("stored-blobs")), "stored-blobs")
        self.assertEqual(artifact["witness_policy"], "stored-blobs")
        for declared in ["unknown", None, ["stored-blobs"], {}]:
            with self.subTest(declared=declared):
                with self.assertRaisesRegex(policy.common.BenchmarkError, "unknown witness policy"):
                    policy.declared({}, row(declared))
        with mock.patch.dict(policy.STORES_BLOB_WITNESSES, {"other": False}):
            with self.assertRaisesRegex(policy.common.BenchmarkError, "'other' after 'stored-blobs'"):
                policy.declared(artifact, row("other"))

    def test_revision_series_hold_each_probe_to_its_own_declaration(self):
        suites = [
            ("git-closure-audit", audit.write_probe,
             ["--commits", "4", "--registry", "both", "--publication-batch-objects", "4,64"]),
            ("git-closure-import", closure_import.write_probe,
             ["--counts", "3", "--max-buffered-bytes", "1024", "--backend", "memory", "--layout", "loose"]),
        ]
        for suite_id, write_probe, forwarded in suites:
            with self.subTest(suite=suite_id), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                spec = revisions.SUITE_BUILD_SPECS[suite_id]
                series = [revisions.RevisionSpec(label, label, label, digit * 40)
                          for label, digit in [("before", "a"), ("after", "b")]]
                # Both orders, as rotated revision rounds run them.
                for order in [series, series[::-1]]:
                    for revision in order:
                        probe = root / revision.label
                        write_probe(probe)
                        output = root / f"{revision.label}.json"
                        self.assertEqual(revisions.invoke_suite(
                            suite_id, spec, forwarded, revision, probe, output), 0)
                        report = json.loads(output.read_text())
                        self.assertTrue(report["complete"])
                        artifact, = report["artifacts"]
                        self.assertEqual(artifact["witness_policy"], "stored-blobs")
                probe = root / "undeclared"
                write_probe(probe, policy="None")
                with self.assertRaisesRegex(policy.common.BenchmarkError, "unknown witness policy"):
                    revisions.invoke_suite(suite_id, spec, forwarded, series[0], probe, root / "undeclared.json")


if __name__ == "__main__":
    unittest.main()
