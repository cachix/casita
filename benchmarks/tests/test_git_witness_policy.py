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


def audit_probe(path, declared, behaves):
    """An audit probe declaring a policy, or none, and witnessing as `behaves` does."""
    audit.write_probe(path, builtin="3" if policy.STORES_BLOB_WITNESSES[behaves] else "2",
                      policy=repr(declared))


def import_probe(path, declared, behaves):
    """An import probe declaring a policy, or none, and witnessing as `behaves` does."""
    closure_import.write_probe(path, policy=repr(declared),
                               blob_witnesses=closure_import.STORED
                               if policy.STORES_BLOB_WITNESSES[behaves] else "0")


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
            ("git-closure-audit", audit_probe,
             ["--commits", "4", "--registry", "both", "--publication-batch-objects", "4,64"]),
            ("git-closure-import", import_probe,
             ["--counts", "3", "--max-buffered-bytes", "1024", "--backend", "memory", "--layout", "loose"]),
        ]
        for suite_id, write_probe, forwarded in suites:
            with self.subTest(suite=suite_id), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                spec = revisions.SUITE_BUILD_SPECS[suite_id]
                # The revisions on either side of derived Git blob completeness.
                series = [(revisions.RevisionSpec(label, label, label, digit * 40), declared)
                          for label, digit, declared in [("before", "a", "stored-blobs"),
                                                         ("after", "b", "derived-blobs")]]
                # Both orders, as rotated revision rounds run them.
                for order in [series, series[::-1]]:
                    for revision, declared in order:
                        probe = root / revision.label
                        write_probe(probe, declared, declared)
                        output = root / f"{revision.label}.json"
                        self.assertEqual(revisions.invoke_suite(
                            suite_id, spec, forwarded, revision, probe, output), 0)
                        report = json.loads(output.read_text())
                        self.assertTrue(report["complete"])
                        artifact, = report["artifacts"]
                        self.assertEqual(artifact["witness_policy"], declared)
                # A later revision that regressed to witnessing blobs again.
                probe = root / "regressed"
                write_probe(probe, "derived-blobs", "stored-blobs")
                with self.assertRaisesRegex(policy.common.BenchmarkError, "witness policy 'derived-blobs'"):
                    revisions.invoke_suite(suite_id, spec, forwarded, series[1][0], probe, root / "regressed.json")
                probe = root / "undeclared"
                write_probe(probe, None, "derived-blobs")
                with self.assertRaisesRegex(policy.common.BenchmarkError, "unknown witness policy"):
                    revisions.invoke_suite(suite_id, spec, forwarded, series[0][0], probe, root / "undeclared.json")


if __name__ == "__main__":
    unittest.main()
