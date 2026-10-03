import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock
from benchmarks.suites import git_blob_file as suite


class GitBlobFileTests(unittest.TestCase):
    def test_accepts_serial_libtest_sample_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            probe.write_text("#!" + sys.executable + "\n" + r'''import json, os
print("test " + "benchmark_git_blob_file" + " ... git_blob_file_sample " + json.dumps(dict(
    strategy=os.environ["CASITA_GIT_ALIAS_STRATEGY"],
    backend=os.environ["CASITA_GIT_ALIAS_BACKEND"],
    file_bytes=int(os.environ["CASITA_GIT_ALIAS_BYTES"]),
    files=int(os.environ["CASITA_GIT_ALIAS_FILES"]),
    wall_nanos=1000, root="same-file",
    correctness="exact identity, length, closure and byte-for-byte readback")))
print("test result: ok. 1 passed; 0 failed;")
''')
            probe.chmod(0o755)
            output = root / "report.json"
            self.assertEqual(suite.main(["--probe-binary", str(probe), "--no-build",
                "--file-bytes", "0", "--files", "1", "--backend", "memory", "--output", str(output)]), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(len(report["samples"]), 2)

    def test_alternates_strategies_and_rejects_mismatched_file_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            probe.write_text("#!" + sys.executable + "\n" + r'''import json, os
strategy = os.environ["CASITA_GIT_ALIAS_STRATEGY"]
size = int(os.environ["CASITA_GIT_ALIAS_BYTES"])
print("git_blob_file_sample " + json.dumps(dict(strategy=strategy,
    backend=os.environ["CASITA_GIT_ALIAS_BACKEND"], file_bytes=size,
    files=int(os.environ["CASITA_GIT_ALIAS_FILES"]),
    wall_nanos=1000000 if strategy == "reread" else 500000,
    root="file-" + str(size),
    correctness="exact identity, length, closure and byte-for-byte readback")))
print("test result: ok. 1 passed; 0 failed;")
''')
            probe.chmod(0o755)
            output = root / "report.json"
            arguments = ["--probe-binary", str(probe), "--no-build", "--output", str(output),
                         "--file-bytes", "0,65536,65537", "--files", "1", "--backend", "both", "--repetitions", "5"]
            self.assertEqual(suite.main(arguments), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(len(report["samples"]), 60)
            self.assertEqual([p["strategy"] for p in report["processes"][:4]],
                             ["reread", "alias", "alias", "reread"])
            self.assertEqual(len(report["paired_summary"]), 6)
            for summary in report["paired_summary"]:
                self.assertTrue(summary["enough_samples"])
                self.assertEqual(summary["median_paired_reduction_percent"], 50)
            probe.write_text(probe.read_text().replace('root="file-" + str(size)', 'root=strategy'))
            with self.assertRaisesRegex(suite.common.BenchmarkError, "different file identities"):
                suite.main(arguments)
            report = json.loads(output.read_text())
            self.assertFalse(report["complete"])
            self.assertIn("error", report)
            self.assertEqual(len(report["samples"]), 2)

    def test_rejects_selections_without_a_supported_case_before_building(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "report.json"
            for arguments, message in [
                (["--files", "64", "--file-bytes", "4194304"], "no supported case"),
                (["--files", "4097", "--file-bytes", "65536"], "at most 4096 files"),
            ]:
                with self.subTest(arguments=arguments), \
                        mock.patch.object(suite.subprocess, "run") as build, \
                        mock.patch("sys.stderr", new_callable=io.StringIO) as stderr, \
                        self.assertRaises(SystemExit) as raised:
                    suite.main(arguments + ["--output", str(output)])
                self.assertEqual(raised.exception.code, 2)
                self.assertIn(message, stderr.getvalue())
                build.assert_not_called()
                self.assertFalse(output.exists())

    def test_profiles_select_batches_only_for_distinct_small_bodies(self):
        self.assertEqual(suite.cases(suite.PROFILE_BYTES["smoke"], suite.PROFILE_FILES["smoke"]),
                         [(0, 1), (1, 1), (65535, 1), (65536, 1), (65537, 1),
                          (65535, 64), (65536, 64), (65537, 64)])
        standard = suite.cases(suite.PROFILE_BYTES["standard"], suite.PROFILE_FILES["standard"])
        self.assertIn((4194305, 1), standard)
        self.assertNotIn((4194304, 1024), standard)
        self.assertNotIn((0, 1024), standard)


if __name__ == "__main__":
    unittest.main()
