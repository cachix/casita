import hashlib
import json
import pathlib
import sys
import tempfile
import unittest
from benchmarks import all as all_suites
from benchmarks.suites import chunk_upload_completion as suite
from benchmarks.suites.repository import BenchmarkError


class ChunkCompletionTests(unittest.TestCase):
    def test_permanent_suite_builds_its_probe(self):
        commands = all_suites.build_commands(["chunk-upload-completion"], pathlib.Path("/build"))
        self.assertEqual(len(commands), 1)
        self.assertIn("chunk_upload_completion", commands[0])

    def probe(self, path, root="same", correctness=suite.CORRECTNESS, serial_prefix=False, concurrency_skew=0):
        prefix = f"test {suite.PROBE} ... " if serial_prefix else ""
        path.write_text("#!" + sys.executable + "\n" + "import os, json\n" +
            "row = dict(file_bytes=int(os.environ['CASITA_CHUNK_COMPLETION_BYTES']), budget=int(os.environ['CASITA_CHUNK_COMPLETION_BUDGET']), delay_ms=int(os.environ['CASITA_CHUNK_COMPLETION_DELAY_MS']), concurrency=int(os.environ['CASITA_CHUNK_COMPLETION_CONCURRENCY']) + " + repr(concurrency_skew) + ", wall_nanos=100, root=" + repr(root) + ", correctness=" + repr(correctness) + ")\n" +
            "print(" + repr(prefix + "chunk_upload_completion_sample ") + " + json.dumps(row))\nprint('test result: ok. 1 passed; 0 failed;')\n")
        path.chmod(0o755)
        return path

    def test_paired_builds_require_matching_fixture_fingerprints(self):
        for candidate_fixture in ("a" * 64, "b" * 64):
            with self.subTest(candidate_fixture=candidate_fixture), tempfile.TemporaryDirectory() as temporary:
                directory = pathlib.Path(temporary)
                before = self.probe(directory / "before")
                after = self.probe(directory / "after")
                after.write_text(after.read_text() + "\n# distinct candidate executable\n")
                for executable, fixture in ((before, "a" * 64), (after, candidate_fixture)):
                    build = dict(executable_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                                 fixture_sha256=fixture, lockfile_sha256="c" * 64,
                                 features=["native", "experimental"], default_features=False,
                                 rustc_version="same compiler", rustflags="")
                    pathlib.Path(str(executable) + ".build.json").write_text(json.dumps(build))
                output = directory / "result.json"
                args = ["--no-build", "--baseline-binary", str(before), "--probe-binary", str(after),
                        "--output", str(output)] + ["--file-bytes", "65536", "--budgets", "65536", "--delays-ms", "0", "--concurrency", "4"]
                if candidate_fixture != "a" * 64:
                    with self.assertRaisesRegex(BenchmarkError, "paired builds differ in fixture_sha256"):
                        suite.main(args)
                    self.assertFalse(output.exists(), "reject incompatible builds before measuring")
                else:
                    self.assertEqual(suite.main(args), 0)
                    report = json.loads(output.read_text())
                    self.assertTrue(report["complete"])
                    self.assertEqual(len(report["samples"]), 2)

    def test_serial_libtest_prefix_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe", serial_prefix=True)
            output = directory / "result.json"
            self.assertEqual(suite.main(["--no-build", "--probe-binary", str(probe),
                "--file-bytes", "65536", "--budgets", "65536", "--delays-ms", "0",
                "--concurrency", "4", "--output", str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["samples"]), 1)

    def test_each_concurrency_reaches_the_probe(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe")
            output = directory / "result.json"
            self.assertEqual(suite.main(["--no-build", "--probe-binary", str(probe),
                "--file-bytes", "65536", "--budgets", "4194304", "--delays-ms", "8",
                "--concurrency", "4,32", "--output", str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertEqual([sample["concurrency"] for sample in result["samples"]], [4, 32])
            self.assertEqual(result["configuration"]["upload_concurrency"], [4, 32])

    def test_standard_profile_covers_both_upload_windows_and_straggler_lengths(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe")
            output = directory / "result.json"
            self.assertEqual(suite.main(["--no-build", "--probe-binary", str(probe), "--profile", "standard",
                "--file-bytes", "65536", "--budgets", "4194304", "--output", str(output)]), 0)
            configuration = json.loads(output.read_text())["configuration"]
            # Both sides of the 64-entry reorder floor and the scaled window.
            self.assertEqual(configuration["upload_concurrency"], [2, 4, 32])
            # Stragglers on both sides of the 32-upload reorder window.
            self.assertEqual(configuration["delays_ms"], [0, 8, 50])

    def test_a_probe_ignoring_its_concurrency_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe", concurrency_skew=1)
            output = directory / "result.json"
            with self.assertRaisesRegex(BenchmarkError, "incorrect chunk completion configuration"):
                suite.main(["--no-build", "--probe-binary", str(probe), "--file-bytes", "65536",
                    "--budgets", "8192", "--delays-ms", "0", "--output", str(output)])

    def test_different_roots_reject_the_pair_and_preserve_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            before = self.probe(directory/"before")
            after = self.probe(directory/"after", root="different")
            output = directory/"result.json"
            with self.assertRaisesRegex(BenchmarkError, "changed identity"):
                suite.main(["--no-build", "--probe-binary", str(after), "--baseline-binary", str(before),
                    "--file-bytes", "65536", "--budgets", "8192", "--delays-ms", "0", "--output", str(output)])
            result = json.loads(output.read_text())
            self.assertFalse(result["complete"])
            self.assertEqual(len(result["processes"]), 2)

    def test_missing_audit_is_never_an_accepted_measurement(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory/"probe", correctness="unchecked")
            output = directory/"result.json"
            with self.assertRaisesRegex(BenchmarkError, "missing audit"):
                suite.main(["--no-build", "--probe-binary", str(probe), "--file-bytes", "65536",
                    "--budgets", "8192", "--delays-ms", "0", "--output", str(output)])
            self.assertFalse(json.loads(output.read_text())["complete"])


if __name__ == "__main__":
    unittest.main()
