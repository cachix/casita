import json
import pathlib
import sys
import tempfile
import unittest
from benchmarks.suites import git_verified_stream as suite


class GitVerifiedStreamTests(unittest.TestCase):
    def test_accepts_serial_libtest_sample_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            probe.write_text("#!" + sys.executable + "\n" + r'''import json, os
print("test " + "benchmark_git_verified_stream" + " ... git_verified_stream_sample " + json.dumps(dict(
    strategy=os.environ["CASITA_GIT_STREAM_STRATEGY"],
    backend=os.environ["CASITA_GIT_STREAM_BACKEND"],
    file_bytes=int(os.environ["CASITA_GIT_STREAM_BYTES"]),
    wall_nanos=1000, root="same-file",
    correctness="exact identity, length, closure and byte-for-byte readback")))
print("test result: ok. 1 passed; 0 failed;")
''')
            probe.chmod(0o755)
            output = root / "report.json"
            self.assertEqual(suite.main(["--probe-binary", str(probe), "--no-build",
                "--file-bytes", "0", "--backend", "memory", "--output", str(output)]), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(len(report["samples"]), 2)

    def test_alternates_strategies_and_rejects_mismatched_file_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            probe.write_text("#!" + sys.executable + "\n" + r'''import json, os
strategy = os.environ["CASITA_GIT_STREAM_STRATEGY"]
size = int(os.environ["CASITA_GIT_STREAM_BYTES"])
print("git_verified_stream_sample " + json.dumps(dict(strategy=strategy,
    backend=os.environ["CASITA_GIT_STREAM_BACKEND"], file_bytes=size,
    wall_nanos=1000000 if strategy == "reread" else 500000,
    root="file-" + str(size),
    correctness="exact identity, length, closure and byte-for-byte readback")))
print("test result: ok. 1 passed; 0 failed;")
''')
            probe.chmod(0o755)
            output = root / "report.json"
            arguments = ["--probe-binary", str(probe), "--no-build", "--output", str(output),
                         "--file-bytes", "0,65536,65537", "--backend", "both", "--repetitions", "5"]
            self.assertEqual(suite.main(arguments), 0)
            report = json.loads(output.read_text())
            self.assertTrue(report["complete"])
            self.assertEqual(len(report["samples"]), 60)
            self.assertEqual([p["strategy"] for p in report["processes"][:4]],
                             ["reread", "stream", "stream", "reread"])
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


if __name__ == "__main__":
    unittest.main()
