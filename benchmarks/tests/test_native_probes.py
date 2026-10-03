import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock
from benchmarks.suites import native_probes as suite


def fake_probe(root, metrics):
    probe = root / "probe"
    probe.write_text("#!" + sys.executable + "\n" + f'''import os
assert os.environ["CASITA_RAW_BLOB_BENCH_BLOBS"] == "4096"
print({metrics!r})
print("test result: ok. 1 passed; 0 failed;")
''')
    probe.chmod(0o755)
    return probe


class NativeProbeTests(unittest.TestCase):
    def test_raw_blob_closures_requires_every_batch_to_be_full(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "report.json"
            with mock.patch.object(suite, "build_probe_binary") as build, \
                    mock.patch("sys.stderr", new_callable=io.StringIO) as stderr, \
                    self.assertRaises(SystemExit) as raised:
                suite.main(["--probe", "raw-blob-closures", "--blobs", "4095", "--output", str(output)])
            self.assertEqual(raised.exception.code, 2)
            self.assertIn("--blobs must be at least 4096", stderr.getvalue())
            build.assert_not_called()
            self.assertFalse(output.exists())

    def test_raw_blob_closures_requires_wal3_cases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            output = root / "report.json"
            arguments = ["--probe", "raw-blob-closures", "--blobs", "4096", "--repetitions", "1",
                         "--output", str(output), "--probe-binary"]
            without = fake_probe(root, "raw_blob_blobs 4096 memory_b512_constructed_publish_nanos 7")
            with self.assertRaisesRegex(suite.common.BenchmarkError, "lacks the s3 feature"):
                suite.main(arguments + [str(without)])
            with_wal3 = fake_probe(root, "raw_blob_blobs 4096 wal3_b512_constructed_fragment_puts 3")
            self.assertEqual(suite.main(arguments + [str(with_wal3)]), 0)
            report = json.loads(output.read_text())
            self.assertEqual(report["samples"][0]["metrics"]["wal3_b512_constructed_fragment_puts"], 3)


if __name__ == "__main__":
    unittest.main()
