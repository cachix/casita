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
    def test_wal3_publication_requires_every_checkpoint_case(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            output = root / "report.json"
            arguments = ["--probe", "wal3-publication-checkpoints", "--blobs", "4096",
                         "--checkpoint-corpora", "8192,131072",
                         "--repetitions", "1", "--output", str(output), "--probe-binary"]
            fields = (("checkpoint_shards", 1), ("after_nanos", 9), ("validated", 1))
            complete = " ".join(
                [f"wal3_b{batch}_{temperature}_{field} {value}"
                 for batch in (511, 512, 513)
                 for temperature in ("warm", "reopened")
                 for field, value in fields + (("before_nanos", 7),)] +
                [f"wal3_c{corpus}_{temperature}_{field} {value}"
                 for corpus in (8192, 131072)
                 for temperature in ("warm", "reopened")
                 for field, value in fields]
            )
            for missing in ("wal3_b513_reopened_validated 1", "wal3_b511_warm_before_nanos 7",
                            "wal3_c131072_reopened_after_nanos 9", "wal3_c8192_warm_checkpoint_shards 1"):
                binary = fake_probe(root, complete.replace(missing, ""))
                with self.assertRaisesRegex(suite.common.BenchmarkError, "missing checkpoint case"):
                    suite.main(arguments + [str(binary)])
                self.assertFalse(output.exists())
            binary = fake_probe(root, complete)
            self.assertEqual(suite.main(arguments + [str(binary)]), 0)
            report = json.loads(output.read_text())
            metrics = report["samples"][0]["metrics"]
            self.assertEqual(metrics["wal3_b511_warm_before_nanos"], 7)
            self.assertEqual(metrics["wal3_b513_reopened_after_nanos"], 9)
            self.assertEqual(metrics["wal3_c131072_reopened_after_nanos"], 9)
            self.assertEqual(report["configuration"]["checkpoint_corpora"], [8192, 131072])

    def test_wal3_publication_passes_its_checkpoint_corpora(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            probe = root / "probe"
            probe.write_text("#!" + sys.executable + "\n" + '''import os
assert os.environ["CASITA_WAL3_CHECKPOINT_CORPORA"] == "8192"
print("probe_ran 1")
print("test result: ok. 1 passed; 0 failed;")
''')
            probe.chmod(0o755)
            arguments = ["--probe", "logical-state", "--checkpoint-corpora", "8192",
                         "--repetitions", "1", "--output", str(root / "report.json"),
                         "--probe-binary", str(probe)]
            self.assertEqual(suite.main(arguments), 0)
            for invalid in ("", "0", "8192,8192", "8192,many"):
                with self.subTest(invalid=invalid), mock.patch("sys.stderr", io.StringIO()):
                    with self.assertRaises(SystemExit):
                        suite.main(arguments[:3] + [invalid] + arguments[4:])

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
