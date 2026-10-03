import hashlib
import json
import pathlib
import sys
import tempfile
import unittest
from benchmarks import all as all_suites
from benchmarks.suites import chunk_manifest_stream as suite
from benchmarks.suites.repository import BenchmarkError


class ChunkManifestTests(unittest.TestCase):
    def test_permanent_suite_builds_its_probe(self):
        commands = all_suites.build_commands(["chunk-manifest-stream"], pathlib.Path("/build"))
        self.assertEqual(len(commands), 1)
        self.assertIn("chunk_manifest_stream", commands[0])

    def probe(self, path, manifest="same", correctness=None, serial_prefix=False):
        prefix = f"test {suite.PROBE} ... " if serial_prefix else ""
        if correctness is None:
            correctness = suite.CORRECTNESS
        path.write_text("#!" + sys.executable + "\nimport os,json\n" +
            f"# {path.name}\n" +
            "row=dict(file_bytes=int(os.environ['CASITA_MANIFEST_BYTES']), backend=os.environ['CASITA_MANIFEST_BACKEND'], wall_nanos=100, root='same', manifest_hash=" + repr(manifest) +
            ", before_rss_bytes=1000, write_peak_rss_bytes=2000, correctness=" + repr(correctness) + ")\n" +
            "print(" + repr(prefix + "chunk_manifest_sample ") + "+json.dumps(row))\nprint('test result: ok. 1 passed; 0 failed;')\n")
        path.chmod(0o755)
        return path

    def run_pair(self, directory, **kwargs):
        before = self.probe(directory / "before")
        after = self.probe(directory / "after", **kwargs)
        return suite.main(["--no-build", "--probe-binary", str(after), "--baseline-binary", str(before),
            "--file-bytes", "65536", "--backend", "memory", "--output", str(directory / "result.json")])

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
                        "--output", str(output)] + ["--file-bytes", "65536", "--backend", "memory"]
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
            self.assertEqual(self.run_pair(directory, serial_prefix=True), 0)
            result = json.loads((directory / "result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["samples"]), 2)

    def test_manifest_bytes_must_match_even_when_blob_identity_matches(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            with self.assertRaisesRegex(BenchmarkError, "changed identity"):
                self.run_pair(directory, manifest="different")
            result = json.loads((directory / "result.json").read_text())
            self.assertFalse(result["complete"])
            self.assertEqual(len(result["processes"]), 2)

    def test_streaming_audit_is_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(BenchmarkError, "missing audit"):
                self.run_pair(pathlib.Path(temporary), correctness="unchecked")

    def test_memory_and_timing_evidence_are_reported_separately(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.assertEqual(self.run_pair(directory), 0)
            result = json.loads((directory / "result.json").read_text())
            self.assertTrue(result["complete"])
            summary = result["paired_summary"][0]
            self.assertEqual(summary["median_paired_reduction_percent"], 0)
            self.assertEqual(summary["median_paired_write_peak_rss_reduction_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
