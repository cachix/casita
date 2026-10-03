import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock
from benchmarks import all as all_suites
from benchmarks.suites import chunk_hash_batch as suite
from benchmarks.suites.repository import BenchmarkError


class ChunkHashTests(unittest.TestCase):
    def test_permanent_suite_builds_its_probe(self):
        commands = all_suites.build_commands(["chunk-hash-batch"], pathlib.Path("/build"))
        self.assertEqual(len(commands), 1)
        self.assertIn("chunk_hash_batch", commands[0])
        self.assertIn("chunk-hash-batch", all_suites.SMOKE)

    def probe(self, path, change=None, serial_prefix=False):
        prefix = f"test {suite.PROBE} ... " if serial_prefix else ""
        path.write_text("#!" + sys.executable + "\nimport os,json\n" + f"# {path.name}\n" +
            "row=dict(file_bytes=int(os.environ['CASITA_HASH_BYTES']), average_chunk_bytes=int(os.environ['CASITA_HASH_AVERAGE']), upload_concurrency=int(os.environ['CASITA_HASH_CONCURRENCY']), memory_budget_bytes=int(os.environ['CASITA_HASH_BUDGET']), content=os.environ['CASITA_HASH_CONTENT'], backend=os.environ['CASITA_HASH_BACKEND'], wall_nanos=100, root='same', manifest_hash='same', chunks=10, correctness=" + repr(suite.CORRECTNESS) + ")\n" +
            "for phase in ['cold', 'duplicate']:\n row.update(phase=phase,chunk_puts=10 if phase=='cold' else 0)\n row.update(" + repr(change or {}) + ")\n prefix = " + repr(prefix) + " if phase=='cold' else ''\n print(prefix+'chunk_hash_sample '+json.dumps(row))\n" +
            "print('test result: ok. 1 passed; 0 failed;')\n")
        path.chmod(0o755)
        return path

    def run_pair(self, directory, change=None):
        before = self.probe(directory / "before")
        after = self.probe(directory / "after", change)
        return suite.main(["--no-build", "--probe-binary", str(after), "--baseline-binary", str(before),
            "--cases", "many-4", "--backend", "memory", "--output", str(directory / "result.json")])

    def test_smoke_profile_measures_the_production_defaults(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe")
            output = directory / "result.json"
            self.assertEqual(suite.main(["--profile", "smoke", "--no-build", "--probe-binary", str(probe),
                                        "--backend", "memory", "--output", str(output)]), 0)
            report = json.loads(output.read_text())
            rows = [row for row in report["samples"] if row["case"] == "production-default"]
            self.assertEqual({row["phase"] for row in rows}, {"cold", "duplicate"})
            for row in rows:
                self.assertEqual(row["average_chunk_bytes"], 262144)
                self.assertEqual(row["upload_concurrency"], 32)
                self.assertEqual(row["memory_budget_bytes"], 67108864)

    def test_auto_build_allows_dependency_resolution_without_a_lockfile(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "compiled-probe")
            output = directory / "result.json"
            self.assertFalse((directory / "Cargo.lock").exists())
            run = suite.subprocess.run
            builds = []

            def build(command, **kwargs):
                if command[:2] != ["cargo", "test"]:
                    return run(command, **kwargs)
                builds.append(command)
                self.assertEqual(kwargs["cwd"], directory)
                self.assertNotIn("--offline", command)
                self.assertNotIn("--locked", command)
                self.assertEqual(command[command.index("--test") + 1], "chunk_hash_batch")
                self.assertEqual(command[command.index("--features") + 1], "native,experimental")
                artifact = dict(reason="compiler-artifact", target=dict(name="chunk_hash_batch"), executable=str(probe))
                return suite.subprocess.CompletedProcess(command, 0, json.dumps(artifact) + "\n", "")

            with mock.patch.object(suite.cli, "ROOT", directory), \
                    mock.patch.object(suite.subprocess, "run", side_effect=build):
                self.assertEqual(suite.main(["--cases", "many-4", "--backend", "memory",
                                             "--output", str(output)]), 0)
            self.assertEqual(len(builds), 1)
            result = json.loads(output.read_text())
            self.assertTrue(result["complete"])
            self.assertEqual({row["phase"] for row in result["samples"]}, {"cold", "duplicate"})

    def test_serial_libtest_prefix_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            probe = self.probe(directory / "probe", serial_prefix=True)
            output = directory / "result.json"
            self.assertEqual(suite.main(["--no-build", "--probe-binary", str(probe),
                "--cases", "many-4", "--backend", "memory", "--output", str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertTrue(result["complete"])
            self.assertEqual({row["phase"] for row in result["samples"]}, {"cold", "duplicate"})

    def test_hash_identity_must_match_between_variants(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            with self.assertRaisesRegex(BenchmarkError, "paired chunk identity"):
                self.run_pair(directory, dict(manifest_hash="different"))
            self.assertFalse(json.loads((directory / "result.json").read_text())["complete"])

    def test_duplicate_write_gate_is_enforced(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(BenchmarkError, "duplicate wrote chunks"):
                self.run_pair(pathlib.Path(temporary), dict(chunk_puts=1))

    def test_audit_is_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(BenchmarkError, "correctness audit"):
                self.run_pair(pathlib.Path(temporary), dict(correctness="unchecked"))

    def test_configuration_must_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(BenchmarkError, "configuration"):
                self.run_pair(pathlib.Path(temporary), dict(upload_concurrency=1))

    def test_cold_and_duplicate_timings_stay_separate(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            self.assertEqual(self.run_pair(directory), 0)
            result = json.loads((directory / "result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["samples"]), 4)
            self.assertEqual({row["phase"] for row in result["paired_summary"]}, {"cold", "duplicate"})
            self.assertTrue(all(not row["enough_samples"] for row in result["paired_summary"]))

    def test_permanent_cases_span_count_memory_byte_and_small_file_boundaries(self):
        self.assertTrue({3, 4, 5} <= {case[2] for case in suite.CASES.values()})
        self.assertTrue({65536, 196608, 262144} <= {case[3] for case in suite.CASES.values()})
        self.assertTrue({524286, 524288, 524290, 1048576} <= {case[1] for case in suite.CASES.values()})
        self.assertTrue({131071, 131072, 131073} <= {case[0] for case in suite.CASES.values()})
        self.assertTrue({32766, 32768, 32770} <= {case[1] for case in suite.CASES.values()})
        # Both sides of the 4 KiB lone-chunk inline bound.
        self.assertTrue({2048, 4096, 8196} <= {case[1] for case in suite.CASES.values()})


if __name__ == "__main__":
    unittest.main()
