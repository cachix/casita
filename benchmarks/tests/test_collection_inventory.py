import json
import pathlib
import tempfile
import unittest
from unittest import mock

from benchmarks import all as all_suites, cli, dashboard, revisions
from benchmarks.suites import collection_inventory as inventory


class CollectionInventoryTests(unittest.TestCase):
    def case(self, count=257, limit=17):
        return dict(count=count, memory_limit=limit, seconds=0.01,
                    spill_files=0 if limit >= count * 16 + 128 else 2,
                    spill_peak_bytes=8192, logical_removed=5, payloads_removed=5,
                    chunks_removed=5, correctness=inventory.CORRECTNESS)

    def output(self, case):
        return (f"test {inventory.PROBE} ... collection_inventory_sample {json.dumps(case)}\n"
                "test result: ok. 1 passed; 0 failed; 0 ignored;\n")

    def test_parser_rejects_failed_missing_duplicate_or_corrupted_results(self):
        case = self.case()
        self.assertEqual(inventory.parse_sample(self.output(case), 257, 17), case)
        for key, value in [("count", True), ("memory_limit", 18), ("correctness", "unchecked"),
                           ("seconds", float("nan")), ("seconds", float("inf")), ("seconds", -1),
                           ("seconds", True), ("spill_files", 0), ("spill_peak_bytes", -1),
                           ("chunks_removed", 0), ("logical_removed", 4), ("payloads_removed", 6)]:
            with self.subTest(key=key, value=value), self.assertRaises(inventory.common.BenchmarkError):
                inventory.parse_sample(self.output({**case, key: value}), 257, 17)
        for output in ("", self.output(case) * 2, self.output(case).replace("1 passed", "0 passed"),
                       "collection_inventory_sample {invalid\ntest result: ok. 1 passed; 0 failed;"):
            with self.assertRaises(inventory.common.BenchmarkError):
                inventory.parse_sample(output, 257, 17)

    def run_fixture(self, root, fail=False):
        binary = root / "probe"
        binary.write_bytes(b"test-only probe")
        output = root / "result.json"

        def measured(command, stdout, stderr, **kwargs):
            self.assertEqual(command.steps[0][1:], [inventory.PROBE, "--exact", "--ignored", "--nocapture"])
            case = self.case(int(command.env["CASITA_COLLECTION_INVENTORY_COUNT"]),
                             int(command.env["CASITA_COLLECTION_INVENTORY_MEMORY"]))
            stdout.write_text(self.output(case))
            stderr.write_text("fixture failure" if fail else "")
            return dict(exit_code=7 if fail else 0, wall_seconds=99, max_rss_bytes=1234)

        with mock.patch.object(inventory.common, "measured_command", side_effect=measured), \
                mock.patch.object(inventory.common, "environment_metadata", return_value={}):
            args = ["--profile", "smoke", "--repetitions", "1", "--probe-binary", str(binary),
                    "--no-build", "--output", str(output)]
            if fail:
                with self.assertRaisesRegex(inventory.common.BenchmarkError, "probe failed"):
                    inventory.main(args)
            else:
                self.assertEqual(inventory.main(args), 0)
        return json.loads(output.read_text())

    def test_smoke_crosses_frontier_and_live_set_boundaries_and_preserves_raw_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_fixture(pathlib.Path(temporary))
        self.assertTrue(result["complete"])
        self.assertEqual({(s["entries"], s["spill_memory_objects"]) for s in result["samples"]},
                         {(count, limit) for count in (255, 256, 257)
                          for limit in (17, count, count + 1, count + 2, count * 16 + 128)})
        self.assertEqual(len(result["processes"]), 15)
        self.assertTrue(all(p["stdout"] and p["exit_code"] == 0 for p in result["processes"]))
        self.assertTrue(all(s["wall_seconds"] == 0.01 for s in result["samples"]))

    def test_failure_is_incomplete_and_retains_raw_process_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_fixture(pathlib.Path(temporary), fail=True)
        self.assertFalse(result["complete"])
        self.assertEqual(result["samples"], [])
        self.assertEqual(result["processes"][0]["exit_code"], 7)
        self.assertEqual(result["processes"][0]["stderr"], "fixture failure")

    def test_dashboard_preserves_boundaries_and_rejects_incomplete_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            result = self.run_fixture(root)
            output = root / "result.json"
            observations = dashboard.normalize_result(output)["observations"]
            self.assertEqual(len(observations), 15)
            self.assertEqual({(o["scale"]["entries"], o["scale"]["spill_memory_objects"])
                              for o in observations},
                             {(s["entries"], s["spill_memory_objects"]) for s in result["samples"]})
            self.assertTrue(all(o["cache_policy"] == "first" for o in observations))
            self.assertTrue(all(o["metrics"]["wall_seconds"] == 0.01 for o in observations))
            result["complete"] = False
            output.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                dashboard.normalize_result(output)

    def test_permanent_registration(self):
        manifest = json.loads((cli.ROOT / "benchmarks/manifest.json").read_text())
        entry = next(e for e in manifest["entrypoints"] if e["id"] == "collection-inventory")
        self.assertEqual(entry["target"], "benchmarks.suites.collection_inventory")
        self.assertEqual(all_suites.SMOKE["collection-inventory"], ["--profile", "smoke"])
        self.assertEqual(revisions.SUITE_BUILD_SPECS["collection-inventory"],
                         revisions.SUITE_BUILD_SPECS["metadata-collection"])


if __name__ == "__main__":
    unittest.main()
