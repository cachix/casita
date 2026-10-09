import copy
import itertools
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from benchmarks import all as all_suites, cli, dashboard, revisions
from benchmarks.suites import collection_mark as mark


class CollectionMarkTests(unittest.TestCase):
    def case(self, parents=256, shape="shared", memory_limit=256, iterations=1, strategy="current", mode="named"):
        objects = 2 * parents if shape == "distinct" else parents + 1
        return dict(parents=parents, shape=shape, memory_limit=memory_limit, iterations=iterations,
                    objects=objects, edges=parents, strategy=strategy, mode=mode, correctness=mark.CORRECTNESS,
                    samples=[dict(iteration=i, warm=i > 0, mode=mode, nanos=(i + 1) * 1000,
                                  record_reads=objects, spill_files=0, spill_peak_bytes=0)
                             for i in range(iterations + 1)])

    def output(self, case):
        return "mark_sample " + json.dumps(case) + "\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n"

    def test_parser_requires_exact_dimensions_and_ordered_modes(self):
        case = self.case()
        self.assertEqual(mark.parse_sample(self.output(case), 256, "shared", 256, 1), case)
        for key, value in [("parents", 255), ("shape", "chain"), ("memory_limit", 250000),
                           ("objects", 256), ("edges", 255), ("correctness", None),
                           ("strategy", "legacy"), ("mode", "pins"),
                           ("samples", case["samples"][:-1]), ("samples", case["samples"][::-1])]:
            with self.subTest(key=key), self.assertRaises(mark.common.BenchmarkError):
                mark.parse_sample(self.output({**case, key: value}), 256, "shared", 256, 1)
        for output in (self.output(case) * 2, "", self.output(case).replace("1 passed", "0 passed")):
            with self.assertRaises(mark.common.BenchmarkError):
                mark.parse_sample(output, 256, "shared", 256, 1)

    def test_parser_rejects_invalid_metrics_and_missing_record_reads(self):
        for key, value in [("nanos", -1), ("nanos", True), ("record_reads", 256),
                           ("spill_files", 1.5), ("spill_peak_bytes", None), ("warm", 1),
                           ("iteration", True), ("mode", "unknown")]:
            case = copy.deepcopy(self.case())
            case["samples"][1][key] = value
            with self.subTest(key=key), self.assertRaises(mark.common.BenchmarkError):
                mark.parse_sample(self.output(case), 256, "shared", 256, 1)

    def test_parser_accepts_rust_single_thread_test_name_prefix(self):
        case = self.case()
        captured = self.output(case).replace("mark_sample ", f"test {mark.PROBE} ... mark_sample ", 1)
        self.assertEqual(mark.parse_sample(captured, 256, "shared", 256, 1), case)

    def run_fixture(self, root, fail=False, extra_args=()):
        binary, output = root / "probe", root / "result.json"
        binary.write_bytes(b"test-only probe")

        def measured(command, stdout, stderr, **kwargs):
            env = command.env
            self.assertIn(mark.PROBE, command.steps[0])
            case = self.case(int(env["CASITA_MARK_PARENTS"]), env["CASITA_MARK_SHAPE"],
                             int(env["CASITA_MARK_MEMORY_LIMIT"]), int(env["CASITA_MARK_ITERATIONS"]),
                             env["CASITA_MARK_STRATEGY"], env["CASITA_MARK_MODE"])
            stdout.write_text(self.output(case))
            stderr.write_text("fixture failure" if fail else "")
            return dict(exit_code=7 if fail else 0, wall_seconds=99, max_rss_bytes=1234)

        with mock.patch.object(mark.common, "measured_command", side_effect=measured), mock.patch.object(mark.common, "environment_metadata", return_value={}):
            args = ["--profile", "smoke", "--repetitions", "1", "--probe-binary", str(binary), "--no-build", "--output", str(output), *extra_args]
            if fail:
                with self.assertRaisesRegex(mark.common.BenchmarkError, "probe failed"):
                    mark.main(args)
            else:
                self.assertEqual(mark.main(args), 0)
        return output

    def test_smoke_preserves_every_workload_dimension_and_phase(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = self.run_fixture(pathlib.Path(temporary))
            result = json.loads(output.read_text())
            self.assertTrue(result["complete"])
            self.assertEqual(len(result["processes"]), 96)
            observations = dashboard.normalize_result(output)["observations"]
            actual = {(o["scale"]["entries"], o["scale"]["shape"], o["scale"]["spill_memory_objects"],
                       o["operation"], o["cache_policy"], o["implementation"]) for o in observations}
            expected = {(n, shape, limit, f"mark-{mode}-{phase}", phase, f"casita-{strategy}")
                        for n, shape, limit, mode, phase, strategy in itertools.product(
                            (127, 128, 255, 256), ("shared", "distinct", "chain"), (256, 250000),
                            ("named", "pins"), ("first", "warm"), ("legacy", "current"))}
            self.assertEqual(actual, expected)
            self.assertEqual(len(observations), 192)
            for observation in observations:
                self.assertEqual(observation["metrics"]["wall_seconds"],
                                 1e-6 if observation["cache_policy"] == "first" else 2e-6)
            result["complete"] = False
            output.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                dashboard.normalize_result(output)

    def test_matching_strategies_are_adjacent_and_reverse_on_even_repetitions(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = self.run_fixture(pathlib.Path(temporary), extra_args=(
                "--parents", "257", "--memory-limits", "250000", "--repetitions", "2"))
            processes = json.loads(output.read_text())["processes"]
            self.assertEqual(len(processes), 24)
            for first, second in zip(processes[::2], processes[1::2]):
                for dimension in ("parents", "shape", "memory_limit", "mode", "repetition"):
                    self.assertEqual(first[dimension], second[dimension])
                expected = ["legacy", "current"] if first["repetition"] == 1 else ["current", "legacy"]
                self.assertEqual([first["strategy"], second["strategy"]], expected)

    def test_failure_is_retained_and_not_comparable(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = self.run_fixture(pathlib.Path(temporary), fail=True)
            result = json.loads(output.read_text())
            self.assertFalse(result["complete"])
            self.assertEqual(result["samples"], [])
            self.assertEqual(result["processes"][0]["exit_code"], 7)
            self.assertIn("fixture failure", result["error"])
            with self.assertRaisesRegex(ValueError, "incomplete"):
                dashboard.normalize_result(output)

    def test_selected_chain_strategy_and_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = self.run_fixture(pathlib.Path(temporary), extra_args=(
                "--parents", "257", "--memory-limits", "250000", "--shape", "chain",
                "--mode", "named", "--strategy", "current"))
            result = json.loads(output.read_text())
            self.assertEqual(len(result["processes"]), 1)
            sample = result["samples"][0]
            self.assertEqual((sample["shape"], sample["variant"], sample["objects"]),
                             ("chain", "current", 258))
            self.assertEqual(sample["operation"], "mark-named-first")

    def test_registered_probe_build_and_all_smoke(self):
        entry = next(e for e in cli.entrypoints() if e["id"] == "collection-mark")
        self.assertEqual(entry["target"], "benchmarks.suites.collection_mark")
        self.assertEqual(revisions.SUITE_BUILD_SPECS["collection-mark"].artifact_name, "casita-lib-test")
        arguments = all_suites.suite_arguments("collection-mark", pathlib.Path("/binaries"), "smoke", 1)
        self.assertIn("/binaries/casita-lib-test", arguments)
        self.assertIn("--no-build", arguments)
        self.assertIn("smoke", arguments)


if __name__ == "__main__":
    unittest.main()
