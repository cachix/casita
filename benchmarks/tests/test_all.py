import importlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
import tomllib
from unittest import mock
from benchmarks import all as runner
from benchmarks import cli

class AllSuiteTests(unittest.TestCase):
    def test_rustfs_protocol_matrix_retains_failed_cases_in_all_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / "results"

            def run(command, log, timeout, environment):
                self.assertEqual(command[2:4], ["benchmarks.suites.pin_protocol_s3", "--profile"])
                pathlib.Path(command[-1]).write_text(json.dumps({
                    "configuration": {"writers": [1], "objects": [9], "faults": ["none"], "repetitions": 1},
                    "attempted_all": True,
                    "samples": [
                        {"status": "ok", "fresh_readback": True},
                        {"status": "failed", "fresh_readback": True},
                        {"status": "ok", "fresh_readback": True},
                    ],
                }))
                return {"status": "passed", "exit_code": 0}

            with mock.patch.object(runner, "execute", side_effect=run), \
                 mock.patch.object(runner.common, "environment_metadata", return_value={}):
                self.assertEqual(runner.main(["--suites", "pin-protocol-s3", "--output", str(output)]), 1)
            ledger = json.loads((output / "execution.json").read_text())
            self.assertFalse(ledger["complete"])
            self.assertEqual(ledger["entries"][0]["status"], "failed")
            self.assertEqual(ledger["entries"][0]["matrix"], {
                "expected_cases": 3, "recorded_cases": 3, "attempted_all": True,
                "failed_cases": 1, "unaudited_cases": 0,
            })

    def test_pack_probe_logs_do_not_collide_with_all_runner_logs(self):
        for name in ("git_pack_boundary", "git_pack_delayed"):
            suite = importlib.import_module(f"benchmarks.suites.{name}")
            with self.subTest(suite=name), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                binary = root / "probe"
                binary.write_bytes(b"probe fixture")
                output = root / "result.json"
                driver_log = output.with_suffix(".log")
                driver_log.write_text("all runner output\n")
                with mock.patch.object(suite.subprocess, "run", side_effect=RuntimeError("probe was started")):
                    with self.assertRaisesRegex(RuntimeError, "probe was started"):
                        suite.main(["--probe-binary", str(binary), "--output", str(output)])
                self.assertEqual(driver_log.read_text(), "all runner output\n")
                self.assertEqual(json.loads(output.read_text())["status"], "failed")

    def test_build_retains_git_fetch_and_cached_pack_examples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            names = ("git_fetch_s3", "git_pack_cached")
            for name in names:
                (root / name).write_bytes(name.encode())

            def build(command, **kwargs):
                if "--example" in command:
                    for name in names:
                        kwargs["stdout"].write(json.dumps({
                            "reason": "compiler-artifact",
                            "target": {"kind": ["example"], "name": name},
                            "executable": str(root / name),
                        }) + "\n")

            with mock.patch.object(runner.subprocess, "run", side_effect=build):
                binaries = runner.build_binaries(root, root / "build")
            artifacts = json.loads((root / "artifacts.json").read_text())
            for name in names:
                self.assertEqual((binaries / name).read_bytes(), name.encode())
                self.assertEqual(artifacts[name]["sha256"], runner.fingerprint(root / name))

    def test_transfer_holds_rejects_missing_cases_and_preserves_samples(self):
        rows = [{"transport": transport, "scope": scope, "payload_bytes": size,
                 "gc": gc, "correctness": "passed"}
                for transport in ("local", "ssh-stdio") for scope in ("snapshot", "selected")
                for size in (4096, 4194304) for gc in (False, True)]
        for supplied, expected in [(rows, 0), (rows[:-1], 1)]:
            with self.subTest(count=len(supplied)), tempfile.TemporaryDirectory() as directory:
                base = pathlib.Path(directory)
                binaries = base / "binaries"
                binaries.mkdir()
                output = base / "results"
                orders = []
                def run(command, log, timeout, environment):
                    orders.append("CASITA_BENCH_TRANSFER_REVERSE" in environment)
                    log.write_text("\n".join(json.dumps(row) for row in supplied))
                    return {"status": "passed", "exit_code": 0}
                with mock.patch.object(runner, "execute", side_effect=run), \
                     mock.patch.object(runner.common, "environment_metadata", return_value={}):
                    result = runner.main(["--suites", "transfer-holds", "--repetitions", "2",
                                          "--bin-dir", str(binaries), "--output", str(output)])
                self.assertEqual(result, expected)
                samples = json.loads((output / "transfer-holds.json").read_text())["samples"]
                self.assertEqual(len(samples), len(supplied) * (2 if expected == 0 else 1))
                if expected == 0:
                    self.assertEqual(orders, [False, True])

    def test_online_holds_standard_covers_both_sizes_and_all_application_scenarios(self):
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            binaries = base / "binaries"
            binaries.mkdir()
            output = base / "results"
            sizes = []
            def successful_run(command, log, timeout, environment):
                sizes.append(int(environment["CASITA_BENCH_IMPORTS"]))
                self.assertEqual(environment["CASITA_BENCH_FILES"], "16")
                self.assertEqual(environment["CASITA_BENCH_READER_SCOPE"], "application")
                self.assertNotIn("CASITA_BENCH_SCENARIO", environment)
                log.write_text("\n".join(json.dumps({"scenario": scenario, "imports": sizes[-1]})
                    for scenario in ["imports", "imports_readers", "imports_gc", "imports_readers_gc"]))
                return {"status": "passed", "exit_code": 0}
            with mock.patch.object(runner, "execute", side_effect=successful_run), \
                 mock.patch.object(runner.common, "environment_metadata", return_value={}), \
                 mock.patch.dict(runner.os.environ, {"CASITA_BENCH_SCENARIO": "imports", "CASITA_BENCH_READER_SCOPE": "snapshot"}):
                result = runner.main(["--suites", "online-holds", "--profile", "standard",
                                      "--repetitions", "2", "--bin-dir", str(binaries), "--output", str(output)])
            self.assertEqual(result, 0)
            self.assertEqual(sizes, [60, 60, 300, 300])
            self.assertTrue(json.loads((output / "execution.json").read_text())["complete"])
            samples = json.loads((output / "online-holds.json").read_text())["samples"]
            self.assertEqual(len(samples), 16)
            self.assertEqual({(row["imports"], row["repetition"]) for row in samples},
                             {(60, 0), (60, 1), (300, 0), (300, 1)})

    def test_online_holds_preserves_completed_samples_when_later_scenario_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            binaries = base / "binaries"
            binaries.mkdir()
            output = base / "results"
            def failed_run(command, log, timeout, environment):
                self.assertEqual(environment["CASITA_BENCH_IMPORTS"], "3")
                self.assertEqual(environment["CASITA_BENCH_FILES"], "2")
                log.write_text('{"scenario":"imports"}\ncollection failed\n')
                return {"status": "failed", "exit_code": 1}
            with mock.patch.object(runner, "execute", side_effect=failed_run), \
                 mock.patch.object(runner.common, "environment_metadata", return_value={}):
                result = runner.main(["--suites", "online-holds", "--profile", "smoke",
                                      "--bin-dir", str(binaries), "--output", str(output)])
            self.assertEqual(result, 1)
            self.assertFalse(json.loads((output / "execution.json").read_text())["complete"])
            self.assertEqual(json.loads((output / "online-holds.json").read_text())["samples"],
                             [{"scenario": "imports", "repetition": 0}])

    def test_every_cargo_benchmark_has_a_registered_runner(self):
        cargo = tomllib.loads((cli.ROOT / "crates/casita/Cargo.toml").read_text())
        declared = {target["name"] for target in cargo["bench"]}
        self.assertEqual(declared, set(runner.CORE_BENCHES) | {"gix_odb", "online_holds", "retained_readers", "transfer_holds", "root_prefix"})
        holds = next(entry for entry in cli.entrypoints() if entry["id"] == "online-holds")
        self.assertEqual(holds["target"][-2:], ["--bench", "online_holds"])
        core = next(entry for entry in cli.entrypoints() if entry["id"] == "core-primitives")
        targets = core["target"]
        selected = {targets[index + 1] for index, argument in enumerate(targets) if argument == "--bench"}
        self.assertEqual(selected, set(runner.CORE_BENCHES))

    def test_every_suite_has_a_bounded_configuration(self):
        self.assertEqual(set(runner.SMOKE) | {"core-primitives", "online-holds", "retained-readers", "transfer-holds", "remote-pin-cost", "root-prefix"}, {entry["id"] for entry in cli.entrypoints()})
        for entry in cli.entrypoints():
            identifier = entry["id"]
            if identifier in {"core-primitives", "online-holds", "retained-readers", "transfer-holds", "remote-pin-cost", "root-prefix"}:
                continue
            args = runner.suite_arguments(identifier, pathlib.Path("/binaries"), "smoke", 1)
            module = importlib.import_module(entry["target"])
            if hasattr(module, "build_parser"):
                extras = ["--s3-url", "s3://bucket/test"] if identifier == "s3-pack" else []
                if identifier == "nixpkgs":
                    extras += ["--nixpkgs", "/source"]
                module.build_parser().parse_args([*args, *extras, "--output", "/result.json"])

    def test_nonzero_and_timeout_have_explicit_results(self):
        with tempfile.TemporaryDirectory() as directory:
            log = pathlib.Path(directory) / "log"
            failed = runner.execute([sys.executable, "-c", "raise SystemExit(7)"], log, 10)
            self.assertEqual((failed["status"], failed["exit_code"]), ("failed", 7))
            timed = runner.execute([sys.executable, "-c", "import time; time.sleep(30)"], log, 0.05)
            self.assertEqual(timed["status"], "timeout")

    def test_registry_default_arguments_reach_the_module(self):
        module = mock.Mock()
        module.main.return_value = 0
        with mock.patch.object(cli.importlib, "import_module", return_value=module):
            cli.run_entrypoint({"kind": "module", "target": "unused", "id": "test", "default_arguments": ["--probe", "chosen"]}, ["--iterations", "3"])
        module.main.assert_called_once_with(["--probe", "chosen", "--iterations", "3"])
