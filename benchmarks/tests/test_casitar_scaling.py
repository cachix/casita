import copy
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from benchmarks import all as runner
from benchmarks import dashboard
from benchmarks.suites import casitar_scaling as suite
from benchmarks.suites import repository as common


class CasitarScalingTests(unittest.TestCase):
    def test_all_builds_cli_for_each_archive_runner(self):
        for identifier in ("casitar-scaling", "casitar-import-profile",
                           "casitar-pin-profile", "casitar-quiet-import"):
            commands = runner.build_commands([identifier], pathlib.Path("/build"))
            self.assertEqual(len(commands), 1)
            command = commands[0]
            self.assertIn("--bin", command)
            self.assertEqual(command[command.index("--bin") + 1], "casita")

    def test_cpu_policy_accepts_compilers_below_ceiling(self):
        # Policy checks use synthetic samples and never read host activity.
        with mock.patch.object(pathlib.Path, "exists", return_value=True):
            monitor = suite.QuietHost(max_cpu_fraction=0.40, allow_competing_builds=True)
        for fraction, expected in [(0.0545, True), (0.10, True), (0.40, True), (0.4001, False)]:
            self.assertEqual(monitor.quiet({"external_cpu_fraction": fraction,
                                          "competing_processes": []}), expected)
        self.assertTrue(monitor.quiet({"external_cpu_fraction": 0.261,
                                       "competing_processes": [{"name": "rustc"}]}))
        self.assertFalse(monitor.quiet({"external_cpu_fraction": 0.401,
                                       "competing_processes": [{"name": "rustc"}]}))

    def test_cpu_ceiling_is_forwarded_and_invalid_values_are_rejected(self):
        with mock.patch.object(suite, "QuietHost") as factory, mock.patch.object(suite, "run_case"):
            factory.return_value.report.return_value = {"quiet": True}
            suite.guarded_case(suite.ArchiveAdapter("candidate"), pathlib.Path("/tmp"),
                               "object-count", 256, 128, 1, [], 1, 60, [], 7.5)
            factory.assert_called_once_with(timeout=60, max_cpu_fraction=0.075,
                                            allow_competing_builds=True)
        for value in ("nan", "inf", "-1", "101"):
            with self.subTest(value=value), mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                suite.main(["--max-external-cpu-percent", value, "--output", "/unused"])

    def test_quiet_case_retains_rejected_measurements_and_admission_evidence(self):
        adapter = suite.ArchiveAdapter("candidate")
        for contaminated in (False, True):
            samples, activity = [], []
            monitor = mock.MagicMock()
            monitor.report.return_value = {"quiet": not contaminated}
            monitor.sample.return_value = {"competing_processes": []}
            def run(*args):
                monitor.sample()
                args[6].append({"status": "ok"})
            with mock.patch.object(suite, "QuietHost", return_value=monitor), mock.patch.object(suite, "run_case", side_effect=run):
                if contaminated:
                    with self.assertRaisesRegex(common.BenchmarkError, "external CPU samples"):
                        suite.guarded_case(adapter, pathlib.Path("/tmp"), "object-count", 256, 128, 1, samples, 1, 60, activity)
                else:
                    suite.guarded_case(adapter, pathlib.Path("/tmp"), "object-count", 256, 128, 1, samples, 1, 60, activity)
            self.assertEqual(activity[0]["status"], "rejected" if contaminated else "accepted")
            self.assertEqual(samples[0]["status"], "rejected" if contaminated else "ok")
            self.assertEqual(len(activity[0]["intervals"]), 1)

    def test_quiet_timeout_saves_incomplete_evidence_without_running_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = pathlib.Path(temporary)
            binary = work / "casita"
            binary.write_bytes(b"binary")
            monitor = mock.MagicMock()
            monitor.__enter__.side_effect = common.BenchmarkError("host did not become quiet")
            with mock.patch.object(suite, "QuietHost", return_value=monitor), mock.patch.object(suite, "run_case") as run, mock.patch.object(suite.common, "environment_metadata", return_value={}):
                with self.assertRaisesRegex(common.BenchmarkError, "host did not become quiet"):
                    suite.main(["--require-quiet-host", "--casita-bin", str(binary), "--output", str(work / "result.json")])
                run.assert_not_called()
            saved = json.loads((work / "result.json").read_text())
            self.assertFalse(saved["complete"])
            self.assertEqual(saved["host_activity"][0]["status"], "rejected")
            with self.assertRaises(ValueError):
                dashboard.normalize_result(work / "result.json")

    def test_quiet_entrypoint_covers_boundary_cases_through_all(self):
        from benchmarks.cli import entrypoints
        entry = next(row for row in entrypoints() if row["id"] == "casitar-quiet-import")
        for profile in ("smoke", "standard"):
            args = suite.build_parser().parse_args([
                *entry["default_arguments"],
                *runner.suite_arguments(entry["id"], pathlib.Path("/bin"), profile, 4),
                "--output", "/result.json"])
            self.assertTrue(args.require_quiet_host)
            self.assertTrue(args.no_phase_timing)
            self.assertTrue(args.investigate_import)
            self.assertEqual(args.profile, profile)
            self.assertEqual(args.repetitions, 4)
            self.assertEqual(args.max_external_cpu_percent, 40)

    def test_pin_budget_rejects_missing_batching_and_per_record_syncs(self):
        profile = {"phases": [{"phase": "protect_records", "calls": 2}]}
        suite.validate_pin_budget(profile, {"journal_append_sync": {"calls": 32}}, 257)
        for phases, calls in [({"phases": []}, 8), (profile, 33), (profile, 262)]:
            with self.assertRaises(common.BenchmarkError):
                suite.validate_pin_budget(phases, {"journal_append_sync": {"calls": calls}}, 257)
        suite.validate_pin_budget({"phases": []}, {"journal_append_sync": {"calls": 262}}, 257, "baseline")

    def test_paired_run_alternates_binaries_and_disables_phase_timing(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = pathlib.Path(temporary)
            for name in ("baseline", "candidate"):
                (work / name).write_bytes(name.encode())
            seen = []
            def record(adapter, directory, family, count, size, repetitions, samples, repetition):
                seen.append((count, adapter.variant, repetition))
                self.assertFalse(adapter.import_profile)
                samples.append({"variant": adapter.variant, "repetition": repetition})
            with mock.patch.object(suite.common, "environment_metadata", return_value={}), mock.patch.object(suite, "run_case", side_effect=record):
                suite.main(["--investigate-import", "--no-phase-timing", "--file-counts", "254,256",
                            "--baseline-bin", str(work / "baseline"), "--casita-bin", str(work / "candidate"),
                            "--repetitions", "2", "--output", str(work / "result.json")])
            self.assertEqual(seen, [(254, "baseline", 1), (254, "candidate", 1),
                                    (256, "baseline", 1), (256, "candidate", 1),
                                    (256, "candidate", 2), (256, "baseline", 2),
                                    (254, "candidate", 2), (254, "baseline", 2)])
            result = json.loads((work / "result.json").read_text())
            self.assertTrue(result["complete"])
            self.assertFalse(result["configuration"]["import_profile"])
            self.assertNotEqual(result["configuration"]["binary_sha256"], result["configuration"]["baseline"]["sha256"])

    def test_pin_trace_aggregation_preserves_counts_and_durations(self):
        event = {"target": "casita::pin_timing", "fields": {
            "phase": "journal_append_sync", "elapsed_seconds": 0.25}}
        lines = "\n".join([json.dumps(event), "casitar_import_profile {}", json.dumps(event)])
        self.assertEqual(suite.pin_profile(lines), {"journal_append_sync": {
            "calls": 2, "seconds": 0.5, "max_seconds": 0.25}})
        with self.assertRaises(common.BenchmarkError):
            suite.pin_profile("casitar_import_profile {}")

    def test_failed_run_retains_completed_samples_as_incomplete(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = pathlib.Path(temporary) / "result.json"
            binary = pathlib.Path(temporary) / "casita"
            binary.write_bytes(b"test binary")
            def fail(*args):
                args[6].append({"status": "ok", "operation": "create"})
                raise common.BenchmarkError("gate failed")
            with mock.patch.object(suite.common, "environment_metadata", return_value={}), mock.patch.object(suite, "run_case", side_effect=fail):
                with self.assertRaises(common.BenchmarkError):
                    suite.main(["--casita-bin", str(binary), "--output", str(output)])
            saved = json.loads(output.read_text())
            self.assertFalse(saved["complete"])
            self.assertEqual(len(saved["samples"]), 1)
            with self.assertRaises(ValueError):
                dashboard.normalize_result(output)

    def test_phase_profile_requires_exact_counts_and_single_report(self):
        report = {"stats": {"payloads": 257, "records": 257},
                  "payloads_written": 0, "payloads_reused": 257}
        counts = {"payload_lookup": 257, "stage_existing": 257,
                  "payload_reuse_verify": 257, "publish_batch": 1, "publish_tail": 1,
                  "verify_closure": 1, "publish_roots": 1}
        profile = {"schema_version": 1, "phases": [
            {"phase": phase, "calls": count, "nanos": 10} for phase, count in counts.items()]}
        line = "casitar_import_profile " + json.dumps(profile)
        self.assertEqual(suite.validate_profile(line, report), profile)
        batched = copy.deepcopy(profile)
        batched["phases"].append({"phase": "protect_records", "calls": 2, "nanos": 1})
        suite.validate_profile("casitar_import_profile " + json.dumps(batched), report)
        batched["phases"][-1]["calls"] = 1
        with self.assertRaises(common.BenchmarkError):
            suite.validate_profile("casitar_import_profile " + json.dumps(batched), report)
        for stderr in ("", line + "\n" + line,
                       line.replace('"calls": 257', '"calls": 256', 1)):
            with self.assertRaises(common.BenchmarkError):
                suite.validate_profile(stderr, report)

    def test_import_profiler_is_registered_for_all_profiles(self):
        from benchmarks.cli import entrypoints
        entries = {entry["id"]: entry for entry in entrypoints()}
        for identifier in ("casitar-import-profile", "casitar-pin-profile"):
            for profile in ("smoke", "standard"):
                args = suite.build_parser().parse_args([
                    *entries[identifier]["default_arguments"],
                    *runner.suite_arguments(identifier, pathlib.Path("/bin"), profile, 3),
                    "--output", "/result.json"])
                self.assertTrue(args.investigate_import)
                self.assertEqual(args.profile, profile)
                self.assertEqual(args.pin_timing, identifier == "casitar-pin-profile")

    def test_dashboard_keeps_pin_traces_separate_and_aggregates_phases(self):
        row = {"status": "ok", "operation": "import", "family": "object-count",
               "files": 256, "file_bytes": 128, "seeded_file_percent": 100,
               "wall_seconds": 2, "import_profile": {"phases": [
                   {"phase": "stage_existing", "calls": 257, "nanos": 1000000000}]},
               "pin_profile": {"journal_append_sync": {"seconds": 0.5, "calls": 257}}}
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "result.json"
            path.write_text(json.dumps({"result_schema": "casita.casitar-scaling.v1",
                "suite_id": "casitar", "configuration": {"import_profile": True, "pin_timing": True, "require_quiet_host": True},
                "samples": [row, row]}))
            observation = dashboard.normalize_result(path)["observations"][0]
        self.assertTrue(observation["scale"]["pin_timing"])
        self.assertTrue(observation["scale"]["require_quiet_host"])
        self.assertEqual(observation["scale"]["max_external_cpu_percent"], 5)
        self.assertEqual(observation["metrics"]["phase_stage_existing_seconds"], 1)
        self.assertEqual(observation["metrics"]["pin_journal_append_sync_seconds"], 0.5)

    def test_normalization_keeps_size_and_reuse_cases_separate(self):
        samples = [{"operation": "import", "status": "ok", "family": "object-count",
                    "files": files, "file_bytes": 128, "seeded_file_percent": reuse,
                    "repetition": repetition, "wall_seconds": 1, "max_rss_bytes": 100,
                    "user_seconds": 0.1, "system_seconds": 0.2}
                   for files in (254, 256) for reuse in (0, 50, 100) for repetition in (1, 2)]
        with tempfile.TemporaryDirectory() as temporary:
            result = pathlib.Path(temporary) / "result.json"
            result.write_text(json.dumps({"result_schema": "casita.casitar-scaling.v1",
                                         "suite_id": "casitar", "configuration": {"profile": "smoke"},
                                         "samples": samples}))
            observations = dashboard.normalize_result(result)["observations"]
        self.assertEqual(len(observations), 6)
        self.assertEqual({row["samples"] for row in observations}, {2})
        self.assertEqual({(row["scale"]["files"], row["scale"]["seeded_file_percent"]) for row in observations},
                         {(files, reuse) for files in (254, 256) for reuse in (0, 50, 100)})
        self.assertEqual(observations[0]["metrics"]["user_seconds"], 0.1)

    def test_archive_reports_are_not_polluted_by_pack_diagnostics(self):
        with mock.patch.dict(suite.os.environ, {"CASITA_PACK_STATS": "1", "CASITA_CASITAR_IMPORT_PROFILE": "1"}):
            adapter = suite.ArchiveAdapter("casita")
            self.assertNotIn("CASITA_PACK_STATS", adapter.env())
            self.assertNotIn("CASITA_CASITAR_IMPORT_PROFILE", adapter.env())
            adapter.import_profile = True
            self.assertEqual(adapter.env()["CASITA_CASITAR_IMPORT_PROFILE"], "1")

    def test_profiles_reach_all_runner_and_cover_boundaries(self):
        for profile in suite.PROFILES:
            args = suite.build_parser().parse_args([
                *runner.suite_arguments("casitar-scaling", pathlib.Path("/bin"), profile, 2),
                "--output", "/result.json",
            ])
            self.assertEqual(args.profile, profile)
            self.assertEqual(args.repetitions, 2)
            sizes = suite.PROFILES[profile]["payload_bytes"]
            counts = suite.PROFILES[profile]["file_counts"]
            self.assertLess(min(sizes), 65536)
            self.assertGreater(max(sizes), 65536)
            self.assertIn(254, counts)
            self.assertIn(256, counts)

    def test_fixture_is_distinct_deterministic_and_restore_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            work = pathlib.Path(temporary)
            first = suite.fixture(work / "first", 2, 65537)
            second = suite.fixture(work / "second", 2, 65537)
            self.assertEqual(first, second)
            self.assertEqual(len({value["sha256"] for value in first.values()}), 2)
            suite.validate_checkout(work / "first", first)
            path = work / "first" / next(iter(first))
            with path.open("r+b") as output:
                output.write(b"corrupt")
            with self.assertRaises(common.BenchmarkError):
                suite.validate_checkout(work / "first", first)

    def test_reuse_gate_rejects_wrong_destination_or_digest(self):
        stats = {"payloads": 3, "records": 3, "archive_digest": "expected"}
        report = {"schema": "casita.archive.v1", "operation": "import", "validity": "imported",
                  "stats": stats, "mappings": [{"index": 0, "name": "bench/received", "root": "key"}],
                  "payloads_written": 2, "payloads_reused": 1, "records_inserted": 2, "records_reused": 1}
        suite.validate_report(report, stats, "key", "import", 50)
        for reuse in (0, 100):
            with self.assertRaises(common.BenchmarkError):
                suite.validate_report(report, stats, "key", "import", reuse)
        for field, value in [("stats", {**stats, "archive_digest": "wrong"}), ("mappings", [])]:
            bad = copy.deepcopy(report)
            bad[field] = value
            with self.assertRaises(common.BenchmarkError):
                suite.validate_report(bad, stats, "key", "import", 50)

    def test_corruption_changes_only_payload_body(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = pathlib.Path(temporary) / "fixture.casitar"
            prefix = b"casitar1" + (3).to_bytes(8, "little") + b"hdr"
            frame = b"\x01" + bytes(32) + (4).to_bytes(8, "little")
            original = prefix + frame + b"body\x00"
            archive.write_bytes(original)
            offset, byte = suite.corrupt_payload(archive)
            damaged = archive.read_bytes()
            self.assertEqual(offset, len(prefix + frame))
            self.assertEqual(byte, b"b")
            self.assertEqual([i for i, pair in enumerate(zip(original, damaged)) if pair[0] != pair[1]], [offset])
            with archive.open("r+b") as output:
                output.seek(offset)
                output.write(byte)
            self.assertEqual(archive.read_bytes(), original)
