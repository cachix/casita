import json
import pathlib
import tempfile
import unittest
from benchmarks import all as runner
from benchmarks.suites import repository as common


class RetainedWalTests(unittest.TestCase):
    def rows(self):
        return [dict(retention=mode, writes=count, payload_bytes=512,
                     write_seconds=count / 1000, peak_wal_bytes=8192,
                     wal_after_writes_bytes=4096, checkpoint_busy=mode == "snapshot",
                     wal_after_checkpoint_bytes=4096 if mode == "snapshot" else 0,
                     correctness="passed")
                for count in (32, 256, 1024) for mode in ("snapshot", "objects")]

    def collect(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "retained_wal.log"
            path.write_text("\n".join(json.dumps(row) for row in rows))
            return runner.retained_wal_samples(path)

    def test_wal_metrics_and_distinct_cases_reach_the_core_report(self):
        samples = self.collect(self.rows())
        self.assertEqual(len(samples), 6)
        self.assertEqual({sample["operation"] for sample in samples},
                         {f"retained-wal/{mode}/{count}" for count in (32, 256, 1024)
                          for mode in ("snapshot", "objects")})
        for sample in samples:
            self.assertEqual(sample["status"], "ok")
            self.assertEqual(sample["wall_seconds"], sample["write_seconds"])
            self.assertEqual(sample["peak_wal_bytes"], 8192)

    def test_collector_includes_wal_rows_alongside_criterion(self):
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            for name in runner.CORE_BENCHES:
                (output / f"{name}.log").write_text("")
            (output / "optimization.log").write_text("Benchmarking sample: Analyzing\n")
            (output / "environment.json").write_text("{}")
            (output / "retained_wal.log").write_text("\n".join(json.dumps(row) for row in self.rows()))
            case = output / "criterion" / "sample" / "new"
            case.mkdir(parents=True)
            (case / "benchmark.json").write_text(json.dumps({"full_id": "sample"}))
            (case / "estimates.json").write_text(json.dumps({"median": {"point_estimate": 1000}}))
            runner.collect_criterion(output, output / "criterion")
            report = json.loads((output / "core-primitives.json").read_text())
            self.assertEqual(len(report["samples"]), 7)
            self.assertEqual(sum(row["operation"].startswith("retained-wal/") for row in report["samples"]), 6)

    def test_missing_duplicate_and_failed_cases_are_rejected(self):
        rows = self.rows()
        for invalid in (rows[:-1], rows + rows[:1],
                        [dict(row, correctness="failed") for row in rows]):
            with self.subTest(rows=invalid):
                with self.assertRaises(common.BenchmarkError):
                    self.collect(invalid)

    def test_inconsistent_checkpoint_evidence_is_rejected(self):
        for change in ({"checkpoint_busy": True}, {"wal_after_checkpoint_bytes": 4096}):
            rows = self.rows()
            rows[1].update(change)
            with self.assertRaises(common.BenchmarkError):
                self.collect(rows)
