import json
import unittest

from benchmarks.suites.pack.demand_latency import parse_samples
from benchmarks.suites.repository import BenchmarkError


class DemandLatencyGates(unittest.TestCase):
    def samples(self):
        common = dict(file_bytes=1024, get_delay_ms=20, cache_bytes=4096,
                      pack_bytes=1024, nanos=1000000, calibration_nanos=20000000,
                      digest="a" * 64, pack_read_bytes=1024, cache_hits=0,
                      readahead_deferrals=0, buffer_bypasses=0,
                      correctness="exact bytes and independent BLAKE3")
        return [dict(common, phase="cold", pack_requests=1),
                dict(common, phase="warm", pack_requests=0, pack_read_bytes=0)]

    def parse(self, rows):
        stdout = "\n".join("latency_sample " + json.dumps(row) for row in rows)
        return parse_samples(stdout, 1024, 20, 4096, 1024)

    def test_verified_pair_passes(self):
        self.assertEqual(len(self.parse(self.samples())), 2)

    def test_incomplete_or_unverified_pair_fails(self):
        with self.assertRaises(BenchmarkError):
            self.parse(self.samples()[:1])
        for phase, key, value in [(0, "correctness", "unchecked"),
                                  (0, "file_bytes", 2048),
                                  (0, "calibration_nanos", 1000000),
                                  (0, "pack_requests", 0),
                                  (1, "pack_requests", 1),
                                  (1, "digest", "b" * 64)]:
            with self.subTest(phase=phase, key=key):
                rows = self.samples()
                rows[phase][key] = value
                with self.assertRaises(BenchmarkError):
                    self.parse(rows)
