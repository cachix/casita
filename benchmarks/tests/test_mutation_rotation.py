import json
import unittest

from benchmarks.suites.mutation_rotation import CORRECTNESS, PROBE, parse_sample, profile_defaults
from benchmarks.suites.repository import BenchmarkError


class MutationRotationTest(unittest.TestCase):
    def test_smoke_is_lighter_but_keeps_the_first_boundary(self):
        self.assertEqual(profile_defaults("smoke"), ([7, 8, 9], 1))
        self.assertEqual(profile_defaults("standard"), ([7, 8, 9, 16, 17], 4))

    def output(self, **changes):
        sample = dict(count=9, mode="rotate", eligible=True, batch_size=64,
                      group_size=8, admissions=1, collections=1,
                      peak_writer_objects=512, nanos=100, correctness=CORRECTNESS)
        sample.update(changes)
        return 'mutation_rotation_sample ' + json.dumps(sample) + '\ntest result: ok. 1 passed; 0 failed;'

    def test_accepts_bounded_success(self):
        self.assertEqual(parse_sample(self.output(), 9, "rotate", True)['admissions'], 1)

    def test_accepts_libtest_status_prefix(self):
        parse_sample(f"test {PROBE} ... " + self.output(), 9, "rotate", True)

    def test_rejects_wrong_json_types(self):
        for changes in (dict(eligible=1), dict(admissions=True)):
            with self.subTest(changes=changes), self.assertRaises(BenchmarkError):
                parse_sample(self.output(**changes), 9, "rotate", True)

    def test_rejects_non_object_or_malformed_json(self):
        for payload in ('[]', 'garbled'):
            with self.subTest(payload=payload), self.assertRaises(BenchmarkError):
                parse_sample('mutation_rotation_sample ' + payload + '\ntest result: ok. 1 passed; 0 failed;',
                             9, "rotate", True)

    def test_rejects_wrong_bounds_admission_or_case(self):
        for changes in (dict(peak_writer_objects=576), dict(admissions=2),
                        dict(collections=2), dict(count=8), dict(mode="sessions"),
                        dict(eligible=False), dict(batch_size=32), dict(group_size=4),
                        dict(correctness="unchecked"), dict(nanos=-1), dict(nanos=True)):
            with self.subTest(changes=changes), self.assertRaises(BenchmarkError):
                parse_sample(self.output(**changes), 9, "rotate", True)

    def test_rejects_missing_or_duplicate_result(self):
        for output in (self.output().split('\n')[0], self.output() + '\n' + self.output()):
            with self.assertRaises(BenchmarkError):
                parse_sample(output, 9, "rotate", True)

    def test_accepts_independent_admissions(self):
        parse_sample(self.output(mode="sessions", admissions=2, collections=2), 9, "sessions", True)

    def test_accepts_long_session_resource_growth(self):
        parse_sample(self.output(mode="long", peak_writer_objects=576, eligible=False,
                                 collections=0), 9, "long", False)
