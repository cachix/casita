import argparse
import unittest
from unittest import mock

from benchmarks import affinity
from benchmarks.suites.repository import BenchmarkError


class AffinityTests(unittest.TestCase):
    def test_cpu_list_sorts_and_deduplicates_nonnegative_ids(self):
        self.assertEqual(affinity.cpu_list('3,0,3,2'), [0, 2, 3])
        for value in ('', '1,', 'a', '-1', '1,-2'):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                affinity.cpu_list(value)

    def test_no_affinity_is_a_noop(self):
        with mock.patch.object(affinity, 'os') as operating_system:
            with affinity.cpu_affinity(None):
                pass
            operating_system.sched_getaffinity.assert_not_called()
            operating_system.sched_setaffinity.assert_not_called()

    def test_unsupported_platform_is_rejected(self):
        with mock.patch.object(affinity, 'os', spec=[]):
            with self.assertRaisesRegex(BenchmarkError, 'not supported'):
                with affinity.cpu_affinity([0]):
                    self.fail('unsupported affinity entered body')

    def test_disallowed_cpu_is_rejected_before_setting_affinity(self):
        with mock.patch.object(affinity, 'os') as operating_system:
            operating_system.sched_getaffinity.return_value = {1, 2}
            with self.assertRaisesRegex(BenchmarkError, 'outside the allowed'):
                with affinity.cpu_affinity([0, 1]):
                    self.fail('invalid affinity entered body')
            operating_system.sched_setaffinity.assert_not_called()

    def test_affinity_restores_on_normal_and_exceptional_exit(self):
        for fails in (False, True):
            with self.subTest(fails=fails), mock.patch.object(affinity, 'os') as operating_system:
                operating_system.sched_getaffinity.return_value = {0, 1, 2}
                try:
                    with affinity.cpu_affinity([1]):
                        operating_system.sched_setaffinity.assert_called_once_with(0, [1])
                        if fails:
                            raise RuntimeError('probe failed')
                except RuntimeError as error:
                    self.assertTrue(fails)
                    self.assertEqual(str(error), 'probe failed')
                operating_system.sched_setaffinity.assert_has_calls([
                    mock.call(0, [1]), mock.call(0, {0, 1, 2}),
                ])


if __name__ == '__main__':
    unittest.main()
