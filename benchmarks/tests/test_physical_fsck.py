import importlib
import json
import unittest
from benchmarks import cli
from benchmarks import all as all_suites


class PhysicalFsckTests(unittest.TestCase):
    def module(self):
        self.assertTrue(any(e['id']=='physical-fsck' for e in cli.entrypoints()),
                        'full physical fsck latency benchmark must be permanent')
        return importlib.import_module('benchmarks.suites.physical_fsck')

    def test_registered_in_all_with_probe_and_both_sides_of_read_window(self):
        module=self.module()
        self.assertEqual(module.COUNTS['standard'], [6,7,8,9,166,840])
        commands=all_suites.build_commands(['physical-fsck'], '/synthetic/build')
        self.assertTrue(any('--lib' in c for c in commands))
        args=all_suites.suite_arguments('physical-fsck', __import__('pathlib').Path('/synthetic/bin'), 'smoke', 1)
        self.assertIn('--probe-binary', args)

    def test_probe_requires_exact_inventory_and_full_corruption_gates(self):
        module=self.module()
        sample=dict(files=840,delay_ms=1,nanos=123,payloads_checked=841,bytes_checked=45000,
                    outboards_checked=210,peak_storage_reads=8,full_logical_clean=True,corruption_rejected=True)
        def raw(value):
            return 'physical_fsck_sample '+json.dumps(value)+'\ntest result: ok. 1 passed; 0 failed;\n'
        self.assertEqual(module.parse_sample(raw(sample),840,1),sample)
        for field,value in [('payloads_checked',840),('outboards_checked',0),('full_logical_clean',False),
                            ('corruption_rejected',False),('nanos',-1)]:
            with self.subTest(field=field),self.assertRaises(ValueError):
                module.parse_sample(raw(dict(sample,**{field:value})),840,1)
        for invalid in [raw(sample)*2,raw(sample).replace('1 passed','0 passed')]:
            with self.assertRaises(ValueError):module.parse_sample(invalid,840,1)
