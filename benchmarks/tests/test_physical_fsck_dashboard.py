import json
from pathlib import Path
import tempfile
import unittest
from benchmarks import dashboard


class PhysicalFsckDashboard(unittest.TestCase):
    def test_full_physical_samples_are_supported_and_incomplete_is_rejected(self):
        result=dict(schema_version=1,result_schema='casita.physical-fsck.v1',
                    suite_id='collection-and-fsck',complete=True,configuration={'profile':'smoke'},
                    samples=[dict(status='ok',operation='full-physical-fsck',files=9,entries=10,
                                  variant='delay-ms-'+str(delay),delay_ms=delay,wall_seconds=.01)
                             for delay in [0,1]])
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'sample.json';p.write_text(json.dumps(result))
            v=dashboard.normalize_result(p)
            self.assertEqual(len(v['observations']),2)
            self.assertEqual(len({s['workload'] for s in v['observations']}),2)
            result['complete']=False;p.write_text(json.dumps(result))
            with self.assertRaises(ValueError):dashboard.normalize_result(p)
