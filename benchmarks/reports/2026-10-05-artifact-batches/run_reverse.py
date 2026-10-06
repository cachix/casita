"""Repeat the existing cases in reverse order with the same sampling settings."""
import argparse
import os
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=Path, required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parent
cases = re.findall(r'^Benchmarking (artifact_batches/[^:\n]+)$', (root / 'run-1.log').read_text(), re.M)
if len(cases) != 24 or len(set(cases)) != 24:
    raise SystemExit('expected exactly 24 original cases')
env = {**os.environ, 'CRITERION_HOME': str(root / 'criterion-run-3')}
with (root / 'run-3.log').open('w') as log:
    for case in reversed(cases):
        print(case, flush=True)
        subprocess.run([str(args.binary.resolve()), '--bench', '--exact', case,
            '--warm-up-time', '1', '--measurement-time', '3', '--noplot'],
            cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
