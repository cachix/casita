#!/usr/bin/env python3
"""Pair release Casita catalog-maintenance probes with permanent correctness gates."""

import argparse
import hashlib
import json
import os
import random
import re
import statistics
import subprocess
import time
from pathlib import Path

TEST = 'blob::pack::benchmarks::benchmark_catalog_reclaim_marker_probe'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def activity():
    result = {'loadavg': os.getloadavg()}
    for kind in ('io', 'cpu'):
        path = Path('/proc/pressure')/kind
        if path.exists():
            result[kind+'_pressure'] = path.read_text().strip()
    return result


def interval(values):
    rng = random.Random(20261001)
    resamples = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(20000))
    def quantile(p):
        at = (len(resamples)-1)*p
        lo = int(at)
        return resamples[lo]+(resamples[min(lo+1, len(resamples)-1)]-resamples[lo])*(at-lo)
    return [quantile(.025), quantile(.975)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--iterations', type=int, default=32)
    parser.add_argument('--rounds', type=int, default=8)
    parser.add_argument('--base-revision', required=True)
    parser.add_argument('--head-revision', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    if min(args.iterations, args.rounds) < 1:
        parser.error('counts must be positive')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    binaries = {'A': args.baseline.resolve(), 'B': args.candidate.resolve()}
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('CASITA_BENCH_') and key != 'RUST_LOG'}
    env['CASITA_CATALOG_MARKER_BENCH_ITERATIONS'] = str(args.iterations)
    metadata = {'complete': False, 'iterations': args.iterations, 'rounds': args.rounds,
                'base_revision': args.base_revision, 'head_revision': args.head_revision,
                'binaries': {v: {'path': str(p), 'sha256': digest(p)} for v, p in binaries.items()},
                'runner_sha256': digest(Path(__file__)), 'warmups': [], 'event_logging': False,
                'test': TEST, 'bootstrap': {'resamples': 20000, 'seed': 20261001}}
    def save():
        (args.output_dir/'metadata.json').write_text(json.dumps(metadata, indent=2)+'\n')
    def trial(variant, label):
        before = activity()
        started = time.perf_counter()
        result = subprocess.run([str(binaries[variant]), TEST, '--exact', '--ignored', '--nocapture', '--test-threads=1'],
                                env=env, text=True, capture_output=True, timeout=300)
        wall = time.perf_counter()-started
        (args.output_dir/(label+'.stdout')).write_text(result.stdout)
        (args.output_dir/(label+'.stderr')).write_text(result.stderr)
        if result.returncode or '1 passed' not in result.stdout:
            raise RuntimeError(result.stdout[-3000:]+result.stderr[-3000:])
        metrics = {key: int(value) for key, value in re.findall(r'\b(catalog_marker_[a-z0-9_]+) ([0-9]+)$', result.stdout, re.MULTILINE)}
        if metrics['catalog_marker_iterations'] != args.iterations:
            raise ValueError('incorrect iteration count')
        for key in ('catalog_marker_existing_pin_operations', 'catalog_marker_existing_pin_journal_syncs'):
            if metrics[key] != args.iterations:
                raise ValueError('pin admission counts changed')
        expected = args.iterations if variant == 'A' else 0
        if metrics['catalog_marker_existing_inode_replacements'] != expected:
            raise ValueError('unexpected marker replacement count')
        return {'variant': variant, 'wall_s': wall, 'metrics': metrics,
                'activity_before': before, 'activity_after': activity()}
    save()
    rows = []
    try:
        for variant in ('A', 'B'):
            metadata['warmups'].append(trial(variant, 'warmup-'+variant))
            save()
        with (args.output_dir/'samples.jsonl').open('w') as output:
            for repetition in range(args.rounds):
                for position, variant in enumerate(('A', 'B') if repetition%2 == 0 else ('B', 'A')):
                    row = trial(variant, f'{repetition}-{variant}')
                    row.update(round=repetition, position=position)
                    rows.append(row)
                    output.write(json.dumps(row)+'\n')
                    output.flush()
                    print(json.dumps(row), flush=True)
        summary = {}
        for metric in ('catalog_marker_create_nanos_per_publication', 'catalog_marker_existing_pinned_nanos_per_publication'):
            pairs = []
            for repetition in range(args.rounds):
                a, b = [next(r['metrics'][metric] for r in rows if r['round']==repetition and r['variant']==v) for v in ('A', 'B')]
                pairs.append({'round': repetition, 'A_nanos': a, 'B_nanos': b,
                              'reduction_percent': 100*(a-b)/a})
            values = [p['reduction_percent'] for p in pairs]
            summary[metric] = {'pairs': pairs, 'median_paired_reduction_percent': statistics.median(values),
                               'bootstrap_95_percent': interval(values), 'candidate_faster': sum(v>0 for v in values),
                               'A_median_nanos': statistics.median(p['A_nanos'] for p in pairs),
                               'B_median_nanos': statistics.median(p['B_nanos'] for p in pairs)}
        (args.output_dir/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
        metadata['complete'] = True
    except Exception as error:
        metadata['error'] = str(error)
        raise
    finally:
        save()


if __name__ == '__main__':
    main()
