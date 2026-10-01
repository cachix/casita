#!/usr/bin/env python3
"""Summarize complete paired wide graph A/B runs without discarding samples."""

import argparse
import hashlib
import json
import random
import statistics
from pathlib import Path


def percentile(values, fraction):
    at = (len(values) - 1) * fraction
    lo = int(at)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (at - lo)


def summarize(directory):
    metadata = json.loads((directory / 'metadata.json').read_text())
    if not metadata['complete']:
        raise ValueError(f'incomplete run: {directory}')
    samples = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
    result = {'metadata': metadata, 'samples_sha256': hashlib.sha256(
        (directory / 'samples.jsonl').read_bytes()).hexdigest(), 'modes': {}}
    for mode in sorted({sample['sandbox'] for sample in samples}):
        rows = [sample for sample in samples if sample['sandbox'] == mode]
        if len(rows) != metadata['rounds'] * 2:
            raise ValueError('missing paired samples')
        pairs = []
        for repetition in range(metadata['rounds']):
            variants = {}
            for row in rows:
                if row['round'] == repetition:
                    if row['variant'] in variants:
                        raise ValueError('duplicate variant in pair')
                    variants[row['variant']] = row
            a, b = variants['A'], variants['B']
            if a['outputs'] != b['outputs']:
                raise ValueError('paired outputs differ')
            for row in (a, b):
                if row['builds'] != metadata['builds'] or row['reopen_builds'] != 0:
                    raise ValueError('incorrect build count')
            pairs.append({'round': repetition, 'A_wall_ms': a['wall_ms'],
                          'B_wall_ms': b['wall_ms'], 'A_graph_ms': a['graph_ms'],
                          'B_graph_ms': b['graph_ms'],
                          'wall_reduction_percent': 100 * (a['wall_ms'] - b['wall_ms']) / a['wall_ms'],
                          'graph_reduction_percent': 100 * (a['graph_ms'] - b['graph_ms']) / a['graph_ms']})
        stats = {'pairs': pairs, 'timed_builds': len(rows) * metadata['builds']}
        for timer in ('wall', 'graph'):
            values = [pair[timer + '_reduction_percent'] for pair in pairs]
            rng = random.Random(20260930)
            resamples = sorted(statistics.median(rng.choices(values, k=len(values)))
                               for _ in range(20000))
            stats[timer] = {'median_paired_reduction_percent': statistics.median(values),
                            'bootstrap_95_percent': [percentile(resamples, .025),
                                                     percentile(resamples, .975)],
                            'candidate_faster': sum(value > 0 for value in values),
                            'A_median_ms': statistics.median(row[timer + '_ms'] for row in rows if row['variant'] == 'A'),
                            'B_median_ms': statistics.median(row[timer + '_ms'] for row in rows if row['variant'] == 'B')}
        result['modes'][mode] = stats
    result['bootstrap'] = {'resamples': 20000, 'seed': 20260930,
                           'method': 'paired percentile, interpolated quantiles'}
    result['all_stderr_empty'] = all(path.stat().st_size == 0 for path in directory.glob('*.stderr'))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    encoded = json.dumps(summarize(args.run), indent=2) + '\n'
    if args.output:
        args.output.write_text(encoded)
    else:
        print(encoded, end='')


if __name__ == '__main__':
    main()
