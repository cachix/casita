"""Full-byte physical fsck under controlled per-read storage latency.

The in-memory fixture models I/O waiting; it is not a provider or paced Relay
qualification. Every sample also runs the complete logical audit and rejects
physical pack corruption, outside timing.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
from benchmarks import cli
from benchmarks.suites import repository as common
from benchmarks.suites.metadata_collection import CARGO_ARGUMENTS
from benchmarks.suites.pack.catalog import parse_probe_binary

PROBE = 'repository::physical_fsck_tests::benchmark_physical_fsck_latency'
COUNTS = {'smoke': [6, 9], 'standard': [6, 7, 8, 9, 166, 840]}
FIELDS = {'files','delay_ms','nanos','payloads_checked','bytes_checked','outboards_checked','peak_storage_reads',
          'full_logical_clean','corruption_rejected'}


def parse_sample(stdout, files, delay_ms):
    try:
        rows = [json.loads(line.removeprefix('physical_fsck_sample '))
                for line in stdout.splitlines() if line.startswith('physical_fsck_sample ')]
        if len(rows) != 1 or 'test result: ok. 1 passed; 0 failed;' not in stdout:
            raise ValueError('physical fsck probe did not pass exactly once')
        row = rows[0]
        if not isinstance(row,dict) or set(row) != FIELDS:
            raise ValueError('physical fsck probe schema differs')
        if (row['files'] != files or row['delay_ms'] != delay_ms
                or row['payloads_checked'] != files+1
                or row['outboards_checked'] < (files+3)//4
                or row['full_logical_clean'] is not True or row['corruption_rejected'] is not True
                or any(type(row[k]) is not int or row[k] < 0 for k in FIELDS-{'full_logical_clean','corruption_rejected'})
                or row['bytes_checked'] <= files*30 or row['peak_storage_reads'] < 1):
            raise ValueError('physical fsck probe lost full inventory or corruption gates')
        return row
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError('invalid physical fsck result') from error


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', choices=COUNTS, default='standard')
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--probe-binary', type=Path)
    parser.add_argument('--no-build', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args(argv)
    if args.repetitions < 1 or args.no_build and args.probe_binary is None:
        parser.error('positive repetitions and a probe binary for --no-build are required')
    binary = args.probe_binary
    if binary is None:
        built = subprocess.run(['cargo', *CARGO_ARGUMENTS], cwd=cli.ROOT, capture_output=True, text=True)
        if built.returncode:
            raise common.BenchmarkError('physical fsck probe compilation failed')
        binary = parse_probe_binary(built.stdout)
    binary = binary.resolve()
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    result = dict(schema_version=1, result_schema='casita.physical-fsck.v1',
        suite_id='collection-and-fsck', complete=False,
        configuration=dict(profile=args.profile, files=COUNTS[args.profile], delay_ms=[0,1],
                           repetitions=args.repetitions, storage='synthetic delayed in-memory'),
        artifacts=[dict(path=str(binary), sha256=digest)], samples=[], processes=[])
    def save():
        common.write_atomic(args.output, json.dumps(result,indent=2)+'\n')
        if args.report:
            common.write_atomic(args.report, '# Physical fsck latency\n\nComplete: '+str(result['complete'])+
                '\n\nSynthetic storage wait model; full logical and corruption gates retained.\n')
    save()
    try:
        for repetition in range(1,args.repetitions+1):
            for files in COUNTS[args.profile]:
                for delay in [0,1]:
                    env = dict(os.environ,CASITA_PHYSICAL_FSCK_FILES=str(files),CASITA_PHYSICAL_FSCK_DELAY_MS=str(delay))
                    process = subprocess.run([str(binary),PROBE,'--exact','--ignored','--nocapture'],
                        env=env,capture_output=True,text=True,timeout=180)
                    result['processes'].append(dict(files=files,delay_ms=delay,repetition=repetition,
                        exit_code=process.returncode,stdout=process.stdout,stderr=process.stderr))
                    if process.returncode:
                        raise common.BenchmarkError('physical fsck native probe failed')
                    row = parse_sample(process.stdout,files,delay)
                    result['samples'].append(dict(row,status='ok',operation='full-physical-fsck',
                        entries=files+1,variant='delay-ms-'+str(delay),repetition=repetition,wall_seconds=row['nanos']/1e9))
                    save()
        if hashlib.sha256(binary.read_bytes()).hexdigest() != digest:
            raise common.BenchmarkError('physical fsck probe binary changed')
        result['complete'] = True
    except BaseException as error:
        result['error_type'] = type(error).__name__
        raise
    finally:
        save()
    return 0


if __name__=='__main__':
    raise SystemExit(main())
