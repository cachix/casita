import itertools
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from benchmarks import all as runner
from benchmarks import cli, dashboard, revisions
from benchmarks.suites import git_ingest_scheduling as suite
from benchmarks.suites import git_ingest_concurrency as native
from benchmarks.suites.git import git_env
from benchmarks.suites import repository as common

# A fake CLI that serves each imported view from its Git source.
FAKE_CASITA = r'''import base64, json, os, pathlib, subprocess, sys, tempfile
assert sys.argv[1] == "--repository"
repository, command = pathlib.Path(sys.argv[2]), sys.argv[3:]
state = repository / "state.json"

def git(source, *arguments, **options):
    return subprocess.run(["git", f"--git-dir={source}", *arguments], check=True, capture_output=True, text=True, **options).stdout

if command == ["init"]:
    repository.mkdir()
elif command[0] == "import":
    source = command[1]
    tip = bytes.fromhex(git(source, "rev-parse", "HEAD").strip())
    tip = "git.sha1.commit.v1:" + base64.urlsafe_b64encode(tip).decode().rstrip("=")
    state.write_text(json.dumps(dict(source=source, tip=tip)))
    print("objects", git(source, "rev-list", "--objects", "--all", "--count").strip())
elif command == ["root", "ls"]:
    print(f"git.view.v1:{json.loads(state.read_text())['tip']} git/bench")
elif command == ["git", "show", "bench"]:
    tip = json.loads(state.read_text())["tip"]
    print(f"view git.view.v1:{tip}\nref refs/heads/main {tip}")
elif command[:2] == ["git", "checkout"]:
    tree = base64.urlsafe_b64decode(command[2].split(":", 1)[1] + "=").hex()
    destination = pathlib.Path(command[3])
    destination.mkdir()
    with tempfile.TemporaryDirectory() as index:
        environment = {**os.environ, "GIT_INDEX_FILE": index + "/index"}
        source = json.loads(state.read_text())["source"]
        git(source, f"--work-tree={destination}", "read-tree", tree, env=environment)
        git(source, f"--work-tree={destination}", "checkout-index", "-a", env=environment)
elif command[0] != "fsck":
    sys.exit(f"unexpected command {command}")
'''


def write_fake_casita(path):
    path.write_text('#!' + sys.executable + '\n' + FAKE_CASITA)
    path.chmod(0o755)


def workload(row):
    entrypoint, scale = row['workload'].split(':', 1)
    return entrypoint, json.loads(scale)


def option_value(arguments, option):
    """The value argparse keeps for `option`: its last occurrence's."""
    return arguments[len(arguments) - arguments[::-1].index(option)]


class GitIngestTests(unittest.TestCase):
    def test_revision_reports_keep_every_view_workload_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            series = [revisions.RevisionSpec(label, label, label, digit * 40)
                      for label, digit in [('before', 'a'), ('after', 'b')]]
            for revision in series:
                write_fake_casita(root / revision.label)
            output = root / 'output'
            with (
                mock.patch.object(revisions, 'resolve_revisions', return_value=series),
                mock.patch.object(revisions, 'git_output', side_effect=lambda arguments:
                                  '' if arguments[0] == 'status' else 'f' * 40),
            ):
                # The manifest's concurrency, layout and budget, with fewer, smaller files.
                code = revisions.main([
                    'before', 'after', '--suite', 'git-view-source-window-32', '--repetitions', '2',
                    '--output-dir', str(output),
                    *[f'--artifact={revision.label}={root / revision.label}' for revision in series],
                    '--', '--counts', '3,4,5', '--file-bytes', '16'])
            self.assertEqual(code, 0)
            report = json.loads((output / 'series.json').read_text())
            expected = set(itertools.product(('incremental-import', 'initial-import'), (3, 4, 5), (1, 16)))
            for revision in report['revisions']:
                observations = revision['normalized']['observations']
                self.assertEqual(len(observations), len(expected))
                self.assertEqual({(row['operation'], workload(row)[1]['entries'], workload(row)[1]['concurrency'])
                                  for row in observations}, expected)
                for row in observations:
                    entrypoint, scale = workload(row)
                    self.assertEqual(entrypoint, 'git-ingest-concurrency')
                    self.assertEqual({name: scale[name] for name in ('layout', 'file_bytes', 'max_buffered_bytes')},
                                     dict(layout='delta', file_bytes=16, max_buffered_bytes=67108864))
                    self.assertEqual((row['status'], row['rounds'], row['samples']), ('ok', 2, 2))
                    self.assertEqual((row['implementation'], row['cache_policy']), ('casita', 'warm'))
            walls = [row for row in report['rows'] if row['metric'] == 'wall_seconds']
            self.assertEqual(len(walls), len(expected))
            for row in walls:
                self.assertEqual([value['status'] for value in row['values']], ['ok', 'ok'])
                self.assertTrue(all(value['value'] > 0 for value in row['values']))

    def test_reports_keep_blob_sizes_apart_and_reject_ambiguous_samples(self):
        base = dict(status='ok', implementation='casita', layout='delta', concurrency=16, max_buffered_bytes=67108864,
                    repetition=0, objects=515, wall_seconds=1.0, max_rss_bytes=1, exit_code=0)
        samples = [{**base, 'operation': operation, 'entries': entries, 'concurrency': concurrency, 'file_bytes': 65536,
                    'reachable_source_bytes': entries << 16, 'new_source_bytes': entries << 16}
                   for operation, entries, concurrency in itertools.product(
                       ('initial-import', 'incremental-import'), (511, 512, 513), (1, 16))]
        # Results written before blob sizes were configurable used the mixed-size fixture.
        legacy = {key: value for key, value in samples[0].items()
                  if key not in ('file_bytes', 'reachable_source_bytes', 'new_source_bytes')}
        result = dict(result_schema='casita.git-ingest-concurrency.v1', suite_id='native-git', complete=True,
                      configuration=dict(profile='standard'), samples=[*samples, legacy, {**samples[0], 'repetition': 1}])
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory) / 'result.json'
            output.write_text(json.dumps(result))
            normalized = dashboard.normalize_result(output)
            self.assertEqual(normalized['suite_id'], 'native-git')
            observations = normalized['observations']
            self.assertEqual(len(observations), 13)
            self.assertEqual(len({(row['operation'], row['workload']) for row in observations}), 13)
            mixed, = [row for row in observations if row['scale']['file_bytes'] is None]
            self.assertEqual((mixed['samples'], set(mixed['metrics'])), (1, {'wall_seconds', 'p95_wall_seconds', 'max_rss_bytes', 'objects'}))
            repeated, = [row for row in observations if row['samples'] == 2]
            self.assertEqual(repeated['scale'], dict(entries=511, layout='delta', file_bytes=65536, concurrency=1,
                                                     max_buffered_bytes=67108864))
            self.assertEqual(repeated['metrics']['reachable_source_bytes'], 511 << 16)
            self.assertEqual({row['profile'] for row in observations}, {'standard'})
            for field, value in (('concurrency', True), ('entries', '511'), ('file_bytes', 65536.0), ('layout', None)):
                with self.subTest(field=field, value=value):
                    output.write_text(json.dumps({**result, 'samples': [{**samples[0], field: value}, *samples[1:]]}))
                    with self.assertRaisesRegex(dashboard.DashboardError, 'invalid workload'):
                        dashboard.normalize_result(output)
            output.write_text(json.dumps({**result, 'samples': [
                legacy, {key: value for key, value in samples[1].items() if key != 'layout'}]}))
            with self.assertRaisesRegex(dashboard.DashboardError, r"lacks \['layout'\]"):
                dashboard.normalize_result(output)
            output.write_text(json.dumps({**result, 'complete': False}))
            with self.assertRaisesRegex(ValueError, 'incomplete Git ingestion'):
                dashboard.normalize_result(output)

    def test_explicit_blob_size_and_exact_reachable_byte_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            sources = native.fixture(pathlib.Path(directory), 2, 'packed', file_bytes=8193)
            prior = set()
            for source, _, _, _, count, inventory in sources:
                objects = inventory['objects']
                self.assertEqual(len(objects), count)
                blobs = [row for row in objects.values() if row['kind'] == 'blob']
                self.assertTrue(blobs)
                self.assertTrue(all(row['bytes'] == 8193 for row in blobs))
                total = 0
                for oid, row in objects.items():
                    body = subprocess.check_output(['git', f'--git-dir={source}', 'cat-file', row['kind'], oid], env=git_env())
                    self.assertEqual(len(body), row['bytes'])
                    total += len(body)
                self.assertEqual(inventory['reachable_bytes'], total)
                self.assertEqual(inventory['new_bytes'], sum(row['bytes'] for oid, row in objects.items() if oid not in prior))
                prior = set(objects)

    def test_probe_requires_both_matching_imports_and_a_passing_test(self):
        base = dict(files=17, concurrency=16, max_buffered_bytes=65536, delay_ms=5, packed=True,
                    root='git.view.v1:example', wall_nanos=1, peak_active=16, peak_bytes=65536,
                    correctness=suite.CORRECTNESS)
        rows = [{**base, 'operation': operation} for operation in ('initial-import', 'incremental-import')]

        def parse(values, footer='test result: ok. 1 passed; 0 failed;'):
            return suite.parse_samples('\n'.join('git_ingest_sample ' + json.dumps(row) for row in values) + '\n' + footer,
                                       17, 16, 65536, 5, True)

        self.assertEqual(parse(rows), rows)
        for field, value in (('files', 16), ('concurrency', 1), ('max_buffered_bytes', 1),
                             ('delay_ms', 0), ('packed', 1), ('root', ''), ('operation', 'initial-import'),
                             ('wall_nanos', True), ('peak_active', 17), ('peak_bytes', 0), ('correctness', '')):
            with self.subTest(field=field), self.assertRaises(common.BenchmarkError):
                parse([rows[0], {**rows[1], field: value}])
        with self.assertRaises(common.BenchmarkError):
            parse(rows[:1])
        with self.assertRaises(common.BenchmarkError):
            parse(rows, 'test result: ok. 0 passed; 0 failed;')

    def test_all_supplies_binaries_for_both_registered_suites(self):
        for name, binary in (('git-ingest-concurrency', 'casita'), ('git-ingest-scheduling', 'casita-lib-test')):
            entry = next(entry for entry in cli.entrypoints() if entry['id'] == name)
            self.assertEqual(entry['suite_id'], 'native-git')
            args = runner.suite_arguments(name, pathlib.Path('/binaries'), 'smoke', 1)
            self.assertIn('/binaries/' + binary, args)
            self.assertIn('--no-build', args)

    def test_source_window_cases_use_existing_probes_and_explicit_workloads(self):
        for path, binary in (('closure', 'git_closure_import'), ('view', 'casita')):
            for boundary in ('32', '128', 'oversized-32', 'oversized-128'):
                name = f'git-{path}-source-window-{boundary}'
                entry = next(entry for entry in cli.entrypoints() if entry['id'] == name)
                args = runner.suite_arguments(name, pathlib.Path('/binaries'), 'smoke', 1)
                self.assertIn('/binaries/' + binary, args)
                self.assertIn('--no-build', args)
                self.assertIn(name, runner.SMOKE)
                workload = entry['default_arguments']
                self.assertIn('--file-bytes', workload)
                self.assertIn('--max-buffered-bytes', workload)
                trigger = int(boundary.removeprefix('oversized-')) << 20
                for profile in ('smoke', 'standard'):
                    with self.subTest(name=name, profile=profile):
                        # Later arguments override the manifest's, as `benchmark run` passes them.
                        arguments = [*workload, *runner.suite_arguments(name, pathlib.Path('/binaries'), profile, 1)]
                        counts = sorted(int(count) for count in option_value(arguments, '--counts').split(','))
                        file_bytes = int(option_value(arguments, '--file-bytes'))
                        if boundary.startswith('oversized'):
                            self.assertEqual(counts, [2])
                            self.assertGreater(file_bytes, trigger)
                        else:
                            self.assertLess(counts[0] * file_bytes, trigger)
                            self.assertGreater(counts[-1] * file_bytes, trigger)
                        if path == 'view':
                            self.assertEqual(option_value(arguments, '--concurrency'),
                                             '16' if profile == 'smoke' else '1,16')
