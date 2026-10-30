"""Positive, alternate-input and fail-closed controls for release inputs."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import check_release_inputs as check


def safe_lock():
    return {"package": [
        {"name": "tokio", "version": "1.53.3"},
        {"name": "yoke-derive", "version": "0.8.4"},
    ]}


class ReleaseInputs(unittest.TestCase):
    def test_project_license_is_accepted(self):
        self.assertEqual(check.manifest_errors({"package": {"license": "Apache-2.0"}}, "fixture"), [])

    def test_missing_empty_or_inherited_license_is_rejected(self):
        for value in (None, "", {"workspace": True}, "Unreviewed", "MIT"):
            with self.subTest(value=value):
                self.assertTrue(check.manifest_errors({"package": {"license": value}}, "fixture"))

    def test_safe_locked_versions_are_accepted(self):
        self.assertEqual(check.lock_errors(safe_lock()), [])

    def test_known_affected_tokio_ranges_are_rejected(self):
        for version in ("0.3.2", "1.51.5", "1.52.0", "1.53.1", "1.53.2"):
            with self.subTest(version=version):
                lock = safe_lock()
                lock["package"][0]["version"] = version
                self.assertTrue(check.lock_errors(lock))

    def test_patched_and_unaffected_tokio_ranges_are_accepted(self):
        for version in ("0.2.25", "1.51.6", "1.51.7", "1.53.3", "1.54.1", "1.53.3+build.1"):
            with self.subTest(version=version):
                lock = safe_lock()
                lock["package"][0]["version"] = version
                self.assertEqual(check.lock_errors(lock), [])

    def test_second_affected_copy_cannot_hide_behind_a_safe_one(self):
        lock = safe_lock()
        lock["package"].append({"name": "tokio", "version": "1.53.1"})
        self.assertTrue(check.lock_errors(lock))

    def test_missing_expected_package_is_rejected(self):
        for index in (0, 1):
            lock = safe_lock()
            del lock["package"][index]
            self.assertTrue(check.lock_errors(lock))

    def test_known_yank_including_build_alias_is_rejected(self):
        for version in ("0.8.3", "0.8.3+build.1"):
            lock = safe_lock()
            lock["package"][1]["version"] = version
            self.assertTrue(check.lock_errors(lock))

    def test_other_stable_derive_versions_are_not_invented_yanks(self):
        for version in ("0.8.2", "0.8.4", "0.9.0"):
            lock = safe_lock()
            lock["package"][1]["version"] = version
            self.assertEqual(check.lock_errors(lock), [])

    def test_prerelease_malformed_and_nonstring_versions_fail_closed(self):
        for index in (0, 1):
            for version in ("1.54.1-rc.1", "0.8.4-beta.1", "1.53", "01.53.3", "1.53.3+", "1.53.3+..", 1533, None):
                with self.subTest(index=index, version=version):
                    lock = safe_lock()
                    lock["package"][index]["version"] = version
                    self.assertTrue(check.lock_errors(lock))

    def test_invalid_package_list_is_rejected(self):
        for packages in ({}, ["not a package"]):
            self.assertTrue(check.lock_errors({"package": packages}))

    def test_cli_reads_all_inputs_and_reports_multiple_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for manifest in check.MANIFESTS:
                p = root / manifest
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text('[package]\nlicense = "Apache-2.0"\n')
            p = root / check.LOCK
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('[[package]]\nname = "tokio"\nversion = "1.53.3"\n'
                         '[[package]]\nname = "yoke-derive"\nversion = "0.8.4"\n')
            command = [sys.executable, str(Path(check.__file__)), "--root", str(root)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["errors"], [])
            for manifest in check.MANIFESTS:
                (root / manifest).write_text('[package]\nname = "fixture"\n')
            p.write_text('[[package]]\nname = "tokio"\nversion = "1.53.1"\n'
                         '[[package]]\nname = "yoke-derive"\nversion = "0.8.3"\n')
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertEqual(len(json.loads(result.stdout)["errors"]), 6)

    def test_unreadable_or_malformed_input_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(len(check.check_project(root)), 5)
            p = root / check.MANIFESTS[0]
            p.parent.mkdir(parents=True)
            p.write_text("not valid TOML !!!")
            self.assertEqual(len(check.check_project(root)), 5)


if __name__ == "__main__":
    unittest.main()
