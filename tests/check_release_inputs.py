#!/usr/bin/env python3
"""Guard isolated manifest and committed-lock regressions alongside cargo-deny.

This is not an advisory database or a replacement for the unchanged release
policy. It covers the fixture metadata and locked versions that a fresh root
workspace resolution cannot check.
"""
import argparse
import json
from pathlib import Path
import re
import tomllib


MANIFESTS = (
    "tests/tls_connectors/Cargo.toml",
    "tests/zstd_seekable/Cargo.toml",
    "tests/zstd_parquet/Cargo.toml",
    "crates/casita/examples/s3_sync/Cargo.toml",
)
LOCK = "crates/casita-fskit/Cargo.lock"
RELEASE = re.compile(
    r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:\+[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*)?"
)


def release_version(value):
    match = RELEASE.fullmatch(value) if isinstance(value, str) else None
    return tuple(map(int, match.groups())) if match else None


def manifest_errors(manifest, label):
    package = manifest.get("package", {})
    if not isinstance(package, dict) or package.get("license") != "Apache-2.0":
        return [f"{label}: declare package.license = Apache-2.0 for this repository fixture"]
    return []


def lock_errors(lock):
    packages = lock.get("package", [])
    if not isinstance(packages, list) or not all(isinstance(p, dict) for p in packages):
        return [f"{LOCK}: invalid package list"]
    errors = []
    for name in ("tokio", "yoke-derive"):
        copies = [p for p in packages if p.get("name") == name]
        if not copies:
            errors.append(f"{LOCK}: expected locked {name} package is missing")
        for package in copies:
            raw = package.get("version")
            version = release_version(raw)
            if version is None:
                errors.append(f"{LOCK}: unreviewed {name} release {raw!r}")
                continue
            # Frozen RUSTSEC-2026-0386 ranges, not a claim about feature use or
            # application exploitability. Check every copy, including aliases.
            if name == "tokio" and not (
                version < (0, 3, 2)
                or (1, 51, 6) <= version < (1, 52, 0)
                or version >= (1, 53, 3)
            ):
                errors.append(f"{LOCK}: tokio {raw} is affected by RUSTSEC-2026-0386")
            # The registry yanked this release. Other current/future yanks
            # remain cargo-deny's responsibility under advisories.yanked=deny.
            if name == "yoke-derive" and version == (0, 8, 3):
                errors.append(f"{LOCK}: yoke-derive {raw} is the known yanked release")
    return errors


def check_project(root):
    errors = []
    for relative in (*MANIFESTS, LOCK):
        try:
            with (root / relative).open("rb") as stream:
                document = tomllib.load(stream)
        except (OSError, tomllib.TOMLDecodeError) as error:
            errors.append(f"{relative}: cannot read release input ({type(error).__name__})")
            continue
        errors.extend(
            lock_errors(document) if relative == LOCK else manifest_errors(document, relative)
        )
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    errors = check_project(args.root)
    print(json.dumps({"errors": errors}, indent=2))
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
