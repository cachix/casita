#!/usr/bin/env python3
"""Check every resolved zstd copy, including the optional WAL/OCI routes.

Run alongside (not instead of) the unchanged cargo-deny policy. Accepting a
saved metadata file makes the pre-fix negative reproducible without changing
the candidate lock or source.
"""
import argparse
import json
import re
import subprocess
from pathlib import Path

FLOORS = {"zstd": (0, 14, 3), "zstd-safe": (8, 2, 0)}


def violations(metadata):
    found = {name: [] for name in FLOORS}
    errors = []
    for package in metadata["packages"]:
        name = package["name"]
        if name not in FLOORS:
            continue
        version = package["version"]
        found[name].append(version)
        release = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+[A-Za-z0-9.-]+)?", version)
        if release is None or tuple(map(int, release.groups())) < FLOORS[name]:
            errors.append(f"{name} {version} is below the security floor")
    for name, versions in found.items():
        if not versions:
            errors.append(f"{name} missing: this check expects the full native/S3/OCI graph")
    return found, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path)
    args = parser.parse_args()
    if args.metadata:
        metadata = json.loads(args.metadata.read_text())
    else:
        metadata = json.loads(subprocess.check_output([
            "cargo", "metadata", "--all-features", "--locked", "--offline", "--format-version", "1"
        ], text=True))
    found, errors = violations(metadata)
    print(json.dumps({"versions": found, "errors": errors}, indent=2))
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
