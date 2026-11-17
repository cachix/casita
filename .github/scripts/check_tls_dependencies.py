#!/usr/bin/env python3
"""Check resolved TLS dependencies, including independently resolved consumers.

Run alongside cargo-deny, which remains the authority for the advisory database,
licenses and allowed sources. These regressions also cover consumers that do not
inherit this workspace's lockfile, features or root Cargo patches.
"""

import argparse
import json
import pathlib
import subprocess
import tempfile


def check_graph(metadata):
    active = {node["id"] for node in metadata["resolve"]["nodes"]}
    tls = []
    for package in metadata["packages"]:
        if package["id"] not in active:
            continue
        name, version = package["name"], package["version"]
        if name not in ("rustls", "rustls-webpki"):
            continue
        # The dependency constraints select stable releases. Do not treat a
        # prerelease as though it contains the stable release's security fixes.
        parts = version.split(".")
        if len(parts) != 3 or not all(p.isdecimal() for p in parts):
            raise ValueError(f"unreviewed TLS prerelease: {name} {version}")
        minimum = (0, 23, 46) if name == "rustls" else (0, 103, 16)
        if tuple(map(int, parts)) < minimum:
            raise ValueError(f"affected TLS dependency: {name} {version}")
        tls.append(f"{name} {version}")
    return sorted(tls)


def metadata(manifest, *features):
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest), *features],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)


def reject_insecure_resolution(manifest):
    selectable = []
    for name, vulnerable, floor in [
        ("rustls", "0.23.45", "0.23.46"),
        ("rustls-webpki", "0.103.15", "0.103.16"),
    ]:
        result = subprocess.run(
            ["cargo", "update", "--manifest-path", str(manifest), "--package", name,
             "--precise", vulnerable],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            selectable.append(f"{name} {vulnerable}")
            continue
        # A network, toolchain or fetch failure is not proof of a version floor.
        error = result.stderr
        if ("failed to select a version for the requirement" not in error
                or f'{name} = "^{floor}"' not in error):
            raise RuntimeError(f"unexpected downgrade failure for {name}: {error}")
    if selectable:
        raise ValueError(f"downstream consumer can still select: {', '.join(selectable)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["all", "s3", "oci", "native", "s3-sync"])
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[2]
    profiles = [args.profile] if args.profile else ["all", "s3", "oci", "native", "s3-sync"]
    for profile in profiles:
        if profile == "all":
            graph = metadata(root / "Cargo.toml", "--all-features")
        elif profile == "s3-sync":
            graph = metadata(root / "crates/casita/examples/s3_sync/Cargo.toml")
        else:
            with tempfile.TemporaryDirectory(prefix="casita-tls-consumer-") as directory:
                consumer = pathlib.Path(directory)
                (consumer / "src").mkdir()
                (consumer / "src/lib.rs").write_text("")
                (consumer / "Cargo.toml").write_text(
                    '[package]\nname="tls-consumer"\nversion="0.0.0"\nedition="2024"\n'
                    '[workspace]\n[dependencies]\ncasita={path='
                    + json.dumps(str(root / "crates/casita"))
                    + ',default-features=false,features=[' + json.dumps(profile) + ']}\n'
                )
                graph = metadata(consumer / "Cargo.toml")
                if profile in ("s3", "oci"):
                    reject_insecure_resolution(consumer / "Cargo.toml")
        tls = check_graph(graph)
        if profile != "native":
            if not any(p.startswith("rustls ") for p in tls):
                raise ValueError(f"{profile}: TLS unexpectedly absent")
            if not any(p.startswith("rustls-webpki ") for p in tls):
                raise ValueError(f"{profile}: verifier absent")
        print(f"{profile}: {', '.join(tls) or 'no TLS dependencies'}", flush=True)


if __name__ == "__main__":
    main()
