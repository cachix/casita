"""Closure witness policies that Git closure probes declare.

Each probe states the witnesses its revision's imports store as a constant in
its source, and asserts its own measurements against it. The harness never
infers a policy from what a probe measured: it holds every artifact exactly to
its own declaration, so a regression fails the policy its revision declares,
and one harness compares probes on either side of a deliberate witness change.
"""
from __future__ import annotations

from benchmarks.suites import repository as common

# Whether built-in imports store a witness for every present Git blob.
STORES_BLOB_WITNESSES = {"stored-blobs": True}


def declared(artifact, row):
    """Return the known policy `row` declares, which every sample of `artifact` shares."""
    policy = row.get("witness_policy")
    if not isinstance(policy, str) or policy not in STORES_BLOB_WITNESSES:
        raise common.BenchmarkError(f"probe declares unknown witness policy {policy!r}")
    if artifact.setdefault("witness_policy", policy) != policy:
        raise common.BenchmarkError(
            f"probe declares witness policy {policy!r} after {artifact['witness_policy']!r}")
    return policy
