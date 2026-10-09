"""Recompute the committed summaries from retained samples (standard library only)."""

import gzip
import hashlib
import json
from pathlib import Path
import random
import statistics


ROOT = Path(__file__).resolve().parent
DATA = json.loads(gzip.decompress((ROOT / "samples.json.gz").read_bytes()))


def quantile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    index = int(position)
    return values[index] + (values[min(index + 1, len(values) - 1)] - values[index]) * (position - index)


def summarize(before, after, resamples):
    changes = [100 * (y / x - 1) for x, y in zip(before, after)]
    bootstrap = [statistics.median([changes[i] for i in indices]) for indices in resamples]
    return {
        "baseline_median": statistics.median(before),
        "candidate_median": statistics.median(after),
        "paired_change_percent_median": statistics.median(changes),
        "change_percent_ci95_low": quantile(bootstrap, .025),
        "change_percent_ci95_high": quantile(bootstrap, .975),
        "baseline_min": min(before), "baseline_max": max(before),
        "candidate_min": min(after), "candidate_max": max(after),
        "paired_change_percent_min": min(changes),
        "paired_change_percent_max": max(changes),
    }


for iterations in (100, 500):
    batch = DATA[str(iterations)]
    samples = batch["samples"]
    assert len(samples) == 22
    assert all(s["status"] == "ok" and s["exit_code"] == 0 for s in samples)
    before, after = [], []
    for pair in range(1, 11):
        for variant, destination in (("baseline", before), ("candidate", after)):
            matches = [s for s in samples if s["pair"] == pair and s["variant"] == variant]
            assert len(matches) == 1
            destination.append(matches[0])
    rng = random.Random(1741)
    resamples = [[rng.randrange(10) for _ in range(10)] for _ in range(10_000)]
    saved = json.loads((ROOT / f"summary-{iterations}.json").read_text())
    assert saved["pairs"] == 10 and saved["iterations"] == iterations
    assert saved["cases"] == 24 and saved["deferred_cases"] == 12
    assert len(saved["timings"]) == 60
    for row in saved["timings"]:
        metric = row["metric"]
        values = [[s["metrics"][metric] / iterations / 1000 for s in group]
                  for group in (before, after)]
        assert row["unit"] == "microseconds_per_operation"
        assert summarize(*values, resamples) == {
            k: v for k, v in row.items() if k not in ("metric", "unit")
        }, metric
    for row in saved["catalog_io"]:
        b = [s["metrics"][row["metric"]] for s in before]
        a = [s["metrics"][row["metric"]] for s in after]
        assert row == {"metric": row["metric"], "baseline": b,
                       "candidate": a, "identical": b == a}
        assert b == a
    for metric, row in saved["resources"].items():
        assert summarize([s[metric] for s in before],
                         [s[metric] for s in after], resamples) == row, metric
    print(f"{iterations} iterations: all timings, intervals, I/O and resources verified")

provenance = json.loads((ROOT / "provenance.json").read_text())
assert hashlib.sha256(gzip.decompress((ROOT / "Cargo.lock.gz").read_bytes())).hexdigest() == provenance["cargo_lock_sha256"]
print("Dependency lock hash verified")
