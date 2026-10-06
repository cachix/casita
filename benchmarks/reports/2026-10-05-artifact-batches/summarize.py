"""Summarize the three retained Criterion runs. Run from this directory."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
samples = []
for run in (1, 2, 3):
    paths = list((root / f"criterion-run-{run}").glob("**/new/estimates.json"))
    if len(paths) != 24:
        raise SystemExit(f"run {run}: expected 24 cases, found {len(paths)}")
    for path in paths:
        estimates = json.loads(path.read_text())
        benchmark = json.loads(path.with_name("benchmark.json").read_text())
        _, case, count = benchmark["full_id"].split("/")
        operation, backend, mode = case.rsplit("-", 2)
        samples.append({"run": run, "operation": operation, "backend": backend,
            "mode": mode, "requests": int(count), "median_ns": estimates["median"]["point_estimate"],
            "median_ci_ns": estimates["median"]["confidence_interval"],
            "mean_ns": estimates["mean"]["point_estimate"],
            "raw_directory": str(path.parent.relative_to(root))})
rows = []
for operation in ("mixed-import", "checkout"):
    for backend in ("memory", "local"):
        for count in (1, 8, 32):
            controls = sorted((s for s in samples if (s["operation"], s["backend"], s["requests"], s["mode"])
                == (operation, backend, count, "individual")), key=lambda s: s["run"])
            batches = sorted((s for s in samples if (s["operation"], s["backend"], s["requests"], s["mode"])
                == (operation, backend, count, "batch")), key=lambda s: s["run"])
            assert len(controls) == len(batches) == 3, (operation, backend, count)
            ratios = [a["median_ns"] / b["median_ns"] for a, b in zip(controls, batches)]
            rows.append({"operation": operation, "backend": backend, "requests": count,
                "individual_median_ms": [s["median_ns"] / 1e6 for s in controls],
                "batch_median_ms": [s["median_ns"] / 1e6 for s in batches],
                "speedup": ratios})
result = {"schema_version": 1, "timing": "median time for the whole request group",
    "speedup_definition": "individual median / batch median, paired within each run",
    "samples": samples, "comparisons": rows}
(root / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
lines = ["| Operation | Backend | Requests | Individual ms, runs 1 / 2 / 3 | Batch ms, runs 1 / 2 / 3 | Speedup, runs 1 / 2 / 3 |",
         "|---|---|---:|---:|---:|---:|"]
for row in rows:
    pair = lambda values: " / ".join(f"{value:.3f}" for value in values)
    lines.append(f"| {row['operation']} | {row['backend']} | {row['requests']} | "
        f"{pair(row['individual_median_ms'])} | {pair(row['batch_median_ms'])} | {pair(row['speedup'])}× |")
(root / "results.md").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
