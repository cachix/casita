#!/usr/bin/env python3
"""Build the unified Casita benchmark catalog and documentation dashboard."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import pathlib
import statistics
import sys
from collections import defaultdict
from typing import Any, Iterable, Sequence

from benchmarks.suites import git_closure_audit, git_closure_import
from benchmarks.suites import repository as common
from benchmarks.metrics import MetricRegistryError, load_metric_registry


CATALOG_SCHEMA_VERSION = 1


class DashboardError(RuntimeError):
    pass


def load_manifest(path: pathlib.Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    required = {
        "schema_version",
        "profiles",
        "principles",
        "dimensions",
        "metrics",
        "entrypoints",
        "frontiers",
        "suites",
    }
    missing = required - set(manifest)
    if missing:
        raise DashboardError(f"benchmark manifest is missing {sorted(missing)}")
    try:
        load_metric_registry(manifest)
    except MetricRegistryError as error:
        raise DashboardError(str(error)) from error
    dimensions = set(manifest["dimensions"])
    frontier_ids: set[str] = set()
    for frontier in manifest["frontiers"]:
        frontier_id = frontier.get("id")
        axis = frontier.get("axis")
        target = frontier.get("target")
        if not frontier_id or frontier_id in frontier_ids:
            raise DashboardError(f"invalid or duplicate frontier id: {frontier_id!r}")
        if not axis:
            raise DashboardError(f"frontier {frontier_id} has no axis")
        if not isinstance(target, (int, float)) or isinstance(target, bool) or target <= 0:
            raise DashboardError(f"frontier {frontier_id} has invalid target {target!r}")
        if not frontier.get("title") or not frontier.get("purpose"):
            raise DashboardError(f"frontier {frontier_id} requires a title and purpose")
        frontier_ids.add(frontier_id)
    suite_ids: set[str] = set()
    for suite in manifest["suites"]:
        suite_id = suite.get("id")
        if not suite_id or suite_id in suite_ids:
            raise DashboardError(f"invalid or duplicate suite id: {suite_id!r}")
        suite_ids.add(suite_id)
        unknown = set(suite.get("dimensions", [])) - dimensions
        if unknown:
            raise DashboardError(f"suite {suite_id} has unknown dimensions: {sorted(unknown)}")
        if suite.get("status") not in {"implemented", "partial", "planned"}:
            raise DashboardError(f"suite {suite_id} has invalid status {suite.get('status')!r}")
    entrypoint_ids: set[str] = set()
    for entrypoint in manifest["entrypoints"]:
        entrypoint_id = entrypoint.get("id")
        if not entrypoint_id or entrypoint_id in entrypoint_ids:
            raise DashboardError(f"invalid or duplicate entrypoint id: {entrypoint_id!r}")
        if entrypoint.get("suite_id") not in suite_ids:
            raise DashboardError(
                f"entrypoint {entrypoint_id} has unknown suite {entrypoint.get('suite_id')!r}"
            )
        if entrypoint.get("kind") not in {"module", "command"} or not entrypoint.get("target"):
            raise DashboardError(f"entrypoint {entrypoint_id} has an invalid target")
        budgets = entrypoint.get("budgets", {})
        if not isinstance(budgets, dict) or any(
            not isinstance(name, str)
            or not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            for name, value in budgets.items()
        ):
            raise DashboardError(f"entrypoint {entrypoint_id} has invalid budgets")
        entrypoint_ids.add(entrypoint_id)
    return manifest


def nearest_rank(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def aggregate_metric(samples: Sequence[dict[str, Any]], field: str) -> tuple[float | None, float | None]:
    values = [float(sample[field]) for sample in samples if isinstance(sample.get(field), (int, float))]
    if not values:
        return None, None
    return statistics.median(values), nearest_rank(values, 0.95)


def result_identity(path: pathlib.Path, result: dict[str, Any]) -> str:
    revision = str(result.get("environment", {}).get("casita_revision") or "unknown")[:12]
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    return f"{revision}-{digest}"


def normalize_repository_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    observations = []
    failed_keys = {
        (
            sample.get("corpus"),
            sample.get("cache_policy"),
            sample.get("operation"),
            sample.get("implementation"),
        )
        for sample in result.get("samples", [])
        if sample.get("status") != "ok"
    }
    for row in result.get("aggregates", []):
        key = (row["corpus"], row["cache_policy"], row["operation"], row["implementation"])
        storage = row.get("median_storage_metrics", {})
        operation_metrics = row.get("median_operation_metrics", {})
        metrics = {
            "wall_seconds": row.get("median_wall_seconds"),
            "p95_wall_seconds": row.get("p95_wall_seconds"),
            "max_rss_bytes": row.get("median_max_rss_bytes"),
            "throughput_bytes_per_second": row.get("median_throughput_bytes_per_second"),
            "repository_allocated_bytes": row.get("median_repository_allocated_bytes"),
            **{
                name: value
                for name, value in operation_metrics.items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            },
            **{
                name: storage.get(name)
                for name in (
                    "pack_count",
                    "pack_bytes",
                    "index_bytes",
                    "blob_allocated_bytes",
                    "metadata_allocated_bytes",
                    "loose_chunk_count",
                    "loose_object_count",
                )
                if storage.get(name) is not None
            },
        }
        observations.append(
            {
                "workload": row["corpus"],
                "profile": result.get("configuration", {}).get("profile", "unknown"),
                "cache_policy": row["cache_policy"],
                "operation": row["operation"],
                "implementation": row["implementation"],
                "status": "failed" if key in failed_keys else "ok",
                "samples": row["samples"],
                "metrics": {name: value for name, value in metrics.items() if value is not None},
                "scale": {
                    "logical_bytes": result.get("corpora", {}).get(row["corpus"], {}).get("base_bytes")
                },
            }
        )
    return normalized_run(path, result, "repository-e2e", observations)


def normalize_git_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for sample in result.get("samples", []):
        operation = str(sample["operation"])
        implementation = "git" if operation.startswith("git-") else "casita"
        if implementation == "git":
            operation = operation.removeprefix("git-")
        groups[(operation, implementation)].append(sample)

    corpus = result.get("corpora", [{}])[-1] if result.get("corpora") else {}
    base = corpus.get("base", {})
    observations = []
    for (operation, implementation), samples in sorted(groups.items()):
        successful = [sample for sample in samples if sample.get("status") == "ok"]
        wall, p95_wall = aggregate_metric(successful, "wall_seconds")
        rss, _ = aggregate_metric(successful, "max_rss_bytes")
        server_rss_values = [
            float(sample.get("metrics", {}).get("server_peak_rss_bytes"))
            for sample in successful
            if isinstance(sample.get("metrics", {}).get("server_peak_rss_bytes"), (int, float))
        ]
        allocated = [
            float(sample.get("repository_usage", {}).get("allocated_bytes"))
            for sample in successful
            if isinstance(sample.get("repository_usage", {}).get("allocated_bytes"), (int, float))
        ]
        metrics = {
            "wall_seconds": wall,
            "p95_wall_seconds": p95_wall,
            "max_rss_bytes": rss,
            "server_max_rss_bytes": statistics.median(server_rss_values) if server_rss_values else None,
            "repository_allocated_bytes": statistics.median(allocated) if allocated else None,
        }
        failures = [str(sample.get("error", "failed")) for sample in samples if sample.get("status") != "ok"]
        observations.append(
            {
                "workload": result.get("configuration", {}).get("shape", "unknown"),
                "profile": result.get("configuration", {}).get("profile", "unknown"),
                "cache_policy": result.get("configuration", {}).get("cache_policy", "unknown"),
                "operation": operation,
                "implementation": implementation,
                "status": "ok" if len(successful) == len(samples) else "failed",
                "samples": len(samples),
                "successful_samples": len(successful),
                "failures": failures,
                "metrics": {name: value for name, value in metrics.items() if value is not None},
                "scale": {
                    "logical_bytes": corpus.get("logical_blob_bytes"),
                    "objects": base.get("reachable_objects"),
                    "pack_bytes": base.get("pack_bytes"),
                },
            }
        )
    return normalized_run(path, result, "native-git", observations)


def normalize_gix_odb_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for sample in result.get("samples", []):
        groups[(sample["backend"], sample["operation"], sample["implementation"])].append(sample)

    configuration = result.get("configuration", {})
    scale = configuration.get("scale", {})
    observations = []
    for (backend, operation, implementation), samples in sorted(groups.items()):
        successful = [sample for sample in samples if sample.get("status") == "ok"]
        nanos, p95_nanos = aggregate_metric(successful, "nanos_per_op")
        wall, p95_wall = aggregate_metric(successful, "wall_seconds")
        throughput, _ = aggregate_metric(successful, "throughput_bytes_per_second")
        rss, _ = aggregate_metric(successful, "process_max_rss_bytes")
        pack_fields = (
            "chunk_range_requests",
            "chunk_range_bytes",
            "whole_pack_requests",
            "whole_pack_bytes",
            "cache_hits",
            "cache_promotions",
            "cache_evictions",
        )
        metrics = {
            "nanos_per_op": nanos,
            "p95_nanos_per_op": p95_nanos,
            "wall_seconds": wall,
            "p95_wall_seconds": p95_wall,
            "throughput_bytes_per_second": throughput,
            "max_rss_bytes": rss,
        }
        for field in pack_fields:
            values = [
                float(sample.get("pack", {}).get(field))
                for sample in successful
                if isinstance(sample.get("pack", {}).get(field), (int, float))
            ]
            if values:
                metrics[f"pack_{field}"] = statistics.median(values)
        failures = [str(sample.get("error", "failed")) for sample in samples if sample.get("status") != "ok"]
        observations.append(
            {
                "workload": f"{backend}-{scale.get('body_bytes', 'unknown')}-byte-objects",
                "profile": configuration.get("profile", "unknown"),
                "cache_policy": "operation-defined",
                "operation": operation,
                "implementation": implementation,
                "status": "ok" if len(successful) == len(samples) else "failed",
                "samples": len(samples),
                "successful_samples": len(successful),
                "failures": failures,
                "metrics": {name: value for name, value in metrics.items() if value is not None},
                "scale": {
                    "objects": scale.get("objects"),
                    "logical_bytes": (
                        scale.get("objects", 0) * scale.get("body_bytes", 0)
                        if scale.get("objects") is not None and scale.get("body_bytes") is not None
                        else None
                    ),
                },
            }
        )
    return normalized_run(path, result, "native-git", observations)


def normalize_graph_traversal_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for sample in result.get("samples", []):
        groups[(str(sample["operation"]), sample.get("objects", 0), sample.get("spill_memory_objects", 0), sample.get("spill_bytes_budget", 0))].append(sample)

    observations = []
    for (operation, objects, threshold, budget), samples in sorted(groups.items()):
        successful = [sample for sample in samples if sample.get("status") == "ok"]
        wall, p95_wall = aggregate_metric(successful, "wall_seconds")
        rss, _ = aggregate_metric(successful, "max_rss_bytes")
        metrics = {
            "wall_seconds": wall,
            "p95_wall_seconds": p95_wall,
            "max_rss_bytes": rss,
            "spill_bytes_budget": samples[0].get("spill_bytes_budget"),
            "spill_memory_objects": samples[0].get("spill_memory_objects"),
            "spill_files_opened": statistics.median(
                float(sample["spill_files_opened"])
                for sample in successful
                if isinstance(sample.get("spill_files_opened"), (int, float))
            )
            if any(isinstance(sample.get("spill_files_opened"), (int, float)) for sample in successful)
            else None,
            "spill_peak_bytes": statistics.median(
                float(sample["spill_peak_bytes"])
                for sample in successful
                if isinstance(sample.get("spill_peak_bytes"), (int, float))
            )
            if any(isinstance(sample.get("spill_peak_bytes"), (int, float)) for sample in successful)
            else None,
        }
        failures = [str(sample.get("error", "failed")) for sample in samples if sample.get("status") != "ok"]
        observations.append(
            {
                "workload": samples[0].get("workload", "wide-tree"),
                "profile": f"objects-{objects}-memory-{threshold}-spill-{budget}",
                "cache_policy": samples[0].get("cache_policy", "forced-spill"),
                "operation": operation,
                "implementation": "casita",
                "status": "ok" if len(successful) == len(samples) else "failed",
                "samples": len(samples),
                "successful_samples": len(successful),
                "failures": failures,
                "metrics": {name: value for name, value in metrics.items() if isinstance(value, (int, float))},
                "scale": {"objects": samples[0].get("objects")},
            }
        )
    return normalized_run(path, result, "graph-traversal", observations)


GIT_CLOSURE_WORKLOADS = {
    "casita.git-closure-import.v1": ("git-closure-import", git_closure_import.WORKLOAD),
    "casita.git-closure-audit.v1": ("git-closure-audit", git_closure_audit.WORKLOAD),
}
GIT_CLOSURE_COUNTS = (
    "imported_objects",
    "reused_objects",
    "source_bytes",
    "blob_witnesses",
    "link_audits",
    "link_audits_per_object",
    "witnesses",
    "witness_commits",
    "max_witness_batch",
)


def normalize_git_closure_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    """Report one observation per operation, variant and complete workload.

    Distinct configurations are distinct workloads, so only repetitions of one
    configuration share an observation.
    """
    if result.get("complete") is not True:
        raise ValueError("incomplete Git closure matrix cannot be compared")
    entrypoint, workload = GIT_CLOSURE_WORKLOADS[result["result_schema"]]
    fields = ("operation", "variant", *workload)
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for sample in result.get("samples", []):
        missing = [field for field in fields if field not in sample]
        if missing:
            raise DashboardError(f"Git closure sample in {path} lacks {missing}")
        key = tuple(sample[field] for field in fields)
        if (any(type(value) not in (str, int, bool) for value in key)
                or any(type(value) is not str for value in key[:2])):
            raise DashboardError(f"Git closure sample in {path} has an invalid workload {key!r}")
        # Typing each value keeps equal values of differing types, such as
        # True and 1, apart, and keys of differing types comparable.
        groups[tuple((type(value).__name__, value) for value in key)].append(sample)
    paired = len({variant for _operation, (_type, variant), *_workload in groups}) > 1
    observations = []
    for typed, samples in sorted(groups.items()):
        operation, variant, *values = (value for _type, value in typed)
        scale = dict(zip(workload, values, strict=True))
        successful = [sample for sample in samples if sample.get("status") == "ok"]
        wall, p95_wall = aggregate_metric(successful, "wall_seconds")
        metrics = {
            "wall_seconds": wall,
            "p95_wall_seconds": p95_wall,
            "max_rss_bytes": aggregate_metric(successful, "max_rss_bytes")[0],
            **{field: aggregate_metric(successful, field)[0] for field in GIT_CLOSURE_COUNTS},
        }
        failures = sample_failures(samples)
        observations.append(
            {
                "workload": f"{entrypoint}:" + json.dumps(scale, sort_keys=True),
                "profile": result.get("configuration", {}).get("profile", "custom"),
                "cache_policy": "cold" if operation.startswith("cold") else "warm",
                "operation": operation,
                "implementation": f"casita-{variant}" if paired else "casita",
                "status": "ok" if len(successful) == len(samples) and not failures else "failed",
                "samples": len(samples),
                "successful_samples": len(successful),
                "failures": failures,
                "metrics": {name: value for name, value in metrics.items() if value is not None},
                "scale": scale,
            }
        )
    return normalized_run(path, result, result["suite_id"], observations)


def flatten_numeric_metrics(value: Any, prefix: str = "") -> dict[str, float]:
    metrics: dict[str, float] = {}
    if not isinstance(value, dict):
        return metrics
    for key, item in value.items():
        name = f"{prefix}_{key}" if prefix else str(key)
        if isinstance(item, dict):
            metrics.update(flatten_numeric_metrics(item, name))
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            metrics[name] = float(item)
    return metrics


def aggregate_flat_metrics(
    samples: Sequence[dict[str, Any]],
    containers: Sequence[tuple[str, str]],
) -> dict[str, float]:
    rows = []
    for sample in samples:
        metrics = {
            key: float(sample[key])
            for key in ("wall_seconds", "max_rss_bytes")
            if isinstance(sample.get(key), (int, float)) and not isinstance(sample.get(key), bool)
        }
        for container, prefix in containers:
            metrics.update(flatten_numeric_metrics(sample.get(container), prefix))
        rows.append(metrics)
    names = {name for row in rows for name in row}
    return {
        name: statistics.median(row[name] for row in rows if name in row)
        for name in sorted(names)
    }


def sample_failures(samples: Sequence[dict[str, Any]]) -> list[str]:
    failures = []
    for sample in samples:
        if sample.get("exit_code") not in (None, 0):
            failures.append(f"exit code {sample['exit_code']}")
        if sample.get("status") in {"failed", "error"}:
            failures.append(str(sample.get("error") or sample["status"]))
        failures.extend(
            f"budget {check.get('id', 'unknown')} {check.get('status')}"
            for check in sample.get("budget_checks", [])
            if check.get("status") not in {"passed", "not_applicable"}
        )
    return failures


def normalize_pack_limits_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    observations = []
    for row in result.get("observations", []):
        metrics = {}
        for name, value in row.items():
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                continue
            if name.startswith("median_"):
                metrics[name.removeprefix("median_")] = float(value)
            elif name.startswith("p95_"):
                metrics[name] = float(value)
        target_mib = int(row["target_mib"])
        observations.append(
            {
                "workload": f"{row['corpus']}-target-{target_mib}mib",
                "profile": result.get("configuration", {}).get("profile", "custom"),
                "cache_policy": row.get("cache_policy", "local"),
                "operation": row["operation"],
                "implementation": "casita-local-pack",
                "status": "ok",
                "samples": row.get("samples", 1),
                "metrics": metrics,
                "scale": {"pack_target_bytes": target_mib * 1024 * 1024},
            }
        )
    return normalized_run(path, result, "blob-backends", observations)


def normalize_pack_index_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    configuration = result.get("configuration", {})
    phases = [("cold", [result["cold"]])] if isinstance(result.get("cold"), dict) else []
    if result.get("warm"):
        phases.append(("warm", result["warm"]))
    observations = []
    for phase, samples in phases:
        failures = sample_failures(samples)
        observations.append(
            {
                "workload": f"files-{configuration.get('files', 'unknown')}",
                "profile": f"target-{configuration.get('pack_target_mib', 'unknown')}mib",
                "cache_policy": phase,
                "operation": "catalog-open",
                "implementation": "casita-local-pack",
                "status": "failed" if failures else "ok",
                "samples": len(samples),
                "failures": failures,
                "metrics": aggregate_flat_metrics(samples, (("metrics", ""),)),
                "scale": {
                    "files": configuration.get("files"),
                    "pack_target_bytes": configuration.get("pack_target_mib", 0) * 1024 * 1024,
                },
            }
        )
    return normalized_run(path, result, "blob-backends", observations)


def normalize_catalog_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    configuration = result.get("configuration", {})
    containers = tuple(
        (name, name)
        for name in (
            "generate",
            "decode",
            "operations",
            "publication",
            "rebase",
            "lazy_run",
            "shard_generate",
        )
    )
    observations = []
    for sample in result.get("samples", []):
        failures = sample_failures([sample])
        entries = int(sample.get("entries", 0))
        observations.append(
            {
                "workload": f"entries-{entries}",
                "profile": configuration.get("profile", "frontier"),
                "cache_policy": "catalog",
                "operation": "catalog-scale",
                "implementation": "casita-catalog-s3" if configuration.get("sharded_operations_backend") else "casita-catalog",
                "status": "failed" if failures else "ok",
                "samples": configuration.get("repetitions", 1),
                "failures": failures,
                "metrics": aggregate_flat_metrics([sample], containers),
                "scale": {
                    "catalog_entries": entries,
                    "manifest_percent": sample.get("manifest_percent"),
                },
            }
        )
    return normalized_run(path, result, "blob-backends", observations)


def grouped(samples: Sequence[dict[str, Any]], fields: Sequence[str]) -> list[tuple[tuple[Any, ...], list[dict[str, Any]]]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        groups[tuple(sample.get(field) for field in fields)].append(sample)
    return sorted(groups.items(), key=lambda item: tuple(str(value) for value in item[0]))


def normalize_pack_gc_result(
    path: pathlib.Path, result: dict[str, Any], *, remote: bool
) -> dict[str, Any]:
    observations = []
    containers = (("metrics", ""), ("request_ledger", "request_ledger")) if remote else (
        ("gc_metrics", ""),
        ("operation_metrics", ""),
    )
    for (target_mib, dead_percent), samples in grouped(
        result.get("samples", []), ("target_mib", "requested_dead_percent")
    ):
        failures = sample_failures(samples)
        observations.append(
            {
                "workload": f"target-{target_mib}mib-dead-{dead_percent}pct",
                "profile": "rustfs" if remote else "local",
                "cache_policy": "remote" if remote else "local",
                "operation": "pack-collect",
                "implementation": "casita-s3-pack" if remote else "casita-local-pack",
                "status": "failed" if failures else "ok",
                "samples": len(samples),
                "failures": failures,
                "metrics": aggregate_flat_metrics(samples, containers),
                "scale": {
                    "pack_target_bytes": int(target_mib) * 1024 * 1024,
                    "dead_percent": dead_percent,
                    "files": samples[0].get("files"),
                },
            }
        )
    return normalized_run(path, result, "collection-and-fsck", observations)


def normalize_s3_pack_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    observations = []
    for (target_mib, cache_mib, promotion_reads), samples in grouped(
        result.get("samples", []), ("target_mib", "cache_mib", "promotion_reads")
    ):
        failures = sample_failures(samples)
        observations.append(
            {
                "workload": f"target-{target_mib}mib",
                "profile": f"cache-{cache_mib}mib" + (f"-promotion-{promotion_reads}" if promotion_reads is not None else ""),
                "cache_policy": f"cache-{cache_mib}mib",
                "operation": "pack-transfer",
                "implementation": "casita-s3-pack",
                "status": "failed" if failures else "ok",
                "samples": len(samples),
                "failures": failures,
                "metrics": aggregate_flat_metrics(samples, (("operation_metrics", ""),)),
                "scale": {
                    "pack_target_bytes": int(target_mib) * 1024 * 1024,
                    "pack_cache_bytes": int(cache_mib) * 1024 * 1024,
                    **({"promotion_reads": promotion_reads} if promotion_reads is not None else {}),
                },
            }
        )
    return normalized_run(path, result, "blob-backends", observations)


def normalize_s3_pack_index_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    observations = []
    for (files, target_mib), samples in grouped(
        result.get("samples", []), ("files", "target_mib")
    ):
        failures = sample_failures(samples)
        collected_metrics = aggregate_flat_metrics(samples, (("metrics", ""),))
        max_rss_bytes, _ = aggregate_metric(samples, "max_rss_bytes")
        if max_rss_bytes is not None:
            collected_metrics["max_rss_bytes"] = max_rss_bytes
        for phase in ("warm_open", "warm_first_snapshot", "warm_repeat_snapshot"):
            ledgers = [sample.get("request_ledger", {}).get(phase, {}) for sample in samples]
            total, _ = aggregate_metric(ledgers, "total")
            if total is not None:
                collected_metrics[f"{phase}_total_requests"] = total
        for phase in ("cold", "warm"):
            wall_nanos, _ = aggregate_metric(
                [sample.get("metrics", {}) for sample in samples], f"{phase}_wall_nanos"
            )
            if wall_nanos is not None:
                collected_metrics[f"{phase}_wall_seconds"] = wall_nanos / 1_000_000_000
        observations.append(
            {
                "workload": f"files-{files}-target-{target_mib}mib",
                "profile": "rustfs",
                "cache_policy": "cold-and-warm",
                "operation": "catalog-open",
                "implementation": "casita-s3-pack",
                "status": "failed" if failures else "ok",
                "samples": len(samples),
                "failures": failures,
                "metrics": collected_metrics,
                "scale": {
                    "files": files,
                    "pack_target_bytes": int(target_mib) * 1024 * 1024,
                },
            }
        )
    return normalized_run(path, result, "blob-backends", observations)


def normalize_transfer_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    groups: dict[tuple[str, int, int, int, int, str, int], list[dict[str, Any]]] = defaultdict(list)
    for sample in result.get("samples", []):
        for phase in ("cold", "warm"):
            phase_sample = dict(sample[phase])
            process_resources = sample.get("process_resources", {})
            if isinstance(process_resources, dict) and isinstance(
                process_resources.get("max_rss_bytes"), (int, float)
            ):
                phase_sample["max_rss_bytes"] = process_resources["max_rss_bytes"]
            groups[
                (
                    str(sample["transport"]),
                    int(sample["rtt_ms"]),
                    int(sample["depth"]),
                    int(sample["subtree_files"]),
                    int(sample["cache_mib"]),
                    phase,
                    int(sample.get("bandwidth_kib_per_connection", 0)),
                )
            ].append(phase_sample)

    observations = []
    for (transport, rtt_ms, depth, files, cache_mib, phase, bandwidth), samples in sorted(groups.items()):
        wall_nanos, p95_wall_nanos = aggregate_metric(samples, "wall_nanos")
        metrics: dict[str, float | None] = {
            "wall_seconds": wall_nanos / 1_000_000_000 if wall_nanos is not None else None,
            "p95_wall_seconds": (
                p95_wall_nanos / 1_000_000_000 if p95_wall_nanos is not None else None
            ),
        }
        metrics["max_rss_bytes"], _ = aggregate_metric(samples, "max_rss_bytes")
        for field in (
            "rpc_requests",
            "published_objects",
            "payloads_sent",
            "chunks_sent",
            "pack_chunk_range_requests",
            "pack_chunk_range_bytes",
            "pack_whole_requests",
            "pack_whole_bytes",
            "pack_cache_hits",
            "pack_cache_promotions",
            "wal_manifest_refresh_requests",
            "wal_fragment_get_requests",
        ):
            metrics[field], _ = aggregate_metric(samples, field)
        observations.append(
            {
                "workload": f"depth-{depth}-files-{files}-rtt-{rtt_ms}ms" + (f"-bandwidth-{bandwidth}kib" if bandwidth else ""),
                "profile": f"cache-{cache_mib}mib",
                "cache_policy": phase,
                "operation": "path-selected-transfer",
                "implementation": transport,
                "status": "ok",
                "samples": len(samples),
                "metrics": {name: value for name, value in metrics.items() if value is not None},
                "scale": {
                    "path_depth": depth,
                    "subtree_files": files,
                    "rtt_ms": rtt_ms,
                    "pack_cache_bytes": cache_mib * 1024 * 1024,
                },
            }
        )
    return normalized_run(path, result, "transfer", observations)


def normalize_filesystem_outputs(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    if result.get("complete") is not True:
        raise ValueError("incomplete filesystem output matrix cannot be compared")
    observations = []
    for (outputs, files, size, mode), samples in grouped(
        result.get("samples", []), ("outputs", "files", "file_bytes", "mode")
    ):
        failures = sample_failures(samples)
        measured = aggregate_flat_metrics(samples, ())
        for field in ("session_nanos", "stage_nanos", "traversal_nanos", "publish_nanos", "maintenance_nanos", "pages", "publications", "walks"):
            value, _ = aggregate_metric(samples, field)
            if value is not None:
                measured[field] = value
        observations.append({
            "workload": f"outputs-{outputs}-files-{files}-bytes-{size}",
            "profile": result.get("configuration", {}).get("profile", "custom"),
            "cache_policy": "warm-source-fresh-repository",
            "operation": "filesystem-outputs",
            "implementation": f"casita-{mode}",
            "status": "failed" if failures else "ok",
            "samples": len(samples), "failures": failures, "metrics": measured,
            "scale": {"outputs": outputs, "files": files, "file_bytes": size},
        })
    return normalized_run(path, result, "state-and-publication", observations)


def normalize_lifecycle_result(path: pathlib.Path, result: dict[str, Any]) -> dict[str, Any]:
    groups = defaultdict(list)
    config = result.get("configuration", {})
    for sample in result.get("samples", []):
        scale = {name: sample.get(name, config.get(name)) for name in
                 ("generation", "writers", "readers", "entries", "iterations", "batch", "value_bytes", "page_size")
                 if name in sample or name in config}
        scale.update({name: sample[name] for name in
            ("generations", "seed_batch_size", "operations", "working_set_bytes", "cache_bytes", "promotion_reads", "pack_target_bytes", "rtt_ms", "bandwidth_kib_per_connection", "subtree_files", "file_bytes", "path_depth", "concurrency", "variant", "spill_memory_objects", "reader_admission")
            if name in sample})
        scale.update({name: sample[name] for name in ("batch_width", "pattern", "requests", "ledger_context", "shape", "max_buffered_bytes") if name in sample})
        if result.get("result_schema") == "casita.casitar-scaling.v1":
            scale.update({name: sample[name] for name in ("family", "files", "seeded_file_percent")})
            if config.get("import_profile"):
                scale["import_profile"] = True
            if config.get("pin_timing"):
                scale["pin_timing"] = True
            if config.get("require_quiet_host"):
                scale["require_quiet_host"] = True
                scale["max_external_cpu_percent"] = config.get("max_external_cpu_percent", 5)
                scale["allow_competing_builds"] = config.get("allow_competing_builds", False)
        groups[(sample["operation"], json.dumps(scale, sort_keys=True))].append(sample)
    observations = []
    for (operation, scale), samples in sorted(groups.items()):
        successful = [sample for sample in samples if sample.get("status") == "ok"]
        metrics = {}
        metrics.update(aggregate_flat_metrics(successful, (("metrics", ""),)))
        for field in ("wall_seconds", "user_seconds", "system_seconds", "max_rss_bytes", "archive_bytes"):
            median, p95 = aggregate_metric(successful, field)
            if median is not None:
                metrics[field] = median
                if field == "wall_seconds": metrics["p95_wall_seconds"] = p95
        phases = {phase["phase"] for sample in successful
                  for phase in sample.get("import_profile", {}).get("phases", [])}
        for phase in phases:
            values = [entry["nanos"] / 1e9 for sample in successful
                      for entry in sample.get("import_profile", {}).get("phases", [])
                      if entry["phase"] == phase]
            metrics[f"phase_{phase}_seconds"] = statistics.median(values)
        pin_phases = {phase for sample in successful for phase in sample.get("pin_profile", {})}
        for phase in pin_phases:
            metrics[f"pin_{phase}_seconds"] = statistics.median(
                sample["pin_profile"][phase]["seconds"] for sample in successful
                if phase in sample.get("pin_profile", {}))
        observations.append({"workload": operation + ":" + scale,
            "profile": config.get("profile", "custom"), "cache_policy": "warm",
            "operation": operation, "implementation": "casita", "metrics": metrics,
            "status": "ok" if len(successful) == len(samples) else "failed",
            "samples": len(samples), "successful_samples": len(successful),
            "scale": json.loads(scale), "failures": [sample.get("error", "failed") for sample in samples if sample.get("status") != "ok"]})
    return normalized_run(path, result, result["suite_id"], observations)


def normalized_run(
    path: pathlib.Path,
    result: dict[str, Any],
    suite_id: str,
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    environment = result.get("environment", {})
    return {
        "id": result_identity(path, result),
        "suite_id": suite_id,
        "result_schema": result.get("result_schema") or f"legacy-{suite_id}",
        "source": path.as_posix(),
        "release_status": (
            "release" if environment.get("casita_worktree_dirty") is False else "development"
        ),
        "captured_at_utc": environment.get("captured_at_utc") or result.get("generated_at"),
        "environment": environment,
        "configuration": result.get("configuration", {}),
        "tools": result.get("tools", {}),
        "raw_samples": len(result.get("samples", [])),
        "observations": observations,
    }


def normalize_result(path: pathlib.Path) -> dict[str, Any]:
    result = json.loads(path.read_text())
    if result.get("result_schema") == "casita.casitar-scaling.v1" and result.get("complete") is False:
        raise ValueError(f"incomplete Casitar benchmark: {path}")
    result_schema = str(result.get("result_schema", ""))
    if result_schema == "casita.collection-inventory.v1":
        if result.get("complete") is not True:
            raise ValueError("incomplete collection inventory matrix cannot be compared")
        normalized = normalize_lifecycle_result(path, result)
        for observation in normalized["observations"]:
            observation["cache_policy"] = "first"
        return normalized
    if result_schema == "casita.filesystem-outputs.v1":
        return normalize_filesystem_outputs(path, result)
    if result_schema in {"casita.git-import-profile.v1", "casita.pin-growth.v1"}:
        if result.get("complete") is not True:
            raise ValueError("incomplete profiling matrix cannot be compared")
        if result_schema == "casita.git-import-profile.v1":
            for sample in result.get("samples", []):
                sample["max_rss_bytes"] = sample.get("peak_rss_at_import_end_bytes")
        return normalize_lifecycle_result(path, result)
    if result_schema == "casita.fsck.v1":
        if result.get("complete") is not True:
            raise ValueError("incomplete fsck matrix cannot be compared")
        normalized = normalize_lifecycle_result(path, result)
        paired = len({sample.get("variant") for sample in result.get("samples", [])}) > 1
        for observation in normalized["observations"]:
            variant = observation["scale"].pop("variant", "candidate")
            observation["workload"] = "fsck:" + json.dumps(observation["scale"], sort_keys=True)
            if paired:
                observation["implementation"] = f"casita-{variant}"
        return normalized
    if result_schema in {"casita.metadata-primitives.v1", "casita.metadata-kv.v1", "casita.object-reads.v1", "casita.reader-coordination.v1", "casita.durable-ledger.v1", "casita.ledger-boundaries.v1", "casita.snapshot-connections.v1"}:
        if result.get("complete") is not True:
            raise ValueError("incomplete primitive matrix cannot be compared")
        return normalize_lifecycle_result(path, result)
    if result_schema in {"casita.metadata-collection.v1", "casita.metadata-scan.v1", "casita.metadata-batch.v1"}:
        if result.get("complete") is not True:
            raise ValueError("incomplete metadata collection matrix cannot be compared")
        normalized = normalize_lifecycle_result(path, result)
        for observation in normalized["observations"]:
            observation["cache_policy"] = "first" if observation["operation"].endswith("-first") else "warm"
            if result_schema == "casita.metadata-batch.v1":
                variant = observation["scale"].pop("variant")
                observation["implementation"] = f"casita-{variant}"
                observation["workload"] = observation["operation"] + ":" + json.dumps(observation["scale"], sort_keys=True)
        return normalized
    if result_schema.startswith("casita.scale."):
        if result.get("complete") is not True:
            raise ValueError("incomplete scale matrix cannot be compared")
        for sample in result.get("samples", []):
            sample["metrics"] = {**sample.get("metrics", {}), **{
                key: sample[key] for key in ("p50_nanos", "p95_nanos", "p99_nanos", "max_nanos",
                    "repository_bytes", "window_storage_growth_bytes", "backend_read_bytes",
                    "pack_range_requests", "whole_pack_requests", "cache_hits", "cache_evictions") if key in sample}}
            if "process_max_rss_bytes" in sample:
                sample["max_rss_bytes"] = sample["process_max_rss_bytes"]
        normalized = normalize_lifecycle_result(path, result)
        for observation in normalized["observations"]:
            if observation["operation"].endswith("-cold"):
                observation["cache_policy"] = "cold"
        return normalized
    if result_schema.startswith(("casita.lifecycle.", "casita.casitar-scaling.", "casita.native-probes.", "casita.process-contention.", "casita.core-primitives.")):
        return normalize_lifecycle_result(path, result)
    if result_schema.startswith("casita.pack-limits."):
        return normalize_pack_limits_result(path, result)
    if result_schema.startswith("casita.pack-index."):
        return normalize_pack_index_result(path, result)
    if result_schema.startswith("casita.catalog-index."):
        return normalize_catalog_result(path, result)
    if result_schema.startswith("casita.pack-gc."):
        return normalize_pack_gc_result(path, result, remote=False)
    if result_schema.startswith("casita.s3-pack-gc."):
        return normalize_pack_gc_result(path, result, remote=True)
    if result_schema.startswith("casita.s3-pack-index."):
        return normalize_s3_pack_index_result(path, result)
    if result_schema.startswith("casita.s3-pack."):
        return normalize_s3_pack_result(path, result)
    if result_schema in GIT_CLOSURE_WORKLOADS:
        return normalize_git_closure_result(path, result)
    suite_id = result.get("suite_id")
    if result_schema.startswith("casita.gix-odb."):
        return normalize_gix_odb_result(path, result)
    if suite_id == "transfer" or result_schema.startswith("casita.s3-path-transfer."):
        return normalize_transfer_result(path, result)
    if suite_id is None:
        configuration = result.get("configuration", {})
        suite_id = "native-git" if "shape" in configuration else "repository-e2e"
    if suite_id == "repository-e2e":
        return normalize_repository_result(path, result)
    if suite_id == "native-git":
        return normalize_git_result(path, result)
    if suite_id == "graph-traversal":
        return normalize_graph_traversal_result(path, result)
    raise DashboardError(f"unsupported benchmark suite {suite_id!r} in {path}")


def build_catalog(manifest: dict[str, Any], result_paths: Iterable[pathlib.Path]) -> dict[str, Any]:
    runs = [normalize_result(path) for path in result_paths]
    known_suites = {suite["id"] for suite in manifest["suites"]}
    unknown = {run["suite_id"] for run in runs} - known_suites
    if unknown:
        raise DashboardError(f"results refer to suites absent from manifest: {sorted(unknown)}")
    captured = sorted(str(run["captured_at_utc"]) for run in runs if run.get("captured_at_utc"))
    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "catalog_as_of": captured[-1] if captured else None,
        "manifest": manifest,
        "runs": runs,
    }


def validate_catalog(catalog: dict[str, Any]) -> None:
    if catalog.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise DashboardError("unsupported catalog schema")
    suite_ids = {suite["id"] for suite in catalog["manifest"]["suites"]}
    for run in catalog["runs"]:
        if run["suite_id"] not in suite_ids:
            raise DashboardError(f"unknown suite in catalog: {run['suite_id']}")
        for observation in run["observations"]:
            if observation["status"] not in {"ok", "failed"}:
                raise DashboardError("invalid observation status")
            for name, value in observation["metrics"].items():
                if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                    raise DashboardError(f"invalid metric {name}={value!r}")
    json.dumps(catalog, allow_nan=False)


def render_html(catalog: dict[str, Any]) -> str:
    validate_catalog(catalog)
    manifest = catalog["manifest"]
    runs = catalog["runs"]
    observations = [observation for run in runs for observation in run["observations"]]
    raw_samples = sum(int(run["raw_samples"]) for run in runs)
    failures = [observation for observation in observations if observation["status"] != "ok"]
    measured_suites = {run["suite_id"] for run in runs}
    suite_by_id = {suite["id"]: suite for suite in manifest["suites"]}
    suite_cards = "".join(
        f"""
        <article class="suite-card">
          <div class="suite-top"><span class="status {html.escape(suite['status'])}">{html.escape(suite['status'])}</span><span>{html.escape(suite['layer'])}</span></div>
          <h3>{html.escape(suite['title'])}</h3><p>{html.escape(suite['summary'])}</p>
          <small>{len(suite['operations'])} operations · {'results available' if suite['id'] in measured_suites else 'no published result'}</small>
        </article>"""
        for suite in manifest["suites"]
    )
    dimension_rows = "".join(
        "<tr><th>{}</th>{}</tr>".format(
            html.escape(suite["title"]),
            "".join(
                f'<td class="coverage {"covered" if dimension in suite["dimensions"] else ""}">{"●" if dimension in suite["dimensions"] else "—"}</td>'
                for dimension in manifest["dimensions"]
            ),
        )
        for suite in manifest["suites"]
    )
    failure_items = "".join(
        f"<li><strong>{html.escape(suite_by_id[run['suite_id']]['title'])}: {html.escape(observation['operation'])}</strong><span>{html.escape((observation.get('failures') or ['validation failed'])[0].splitlines()[-1])}</span></li>"
        for run in runs
        for observation in run["observations"]
        if observation["status"] != "ok"
    ) or "<li class='none'>No failures in the selected published results.</li>"
    principle_cards = "".join(
        f"<li><span>{index:02d}</span>{html.escape(principle)}</li>"
        for index, principle in enumerate(manifest["principles"], 1)
    )
    frontier_cards = "".join(
        f"""
        <article class="frontier-card">
          <div class="frontier-axis">{html.escape(frontier['axis'].replace('_', ' '))}</div>
          <h3>{html.escape(frontier['title'])}</h3>
          <p>{html.escape(frontier['purpose'])}</p>
        </article>"""
        for frontier in manifest["frontiers"]
    )
    catalog_json = json.dumps(catalog, separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    dimension_headers = "".join(f"<th>{html.escape(dimension)}</th>" for dimension in manifest["dimensions"])
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="The complete Casita correctness, performance, scalability, and resilience benchmark dashboard.">
<title>Casita benchmark north star</title><style>
:root{{--ink:#17211d;--muted:#66736d;--paper:#f4f0e7;--card:#fffdf8;--line:#d9d3c6;--green:#174f3d;--lime:#c9f27b;--orange:#e8743b;--blue:#5874d8;--shadow:0 18px 50px #17211d12;color-scheme:light}}*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(circle at 88% 0,#c9f27b66,transparent 27rem),var(--paper);color:var(--ink);font-family:Inter,ui-sans-serif,system-ui,sans-serif;font-variant-numeric:tabular-nums}}a{{color:inherit}}.shell{{width:min(1240px,calc(100% - 36px));margin:auto}}nav{{display:flex;justify-content:space-between;padding:24px 0;font-weight:750}}header{{padding:72px 0 54px}}.eyebrow,.status,.frontier-axis{{font-size:.72rem;text-transform:uppercase;letter-spacing:.11em;font-weight:800}}.eyebrow,.frontier-axis{{color:var(--green)}}h1,h2{{font-family:Georgia,serif;font-weight:500;letter-spacing:-.05em}}h1{{max-width:950px;margin:18px 0 24px;font-size:clamp(3.7rem,8vw,7.4rem);line-height:.88}}.lede{{max-width:760px;color:var(--muted);font-size:1.2rem;line-height:1.65}}.summary{{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;margin:30px 0 82px;overflow:hidden;border:1px solid var(--line);border-radius:18px;background:var(--line);box-shadow:var(--shadow)}}.summary div{{padding:25px;background:#ffffffdd}}.summary strong{{display:block;font:500 2.6rem Georgia,serif}}.summary span,p,small{{color:var(--muted)}}section{{margin-bottom:88px}}.section-head{{display:flex;justify-content:space-between;align-items:end;gap:30px;margin-bottom:22px}}h2{{margin:0;font-size:clamp(2.3rem,5vw,4rem)}}.section-head p{{max-width:560px;line-height:1.55}}.principles,.suites,.frontiers{{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;list-style:none;padding:0}}.principles li,.suite-card,.frontier-card,.panel{{border:1px solid var(--line);border-radius:16px;background:#ffffffa8;box-shadow:var(--shadow)}}.principles li{{display:flex;gap:15px;padding:20px;line-height:1.45}}.principles span{{color:var(--green);font-weight:800}}.suite-card,.frontier-card{{padding:22px}}.suite-card h3{{margin:18px 0 8px}}.suite-card p{{min-height:66px;line-height:1.5}}.frontier-card h3{{margin:12px 0 8px;font:500 1.7rem Georgia,serif}}.frontier-card p{{margin-bottom:0;line-height:1.5}}.suite-top{{display:flex;justify-content:space-between;color:var(--muted);font-size:.72rem;text-transform:uppercase}}.status{{padding:5px 8px;border-radius:999px;background:#ddd}}.status.implemented{{background:var(--lime)}}.status.partial{{background:#f7d895}}.status.planned{{background:#e5e5e0}}.panel{{overflow:hidden}}.controls{{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;padding:20px;border-bottom:1px solid var(--line)}}label{{display:grid;gap:6px;color:var(--muted);font-size:.7rem;text-transform:uppercase;font-weight:800}}select{{width:100%;padding:11px;border:1px solid var(--line);border-radius:8px;background:#fff}}#chart{{min-height:300px;padding:28px}}.chart-head{{display:flex;justify-content:space-between;margin-bottom:22px}}.comparison-groups{{display:grid;gap:18px}}.comparison-group{{padding:20px;border:1px solid var(--line);border-radius:12px;background:#fffdf8}}.comparison-head{{display:flex;justify-content:space-between;gap:18px;margin-bottom:14px}}.comparison-head small{{text-align:right}}.metric-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:12px}}.metric-graph{{padding:14px;border:1px solid var(--line);border-radius:9px;background:#fff}}.metric-graph-head{{display:flex;justify-content:space-between;gap:12px;margin-bottom:10px;font-size:.78rem}}.metric-graph-head span{{color:var(--muted);font-size:.68rem;text-transform:uppercase;letter-spacing:.06em}}.bar-row{{display:grid;grid-template-columns:130px 1fr 105px;gap:10px;align-items:center;margin:9px 0}}.bar-track{{height:25px;border-radius:6px;background:#ebe7df;overflow:hidden}}.bar{{height:100%;min-width:3px;border-radius:inherit;background:var(--green)}}.bar-row.comparator .bar{{background:var(--blue)}}.bar-row.best strong{{color:var(--green)}}.bar-row.failed .bar{{background:var(--orange)}}.bar-value{{text-align:right;font-weight:700;font-size:.82rem}}.best-mark{{display:block;color:var(--green);font-size:.62rem;text-transform:uppercase;letter-spacing:.08em}}.result-list{{display:grid;gap:1px;overflow:hidden;border:1px solid var(--line);border-radius:9px;background:var(--line)}}.result-row{{display:grid;grid-template-columns:180px minmax(260px,1fr);gap:20px;padding:13px;background:var(--card)}}.result-row.failed{{border-left:4px solid var(--orange)}}.metric-chips{{display:flex;flex-wrap:wrap;gap:7px;justify-content:flex-end}}.metric-chip{{padding:5px 8px;border-radius:7px;background:#ebe7df;font-size:.72rem}}.empty{{display:grid;place-items:center;min-height:230px;color:var(--muted)}}.coverage-wrap{{overflow:auto;border:1px solid var(--line);border-radius:16px;background:#ffffffa8}}table{{width:100%;border-collapse:collapse;font-size:.8rem}}th,td{{padding:12px;border-bottom:1px solid var(--line);text-align:center}}th:first-child{{text-align:left;white-space:nowrap}}thead th{{text-transform:uppercase;font-size:.65rem;color:var(--muted)}}.coverage.covered{{color:var(--green)}}.failures{{margin:0;padding:0;list-style:none}}.failures li{{display:flex;justify-content:space-between;gap:20px;padding:16px 20px;border-bottom:1px solid var(--line)}}.failures li span{{color:var(--orange);font-family:ui-monospace,monospace;font-size:.75rem}}.actions{{display:flex;gap:10px;margin-top:20px}}button,.button{{padding:11px 15px;border:1px solid var(--ink);border-radius:9px;background:var(--ink);color:#fff;font-weight:700;cursor:pointer;text-decoration:none}}footer{{padding:30px 0 48px;border-top:1px solid var(--line);color:var(--muted);font-size:.8rem}}@media(max-width:850px){{.summary{{grid-template-columns:1fr 1fr}}.principles,.suites,.frontiers{{grid-template-columns:1fr 1fr}}.controls{{grid-template-columns:1fr 1fr}}.result-row{{grid-template-columns:1fr}}.metric-chips{{justify-content:flex-start}}}}@media(max-width:560px){{.principles,.suites,.frontiers,.controls{{grid-template-columns:1fr}}.metric-grid{{grid-template-columns:1fr}}.bar-row{{grid-template-columns:90px 1fr 88px}}.comparison-head{{display:block}}.comparison-head small{{display:block;margin-top:5px;text-align:left}}}}
</style></head><body><nav class="shell"><a href="/">casita</a><a href="https://github.com/cachix/casita/tree/main/benchmarks">methodology &amp; raw results ↗</a></nav><main class="shell">
<header><div class="eyebrow">Correctness · performance · scale · resilience</div><h1>The benchmark<br>north star.</h1><p class="lede">One evidence system for every Casita layer. It shows where results exist, where coverage is missing, and which architectural limits fail before production finds them.</p></header>
<div class="summary"><div><strong>{len(manifest['suites'])}</strong><span>registered suites</span></div><div><strong>{len(measured_suites)}</strong><span>suites with results</span></div><div><strong>{raw_samples:,}</strong><span>raw samples</span></div><div><strong>{len(failures)}</strong><span>visible failures</span></div></div>
<section><div class="section-head"><h2>What good means</h2><p>No composite score. A fast operation that loses verification, grows memory with history, or explodes backend requests is a failed design.</p></div><ol class="principles">{principle_cards}</ol></section>
<section><div class="section-head"><h2>Huge means multiple cliffs</h2><p>Repository size is not one number. Each target isolates a resource boundary so a result explains what broke; release evidence must also include a real large repository.</p></div><div class="frontiers">{frontier_cards}</div></section>
<section><div class="section-head"><h2>Whole-system coverage</h2><p>The registry is also the roadmap. Planned and partial suites stay visible until their measurements exist.</p></div><div class="suites">{suite_cards}</div></section>
<section><div class="section-head"><h2>Explore results</h2><p>Every normalized benchmark is visible by default. Narrow the list by suite, run, workload, operation, or a comparable metric.</p></div><div class="panel"><div class="controls"><label>Suite<select id="suite"></select></label><label>Run<select id="run"></select></label><label>Workload<select id="workload"></select></label><label>Operation<select id="operation"></select></label><label>Metric<select id="metric"></select></label></div><div id="chart"></div></div></section>
<section><div class="section-head"><h2>Coverage matrix</h2><p>Dots mean the suite is responsible for measuring the dimension, not that the work is complete.</p></div><div class="coverage-wrap"><table><thead><tr><th>Suite</th>{dimension_headers}</tr></thead><tbody>{dimension_rows}</tbody></table></div></section>
<section><div class="section-head"><h2>Known cliffs</h2><p>Protocol errors, limit failures, and validation mismatches are benchmark results.</p></div><div class="panel"><ul class="failures">{failure_items}</ul></div><div class="actions"><button id="download">Download normalized catalog</button><a class="button" href="https://github.com/cachix/casita/blob/main/benchmarks/NORTH_STAR.md">Read the contract</a></div></section>
</main><footer class="shell">Catalog schema v{CATALOG_SCHEMA_VERSION} · as of {html.escape(str(catalog.get('catalog_as_of') or 'unpublished'))} · raw runner files remain authoritative</footer>
<script id="catalog" type="application/json">{catalog_json}</script><script>
const data=JSON.parse(document.getElementById('catalog').textContent);
const suites=Object.fromEntries(data.manifest.suites.map(value=>[value.id,value]));
const metricPolicies=Object.fromEntries(data.manifest.metrics.map(value=>[value.id,value]));
const entrypoints=Object.fromEntries(data.manifest.entrypoints.map(value=>[value.id,value]));
const runLabels=Object.fromEntries(data.runs.map(value=>{{const id=value.source.split('/').at(-1).replace(/\\.json$/,'');return [value.id,entrypoints[id]?.title||id.replaceAll('-',' ')]}}));
const rows=data.runs.flatMap(value=>value.observations.map(observation=>({{...observation,suite_id:value.suite_id,run_id:value.id}})));
const suite=document.getElementById('suite'),run=document.getElementById('run'),workload=document.getElementById('workload'),operation=document.getElementById('operation'),metric=document.getElementById('metric');
const unique=(items,key)=>[...new Set(items.map(item=>item[key]))].sort();
const option=(value,label)=>Object.assign(document.createElement('option'),{{value,textContent:label}});
const fill=(node,values,keep,allLabel,label=value=>value)=>{{
  node.replaceChildren(option('',allLabel),...values.map(value=>option(value,label(value))));
  node.value=values.includes(keep)?keep:'';
}};
const matches=(row,node,key)=>!node.value||row[key]===node.value;
const filteredRows=()=>rows.filter(row=>matches(row,suite,'suite_id')&&matches(row,run,'run_id')&&matches(row,workload,'workload')&&matches(row,operation,'operation'));
function cascade(){{
  fill(suite,unique(rows,'suite_id'),suite.value,'All suites',value=>suites[value].title);
  const suiteRows=rows.filter(row=>matches(row,suite,'suite_id'));
  fill(run,unique(suiteRows,'run_id'),run.value,'All runs',value=>runLabels[value]);
  const runRows=suiteRows.filter(row=>matches(row,run,'run_id'));
  fill(workload,unique(runRows,'workload'),workload.value,'All workloads');
  const workloadRows=runRows.filter(row=>matches(row,workload,'workload'));
  fill(operation,unique(workloadRows,'operation'),operation.value,'All operations');
  const operationRows=workloadRows.filter(row=>matches(row,operation,'operation'));
  fill(metric,[...new Set(operationRows.flatMap(row=>Object.keys(row.metrics)))].sort(),metric.value,'All metrics',value=>value.replaceAll('_',' '));
  render();
}}
function format(value,name){{
  if(name.includes('seconds'))return value<1?(value*1000).toFixed(1)+' ms':value.toFixed(3)+' s';
  if(name.includes('bytes')){{let unit=0,units=['B','KiB','MiB','GiB','TiB'];while(value>=1024&&unit<4){{value/=1024;unit++}}return value.toFixed(1)+' '+units[unit]}}
  return value.toLocaleString(undefined,{{maximumFractionDigits:2}});
}}
const preferredMetrics=['wall_seconds','max_rss_bytes','throughput_bytes_per_second','pack_chunk_range_requests','pack_whole_requests','request_ledger_total'];
function metricSummary(row){{
  const names=[...preferredMetrics.filter(name=>Number.isFinite(row.metrics[name])),...Object.keys(row.metrics).filter(name=>!preferredMetrics.includes(name))].slice(0,4);
  return names.map(name=>'<span class="metric-chip"><strong>'+name.replaceAll('_',' ')+'</strong> '+format(row.metrics[name],name)+'</span>').join('');
}}
const comparisonKey=row=>JSON.stringify([row.suite_id,row.run_id,row.workload,row.operation,row.profile,row.cache_policy]);
function comparisonGroups(selected){{
  const grouped=new Map();
  for(const row of selected){{const key=comparisonKey(row);if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(row)}}
  return [...grouped.values()].sort((left,right)=>comparisonKey(left[0]).localeCompare(comparisonKey(right[0])));
}}
const cacheLabel=value=>value==='warm'?'warm OS cache':value==='cold'?'cold OS cache':'cache policy: '+value;
const profileLabels={{smoke:'quick validation run',standard:'standard benchmark run',release:'release benchmark run',frontier:'frontier scale run',criterion:'Criterion run',custom:'custom benchmark run'}};
function comparisonHeader(group){{
  const row=group[0];
  return '<div class="comparison-head"><div><strong>'+suites[row.suite_id].title+' · '+row.operation+' · '+cacheLabel(row.cache_policy)+'</strong><br><small>'+row.workload+'</small></div><small>'+runLabels[row.run_id]+'<br>'+(profileLabels[row.profile]||row.profile.replaceAll('-',' ')+' run')+'</small></div>';
}}
function metricDirection(name){{
  return metricPolicies[name]?.direction||'neutral';
}}
function comparableMetrics(group){{
  const counts=new Map();
  for(const row of group)for(const [name,value] of Object.entries(row.metrics))if(Number.isFinite(value))counts.set(name,(counts.get(name)||0)+1);
  const names=[...counts].filter(([,count])=>count>1).map(([name])=>name);
  return [...preferredMetrics.filter(name=>names.includes(name)),...names.filter(name=>!preferredMetrics.includes(name)).sort()];
}}
function metricGraph(group,name){{
  const values=group.filter(row=>Number.isFinite(row.metrics[name]));
  const max=Math.max(...values.map(row=>row.metrics[name]));
  const direction=metricDirection(name);
  const bestValue=direction==='higher'?max:direction==='lower'?Math.min(...values.map(row=>row.metrics[name])):null;
  const directionLabel=direction==='higher'?'higher is better ↑':direction==='lower'?'lower is better ↓':'reported value';
  return '<div class="metric-graph"><div class="metric-graph-head"><strong>'+name.replaceAll('_',' ')+'</strong><span>'+directionLabel+'</span></div>'+values.sort((left,right)=>left.metrics[name]-right.metrics[name]).map(row=>{{const best=values.length>1&&row.metrics[name]===bestValue;return '<div class="bar-row '+row.status+(row.implementation.startsWith('casita')?'':' comparator')+(best?' best':'')+'"><div><strong>'+row.implementation+'</strong>'+(best?'<span class="best-mark">best</span>':'')+'</div><div class="bar-track"><div class="bar" style="width:'+(max?Math.max(1.5,row.metrics[name]/max*100):1.5)+'%"></div></div><div class="bar-value">'+format(row.metrics[name],name)+'</div></div>'}}).join('')+'</div>';
}}
function render(){{
  const chart=document.getElementById('chart');
  let selected=filteredRows();
  if(!selected.length){{chart.innerHTML='<div class="empty">No published results for this combination.</div>';return}}
  if(!metric.value){{
    const groups=comparisonGroups(selected);
    chart.innerHTML='<div class="chart-head"><strong>'+selected.length+' benchmark results</strong><span>'+groups.length+' comparable cohorts · grouped by metric</span></div><div class="comparison-groups">'+groups.map(group=>{{const metrics=comparableMetrics(group);const body=metrics.length?'<div class="metric-grid">'+metrics.map(name=>metricGraph(group,name)).join('')+'</div>':'<div class="result-list">'+group.sort((left,right)=>left.implementation.localeCompare(right.implementation)).map(row=>'<div class="result-row '+row.status+'"><div><strong>'+row.implementation+'</strong><br><small>'+row.samples+' sample'+(row.samples===1?'':'s')+'</small></div><div class="metric-chips">'+metricSummary(row)+'</div></div>').join('')+'</div>';return '<div class="comparison-group">'+comparisonHeader(group)+body+'</div>'}}).join('')+'</div>';
    return;
  }}
  selected=selected.filter(row=>Number.isFinite(row.metrics[metric.value]));
  if(!selected.length){{chart.innerHTML='<div class="empty">No published value for this metric.</div>';return}}
  const groups=comparisonGroups(selected);
  chart.innerHTML='<div class="chart-head"><strong>'+selected.length+' comparable results</strong><span>'+groups.length+' cohorts · '+metric.value.replaceAll('_',' ')+'</span></div><div class="comparison-groups">'+groups.map(group=>'<div class="comparison-group">'+comparisonHeader(group)+metricGraph(group,metric.value)+'</div>').join('')+'</div>';
}}
suite.addEventListener('change',cascade);run.addEventListener('change',cascade);workload.addEventListener('change',cascade);operation.addEventListener('change',cascade);metric.addEventListener('change',render);
document.getElementById('download').addEventListener('click',()=>{{const url=URL.createObjectURL(new Blob([JSON.stringify(data,null,2)+'\\n'],{{type:'application/json'}}));const anchor=Object.assign(document.createElement('a'),{{href:url,download:'casita-benchmark-catalog.json'}});anchor.click();URL.revokeObjectURL(url)}});
cascade();
</script></body></html>"""


def discover_default_results() -> list[pathlib.Path]:
    return sorted(pathlib.Path("benchmarks/baselines").glob("*.json"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=pathlib.Path, default=pathlib.Path("benchmarks/manifest.json"))
    parser.add_argument("--result", action="append", type=pathlib.Path, dest="results")
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("docs/public/benchmarks/index.html"))
    parser.add_argument("--catalog-output", type=pathlib.Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        results = args.results or discover_default_results()
        if not results:
            raise DashboardError("no benchmark results selected")
        manifest = load_manifest(args.manifest)
        catalog = build_catalog(manifest, results)
        validate_catalog(catalog)
        common.write_atomic(args.output, render_html(catalog))
        if args.catalog_output:
            common.write_atomic(args.catalog_output, json.dumps(catalog, indent=2, sort_keys=True) + "\n")
        print(f"dashboard: {args.output}")
        if args.catalog_output:
            print(f"catalog: {args.catalog_output}")
        return 0
    except (DashboardError, common.BenchmarkError, OSError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
