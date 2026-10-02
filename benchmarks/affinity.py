"""Validated optional CPU affinity for reproducible benchmark processes."""
import argparse
import contextlib
import os

from benchmarks.suites import repository as common


def cpu_list(value):
    try:
        cpus = sorted({int(part) for part in value.split(",")})
    except ValueError as error:
        raise argparse.ArgumentTypeError("CPU IDs must be comma-separated integers") from error
    if not cpus or cpus[0] < 0:
        raise argparse.ArgumentTypeError("CPU IDs must be nonnegative")
    return cpus


@contextlib.contextmanager
def cpu_affinity(cpus):
    if cpus is None:
        yield
        return
    if not hasattr(os, "sched_setaffinity"):
        raise common.BenchmarkError("CPU affinity is not supported on this platform")
    previous = os.sched_getaffinity(0)
    if not set(cpus).issubset(previous):
        raise common.BenchmarkError("requested CPUs are outside the allowed affinity")
    os.sched_setaffinity(0, cpus)
    try:
        yield
    finally:
        os.sched_setaffinity(0, previous)
