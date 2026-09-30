# Performance work

Always add benchmarks created during performance investigations to the permanent
benchmark corpus. Register runnable cases in `benchmarks/manifest.json`, include
them in `benchmark all`, and retain correctness gates and reproducible commands.
Temporary probes and saved reports alone do not satisfy this requirement. Cover
both sides of any discovered threshold or performance cliff.

# Reliability work

The public guarantees are in `docs/src/content/docs/reference/reliability.md`
(clauses C1–C15). Open defects, the working agreement, and the per-milestone
clause audit are in `docs/reliability/findings.md` (rows F-…), which is not
published.

Drift rule: the contract, its falsifier tests, and the code agree in one
change. If they disagree, stop and record a register row before changing any
of them. Describe only what `main` does; a fix that exists only in an open PR
is a known limit, not a guarantee.

A reliability change is done when:

- the clauses it affects are created or updated in `reference/reliability.md`;
- its register rows are opened or closed, naming a test that fails before the
  change and passes after it;
- tests sit at the right tier, and race tests use gates rather than sleeps;
- user-visible changes are documented in the site docs and in `CHANGELOG.md`
  under `## [Unreleased]`;
- CI is green on Linux, macOS, and Windows; and
- a performance-affecting change also follows Performance work above: register
  the benchmark in `benchmarks/manifest.json`, add bounded arguments to `SMOKE`
  in `benchmarks/all.py` so `test_every_suite_has_a_bounded_configuration` in
  `benchmarks/tests/test_all.py` passes, keep correctness gates in the suite
  module, and cover both sides of any threshold.
