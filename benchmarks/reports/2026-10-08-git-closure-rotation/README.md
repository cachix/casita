# Git closure writer rotation

A `GitClosureImport` run through `Repository::import` used one mutation writer
for the whole import, so its staging pin grew with every published object. The
importer now rotates that writer with `MutationSession::rotate` once a decoded
group arrives after eight publications, keeping what was published under a
read hold. Locally, a publication holds up to 4,096 objects.

## Measurements

`git-closure-import-rotation` imports 1 KiB files from a loose Git object
directory into a local repository, with a 64 MiB decoded-source budget and 16
concurrent writes. Each process builds its fixture, imports it cold, and then
runs the warm, subtree-delta and wide-delta imports and the suite's exhaustive
audits. Baseline and candidate processes alternate in five pairs per count.

The fixture is a root tree, one subtree and the files, decoded 16 at a time. The
32,766th file completes the eighth publication; its group is then published
whole, and only the 32,769th file starts a new group. So 32,768 files never
rotate and 32,769 files rotate once, while 65,536 and 131,072 files rotate once
and three times. The probe counts the importer's rotation events, and the suite
fails unless every candidate cold import rotated exactly that often. The
baseline never rotates.

| Files | Rotations | Cold import s | Written MiB | Peak RSS MiB |
|---:|---:|---|---|---|
| 32,768 | 0 | 8.55 → 8.52 | 1,514.7 → 1,514.7 | 158 → 158 |
| 32,769 | 1 | 8.55 → 8.44 | 1,515.4 → 1,515.4 | 151 → 164 |
| 65,536 | 1 | 22.12 → 22.45 | 6,478.2 → 6,426.7 | 283 → 263 |
| 131,072 | 3 | 63.77 → 63.88 | 26,194.3 → 25,921.9 | 560 → 423 |

Values are the baseline's and the candidate's medians for the cold import.
`peak_rss_bytes` is the resident high-water mark during that import: the probe
resets it just before, so it starts from the resident fixture, which both builds
share.

- At 131,072 files, peak RSS was lower in every pair: 512 to 585 MiB before
  and 395 to 437 MiB after, a median paired reduction of 26.6%.
- At 65,536 files, after one rotation, it was lower in four pairs and higher in
  one (249 to 263 MiB), a median paired reduction of 4.8% with overlapping
  ranges (249 to 292 MiB before, 246 to 269 MiB after).
- A single rotation right after the boundary costs memory: at 32,769 files the
  peak was higher in every pair, 142 to 157 MiB before and 156 to 168 MiB after,
  a median of 7.3% more. At 32,768 files the two builds match.

So the first rotation costs about 13 MiB at the boundary. The import then
releases enough staging resources to break even by one rotation at 65,536 files
and to save about a quarter of peak memory after three. The probe does not
attribute memory to components; during a rotation, the previous writer, its
replacement and the read hold coexist until the replacement succeeds.

Bytes written are deterministic: they fell 0.79% at 65,536 files and 1.04% at
131,072, identically in every pair, and did not change at the boundary. Cold
import time did not change: median paired differences were within 1.3%, with
every pair within 6% either way. Warm, subtree-delta and wide-delta imports never rotate;
their times show no consistent change.

`boundary.json`, `65536.json` and `131072.json` hold every sample, including
each import's `writer_rotations`, the paired summaries, both executables' build
records, and the host.

## Build and host

The candidate is the rotation commit as committed, `crates/casita` tree
`c5bc9c061fae73c9c154c4a84c50d3f1219bf825`. The baseline is its parent, which
adds `MutationSession::rotate` without using it, built with the candidate's
probe minus its `rotation` test module, which the parent lacks: `crates/casita`
tree `ae1fefed7a7cdb17191eefe10c71b2ec3de1262e`. Both were built with Rust
1.96.0 from the upstream lockfile, and each build record names its tree. The
results also record the executable and lockfile digests and the build command.

The host was a shared AMD EPYC 9454P with 96 logical CPUs, the runs were
pinned to CPUs 32 to 47, and repositories and fixtures were on tmpfs.

## Reproduce

Build the probe from each source:

```sh
cargo test --release -p casita --no-default-features --features native,git,experimental --test git_closure_import --no-run
```

Then run the pairs, passing each build's executable:

```sh
TMPDIR=/dev/shm benchmark run git-closure-import-rotation --counts 32768,32769 --expected-rotations 32768=0,32769=1 --repetitions 5 --baseline-binary /path/to/baseline --probe-binary /path/to/candidate --no-build --cpu-affinity 32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47 --output boundary.json
TMPDIR=/dev/shm benchmark run git-closure-import-rotation --counts 65536 --expected-rotations 65536=1 --repetitions 5 --baseline-binary /path/to/baseline --probe-binary /path/to/candidate --no-build --cpu-affinity 32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47 --output 65536.json
TMPDIR=/dev/shm benchmark run git-closure-import-rotation --counts 131072 --expected-rotations 131072=3 --repetitions 5 --baseline-binary /path/to/baseline --probe-binary /path/to/candidate --no-build --cpu-affinity 32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47 --output 131072.json
```
