# Named-root collection marking

## Named roots

Collection marks everything reachable from named roots. The traversal fetched
each frontier's records before checking whether a key was already marked, so
an object reached through many edges was read once per edge. Named-root
marking now inserts keys into the mark set first and fetches only newly marked
records. Pin marking is unchanged and serves as a control: its two labels run
identical code.

The `collection-mark` suite builds reopened Turso metadata where each parent
has one edge to a shared leaf, to its own leaf, or, for the chain, to the next
directory of a single-root chain. The previous traversal is kept in the probe
module and compiled into the same executable, and each process runs one
strategy and mode; matching processes are adjacent, in alternating order.
Eight repetitions ran every case with three warm iterations each. Times are
milliseconds, the median over repetitions of each process's warm mean. Record
reads are exact. An occasional process of either label runs all its warm
iterations much faster or slower than the rest, the pin control included.
Besides the median, the table therefore counts how many of the eight adjacent
pairs the current code won.

| 8,192 parents | Memory keys | Previous | Current | Change | Faster pairs | Previous reads | Current reads | Pin control change | Pin control faster pairs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| shared | 256 | 614.4 | 555.6 | -9.6% | 4 of 8 | 16,384 | 8,193 | -9.7% | 5 of 8 |
| shared | 250,000 | 123.1 | 81.4 | -33.9% | 8 of 8 | 16,384 | 8,193 | +0.3% | 4 of 8 |
| distinct | 256 | 990.4 | 1,020.2 | +3.0% | 4 of 8 | 16,384 | 16,384 | -9.5% | 7 of 8 |
| distinct | 250,000 | 139.9 | 143.2 | +2.4% | 3 of 8 | 16,384 | 16,384 | -0.2% | 4 of 8 |
| chain | 256 | 520.9 | 400.5 | -23.1% | 4 of 8 | 8,193 | 8,193 | +54.6% | 3 of 8 |
| chain | 250,000 | 159.3 | 159.3 | -0.0% | 4 of 8 | 8,193 | 8,193 | +20.7% | 4 of 8 |

Shared graphs read each record once, halving reads. In memory, marking takes
34 percent less time at the median, and all eight pairs are faster, by 32 to
61 percent. Distinct leaves and chains have no repeated edges and read the
same records as before; three or four of their eight pairs are faster, while
the pin control, with identical code, wins 3 to 7. With a 256-key mark set the
traversal spills to disk, which dominates and hides the gain. For 127 to 257
parents the in-memory shared case improves by 28 to 37 percent, but those runs
take a few milliseconds and their pin controls move by up to 24 percent; the
read counts are the reliable result there.

Each process checks the exact marked set, its cardinality, whether it spilled
and the reopened revision. A missing named root still fails before a later
object-limit error, and a frontier whose keys were all marked already does not
stop the traversal. `build.json` and `results.json` hold this run's build and
every process sample, including first iterations, spill files and spill bytes.

## Build and host

Each build file records the build command, compiler, lockfile and executable
digests and the exact `crates/casita` tree it was built from; each results
file records the host environment and every process. Runs were pinned with
`taskset -c 36-41`, cores that share one L3 cache, on a shared AMD EPYC 9454P
host.

## Reproduce

```sh
taskset -c 36-41 benchmark run collection-mark --profile standard --repetitions 8 --output /tmp/collection-mark.json
benchmark all --suites collection-mark --profile smoke --repetitions 1 --output /tmp/collection-mark-smoke
```
