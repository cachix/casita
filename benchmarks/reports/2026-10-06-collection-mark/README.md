# Collection marking

## Named roots

Collection marks everything reachable from named roots. The traversal fetched
each frontier's records before checking whether a key was already marked, so
an object reached through many edges was read once per edge. Named-root
marking now inserts keys into the mark set first and fetches only newly marked
records. Pin marking, which this change leaves alone, serves as its control:
its two labels run identical code.

The `collection-mark` suite builds reopened Turso metadata where each parent
has one edge to a shared leaf, to its own leaf, or, for the chain, to the next
directory of a single-root chain. The previous traversal is kept in the probe
module and compiled into the same executable, and each process runs one
strategy and mode; matching processes are adjacent, in alternating order.
Eight repetitions ran every case with three warm iterations each. Times are
milliseconds, the median over repetitions of each process's warm mean. Record
reads are exact. An occasional process of either label runs all its warm
iterations much faster or slower than the rest, the pin control included.
Besides the median, the tables therefore count how many of the eight adjacent
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

## Snapshot pins

A snapshot pin marks every record created through its generation and queues
their dependencies. The traversal then read each queued key's record, although
most of those dependencies were scanned records already marked and expanded.
The queued scan dependencies are now checked against the mark set before their
records are read. Only that initial prefix of the queue is filtered, and
filtering stops after two consecutive batches find nothing already marked.

The suite's snapshot modes pin the whole generation (full), half of it
(partial), one parent (sparse), or old parents whose leaves are published
later (forward); the previous pin traversal is compiled into the probe as the
legacy strategy. Plain pins run no generation scan, so the new filter never
runs and the pins mode stays a control. Every process checks the scanned
record count and the exact marked set. Eight repetitions ran in paired,
alternating processes, as above:

| 8,192 parents | Shape | Memory keys | Previous | Current | Change | Faster pairs | Previous reads | Current reads |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| snapshot-full | shared | 256 | 619.8 | 356.7 | -42.5% | 6 of 8 | 8,192 | 0 |
| snapshot-full | shared | 250,000 | 40.4 | 25.4 | -37.2% | 8 of 8 | 8,192 | 0 |
| snapshot-full | distinct | 256 | 1,058.0 | 826.3 | -21.9% | 6 of 8 | 8,192 | 0 |
| snapshot-full | distinct | 250,000 | 70.0 | 68.1 | -2.7% | 8 of 8 | 8,192 | 0 |
| snapshot-full | chain | 256 | 698.6 | 292.6 | -58.1% | 8 of 8 | 8,192 | 0 |
| snapshot-full | chain | 250,000 | 49.3 | 31.2 | -36.8% | 8 of 8 | 8,192 | 0 |
| snapshot-partial | shared | 256 | 858.9 | 788.9 | -8.2% | 7 of 8 | 16,384 | 12,288 |
| snapshot-partial | shared | 250,000 | 61.4 | 52.8 | -14.0% | 7 of 8 | 16,384 | 12,288 |
| snapshot-partial | distinct | 256 | 1,234.9 | 1,022.7 | -17.2% | 6 of 8 | 16,384 | 12,288 |
| snapshot-partial | distinct | 250,000 | 82.5 | 72.0 | -12.7% | 8 of 8 | 16,384 | 12,288 |
| snapshot-partial | chain | 256 | 632.1 | 474.2 | -25.0% | 8 of 8 | 8,193 | 4,097 |
| snapshot-partial | chain | 250,000 | 103.1 | 125.2 | +21.5% | 3 of 8 | 8,193 | 4,097 |
| snapshot-sparse | shared | 256 | 672.3 | 717.2 | +6.7% | 3 of 8 | 16,384 | 16,383 |
| snapshot-sparse | shared | 250,000 | 52.4 | 52.6 | +0.5% | 2 of 8 | 16,384 | 16,383 |
| snapshot-sparse | distinct | 256 | 856.2 | 894.9 | +4.5% | 3 of 8 | 16,384 | 16,383 |
| snapshot-sparse | distinct | 250,000 | 66.6 | 67.3 | +1.1% | 0 of 8 | 16,384 | 16,383 |
| snapshot-sparse | chain | 256 | 462.4 | 549.2 | +18.8% | 3 of 8 | 8,193 | 8,192 |
| snapshot-sparse | chain | 250,000 | 191.0 | 154.9 | -18.9% | 7 of 8 | 8,193 | 8,192 |
| snapshot-forward | shared | 256 | 349.3 | 283.1 | -19.0% | 4 of 8 | 8,192 | 256 |
| snapshot-forward | shared | 250,000 | 40.4 | 29.2 | -27.8% | 8 of 8 | 8,192 | 256 |
| snapshot-forward | distinct | 256 | 735.9 | 642.0 | -12.8% | 5 of 8 | 8,192 | 8,192 |
| snapshot-forward | distinct | 250,000 | 54.6 | 55.7 | +2.1% | 2 of 8 | 8,192 | 8,192 |
| snapshot-forward | chain | 256 | 632.0 | 435.8 | -31.0% | 7 of 8 | 8,192 | 1 |
| snapshot-forward | chain | 250,000 | 49.8 | 33.0 | -33.7% | 8 of 8 | 8,192 | 1 |
| pins | shared | 256 | 601.8 | 661.8 | +10.0% | 1 of 8 | 16,384 | 16,384 |
| pins | shared | 250,000 | 100.9 | 101.1 | +0.3% | 4 of 8 | 16,384 | 16,384 |
| pins | distinct | 256 | 874.5 | 1,018.3 | +16.4% | 1 of 8 | 16,384 | 16,384 |
| pins | distinct | 250,000 | 129.4 | 127.4 | -1.6% | 4 of 8 | 16,384 | 16,384 |
| pins | chain | 256 | 444.3 | 507.9 | +14.3% | 3 of 8 | 8,193 | 8,193 |
| pins | chain | 250,000 | 157.4 | 154.4 | -1.9% | 5 of 8 | 8,193 | 8,193 |

In memory, covered snapshots read no records instead of 8,192 and every pair
marks faster, by 37 percent at the median with shared leaves and the chain and
by 3 percent with distinct leaves. Half-covered snapshots read 4,096 fewer
records and mark 13 to 14 percent faster with shared or distinct leaves, in
seven and eight of eight pairs. A sparse pin reads one record fewer and costs
0.5 to 1.1 percent with shared or distinct leaves, with at most two of eight
pairs faster: the price of probing batches that find nothing marked. Old
parents with newer leaves read only the records their scan did not already
mark, 256 instead of 8,192 with shared leaves and one with the chain, and mark
28 and 34 percent faster in every pair. All-new distinct leaves read every
record as before and cost 2.1 percent, with two of eight pairs faster.

The other rows vary too much on this host to read their medians as changes:
with a 256-key mark set the pin control's median moves by up to 16 percent
although both its labels run identical code. Their read counts are exact. At
511, 512 and 513 parents the read counts follow the same pattern on both sides
of the 512-key batch after which filtering can stop. `snapshot-build.json` and
`snapshot-results.json` hold this run's build and every process sample.

## Build and host

Each build file records the build command, compiler, lockfile and executable
digests and the exact `crates/casita` tree it was built from; each results
file records the host environment and every process. Runs were pinned with
`taskset -c 36-41`, cores that share one L3 cache, on a shared AMD EPYC 9454P
host.

## Reproduce

```sh
taskset -c 36-41 benchmark run collection-mark --profile standard --repetitions 8 \
  --mode named --mode pins --output /tmp/collection-mark.json
taskset -c 36-41 benchmark run collection-mark --profile standard --repetitions 8 \
  --mode pins --mode snapshot-full --mode snapshot-partial --mode snapshot-sparse \
  --mode snapshot-forward --output /tmp/collection-mark-snapshot.json
benchmark all --suites collection-mark --profile smoke --repetitions 1 --output /tmp/collection-mark-smoke
```
