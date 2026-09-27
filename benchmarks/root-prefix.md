# Named-root prefix reads

`Repository::roots_under` and retained readers list one root and its
descendants from a stable revision. SQLite uses the named-root key index to
read only the requested range. Other metadata backends retain the same
semantics through a filtered root stream.

Run the correctness test and permanent benchmark:

```sh
devenv shell cargo test --features experimental --test application_api root_prefix_reads_exact_descendants_and_retained_revision
devenv shell benchmark run root-prefix
devenv shell benchmark all --suites root-prefix --profile smoke --output /tmp/casita-root-prefix
```

The benchmark covers 255 and 257 matching roots on either side of the
256-row SQLite page boundary, then 4,096 roots with 8 and 257 matches to
measure sparse and dense prefixes. Every timed result checks names and counts;
repository `fsck` runs after each case. JSON lines report median prefix and
full-scan latency in microseconds. Set
`CASITA_BENCH_ROOT_PREFIX_ITERATIONS` to change the default five repetitions.
