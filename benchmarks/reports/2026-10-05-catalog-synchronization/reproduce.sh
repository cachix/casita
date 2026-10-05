#!/usr/bin/env bash
# Run from the configured development environment, on an otherwise idle host.
set -euo pipefail
if [[ ${1:-} == --help || $# -gt 1 ]]; then
    echo "Usage: bash reproduce.sh [CPU_NUMBER] (default: 2)"
    exit 0
fi
bench_cpu=${1:-2}
report_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(git -C "$report_dir" rev-parse --show-toplevel)
bench_work=$(mktemp -d "${TMPDIR:-/tmp}/casita-catalog-compare.XXXXXX")
echo "Worktrees and results: $bench_work"
git -C "$repo_dir" worktree add --detach "$bench_work/baseline" \
    84ec2920791276cd4ad8c029cd60529810e15705
git -C "$repo_dir" worktree add --detach "$bench_work/candidate" HEAD
git -C "$bench_work/baseline" apply "$report_dir/baseline-adapter.patch"
mkdir "$bench_work/bin"
for variant in baseline candidate; do
    gzip -dc "$report_dir/Cargo.lock.gz" > "$bench_work/$variant/Cargo.lock"
    (
        cd "$bench_work/$variant"
        CARGO_TARGET_DIR="$bench_work/target-$variant" cargo test --locked \
            -p casita --release --no-default-features --features native \
            --lib --no-run --message-format=json > "$bench_work/$variant-build.jsonl"
    )
    python3 - "$bench_work/$variant-build.jsonl" "$bench_work/bin/$variant" <<'PY'
import json, shutil, sys
with open(sys.argv[1]) as log:
    artifacts = [json.loads(line) for line in log]
executables = [a["executable"] for a in artifacts
               if a.get("reason") == "compiler-artifact"
               and a.get("executable") and a["target"]["kind"] == ["lib"]
               and a["target"]["name"] == "casita"]
assert len(executables) == 1, executables
shutil.copy2(executables[0], sys.argv[2])
PY
done
run_sample() {
    local variant=$1 label=$2 iterations=$3
    (
        cd "$bench_work/candidate"
        taskset -c "$bench_cpu" python3 -m benchmarks.cli run \
            catalog-synchronization --probe-binary "$bench_work/bin/$variant" \
            --entries 65536 --iterations "$iterations" --repetitions 1 \
            --output "$bench_work/paired-$iterations/$label.json"
    )
}
for iterations in 100 500; do
    mkdir "$bench_work/paired-$iterations"
    run_sample baseline warmup-baseline "$iterations"
    run_sample candidate warmup-candidate "$iterations"
    for pair in {1..10}; do
        order=(baseline candidate)
        if (( pair % 2 == 0 )); then order=(candidate baseline); fi
        for variant in "${order[@]}"; do
            printf -v label 'pair-%02d-%s' "$pair" "$variant"
            run_sample "$variant" "$label" "$iterations"
        done
    done
done
echo "Completed comparison: $bench_work"
