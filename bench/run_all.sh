#!/usr/bin/env bash
set -euo pipefail

SWEEPS=(
    "radix_sweep.py"
    "relation_sweep.py"
    "thread_sweep.py"
    "zipf_sweep.py"
)

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$PROJECT/bench_distr.sh"

echo "Running all benchmarks..."

for SWEEP in "${SWEEPS[@]}"; do
    echo "Starting $SWEEP"
    SWEEP="./bench/$SWEEP" bash "$BENCH"
    sleep 5
done

echo "All benchmarks completed!"
