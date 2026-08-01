#!/usr/bin/env bash
set -euo pipefail

NODE_ID="0"
OUT_DIR="default"
BENCH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--node-id) NODE_ID="$2"; shift 2 ;;
        -o|--out)     OUT_DIR="$2"; shift 2 ;;
        -b|--bench)   BENCH="$2"; shift 2 ;;
        --)           shift; break ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
done

ARGS="$@"

# ==== CONFIGURATION ===========================================================
SETUP="./bench/setup.sh"
# ==============================================================================

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

DIR_RESULTS="$OUT_DIR/results"
DIR_LOGS="$OUT_DIR/logs"

print() {
    echo "[BENCH][Node${NODE_ID}] " "$@"
}

mkdir -p "$DIR_RESULTS" "$DIR_LOGS"
print "Created output directory: $OUT_DIR"

METADATA="$OUT_DIR/metadata.txt"
{
    echo "==== Node Metadata ===="
    echo "Node ID: $NODE_ID"
    echo "Timestamp: $(date)"
    echo "Hostname: $(hostname)"
    echo "Kernel: $(uname -r)"
} > "$METADATA"

if [ -n "$SETUP" ] && [ -f "$PROJECT/$SETUP" ]; then
    print "Running setup script"
    bash "$PROJECT/$SETUP" > "$DIR_LOGS/setup.log" 2>&1 || {
        print "Error: Setup script failed. Check $DIR_LOGS/setup.log" >&2
        exit 1
    }
fi

print "Starting benchmark: $BENCH $ARGS"
START=$(date +%s)

$BENCH --out "$DIR_RESULTS" --node-id "$NODE_ID" $ARGS 2>&1 | tee "$DIR_LOGS/benchmark.log"

END=$(date +%s)
echo "Benchmark Duration: $((END - START)) seconds" >> "$METADATA"
print "Finished. Results written to $DIR_RESULTS"