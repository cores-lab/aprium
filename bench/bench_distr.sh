#!/usr/bin/env bash
set -euo pipefail

# ==== CONFIGURATION ===========================================================
HOST1="10.0.0.1"
HOST2="10.0.0.2"
BENCH="${SWEEP:-./bench/radix_sweep.py}"
ARGS=""
SNAPSHOT="./src/config.h"
# ==============================================================================

print() {
    echo "[BENCH] " "$@"
}

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
NAME=$(basename "$BENCH" | cut -f 1 -d '.')
OUT="$PROJECT/data/${TIMESTAMP}_${NAME}"

DIR_SNAPSHOT="$OUT/snapshot"
DIR_NODE0="$OUT/node0"
DIR_NODE1="$OUT/node1"

mkdir -p "$DIR_SNAPSHOT" "$DIR_NODE0" "$DIR_NODE1"
print "Created output directory: $OUT"

if [ ! -f "$BENCH" ]; then
    print "Error: Benchmark script '$BENCH' not found." >&2
    exit 1
fi
cp "$BENCH" "$DIR_SNAPSHOT/"

METADATA="$OUT/metadata.txt"
echo "==== Benchmark Run Metadata ====" > "$METADATA"
echo "Timestamp: $TIMESTAMP" >> "$METADATA"
echo "Bench Script: $BENCH" >> "$METADATA"
echo "Bench Args: $ARGS" >> "$METADATA"

if git -C "$PROJECT" rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    echo "Git Commit: $(git -C "$PROJECT" rev-parse HEAD)" > "$METADATA"
    echo "Git Branch: $(git -C "$PROJECT" rev-parse --abbrev-ref HEAD)" >> "$METADATA"
    git -C "$PROJECT" diff > "$DIR_SNAPSHOT/uncommitted_changes.diff"
fi

if [ -n "$SNAPSHOT" ]; then
    IFS=',' read -ra PATHS <<< "$SNAPSHOT"
    for p in "${PATHS[@]}"; do
        if [ -e "$PROJECT/$p" ]; then
            cp -r "$PROJECT/$p" "$DIR_SNAPSHOT/"
        else
            print "Warning: Incomplete snapshot: '$PROJECT/$p' not found. Skipping." >&2
        fi
    done
fi

print "Syncing project to host2 ($HOST2)"
ssh "$HOST2" "mkdir -p $PROJECT"
rsync -avz --delete \
  --exclude='.git/' \
  --exclude='data/' \
  --exclude='obj/' \
  --exclude='bin/' \
  "$PROJECT/" "$HOST2:$PROJECT/"

print "Launching host2"
ssh "$HOST2" "cd $PROJECT && ./bench/bench.sh --node-id 1 --out $DIR_NODE1 --bench $BENCH -- $ARGS" &
HOST2_PID=$!

print "Launching host1"
./bench/bench.sh --node-id 0 --out "$DIR_NODE0" --bench "$BENCH" -- $ARGS

print "Waiting for host2 to finish..."
wait "$HOST2_PID"

print "Consolidating results"
rsync -avz "$HOST2:$DIR_NODE1" "$DIR_NODE1"

print "Benchmark completed. Check $OUT for results."
