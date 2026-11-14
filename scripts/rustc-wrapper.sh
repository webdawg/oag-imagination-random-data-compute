#!/bin/bash
# Prune old oag build generations, then run rustc.
# Cargo calls this once per compilation unit. The pruner runs once per cargo
# process, before the first unit compiles. Its test suite is standalone:
#   python3 scripts/test_prune_target.py
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
out=""
prev=""
for arg in "$@"; do
    if [[ "$prev" == "--out-dir" ]]; then
        out=$arg
        break
    fi
    prev=$arg
done

# Cargo always passes --out-dir as <target>/<profile>/deps. When a caller overrides
# --target, that path still points at the real build, so it is the reliable root. Only
# fall back to this checkout's target when cargo did not say.
if [[ -z "$out" ]]; then
    out=$(cd "$HERE/.." && pwd)/target/debug/deps
fi

cargo_pid=""
pid=$PPID
while [[ "$pid" -gt 1 ]]; do
    comm=$(ps -p "$pid" -o comm= 2>/dev/null || true)
    if [[ "$comm" == "cargo" ]]; then
        cargo_pid=$pid
        break
    fi
    pid=$(ps -p "$pid" -o ppid= 2>/dev/null | tr -d ' ')
    [[ -n "$pid" ]] || break
done

# Cargo creates the incremental/.fingerprint/build directories for every unit it is about to
# compile *before* it invokes the first rustc, and then writes into them after. Pruning one of
# those mid-build makes cargo fail with "failed to write .../invoked.timestamp: No such file or
# directory". The process's own start time is the horizon: anything newer is in use by this
# build. /proc/<pid>'s mtime is the process start time on Linux.
protect_after=0
if [[ -n "$cargo_pid" ]]; then
    protect_after=$(stat -c %Y "/proc/$cargo_pid" 2>/dev/null || echo 0)
fi

PRUNE_OUT_DIR="$out" PRUNE_CARGO_PID="$cargo_pid" \
    python3 "$HERE/prune-target.py" --once --keep 5 --out-dir "$out" --protect-after "$protect_after"
exec "$@"
