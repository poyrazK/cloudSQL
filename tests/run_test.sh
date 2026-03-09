#!/bin/bash

# cloudSQL E2E and Logic Test Runner
# This script builds the engine and runs both Python E2E tests and SLT logic tests.

cleanup() {
    echo "Shutting down..."
    if [ ! -z "$SQL_PID" ]; then
        kill $SQL_PID 2>/dev/null
    fi
}

# Trap exit, interrupt and error signals
trap cleanup EXIT INT ERR

# Resolve absolute paths
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
TEST_DATA_DIR="$ROOT_DIR/test_data"

rm -rf "$TEST_DATA_DIR" || true
mkdir -p "$TEST_DATA_DIR"

# Detect CPU count for parallel make
if command -v nproc >/dev/null 2>&1; then
    CPU_COUNT=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
    CPU_COUNT=$(sysctl -n hw.ncpu)
elif command -v getconf >/dev/null 2>&1; then
    CPU_COUNT=$(getconf _NPROCESSORS_ONLN)
else
    CPU_COUNT=1
fi

echo "Detected $CPU_COUNT CPUs, building with -j$CPU_COUNT"

cd "$BUILD_DIR" || exit 1
make -j"$CPU_COUNT"
./cloudSQL -p 5438 -d "$TEST_DATA_DIR" &
SQL_PID=$!
sleep 2

echo "--- Running E2E Tests ---"
python3 "$ROOT_DIR/tests/e2e/e2e_test.py" 5438
RET=$?

if [ $RET -eq 0 ]; then
    echo "--- Running SLT Logic Tests ---"
    for slt_file in "$ROOT_DIR"/tests/logic/*.slt; do
        echo "Running $slt_file..."
        python3 "$ROOT_DIR/tests/logic/slt_runner.py" 5438 "$slt_file"
        SLT_RET=$?
        if [ $SLT_RET -ne 0 ]; then
            RET=$SLT_RET
            break
        fi
    done
fi

exit $RET
