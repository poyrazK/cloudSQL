#!/bin/bash
set -ex

# cloudSQL Distributed E2E and Logic Test Runner
# This script launches a 3-node cluster (1 Coordinator, 2 Data Nodes)
# and runs the SLT logic tests against the cluster.

cleanup() {
    echo "Shutting down cluster..."
    # Capture tails of logs before killing for debugging
    if [ -f "$DIST_DATA_DIR/coord.log" ]; then
        echo "=== Coordinator Log (Full) ==="
        cat "$DIST_DATA_DIR/coord.log" || true
    fi
    if [ -f "$DIST_DATA_DIR/data1.log" ]; then
        echo "=== Data Node 1 Log (Full) ==="
        cat "$DIST_DATA_DIR/data1.log" || true
    fi
    if [ -f "$DIST_DATA_DIR/data2.log" ]; then
        echo "=== Data Node 2 Log (Full) ==="
        cat "$DIST_DATA_DIR/data2.log" || true
    fi
    
    if [ ! -z "$COORD_PID" ]; then kill $COORD_PID 2>/dev/null || true; fi
    if [ ! -z "$DATA1_PID" ]; then kill $DATA1_PID 2>/dev/null || true; fi
    if [ ! -z "$DATA2_PID" ]; then kill $DATA2_PID 2>/dev/null || true; fi
    # Final cleanup of any lingering cloudSQL processes on these ports
    pkill -f "cloudSQL.*5440" || true
    pkill -f "cloudSQL.*5441" || true
    pkill -f "cloudSQL.*5442" || true
}

# Trap exit, interrupt and error signals
trap cleanup EXIT INT ERR

# Resolve absolute paths
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
DIST_DATA_DIR="$ROOT_DIR/dist_test_data"

# Setup clean directories
rm -rf "$DIST_DATA_DIR" || true
mkdir -p "$DIST_DATA_DIR/coord" "$DIST_DATA_DIR/data1" "$DIST_DATA_DIR/data2"

# Ensure build directory exists
mkdir -p "$BUILD_DIR"

# Detect CPU count for parallel build
if command -v nproc >/dev/null 2>&1; then
    CPU_COUNT=$(nproc)
elif command -v sysctl >/dev/null 2>&1; then
    CPU_COUNT=$(sysctl -n hw.ncpu)
else
    CPU_COUNT=1
fi

echo "--- Building Engine ---"
cd "$BUILD_DIR" || exit 1
cmake -DBUILD_COVERAGE=OFF ..
make -j"$CPU_COUNT"

echo "--- Launching Cluster ---"

# 1. Start Data Node 1
echo "Starting Data Node 1 on port 5441..."
tail -f /dev/null | ./cloudSQL --mode data --port 5441 --cluster-port 6441 --data "$DIST_DATA_DIR/data1" > "$DIST_DATA_DIR/data1.log" 2>&1 &
DATA1_PID=$!

# 2. Start Data Node 2
echo "Starting Data Node 2 on port 5442..."
tail -f /dev/null | ./cloudSQL --mode data --port 5442 --cluster-port 6442 --data "$DIST_DATA_DIR/data2" > "$DIST_DATA_DIR/data2.log" 2>&1 &
DATA2_PID=$!

sleep 2

# 3. Start Coordinator (with seeds pointing to data nodes)
echo "Starting Coordinator on port 5440..."
tail -f /dev/null | ./cloudSQL --mode coordinator --port 5440 --cluster-port 6440 --data "$DIST_DATA_DIR/coord" --seed "127.0.0.1:6441,127.0.0.1:6442" > "$DIST_DATA_DIR/coord.log" 2>&1 &
COORD_PID=$!

echo "Waiting for cluster bootstrap and node registration..."
# Wait up to 30s for nodes to register
for i in {1..30}; do
    if grep -q "node_6441" "$DIST_DATA_DIR/coord.log" && grep -q "node_6442" "$DIST_DATA_DIR/coord.log"; then
        echo "All nodes registered!"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

# Final stabilization sleep
sleep 5

# Check if processes are still running
if ! kill -0 $COORD_PID 2>/dev/null; then
    echo "ERROR: Coordinator failed to start. Check $DIST_DATA_DIR/coord.log"
    exit 1
fi

echo "--- Running SLT Logic Tests (Distributed) ---"
RET=0
for slt_file in "$ROOT_DIR"/tests/logic/*.slt; do
    # Skip transactions for first dist pass
    if [[ "$slt_file" == *"transactions.slt"* ]]; then
        continue
    fi
    
    echo "Running $slt_file against Coordinator (Port 5440)..."
    python3 "$ROOT_DIR/tests/logic/slt_runner.py" 5440 "$slt_file"
    SLT_RET=$?
    if [ $SLT_RET -ne 0 ]; then
        echo "FAILURE in $slt_file"
        RET=$SLT_RET
        break
    fi
done

if [ $RET -eq 0 ]; then
    echo "SUCCESS: All distributed logic tests passed!"
else
    echo "ERROR: Some distributed tests failed."
fi

exit $RET
