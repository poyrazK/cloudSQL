#!/usr/bin/env bash
# cleanup function to ensure background cloudSQL process is terminated
cleanup() {
    if [ -n "$SQL_PID" ]; then
        kill $SQL_PID 2>/dev/null || true
        wait $SQL_PID 2>/dev/null || true
    fi
}

# Trap exit, interrupt and error signals
trap cleanup EXIT INT ERR

rm -rf ../test_data || true
mkdir -p ../test_data
cd ../build
make -j4
./cloudSQL -p 5438 -d ../test_data &
SQL_PID=$!
sleep 2
echo "Running E2E"
python3 ../tests/e2e/e2e_test.py 5438
RET=$?
exit $RET
