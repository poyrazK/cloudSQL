#!/usr/bin/env bash
rm -rf ../test_data || true
mkdir -p ../test_data
cd ../build
make -j4
./sqlEngine -p 5438 -d ../test_data &
SQL_PID=$!
sleep 2
echo "Running E2E"
python3 ../tests/e2e/e2e_test.py 5438
RET=$?
kill $SQL_PID
exit $RET
