WORKLOAD_PATH="generator/workload.txt"

echo "Starting profiling and monitoring..."

iostat -dx 2 > experiments/workload_log.txt &
IOSTAT_PID=$!
valgrind --tool=cachegrind ./database < $WORKLOAD_PATH > experiments/workload_log_valgrind.txt 2>&1

kill $IOSTAT_PID

echo "IO stat stopped..."

wait $IOSTAT_PID 2>/dev/null

echo "profiling, monitoring, cache stats, and logs have been generated."
