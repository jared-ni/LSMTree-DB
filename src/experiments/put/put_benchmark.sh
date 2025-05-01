# every DataPair is 24 bytes, so approximately 43690 DataPairs fit in 1 MB

# I want to do 1 MB, 10 MB, 100 MB, 500 MB, 1GB, 10GB

# 41666667: long
# 87380000: int

# long: 43690, # 436,900, # 4,369,000, # 21,845,000, 43,690,000 DataPairs each
# int : 87380, # 873,800, # 8,738,000, # 43,690,000, 87,380,000 DataPairs each
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[BENCHMARK] put script in directory: $script_dir"

# 1, 1MB, 10MB, 25MB, 100MB, 500MB
data_size_str=$1
workload_file="put/${data_size_str}_put.txt"

time_database_output="${script_dir}/sizeRatio2-buffer100/latency_${data_size_str}.txt"
iostat_output="${script_dir}/sizeRatio2-buffer100/iostat_${data_size_str}.txt"

# Start iostat, monitor disk I/O statistics
echo "---------NEW-TRIAL----------" >> "$iostat_output"
iostat 1 >> "$iostat_output" 2>&1&
IOSTAT_PID=$!

cd "${script_dir}"

cd ../../

# Run the C++ program for the database, redirecting output to the put_profile subdir
make clean
make benchmark
{ time ./benchmark "${workload_file}"; } >> "$time_database_output" 2>&1
PROGRAM_EXIT_CODE=$?

# Kill iostat
kill $IOSTAT_PID
sleep 1

if [ $PROGRAM_EXIT_CODE -ne 0 ]; then
    echo "${data_size_str} put workload failed with exit code ${PROGRAM_EXIT_CODE}"
else
    echo "${data_size_str} put workload successfully"
fi
