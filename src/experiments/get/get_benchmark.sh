# GET 10MB (436900 structs) DataPairs, given <data_size_str> sized database

# every DataPair is 24 bytes, so approximately 43690 DataPairs fit in 1 MB
# I want to do 1 MB, 10 MB, 100 MB, 500 MB. 

# 43690, # 436,900, # 4,369,000, # 21,845,000 DataPairs each
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[BENCHMARK] get script in directory: $script_dir"

data_size_str=$1
workload_file="get/${data_size_str}_get.txt"

time_database_output="${script_dir}/sizeRatio2-buffer100/latency_${data_size_str}.txt"
iostat_output="${script_dir}/sizeRatio2-buffer100/iostat_${data_size_str}.txt"

# Start iostat, monitor disk I/O statistics
# each trial separated by a NEW TRIAL
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
    echo "${data_size_str} get workload failed with exit code ${PROGRAM_EXIT_CODE}"
else
    echo "${data_size_str} get workload successfully"
fi