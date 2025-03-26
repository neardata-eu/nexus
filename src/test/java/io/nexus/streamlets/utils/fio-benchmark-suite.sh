#!/bin/bash

# Output CSV file
RESULT_FILE="/tmp/fio_results.csv"
FIO_LOG_DIR="/tmp/fio_logs"
ZIP_FILE="/tmp/fio_results_$(date +"%Y%m%d_%H%M%S").zip"

# Ensure the log directory exists
mkdir -p "$FIO_LOG_DIR"

# Define test parameters
IO_THREADS_VALUES=(1 2 4)  # Number of concurrent processes
IO_SIZE_VALUES=(1 10 100)  # IO block size in MB

# Write CSV header (only if file doesn't exist to avoid overwriting in multiple runs)
if [ ! -f "$RESULT_FILE" ]; then
    echo "Timestamp,IO_THREADS,IO_SIZE_MB,Process,Throughput_MBps,IOPS,Latency_50th_ms,Latency_90th_ms,Latency_99th_ms" > "$RESULT_FILE"
fi

# Iterate over IO_THREADS and IO_SIZE values
for IO_SIZE in "${IO_SIZE_VALUES[@]}"; do
    for IO_THREADS in "${IO_THREADS_VALUES[@]}"; do
        echo "Running test: IO_THREADS=$IO_THREADS, IO_SIZE=$IO_SIZE MB"
        PIDS=()

        # Generate a timestamp
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

        for ((i=1; i<=IO_THREADS; i++)); do
            UNIQUE_BLOB_NAME="${BLOB_NAME}_proc${i}"
            LOG_FILE="${FIO_LOG_DIR}/fio_${IO_THREADS}procs_${IO_SIZE}MB_proc${i}_${TIMESTAMP}.log"

            # Generate FIO configuration file
            cat << EOF > /tmp/fio-test-${i}.fio
[global]
ioengine=http
http_mode=s3
https=off
http_s3_region=us-east-1
http_host=nexus-svc:9092
http_s3_keyid=${IDENTITY}
http_s3_key=${CREDENTIALS}
filename=/${BUCKET_NAME}/${UNIQUE_BLOB_NAME}
direct=1
http_verbose=${HTTP_VERBOSE}
unique_filename=1
group_reporting
runtime=60s
time_based=1
continue_on_error=all

[create]
rw=rw
bs=${IO_SIZE}M
size=${IO_SIZE}M
io_size=${IO_SIZE}M
numjobs=1
EOF

            # Run FIO and save full output to log file
            fio /tmp/fio-test-${i}.fio > "$LOG_FILE" 2>&1 &
            PIDS+=("$!")
        done

        # Wait for all fio processes to finish
        for PID in "${PIDS[@]}"; do
            wait $PID
        done

        # Aggregate results from logs
        for ((i=1; i<=IO_THREADS; i++)); do
            LOG_FILE="${FIO_LOG_DIR}/fio_${IO_THREADS}procs_${IO_SIZE}MB_proc${i}_${TIMESTAMP}.log"

            THROUGHPUT_RAW=$(grep -oP '(?<=BW=)[0-9.]+[ ]?[KMGT]?iB/s' "$LOG_FILE" | head -1 || echo "N/A")
            if [[ $THROUGHPUT_RAW == "N/A" ]]; then
                THROUGHPUT_RAW=$(grep -oP '(?<=BW=)[0-9.]+[ ]?MB/s' "$LOG_FILE" | head -1 || echo "N/A")
            fi

            THROUGHPUT=$(echo "$THROUGHPUT_RAW" | grep -oP '^[0-9.]+' || echo "N/A")
            UNIT=$(echo "$THROUGHPUT_RAW" | grep -oP '[KMGT]?iB/s' || echo "$THROUGHPUT_RAW" | grep -oP 'MB/s' || echo "N/A")

            IOPS=$(grep -oP '(?<=IOPS=)[0-9]+' "$LOG_FILE" | head -1 || echo "N/A")
            LAT_50TH=$(grep -oP '(?<=50.00th=

\[)[0-9]+' "$LOG_FILE" | head -1 || echo "N/A")
            LAT_90TH=$(grep -oP '(?<=90.00th=

\[)[0-9]+' "$LOG_FILE" | head -1 || echo "N/A")
            LAT_99TH=$(grep -oP '(?<=99.00th=

\[)[0-9]+' "$LOG_FILE" | head -1 || echo "N/A")

            # Convert throughput to MB/s if needed
            case $UNIT in
                KiB/s) THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $THROUGHPUT / 1024}") ;;
                MiB/s) ;; # Already in MB/s
                GiB/s) THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $THROUGHPUT * 1024}") ;;
                MB/s) ;; # Already in MB/s
                *) THROUGHPUT="N/A" ;;
            esac

            # Write individual process results to CSV
            echo "${TIMESTAMP},${IO_THREADS},${IO_SIZE},${i},${THROUGHPUT},${IOPS},${LAT_50TH},${LAT_90TH},${LAT_99TH}" >> "$RESULT_FILE"
        done
    done
done

# Print the content of fio_results.csv
cat "$RESULT_FILE"

# Create zip archive with logs and results
zip -r "$ZIP_FILE" "$FIO_LOG_DIR" "$RESULT_FILE" /tmp/fio-test-*.fio

echo "All tests completed. Summary: $RESULT_FILE | Logs: $FIO_LOG_DIR | Archive: $ZIP_FILE"

# Clean up files
rm -rf "$FIO_LOG_DIR"/*
rm /tmp/fio-test-*.fio
rm "$RESULT_FILE"

echo "Clean-up done."