#!/bin/bash

# Usage: ./putObject.sh <file_path> [s3_bucket] [s3_key]

DEFAULT_BUCKET="nexus-lambda-events"

startTimeEpochMs=$(date +%s%3N)

if [ $# -lt 1 ]; then
    echo "Usage: $0 <file_path> [s3_bucket] [s3_key]"
    exit 1
fi

FILE_PATH="$1"
S3_BUCKET="${2:-$DEFAULT_BUCKET}"
S3_KEY="${3:-$(basename "$FILE_PATH")}"

echo "Uploading $FILE_PATH to s3://$S3_BUCKET/$S3_KEY ..."

aws s3 cp "$FILE_PATH" "s3://$S3_BUCKET/$S3_KEY"

endTimeEpochMs=$(date +%s%3N)
elapsedTimeMs=$((endTimeEpochMs - startTimeEpochMs))

if [ $? -eq 0 ]; then
    echo "Upload successful: s3://$S3_BUCKET/$S3_KEY"
else
    echo "Upload failed."
    exit 3
fi

echo "Elapsed time (ms): $elapsedTimeMs"

json_output="{\"startTimeMs\": $startTimeEpochMs, \"endTimeMs\": $endTimeEpochMs, \"elapsedTimeMs\": $elapsedTimeMs}"
timestamp=$(date +"%Y%m%d_%H%M%S")
json_file="upload_times_${timestamp}.json"
echo "$json_output" > "$json_file"
