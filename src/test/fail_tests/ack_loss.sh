# #!/bin/bash
# Acknowledgment Loss at S2
# Data should be stored at least once after execution

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo $SCRIPT_DIR
echo $PROJECT_DIR

cd "$PROJECT_DIR"

kill_existing() {
    echo "Removing previous storage.."
    rm /tmp/blobstore/test-metadata/scope/stream/test.txt | true

    echo "Checking for existing processes on ports 8181 and 8182..."
    for PORT in 8181 8182; do
        PID=$(lsof -ti :$PORT 2>/dev/null || true)
        if [ -n "$PID" ]; then
            echo "  Killing process $PID on port $PORT..."
            kill -9 $PID 2>/dev/null || true
            sleep 1
            pkill -9 -P $PID 2>/dev/null || true
        fi
    done
}

kill_existing

populateRedis() {
    redis-cli PING > /dev/null 2>&1 || { echo "ERROR: Redis is not running"; exit 1; }
    echo "Populating Redis..."

    redis-cli SET policy:P1 "{\"id\":\"P1\",\"system\":\"system\",\"scope\":\"*\",\"stream\":\"*\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.AckLossStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"GPU\",\"partitionLocality\":true,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"CLOUD\",\"arguments\":[]}],\"storage\":[]}" || { echo "Failed to set policy"; return 1; }

    redis-cli SET streamletdescriptor:io.nexus.streamlets.functions.AckLossStreamlet "{\"id\":\"io.nexus.streamlets.functions.AckLossStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"GPU\",\"partitionLocality\":true,\"transformsContent\":false,\"dataRouting\":false}" || { echo "Failed to set streamlet"; return 1; }

    redis-cli SET swarmletdescriptor:http://0.0.0.0:8181/ "{\"serviceEndpoint\":\"http://0.0.0.0:8181/\",\"region\":\"EDGE\",\"hardware\":\"NONE\"}" || { echo "Failed to set swarmlet 1"; return 1; }
    redis-cli SET swarmletdescriptor:http://0.0.0.0:8182/ "{\"serviceEndpoint\":\"http://0.0.0.0:8182/\",\"region\":\"CLOUD\",\"hardware\":\"GPU\"}" || { echo "Failed to set swarmlet 2"; return 1; }

    redis-cli SET streamletcode:io.nexus.streamlets.functions.AckLossStreamlet "$(cat $PROJECT_DIR/src/main/java/io/nexus/streamlets/functions/AckLossStreamlet.java)" || { echo "Failed to set streamlet code"; return 1; }
    
    echo "Redis populated successfully"
}

populateRedis || { echo "Error while populating Redis"; exit 1; }

cleanup() {
    echo "\n[Cleanup] Stopping Nexus instances..."
    kill $PID_CLOUD 2>/dev/null || true
    kill $PID_EDGE 2>/dev/null || true
    wait $PID_CLOUD 2>/dev/null || true
    wait $PID_EDGE 2>/dev/null || true
    echo "[Cleanup] Done."
}
trap cleanup EXIT

echo "============================================"
echo " Failure Test: Acknowledgement Loss"

echo "\n[1/3] Starting EDGE Nexus on port 8181..."
S3PROXY_ENDPOINT="http://0.0.0.0:8181" \
NEXUS_REGION="EDGE" \
NEXUS_HARDWARE="NONE" \
JCLOUDS_PROVIDER='filesystem' \
JCLOUDS_FILESYSTEM_BASEDIR="/tmp/blobstore" \
REDIS_HOST="localhost" \
REDIS_PORT=6379 \
WEBSERVER_PORT=1234 \
./gradlew run > nexus-edge.log 2>&1 &
PID_EDGE=$!
echo "  EDGE PID: $PID_EDGE (logs: nexus-edge.log)"

sleep 15
echo "============================================"

echo "\n[2/3] Starting CLOUD Nexus on port 8182..."
S3PROXY_ENDPOINT="http://0.0.0.0:8182" \
NEXUS_REGION="CLOUD" \
NEXUS_HARDWARE="GPU" \
JCLOUDS_PROVIDER='filesystem' \
JCLOUDS_FILESYSTEM_BASEDIR="/tmp/blobstore" \
REDIS_HOST="localhost" \
REDIS_PORT=6379 \
WEBSERVER_PORT=1235 \
./gradlew run > nexus-cloud.log 2>&1 &
PID_CLOUD=$!
echo "  CLOUD PID: $PID_CLOUD (logs: nexus-cloud.log)"

echo "\n[Wait] Waiting for instances to start..."
sleep 15

echo "============================================"

curl -X PUT http://0.0.0.0:8181/test-metadata 2>/dev/null || true
echo "\n Creating bucket..."
sleep 5

echo "\n[3/3] Sending PUT request..."
echo -n "THIS IS THE ACKNOWLEDGMENT ERROR TEST - At least once successful" > /tmp/test_data.txt

curl -X PUT \
  -T /tmp/test_data.txt \
  http://0.0.0.0:8181/test-metadata/scope/stream/test.txt \
  --connect-timeout 3 \
  --retry 5 \
  --retry-delay 2 \
  --retry-connrefused \
  --retry-max-time 60 \
  -o /tmp/worker_crash_s1_response.txt \
  -w "/tmp/worker_crash_s1_http_code.txt" \
  2>&1 &
CURL_PID=$!

sleep 10

echo "\n============================================"
echo " Verification:"

if [ -f "/tmp/blobstore/test-metadata/scope/stream/test.txt" ]; then
    echo "  Data stored at least once - Validation successful"
    STORED_CONTENT=$(cat /tmp/blobstore/test-metadata/scope/stream/test.txt 2>/dev/null)
    echo "  Stored content: $STORED_CONTENT"
else
    echo "  Data NOT stored - Validation failed"
fi

echo "\n Test complete. Check logs:"
echo "  CLOUD: nexus-cloud.log"
echo "  EDGE:  nexus-edge.log"
