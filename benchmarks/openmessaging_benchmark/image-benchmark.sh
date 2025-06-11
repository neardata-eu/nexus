#!/bin/bash
# This is a script to automate the benchmarking of a Nexus instance
# with a CUDA GPU installed, and S3 bucket routing support
# The metadata is collected then deleted from Nexus after every execution

kubectl() {
    microk8s kubectl "$@" 
}

REDIS_POD=redis-deployment-65fbc74dcd-7fqkc
BENCHMARK_POD=image-benchmark
MINIO_POD=minio-deployment-7478fc4d9b-jfrk2
NEXUS_GPU_POD=nexus2-deployment-79c9fc485d-m52zj
RESULTS_DIR=Routing-SP100-MS10
mkdir -p $RESULTS_DIR

clear_minio_buckets() {
  echo "Clearing the buckets..."
  local bucket_name=$1
  if ! kubectl exec $MINIO_POD -- mc rm --recursive data/$bucket_name/ --force; then
    echo "Error clearing $bucket_name bucket: $?"
    exit 1
  fi
  sleep 5s
  echo "Cleared $bucket_name bucket!"
}

remove_metadata_files() {
  echo "Clearing the metadata files..."
  local file_name=$1
  if ! kubectl exec $NEXUS_GPU_POD -- rm -f /tmp/$file_name; then
    echo "Error removing $file_name: $?"
    exit 1
  fi
}

copy_metadata_files() {
  echo "Copying metadata files..."
  local file_name=$1
  if ! kubectl cp $NEXUS_GPU_POD:/tmp/$file_name /home/ubuntu/nexus_test/routing/$RESULTS_DIR/$file_name; then
    echo "Error copying $file_name: $?"
    exit 1
  fi
}

deploy_nexus(){
  echo "Deploying Nexus GPU..."
  kubectl delete deployment nexus2-deployment
  sleep 10s
  kubectl apply -f nexus2.yaml
  sleep 15s

  NEXUS_GPU_POD=$(kubectl get pods -l app=nexus2 --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

  if [ -z "$NEXUS_GPU_POD" ]; then
    echo "No running pods found with label app=nexus2"
    exit
  else
    echo "Running pod found: $NEXUS_GPU_POD"
  fi
}

execute_benchmark(){
  local streamlet=$1
  local sampling=$2
  local locality=$3

  echo "Executing $streamlet with Sampling Percentage = $sampling..."
  sleep 5s

  RESULTS_DIR=$streamlet-SP$sampling-MS10
  mkdir -p $RESULTS_DIR

 # deploy_nexus

  clear_minio_buckets "humans"
  clear_minio_buckets "nonhumans"

  remove_metadata_files "metadata-accesses.txt"
  remove_metadata_files "image-routing-log.txt"

  POLICY_IMAGE="{\"id\":\"policy-fix\",\"system\":\"kafka\",\"scope\":\"test\",\"stream\":\"*\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.KafkaHumanDetectionEventStreamlet\",\"executeOn\":\"PUT\",\"hardware\":\"GPU\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"CLOUD\",\"arguments\":[\"sampling-percentage=$sampling\"]},{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.$streamlet\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":$locality,\"transformsContent\":false,\"dataRouting\":true},\"region\":\"CLOUD\",\"arguments\":[]}],\"storage\":[\"nonhumansBucket\",\"humansBucket\"]}"

  echo "Updating policy..."
  kubectl exec -it $REDIS_POD -- redis-cli SET policy-fix "$POLICY_IMAGE"

  sleep 5s

  echo "Starting benchmark..."
  ( kubectl exec $BENCHMARK_POD -- bin/benchmark --drivers experiments/kafka-driver.yaml  experiments/10size.yaml ) & pid=$!
  ( sleep 3m ; kill $pid ) &
  wait $pid
  echo "Finished benchmark!"

  copy_metadata_files "metadata-accesses.txt"
  copy_metadata_files "image-routing-log.txt" 

  sleep 3s
}


execute_benchmark "ImageRoutingStreamlet" "50" "true"

execute_benchmark "ImageRoutingStreamlet" "100" "true"

execute_benchmark "ImageRoutingSharedStreamlet" "50" "false"

execute_benchmark "ImageRoutingSharedStreamlet" "100" "false"

exit
