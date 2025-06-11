#!/bin/bash

REDIS_POD=redis-deployment-7b6d6579b8-zmrwn
BENCHMARK_POD=main-benchmark

POLICY_NOOP="{\"id\":\"pulsar\",\"system\":\"pulsar\",\"scope\":\"pulsar\",\"stream\":\"pulsar\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false,\"dataSource\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Starting benchmark..."

echo "Redis policy reset..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:pulsar "$POLICY_NOOP"
sleep 5s


echo "Applying main Pulsar manifest.."
kubectl delete deployment pulsar-standalone
sleep 15s
kubectl apply -f pulsar.yaml
sleep 75s
MAIN_PULSAR_POD=$(kubectl get pods -l app=pulsar-standalone --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

if [ -z "$MAIN_PULSAR_POD" ]; then
  echo "No running pods found with label app=pulsar-standalone"
  exit
else
  echo "Running pod found: $MAIN_PULSAR_POD"
fi


echo //////////////////////////////////////////// Executing No Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 5ms5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pulsar-benchmark.py -b pulsar-bucket -d experiments/pulsar-driver-5k-uncompressed.yaml -w experiments/workloads -r first/5ms5kb/uncompressed 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pulsar-benchmark.py -b pulsar-bucket -d experiments/pulsar-driver-100k-uncompressed.yaml -w experiments/workloads -r first/100ms10kb/uncompressed  
  sleep 30s
done

# #////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing Client Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 5ms5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pulsar-benchmark.py -b pulsar-bucket -d experiments/pulsar-driver-5k-compressed.yaml -w experiments/workloads -r first/5ms5kb/client 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pulsar-benchmark.py -b pulsar-bucket -d experiments/pulsar-driver-100k-compressed.yaml -w experiments/workloads -r first/100ms10kb/client  
  sleep 30s
done


echo //////////////////////////////////////////// Executing Nexus Compression ////////////////////////////////////////////

POLICY_COMPRESSION="{\"id\":\"pulsar\",\"system\":\"pulsar\",\"scope\":\"pulsar\",\"stream\":\"pulsar\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.CompressionStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":true,\"dataRouting\":false,\"dataSource\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]},{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false,\"dataSource\":false},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Updating policy..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:pulsar "$POLICY_COMPRESSION"

sleep 5s

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 5ms5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pulsar-benchmark.py -b pulsar-bucket -d experiments/pulsar-driver-5k-uncompressed.yaml -w experiments/workloads -r first/5ms5kb/nexus 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pulsar-benchmark.py -b pulsar-bucket -d experiments/pulsar-driver-100k-uncompressed.yaml -w experiments/workloads -r first/100ms10kb/nexus  
  sleep 30s
done

kubectl exec -it $REDIS_POD -- redis-cli SET policy:pulsar "$POLICY_NOOP"

echo "Pulsar Benchmarking Done!"