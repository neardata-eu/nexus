#!/bin/bash

REDIS_POD=redis-deployment-7b6d6579b8-zmrwn
BENCHMARK_POD=main-benchmark

POLICY_NOOP="{\"id\":\"test\",\"system\":\"kafka\",\"scope\":\"test\",\"stream\":\"test\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false,\"dataSource\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Starting benchmark..."

echo "Redis policy reset..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:test "$POLICY_NOOP"
sleep 5s


echo "Applying main Kafka manifest.."
kubectl delete deployment kafka-broker
sleep 15s
kubectl apply -f kafka.yaml
sleep 60s
MAIN_KAFKA_POD=$(kubectl get pods -l app=kafka-broker --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

if [ -z "$MAIN_KAFKA_POD" ]; then
  echo "No running pods found with label app=kafka-broker"
  exit
else
  echo "Running pod found: $MAIN_KAFKA_POD"
fi


echo //////////////////////////////////////////// Executing No Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 1ms100b ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-100-uncompressed.yaml -w experiments/workloads -r second/5ms5kb/uncompressed 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-10k-uncompressed.yaml -w experiments/workloads -r second/100ms100kb/uncompressed 
  sleep 30s
done

#////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing Client Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 1ms100b ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-100-compressed.yaml -w experiments/workloads -r second/5ms5kb/client 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-10k-compressed.yaml -w experiments/workloads -r second/100ms100kb/client 
  sleep 30s
done

#////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing Broker Compression ////////////////////////////////////////////

echo "Applying Kafka broker compression manifest.."
kubectl delete deployment kafka-broker
sleep 15s
kubectl apply -f kafka-broker-compression.yaml

sleep 60s
RUNNING_KAFKA_BROKER_POD=$(kubectl get pods -l app=kafka-broker --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

if [ -z "$RUNNING_KAFKA_BROKER_POD" ]; then
  echo "No running pods found with label app=kafka-broker"
  exit
else
  echo "Running pod found: $RUNNING_KAFKA_BROKER_POD"
fi

for i in 1 2 3
do
  echo //////////////////////////////////////////// Broker Compression - 1ms100b ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-100-broker.yaml -w experiments/workloads -r second/5ms5kb/broker 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// Broker Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-10k-broker.yaml -w experiments/workloads -r second/100ms100kb/broker 
  sleep 30s
done

#////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing RSM Compression ////////////////////////////////////////////

echo "Applying Kafka RSM compression manifest.."
kubectl delete deployment kafka-broker
sleep 15s

kubectl apply -f kafka-RSM-compression.yaml
sleep 60s
RUNNING_KAFKA_RSM_POD=$(kubectl get pods -l app=kafka-broker --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

if [ -z "$RUNNING_KAFKA_RSM_POD" ]; then
  echo "No running pods found with label app=kafka-broker"
  exit
else
  echo "Running pod found: $RUNNING_KAFKA_RSM_POD"
fi

for i in 1 2 3
do
  echo //////////////////////////////////////////// RSM Compression - 1ms100b ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-100-uncompressed.yaml -w experiments/workloads -r second/5ms5kb/rsm 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// RSM Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-10k-uncompressed.yaml -w experiments/workloads -r second/100ms100kb/rsm 
  sleep 30s
done

#////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing Nexus Compression ////////////////////////////////////////////

echo "Applying Kafka manifest.."
kubectl delete deployment kafka-broker
sleep 15s

kubectl apply -f kafka.yaml
sleep 60s
RUNNING_KAFKA_POD=$(kubectl get pods -l app=kafka-broker --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

if [ -z "$RUNNING_KAFKA_POD" ]; then
  echo "No running pods found with label app=kafka-broker"
  exit
else
  echo "Running pod found: $RUNNING_KAFKA_POD"
fi

POLICY_COMPRESSION="{\"id\":\"test\",\"system\":\"kafka\",\"scope\":\"test\",\"stream\":\"test\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.CompressionStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":true,\"dataRouting\":false,\"dataSource\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]},{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false,\"dataSource\":false},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Updating policy..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:test "$POLICY_COMPRESSION"

sleep 5s

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 1ms100b ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-100-uncompressed.yaml -w experiments/workloads -r second/5ms5kb/nexus 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 kafka-benchmark.py -b kafka-bucket -d experiments/kafka-driver-10k-uncompressed.yaml -w experiments/workloads -r second/100ms100kb/nexus 
  sleep 30s
done

kubectl exec -it $REDIS_POD -- redis-cli SET policy:test "$POLICY_NOOP"

echo "Kafka Benchmarking Done!"