#!/bin/bash
# This is an unpolished version of a bash script
# To execute all compression and latency experiments on all three systems with their deployments
# Assuming that two Nexus instances are deployed (Edge and Cloud), with MinIO and Redis 
# properly set up with their respective buckets and metadata

kubectl() {
    microk8s kubectl "$@" 
}

REDIS_POD=redis-deployment-65fbc74dcd-7fqkc
BENCHMARK_POD=main-benchmark
DRIVER_DIR=experiments/drivers
RESULT_DIR=first
WORKLOAD_DIR=experiments/workloads

KAFKA_PYTHON_FILENAME=kafka-benchmark.py
KAFKA_BUCKET=kafka-bucket
PULSAR_PYTHON_FILENAME=pulsar-benchmark.py
PULSAR_BUCKET=pulsar-bucket

#///////////////////////////////////////////////////////////////////////////////////////////////////KAFKA

POLICY_NOOP="{\"id\":\"test\",\"system\":\"kafka\",\"scope\":\"test\",\"stream\":\"test\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Starting Kafka benchmarks..."

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
  echo //////////////////////////////////////////// No Compression - 5ms/5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-5k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/uncompressed 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-100k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms100kb/uncompressed 
  sleep 30s
done

#////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing Client Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 5ms/5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-5k-compressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/client 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-100k-compressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms100kb/client 
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
  echo //////////////////////////////////////////// Broker Compression - 5ms/5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-5k-broker.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/broker 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// Broker Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-100k-broker.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms100kb/broker 
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
  echo //////////////////////////////////////////// RSM Compression - 5ms/5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-5k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/rsm 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// RSM Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-100k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms100kb/rsm 
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

POLICY_COMPRESSION="{\"id\":\"test\",\"system\":\"kafka\",\"scope\":\"test\",\"stream\":\"test\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.CompressionStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":true,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]},{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Updating policy..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:test "$POLICY_COMPRESSION"

sleep 5s

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 5ms/5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-5k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/nexus 
  sleep 30s
done

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $KAFKA_PYTHON_FILENAME -b $KAFKA_BUCKET -d $DRIVER_DIR/kafka-driver-100k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms100kb/nexus 
  sleep 30s
done

kubectl exec -it $REDIS_POD -- redis-cli SET policy:test "$POLICY_NOOP"

kubectl delete deployment kafka-broker
echo "-------------------------------------Kafka Benchmarking Done!------------------------------------------"






#///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////PULSAR

POLICY_NOOP="{\"id\":\"pulsar\",\"system\":\"pulsar\",\"scope\":\"pulsar\",\"stream\":\"pulsar\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Starting Pulsar benchmark..."

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
  kubectl exec -it $BENCHMARK_POD -- python3 $PULSAR_PYTHON_FILENAME -b $PULSAR_BUCKET -d $DRIVER_DIR/pulsar-driver-5k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/uncompressed 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $PULSAR_PYTHON_FILENAME -b $PULSAR_BUCKET -d $DRIVER_DIR/pulsar-driver-100k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms10kb/uncompressed  
  sleep 30s
done

#////////////////////////////////////////////////////////////////

echo //////////////////////////////////////////// Executing Client Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 5ms5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $PULSAR_PYTHON_FILENAME -b $PULSAR_BUCKET -d $DRIVER_DIR/pulsar-driver-5k-compressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/client 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// Client Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $PULSAR_PYTHON_FILENAME -b $PULSAR_BUCKET -d $DRIVER_DIR/pulsar-driver-100k-compressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms10kb/client  
  sleep 30s
done


echo //////////////////////////////////////////// Executing Nexus Compression ////////////////////////////////////////////

POLICY_COMPRESSION="{\"id\":\"pulsar\",\"system\":\"pulsar\",\"scope\":\"pulsar\",\"stream\":\"pulsar\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.CompressionStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":true,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]},{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Updating policy..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:pulsar "$POLICY_COMPRESSION"

sleep 5s

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 5ms5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $PULSAR_PYTHON_FILENAME -b $PULSAR_BUCKET -d $DRIVER_DIR/pulsar-driver-5k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/5ms5kb/nexus 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 100ms100kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 $PULSAR_PYTHON_FILENAME -b $PULSAR_BUCKET -d $DRIVER_DIR/pulsar-driver-100k-uncompressed.yaml -w $WORKLOAD_DIR -r $RESULT_DIR/100ms10kb/nexus  
  sleep 30s
done

kubectl exec -it $REDIS_POD -- redis-cli SET policy:pulsar "$POLICY_NOOP"
kubectl delete deployment pulsar-standalone

echo "Pulsar Benchmarking Done!"





#//////////////////////////////////////////////////////////////////////////////////////////////////////PRAVEGA


PRAVEGA_CONTROLLER=pravega-pravega-controller-54f967d7c8-t7ppc 

POLICY_NOOP="{\"id\":\"pravega\",\"system\":\"pravega\",\"scope\":\"scopeTest\",\"stream\":\"*\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Starting Pravega benchmark..."

echo "Redis policy reset..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:pravega "$POLICY_NOOP"
sleep 5s


echo //////////////////////////////////////////// Executing No Compression ////////////////////////////////////////////

for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 5ms5kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pravega-benchmark.py -b pravega-lts -d $DRIVER_DIR/pravega-driver-100.yaml -w $WORKLOAD_DIR/pravega-100/ -r $RESULT_DIR/uncompressed 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// No Compression - 100ms10kb ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pravega-benchmark.py -b pravega-lts -d $DRIVER_DIR/pravega-driver-1000.yaml -w $WORKLOAD_DIR/pravega-1k -r $RESULT_DIR/uncompressed  
  sleep 30s
done

#////////////////////////////////////////////////////////////////


echo //////////////////////////////////////////// Executing Nexus Compression ////////////////////////////////////////////

POLICY_COMPRESSION="{\"id\":\"pravega\",\"system\":\"pravega\",\"scope\":\"scopeTest\",\"stream\":\"*\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.CompressionStreamlet\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":true,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]},{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"

echo "Updating policy..."
kubectl exec -it $REDIS_POD -- redis-cli SET policy:pravega "$POLICY_COMPRESSION"

sleep 5s

for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 100rate ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pravega-benchmark.py -b pravega-lts -d $DRIVER_DIR/pravega-driver-100.yaml -w $WORKLOAD_DIR/pravega-100 -r $RESULT_DIR/nexus 
  sleep 30s
done


for i in 1 2 3
do
  echo //////////////////////////////////////////// Nexus Compression - 1k rate ////////////////////////////////////////////
  kubectl exec -it $BENCHMARK_POD -- python3 pravega-benchmark.py -b pravega-lts -d $DRIVER_DIR/pravega-driver-1000.yaml -w $WORKLOAD_DIR/pravega-1k -r $RESULT_DIR/nexus  
  sleep 30s
done

kubectl exec -it $REDIS_POD -- redis-cli SET policy:pravega "$POLICY_NOOP"

echo "Pravega Benchmarking Done!"