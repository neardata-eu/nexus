import time
import subprocess
from kubernetes import client, config
from kubernetes.stream import stream

'''
This is the main deployment script for instantiating Nexus and RSM Kafka pods with Kubernetes,
alongside other essential services. It also creates a bucket and a topic for basic testing.

Testing can be done by exec-ing into the Kafka Client pod and producing/consuming messages.
'''
namespace = "nexus-ns"
config.load_kube_config()
v1 = client.CoreV1Api()

def run_command(tool, command):
    try:
        # Run command and capture the output
        result = subprocess.run([tool] + command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Print the command output
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        # Print error output if the command fails
        print(f"Error: {e.stderr}")


def create_minIO_bucket():
    try: 
        
        bucket_name = 'data/kafka-bucket'
        label_selector = 'app=minio'
        command = ['/bin/sh', '-c', f'mc mb {bucket_name}']
        
        # Retrieve the list of pods based on the label selector
        pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)

        if not pods.items:
            print("MinIO pod could not be found")
            exit(1)

        minio_pod = pods.items[0].metadata.name
        print(f"Found MinIO pod: {minio_pod}")
        
        # Exec into the minIO pod and create the bucket
        resp = stream(v1.connect_get_namespaced_pod_exec,
                name=minio_pod,
                namespace=namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False)
        
        print(f'Created {bucket_name} bucket. Response: "{resp}"')

    except Exception as e:
        print(f"Error: {str(e)}")

def create_kafka_topic():
    try: 
        label_selector = 'app=kafka-client'
        topic_name='topic1'
        partitions='1'
        retention_duration='1000'
        command = ['/bin/sh', '-c', f'`kafka-topics --bootstrap-server kafka-svc:9092 --create --topic {topic_name} --partitions {partitions}  --replication-factor 1 --config remote.storage.enable=true --config local.retention.ms={retention_duration}`']
        
        pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)

        if not pods.items:
            print("Kafka Client pod could not be found")
            exit(1)

        kafka_client_pod = pods.items[0].metadata.name
        print(f"Found Kafka Client pod: {kafka_client_pod}")
        
        resp = stream(v1.connect_get_namespaced_pod_exec,
                name=kafka_client_pod,
                namespace=namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False)
        
        print(f'Topic named: "{topic_name}" created in Kafka.')

    except Exception as e:
        print(f"Error: {str(e)}")

def populateRedis():
    try:
        label_selector = 'app=redis'
        pods = v1.list_namespaced_pod(namespace, label_selector=label_selector)
        if not pods.items:
            print("Redis pod could not be found")
            exit(1)
        
        redis_pod = pods.items[0].metadata.name
        print(f"Found Redis pod: {redis_pod}. Populating test metadata...")

        #Keep this updated with latest system metadata changes
        keys_values = {
            "swarmletdescriptor:1": "{\"serviceEndpoint\":\"1\",\"region\":\"EDGE\",\"hardware\":\"NONE\"}",

            "streamletdescriptor:io.nexus.streamlets.utils.NoOpStreamlet2": "{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false}",

            "streamletcode:io.nexus.streamlets.functions.NoOpStreamlet2": "package io.nexus.streamlets.functions;\n\nimport io.nexus.streamlets.ByteStreamlet;\nimport io.nexus.streamlets.context.StreamletContext;\nimport io.nexus.streamlets.metadata.Policy;\nimport io.nexus.streamlets.utils.StreamletIO;\n\nimport java.io.InputStream;\nimport java.io.OutputStream;\nimport java.util.Random;\n\nimport org.slf4j.Logger;\n\n/**\n * \n * A basic Streamlet that simply reads the provided data\n * \n */\npublic class NoOpStreamlet2 extends ByteStreamlet {\n\n    private final String name = \"NOOP\";\n    private final int randomNumber;\n\n    public NoOpStreamlet2() {\n        this.randomNumber = new Random().nextInt();\n    }\n\n    @Override\n    public void processPutBytes(StreamletIO event, StreamletContext context) {\n        Logger logger = context.getLogger();\n        Policy policy = context.getPolicy();\n\n        // Example of adding metadata\n        context.putUserMetadata(\"hello\" + randomNumber, String.valueOf(randomNumber));\n        logger.info(\"Storing metadata tag: Key: \" + \"hello\" + randomNumber + \",Value: \" + randomNumber);\n\n        logger.info(\"PUT - Executing Streamlet: \" + name + \", as part of pipeline: {}\", policy.getPipeline());\n        doProcess(event.input(), event.output(), logger);\n    }\n\n    @Override\n    public void processGetBytes(StreamletIO event, StreamletContext context) {\n        Logger logger = context.getLogger();\n        Policy policy = context.getPolicy();\n\n        // Example of getting metadata\n        String tagValue = context.getUserMetadata(\"hello\" + randomNumber);\n        logger.info(\"Getting metadata tag: Key: \" + \"hello\" + randomNumber + \",Value: \" + tagValue);\n\n        logger.info(\"GET - Executing Streamlet: \" + name + \", as part of pipeline: {}\", policy.getPipeline());\n        doProcess(event.input(), event.output(), logger);\n    }\n\n    private void doProcess(InputStream input, OutputStream output, Logger logger) {\n        int totalBytesRead = 0;\n        try {\n            int currentBytesRead = 0;\n            byte[] target = new byte[8192];\n            while ((currentBytesRead = input.read(target)) != -1) {\n                output.write(target, 0, currentBytesRead);\n                totalBytesRead += currentBytesRead;\n            }\n            logger.info(\"Finished Streamlet \" + name + \" operations. Processed Bytes: \" + totalBytesRead);\n            output.close();\n        } catch (Exception e) {\n            logger.error(\"Error deserializing the input\", e);\n        }\n    }\n}\n",

            "policy:P1":"{\"id\":\"P1\",\"system\":\"kafka\",\"scope\":\"topic1\",\"stream\":\"topci1\",\"pipeline\":[{\"streamlet\":{\"id\":\"io.nexus.streamlets.functions.NoOpStreamlet2\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":false,\"transformsContent\":false,\"dataRouting\":false},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}"
        }

        #Populate the DB with the dictionary's keys and values
        for key, value in keys_values.items():
            command = ['redis-cli', 'SET', key, value]
            resp = stream(v1.connect_get_namespaced_pod_exec,
                name=redis_pod,
                namespace=namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False)
            print(f'Executed command for {key}: {resp}')

        print("Keys set successfully in Redis.")

    except Exception as e:
        print(f"An error occurred while populating metadata: {e}")


def main():

    try:
        run_command("kubectl", ["apply", "namespace", f"{namespace}"])

        run_command("kubectl", ["apply", "-f", "./nexus.yaml"])
        run_command("kubectl", ["apply", "-f", "./redis.yaml"])

        run_command("kubectl", ["apply", "-f", "./minio.yaml"])
        print("Waiting for MinIO pod to spin up...")
        time.sleep(15)
        create_minIO_bucket();

        run_command("kubectl", ["apply", "-f", "./zookeeper.yaml"])
        print("Waiting for ZooKeeper pod to start...")
        time.sleep(10);

        run_command("kubectl", ["apply", "-f", "./kafka.yaml"])
        
        #Creating a client to interact with the main broker
        run_command("kubectl", ["apply", "-f", "./kafka_client.yaml"])
        print("Waiting for the Kafka Client pod to start...")
        time.sleep(15);
        create_kafka_topic();

        run_command("kubectl", ["apply", "-f", "./prometheus.yaml"])
        print("Waiting for Prometheus's pod to start...")
        time.sleep(5);

        run_command("kubectl", ["apply", "-f", "./grafana.yaml"])

        #install NGINX and MetalLB ot expose proper endpoints for Prometheus and Grafana
        run_command("kubectl", ["apply", "-f", "https://raw.githubusercontent.com/metallb/metallb/v0.14.9/config/manifests/metallb-native.yaml"])
        run_command("kubectl", ["apply", "-f", "https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.12.0/deploy/static/provider/cloud/deploy.yaml"])

        #IMPORTANT: CREATE THIS MANIFEST WITH SUITABLE IP POOL FOR EXPOSURE
        #The manifest should contain basic IPAddressPool and L2Advertisement resources within the metallb-system namespace
        #For reference: https://kubernetes.github.io/ingress-nginx/deploy/baremetal/#a-pure-software-solution-metallb
        #If not set correctly, nginx service will have a <pending> external IP 
        run_command("kubectl", ["apply", "-f", "./metallb-config.yaml"])

        #Creating an ingress to expose endpoints for metrics
        run_command("kubectl", ["apply", "-f", "./ingress.yaml"])

        #Populate Redis with test metadata
        populateRedis()

        print("Nexus test environment is now up and running.")


    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
