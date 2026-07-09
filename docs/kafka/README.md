# Nexus and Apache Kafka Step-by-Step Deployment Guide

## Table of Contents
- [Nexus and Apache Kafka Step-by-Step Deployment Guide](#nexus-and-apache-kafka-step-by-step-deployment-guide)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Prerequisites](#prerequisites)
    - [Images Needed](#images-needed)
  - [Configuration and Environment Variables](#configuration-and-environment-variables)
    - [**Nexus**](#nexus)
    - [**Kafka**](#kafka)
    - [**Minio**](#minio)
    - [Redis](#redis)
  - [Application Deployment](#application-deployment)
  - [Example Interception Logs](#example-interception-logs)
  - [Troubleshooting](#troubleshooting)

## Introduction
This guide provides step-by-step instructions to deploy an instance of Nexus, alongside Apache Kafka and MinIO.

## Prerequisites
- Docker Compose or Kubernetes (This guide will be using Docker, but the same steps can be applied to Kubernetes)
   - In case of Docker Compose, there is an [example docker-compose.yml](https://github.com/RaulGracia/nexus-tiered-stream-manager/blob/master/docker-compose.yml) in the repository
   - In the case of Kubernetes, please refer to the [K8s folder](https://github.com/RaulGracia/nexus-tiered-stream-manager/tree/master/k8s) of the repository
- Basic knowledge of Kafka

### Images Needed
1. **Nexus** the latest image can be [found here](https://hub.docker.com/repository/docker/hossamelghamry/nexus-tiered-stream-manager/tags/latest/sha256-869d145d993a1eb85d89bb5bc94c5850722662b0246870b7dd47b12dc54a63e8) 
2. **Kafka** instance with tiered storage support. This guide will be using [this image.](https://hubgw.docker.com/layers/raulgracia/kafka/3.7.0-tiered-storage/images/sha256-11776ac0114eb039344de0d12c4804ed1dc29bdb1e1a76a85f3aa6de6b784450)
3. **Kafka Client** to interact with the main Kafka instance (Ex: [Confluent's](https://hub.docker.com/r/confluentinc/cp-kafka))
4. **Apache ZooKeeper**
5. **Redis** as a metadata service for Nexus
6. **MinIO** as object storage

## Configuration and Environment Variables
### **Nexus**
  - `REDIS_HOST` to the name of the redis instance
  - `REDIS_PORT` to the Redis service port. `6379` by default.
  - `S3PROXY_ENDPOINT` to be Kafka's 'object storage' endpoint. Nexus will be listening to this endpoint. `http://0.0.0.0:8181` by default.
  - `WEBSERVER_PORT=1234`
  - `JCLOUDS_PROVIDER=s3`
  - `JCLOUDS_IDENTITY` to the identity of the object storage. For testing purposes, let it be `minioadmin`.
  - `JCLOUDS_CREDENTIAL` to the identity of the object storage. For testing purposes, let it be `minioadmin`.
  - `JCLOUDS_ENDPOINT=http://<minioServiceName>:<minioPort>`
  - `NEXUS_REGION=CLOUD`
  - `NEXUS_HARDWARE=NONE`
### **Kafka**
  - `KAFKA_REMOTE_LOG_STORAGE_SYSTEM_ENABLE=true` 
  - `KAFKA_RSM_CONFIG_STORAGE_S3_ENDPOINT_URL=http://nexus:8181`
  - `KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME=test-bucket` (Name of the MinIO bucket)
  - `KAFKA_RSM_CONFIG_STORAGE_AWS_ACCESS_KEY_ID=minioadmin`
  - `KAFKA_RSM_CONFIG_STORAGE_AWS_SECRET_ACCESS_KEY=minioadmin`
  - Please refer to [Aiven's repository](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/tree/main) for optional tweaks to tiered storage management
### **Minio**
  - `MINIO_ROOT_USER=minioadmin`
  - `MINIO_ROOT_PASSWORD=minioadmin`
  - Ports:
      - `9000:9000`: Main communication port
      - `9001:9001`: User interface
### Redis
-  In order for Nexus to be able to listen to metadata changes while it is running, Redis needs to be configured as follows:
1.  Exec into the `redis-cli` container:
    ```bash
    docker exec -it redis-cli bash
    ```
2.  Connect to the main Redis container:
    ```bash
    redis-cli -h redis -p 6379
    ```
3. Edit the keyspace events:
    ```
    config set notify-keyspace-events AKE
    ```

## Application Deployment
1. **Run Docker Compose**:
   - `docker compose up -d`

2. **Create the S3 Bucket**:
   - Go to Minio's interface (http://localhost:9001/buckets) and manually create a bucket named test-metadata (Same name in KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME)
   - Alternatively, you can bash into the MinIO container and run `mc mb data/test-metadata`

3. **Example Nexus Metadata**
   - For simplicity, we can define one Policy with one Streamlet, and one Swarmlet
   - You can use `EntryPoint.java` CLI tool to enter the following metadata:
   - **Streamlet Code** *Example*
     - Name: `io.nexus.streamlets.functions.NoOpStreamlet2` 
     - Path: Enter absolute path to the streamlet function
       - `/home/user/nexus_test/nexus-tiered-stream-manager/src/main/java/io/nexus/streamlets/functions/NoOpStreamlet2.java`
   - **Streamlet**
     - Name: `io.nexus.streamlets.functions.NoOpStreamlet2`
     - Execute on: `All requests`
     - Locality: `Yes`
     - Data routing: `No`
     - Hardware: `NONE`
   - **Policy**
     - Name: `P1 `
     - System: `kafka`
     - Scope: `topic1` (Kafka's topic name)
     - Stream: `0`
     - StreamletID:
       - Name: `noop-1`
       - Region `CLOUD`
       - No streamletArgs required
     - No storage values required 
   - **Swarmlet**
     - Endpoint: `1`
     - Region: `CLOUD`
     - Hardware: `NONE`
   - If there are inconveniences, you can enter these values manually into **Redis** [using the `redis-cli` container](#troubleshooting)

4. **Logs to Display**:
   - If you want to see the full results in action, there are three main logs to display:
     - Nexus's logs: `docker logs -f nexus`
     - Kafka's logs `docker logs -f kafka`
     - Bash into the **Kafka Client** container: `docker exec -it kafka-client bash`

5. **Create a Topic** :
   - In the **Kafka Client** container, create a topic of small retention:
     ```bash
     kafka-topics --bootstrap-server kafka:9092 --create --topic topic1 --partitions 1 --replication-factor 1 --config remote.storage.enable=true --config local.retention.ms=1000
     ```
   - We are creating a topic of small retention for testing purposes, but the retention values can always be configured as desired in Kafka's environment variables
  
6. **Produce to the Topic**
   - Continuing in the **Kafka Client**, produce to the created topic, and input some messages:
      ```bash
      kafka-console-producer --bootstrap-server kafka:9092 --topic topic1
      ```
   - After a small while, you should start seeing the logs for Kafka's multipart-uploading process, and Nexus intercepting said multipart requests 
   - The offloaded files should end up in the MinIO bucket under `test-bucket/<topicName>-<randId>/<partitionNumber>/<file>`

7. **Consume from the topic**
   - Similarly, you can consume the offloaded messages from MinIO through Kafka
     ```bash
     kafka-console-consumer --bootstrap-server kafka:9092 --topic topic1 --from-beginning
     ```
   - You should see similar interception logs in Nexus, indicating that the GET request has been processed

## Example Interception Logs
```
10:38:20.919 [main] INFO  io.nexus.Main - S3Proxy configuration S3ProxyProperties{identity='minioadmin', credential='minioadmin', endpoint='http://0.0.0.0:8181'}
10:38:20.933 [main] INFO  io.nexus.Main - JClouds configuration JCloudsProperties{provider='s3', identity='minioadmin', credential='minioadmin', filesystem_base_directory='/tmp/blobstore', storage_endpoint='http://minio:9000'}
10:38:20.938 [main] INFO  io.nexus.Main - Redis configuration RedisConfig{host='redis', port=6379}
10:38:20.941 [main] INFO  io.nexus.Main - Nexus configuration NexusConfig{region=CLOUD, hardware=NONE}
10:38:21.972 [main] INFO  io.nexus.Main - Initialized object storage link
10:38:22.316 [main] INFO  i.n.s.metadata.MetadataService - Instantiating MetadataService
10:38:22.316 [main] INFO  io.nexus.Main - Initialized metadata service
10:38:22.344 [main] INFO  io.nexus.Main - Initialized interceptor middleware
10:38:22.457 [main] INFO  i.n.s.CrossOriginResourceSharing - CORS allowed origins: [*]
10:38:22.457 [main] INFO  i.n.s.CrossOriginResourceSharing - CORS allowed methods: [GET, HEAD, PUT, POST, DELETE]
10:38:22.457 [main] INFO  i.n.s.CrossOriginResourceSharing - CORS allowed headers: [*]
10:38:22.458 [main] INFO  i.n.s.CrossOriginResourceSharing - CORS exposed headers: [*]
10:38:22.458 [main] INFO  i.n.s.CrossOriginResourceSharing - CORS allow credentials:
10:38:22.461 [main] INFO  org.eclipse.jetty.server.Server - jetty-11.0.22; built: 2024-06-27T16:27:26.756Z; git: e711d4c7040cb1e61aa68cb248fa7280b734a3bb; jvm 21+35-2513
10:38:22.518 [main] INFO  o.e.jetty.server.AbstractConnector - Started ServerConnector@184fb68d{HTTP/1.1, (http/1.1)}{0.0.0.0:8181}
10:38:22.522 [main] INFO  org.eclipse.jetty.server.Server - Started Server@3936df72{STARTING}[11.0.22,sto=0] @2210ms
10:38:22.522 [main] INFO  io.nexus.Main - Initialized S3 Proxy interceptor
10:38:22.522 [main] INFO  io.nexus.Main - Main server task is running...
10:39:22.477 [main] INFO  io.nexus.Main - Main server task is running...
10:40:22.481 [main] INFO  io.nexus.Main - Main server task is running...
10:40:58.078 [S3Proxy-Jetty-113] INFO  i.n.streamlets.StreamletsInterceptor - Multipart upload initiated for file topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.log
10:40:58.164 [S3Proxy-Jetty-95] INFO  i.n.streamlets.StreamletsInterceptor - Uploading part 1 for multi-part upload d9c71dc5-313b-4650-9e04-90c2e66f0cca.
10:40:58.210 [S3Proxy-Jetty-95] INFO  i.n.streamlets.StreamletsInterceptor - Completed part in multipart upload d9c71dc5-313b-4650-9e04-90c2e66f0cca with 1004 bytes.
10:40:58.281 [Thread-15] INFO  i.n.streamlets.StreamletsInterceptor - Creating blob metadata d9c71dc5-313b-4650-9e04-90c2e66f0cca.
10:40:58.285 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - PUT request for pul / topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.log.
10:40:58.368 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsExecutor - Creating durable log object.
10:40:58.369 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsExecutor - Submitting processing pipeline task for stream partition StreamPartitionPojo{container='pul', scope='topic1-tteRKk_2TjeR42unsaTBmA', stream='0', partition='0', object='00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.log'}.
10:40:58.371 [streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - PUT - Executing Streamlet: NOOP, as part of pipeline: [StreamletExecutionDescriptor{streamlet=Streamlet{id='noop-1', executeOn ='ALL', type='NONE', partitionLocality=Yes}, region=CLOUD, arguments=[]}]
10:40:58.373 [streamlet-threadpool-2] INFO  i.n.streamlets.StreamletsExecutor - Stored and processed 1004 bytes from the request input stream.
10:40:58.373 [streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - Finished Streamlet NOOP operations. Processed Bytes: 1004
10:40:58.373 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsExecutor - Submitted streamlets for processing topic1-tteRKk_2TjeR42unsaTBmA/0/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.log based on Policy{id='topic1', system='kafka', scope='topic1', stream='0', pipeline=[StreamletExecutionDescriptor{streamlet=Streamlet{id='noop-1', executeOn ='ALL', type='NONE', partitionLocality=Yes}, region=CLOUD, arguments=[]}], storage=[]}
10:40:58.375 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - This is the terminal pipeline Region (CLOUD), storing data to storage.
10:40:58.425 [Thread-15] INFO  i.n.streamlets.StreamletsInterceptor - Multipart upload complete topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.log (d9c71dc5-313b-4650-9e04-90c2e66f0cca).
10:40:58.449 [S3Proxy-Jetty-113] INFO  i.n.streamlets.StreamletsInterceptor - Skipping multipart upload interception for non-log/ledger blob: topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.indexes
10:40:58.478 [S3Proxy-Jetty-107] INFO  i.n.streamlets.StreamletsInterceptor - Continuing multipart interception skips...
10:40:58.494 [S3Proxy-Jetty-113] INFO  i.n.streamlets.StreamletsInterceptor - Skipping interception for non-log/ledger blob: topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.indexes
10:40:58.508 [Thread-17] INFO  i.n.streamlets.StreamletsInterceptor - Completely skipped multipart upload interception for non-log/ledger blob: topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.indexes
10:40:58.593 [S3Proxy-Jetty-107] INFO  i.n.streamlets.StreamletsInterceptor - Skipping multipart upload interception for non-log/ledger blob: topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.rsm-manifest
10:40:58.610 [S3Proxy-Jetty-113] INFO  i.n.streamlets.StreamletsInterceptor - Continuing multipart interception skips...
10:40:58.624 [S3Proxy-Jetty-107] INFO  i.n.streamlets.StreamletsInterceptor - Skipping interception for non-log/ledger blob: topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.rsm-manifest
10:40:58.631 [Thread-18] INFO  i.n.streamlets.StreamletsInterceptor - Completely skipped multipart upload interception for non-log/ledger blob: topic1-tteRKk_2TjeR42unsaTBmA/0/00000000000000000000-ITqd4DqWTaWhZtMQ-fd2qg.rsm-manifest
10:41:22.499 [main] INFO  io.nexus.Main - Main server task is running...
```
## Troubleshooting
- **Common Issues**:
  - Ensure Zookeeper and Kafka are running.
  - Check firewall settings if Kafka clients cannot connect.
  - Review Kafka and application logs for errors.
- **Metadata**
  - You can manually enter Nexus's metadata into Redis using the `redis-cli` container in case there are problems when running the CLI tool:
  
    ```bash
    docker exec -it redis-cli bash
    ```
    ```bash
    redis-cli -h redis -p 6379
    ```
    ```bash
    SET policy:P1 "{\"id\":\"P1\",\"system\":\"kafka\",\"scope\":\"topic1\",\"stream\":\"0\",\"pipeline\":[{\"streamlet\":{\"id\":\"noop-1\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":true},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}" 
    ```
    ```bash
    SET streamletdescriptor:noop-1 "{\"id\":\"noop-1\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":true}"
    ```
    ```bash
    SET swarmletdescriptor:1 "{\"serviceEndpoint\":\"1\",\"region\":\"CLOUD\",\"hardware\":\"NONE\"}"
    ```

