# Nexus and Apache Pulsar Step-by-Step Deployment Guide

## Table of Contents
- [Nexus and Apache Pulsar Step-by-Step Deployment Guide](#nexus-and-apache-pulsar-step-by-step-deployment-guide)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Prerequisites](#prerequisites)
    - [Images Needed](#images-needed)
  - [Configuration and Environment Variables](#configuration-and-environment-variables)
    - [**Nexus**](#nexus)
    - [**Pulsar**](#pulsar)
    - [**Minio**](#minio)
    - [Redis](#redis)
  - [Application Deployment](#application-deployment)
  - [Example Interception Logs](#example-interception-logs)
  - [Troubleshooting](#troubleshooting)

## Introduction
This guide provides step-by-step instructions to deploy an instance of Nexus, alongside Apache Pulsar and MinIO.

## Prerequisites
- Docker Compose or Kubernetes (This guide will be using Docker, but the same steps can be applied to Kubernetes)
   - In case of Docker Compose, there is an [example docker-compose.yml](docker-compose.yml) in the repository
- Basic knowledge of Pulsar

### Images Needed
1. **Nexus**; The latest image can be [found here](https://hub.docker.com/repository/docker/hossamelghamry/nexus-tiered-stream-manager/tags/latest/sha256-869d145d993a1eb85d89bb5bc94c5850722662b0246870b7dd47b12dc54a63e8) 
2. **Pulsar**; This guide will be using extending upon [the official documentation's compose file](https://pulsar.apache.org/docs/4.0.x/getting-started-docker-compose/). The compose file includes Bookkeeper, Zookeeper, and the broker services
3. **Redis** as a metadata service for Nexus
4. **MinIO** as object storage

## Configuration and Environment Variables
### **Nexus**
  - `REDIS_HOST` to the name of the redis instance
  - `REDIS_PORT` to the Redis service port. `6379` by default.
  - `S3PROXY_ENDPOINT` to be Pulsar's tiered storage endpoint. Nexus will be listening to this endpoint. `http://0.0.0.0:8181` by default.
  - `JCLOUDS_PROVIDER=s3`
  - `JCLOUDS_IDENTITY` to the identity of the object storage. For testing purposes, let it be `minioadmin`.
  - `JCLOUDS_CREDENTIAL` to the identity of the object storage. For testing purposes, let it be `minioadmin`.
  - `JCLOUDS_ENDPOINT=http://<minioServiceName>:<minioPort>`
  - `NEXUS_REGION=CLOUD`
  - `NEXUS_HARDWARE=NONE`
### **Pulsar**
  - Pulsar's configuration can be set inside the broker in `broker.conf`
  - Pulsar's tiered storage offloader drivers are already bundled in the `apachepulsar/pulsar-all` image [as per the documentation](https://pulsar.apache.org/docs/4.0.x/tiered-storage-overview/#how-to-install-tiered-storage-offloaders)
  - In the compose file's directory, create `data/bookkeeper` and `data/zookeeper` directories
  - An example `broker.conf` file can be [found here](/docs/pulsar/conf/broker.conf). This file will be mounted when used with the mentioned Docker file.
```
managedLedgerOffloadDriver=s3
offloadersDirectory=./offloaders

s3ManagedLedgerOffloadBucket=test-metadata
s3ManagedLedgerOffloadServiceEndpoint=http://nexus:8181
managedLedgerOffloadServiceEndpoint=http://nexus:8181
managedLedgerOffloadBucket=test-metadata

managedLedgerOffloadMaxBlockSizeInBytes=67108864  
managedLedgerOffloadReadBufferSizeInBytes=1048576 
managedLedgerMinLedgerRolloverTimeMinutes=1 
managedLedgerMaxLedgerRolloverTimeMinutes=2 #For automatic offloading every X minutes
```
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
   - Go to Minio's interface (http://localhost:9001/buckets) and manually create a bucket named test-metadata
   - Alternatively, you can bash into the MinIO container and run `mc mb data/test-metadata`

3. **Example Nexus Metadata**
   - **IMPORTANT**: Currently, Nexus supports only one policy for Pulsar that should have a scope/stream named `pulsar`
   - As such, you can use `EntryPoint.java` CLI tool to enter the following metadata:
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
     - Name: `Pulsar`
     - System: `pulsar`
     - Scope: `pulsar`
     - Stream: `pulsar`
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
     - Pulsar's logs `docker logs -f broker`
     - Bash into the **broker**'s container: `docker exec -it broker bash`

5. **Create Components** :
   - In the broker's bash session, create a tenant:
     ```bash
     bin/pulsar-admin tenants create scope
     ```
   - Create a namespace:
     ```bash
     bin/pulsar-admin namespaces create scope/stream
     ```
   - Create a partitioned topic:
     ```bash
     bin/pulsar-admin topics create-partitioned-topic scope/stream/topic1 -p 1
     ```
6. **Edit Offloading Policies**
   - Edit the created namespace's policies:
     ```bash
     bin/pulsar-admin namespaces set-offload-policies --driver S3 --bucket test-metadata --endpoint http://nexus:8181 scope/stream
     ```
   - Edit the namespace's retention properties:
     ```bash
     bin/pulsar-admin namespaces set-offload-threshold scope/stream --size 0M
     bin/pulsar-admin namespaces set-retention scope/stream --time 72h --size 10G
     bin/pulsar-admin namespaces set-offload-deletion-lag scope/stream --lag 24h
     ``` 
   - You can check if the policies are set correctly using:
     ```bash
     bin/pulsar-admin namespaces get-offload-policies scope/stream
     ```

7. **Produce to the Topic's Partition**
   - Using the created partition:
     ```bash
     bin/pulsar-client produce persistent://scope/stream/topic1-partition-0 --messages "Hello Pulsar"
     ```
   - After producing a couple of messages, Nexus's logs should indicate the interception process taking place

## Example Interception Logs
```
10:22:59.981 [main] INFO  io.nexus.Main - Main server task is running...
10:23:12.892 [S3Proxy-Jetty-95] INFO  i.n.streamlets.StreamletsInterceptor - Multipart upload initiated for file 388b22d5-8962-4335-863c-21a4f843935e-ledger-2
10:23:12.905 [S3Proxy-Jetty-109] INFO  i.n.streamlets.StreamletsInterceptor - Uploading part 1 for multi-part upload 7474ea92-5e6d-4931-9a44-8a4b5e95ac0b.
10:23:12.933 [S3Proxy-Jetty-109] INFO  i.n.streamlets.StreamletsInterceptor - Completed part in multipart upload 7474ea92-5e6d-4931-9a44-8a4b5e95ac0b with 368 bytes.
10:23:12.943 [Thread-15] INFO  i.n.streamlets.StreamletsInterceptor - Creating blob metadata 7474ea92-5e6d-4931-9a44-8a4b5e95ac0b.
10:23:12.948 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - PUT request for pul / 388b22d5-8962-4335-863c-21a4f843935e-ledger-2.
10:23:13.014 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsExecutor - Creating durable log object.
10:23:13.016 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsExecutor - Submitting processing pipeline task for stream partition StreamPartitionPojo{container='pul', scope='pulsar', stream='pulsar', partition='0', object='388b22d5-8962-4335-863c-21a4f843935e-ledger-2'}.
10:23:13.018 [streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - PUT - Executing Streamlet: NOOP, as part of pipeline: [StreamletExecutionDescriptor{streamlet=Streamlet{id='noop-1', executeOn ='ALL', type='NONE', partitionLocality=Yes}, region=CLOUD, arguments=[]}]
10:23:13.020 [streamlet-threadpool-2] INFO  i.n.streamlets.StreamletsExecutor - Stored and processed 368 bytes from the request input stream.
10:23:13.020 [streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - Finished Streamlet NOOP operations. Processed Bytes: 368
10:23:13.019 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsExecutor - Submitted streamlets for processing pulsar/pulsar/0/388b22d5-8962-4335-863c-21a4f843935e-ledger-2 based on Policy{id='pulsar', system='pulsar', scope='pulsar', stream='pulsar', pipeline=[StreamletExecutionDescriptor{streamlet=Streamlet{id='noop-1', executeOn ='ALL', type='NONE', partitionLocality=Yes}, region=CLOUD, arguments=[]}], storage=[]}
10:23:13.023 [data-transfer-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - This is the terminal pipeline Region (CLOUD), storing data to storage.
10:23:13.043 [Thread-15] INFO  i.n.streamlets.StreamletsInterceptor - Multipart upload complete 388b22d5-8962-4335-863c-21a4f843935e-ledger-2 (7474ea92-5e6d-4931-9a44-8a4b5e95ac0b).
10:23:13.051 [S3Proxy-Jetty-109] INFO  i.n.streamlets.StreamletsInterceptor - Skipping PUT interception for non-log/ledger blob: 388b22d5-8962-4335-863c-21a4f843935e-ledger-2-index
10:23:13.094 [S3Proxy-Jetty-109] INFO  i.n.streamlets.StreamletsInterceptor - Multipart upload initiated for file 54767581-611d-4bab-994e-4750234d61b7-ledger-3
```

## Troubleshooting
- **Common Issues**:
  - Ensure Zookeeper and Bookkeeper are running.
  - Check firewall settings if Pulsar cannot connect.
  - Review the broker's logs for errors.
- **Metadata**
  - You can manually enter Nexus's metadata into Redis using the `redis-cli` container in case there are problems when running the CLI tool:
  
    ```bash
    docker exec -it redis-cli bash
    ```
    ```bash
    redis-cli -h redis -p 6379
    ```
    ```bash
    SET policy:P1 "{\"id\":\"P1\",\"system\":\"pulsar\",\"scope\":\"pulsar\",\"stream\":\"pulsar\",\"pipeline\":[{\"streamlet\":{\"id\":\"noop-1\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":true},\"region\":\"CLOUD\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}" 
    ```
    ```bash
    SET streamletdescriptor:noop-1 "{\"id\":\"noop-1\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":true}"
    ```
    ```bash
    SET swarmletdescriptor:1 "{\"serviceEndpoint\":\"1\",\"region\":\"CLOUD\",\"hardware\":\"NONE\"}"
    ```

