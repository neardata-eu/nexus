# Nexus and File System Step-by-Step Deployment Guide

## Table of Contents
- [Nexus and File System Step-by-Step Deployment Guide](#nexus-and-file-system-step-by-step-deployment-guide)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Prerequisites](#prerequisites)
  - [Configuration and Environment Variables](#configuration-and-environment-variables)
    - [Redis](#redis)
    - [Nexus](#nexus)
  - [Application Deployment](#application-deployment)
  - [Example Interception Logs](#example-interception-logs)
  - [Troubleshooting](#troubleshooting)

## Introduction
This guide provides step-by-step instructions to run Nexus locally, with the filesystem acting as the storage backend.

## Prerequisites
-  A running local instance of Redis 

## Configuration and Environment Variables
### Redis
-  In order for Nexus to be able to listen to metadata changes while it is running, Redis needs to be configured as follows:
 1. Connect to the Redis instance using `redis-cli`:
     ```bash
     redis-cli -h localhost -p 6379
     ```
 2. Edit the keyspace events:
     ```
     config set notify-keyspace-events AKE
     ```
### Nexus
Local configurations for Nexus can be found in the [application.properties](/src/main/resources/application.properties) file.

Important variables for this guide are:

  - `REDIS_HOST=localhost`
  - `REDIS_PORT` to the Redis service port. `6379` by default.
  - `S3PROXY_ENDPOINT` to be the interception endpoint. Nexus will be listening to this endpoint. For this guide we are using `http:0.0.0.0:8181`
  - `JCLOUDS_PROVIDER=filesystem` 
  - `JCLOUDS_IDENTITY=dev-local`
  - `JCLOUDS_CREDENTIAL=dev-credential`
  - `NEXUS_REGION=EDGE`
  - `NEXUS_HARDWARE=NONE`
  - `WEBSERVER_PORT=1234`

## Application Deployment

1. **Input Nexus Metadata**
   - For simplicity, we can define one Policy with one Streamlet, and one Swarmlet
   - You can use `EntryPoint.java` CLI tool to enter the following metadata (use `./gradlew build -Padmincli` then run the JAR `java -jar build/libs/nexus-admin-cli.jar`. Some tests might fail due to stdin, but you may safely proceed with the JAR execution)
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
     - Name: `P1`
     - System: `system`
     - Scope: `*` 
     - Stream: `*`
     - StreamletID:
       - Name: `io.nexus.streamlets.functions.NoOpStreamlet2`
       - Region `EDGE`
       - No streamletArgs required
     - No storage values required 
   - **Swarmlet**
     - Endpoint: `http://0.0.0.0:8181/`
     - Region: `EDGE`
     - Hardware: `NONE`

2. **Set Up the Filesystem**
   - Create a test file:
        ```bash
        echo 123456789 > test.txt
        ```
3. **Run Nexus**
   - In the root directory, build the project:
        ```bash
        ./gradlew build
        ```
   - Run the Nexus server:
        ```bash
        ./gradlew run
        ```
      - Nexus should indicate proper configurations, and that the `Main server task is running...`
   - In another terminal, create a bucket:
        ```bash
        curl -X PUT http://0.0.0.0:8181/test-metadata -v
        ```
   - Instantiate a PUT request to send the test file to the created bucket:
        ```bash
        curl -X PUT -T "scope/stream/test.txt" http://0.0.0.0:8181/test-metadata/scope/stream/test.txt -v
        ```
        - Nexus's logs should be displaying that an interception has taken place, alongside the number of bytes being read and the streamlet executed
   - Similarly, you can GET the test file from the created bucket:
        ```bash
        curl http://0.0.0.0:8181/test-metadata/scope/stream/test.txt -v
        ```

## Example Interception Logs
```
[S3Proxy-Jetty-147] INFO  i.n.streamlets.StreamletsExecutor - Building streamlet pipeline: isCachedStreamlet=false, streamPartitioningGranularity=scope/stream, streamlet=io.nexus.streamlets.functions.NoOpStreamlet2
[S3Proxy-Jetty-147] INFO  i.n.streamlets.utils.StreamletsCache - Loading new Streamlet io.nexus.streamlets.functions.NoOpStreamlet2.
[S3Proxy-Jetty-147] INFO  i.n.streamlets.StreamletsExecutor - Submitting processing pipeline task for stream partition StreamPartitionPojo{container='test-metadata', scope='scope', stream='stream', partition='test.txt'}.
[streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - Finished Streamlet NOOP2 operations. Processed Bytes: 10
[streamlet-threadpool-2] INFO  i.n.streamlets.StreamletsExecutor - Stored and processed 10 bytes from the request input stream.
[S3Proxy-Jetty-147] INFO  i.n.streamlets.StreamletsExecutor - Submitted streamlets for processing scope/stream/test.txt based on Policy{id='P1', system='system', scope='*', stream='*', pipeline=[StreamletExecutionDescriptor{streamlet=Streamlet{id='io.nexus.streamlets.functions.NoOpStreamlet2', executeOn ='ALL', type='NONE', partitionLocality=Yes', transformsContent=No', dataRouting=No}, region=EDGE, arguments=[]}], storage=[]}
[S3Proxy-Jetty-147] INFO  i.n.streamlets.cluster.ClusterRing - Key 'scope/stream/test.txt' hashes to 3904213556559896169 and maps to node 'W-2LF9K34:8181'
Hash Ring:
[812873489580285307, 3166414457833795738) → W-2LF9K34:8181
[3166414457833795738, 6072890917423799077) → W-2LF9K34:8181
[6072890917423799077, 7121466252492667343) → W-2LF9K34:8181
[7121466252492667343, 8432866502399309354) → W-2LF9K34:8181
[8432866502399309354, MAX] ∪ [0, 812873489580285307) → W-2LF9K34:8181

[S3Proxy-Jetty-147] INFO  i.n.streamlets.StreamletsInterceptor - This is the terminal pipeline Region (EDGE), storing data to storage.
```

## Troubleshooting
- **Common Issues**:
  - Ensure that Redis is running prior to Nexus's instantiation
  - Review Nexus's logs and initial configurations are correct
- **Metadata**
  - You can manually enter Nexus's metadata into Redis using `redis-cli` in case there are problems when using the CLI tool:
    ```bash
    docker exec -it redis-cli bash
    ```
    ```bash
    redis-cli -h redis -p 6379
    ```
    ```bash
    SET policy:P1 "{\"id\":\"P1\",\"system\":\"kafka\",\"scope\":\"scope\",\"stream\":\"stream\",\"pipeline\":[{\"streamlet\":{\"id\":\"noop-1\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":true},\"region\":\"EDGE\",\"arguments\":[\"\"]}],\"storage\":[\"\"]}" 
    ```
    ```bash
    SET streamletdescriptor:noop-1 "{\"id\":\"noop-1\",\"executeOn\":\"ALL\",\"hardware\":\"NONE\",\"partitionLocality\":true}"
    ```
    ```bash
    SET swarmletdescriptor:1 "{\"serviceEndpoint\":\"1\",\"region\":\"EDGE\",\"hardware\":\"NONE\"}"
    ```

