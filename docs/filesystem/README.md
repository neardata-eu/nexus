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

## Application Deployment

1. **Input Nexus Metadata**
   - For simplicity, we can define one Policy with one Streamlet, and one Swarmlet
   - You can use `EntryPoint.java` CLI tool to enter the following metadata:
   - **Streamlet**
     - Name: `noop-1`
     - Execute on: `All requests`
     - Locality: `Yes`
     - Hardware: `NONE`
   - **Policy**
     - Name: `P1`
     - System: `system`
     - Scope: `scope` 
     - Stream: `stream`
     - StreamletID:
       - Name: `noop-1`
       - Region `EDGE`
       - No streamletArgs required
     - No storage values required 
   - **Swarmlet**
     - Endpoint: `1`
     - Region: `EDGE`
     - Hardware: `NONE`

2. **Set Up the Filesystem**
   - Create the directory:
        ```bash
        mkdir -p scope/stream
        ```
   - Create a test file:
        ```bash
        echo 123456789 > scope/stream/test.txt
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
        - Nexus's logs should be displaying that an interception has taken place, alongside the number of bytes being read
   - Similarly, you can GET the test file from the created bucket:
        ```bash
        curl http://0.0.0.0:8181/test-metadata/scope/stream/test.txt -v
        ```

## Example Interception Logs
```
10:20:42.012 [S3Proxy-Jetty-111] INFO  i.n.streamlets.StreamletsInterceptor - PUT request for test-metadata / scope/stream/test.txt.
StreamPartitionPojo{container='test-metadata', scope='test-metadata', stream='scope', partition='stream', object='test.txt'}
10:20:42.031 [S3Proxy-Jetty-111] INFO  i.n.streamlets.StreamletsExecutor - Creating durable log object.
10:20:42.032 [S3Proxy-Jetty-111] INFO  i.n.streamlets.StreamletsExecutor - Submitting processing pipeline task for stream partition StreamPartitionPojo{container='test-metadata', scope='test-metadata', stream='scope', partition='stream', object='test.txt'}.
10:20:42.034 [streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - PUT - Executing Streamlet: NOOP, as part of pipeline: [StreamletExecutionDescriptor{streamlet=Streamlet{id='noop-1', executeOn ='ALL', type='NONE', partitionLocality=Yes}, region=EDGE, arguments=[]}]
10:20:42.036 [S3Proxy-Jetty-111] INFO  i.n.streamlets.StreamletsExecutor - Submitted streamlets for processing test-metadata/scope/stream/test.txt based on Policy{id='P1', system='system', scope='scope', stream='stream', pipeline=[StreamletExecutionDescriptor{streamlet=Streamlet{id='noop-1', executeOn ='ALL', type='NONE', partitionLocality=Yes}, region=EDGE, arguments=[]}], storage=[]}
10:20:42.037 [streamlet-threadpool-2] INFO  i.n.streamlets.StreamletsExecutor - Stored and processed 18 bytes from the request input stream.
10:20:42.037 [streamlet-threadpool-1] INFO  i.n.streamlets.StreamletsInterceptor - Finished Streamlet NOOP operations. Processed Bytes: 18
10:20:42.038 [S3Proxy-Jetty-111] INFO  i.n.streamlets.StreamletsInterceptor - This is the terminal pipeline Region (EDGE), storing data to storage.
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

