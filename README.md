<h1 align="center">
Nexus: A Data Management Mesh for Tiered Data Streams
</h1>

## Description
Nexus is a data management mesh that transparently mediates storage operations between streaming systems tiering data to an external storage.
The main goal of Nexus is to provide a substrate for advanced data management on data streams during the storage tiering process.

The main building blocks of Nexus include: 

- **Streamlets**: Data management functions —e.g., processing, routing, caching— executed on a chunk of tiered data.
- **Policies**: Set of rules for the execution of a single streamlet or multiple ones composed in an execution pipeline. 
- **Swarmlets**: A swarmlet is set of identical Nexus worker instances for executing streamlets. 

The architecture of Nexus is depicted in the figure below. Nexus worker instances are deployed as swarmlets (step 1) based
on their infrastructure location (Edge, Cloud) and hardware resources (e.g., GPU, DPU). Administrators can then deploy
streamlets (step 2) by loading binaries and setting up their metadata descriptor that describes key aspects for their
execution (e.g., hardware requirements). With swarmlets and streamlets in place, an administrator defines policies to 
orchestrate the execution of streamlets on data streams (step 3). The metadata of streamlets, swarmlets, and policies
is stored in the metadata store.

Event streaming systems can be configured to offload chunks of tiered stream data against a swarmlet service endpoint. 
Nexus worker instances act as a proxy by implementing standard APIs (e.g., AWS S3, Azure) for transparently intercepting
storage operations from the event streaming system viewpoint (step 4). Moreover, worker instances take care of the
execution of streamlets (step 5). As visible in the figure, Nexus executes streamlet pipelines enforcing the streamlet
location specified by the policy. This is possible thanks to Nexus’ mesh-like data routing (step 6).

![image](https://github.com/user-attachments/assets/b47828f9-97b0-451e-8eb8-e3dc3249cb3e)

## Getting Started
Developers can run Nexus locally using the filesystem as the storage backend, and simulate the interception process.

A step-by-step guide can be [found here](/docs/filesystem/).

Full local test automation scripts can be also [found here](/nexus-tiered-stream-manager/src/test/fail_tests/).

## Deployment with Apache Kafka
Nexus can intercept and process Apache Kafka's tiered storage requests to object storage. 

By default, Kafka offloads to the created S3 bucket in this directory format:
```
/<bucket>/<topicName>-<topicId>/<partitionNumber>/<file>
```
There are three types of files offloaded by Kafka:
- `.log` 
- `.indexes`
- `.rsm-manifest`

Nexus reads and operates on **`.log`** files only, and does not tamper with any indexing logic that Kafka uses for offloading/retrieving the offloaded data. This ensures proper read/write operations from the producer/consumer perspective.

A full Nexus/Kafka deployment guide can be [found here](/docs/kafka/).

## Deployment with Apache Pulsar
Nexus can also operate on Apache Pulsar's tiered storage requests.
Although, unlike Kafka, Pulsar offloads data to tiered storage directly to the created bucket, with no directory. Offloaded file names will:

- `-ledger-<ledgerId>` 
- `-ledger-<ledgerId>-index`

Nexus reads and operates on `-ledger-<ledgerId>` files only, and does not interfere with any indexing that Pulsar needs for offloading/retrieving the offloaded data. 

A full step-by-step Nexus/Pulsar deployment guide can be [found here](/docs/pulsar/).

### Current Limitations

- As per Pulsar's current tiered storage implementation, there are no identifiers for the `-ledger` files. 
From Nexus's metadata POV, this implies a limitation where there should be only one policy for Pulsar streams, having its scope/stream set to `pulsar` .

## References
- [S3Proxy](https://github.com/gaul/s3proxy/tree/master?tab=readme-ov-file); Interceptor middleware
- [Apache JClouds](https://jclouds.apache.org/); Storage backend support
- [Redis](https://redis.io/); Metadata service
