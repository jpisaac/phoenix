# Low Latency Core App Log Ingestion PoC

Authors : [Praveen Kumar Vuligonda](https://salesforce.quip.com/DHKAEAJfEiH), [Kadir Ozdemir](https://salesforce.quip.com/UNCAEAzPswn)
Date: May 05, 2022

This document explains the details and results of a PoC for a low latency log ingestion solution with Phoenix. The PoC is motivated by EM (Event Monitoring) use cases but applicable in general ingesting logs. EM use cases requires making a subset of core app logs available for Salesforce customers to query in seconds. This is not feasible with the current ingestion pipeline since the processing of logs takes about 10 minutes.

# Objectives

1. Demonstrate that by leveraging a real-time store like Phoenix we can reduce log ingestion processing time from 10 minutes to 10 seconds. Although 10 second processing time is lower than what EM use cases requires initially, this objective is to demonstrate that Phoenix can be a long term solution for EM like use cases.
2. Leverage existing technologies in production and and leverage and possibly simplify the current implementations in place to reduce the development cost and complexity. These include Kafka, Spark and Trino technologies and the Trino-Phoenix connector and UIP code.
3. Do not require significant improvements on the existing log ingestion pipeline such as requiring one topic per LRT and more efficient serialization of logs.
4. Demonstrate that the cost of adding log ingestion with Phoenix to meet the EM use case requirements will require a small fraction of the cost of existing log ingestion solution. 
5. Demonstrate that the solution with Phoenix can replace existing log ingestion pipeline and improve the ingestion pipeline cost while reducing ingestion latencies for all logs.
6. Demonstrate that the solution can be deployed locally at the region level or centrally.

# High Level Design

Phoenix can ingest high volumes of data with very high ingestion throughput even when the data is streamed into Phoenix row by row. By being a real-time data store, whenever a log record is ingested into Phoenix it becomes immediately queryable. Phoenix also uses compute resources efficiently. This makes Phoenix a viable candidate for a real-time store to be used as an ingestion front-end for log ingestion. Please see [the results](https://salesforce.quip.com/rrqGAFCyxNXN) of the performance study that analyzes the Phoenix ingestion and read throughput in isolation for LRTs.

In addition to staging log records in Phoenix, we would serve queries on most recent data from Phoenix as proposed in [Phoenix + Trino for Ingestion](https://salesforce.quip.com/LAwdAgdUgzrl). There are two main candidate architectures for this: Pipeline and Lambda. 

1. In the Pipeline architecture, Phoenix is used for serving real-time queries as well as generating columnar files stored in S3 by reading data from Phoenix views and transforming it to columnar files in S3
2. In the Lambda architecture, data is written to Phoenix cluster and S3 bucket using separate pipelines and Phoenix is used for serving real-time queries.


The rest of this document focuses on log ingestion into Phoenix, which is independent of the choice for the overall architecture, i.e., Pipeline or Lambda. The main tenets of the ingestion design are as follows:

1. The logs will be ingested using fine-granular micro-batches. The PoC uses 10 second micro-batches.
2. For the processing of logs, a light weight ETL process which does not include any expensive operations that require shuffling data is used. This means the data is not repartitioned in memory. The number of Spark partitions will be equal to the number of Kafka partitions (or the number of S3 files when the logs is needed to be read from S3).
3. The logs are written to the same HBase table. LRTs are logically separated at the Phoenix level using a separate updatable view for each LRT on top a single Phoenix table backed by the HBase table. 
4. The Phoenix table is salted with 256 salt buckets. This means that a batch of rows written to a view (i.e., a batch of log records for a given LRT) will be spread into these buckets. Each bucket corresponds to one or more HBase table region. Salting is used to eliminate hot spotting during writes and to increase parallelism during queries.
5. The rows are written to Phoenix using its JDBC driver one row at a time. We use connections with disabled auto commit (the default behavior). After writing 2560 rows, these rows are committed. This allows the Phoenix client to give a batch of 2560 rows to the HBase client which then groups these rows based on their table regions and sends them to the HBase region servers in parallel.
6. The thread context writing the rows to Phoenix is different than the thread context processing the rows within Spark executors. This is achieved by using a separate thread for Phoenix writes. We observed significant performance gain was achieved by decoupling the LRT processing from Phoenix writing since this allows reading from Kafka plus processing in Spark can happen in parallel with writing the processed log records to Phoenix within the same 10 second Spark micro-batch.
7. The primary key for the views starts with the org id and timestamp of the log records produced by the sources for these records. This allows efficient time range searching in Phoenix. It is expected that queries from Phoenix will be for the data that is ingested very recently such as in the last hour. This enables Phoenix to return the results for the queries even the complex ones with good latencies. For the performance testing of these queries, please see the [doc](https://salesforce.quip.com/TGEnARXbq0SY).

# Details

## **Stream Processing Design**

Spark Streaming is to read from Kafka, [others possibilities are Apache Pulsar, Apache Flink]. Spark Streaming has built-in support to stream from various sources, e.g., network sockets, files, Kafka, Kinesis, Flume. A continuous stream of data from these sources [represented as a [DStream]](https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html#discretized-streams-dstreams) and is divided into batches received in a predefined fixed time interval, called micro-batches. Each DStream internally is a continuous series of RDDs, one RDD per micro-batch. Thus, each RDD represents a certain amount data received within an interval of time, a micro-batch.

### Streaming Approach

Spark Streaming provides two approaches to consume from these sources:

* Direct Stream (provides built-in support for sources e.g. Kafka; and [integrates](https://spark.apache.org/docs/2.2.0/streaming-kafka-integration.html) with source provided libraries e.g. Kafka API)
* Receivers (provides framework that can be extended to consume from sources other than above)

We opted to use the Direct Stream approach for the following reasons:

* It has built-in support for Kafka as a source.
* It provides simple parallelism and 1:1 correspondence between Kafka partitions and Spark partitions.
    * e.g. if Kafka topic has 60 partitions, then each RDD in the input `DStream` contains 60 Spark Partitions
* It eases Offset Management with access to offsets and metadata [`HasOffsetRanges`].
* It provides ability to consume from beginning or latest or specific per-partition offsets.
    * This would be useful in production to recover from failures by replaying the logs from Kafka.
    * We leveraged this during the PoC to analyze and verify our results to make sure they are consistent across multiple runs.

### Kafka Offset Management

For the offset management for Kafka, we opted to manage consumer offsets using Kafka `AdminClient` API. The partition offsets for a Kafka consumer group are stored on a Table on Phoenix. The Topic Subscription info is initially pre-populated in this table such that a consumer group is mapped to a tuple of  topic, partition number and offset. The initial value for the offset is set to zero. To consume from the topic(s), we basically assign the above subscription info using the `Assign ConsumerStrategy.`

During the post processing of a micro-batch, the latest offsets are updated on the Phoenix table. This allows on-demand replay from any topic(s)/partition/offset using same or different consumer id, if required, by updating the table. It is worthwhile to note that this [integration](https://git.soma.salesforce.com/dva-transformation/sfdc-spark/tree/branch-3.1.1-sfdc/external/kafka-0-10) can be enhanced when needed e.g. to apply any LRT specific filtering or transformations directly on the data received when building the RDD within a micro-batch. This will eliminate a mapping step post building the RDD.

### Streaming BackPressure

For some reason, if processing a configured number of log records takes more than the time for the micro-batch, we can use the built-in support for back pressure. For the PoC, this back pressure capability was used initially to stabilize our runs and to identify an optimal number of records to ingest during a micro-batch.

### Spark Checkpointing

We have not used Spark Checkpointing(WAL) for this PoC. Since we are planning to use very small micro-batches, instead of recovering a failed batch partially, we can replay the entire micro-batch. This is because using the checkpointing slows down the application performance significantly.

### Spark Caching

Our PoC Spark application is deployed to our Spark-as-a-Service platform ([Salesforce Flowsnake](https://confluence.internal.salesforce.com/display/FLOWSNAKEDOC/Salesforce+Spark+Architecture)) that runs Spark on Kubernetes. We observed that caching RDDs in memory did not improve the overall throughput. In order to avoid additional memory overhead for caching, we did not use caching. We also did not repartition or coalesce RDDs as we did not need it and also to avoid expensive network shuffling operations in Spark. Since Phoenix/HBase supports random writes, there is no need to group records per-LRT to persist them in Phoenix/HBase. This greatly simplified the implementation.

## Read Path Design

To achieve low end-to-end ingestion latencies, an earlier PoC conducted read/write throughput tests on a 21 region server HBase cluster. The results showed 64+GB/min write throughput. To match the Phoenix/HBase write throughput, the Kafka READ throughput was scaled along with the Topic partitions and Brokers as follows:

* Per Broker READ quota was increased from default 4MB/sec to 100MB/sec
* Topic Partitions from 10 to 66 and Brokers from 6 to 33, for 2:1 Partition to Broker spread
* PoC achieved a 3GB/sec READ throughout 
* PoC uses the Spark Streaming configuration `spark.streaming.kafka.maxRatePerPartition` to manage the number of records read per partition per second

## Write Path Design

In this PoC, Apache Phoenix **** is chosen as an intermediate storage layer to persist Kafka events, i.e.,  log records. There are two main reasons for that :

* **Performance**: Apache Phoenix provides low latency random and strongly consistent single row writes. The single row write latency is about 3-4 ms. Writing time series data creates hot spotting if the primary key of the data table is not salted. This is the reason, we used 256 salted buckets for the data table. Phoenix internally prepends the HBase primary key of the table with a salt byte and compute one byte hash of the primary key and stores it in this byte. The rows for a given salt bucket are stored in a separate set of HBase table regions. This allows multiple table regions and thus region servers to be used during ingestion to scale write performance. In a similar fashion, the queries on Phoenix enjoys this parallelism which improves query performs especially aggregations which are typically done on the region servers.
* **Simplicity**: Phoenix provides JDBC API to both write and query data. This simplifies integration with Phoenix. There are also plugins, Phoenix connectors, for Trino and Spark. In this PoC, we did not need to use Spark plugin instead directly write to Phoenix using Phoenix client. 


In the PoC, log events are persisted to their LRT specific Phoenix Views which are essentially updatable logical tables which share the same physical table (i.e., HBase table). The primary row key is composed of OrganizationID, LRT, Event Timestamp, and UUID (for uniqueness). Using views as logical tables is a commonly used design pattern in Phoenix.

The PoC Application uses [HikariCP](https://github.com/brettwooldridge/HikariCP), a lightweight, high-performant connection pool to cache JDBC connections for frequent reuse.

Phoenix-Spark Plugin supports only Spark V2.4 and is not compatible with Spark V3+ version used on Flowsnake Spark, hence JDBC API is used for connection management as mentioned above. Although Phoenix has good single row write performance, the write performance can be further improved by batching rows. In thePoC, the Spark application submits Phoenix UPSERT statements in batches (size=2560) by disabling auto-commit and issuing a separate commit for every batch of rows. Phoenix internally buffers these writes and prepares corresponding HBase mutations. During commits, it passes these buffered batch of mutations to its HBase client. In turn, the HBase client groups these mutations based on their table regions and then submits these groups of mutations in parallel using its multi-threaded async framework. 

Each Spark partition consists of one or more such batches depending on the input records [e.g. 675840 records in a mico-batch interval of 10 seconds, 66 executors => 10240 records per partitions => 4 batches of 2560 each]. The PoC applications maintains a separate thread pool to submit these batches to Phoenix/HBase Client, which further groups the records into smaller sub-groups equal to the number of salts in the data table(=256), assigning a unique salt to each sub-group and persists them to the HBase storage.

# Environment Setup

|Spark/Flowsnake Cluster	|
|---	|
|	|EKS Cluster/
Namespace	|Instance
Type	|Instances	|vCPU	|Memory 
(gb)	|Network Bandwidth Gbps	|
|Spark/
Flowsnake	|fstrino1/
fs-trino	|r5.8xlarge	|10	|32	|256	|25	|
|	|	|	|	|	|	|	|
|	|	|	|	|	|	|	|
|HBase Cluster	|
|HBase	|dev-phoenix-hbase3a/
hbase	|m5.8xlarge	|21	|32	|128	|10	|
|HBase (Initial)	|dev-phoenix-hbase3a/
spkhbase	|m5.8xlarge	|9	|32	|128	|10	|
|	|	|	|	|	|	|	|
|	|	|	|	|	|	|	|
|Kafka Cluster	|
|	|Name	|Brokers	|Partitions	|Read Quota 
Per Broker	|
|Topic	|sfdc.dev.rsyslog__aws.dev1-uswest2.monitoring.ajnalocal1__logs.coreapp.rest	|33	|66	|100 MB/s	|
|Server	|[ajna-kafka.operations-logs.ajna.local.sfdc.net:9093](http://ajna-kafka.operations-logs.ajna.local.sfdc.net:9093/)	|
|	|	|	|	|	|	|	|
|	|	|	|	|	|	|	|
|Spark Streaming Application Configuration	|
|Streaming	|batch interval	|10s	|used initially to arrive at a batch size that completes within batch interval
	|
|backpressure	|spark.streaming.backpressure.enabled	|FALSE	|
|spark.streaming.backpressure.initialRate	|67584	|
|Kafka read rate	|spark.streaming.kafka.maxRatePerPartition	|1024	|Expected records per batch interval=maxRate * # of Kafka Partitions * Batch Interval
= 1024 * 66 * 10 = 675840	|
|Spark Executor memoryOverhead	|spark.executor.memoryOverhead	|2g	|To avoid k8s killing the executor pod(SystemOOM) as the default (=0.1 of executor.memory) was lower than 1g	|
|	|	|	|	|	|	|	|
|	|	|	|	|	|	|	|

## Data Set for Performance Tests

Test files from S3 bucket: 

```
s3://mirus-agg-nonprod-test1-uswest2-monitoring/topics/
```

Use expression to find the topics with in-scope LRTs [same expression used by Huron/UIP ingestion]

```
topics\/sfdc\.prod\.logbus__.+\.ajna_local__logs\.coreapp\..+\..+|sfdc\.(prod|stage|esvc)\.rsyslog__aws\..+\.(foundation|monitoring)\..+**logs\.casam\.sam|sfdc\.(dev|test)\.rsyslog__aws\..+\.(foundation|monitoring)\..+__logs\.casam\.sam**
```

List of Topics that match the above expression

```
~/work/presto/phoenix-setup/sfdc/em-poc/avro-data % **aws s3 ls s3://mirus-agg-nonprod-test1-uswest2-monitoring/topics/ |** **egrep "topics\/sfdc\.prod\.logbus**.+\.ajna_local__logs\.coreapp\..+\..+|sfdc\.(prod|stage|esvc)\.rsyslog__aws\..+\.(foundation|monitoring)\..+__logs\.casam\.sam|sfdc\.(dev|test)\.rsyslog__aws\..+\.(foundation|monitoring)\..+__logs\.casam\.sam" 
          PRE sfdc.dev.rsyslog__aws.aws-dev2-uswest2.foundation.ajnalocal1__logs.casam.sam/
          PRE sfdc.dev.rsyslog__aws.dev1-uswest2.foundation.ajnalocal1__logs.casam.sam/
          PRE sfdc.test.rsyslog__aws.perf1-useast2.foundation.ajnalocal1__logs.casam.sam/
          PRE sfdc.test.rsyslog__aws.test1-uswest2.foundation.ajnalocal1__logs.casam.sam/
```

From these topics, set of AVRO files were copied from below topic

```
sfdc.test.rsyslog__aws.test1-uswest2.monitoring.ajnalocal1__logs.casam.sam
```

From these input AVRO files, filter AILTN & AUGEN records which are both EM LRTs

Given AILTN has a higher per record size, this was chosen for the PoC tests

A 39M record data set was prepared and populated to Kafka
This data set is available on S3 at 
`s3://flowsnake-11086916379230561439/trino-phoenix-poc/perf-test/ailtn-avro-test-files`

For consistent numbers, the initial tests consisted of reading once from Kafka, caching and iterating the below steps multiple times [10]

* Extracting the raw LRT json from Kafka record [ConsumerRecord]
* Applying Ajna Schema and UDF
* Extracting the message and payload maps and writing to Phoenix

Later tests did not cache any RDD

# Results 

The following table summarizes the specific of the PoC ingestion runs. 

|LRT	|Log
Records	|Input
Data Size 
(in GB)	|Avg Raw Log
Record Size 
(in bytes)	|Cores per Executor	|Executors	|Records Per Micro-batch 	|RDD Partitions	|Log Records
Per
Partition	|Phoenix
Batch Size (in log records)	|Phoenix Batches 
Per Executor	|10s Micro-batches
Per Test Run	|
|---	|---	|---	|---	|---	|---	|---	|---	|---	|---	|---	|---	|
|AILTN	|38715584	|189GB	|5200	|1	|66	|675840	|66	|10240	|2560	|4	|50+	|

In each 10 second micro-batch, 675840 ALTN log records were ingested. These records were read from 66 Kafka partitions via 33 Kafka brokers. The resulting RDD in Spark included 66 partitions. This means each partition roughly included 10240 log records. We used 66 Spark executors each with one core. This implies that each executor was assigned to one RDD partition. Within an executor, 10240 logs records were processed and passed to its Phoenix client. After every 2560 log records, the commit call on the Phoenix connection used by this executor was issued. This results in roughly 4 Phoenix batches each with up to 2560 log records (or Phoenix rows).  Each performance runs executed 50+ micro batches. We observed that initial couple micro-batches took longer than average but then the remaining runs completed around average time without much fluctuation with in the micro-batch interval. For example, if the average completion time is 7.5 seconds, micro-batches completed in 7 or 8 seconds except a couple of initial micro-batches. It is important to note that we used the UIP code to process the LRTs in the same was UIP does and we generated roughly equivalent output (roughly equivalent number of uncompress output bytes per row). EM use cases needs only a subset of this output. For example, EM output for AILTN is about 300 bytes where we generated about 8KB output for each AILTN row to be stored in Phoenix.

The following table shows the results with different memory configurations. From this table, the optimal memory size for an executor is about 6GB (4GB for executor and 2GB overhead memory for the other services within the Kubernetes pod for that executor). With that, 10 second micro-batches completed in roughly 7.5 seconds.

|Memory per Executor including 2GB overhead (in GB)	|Avg completion time for 675840 records within 10s micro-batch (in seconds)	|
|---	|---	|
|4	|8.5	|
|6	|7.5	|
|10	|7.5	|

We repeated these runs with smaller Spark and Kafka cluster (10 Kafka partitions and 10 executors) and observed that the results scaled linearly. In other words, both configs processed the same number of log records per executor.

Now, let us compare the size of the data the PoC processed in 10 sec with the size of data processed in production today. Today, the total size of LRTs ingested by the current ingestion pipeline is about 150TB a day in the Avro format. This means on average there would be 1.77GB data to ingest in Avro.

When we computed the average Avro size of ALTN records we processed, we found that it was about 1100 bytes. This means that the PoC processed 675840 x 1100 = 0.69GB Avro  in 7.5 seconds, or 0.092GB in one second. This is about 1/19.23 or roughly 1/20th of the production load. This implies that we need to scale about 20 times our Kafka and Spark clusters and about 10 times the Phoenix cluster (as we used about half of the Phoenix cluster throughput in the PoC runs).  This means we need about 20 x 66 = 1320 cores for the Spark cluster, 1320 Kafka partitions and 210 node Phoenix cluster to ingest all LRTs. To put this in perspective, UIP today needs 32000 cores to process all LRTs and store them in the Parquet format in S3.  

# Conclusions

This POC has shown that using a row based horizontally scalable SQL store as a front-end architectural component (i.e., a real-time store) has several benefits: simplicity, scalability, real-time log ingestion, and cost reduction.  

The stream processing solution used in this PoC is much simpler than the existing implementation in terms of design and code complexity. The simplicity comes from the fact that the solution can work with one Kafka topic, one RDD to represent all LRTs in Spark, and one physical table to store the data in Phoenix. Although Spark was chosen to do stream processing in this PoC in order to leverage the parts of the existing UIP implementation, since the PoC does not need to use advanced Spark capabilities, it can be implemented using a Java program to read log records from a Kafka partition or S3 file, apply transformations at the row level and finally write the result to Phoenix at the row level.

This one-to-one mapping (one Kafka partition to one Spark partition per executor) allows horizontal scaling by adding more partitions to Kafka, single-core executors to Spark and nodes to Phoenix. This simplicity also results in high performance and efficiency. We observed that the PoC highly utilized compute and storage resources such that IO wait times were minimized by reading, processing and writing log records in parallel where the unit of parallelism was essentially a single row.

With this simplicity, the PoC can read, parse, schematize and persist logs in their specific views using only about 1/24th existing compute cluster for ingesting LRTs. Please note that the PoC was also designed to cover the UIP use case. For the specific EM use case, which only needs a small subset of these log records, the required compute cluster will be a fraction of what the UIP use case needs. Thus, (using the Lambda architecture) adding the EM specific cluster into the current log ingestion pipeline will be cost effective. 

This PoC also suggests that even the UIP use case can be improved using the Pipeline architecture mentioned earlier in the document. After persisting data in Phoenix, we can read the already processed data from Phoenix, generate the Parquet files and write them to S3. We expect the additional compute cluster to do this transform and load operation will be also relatively small since the data in Phoenix is already processed, schematized and sorted based on the primary key (org ID and event timestamps). Also Phoenix read performance is more than 3 times of its write performance which indicates that reading data from Phoenix should be viable without adding significantly more nodes.
