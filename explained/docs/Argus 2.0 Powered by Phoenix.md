# Argus 2.0 Powered by Phoenix

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)[David Manning](mailto:david.manning@salesforce.com)[Viraj Jasani](mailto:vjasani@salesforce.com)  
Updated: Feb 22, 2025

The proposal aims to solve existing production issues and enhance architectural attributes such as availability, scalability, performance, and efficiency. It proposes a new architecture and design, built on Phoenix, where Phoenix evolves into a time series database and replaces OpenTSDB.

This proposal begins by defining the problem domain of a metric store and outlining a high-level reference design based on this problem domain. This reference design is technology-agnostic and demonstrates how hot, warm, and cold stores can work together to meet metric store requirements. We then identify current production issues and their root causes, followed by a detailed description of the Argus 2.0 proposal, including its data model, query and ingestion path, and how it maps to the reference design.

Next, we suggest deploying a version of Argus 2.0 without changing the existing data or schema. This approach allows us to gain experience with running Phoenix for metric store use cases before deploying the full Argus 2.0, thereby reducing risk. Then we outline a smooth transition to the full Argus 2.0 version.

Lastly, we compare Argus 2.0 to other time series databases and present our conclusion.

# Metric Store

This section will analyze the problem domain for a metric store, extract the requirements from the analysis, and present a reference architecture and high-level design to meet the requirements.

## Problem Domain

### Multi dimensional immutable data

A metric, for example, “cpu-utilization”, can be emitted from different types of hosts. These hosts can be kubernetes pods, virtual machines, or physical servers, can be stand-alone servers, or members of clusters, and can be deployed in first party data centers, or public clouds. All these attributes and more can be attached as tags (key-values) to each metric data point. In this example, the CPU utilization metric has a possibly four byte metric value and hundreds of bytes of tag information. Each tag key is essentially a metric dimension. In addition to these tags, the metric has a unique name and timestamp. Thus, **a metric store should be able to efficiently query multi-dimensional data.**

### Continuous and high volume of data ingestion

As in the above example, the CPU utilization metric can be emitted every second from a source. Considering that there can be tens of thousands of sources emitting CPU utilization metrics and possibly thousands of other metrics, it is clear that **a metric store has to be able to ingest high volume and constant streams of metric data**.

### Computationally intensive range queries

Metric data is visualized as times series. A time series is identified by a metric and its tags. The queries operate on time series for a given time range. For example, a query may be for finding the average cpu utilization for the last hour. This query requires scanning all time series for this metric for the last hour. Typically queries include tags to narrow down the set of time series to be scanned as finding cpu utilization on all hosts used for different purposes may not be of much use. 

The queries on metric stores are used for dashboarding, alerting and analysis. In almost all cases, the query includes computations on time series for operations such as aggregation, grouping, down sampling, etc. Thus, **a metric store should perform computationally intensive range queries fast and efficiently**.

### Mostly-append write pattern

Metric data is mostly immutable and usually results in a mostly-append write pattern for a given time series. This is because new data points for a given time series typically have newer timestamps. Although data points may arrive out of order due to issues in the metric data ingestion pipeline, this will be rare.

When the physical layout of the data follows the logical layout, this mostly-append write pattern can create write hotspotting on the backend of the metric store. **It is expected that the metric store will be able to ingest the mostly-append write pattern without having availability or performance issues**.

### Near-real-time ingestion and query

We need the ability to generate near real-time alerts from metrics to reduce time to detect issues. We also need the ability to analyze metrics in near-real time to diagnose, troubleshoot, and monitor issues interactively or via dashboarding. Thus, **a metric store needs to ingest metric data and make them queryable in near-realtime**. 

### Metric compaction

As mentioned previously, the dimension information (tags) of a single metric data point can be hundreds of bytes, while the metric value itself may only be 4 bytes. These tags are repeated for every metric point in a given time series. If each metric data point is stored with its tags, over 90% of disk space will be taken up by redundant data. **A metric store should be able to compact the metric data not only for the on-disk but also for the in-memory representation of metric data points to efficiently use resources.**

### Metric rollup

The value of high-resolution (second or minute level) metric data diminishes over time. High-resolution metric data, such as second or minute level data, is important for recent data but loses value as it ages. Therefore, it is beneficial to roll up older data to lower resolutions, such as from seconds to minutes and from minutes to hours. Therefore, **it is desirable to roll up older, second-level data points to the minute level, and minute-level data points to the hour level as the data ages and loses its value.**

## Solution Domain

The following diagram depicts a high level reference design of a metric store solution based on the analysis of the problem domain.

### Streaming Service

It is required to ship the metric data from many sources to a metric store timely and efficiently. A well-known proven approach is to use a near real-time streaming service. For example, Kafka based streaming is typically used for this purpose.

### Buffering In Memory

The sources emit their metrics near real-time too. This means that the data points for a given time series will likely arrive at a metric store individually not in batches. Translating individual metric points into transactional updates on the metric store will not scale and lead to inefficient use of network, compute and I/O resources.

To improve ingestion efficiency and scalability, metric data points that are received from the streaming service should be buffered briefly. Even buffering for a second will lead to a good size of batching and thus will significantly improve ingestion throughput.

### Hot Store \- Memory Database

It is required to access the most recent metric data in near-real time for many use cases including alerting and dashboarding. Accessing data directly from memory is the fastest and most efficient method. Thus caching the recent metric data in memory is the desired solution to meet latency and efficiency requirements. 

### Logging

Metric data must be stored in memory to enable a hot store (a memory database) and to index it for quick retrieval of specific time series data. However, data stored in memory is volatile and can be lost due to server crashes or power outages. To address this, data is persisted to disk in real-time using a Write Ahead Log (WAL), a common technique for an efficient data storage solution.

### Indexing

As mentioned above, the data on disk needs to be indexed in order to reduce the query latencies. In addition to the primary key indexing based on the schema of the metric data, additional indexes can be created to speed up the queries.

### Warm Store \- Disk Database

It is not efficient and feasible to hold large amounts of data in memory. However, the throughput and latency of the queries for troubleshooting and extracting insights, that is data analytics requires metric data to be stored on a low latency disk database, that is, a warm store. 

### Cold Store \- Data Lake

It is desirable to maintain historical metric data. Historical data is accessed less frequently and the latency of the queries on the historical data can be much longer than that of the queries on the recent data on the warm store. In order to reduce the cost of historical data, it is typically stored in a cold store optimized for capacity. 

### Push Down Query Processing

The queries on metric data tend to scan data for a given time range and are computationally expensive due to aggregation, grouping and down sampling on multiple time series.  The data to be scanned can be across multiple stores, hot, warm and possibly cold stores. Pushing these queries to nodes near the storage layer instead of implementing the compute and data intensive queries on the client side saves network and compute resources.

### Compaction

Multiple metric data points can be merged into a multi-data point structure to remove duplicate tags as the time series is formed. This compaction results in less compute to process metric data during queries and reduces both in-memory and on-disk footprint. 

### Rollup

As the metric data points of a time series ages, they can be rolled up to a lower resolution. This improves storage footprint and query performance on historical metric data.

# Current Challenges

In this section, we present the current Argus design in production and its challenges.

The metric data is currently stored in Argus HBase clusters and Huron. HBase clusters are built on EBS volumes and Huron on S3. These are independent systems. In the rest of this section we will focus on Argus-HBase clusters.

The Argus data model is schemaless. Tags that are attached to metric data points are a collection of key-values. Both keys and values are represented by 6 byte UIDs internally. The forward and backward mapping between UIDs and name literals are stored in a metadata table referred to as the UID table. Each OpenTSDB node maintains its cache of the UID table in memory. On TSDB shutdown, it may persist the UID cache to S3. On TSDB startup, it may load the UID cache from S3. This warms the cache more quickly and with less load on the UID table.

Each row in the metric table includes multiple columns. Each column holds a metric data point. The column name encodes the delta timestamp from the timestamp component of the row key.  Each row holds an hour worth of metric data. The number of data points and column names can change from one row to the next.

The current design of Argus is illustrated below.

We make the following observations on the current design:

* It has a limited memory database as the amount of recently written data held in memory is typically for minutes, mostly less than an hour.  
* All the query processing is done on the client side in Argus WS (Web Service).  
* Indexing is mainly done on metric name, and timestamp, and also on scope tags if they are pushed into the metric name.  
* To expand query wildcards (\* and ?) to matching named literals, Redis and ElasticSearch are used.   
* OpenTSBD does not use the open source HBase client, instead it uses its own custom client.  
* No time series compaction or rollup is performed other than the HBase compaction on HFiles.

In the rest of this section we will describe the current challenges in Argus.

## High Cardinality \+ Schemaless Data Model → Query Performance Problems and Read Hot Spotting

Cardinality of a column in a database is the number of unique values the column can have. In the context of a time series database, the cardinality of a metric tag matters and high cardinality may impact the query performance. In the Argus data model, tags are not represented as individual columns but as entries of a single map column. For a map column, the cardinality of the column is the multiplication of cardinalities of all tags. This means in the Argus data model each row includes around 15-20 tag values, the cardinality of this map column easily exceeds 100K. 

A map column in Argus is an array of key-values serialized within the row key of the metric table. The Argus data model is currently schemaless and the set of key-values included in a metric data point is not predefined. This prevents constructing range queries using tag values and results in scanning all rows within the time range of the query.

A time series is identified by a metric and its tags. Thus a metric can have N time series where N is the cardinality of the map column including tags for the metric. If the cardinality of the tags for a given metric is 700K then the metric has 700K time series. Since the range queries cannot be constructed using tags, any query in Argus for this metric has to scan all these 700K time series even when the query returns one time series. This leads to major query performance problems and hot spotting.

## Client Only Query Processing → Inefficient Use of Resources

OpenTSDB does not have any server component running at the storage layer and thus all aggregations, grouping and down sampling happens on the client side. This means that large amounts of data have to be transferred from storage servers to OpenTSDB clients. 

OpenTSDB leverages only HBase filters as server side processing. However, these filters are for filtering rows, that is, time series. Since a given row includes one hour worth of data points, queries for shorter time ranges, or time ranges that do not align on hour boundaries have to return more data points than necessary.

These data transfers cause inefficient use of compute and network resources which can be mostly avoided by pushing down query processing to the server side.

## Heavily Encoded Data → Excessive Compute Usage

The on-disk representation of metric names and tag key and value names are all encoded using UIDs. These UIDs are internal to OpenTSDB. Metric data points ingested and the queries served by OpenTSDB are expressed by explicit names. The forward and backward mapping between UIDs and names are stored in a metadata table called tsdb-uid. 

While this encoding reduces the in-memory representation of the data points, it requires caching the forward and backward mappings in-memory in order to prevent extra lookups during write and read operations. During the processing of a query, it is required to identify all tag values that match the wildcards included in queries. The enumeration of the tag key’s values are necessary since these wildcards cannot be directly evaluated on UIDs. The wildcards are first expanded into a list of names in Argus WS and these names are converted to UIDs in OpenTSDB and included in the HBase filters. All these UIDs are then compared to the UIDs included in the scanned rows.

A simple regular expression which could be checked on a name quickly and efficiently while the  number of entries in the corresponding list of literal names and thus the list of UIDs may be in hundreds. Comparing each UID on a row key with UIDs in such a list obviously can be orders of magnitude more costly computationally. 

In addition to the computational cost of filters with many UIDs on the HBase region server side, the Argus client maintains an ElasticSearch cluster and a Redis cluster to quickly identify the names matching a query. Even this matching is not done optimally. For example, the number of unique k8 pods names (i.e., the number of k8 pods) changes from one k8 cluster to another. If a query is for an HBase cluster with 20 regionserver pods includes the expression “region\*” for the pod name, this expression is expanded to a list of 400+ pod names instead of 20 (regionserver-1, regionserver-2, and etc.)  when the largest HBase cluster has 400+ region server pods.

## No Salting → Write Hotspotting

The Argus metric table has the row prefix of metric uid and timestamp. This means that the metric data points at a given time for a given metric is written in a single table region. This naturally creates hotspotting during ingestion if this metric is a high-volume metric. OpenTSDB supports salting but it is not currently used in production. A 16-bucket salt was tried once during the migration from 1P to Falcon in 2021 ([Salting in TSDB](https://salesforce.quip.com/wsaTA3y2DGXe)), but it resulted in undesirable load characteristics ([doc for W-9352470](https://salesforce.quip.com/4xpeAqiDGzo5).) It is very likely that the salting resulted in increased load of Scan RPCs and BlockCache, which was not properly anticipated or provisioned or discussed. The concept has not been revisited since then.

## Outdated OpenTSDB Software → Excessive Metadata Lookups

The OpenTSDB community is not very active anymore. The last version (2.4.1) was released in September 2021\. In addition to some known unresolved bugs, OpenTSDB uses a custom outdated HBase client that does not work well with the recent HBase server versions. This creates a storm of metadata lookups on HBase and causes availability and performance issues.

## Suboptimal HBase Load Balancing and Recovery Operations → Availability Issues

Not all issues in production are because of OpenTSDB or the Argus data model. There are issues in HBase that need to be addressed. These issues impact the availability and performance of the cluster.

Region transitions and server recovery operations sometimes take many minutes or require manual intervention. They should ideally be completed automatically in seconds. Even for normal operations, more effort could be made to reduce the duration of region transitions, and the total number of region transitions.

The date-tiered compaction model for HFiles could be revisited. Do the benefits of date-tiering store files truly outweigh the cost of maintaining more store files? This is especially questionable given we do not push down the time predicate to be used in pruning the store files ([W-9480225](https://gus.lightning.force.com/lightning/r/ADM_Work__c/a07AH000000Orx8YAC/view) tsd.storage.use\_otsdb\_timestamp=true.) We need to at least read an index of every store file. Managing more store files can impact transition times, as well as impact read performance.

# Argus 2.0

In this section, we will present a next generation architecture and design (Argus 2.0) for a metric store that meets the requirements described in the Problem Domain section and solves the current production issues except some known suboptimal load balancing and recovery behavior in HBase. These will be fixed through the regular HBase issue resolution and patching process as part of the first Argus 2.0 deployment phase as explained later in the document.

## High Level Design

The following diagram shows the architecture and high level design of Argus 2.0. The diagram also shows how this design maps to the reference design directly derived from the problem domain and presented in the Solution Domain section. The dotted lines and boxes are to show this mapping.  

### New Phoenix Features for Time Series Data

This design optimized for supporting time series has some additional features that are not currently implemented by the Phoenix/HBase deployments for core in production. These additional features will be described in this section.

#### Time Series Data Compaction

The Phoenix server will compact time series rows such that many data points of a row are packed in a single HBase cell to have a compact representation of time series both in-memory and on-disk. This compact representation leads to efficient use of memory, compute and I/O resources by about N times where N is the number of data points per hour (assuming that each row in HBase will hold data points for an hour). Typically N is about 30\. This will be further explained later in the next section. 

The Phoenix compaction currently runs during HBase memstore flushes, minor and major compaction.The time series compaction will be a straightforward extension of the current Phoenix compaction feature. 

#### Time Series Data Rollup

The time series data rollup feature will be down sampling during Phoenix compaction. Thus it will be another extension of the Phoenix compaction feature and rollups will be done during major compaction.

#### Time Series Optimized Physical Schema

Phoenix will have separate schemas for logical and physical representation of time series data. The logical schema is optimized for a SQL query engine while the physical schema is optimized for a key-value store, that is, HBase in Argus 2.0. The Phoenix server will have the functionality to do the translation between these two different schemas. The proof of concept for this dual schema approach has been completed as part of the [Accessing Argus Data Using Phoenix](https://docs.google.com/document/d/1291G0ClYZrHUZVKMvEf0XmIS6b9GLKTzFZohs-Udt5A/edit?tab=t.0) proposal. Argus 2.0 is an extension of that proposal.

#### Improved Salting

The new improved salting feature will be different from the OpenTSDB or the current Phoenix salting such that the salting function will be based on a user-specified list of columns to be included in the salt computation. For example, salting will not include the timestamp column in computing the salt bucket. This will ensure a given time series will always go to the same salt bucket. Also we may want to include only metric name, falcon instance, functional domain, and cluster in the salting function for falcon metrics. This further makes sure all time series for a source cluster’s metric go to the same salt bucket.

By specifying the list of columns to be included in the salt computation, we will reduce the unwanted side effects of salting such as excessive fan out during writes and queries but still eliminate hot spotting. An improvement Phoenix [Jira](https://issues.apache.org/jira/browse/PHOENIX-4757) was created in the past for exactly this salting improvement, and this Jira will be implemented here.

#### Write Caching

We will enable persistent Bucket Cache and CACHE\_ON\_WRITE in HBase which will allow us to implement a more scalable memory database by storing most recent writes on Memstore and Bucket Cache. With Bucket Cache, Block Cache is split into two sections, on-heap and out of heap. The out-of-heap cache is implemented by Bucket Cache. Bucket Cache can be offheap (direct memory access), local file, memory-mapped file, and persistent memory. For example, we can configure Bucket Cache on NVMe SSD using the local file option. Combining Memstore with Bucket Cache leads to a scalable Memory Database (Hot Store). 

## Data Model and On-disk Format

The data model for Argus 2.0 will take advantage of updatable views. Updatable views allows a table to be partitioned into multiple virtual tables (updatable views). Updatable views are hierarchical such that they can extend their parent schema with additional primary key (PK) columns as well as regular columns. This hierarchical representation drastically reduces the number of schemas to be defined for metrics. A possible schema hierarchy for Salesforce metric store can be depicted as follows.

The above schema can express the tags as predefined PK columns and as well as dynamic key-value pairs when needed. Having important tags defined as predefined (i.e., static) PK columns is the ultimate tool to tackle high cardinality issues by allowing the Phoenix query optimizer to form range queries using tags. At the same time, the above schema allows users to add dynamic tags on the fly to create flexible schemas.

Another powerful concept this design introduces is decoupling between the logical and physical schema so that the logical schema is designed for expressing metric store queries using SQL and leveraging its query optimizer while the physical schema is designed for a multidimensional time series data model.

In the logical schema, every data point has its own row and a set of rows with the same tags forms a time series. Since each data metric point has a single value, this will translate a row with a single cell where the row key of the cells encodes all the PK columns and the value of the cell holds metric value. However, if we use the same representation in-memory or on-disk, as explained previously, we store lots of redundant data as all the data points will have the same tags for a given time series. 

To address the redundancy problem in the logical representation, we introduce a physical representation and transformation method between them. In the physical representation, a row can hold many data points as opposed to the logical representation which holds a single data point. In the physical representation, a row will hold hourly data such that all data points for a given hour will be stored on the same row. This is similar to the OpenTSDB format except that tag keys or values will not be encoded using UIDs. The following diagram illustrates the mapping between logical and physical schema.

With the physical layout, a single row will include multiple cells, one for each data point of the rows. This does not directly reduce in-memory or on-disk footprint however greatly improves the filtering on the tags as this filtering will be done once for each row. This is shown in the figure below

The compression on this physical representation in-memory and on-disk will come from the time series compaction functionality that will be added to Phoenix. This compaction is supported in OpenTSDB but it is done on the client side and thus requires reading written data back periodically and then writing back in the compacted format. In Argus 2.0, this time series compaction will be done as part of the Phoenix compaction which is executed during HBase memstore flushes, minor and major compactions on the server side. 

Similarly, rollups will be executed during major compactions. 

## Transition to Argus 2.0

Argus 2.0 brings several architectural and design changes and new capabilities when compared to Argus 1.0. In order to transition to Argus 2.0 with less risk and to gain some experience in running Phoenix as a time series database, we propose an incremental deployment in two phases.

### Phoenix To Read and OpenTSDB To Write

In this phase, we do not make any changes on the existing Argus table schemas or disk layout. We let OpenTSDB ingest data and let Phoenix be the query engine. It also allows Huron to access the Argus data over a Trino-Phoenix connector. The details of this approach is explained in [Accessing Argus Data Using Phoenix](https://docs.google.com/document/d/1291G0ClYZrHUZVKMvEf0XmIS6b9GLKTzFZohs-Udt5A/edit?tab=t.0). This high level design for this phase is illustrated below.

The design is a subset of the Argus 2.0 design such that it does not have the schematized data model. When compared to the current solution in production, this design has the following benefits:

* The Phoenix server eliminates the need for Redis and ElasticSearch by directly checking if a regular expression matches a tag from the row key during the row scan. This removes the need to convert wildcards into a list of name literals. More details on this design can be found in [Accessing Argus Data Using Phoenix](https://docs.google.com/document/d/1291G0ClYZrHUZVKMvEf0XmIS6b9GLKTzFZohs-Udt5A/edit?tab=t.0).  
* Huron can query Argus data efficiently as described [here](https://docs.google.com/document/d/1291G0ClYZrHUZVKMvEf0XmIS6b9GLKTzFZohs-Udt5A/edit?tab=t.0#heading=h.exs9568q5b4).  
* Pushing down the queries to the server side reduces the compute and network usage.  
* OpenTSDB compactions merge multiple columns within an HBase row into a single column. This compaction reduces the disk space and as well as the block cache footprint. Implementing this compaction on the server side as part of memstore flushes and minor and major compaction improves resource utilization, including CPU, memory, disk space and I/O.

* The Argus WS expands regular expressions in queries into numerous literal names using ElasticSearch. These are then converted into UIDs on the client-side, and large HBase filters are constructed from these UIDs. This leads to high CPU usage on region servers due to these large filters. By eliminating these steps in this phase, query efficiency and performance are expected to improve significantly.  
* Fine-grain [server paging](https://docs.google.com/document/d/1Vt28i9JLQPG3lAnbW3RcO7fEUG4KT5AhyydLMiau59k/edit?tab=t.0#heading=h.ihnqsrqr86zp) improves latency and availability by breaking operations into time-bound slices, preventing any single query from holding system resources for extended periods. This end-to-end query pacing eliminates timeouts, improves time-sharing among queries, and increases overall system availability.  
* Write caching is enabled in HBase to improve latency for queries on recently ingested data. This improvement primarily benefits queries for alerting on critical metrics.  
* [The asynchronous client in OpenTSDB is replaced with the actively maintained asynchronous HBase client that ships with the HBase distribution](https://salesforce.quip.com/2CNPABa40195).   
* [Fixes for HBase load balancing and recovery related issues](https://salesforce.quip.com/eLgIAa30QaJH).

Please note that rollups can be done during major compaction if needed. The above improvements, especially server side query processing, efficient filtering, and compaction will definitely alleviate the high cardinality issues.

### Phoenix To Replace OpenTSDB

In this Phase, we start ingesting data using Phoenix and the schematized data model of Argus 2.0. The newly ingested data will be written to a new set of tables. The queries will be federated over old and new tables until the old tables are removed. The compaction and rollup on time series data will be supported on the new on-disk format, and salting will be enabled in this phase. This phase will have the following additional benefits on top of the benefits of Phase 1\.

* A schematized data model using primary key indexing (and secondary key indexing if needed) eliminates high cardinality issues through range queries over tags.  
* A new and improved salting feature eliminates read and write hotspotting. The salting function will be based on a user-specified list of columns to be included in the salt computation. This reduces the unwanted side effects of salting, such as excessive fan out during writes and queries, while still eliminating hot spotting.  
* The UID encoding will no longer be used. This eliminates the UID table and the need for additional lookups when mapping between UID and literal names.  
* The rollup feature improves storage and memory footprint for time series.

# Comparison of Time Series Databases

The purpose of this section is to show that modern time series databases come with a structured data model and SQL query language. They all implement the standard features that are identified in the solution domain section of this document. From the scalability, feature set and data model perspective, Argus 2.0 is comparable with them. 

|  | Data Model | Query  Language | Features | Notes |
| :---- | :---- | :---- | :---- | :---- |
| [Prometheus](https://prometheus.io/) | Schemaless: Key-value pair tags  | PromQL | Alerting Standard features  | Limited scalability, single node. [Low](https://valyala.medium.com/prometheus-storage-technical-terms-for-humans-4ab4de6c3d48) cardinality. |
| [TimescaleDB](https://www.timescale.com/?utm_source=google&utm_medium=cpc&utm_campaign=sitelink&utm_term=time+series+database) | Structured | SQL | Standard features |  |
| [Druid](https://druid.apache.org/docs/latest/design/) | Structured | SQL | Standard features |  |
| [Riak](https://en.wikipedia.org/wiki/Riak) | Structured | SQL | Standard features |  |
| Argus 2.0 | Structured | SQL | Standard features  | Homegrown. Smooth transition. Optimized for Salesforce.   |

# Conclusion

We have shown that by adding a few time series database features, such as a time series optimized data format, compaction and rollup, and improved salting, Phoenix can evolve into a modern time series database. This can be achieved without changing the Phoenix architecture or design. This modern time series solution will meet current challenges and be comparable to, if not better than, existing time series databases.

Furthermore, we have demonstrated the ability to enhance the current solution incrementally and ensure a seamless transition to Argus 2.0.  Evolving the existing technology, instead of introducing a new one, offers multiple benefits. These include utilizing the existing infrastructure, operational tools, knowledge base, and talent. Additionally, building Argus 2.0 on Phoenix will allow the Phoenix team to innovate and contribute to the Argus service, enabling a new set of skills to benefit the Argus service.

