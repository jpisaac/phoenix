# Change Data Capture for Phoenix Stream

*Authors: [Viraj Jasani](https://salesforce.quip.com/caKAEATfZQW) [Kadir Ozdemir](https://salesforce.quip.com/UNCAEAzPswn)*

This high level document describes the Change Data Capture using Phoenix streaming concept.

Apache Phoenix provides Change Data Capture (CDC) with [PHOENIX-7001](https://issues.apache.org/jira/browse/PHOENIX-7001). The CDC design in Phoenix leverages the write-optimized Uncovered Index as well as Max Lookback features. The changes are captured in the time-ordered event of row level modifications with an option to provide pre-image and post-image with every modifications.


## Problem Statement:

Since the CDC uses uncovered index on PHOENIX_ROW_TIMESTAMP(), the CDC consumer needs to provide time duration for which the records are expected to be read from the CDC Index. In this approach, any event of HBase table region split does not have any impact on the consumer queries as the index regionserver performs the scans on multiple data table regions depending on how many regions have been involved with the table data modifications in the given time range. Therefore, the consumer does not have option to consume only the given table region (partition) specific change events. It is specifically important for the cloud native applications that consume Change Stream records to be able to identify how much compute unit (memory, CPU, IO etc) needs to be allocated according to the num of data table regions involved for the given time range. As the region size grows beyond a certain limit, HBase provides split policy framework for users to configure different split policies. The default split policy is based on region size growth. For instance, regions are not allowed to grow beyond 10 GB by default.

_**Note:**_ The design for the CDC Stream in Phoenix is inspired by DynamoDB and Google Spanner DB, both provide ability for consumers to allocate predictable compute units to process the change records at scale.


## Proposed Solution:

The solution requires new framework introducing the Streaming concepts for Phoenix CDC. The solution needs to provide one active stream for the given table on which the CDC is enabled by the client or consumer.

**Change Stream:**

Phoenix Stream captures a time-ordered sequence of row-level modifications in any table and stores this information in a log for up to TTL window (24 hour by default). Client applications can access this log and view the changes with an optional support of how the data appeared before and after the row is modified, in near-real time.

**Stream Partitions:**

Stream records are organized into groups, or partitions. Each partition acts as a container for multiple stream records and contains information required for accessing and iterating through these records. The stream records within a partition are removed automatically after the TTL window.


Create CDC Stream on Phoenix Table:

```
CREATE CDC_STREAM <cdc-stream-name> ON <table-name>
```


Phoenix CDC (without stream support) created Uncovered Index on the data table with index on PHOENIX_ROW_TIMESTAMP().

Here, Phoenix CDC Stream needs to create Uncovered Index on the data table with index on (PARTITION_ID(), PHOENIX_ROW_TIMESTAMP()).


* PARTITION_ID() → Introduce new Server side function to retrieve the encoded region name from RegionInfo object while performing the mutation as part of IndexRegionObserver#preBatchMutate() coprocessor hook. This is the partition id for the data table region. It will be 32 byte string.
* PHOENIX_ROW_TIMESTAMP() → Existing function to retrieve the row update timestamp from the empty column cell.


A partition of an index is a logical partition defined by PARTITION_ID(). An index region may have rows from one or more index partitions. Since PARTITION_ID() will be the index row key prefix, the rows of a given partition will be laid out on the the index table consecutively. When a data table region splits into two daughter regions, the parent region gets archived and no longer receives any mutations. The new daughter regions start receiving new mutations and hence the mutations are recorded in the order of their arrival for the new partition that aligns with new daughter regions.

 Each partition of the Index table refers to one partition for the given stream. Partitions can be categorized into two categories:


1. Open partitions: Any partition with corresponding data table region that is currently active is considered as open partition. The data table region can continue to server read/write requests until it is split into two daughter regions or multiple parent regions are merged into one region.
2. Closed partitions: Any partition with corresponding data table regions that is not longer alive and ready to be archived or already archived after getting split or merged into new region(s), is considered as closed partition. The data table region is no longer live and hence can no longer server any more read/write requests.


Both open and closed partitions can contain records if the corresponding live and archived regions received data modifications. Both open and closed partitions can be consumed such that the data are read from the Uncovered Index with optional pre-image and post-image modifications. The records will no longer be available after the TTL expiry defined on the CDC Stream Index table.

Partition records should be identified by a numerical timestamp value represented by PHOENIX_ROW_TIMESTAMP() that will increase with new mutations. The timestamp number can also be used to retrieve the records from the given partition from a specific position. This helps resume the scan operation by the consumer on the open partition when all records of the given partition were consumed previously.

It is also important to store parent → child relationship among the partitions as the regions get split and/or merged.

**Region split with Partition split**




## CDC Stream Metadata:

The metadata related to all open and closed partitions for the given stream need to be persisted in new system tables: SYSTEM.CDC_STREAM and SYSTEM.CDC_STREAM_STATUS.

**SYSTEM.CDC_STREAM Table Description:**


Composite Primary Key of the table consists of <TABLE_NAME, STREAM_NAME, PARTITION_ID>.

Each Partition also consists of Partition start and end timestamp that uniquely identifies the time range for the data modifications. When a partition is created, Partition start time is provided as the timestamp when the region is created.

Until the partition is closed, Partition end time stays null. When the partition is closed, the partition end time is provided as the timestamp when the region is closed while splitting or merging.

**SYSTEM.CDC_STREAM_STATUS Table Description:**



## PARTITION_ID() Function:

Tracking partitions based on data table regions requires a new server side function: PARTITION_ID().

The function needs to extract unique region name (provided by `RegionInfo#getRegionNameAsString()`) from the Tuple at the server side. For the function to be able to extract region name, the Put or Delete mutation needs to include the regionName as attribute. The attribute can be attached by IndexRegionObserver as part of preBatchMutate() coprocessor hook.

For instance, ValueGetter (Tuple implementation) like SimpleValueGetter takes Put mutation. Once IndexRegionObserver attaches the region name as a new Mutation attribute, PARTITION_ID() function can retrieve Put mutation from ValueGetter and retrieve region name.



## How the region split needs to trigger the partition split?

HBase uses ProcedureV2 framework for various region assignment workflows. The logic for assignment, server crash, split and merge handling has been recast as procedures (or Pv2, see [HBASE-13202](https://issues.apache.org/jira/browse/HBASE-13202) and its detailed [overview](https://issues.apache.org/jira/secure/attachment/12724813/ProcedureV2-overview.pdf)).

Active master daemon is always in charge of region split and merge operations. For the purpose of region split, once the split operation is successful, master invokes a master coprocessor hook:


```
  /**
   * Called after the region is split.
   * @param c           the environment to interact with the framework and master
   * @param regionInfoA the left daughter region
   * @param regionInfoB the right daughter region
   */
  default void postCompletedSplitRegionAction(final ObserverContext<MasterCoprocessorEnvironment> c,
    final RegionInfo regionInfoA, final RegionInfo regionInfoB) throws IOException {
  }
```

Phoenix can introduce a new MasterCoprocessor that implements the above action to update the partition metadata on SYSTEM.CDC_STREAM table. 

* Query SYSTEM.CDC_STREAM_STATUS with TABLE_NAME=regionInfoA/B.getTable() and  STREAM_STATUS=‘ACTIVE‘
* If a stream is found, then for every daughter region:
    * insert a row in SYSTEM.CDC_STREAM with the following data:
        * TABLE_NAME →  regionInfo.getTable()
        * STREAM_NAME → from previous query on SYSTEM.CDC_STREAM_STATUS
        * PARTITION_ID → regionInfo.getEncodedName()
        * PARENT_PARTITION_ID → look up in the table, need to reorder the columns so that this query is efficient
            * select PARTITION_ID from SYSTEM.CDC_STREAM where table_name=tableName and stream_name=streamName and partition_start_key= min(daughter1 start key, daughter2 start key)
        * PARTITION_START_TIME → regionInfo.getRegionId()
        * PARTITION_END_TIME → not needed, will be updated when daughters split in the future. 
        * PARTITION_START_KEY → regionInfo.getStartKey()
        * PARTITION_END_KEY → regionInfo.getEndKey()
* Update the end time for the parent region/partition
    * UPSERT INTO SYSTEM.CDC_STREAM (table_name, stream_name, partition_id, partition_end_time) VALUES (tableName, streamName, **parent_partition_id**, **daughterStartTime-1**)



## How consumers can consume the Change Stream?

For a consumer to start consuming the stream records, a series of steps need to be followed:

**Step 1:** Get the active table stream from the metadata.

`SELECT STREAM_NAME FROM SYSTEM.CDC_STREAM_STATUS WHERE TABLE_NAME = <table-name> AND STREAM_STATUS = 'ACTIVE'`

**Step 2:** Get the partitions for the given table from the metadata.

`SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME = <table-name> AND STREAM_NAME = <stream-name>`

**Step 3:** Consume the stream records from the given partition.

`SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM <cdc-stream-name> WHERE PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?`



### Demo

[Image: Phoenix-Stream-Demo.mov]

JIRA:


1. [PHOENIX-7425](https://issues.apache.org/jira/browse/PHOENIX-7425) Partitioned CDC Index for eliminating salting
    1. Create PARTITION_ID() function and use for CDC Stream





