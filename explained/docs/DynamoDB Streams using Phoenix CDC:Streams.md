# DynamoDB Streams using Phoenix CDC/Streams

>This document describes how to implement DynamoDB Streams 
abstractions and APIs in [phoenix-shim](https://git.soma.salesforce.com/bigdata-packaging/phoenix-shim) using [Phoenix CDC](https://issues.apache.org/jira/browse/PHOENIX-7001) and [Phoenix Streams](https://issues.apache.org/jira/browse/PHOENIX-7456). 



## Background

>If you are familiar with both DynamoDB Streams, Phoenix CDC and Streams, jump to [DynamoDB Phoenix-Shim Change Stream Implementation](https://salesforce.quip.com/vunDA0Fwedt5#temp:C:LNT4487efbb522447c8b1cfd3943). 

### DynamoDB Streams

DynamoDB Streams captures a time-ordered sequence of item-level modifications in any DynamoDB table and stores this information in a log for up to **24 hours**. Applications can access this log and view the data items as they appeared before and after they were modified, in near-real time.

A DynamoDB stream is an ordered flow of information about changes to items in a DynamoDB table. When you enable a stream on a table, DynamoDB captures information about every modification to data items in the table.

Whenever an application creates, updates, or deletes items in the table, DynamoDB Streams writes a stream record with the primary key attributes (partition key or partition key with sort key) of the items that were modified. A stream record contains information about a data modification to a single item in a DynamoDB table. You can configure the stream so that the stream records capture additional information, such as the "before" and "after" images of modified items.


#### Enabling a stream

Streams can be enabled on a DynamoDB table either at the time of creation using CreateTable API or after the table has been created using UpdateTable API. 

* StreamEnabled → set to true
* StreamViewType → information written to the stream
    * KEYS_ONLY — Only the key attributes of the modified item.
    * NEW_IMAGE — The entire item, as it appears after it was modified.
    * OLD_IMAGE — The entire item, as it appeared before it was modified.
    * NEW_AND_OLD_IMAGES — Both the new and the old images of the item.

#### Reading and processing a stream

* A ***stream*** consists of ***stream records***, which represent a single data modification. 
* Each ***stream record*** is assigned a ***sequence number***, reflecting the order in which the record was published
* ***Stream records*** are organized into groups, or ***shards***. Each shard acts as a container for multiple stream records.
    * Shards have number of interesting properties that allow distributed exactly once processing of change records. Here are some highlights:
        * Shards are ephemeral, a shard is "open" for ~4 hours, "closed" for ~24 hours, then disappears
        * Db table has multiple open shards at any given time. Number of open shards matches number of ddb table partitions and is a function of data volume and highest observed request rate
        * Shards can split, but cannot merge, which means number of shards for any db table can only increase over time
        * When shard closes, one or more of its children shards open
        * Each shard behaves like append-only log files, new records can only be added to the end of the log
        * In each shard, change record sequence numbers are ***unique***, increasing and between 21 and 40 digits
        * (according to AWS support, change record sequence numbers are also unique and increasing for any given primary key within a db table, but this is not documented publicly and ZOS does not rely on this)
        * Change records for the same primary key go to the same open shard. When the shard closes, go to one of the child shards.
        * With high scale, typically ~500 shards can be open at any given time. As per AWS support, it can go up to  ~10k as well.
* High Level Access Pattern
    * determine the unique stream on the table
    * determine the shard in the stream which contains the required records
    * access the shard and retrieve the records
    * See [Understanding DynamoDB Change Stream at Scale](https://salesforce.quip.com/2sQnA4UyH7Du)




#### DynamoDB Stream APIs

* [ListStreams](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_ListStreams.html) — Returns a list of stream descriptors for the current account and endpoint. You can optionally request just the stream descriptors for a particular table name.
* [DescribeStream](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_DescribeStream.html) -- Returns detailed information about a given stream. The output includes a list of shards associated with the stream, including the shard IDs.
* [GetShardIterator](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html)— Returns a *shard iterator*, which describes a location within a shard. You can request that the iterator provide access to the oldest point, the newest point, or a particular point in the stream.
* [GetRecords](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html) — Returns the stream records from within a given shard. You must provide the shard iterator returned from a GetShardIterator request.

### Phoenix CDC

[PHOENIX-7001](https://issues.apache.org/jira/browse/PHOENIX-7001) implements Change Data Capture to retrieve changes to tables or updatable views in near real-time. It leverages Phoenix Max Lookback and Uncovered Global Indexes. The max lookback feature retains recent changes to a table, that is, the changes that have been done in the last x days typically. An uncovered index based on timestamp helps in delivering changes in order of their arrival. 


#### Enabling Phoenix CDC

```
CREATE CDC <CDC Table Name> on <Data Table Name> 
INCLUDE (pre | post) SALT_BUCKETS=<salt bucket count>
```

* The above CDC DDL creates a virtual CDC table and an uncovered index.
* The CDC table PK columns start with the timestamp and continue with the data table PK columns.
* The CDC table includes one non-PK column which is a JSON column representing the data modification

#### Accessing Changes using Phoenix CDC

```
SELECT * FROM <CDC Table Name> 
WHERE PHOENIX_ROW_TIMESTAMP() >= TS1 AND 
PHOENIX_ROW_TIMESTAMP() <= TS2
```

* The above query returns the data modifications to the Data Table in the time window [TS1, TS2]
* This query can be hinted to return just the actual change, pre, or post image of the row, or a combination of them

### Phoenix Streams

Phoenix Streams provide a new framework for efficient consumption of change records by applications. [See [Change Data Capture for Phoenix Stream](https://salesforce.quip.com/oJgoAze8OlYg) and [PHOENIX-7456](https://issues.apache.org/jira/browse/PHOENIX-7456)]. This framework introduces the concept of *partitions*, ** which act as containers for multiple change records. Partitions are nothing but groupings of change records based on the encoded region name of the data table region where the mutation landed. Since the data table regions can split/merge, the framework will also track the parent-child lineage of partitions. This partition metadata will be exposed to the application so that they can scale compute for their consumers of the change records and also consume change records in order of their arrival.

#### Accessing Changes using Phoenix Streams

1. Get the active stream for the table.
    `SELECT STREAM_NAME FROM SYSTEM.CDC_STREAM_STATUS WHERE TABLE_NAME = <table-name> AND STREAM_STATUS = 'ENABLED'`
2. Get the partitions for the stream. 
    `SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME = <table-name> AND STREAM_NAME = <stream-name>`
3. Consume records from the partition(s).
    `SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM <cdc-stream-name> WHERE PARTITION_ID() = ? AND PHOENIX_ROW_TIMESTAMP() >= ? AND PHOENIX_ROW_TIMESTAMP() <= ?`



## DynamoDB Phoenix-Shim Change Stream Implementation

### Recommended Approach

* Use Phoenix Stream framework to implement DynamoDB Streams abstractions. 

#### Sequence Number Generation

* DynamoDb assigns a sequence number to every change which is unique and monotonically increasing in the context of a shard. 
* In phoenix, there can be only one change at a given timestamp (HBase timestamp unit: millisecond) for a rowkey but there can be multiple changes at the same timestamp to different rowkeys.
* To return a unique sequence number in the GetRecords API, we can use a fixed length counter when iterating over changes from a partition (shard) in the order of data table rowkey and append it to the timestamp. For example, with a fixed length of 5, we can support 100000 different changes at the same timestamp. 
    * sequence number → <timestamp><counter offset>
    * for example **173647394534200034**

### API

#### CreateTable

**Request**

```
"StreamSpecification": { 
      "StreamEnabled": boolean,
      "StreamViewType": "string"
}
```

* If `StreamEnabled` is set to `true`, phoenix-shim will create the CDC virtual table and index
    * CREATE CDC CDC_<tableName> on <tableName> INCLUDE (pre | post)
        
    * We need to store the type of stream so that we know what hint to give to the CDC queries later. PTable has 2 VARCHAR columns from the old CDC design which could be re-purposed for this use → *SCHEMA_VERSION* and STREAMING_TOPIC_NAME. In phoenix-shim, we can then alter this column to contain the stream type.
    * `ALTER TABLE <tableName> set SCHEMA_VERSION = <StreamViewType>` 
* The creation time of the CDC index `__CDC__<CDCName>` can be used to denote when streaming was enabled on a table. This creation time will also be embedded inside the streamName created by Phoenix [See [CDC_STREAM_NAME_FORMAT](https://github.com/apache/phoenix/blob/master/phoenix-core-client/src/main/java/org/apache/phoenix/util/CDCUtil.java#L45)]. 
* The Phoenix Streams framework should take care of bootstrapping the stream and partition metadata in the appropriate SYSTEM tables. [see [PHOENIX-7459](https://issues.apache.org/jira/browse/PHOENIX-7459)] 

**Response**

* CreateTable returns a [TableDescription](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html) object. Relevant stream related attributes are:
    * StreamSpecification → as is, from the request
    * LatestStreamArn → unique identifier of the current active stream
        * STREAM_NAME from SYSTEM.CDC_STREAM_STATUS table
    * LatestStreamLabel → start timestamp of the current active stream
        * parse creation time from LatestStreamArn

#### UpdateTable

**Request**

```
"StreamSpecification": { 
      "StreamEnabled": boolean,
      "StreamViewType": "string"
}
```

* If `StreamEnabled` is set to `false` , mark the stream DISABLED in `SYSTEM.CDC_STREAM_STATUS`.
    * TODO: We will soon move to ENABLE/DISABLE CDC operations which would do this. 
    * we want to retain changes for 24h even if the stream is disabled, hence we should not drop the CDC entities. 
* If `StreamEnabled` is set to `true` , phoenix-shim will create the CDC virtual table and index, and save the type. 
    * `CREATE CDC CDC_<tableName> on <tableName> INCLUDE (pre | post)
        `
    * `ALTER TABLE <tableName> set SCHEMA_VERSION = <StreamViewType>` 

**Response**

* UpdateTable also returns a [TableDescription](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html) object. We can return attributes similar to CreateTable [Response](https://salesforce.quip.com/vunDA0Fwedt5#temp:C:LNTa981515011fb431bad28520d5)

#### [ListStreams](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_ListStreams.html)

**Request**

```
{
   "ExclusiveStartStreamArn": "string",
   "Limit": number,
   "TableName": "string"
}
```

    * Assume that TableName is provided. [TODO: what if tableName is not provided]
    * Assume that we always return all results so no pagination is required using `ExclusiveStartStreamArn` .
    * Query `SYSTEM.CDC_STREAM_STATUS` for any active stream on this table.

**Response**

    * Phoenix Query
        * `SELECT STREAM_NAME FROM SYSTEM.CDC_STREAM_STATUS WHERE TABLE_NAME = <tableName> AND STREAM_STATUS IN ('ENABLED', 'ENABLING')`
    * For the active stream, we return the following:
        * StreamArn →  unique identifier, STREAM_NAME
        * StreamLabel → start timestamp of the stream - parse from stream_name OR index creation timestamp 
        * TableName → from the request

#### [DescribeStream](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_DescribeStream.html)

**Request**

```
{
   "ExclusiveStartShardId": "string",
   "Limit": number,
   "StreamArn": "string"
}
```

* Query `SYSTEM.CDC_STREAM` table with STREAM_NAME = StreamArn and TABLE_NAME = <parse tableName from StreamArn>

**Response**

* Phoenix Query
    * `SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME = <tableName> and STREAM_NAME = <streamName>`
* [StreamDescription](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamDescription.html) object
    * CreationRequestDateTime → CDC index creation date time or parse from StreamName
    * KeySchema → table PK Columns
    * Shards
        * ShardId → PARTITION_ID
        * SequenceNumberRange → <epoch timestamp><counter offset of 5 digits>
            * See [Sequence Number Generation](https://salesforce.quip.com/vunDA0Fwedt5#temp:C:LNT06e70b01a9a94f31b3129a019)
            * EndingSequenceNumber → null for active shard, 
                * For closed shard → <PARTITION_END_TIME><99999>
                    * eg: If child start time was 10, phoenix would update end time for parent as 10. Parent shard should have ending sequence number as <10><99999> and child will have start sequence number as <10><00000>
                        * Note that sequence numbers do not need to be globally unique, they are unique and increasing only in the context of a shard.
            * StartingSequenceNumber → <PARTITION_START_TIME><00000>
        * ParentShardId → PARENT_PARTITION_ID
    * StreamArn → from request
    * StreamLabel → stream start time
    * StreamStatus → ENABLED/ENABLING 
        * Query SYSTEM.CDC_STREAM_STATUS
        * Stream can be disabled, changes in shards will still be there to consume for 24h
    * StreamViewType → from PTable
    * TableName 

#### [GetShardIterator](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetShardIterator.html)

**Request**

```
{
   "SequenceNumber": "string",
   "ShardId": "string",
   "ShardIteratorType": "string",
   "StreamArn": "string"
}
```

    * Shard Iterator should have all the information needed to query Phoenix CDC. 
        * table, partitionID, starting sequence number, cdc object name, stream_view_type
        * sequence number → <timestamp><counter offset>
    * ShardIteratorType will decide starting sequence number
        * TRIM_HORIZON → start = partition’s start timestamp since trimming should be taken care of by TTL, offset = 00000
        * LATEST → start = currentTime (this should only be for the active/open shard), offset = 00000
        * AT_SEQUENCE_NUMBER → start = SequenceNumber
        * AFTER_SEQUENCE_NUMBER → start = SequenceNumber, offset+1

**Response**

```
{
   "ShardIterator": "string"
}
```

* Shard Iterator can be of the form `shardIterator/<tableName>/<cdcObject>/<streamType>/<partitionID>/<startSeqNum>`
* TableName and StreamType should be present in the provided StreamArn. 

#### [GetRecords](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html)

**Request**

```
{
   "Limit": number,
   "ShardIterator": "string"
}
```

* ShardIterator should have all the information needed to build the Phoenix CDC query. 
* `shardIterator/<tableName>/<cdcObject>/<streamType>/<partitionID>/<startSeqNum>`
* Since we need to compare pre/post images to differentiate between an insert and an update, we can always query for both pre and post images. 
* startSeqNum → <time><offset>

```
`SELECT ``/*+ CDC_INCLUDE(PRE, POST) */`` ``*`
`FROM ``<cdcObject``>`` `
`WHERE`
` PARTITION_ID ``=`` ``<partitionID>`` AND`
`PHOENIX_ROW_TIMESTAMP``()`` ``>=`` ``<time>`` `
`LIMIT ``<``Limit``> OFFSET <offset>`
```

* [Not Needed] There could be multiple records at the latest timestamp value from the result of the previous query and those might get trimmed because of the LIMIT. Hence in case the request provides a limit, we can do another query for <latest_timestamp> and return those records as well. Each records needs to be delivered exactly once so some deduplication will be required between results of the previous query and this one. 
    * **This will not be needed anymore since the sequence number generation will take care of pagination of changes at the same timestamp.**

```
SELECT /*+ CDC_INCLUDE(PRE, POST) */ *
FROM <CDC tableName> 
WHERE
PARTITION_ID = <partitionID> AND
PHOENIX_ROW_TIMESTAMP() = <latest_timestamp>
```

**Response**

* NextShardIterator 
    * set it to null if partition has split and we have no more records to return
    * otherwise, set start sequence number to be largest sequence number + 1 from the records returned so far.
* Array of [Record](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html) objects
    * eventName → INSERT | MODIFY | REMOVE
    * eventVersion
    * dynamodb → StreamRecord
        * ApproximateCreationDateTime
        * Keys → table PK Columns
        * NewImage → post_image from phoenix CDC query
        * OldImage → pre_image from phoenix CDC query
        * SequenceNumber → <timestamp>+<offset>, implement in memory when iterating through records
            * See [Sequence Number Generation](https://salesforce.quip.com/vunDA0Fwedt5#temp:C:LNT06e70b01a9a94f31b3129a019)
        * StreamViewType → from shardIterator
        * SizeBytes




### Alternate Approach


* To consume change events from Phoenix, we basically need a _*time window*_ [start, end]. 
* We can divide time into blocks of 24h each, starting from the time when stream in enabled on a table (CDC index creation time). 
* Each **24h time window** will represent a **stream**. 
* Each 24h time window can be divided into sub-windows. 
    * These **sub-windows** will represent **shards**. ****
    * Shards need to support distributed processing and high throughput for clients consuming the change records.
    * Therefore, we will cap the number of records in a shard to a configurable value **MAX_SHARD_SIZE**. 
* A stream can be represented uniquely using the following format 
    * `stream-<tableName>-<streamType>-<startTime>-<endTime>`
* Similarly, a shard can be represented like this
    * `shard-<tableName>-<streamType>-<startTime>-<endTime>`
* Dynamo only keeps data in streams for 24 hours. This implies that at any given current time, a client would only be able to see at most two streams - one Active stream and one Historical stream
    * Note that not all of the data in the Historical stream’s time window needs to be visible
    * We can achieve this by setting a 24h TTL on the CDC index table. This way, any change data part of Historical stream’s time window older than 24h from now will be trimmed.
    * Jira: [PHOENIX-7382](https://issues.apache.org/jira/browse/PHOENIX-7382)
* Each record in DynamoDB has a sequence number. We will use PHOENIX_ROW_TIMESTAMP to denote this sequence number.  Note that the timestamp may not be unique for all records in Phoenix CDC but that is okay as long as we return **all** records in the given time window. See [this](https://salesforce.quip.com/vunDA0Fwedt5#temp:C:LNT25635ec644134174a228e190b).

## References

1. [Change data capture for DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
2. [Amazon DynamoDB Streams APIs](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB_Streams.html)
3. [Phoenix Change Data Capture leveraging Max Lookback and Uncovered Indexes](https://issues.apache.org/jira/browse/PHOENIX-7001)
4. [Phoenix-Shim](https://git.soma.salesforce.com/bigdata-packaging/phoenix-shim)


