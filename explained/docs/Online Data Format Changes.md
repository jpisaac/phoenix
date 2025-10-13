# Online Data Format Changes

>This document is written by Gokcen Iskender to describe the current state in Phoenix and changes we need to do to enable online data format changes



## Background

Today, using ALTER TABLE or ALTER INDEX commands, the user can make certain changes to the table/index schema. For example, changing certain table properties like TTL and immutability, adding nullable columns/dropping non-pk columns are allowed as well as certain index state changes. All of the allowed changes don’t require the table to be re-written. As soon as the ALTER command returns, most of the changes became available immediately (eg. index disable, TTL) but some of them might take some time and the syntax lets you to specify async (eg. Rebuild index) and depending on the client cache settings, some changes never make it to the client (eg. select * that run from a client never seeing the new column since its schema cache is not updated). 
If the user wants to change the schema properties that require table’s data format to be changed, it is not supported. It is desirable to change some of the table schemas and attributes, such changing the row key (primary keys), the type of a column, the table storage format, the column encoding, etc. with no or minimal service interruption.

## Motivation and high level design

If we want to change the storage or column encoding of an existing table or a schema attribute like that, we cannot easily do that today. Ideally, we would want to reformat the data within the same table in place but it is very complex (for other considerations refer [here](https://salesforce.quip.com/9mThAaGfU8YX#fBeACAi7pve)). We did some analysis to see if it could be done but it showed to be complicated to support two different formats on the same table.
Another way to do this could be; preparing the new data table in the new format and just renaming the new table to be the old table name in Hbase. However, Hbase and Phoenix does not provide a simple way to rename physical tables. In order to rename a table in Hbase, the table with the old name needs to be dropped, a snapshot on the new table needs to be taken and restored with the old name which requires downtime. Renaming is also not supported in Phoenix. Currently, the tables in Phoenix have the same name as the underlying Hbase table. This creates some limitations.
Separating logical and physical table name, ie. Having a Phoenix table point to an Hbase table with a different name has some advantages. 
In this scenario, in order to take advantage of the new storage formats, it would be more flexible to have a solution where the Phoenix table can point to the new Hbase table and data format change would be seamless. 
Even if the physical and logical name of the tables are separated, we still need a way to ensure that the new table and the old table are in sync. If we can do that, we don’t need to worry about downtime and we can call  it online data format change.

The problem of keeping old and new tables in sync is very similar to the problem of keeping data and index tables in sync.
In fact, we can consider an index as another form of data transformation. The difference is that the index transformation is permanent in the sense that it is done all the time for the index’s lifetime but the online data transformation is applied temporarily; i.e until the data turned into another format.

For this design, we need to transform the existing table to a new data format asynchronously and after the table has all the data as the old one, we simply want to switch to using that table underneath the logical name. While the transform is happening, the reads go to the old table and writes go to both the old and the new table. After the cutover, the reads and writes only go to the new table. We also need all the schema caches to be updated to reflect the change after cutover. 

Table transformation can be interpreted as schema transformation and the transformation of the data in the table (ex. converting lowercase to uppercase or Pivoting concept). This design supports online data format changes that does or doesn’t require schema changes.

With this design, the aim is to provide a framework for the data format transformation. There may be many use cases that would require data format changes and they can be implemented with some tweaks and they can come later.
An immediate use case of this design is converting storage scheme of a data table (for example from ONE_CELL to SINGLE_CELL) that we will focus on. 

The transformation on the table is meant to be one at a time which means the transform will not be applied to the child tables. Views share the same physical base table, so they will be auto transformed but the indexes will need to be separately built.

**Out of scope:**

* Any change to the schema that we can already do today like adding/dropping columns, changing TTLs will not be part of this feature. This feature is mainly focused on *transformation* of the table, in other words, cases where we need table rewrites due to data format change.

* In place schema format changes (on the same table). 
* Creating a new table with a different physical and logical name is not in scope.
* Expiring client metadata caches on the connection profiles is not in the scope of this design. This design assumes that it is available.
* This design only works with the tables that are upgraded to Strongly Consistent Indexes (PHOENIX-5156). The tables that have Indexer as the coprocessor (rather than IndexRegionObserver) are not supported.

### Terminology:

**Transform**: In this document, it refers to the data format transformation of a table which requires a re-write of the table
**Cutover**: Cutover is a point in time, where we switch from using old physical table for reads and writes to the new physical table for reads and writes. Once the cutover is done, the old table is not updated or read from. 
**TransformTool**: A map/reduce job to transform and populate the existing data into the new table. It is like IndexTool
**TransformMonitor**: Constantly monitors the result of TransformTool and retries if necessary.

## Requirements

* Preparing the table in the new schema form while the old table is serving CRUD and keeping them in sync
* While Phoenix is working on the transformation, further changes to the schema on this table should be blocked. 
* After the table is in sync with the old format, serving CRUD from the new table
* Separating physical and logical table names in Phoenix ([PHOENIX-6247](https://issues.apache.org/jira/browse/PHOENIX-6247))
* Phoenix clients should be blocked to use the old table after the cutover is done.

## Design details

### API for transformation:

For the schema changes that require a table to transform, we can use ALTER TABLE syntax and do the changes behind the scenes. As any other schema related changes like TTL, the user can use ALTER to specify what is changing. As with other ALTER statements, it is idempotent. 

We can start with storage schema and column encoding change and gradually evolve the ALTER TABLE syntax and support more scenarios as we have more use cases. 
`ALTER TABLE tableName IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS`

The changes that requires a transform via ALTER support both async and sync runs. If the user doesn’t specify ASYNC keyword, the map/reduce job will not be used and the statement will return when the transform finishes.

### Workflow for transform start:




1. User runs: ALTER TABLE with phoenix table name and parameters to convert to, like storage format, etc. 
2. Phoenix takes a lock on the table in Syscat so that the other ALTERs will fail by setting a new column: IS_MODIFIABLE. We can use SYSTEM.MUTEX to lock the table itself but the mutex table has a TTL of 15 minutes, so it is better to use IS_MODIFIABLE.
3. Phoenix queries Syscat with the old table name to get the new table structure and combines the existing structure with given parameters (eg: Gets columns, column types etc but changes storage format).
4. Phoenix generates a new table name. 
5. New Phoenix table is created with the new name and all the coprocs we have today. In this step, we have the new table in the Syscat and associated hbase table. It is as if we run Create table for the new table.
6. Phoenix creates the new table on the DR buddy. This is needed so that the replication doesn’t fail and is ready to work when we switch to the new table.
7. Phoenix records the new table, old table, old table’s metadata and the transform in the SYSTEM.TRANSFORM table
8. A mutation generation logic on the IndexRegionObserver is enabled on the old table. This coprocessor’s job is to get the mutations that are currently happening to the old table into the new table. The coprocessor knows which table to copy to by using SYSTEM.TRANSFORM and like IndexMaintainer, uses TransformMaintainer to generate mutations.
9.  Phoenix also kicks a copy logic for getting the existing data into the new table. Data can be copied to the new table via an IndexTool like tool (TransformTool) that would create MR jobs to copy data. JobID will be recorded to SYSTEM.TRANSFORM
        1. This tool will also copy local index and view data as well since the physical table has this. It will generate hbase mutations from hbase record so that we don’t need to worry about tenant view etc.
        2. TransformTool needs to be runnable externally and provide a way for validation
        3. If the TransformTool says it is in sync, it should update the SYSTEM.TRANSFORM table with a state saying it is ready to switch to new table or failure.
        4. At this point, Phoenix needs to monitor this tool and re-kick and look at the validation for deciding to end the transform

### Workflow for transform end:


1. TransformTool updates status into System.Transform table. 
2. TransformMonitor keeps querying the System.Transform table and decides to retry up to a certain number or do cutover
3. Once TransformTool is successful  (all the data is in sync), TransformMonitor decides to do cutover.
4. TransformMonitor writes the new table name to the PHYSICAL_TABLE_NAME column and changes the metadata values (eg. Storage format). After this point, reads and writes are served from the new table.
5. TransformMonitor expires caches so that the logical table points to the physical table
6. TransformMonitor kicks one more partial TransformTool run and  TransformMonitor monitors and retries if necessary. This last transform is to handle the failed online mutations that the TransformTool did not know about. In order for this to work, we need the old table metadata from System.Transform table in order to parse and transform the old data. From TRANSFORM_TYPE column, TransformTool will know what values to use in Syscat for metadata.
7.  At this phase, the entries in SYSCAT for the new table is removed and IS_MODIFIABLE column is set to true.
8. Transform record is marked as done on the System.Transform table.
9. Conditional step: Run index rebuild for all the indexes of this table and its views. This step is necessary if the primary key structure of the table is changed. It might not be necessary for other changes.



### Error handling for IndexRegionObserver and TransformTool

TransformTool is retried until it is successful in syncing old and new data. TransformTool can verify and record the mismatch just like IndexTool.

For the current mutations, if the table is **Mutable**:
 IndexRegionObserver follows the approach as indexes and the new table is like another index table. There will be a 3 step write.
1: IndexRegionObserver writes to the new table and indexes with Unverified flag. If fails, fail the write
2: IndexRegionObserver writes to the old table. If fails, fail the write
3: IndexRegionObserver Update the new table and indexes with Verified flag. If fails, ignore



If we want to copy and re-write existing table data, we need to handle errors like we do for index tables. To ensure that we handle failures; the verified/unverified flag is stored in the same empty column that the consistent indexing uses. 
TransformTool will fix the rows that are Unverified after the cutover.

For **Immutable** tables:
For consistent indexing, the client sends the mutations for both index and the data table. We could have followed the same approach for the new table but it is more expensive to solve issues like what if the client’s cache is not renewed and client doesn’t know that the transformation is started. So, a slightly different approach is followed;
 
1: Client writes to indexes with Unverified flag. If fails, fail the write
2: Client writes to the old table but IndexRegionObserver intercepts and writes to the new table with Unverified flag and writes to the old table. If fails, fail the write
3: Client updates the indexes with Verified flag. IndexRegionObserver updates the new table with Verified flag. If fails, ignore






### Workflow for transform Pause:

For availability reasons, we might need to pause the transform. In order to do that, we can set the TRANSFORM_STATUS field to PAUSE and when paused, the TransformTool will not be re-tried from TransformMonitor. We can restart it only by setting it to RESUME in which case, TransformMonitor will kick another full run.

### Workflow for transform Abort:

We can only Abort the transform before it completes.



1. TransformTool queries System.Transform for the ongoing transform on the old table. If there is no transform record, it doesn’t do anything
2. TransformTool gets the running JobID and kills it and removes the record from System.Transform table. 
3. The transform functionality on IndexRegionObserver is disabled on the old table
4. TransformTool removes the entires for the new table in Syscat
5. TransformTool removes the lock on the table.

TransformTool can be used to Pause/Abort a transform.

```
hostname$ /usr/hdp/current/hbase-client/bin/hbase org.apache.phoenix.mapreduce.TransformTool 
   -t tableInTransform -abort
```

### **Rolling back transform**

If the table is finished transforming, we cannot go back to the old table. The only way of doing that would be to rollback the Syscat. This will point the logical table back to the old physical table but the data that is changed after the transform will be lost. Not all new data can be compatible with the old table. An example of this could be changing the column type from CHAR(10) to CHAR(15) and after transform, writing >10 chars in the column. This data cannot be converted to old table as is.

### IndexRegionObserver Design Changes

IndexRegionObserver has a new job to propagate mutations to the new table as well as the indexes (if any).
IndexRegionObserver fails the mutation if it cannot write the mutation to the new table. It does 3 step writing like consistent index design. First new table is updated with unverified flag, then the old table and indexes are updated and finally verified flag is set. If the first and second steps fail, we return failure to the client. The new table is treated like any other index.

PTable has an attribute to enable Transform functionality on the IndexRegionObserver of the old table. Like IndexMaintainer class that this coprocessor uses today, it uses a TransformMaintainer to generate mutations for the new table.

TransformMaintainer doesn’t need to be serialized. It uses the System.Transform table to get the new table name, old table schema and creates mutations for the new table schema.

TransformMaintainer should also prepare mutations for the local indexes as well.

One more change for the IndexRegionObserver is to fail the mutation that comes for the old table after cutover. IndexRegionObserver can do this by looking at the mutations destination table and making sure that it is the matching physical table. IndexRegionObserver will also block the client that is not sending mutations to an immutable new table due to its stale cache.

### TransformMonitor Design Details

TransformMonitor is the TransformTool overseer component. Its purpose is to continuously check the System.Transform table and do retries (in case the TransformTool job fails)

**Option 1**: Use System.Task table to register TransformMonitor task. This task will be similar to IndexRebuildTask.
**Option 2**: Use a separate tool like index-monitor.

We will go with Option 1 since the Open Source will not have index-monitor like an external tool.

### TransformTool Design Details

We expect TransformTool to run only between the timeframe of transform of a table and once after the cutover. It will not run once we have a successful cutover since the old table metadata will be cleaned up from Syscat.
The syntax of TransformTool looks something like this:

```
hostname$ /usr/hdp/current/hbase-client/bin/hbase org.apache.phoenix.mapreduce.TransformTool 
   -t tableInTransform ...
        org.apache.phoenix.mapreduce.TransformTool$Counters 
                ONLY_ON_SOURCE_ROWS=2
                CONTENT_DIFFERENT_ROWS=1
                GOOD_ROWS=1
                ONLY_ON_DESTINATION_ROWS=1
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=0 
```

The bad rows are written to an Hbase table which is similar to PIT table.
Other optional parameters are:

|`--starttime=<timestamp>`	|Beginning of the time range, in milliseconds. Time range is forever if no end time is defined.	|
|---	|---	|
|--abort	|Abort the transform	|
|--pause	|Pause the transform	|
|--resume	|	|

The TransformTool copies the data and does a validate and emits the counters above (like -v AFTER in IndexTool). While copying the mutations, the same timestamp as the old table’s mutations is used.

TransformTool records its status and JobID in the System.Transform table. And once it is successful, it will do cutover and do partial run. It will also do pause and abort transform.

### SYSTEM TABLE Changes: 

**SYSCAT Changes:**
For data tables, the physical table name will be stored in the new column of Syscat: PHYSICAL_TABLE_NAME. If it is empty, the value is the TABLE_NAME.
For indexes, the data table name is stored in DATA_TABLE_NAME of index row on Syscat.
For views, the base data table name is stored in COLUMN_FAMILY field of the main view row on Syscat. Instead of updating different Syscat rows, these fields will just mean the logical name of the table.
There will be another column IS_MODIFIABLE added to Syscat.

**SYSTEM.TRANSFORM Table:**
This is a new table for Phoenix to use to retry or work on a transform. It will look like the following:

* Table_Name : Logical name of the table
* New_TABLE: Physical name of the new table
* STATUS: Transform status for coproc to use.  
* TRANSFORM_JOB_ID : ID of the map/reduce job that transforms and populates the new table
* RETRY_COUNT:  How many times is the transform job retried
* TIMESTAMP: Populated by TransformTool for the timestamp of its status
* TRANSFORM_TYPE: Full or partial transform to distinguish if this is the last transform after cutover
* OLD_TABLE_METADATA: Json for metadata of old table. We need this so that after the cutover we still need one more transform from old table to new.
* NEW_TABLE_METADATA
* TRANSFORM_FUNCTION: Could be used for data transformation in the future

### Index Table Transformation

If the table that is transforming is an Index table, the changes to the above design are following:

* An index with the metadata changes to the old index is created on the table as if this is another index.
* Regular index build process will build the index in the new format and IndexRegionObserver will work as before
* After the index is created and in ACTIVE state, PHYSICAL_TABLE_NAME of the old index is set to the new index physical table along with other Syscat changes

## Replication and SOR

Once the new table is generated on one DR buddy, the same table needs to be generated in the other buddy with the same name. Once the tables are generated, they can start replicating. 
If we don’t create the table in the DR buddy, we will need to turn off and turn on the replication later after the new table is created on the destination and this step can easily be missed. Also, the replication will take some time if it is done much later. Creating the new table on the DR buddy also helps with tenant migrations.

For SOR, a backup is taken via SOR tooling within the 24 hours so that if shortly after it needs a restore, we will have a base. If the old backup needs to be restored on the new table, we can either restore the backup on the old table and run TransformTool or implement transformation as part of restoration.

## Performance Impact

Online schema change and table transformation will only impact the system during the transformation time. The population of the new table via IndexRegionObserver and TransformTool will be similar to creating another index on the data table today. After the new table is ready, the transformation will be out of the picture. 
For write performance; it will be two extra writes like an additional index table
For read performance there will not be any impact.

## Testability

For testing, we can follow the same approach with PHOENIX-5156 (Strongly Consistent Indexes). Along with integration tests, we can simulate real time load via long running testing and monitor TransformTool results and verify the new table.

## Upgrade and Backward Compatibility

Since we will use the existing IndexRegionObserver coproc, we will not need to upgrade table coprocs. 
As part of the major version upgrade, SYSTEM.CATALOG will be changed and the new SYSTEM.TRANSFORM table will be introduced.

For backward compatibility; The clients that don’t have their cache updated including old clients that are sending mutations to the old table after cutover will be rejected.

## Tenant Deletions and Org Migrations

Tenant Deletions work with this design since the tenant delete goes through all the hbase tables and removes the entries. In this case, the tenant will be removed from both the old and the new table.
Refer to [this](https://salesforce.quip.com/Hnx3AgECcMCk) doc for further design details.

## Proposed Changes Summary

1. Add a new column (PHYSICAL_TABLE_NAME) in SYSTEM.CATALOG. By default, the value of this column is null. If it is null, this means that the Phoenix and Hbase table names are the same. If it is not null, Phoenix will use this table in Hbase as physical table.
2. Create a table with all the coprocessors. This table definition is the same as the old table plus the differences defined in the transformation.
3. Move the old data into new data in the new format via map/reduce (An IndexTool like *TransformTool*)
4. We need a new coprocessor functionality to sync online mutations from old table to the new table. Details of this coprocessor is in the following section of this document.
5. Validate that the new table is up to date with the old table
6. Cutover and move to the new table. *Cutover* is the point where all reads and writes only go through the new table.
7. Expire the metadata related caches (ConnectionQueryServices cache, client caches, etc) for the table when PHYSICAL_TABLE_NAME is set.
8. When a table has views and indexes on top, the physical name change should be reflected on the views and indexes as well. (COLUMN_FAMILY should point to logical name for views)
9. When a table is going through transform, further transformations or alterations should be blocked on the table (IS_MODIFIABLE column in Syscat).
10. For error handling of map reduce and retrials, have another SYSTEM.TRANSFORM table

Changing the physical name of the index table is also supported.
For views, the physical base table name change is handled as part of regular table change.


### Challenges and things to do:

Need to investigate:

1. For immutable tables, what happens to a client that does a mutation while the underlying physical table name is changed?
2. What happens to the ongoing MR jobs? 
3. Do we need a change in HashJoinCache?
4. How to handle local index and dynamic columns when we create the new table?
5. How many versions should TransformTool worry about? Is there a case where just the last version not enough?
6. Compatibility handling of new and old physical tables:
    1. ~~(Handle) Column structure (types, nullable) and number of columns and primary keys: If the physical table has different columns than the logical table, how to handle. If the physical table has less number of columns, we can block this. Also handle the column structure (type etc) change. If we allow a non-matching new physical table to be associated with old logical name, we need the user to tell us the new primary keys, column types, new columns, new storage format etc.~~
        1. ~~For Pre-existing tables; How about missing local index columns? How to handle that? Block for now?~~
            1. ~~We don’t need to worry about local indexes or views if we generate hbase mutations by looking at the hbase table.~~
    2. (Block) If the new data table is changing the storage format and it had indexes on it, there might be a case where the data table and index table will not be compatible. For example, today, data table being SINGLE_CELL and index table being ONE_CELL_PER_COLUMN is not supported but the reverse is supported (i.e index SINGLE_CELL, data table ONE_CELL). If the new physical table is in SINGLE_CELL format, and the table has ONE_CELL index, we should block this change if they are incompatible with index. In that case, the user needs to start from changing the indexes to new physical tables and then the data table.  


4. Other changes:

1. We should add another column to the Syscat so that the other ALTERs etc will fail. 
2. We need to change IndexTool and IndexScrutinyTool, COLUMN_FAMILY to handle view index build/queries. Map/reduce input queries use the base table name which should be handled.

### References:

* [https://docs.google.com/document/d/1Vsf23GCT0_CK4q8g_xaXyE_4Dw3aH71BfZypEy3T9iQ](https://docs.google.com/document/d/1Vsf23GCT0_CK4q8g_xaXyE_4Dw3aH71BfZypEy3T9iQ/edit)



## APPENDIX

For implementation plan: [Online Data Format Changes Implementation Plan](https://salesforce.quip.com/855MAXaxtRa7)



### Design alternative: Using same table but different Column Family to do Transform

Rather than creating a different table for transform, an alternative design could be using the same table with different column family. The mutations will go into the new CF and after it is done, in SYSCAT, the table will point to the new CF. This alternative has following shortcomings:

1. The rowkey change will not work: If we need to change the rowkey, having a different CF will not work
2. There might be more than one CF in the original table which will make the handling complicated.
3.  Adding and removing a CF requires disabling the table at the HBase level which will result in more service interruption. 
4. More complications and code changes are required: Having a second table is cleaner, simpler and better aligned with the current architecture and design of Phoenix. Otherwise, we need to make additional changes in several areas. For example, there is only one empty column for a given row and we use it to implement two-phase commits (verified/unverified status). If we were to use a separate set of CFs, then we would need one empty column per set instead of one per row. We also need to change the places that translate the Phoenix columns to HBase columns as now we need to translate Phoenix CF to HBase CF. Another the class of use cases, where using a set of CFs instead of a separate HBase table would result in more complexity and code changes are the ones requiring changing table level attributes such as changing the storage format or column encoding. Currently, all column families are subject to the same encoding and storage format. 
5. Performance and metadata cache updates become more critical if we use the same table.

* * *

### Metadata Cache expiring (not part of this design)

Changes such as changing the physical name of a table or changing the immutability of the table, and more metadata scenarios like this requires the metadata caches to be refreshed. Some caches live in a the region server of Syscat and some are per connection profile.   

```
// Expires client cache (per connection)
conn.unwrap(PhoenixConnection.class).getMetaDataCache().removeTable(null, tableName, null, 0);

// *Clears the Phoenix meta data cache on each Syscat region server (MetadataEndpointImpl cache)*
conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
```

getQueryServices().clearCache() method makes an RPC call to all the Syscat region servers. These regions have a GlobalCache that has the metadata cache. This call also clears tenant caches which also reside in the GlobalCache.

There are some alternative approaches to expire the cache when a metadata change occurs:

* **Option 1:** Using UPDATE_CACHE_FREQUENCY. When the user calls BEGIN TRANSFORM, we can update the cache frequency of this table to 0 (ALWAYS). ALWAYS is the default value but this can be set per table. When we finish the transform later some time, we expect that the client fetches the latest metadata.
    * Cons: 1- Chicken&egg problem. When this value is set, the code expires the current JVM cache but doesn’t expire other caches. We can force the server cache update but client cache can still end up with stale data in the cache. 2- Hard to set it back to its original value.
* **Option 2:** Implement a Zookeeper Watcher. This watcher can listen on a znode representing MetaData. (ex.  /metadata/versionCnt and /metadata/changedTables/..) 
    * Cons: 1- Will it work for thin clients? 
    *            2- Will not work if we move away from ZK  
    *            3- Lag between watcher and the actual update since it is async
* **Option 3:** Hbase Procedure/Notification-Bus ([W-7006316](https://gus.lightning.force.com/lightning/r/ADM_Work__c/a07B0000007rIPIIA2/view)): A Procedure is a transform made on an HBase entity like Regions and Tables. The Procedure framework can send notifications across multiple machines. Schema object refers to PTable.
    * Updating client caches:
        * The client keeps timestamp of each schema object in the cache.
        * For every operation, the client puts the timestamp of the relevant schema object in an attribute on the RPC message
        * Phoenix checks this timestamp while processing this RPC. If it is older than the current one (ddl timestamp), it returns a new exception (SchemaOutOfDateException)
        * The client handles the exception by retrieving the latest/or expiring its cache and retrying the query.
    * Updating server caches: 
        * The server keeps timestamp of each schema object in the cache. 
        * When a metadata update occurs, Hbase Procedure framework will be executed. This framework will update all the caches (push).
        * The Procedure framework will make sure that the global barrier is reached. If not, it will fail and needs to be retried. If still fails, the metadata change will fail.
    * Pros: Clients will never use an out of date schema regardless of UPDATE_CACHE_FREQUENCY. And server will not need to query Syscat unnecessarily either (even for timestamp check) since the update will be pushed and server cache coherency is guaranteed.
    * Cons: 1- There is some latency cost while waiting for all the region servers to reach the barrier point but it should be on par with snapshot or table flush. Given the metadata changes are infrequent, it should be ok. 2- DDL operations may fail more often




The Client maps to Phoenix in this case. 

We can implement the update client caches in Option 3 and use ConnectionQueryServicesImpl. clearTableFrom cache to update the cache.

