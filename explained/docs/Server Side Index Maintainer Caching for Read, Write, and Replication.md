# Server Side Index Maintainer Caching for Read, Write, and Replication

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com), [Viraj Jasani](mailto:vjasani@salesforce.com), [Rushabh Shah](mailto:rushabh.shah@salesforce.com)  
Updated: Aug 15, 2023

This document describes a design for eliminating replicating global index tables and improving read and write paths for tables with indexes by caching index metadata on the server side. Given that updates to index table schemas or creating indexes are rare operations when compared to read and write operations, the performance improvement for read/write path using cached index metadata will be noticeable. Eliminating replication for index tables will bring even more performance and efficiency improvements as it will cut down replication bandwidth requirements significantly. More importantly, it will eliminate known data integrity issues caused by replicating global secondary indexes.

# Background

Phoenix does not implement secondary indexing using well-known B-Tree data structures directly. Instead Phoenix leverages the indexing built into HBase tables. HBase provides indexing for fast access to table rows using row keys or row key prefixes. Phoenix implements global secondary indexes by creating a separate table for each secondary index and allows secondary indexes on views to share the same HBase table (instead of creating a separate index table for each view index). Phoenix also provides local secondary indexes that are implemented by allocating a separate column family for local indexes within the same data table. This document focuses on the global indexes but the proposed solution is also applicable to local indexes.

A given table or view may have multiple secondary indexes. These indexes are updated synchronously while the table or view rows are mutated. Phoenix uses a two-phase commit protocol to keep indexes strongly consistent with their data tables or views. In the rest of this document, the term index refers to both an index on a table and index on a view (i.e., view index) if it is not qualified otherwise. Similarly, the term data table refers to both a Phoenix table or view on which an index is created. 

An index table includes all primary key (PK) columns and secondary key columns and can also include some of its data table non PK columns. Including non PK columns is to boost the performance of the queries. By including all the columns for a given query, Phoenix can serve the query solely using the index without joining index rows with the data table rows.

Phoenix tables are replicated at the HBase level using HBase replication. Since Phoenix creates separate HBase tables for secondary indexes, by being HBase tables, these index tables are also replicated by HBase. 

The relationship between a data table and index is somewhat involved. Phoenix needs to transform a data table row to the corresponding index row, extract a data table row key (i.e., a primary key) from an index table row key (a secondary key), and map data table columns to index table included columns. The metadata for these operations and the operations are encapsulated in the class called IndexMaintainer. Phoenix creates a separate IndexMaintainer object for each index table in memory on the client side. IndexMaintainer objects are then serialized using the protobuf library and sent to servers along with the mutations on the data tables and scans on the index tables. The Phoenix server code (more accurately Phoenix coprocessors) then uses IndexMaintainer objects to update indexes and leverage indexes for queries. 

The Phoenix server code uses IndexMaintainer objects associated with a given batch of mutations or scan operation only once (i.e., for the batch or scan) and the Phoenix client sends these objects along with every batch of mutations and every scan. For scans, IndexMaintainer objects are serialized into a scan attribute. For a batch of mutations, the serialized objects are sent to Phoenix server side caches using separate RPC calls. Phoenix implements a separate HBase endpoint coprocessor for server side caching. In addition to index use cases, the server side caching is used for join use cases by caching hash join data on the server side. 

# Issues with Replicating Global Index Tables

Before we explain the issues with replicating index tables, we need to understand why we needed to replicate index tables in the first place. There are two main reasons for that:

1. The current design of the Phoenix server requires the Phoenix client to send the metadata for the secondary indexes (i.e.,IndexMaintainer), to transform data table mutations to the corresponding index table mutations.  
2. Replication happens at the HBase level and thus Phoenix secondary indexing metadata is not available in the replication path. 

Now that we know why the current design needs to replicate index tables, we can describe the issues with replicating global index tables. There are three such issues: consistency, data loss and efficiency. Please note local indexes do not have the consistency and data loss issues but share the same efficiency issues.

The secondary indexes are used to improve the performance of queries on the secondary index columns. The secondary indexes are required to be consistent with their data tables. The consistency here means that regardless of whether a query is served from a data table or index table, the same result is returned. This consistency promise cannot be kept when the data and their index table rows are replicated independently, which happens at the HBase level replication. 

HBase replicates WALs (Write Ahead Logs) of regions servers and replays these WALs at the destination cluster. A given data table row and the corresponding index table row are likely served by different region servers and the WALs of these region servers are replicated independently. This means these rows can arrive at different times which makes data and index tables inconsistent at the destination cluster.

Data loss issues happen if the first phase of the two phases of index table row update (an unverified/uncommitted index row) arrives before the corresponding data table row. During the read path, these unverified index rows are recovered from their data table rows. However, if the data table rows do not make it to the destination on time, unverified index table rows are deleted using delete markers. These delete markers can lead to the physical deletion of the index rows during compaction. Even if the second phase of index updates arrive after these delete markers, they will be also physically deleted during compaction due to the delete markers inserted by the index read repair code path. The current implementation delays adding delete markers for the unverified rows for a week by default to reduce the likelihood of data loss but this does not completely eliminate the possibility of data loss.

Replicating global indexes leads to inefficient use of the replication bandwidth due to the additional overhead of replicating data that can be derived from the data that has been already replicated. As in the write path, the index table rows can be derived from the data table row in the replication path at the destination. When one considers that an index table is essentially a copy of its data table without the columns that are not included in the index, and a given data table can have multiple indexes, it is easy to see that replicating indexes can double the replication bandwidth requirement easily for a given data table.

# Design

A solution for eliminating index table replication is to add just enough metadata to WAL records for the mutations of the data tables with indexes and have a replication endpoint and coprocessor endpoint to generate index mutations from these records, please see [PHOENIX-5315](https://issues.apache.org/jira/browse/PHOENIX-5315). This document extends this solution to eliminate replicating index tables but also to improve read and write path for index tables.

The idea behind the proposed solution is to cache the index maintainers on the server side and thus eliminate transferring index maintainers during read and write as well as replication. The coprocessors that currently require the index maintainers are IndexRegionObserver for the write path and some other coprocessors including GlobalIndexChecker for read repair in the read path. 

## Write Path Design

IndexRegionObserver observes the mutations made to a table region and generates index mutations from them. IndexRegionObserver will just use the metadata that is cached at its region server by ServerCachingEndpointImpl and generate index mutations for the replication and write paths in the same way. 

ServerCachingEndpointImpl uses [Guava Cache](https://github.com/google/guava/wiki/CachesExplained) which is similar to a Java Map. The difference is that the entries are evicted automatically based on the configured eviction policy. Each cache entry is a key-value pair. Phoenix code generates a unique key for each cache entry even if the same object (i.e., the value) is inserted to the cache. That is why the key for a cache entry is referred to as UUID in the Phoenix code. In the rest of the document, we will not use the term UUID as this proposal will use the same key for the same object for every insertion of the object to the cache.

The proposed solution leverages the existing capability of adding IndexMaintainer objects in the server side cache implemented by ServerCachingEndpointImpl. The design eliminates global index table replication and also eliminates the server side cache update with IndexMaintainer objects for each batch write.

IndexRegionObserver (the coprocessor that generates index mutations from data table mutations) needs to access IndexMaintainer objects for the indexes on a table or view. The metadata transferred as a mutation attribute will be used to identify the table or view for which a mutation is. The metadata will include the tenant Id, table schema, and table name. The cache key for the array of index maintainers for this table or view will be formed from this metadata. When IndexRegionObserver intercepts a mutation on an HBase table (using the preBatchMutate coprocessor hook), IndexRegionObserver will form the cache key for the array of index maintainer and retrieve it from the server cache. 

Please note that when a table (or view) has multiple indexes, all of the indexes are updated when a table (or view) row is updated. This is the reason to cache all IndexMaintainers together in an array instead of caching them separately. If the cache does not include the array, then IndexRegionObserrver will retrieve the array of IndexMaintainer objects using the Phoenix client code from the system catalog table.

### Changes for IndexRegionObserver

In addition to retrieving the array of index maintainers from the system catalog table, IndexRegionObserver should not attempt to change the timestamps on the mutations from a replication stream. To identify if a mutation is from a replication vs from a client or local region server, IndexRegionObserver needs to check if mutations include a specific mutation attribute. This could be a new mutation attribute or an existing attribute. 

### Metadata Cache Coherency

This design requires maintaining metadata caches at region servers. These caches need to be consistent, that is, these caches should not have stale metadata. The cache coherency will be maintained using a form of two-phase commit protocol to update these caches. 

The metadata caches are updated when MetaDataEndpointImpl updates the metadata. It first invalidates the metadata caches. If the update for an existing index, such as adding and removing a column, then MetaDataEndpointImpl removes the corresponding list of IndexMaintainer objects from the caches. If the update adds, removes, enables, or disables an index, then the corresponding array of IndexMaintainers.  This is the first phase of the two-phase commit protocol. If the first phase (that is, the object removal from all caches) fails, then the metadata operation fails and the failure response is returned to the client. If it succeeds, then MetaDataEndpointImpl updates the SYSTEM.CATALOG table which is the second phase of the two-phase commit.

It is important to note that MetaDataEndpointImpl needs to use locking to serialize read and write operations on the metadata. After the caches are invalidated in the first phase, IndexRegionObservers would attempt to retrieve an array index maintainers objects from MetaDataEndpointImpl. This retrieval operation has to wait for the ongoing two-phase commit transaction to complete.

## Read Path Design

This solution described so far eliminates the RPC call from a Phoenix client to a server in the write path, and the index table mutation transfer from a source to destination cluster in the replication path.  Now we can extend this solution for the read path.

For every query including point lookup queries, the Phoenix client currently serializes an IndexMaintainer object, and attaches it to the scan object as a scan attribute, and then Phoenix coprocessors deserialize it from the scan object. To eliminate this serialization/deserialization and save the network bandwidth for IndexMaintainer, the Phoenix client can pass the cache key for the array of IndexMaintainer objects and the name of the index (instead of the IndexMaintainer object), and the coprocessor can retrieve the the array of IndexMaintainer objects from its server cache and identifies the one for the given index. If the array of IndexMaintainer objects is not in the cache, the coprocessor using the Phoenix client library can construct the array of IndexMaintainer objects and populate the server cache with it.

## Cache Efficiency 

A mutation or batch of mutations on a data table requires updating all the indexes on that data table. For the cache to be efficient, we need to have the IndexMaintainer objects for all of these indexes. This is the reason this design chooses to cache the array of IndexMaintainer objects (for a given table or view) instead of caching individual IndexMaintainer objects. 

## Availability Impact

This design achieves cache coherency which impacts the availability of the metadata operations. For a metadata operation to succeed, MetaDataEndpointImpl should be able to invalidate the index maintainer caches on the region servers first so that Phoenix coprocessors would need to retrieve the most recent metadata from MetaDataEndpointImpl when they need it. Depending on how table regions are distributed over region servers, for a given metadata a subset or all of the server caches may need to be invalidated. 

It is important to note that the general cluster availability is not impacted significantly as the metadata operations are rare compared to the read/write operations on the user data in terms of number of operations or frequency of operations.

## Switch from old design to new design

Deployment from old design to new design should support both a) direct index table replication from source to destination as well as b) index table replication as a result of data table replication as per the new design. Until both source and destination clusters are upgraded to the new design, we cannot eliminate the old design of individual index table replications.

TODO: rollback support?  
