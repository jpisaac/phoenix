# SeaS Document Store on Phoenix

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)  
Updated: Jun 2, 2022

Search as a Service (SeaS) indexes entities stored in the core app DB using Solr. For this reason, SeaS uses the core app DB as the source of truth and thus needs to retrieve documents from the core app DB when they are created and updated. As outlined in [the doc capturing requirements and meeting notes on this](https://salesforce.quip.com/62O1AzfEggGr), additionally SeaS needs to retrieve documents from the core app DB to create duplicate indexes for scenarios like testing and rolling out a new version of Solr, experimenting with different data partitioning or non-backwards compatible schema changes, or to recover failed Solr nodes.

Fetching documents and their incremental updates from the core app DB only once and storing them in a separate scalable and highly available store for further access is highly desirable to reduce the load on the core app DB. Having such a document store would also simplify the document ingestion pipeline.   

# Reference Architecture

The high level architecture for using Phoenix as a SeaS document store is illustrated in the following diagram where Phoenix is used as both a staging area in the document ingestion pipeline and long term document store to update and retrieve documents for various reasons. In the diagram, DB and CDC stand for database and change data capture. 

This architecture suggests that documents are copied to Phoenix at day zero. While this copy is in progress, the updates on these documents are briefly stored on a second table on Phoenix. When the day zero copy operation is completed, the buffered updates are applied to the primary table on Phoenix on top of the day zero copy. When the buffered updates are drained, the incremental changes are directly applied to the primary table.  

In the diagram above, the black arrows represent data flow and the red lines show the control path where SeaS coordinates the activities of various components. The architecture described here is a high level reference architecture. It is assumed while the actual architecture may differ from it, the requirements on Phoenix would not change significantly.

# Requirements

1. Search should be able to store documents on Phoenix on a table referred as the document table in the rest of the document.   
2. The document table should be mutable and support atomic and conditional updates on rows, more specifically ON DUPLICATE KEY clauses with the UPSERT statements.  
3. The documents (and the rows of the document table) will be identified uniquely (for example, using org id and document id). Phoenix should provide about 10ms latency to retrieve or update individual rows using their unique keys.  
4. Phoenix should allow rows to be queried (i.e., scanned) in the order of their modification time (i.e., support a form of change data capture, CDC). The modification time is initialized to the server wall clock time when the row is stored on Phoenix for the first time and updated each time the row is updated. The retrieval time for individual rows should be around 10ms when they are retrieved using modification time range queries.   
5. If a row is updated partially or completely, the Phoenix CDC query should return the latest version of the row when the query is initiated, not the history of updates on the row. If the same row is updated after the start of the CDC query, the updated version may also be included in the result set of the same query.  
6. A Phoenix cluster should be able to roughly provide about 2GB per minute per node document ingestion throughput and 6GB per minute per node document retrieval throughput where the node is a region server with m5.8x VM instance. The cluster performance should scale roughly linearly with the number of region servers in the cluster.  
7. The number of concurrent ingestion and retrieval streams should be in tens per region server and should scale linearly with the number of region servers in a Phoenix cluster.

# Background

Recently there have been significant enhancements made to Phoenix that allows Phoenix to be a suitable candidate for being a document store. These enhancements are as follows:

* [PHOENIX-5629 Phoenix Function to Return HBase row timestamp](https://issues.apache.org/jira/browse/PHOENIX-5629)  
* [PHOENIX-6387 Conditional updates on tables with indexes](https://issues.apache.org/jira/browse/PHOENIX-6387)  
* [PHOENIX-6434 Secondary Indexes on PHOENIX\_ROW\_TIMESTAMP()](https://issues.apache.org/jira/browse/PHOENIX-6434)  
* [PHOENIX-6458 Using global indexes for queries with uncovered columns](https://issues.apache.org/jira/browse/PHOENIX-6458)  
* [PHOENIX-6501 Use batching when joining data table rows with uncovered global index rows](https://issues.apache.org/jira/browse/PHOENIX-6501)  
* [PHOENIX-6663 Use batching when joining data table rows with uncovered local index rows](https://issues.apache.org/jira/browse/PHOENIX-6663)

By using PHOENIX\_ROW\_TIMESTAMP(), it is possible to get the row timestamp (the last modification time) of a row. PHOENIX\_ROW\_TIMESTAMP() can be included in the select clause to include row timestamps in the result of a query or in the where clause to have time range queries.  Please note that the schema of the table does not need to include an expression with PHOENIX\_ROW\_TIMESTAMP(). 

One can also create secondary indexes using PHOENIX\_ROW\_TIMESTAMP(). This allows efficient time range queries or implement a form of CDC stream to retrieve the rows in the order of their modification time. 

Phoenix historically did not have efficient uncovered secondary indexes. This is because the columns that are not covered by an index had to be retrieved from the data table row by row on the client side by leveraging the Phoenix join compiler. This is the main reason why most of the secondary indexes in production are covered to prevent this client side expensive join operation. In addition, this implementation was suitable only for queries that returned a small number of rows due to scalability limits of the implementation.

Enhancements around uncovered indexes listed above have addressed this performance and scalability issues by implementing the join operations on the server without using the join compiler. Essentially, the query on the data table is rewritten to be performed on the index table. And on the server side while the index is scanned, the uncovered columns are retrieved from the data table regions in parallel and multiple rows at a time to reduce the query latencies and use resources more efficiently. 

Another key enhancement was to extend the support for atomic conditional updates to tables with indexes. Conditional and atomic updates are now supported on tables with one or more indexes. Applications such as SeaS can be highly concurrent and distributed and may need to update the same row concurrently but still ensure a deterministic result. This is achieved by using conditional and atomic updates where an update on a row is made only when the specified condition is satisfied. The check for the condition and the update on the row are parts of one transition that is performed atomically. 

These enhancements are the foundations for the proposal for using Phoenix as a document store for SeaS.

# Design Details

It is assumed that a multi-tenant data table and an uncovered global secondary index table on row timestamp will be used for SeaS to store documents. The primary key columns of the data table will include org id and document id (and possibly some other additional columns). While the primary key of the data table is used for fast access documents using their document id, the index is used to provide fast access based on last modification time. Phoenix provides strongly consistent indexes which means the data table and its indexes are updated transactionally. 

Since the prefix of the global secondary index is org id and row timestamp, the updates on an index for a given org will be monotonically increasing. In general, monotonically increasing keys creates hot spotting on region servers. However, since the index will be uncovered, thus the row size of the index table will be a fraction of the row size of the data table, the index table regions should not be the reason for hot spotting. However, if the data table (i.e., document table) row key is monotonically increasing then salting can be enabled to prevent any hot spotting. 

It is assumed that the documents for a given org will be copied from the core app database to the document table on Phoenix directly. While this copy is happening, the updates to the rows of this table cannot be directly made to this table as this will change the order of updates for a given row. Therefore, these updates will be made to another data table, secondary data table, whose row key will be org id and doc id. When the copy operation completes, the updates for this org will be paused until the rows from the secondary data table are transferred to the primary data table (the document table).

As shown in the architecture diagram, SeaS using its Phoenix CDC component will scan the document table to retrieve the rows in the order of their modification time using a SQL query such as SELECT \* from DOC\_TABLE where PHOENIX\_ROW\_TIMESTAMP() \> TO\_TIMESTAMP( \<last timestamp\>) and ORG\_ID \> \<last org id\> and DOC\_ID \> \<last doc id\> When a set of the row is retrieved and indexed in Solr,  the row timestamp, org id, doc id of the last row scanned can be recorded in a separate Phoenix table. Let us call this table OFFSET\_TABLE. The row of this table can be composed of org id, doc id, and row timestamp where org id is the primary key of this table. Seas can use this table to identify the starting offset of the next CDC stream for a given org id. Please note that it is also possible to retrieve documents for a set of org ids in a single CDC. In his case, the documents will appear in the stream in the order of their org id and modification time. In other words, the documents will be grouped by org id in the stream but still ordered based on the modification time within a group.

When a document is required to be deleted, it is logically deleted from the document table. This logical delete is achieved using a status column which indicates if the row is logically deleted. SeaS can check this status column and delete the logically deleted documents from Solr. After a document is deleted from Solr, it can be physically deleted from Phoenix too.

# Next Steps

Phoenix can ingest high volumes of data with very high ingestion throughput even when the data is streamed into Phoenix row by row (please see [a recent PoC on this](https://salesforce.quip.com/TFSKAChoxzam)). 

SeaS requires Phoenix to maintain highly concurrent ingestion and retrieval streams on a table with indexes with high throughput. The throughput and latency numbers suggested in this document as requirements are based on known performance characteristics of Phoenix from earlier performance studies. 

After verifying the reference architecture and the requirements listed in this document, the next step can be that the Phoenix and SeaS team collaborate to identify how such a document store is deployed in Falcon and design and conduct a PoC to verify the scalability and performance attribute of the document store suggested here. 