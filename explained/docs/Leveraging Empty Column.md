# Leveraging Empty Column for

# Phoenix TTL and Index Replication

This document describes how Empty Column can be leveraged to greatly improve the efficiency and scalability of Phoenix TTL and completely eliminate replicating index tables.

# Background

HBase is a key-value store. A key-value pair represented by the cell concept (Cell)  in HBase. An HBase table can be viewed as a set of cells where each cell is identified by a key, which is composed of a row key, family name, column qualifier, and timestamp. A cell holds a value expressed using a byte array. A row is identified with a row key and is the set of cells with this row key. Rows in an HBase table are sorted by the row key of the table. 

Phoenix is a relational database with a SQL interface, and uses HBase as its backing store. Phoenix tables are backed by HBase tables, and the primary key of a Phoenix table maps to the row key of the underlying HBase table. This means that the PK columns of a Phoenix table are  packed in the row key of the underlying HBase table. A Phoenix table may not have any non-PK column when it is created or may start with some non-PK column and these columns can be dropped over time. This means that Phoenix should be able to support tables with no non-PK columns. However, to form a cell, Phoenix needs to have a non-PK column that is stored on every row. This is the motivation for having an internal column included in every table that is not visible to users. This column is called the empty column. Phoenix includes the empty column cell in every mutation on a given table.

Initially empty column values are ignored and a single byte placeholder value “x” is used. Leveraging empty columns for other purposes started with the redesign of Phoenix global secondary indexes where the value “x” is replaced with the row status “1” and “2” for index tables where “1” means verified and “2” means unverified row. This row status is used for implementing two-phase commit writes on index tables.

Later the design of Phoenix TTL used the empty column cell timestamp as the last modified time for table rows since a given row mutation has the same timestamp for every cell included in the mutation. 

Finally, the online data format change feature also leveraged empty columns to store row status for data table rows in order to implement a two phase-commit write over the current table and the new table with the new data format. 

The proposal for further leveraging empty columns for improving Phoenix TTL design and eliminating index table replications. In the rest of the document, we will describe the issues with the current Phoenix TTL design and replicating indexes and how empty column cells can be leveraged to address them.

# Problem Statement

## Phoenix TTL

The current design of Phoenix TTL requires adding delete markers for expired rows. This requires scanning table rows periodically and inserting delete markers. This is very expensive compared to HBase TTL which does simply skip expired rows during compaction. In other words, HBase TTL does not add any significant overhead while Phoenix TTL needs to run expensive MR jobs to detect expired rows and insert delete markers for them. In addition to the cost of running these MR jobs, the inserted delete markers also impact the performance of scan operation in general. The rows that are deleted by these delete markers still need to be scanned until they are compacted.

## Index Replication

The issues related to index replication are described in [here](https://docs.google.com/document/u/0/d/1HRLJ9WW8TDDpGnAklnH-dXFeq7AWGb5Zrk0LUwn12C4/edit).

# High Level Design

The data for an empty column has been so far limited to a single byte value. This proposal replaces it with a structure called EmptyColumn. This structure will be more than one byte. This structure will include the verify byte that has been used to implement the two-phase commit writes and a fixed size value to identify a table, i.e., table id. The table id will be an internal id that uniquely identifies a table, view, index or view index. Having the table id in an empty column cell will allow Phoenix coprocessors to map a row mutation to its table which can be a view, view index, data table or index table. To map a table id to the tuple of tenant id, schema and name, a new system table called SYSTEM.TABLE\_ID will be created. The unique id can be generated using Phoenix sequence but currently Phoenix sequences are not guaranteed to be unique globally. Since table ids do not need to be monotonically increasing, they can be generated using a UUID generator (for example, using java.util.UUID). 

Currently empty column cells are included in one of the column families of a table. For Phoenix TTL, we need to include the empty column cells in all column families.  

With these changes, we will be able to improve Phoenix TTL and eliminate index verification.

## Phoenix TTL Improvement

Phoenix TTL improvement will come from eliminating long running MR jobs to insert delete markers to purge expired rows. Instead, the PhoenixTTL coprocessor will implement the preCompact coprocessor hook to wrap the internal scanner that scans a store file with its own internal scanner to filter out (i.e., mask) expired rows. 

Since each row in a store file will include an empty column cell which includes the table id the row belongs to, the PhoenixTTL coprocessor will be able to fetch the TTL value for the table to determine if the row is expired. If so, it will skip the row instead of returning the caller, i.e., the HBase major compaction process.

The TTL values can be cached in the server cache of the region process using a consistent server side caching described [here](https://docs.google.com/document/u/0/d/1HRLJ9WW8TDDpGnAklnH-dXFeq7AWGb5Zrk0LUwn12C4/edit). 

## Eliminating Index Replication

Being able to get the table id directly from HBase table mutations simplifies the design described [here](https://docs.google.com/document/u/0/d/1HRLJ9WW8TDDpGnAklnH-dXFeq7AWGb5Zrk0LUwn12C4/edit) by removing the metadata and associated steps to discover the table or view from mutations. 

  

 

   
