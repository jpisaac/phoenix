# **Phoenix Annotations for HBase WALs**

Created by Geoffrey Jacoby in October, 2020 to discuss the design and implementation of PHOENIX-5435 and how it fits into our larger efforts towards Change Data Capture (CDC) and Phoenix-level Replication.

**Background**

HBase, like BigTable which preceded and inspired it, has a relatively simple data model. Each row in a table has one or more Cells. Each Cell shares the same row key (an untyped byte array), but can have different columns (grouped into column families) and values (also an untyped byte array). The only kinds of fixed schema are for namespaces, tables, and column families.

Phoenix, on the other hand, adds a much richer (and more complex) metadata model on top of HBase. These include:

* Secondary indexes, which can be either global (stored in a separate HBase table) or local (stored in a special column family in the same HBase table)  
* Multi-tenancy support (rows can be owned by a particular tenant, and tenant connections can only see rows owned by its tenant)  
* Views, including views owned and only visible to a particular tenant  
* Secondary indexes on views. All view indexes for a physical HBase table are co-located in the same index HBase table, indicated by the naming convention \_IDX\_ \+ the base HBase table name  
* Fixed SQL-like column schemas with rich type support, such as string, numeric, and date types

To make HBase mutations (either Puts, Deletes, Appends, or Increments) resilient to server failures, HBase writes to a write-ahead-log (WAL) before making changes visible to end users. WALs can be replayed if the server crashes, and also form the basis for HBase’s replication service.

However, the HBase WALs only contain information about the simpler, HBase-level metadata, such as what table was written to and what row, column, and byte array value a particular Cell contained. All of the Phoenix metadata is lost.

This makes it nearly impossible to write a Phoenix-aware replication service, or CDC stream, because there’s not enough information to “replay” the original Phoenix logic. For example, one can’t take a mutation to a base table in the WAL, and generate any necessary index updates, because the same physical HBase table can have multiple logical Phoenix views stored in it.

**Functional Requirements**  
The only functional requirement is that Phoenix annotate sufficient information into the WAL to be able to lookup a unique schema in the schema registry that corresponds to the Phoenix object that created the WAL entry. 

**High Level Solution**

One simple way to solve this mismatch between Phoenix schema and HBase WALs is to annotate each entry in the WALs with sufficient Phoenix metadata information. (Each WAL.Entry contains a WALKey containing metadata and a WALEdit containing the data Cells)

The following information will be annotated. More annotations can be added in the future. 

* Schema / namespace name  
* Phoenix table / view name  
* Table type (e.g TABLE, INDEX, or VIEW). Uses the string version of PTableType.   
* Timestamp the table metadata was last modified (need to get from PTable \-- see pre-reqs below)  
* TenantId (if any)

The critical pieces here are two HBase JIRAs in HBase 1.5 and HBase 2.3: HBASE-22622 and HBASE-22623. The former adds a map of string→ byte\[\] annotation pairs to the WALKey interface, and the latter provides a coprocessor hook, preWALAppend, which gives a coprocessor the opportunity to add annotations before the WAL is written to disk.

Since Phoenix’s server-side logic is implemented as HBase coprocessors, in theory it’s straightforward to have Phoenix add the metadata using this new coprocessor hook and WALKey API.

Strictly speaking, we only *need* to annotate writes to a base table or view, not indexes, because indexes can always be regenerated from the base write. However, it’s good to annotate indexes where we can, to make it easy to distinguish between “unannotated because the client doesn’t support annotation” and “an index write”. Hence, this design will annotate global indexes. (Local indexes, which are often-but-not-always mixed in the same WAL entry as the base data, can’t be consistently annotated, so we don't try to do so.)

**Obstacles**

In practice, the solution is complex, because of the variety of ways that Phoenix’s write path logic works. Some complications:

* Phoenix supports HBase 1.3-1.6 and HBase 2.1 \- 2.3, but the two annotation JIRAs are only present in 1.5+ and 2.3+. Therefore all of the annotation logic needs to use Phoenix’s compatibility APIs to detect when it’s compiling against an old HBase, and use no-op APIs instead. Tests have to no-op rather than fail if run on the run version.   
* The new, consistent global secondary indexes require three physical HBase writes for one logical Phoenix write — one for the data table and two for each index. (One index write goes before the datatable write but is marked “unverified”, or tentative, and the second index write goes after the data table write and marks the row as “verified”, or committed.)  
* The Phoenix write path works very differently depending on whether a table is marked as immutable (only whole-row inserts, whole-row deletes, and TTL expirations are allowed) or mutable.  
* The WALs are appended to on server-side in the regionserver, but full details about Phoenix metadata are mostly handled on the client-side, during query parsing and optimization.  
* The Schema Registry will only support lookups based on DDL creation/alter time, not DML mutation time. This means that we also need an efficient way to get DDL time of a table/view at query time

**Prerequisite**

Before PHOENIX-5435 can be completed, we will first need PHOENIX-6186, which adds a last modified timestamp to System.Catalog that is updated when creating a table or view, and when adding/removing a column. 

**Immutable Write Path \- How it works now**

(diagram source: Kadir Ozedmir, Design doc for PHOENIX-5156)

As the diagram above shows the Phoenix write path for mutable tables with indexes works as follows:

0\. An application sends a query to the Phoenix client. This client could be "thick" (in the same JVM) or "thin" (a remote Phoenix Query Service).   
1-2. The client translates the query into a series of HBase mutations (either Put or Delete). In some cases, such as an UPSERT SELECT or a range DELETE, this requires performing a SELECT against a data table or an index in order to return to the client the data to be upserted or deleted  
3\. The client writes any index update, marking those rows as "unverified" (and hence invisible to readers)  
4\. The client writes to the target data table being upserted or deleted.  
5\. The client writes to any updated index rows from Step 3 marking them as "verified", and hence visible to reads. 

**What's Changing?**

In the immutable case, almost all of the logic takes place on the client side. Therefore, in order to annotate the WAL, we first have to annotate the mutations on the client side so we can read them on the server-side. These annotations use the existing "attribute" map on each HBase mutation. 

The information for the annotations come from the query itself or the PTable loaded as part of query planning. 

Since all the mutations in the same HBase batch mutation call will go into the same WAL.Entry, in theory we only need to annotate the first mutation in the batch. However, the same Phoenix Connection.commit() call can create multiple batch mutations to multiple region servers, so for simplicity's sake we annotate each mutation. (In the future we can consider optimizations that will only annotate one per batch mutation call.) 

The annotations are applied in the MutationState class, during its send() operation that occurs after a connection commit.

On the server side of the *base table*, in the IndexRegionObserver coprocessor (which exists regardless of whether a table has an index or not), the new preWALAppend() method reads the attributes we care about and annotates the WALKey with them. 

On the server side of the *index table*, we don't have an IndexRegionObserver, but we have added in similar logic to the preWALAppend method of the GlobalIndexChecker, a coprocessor that all indexes using the new global index framework have. 

**Mutable Write Path \- How it works now**

(modified from diagram by Kadir Ozdemir, Design doc for PHOENIX-5156)

As the diagram above shows the Phoenix write path for mutable tables with indexes works as follows:

0\. Mutations for the data table are generated client-side in much the same way as they are for immutable tables in immutable steps 0-2. 

0.5 Optionally, UPSERT SELECTs and range-scan DELETEs can be done purely server-side. This requires that they not have global indexes, that the UPSERT/DELETE and SELECT be from the same table, that auto-commit is set, and that a config flag is set. These mutations are generated in the UngroupedAggregateRegionObserver by opening up an HBase Scan from the client to the region server using special Scan properties. The UARO then sends mutations through the HBase batch mutation API to the IndexRegionObserver.

1\. Mutations for the index are generated on the server side in IndexRegionObserver. Index mutations are never generated on the client-side for mutable tables. Part of the payload sent to the server is one or more IndexMaintainers, containing metadata about each index. 

2\. Index mutations are sent to the index regions in "unverified" or tentative state, where they're written to both the index region server's WAL and then MemStore. Before PHOENIX-5435, the GlobalIndexChecker did not do anything on the write path. 

3\. The data mutations are committed to the WAL of the data table region's region server

4\. And then they're committed to the MemStore and, sometime later on the next flush, the HFiles of the data table's region. 

5\. The index rows are updated to be in "verified" or committed state. This also gets written to both the WAL and MemStore/HFiles using the normal HBase batch mutation write path.

**What's Changing?**

Mutations generated on the client side (such as UPSERT VALUES, or UPSERT SELECTs done on client side) will work similarly to the immutable case above. That is, they'll put attributes on the mutations client-side, which will be read in IndexRegionObserver's preWALAppend hook and written to the WAL. 

Index mutations generated on the server side in IndexRegionObserver will also have to put attributes on the mutations, so that the GlobalIndexChecker's preWALAppend hook can annotate the index region server's wall appropriately. Most of the required information was already present on the server-side, with one exception. 

That exception is for view indexes, where the physical table doesn't correspond to the logical table. To work around this, we have added a LogicalTableName field to the IndexMaintainer protobuf which is serialized along with the data mutations. Where present, this will be used when generating the annotations for index mutations in IndexRegionObserver.

Likewise, the DDL timestamp for indexes will also need to be put in the IndexMaintainer. 

For UPSERT SELECTs and range DELETEs that get processed on the server side, the  UpsertCompiler/DeleteCompiler on the client side will put metadata information on the Scan that triggers the UngroupedAggregateRegionObserver. The UARO will then use this information when it generates the data table mutations 

For index builds/rebuilds/read repairs, the special Scan that triggers the rebuild in the UngroupedAggregateRegionObserver will be supplemented with metadata properties, similarly to how the server-side UPSET SELECTs worked above. 