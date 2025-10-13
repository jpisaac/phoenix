# Consistent Global Indexes for Non-Transactional Tables

Kadir Ozdemir \<[kadirozde@gmail.com](mailto:kadirozde@gmail.com)\>

Without transactional tables, the global indexes can get easily out of sync with their data tables in Phoenix. Transactional tables require a separate transaction manager, have some restrictions and performance penalties, and are still in beta. This proposal lays out a design to have consistent global indexes without the need for using transactional tables.

# Objectives

1. The global indexes should be always in sync with their data table.  
2. Satisfying objective 1 should not impact the write or read performance on the data table significantly.  
3. The code changes to implement this proposal should not require rewrites of existing Phoenix modules.  
4. Consistent indexes should result in operational simplification by virtually eliminating index rebuilds.

# Background

In HBase, every HFile has an index where row keys are sorted lexicographically. Each column family of an HBase table has its own set of HFiles. Using row keys allows accessing rows of a table quickly with a very small number of disk IOs. A Phoenix table maps to an HBase table, and thus the primary key of a Phoenix table maps to the row key of the corresponding HBase table. Phoenix directly leverages HBase row key indexing to provide primary key indexing. However, accessing a Phoenix table through a non-primary-key column requires scanning the entire table without secondary indexing on this column. There are two types of indexing in Phoenix, global and local indexing. Global indexing uses a separate table for each secondary index of a table whereas local indexing uses a separate column family within the same data table to implement all secondary indexes. 

A global index sorts the entire set of the rows of its data table based on the column, or the set of columns (i.e., secondary key)  associated with the index.  A local index does not sort the rows of the entire data table based on the secondary key. It sorts the rows with a table region. This implies that the range of the secondary key for a table region can overlap with the range for another table region. Since HBase maintains only primary key ranges for table regions in its metadata, if a query is not selective for a primary key range, then all the table regions must be looked up for a given query. However, when the primary key column values are provided within a query, the table regions to be searched can be pruned and only the required table regions are searched for the query. Without such pruning, local indexes may not provide as good read performance as global indexes do. For such pruning to be effective in the current Phoenix implementation, the primary key and the secondary key must have a common prefix (which is used for pruning). 

A local index in Phoenix is self consistent since updating a table row and the index rows happens within the same table region (and thus within the same region server). However, updating a table with one or more global indexes requires updating multiple table regions, likely distributed over multiple region servers. Translating a single table update operation into a multi-table write operation poses consistency issues as Phoenix does not provide a reliable multi-table update capability without using transactional tables. Transactional tables are still in beta, require a separate transaction manager, and have some restrictions and performance penalties.

In the current implementation, mutable global index updates for a given data table are done on the server side within the Indexer coprocessor after the data table updates are completed. Indexer goes through the data table mutations included in a batch of table row mutations, prepares the corresponding mutations for index tables before applying the batch on the table, and then applies these mutations on the index tables after the data table is updated. 

The index mutations are likely to be done remotely as the index table regions will likely be on other region servers. An index table update can fail for several reasons such as RPC timeouts due to slow network and/or busy region server, region server failures, disk failures, and so on. A recently resolved JIRA (PHOENIX-4130) employs a best effort approach to recover from these failures such that if any of these index writes fails, the index failure exception is returned to the Phoenix client, and the client attempts to replay the mutations on the data table with the mutation attribute “INDEX\_ONLY” to request the Indexer coprocessor to apply these data mutations on the failed index tables without updating data table. However, this does not guarantee that these attempts will be successful. Also, there is no guarantee that all the index failures will be detected as cascading failures can happen and so both the client and server can happen without recovering from the index failures. Therefore, index tables can have missing updates and can be out of sync with their data tables.

For immutable global indexes, the index mutations are prepared on the client side and these mutations and the data table mutations are sent to the corresponding region servers in parallel. This implies that there is no deterministic order in which index table and data table mutations are applied. Similarly, index or data table write failures can leave index tables in inconsistent state.

# Design

Updating multiple tables (in our case, a data table and its indexes) atomically requires implementing a form of  two-phase commit protocol, a transaction capability. Two-phase commit protocols are known to be expensive.  Also implementing a two-phase commit capability within the secondary indexing data path requires extensive changes on the current indexing implementation.

In the general transactional update problem, there is no special relationship among the updates to be made over multiple tables within a transaction. In other words, one cannot derive the update for a table from the updates for the other tables within the same transaction. Therefore, usually the content of a transaction has to be logged on a durable media before committed to the individual tables in order to be able to recover from failures.

Achieving self-consistent global indexing does not really require implementing a general purpose transaction capability. The reason for this is that the update for an index table can always be extracted from the update for the data table. This means if a global index is corrupted, lost or becomes inconsistent with its data table, then it can be rebuilt from the data table. This observation allows us to come up with a self-consistent global indexing solution that leverages this property and is optimized for the secondary indexing problem.

Another important observation is that HBase is a log-structured data store, that is, updates are never done in place. In these systems, writes are much faster when compared to in-place update systems because random writes are handled as fast as sequential writes. This allows us to add an extra write phase during updates without severely impacting the write performance, which simplifies the overall design.

The proposed design is significantly different from the current design since the proposed design has an extra write phase, changes the order of operations and maintains per row status on index tables. It updates a data table and its index tables using a three phase write approach. In the first phase, the index table rows are updated with the “unverified” status in parallel. The verify status is a per-row-status and stored in a column of every global index table for non-transactional tables. If updating any of the index tables for a given data table fails after a number of retries, a write failure is returned to the application. This means that some of the index tables are updated with unverified rows. However, this does not pose a consistency problem since data from unverified rows are never used for serving reads (i.e., SQL queries). These rows will be rebuilt from the data table or they will be deleted if the corresponding data table rows do not exist during read operations (i.e., using a read-repair technique). 

In the second phase which happens if the first phase is successful, the data table is updated. If the data table update fails, then a write failure is returned to the application. Again, since the rows written to the index tables in the first phase are still unverified and data from unverified rows are never used, data table write failures (in the second write phase) do not lead to correctness issues as in the case of index table write failures in the first write phase.

In the third phase, which happens if the second phase is successful, the index table rows are updated with the “verified” status or deleted. A failure during the third phase is simply ignored as such failures are recovered during the read operations on the index tables. After the third write phase,  the completion status for this batch of writes is returned to the application.The third phase of the write can be done lazily, i.e., asynchronously. In the rest of the document, it is assumed that the third phase is done synchronously if not stated otherwise.

Phoenix maintains a shadow column for every data and index table. A predefined value (that is “x”) is stored in this column for every row in a table. Phoenix uses the existence of this column but does not care about its value. This column is referred to as the empty column within the Phoenix code.  The verify status/flag is stored in the existing empty column. The column that holds the verify flag will be called the verify column in this document regardless if the index is mutable or immutable. 

As before, before a write operation is applied on a mutable global index table, the index row corresponding to the previous version of the row to be updated by the write operation needs to be deleted from the index table, and a new full row index row needs to be constructed for the write operation. Again as before, if the values for some covered columns are not included in the write mutation, they are read from the data table to construct the full index row. This implementation is leveraged for preparing index updates, i.e., constructing and  deleting index rows.

To delete a row from a data table, the verify status is set to false first on the index tables for the row in the first write phase. Then the row is deleted from the data table in the second write phase. After deleting the row from the data table, the row is deleted from index tables in the third write phase. Similarly, if an index row is deleted because of an overwrite on the data table (as described above), the index row is marked unverified in the first write phase and deleted in the third write phase.

In summary,  this proposal will introduce a new coprocessor, called IndexRegionObserver,  to replace the existing coprocessor Indexer for mutable global index writes on the server side, which will change the order of operations and add an additional step. In the current implementation, Indexer has the following steps:

1. Lock the rows to be updated on the data table  
2. Get the current timestamp and use it as the timestamp for data and index table rows  
3. Prepare mutations for the index tables  
4. Update the data table  
5. Unlock the rows  
6. Update the index tables in parallel  
7. Return write completion status

This proposed design suggests the following behavior for IndexRegionObserver:

1. Lock the rows to be updated on the data table  
2. Get the current the current system millisecond and use it as the timestamp for data and index table rows  
3. Prepare mutations for the index tables  
4. Unlock the rows  
5. The first write phase : Update the index tables in parallel, where rows are updated with unverified status  
6. Lock the rows to be updated on the data table  
7. The second write phase : Update the data table  
8. Unlock the rows  
9. The third write phase : Update index tables in parallel to change the verify status to verified and/or delete index rows  
10. Return write completion status

HBase only has millisecond time resolution currently. This means that if the index update preparation takes less than 1ms, concurrent updates on the same row may use the same timestamp.  Since the row locks are not held during index updates (step 5), the order in which the updates happen on the data table can be different than that on an index table for a given row that is concurrently updated. Since these updates can use the same timestamp, HBase cannot order them correctly. This will lead to inconsistencies between a data table and its index tables. To prevent this, if the current millisecond after preparing the index updates for a given batch is the same as the millisecond just before the index update preparation for this batch, the thread preparing the index updates sleeps for 1ms before releasing the row locks so that the next batch of updates does not get the same timestamp. 

Another issue is that if there are concurrent mutations on the same row, IndexRegionObserver may read the same previous state of the row for these mutations. This can happen because the row lock is released after reading the row and preparing the index updates. Then, the row is locked again to update the data table row. If another mutation for the same row arrives between these two row lock operations, then IndexRegionObserver will retrieve the same row state for this mutation too. This means the second mutation will prepare an index mutation without knowing about the first update.  Let {1, a, x, y} be a row in the data table. The corresponding row in the index table would be { {a, 1}, y}. Now, let the same data table row be mutated and the new state of the row be {1, b, x, y}. The row {{a, 1}, y} is not valid any more in the index table and needs to be deleted. Thus, the prepared index mutations will include the delete row mutation for the row key {a, 1} and a put mutation, that is, put {row key \= {b, 1}, c3 \= y} for the new row.  Let {1, c, x, y} be another mutation on the same row that arrives before the first mutation updates the data table. This means that the prepared index mutations will include the delete row mutation for the row key {a, 1} and a put mutation, that is, put {row key \= {c, 1}, c3 \= y} for the new row. However, the second mutation should have deleted index row {b, 1} instead of {a, 1}. To prevent this, IndexRegionObserver will maintain a collection of data table row keys for each pending data table row update in order to detect concurrent updates, and will skip the third write phase (i.e., step 9\) for them. The read-repair operation on these rows will lead to proper resolution of these concurrent updates. 

Indexer uses a NonTxIndexBuilder which then uses an implementer of IndexCodec, i.e., PhoenixIndexCodec for uncovered global indexes or CoveredColumnIndexCodec for covered global indexes to prepare index updates. IndexRegionObserver does not require any change to this aspect of the current design. 

While reading from a row of the index table, a new region observer coprocessor called GlobalIndexChecker, will check if the verify status is true for the row. If so, the data can be returned to the application. If the verify status is false, then the index row is rebuilt using the existing index rebuild code (i.e., using UngrouppedAggregateRegionObserver and Index coprocessors). If there is no such a row in the data table or the index row status is still unverified after the rebuild, then the index row is deleted  (if the age of the index row is old enough based on a configured age threshold).

For immutable global indexes, since rows are immutable, that is, a row is never overwritten, there is no need to read the previous state of a given row or delete an existing row from an index table due to overwrites. This property holds in the proposed design too.

Currently, the mutations for immutable global tables are prepared by the addRowMutations method of the MutationState class on the client side. This design continues using MutationState and prepares immutable global index mutations on the client side. However, instead of writing to the data table and index tables in parallel, as in the case for mutable indexes, the index tables are updated first with an unverified state, then the data table is updated and finally the index table verified flag is set to true. Again the last phase can be done lazily. Please note the verification and rebuild of the rows (when needed) of immutable tables are done by the GlobalIndexChecker coprocessor. 

The proposed design does not change how indexes are fully built or partially rebuilt as during index builds or rebuilds. As before, the data table is scanned, index rows are constructed, and index tables are updated using UngroupedAggregateRegionObserver and IndexRegionObserver (see [https://docs.google.com/document/d/1lOc5JsYaPAt7jFyKRzq0ZENjvtFIyReWYt4hW-ek9oc/edit?usp=sharing](https://docs.google.com/document/d/1lOc5JsYaPAt7jFyKRzq0ZENjvtFIyReWYt4hW-ek9oc/edit?usp=sharing)). During index full builds or partial rebuilds, we do not need to go through the two phase update to update an index row; the verify status is set to true when the row is constructed. 

## Mutable Global Index Design Details

In this section, we will have a closer look at the end-end operations for the mutable global indexes in the proposed design.

### Mutations (Upserts and Deletes)

The following figure illustrates how mutations will be handled on the data tables with global mutable indexes in the proposed design. Phoenix provides a JDBC interface to its application. The JDBC interface is implemented by the Phoenix client package. 

0) An application issues a batch of SQL upsert and/or delete operations on a JDBC connection and commits these operations. The Phoenix client translates these operations into corresponding HBase operations, i.e., puts and deletes. The HBase client turns these operations into batches of mutations based on table region boundaries, and sends each batch to the region server serving the table region corresponding to the batch.   
1) The preBatchMutate hook of the IndexRegionObserver coprocessor on one of these region servers acquires the locks for the rows in its batch.   
2) Then it gets the current system millisecond and uses it as the timestamp for data and index table rows. More specifically, the timestamp of the HBase cells in these row mutations will be set to this current system millisecond value. IndexRegionObserver maintains a collection of rows keys and timestamps, one entry for each pending mutation. This collection is used to identify concurrent mutations. IndexRegionObserver checks first which of the mutations on this batch are concurrent mutations and marks them as “concurrent” and then adds the row key and timestamp for each mutation in the batch to the collection of pending mutations.  
3) The next step is to read the previous states of these rows to prepare mutations for the index tables. Even if the values for all the columns of an index table are provided in the data table mutations, IndexRegionObserver still needs to read the previous data table row states to prepare the required mutations to delete the corresponding index table rows. Let the data table have the columns pk, c1, c2, c3 where pk is the primary key of the data table, and an index table have c1, pk and c3 where the index table is indexed on column c1 and thus c1 and pk forms the primary key for the index table. Let {1, a, x, y} be a row in the data table. The corresponding row in the index table would be { {a, 1}, y}. Now, let the same data table row be mutated and the new state of the row be {1, b, x, y}. The row {{a, 1}, y} is not valid any more in the index table and needs to be deleted. Thus, the prepared index mutations will include the delete row mutation for the row key {a, 1} and a put mutation, that is, put {row key \= {b, 1}, c3 \= y} for the new row.  The proposed design applies these index mutations in two phases, the first and third write phase. In this case, the mutations for the first write phase will be put {{a, 1}, verify \= “unverified”} and put {{b, 1}, c3 \= y, verify \= “unverified”} and the mutations for the third write phase delete {a, 1} and put {{b, 1}, verify \=  “verified”}. Please note that it is not safe to delete the existing index row in the first phase since the second phase can fail, and if so, it will not be able to recover from this without rebuilding the entire index.  
4) IndexRegionObserver checks if the current millisecond after preparing the index updates for a given batch is the same as the millisecond just before the index update preparation for this batch. If so, its thread for this batch sleeps for 1ms so that the next batch of updates does not get the same timestamp. Then, it releases the row locks.  
5) The first write phase : IndexRegionObserver updates index tables in parallel, where rows are updated with unverified status. If any of the index updates fails, then it fails the batch and returns a failure status back to the HBase client. This means the rest of the steps are not executed if this step fails. Please note that leaving rows in the unverified state is safe as the proposed design never returns unverified rows to clients. These rows either will be reconstructed or deleted during read (i.e., scan) operations.   
6) IndexRegionObserver locks the rows again and checks if any concurrent mutations arrived on the rows of this batch. If so, it skips post index updates (i.e., third write phase, step 9\) for the rows that have pending concurrent mutations.   
7) The second write phase : IndexRegionObserver updates the data table. If the update fails then row locks are released and a failure status is returned to the client. As in the first write phase, leaving rows in the unverified state is safe for the same reasons.  
8) IndexRegionObserver unlocks the rows.  
9) The third write phase : IndexRegionObserver updates index tables in parallel to change verify status to verified and/or delete index rows. It returns the success status to the client even if one or more of the index updates fails.

### Scans and Read Repair

The following figure illustrates how a scan operation is handled in the proposed design.

0) An application issues a SQL select operation on a JDBC connection. The Phoenix client translates this into an HBase scan operation. The HBase client turns this scan into a set of HBase scan operations based on table region boundaries and sends each scan to the region server serving the table region corresponding to the scan. The region scanner for this scan operation is wrapped by a scanner implemented by the GlobalIndexChecker coprocessor in the postScannerOpen hook. The sole purpose of this is to intercept individual row scans initiated by a Phoenix scan region observer and verify these rows and repair them when needed.  
1) A scan region observer starts calling the next operation on the GlobalIndexChecker scanner to scan rows one by one.  
2) GlobalIndexChecker calls the next operation on the wrapped region scanner and checks if the row is verified (by checking the verify status).  If the row is verified, the row is simply returned to the caller.   
3) If the row is not verified, then GlobalIndexChecker generates the data row key from the index row key and issues a scan operation on the data table to scan the data table row corresponding to the unverified index row. It sets the scan attributes UNGROUPED\_AGG and REBUILD\_INDEXES to true to inform UngroupedAggregateRegionObserver to rebuild the index table row using this scan. It starts the scan on the data table using the HBase client scan API.    
4) UngroupedAggregateRegionObserver intercepts this scan. It checks if the data row exists. If so, it forms the mutation to be replayed on the data table to rebuild the index rows. The REPLAY\_WRITES attribute is set to REPLAY\_ONLY\_INDEX\_WRITES for this mutation which means that the mutation will not be actually replayed on the data table but will be merely used to rebuild the corresponding index row. UngroupedAggregateRegionObserver creates a batch of data mutations and applies the batch on the data table using the HBase client API.   
5) The preBatchMutate hook of the IndexRegionObserver coprocessor on the region server prepares mutations for the index tables.  
6) IndexRegionObserver updates index table regions in parallel, where the rows are updated with verified status. Note these updates are done within a single phase.   
7) The mutation completion status is returned to UngroupedAggregateRegionObserver which returns the result of the scan operation to GlobalIndexChecker. It checks if the data row exists. If not, this unverified index row is skipped (i.e., not returned to the client), and it is deleted if it is old enough. The age check is necessary in order not to delete the index rows that are currently being updated. If the data row exists,it continues with the rest of the steps.  
8) The current scanner is closed as the newly rebuilt row will not be visible to the current scanner. If the data row does not point back to the unverified index row (i.e., the index row key generated from the data row does not match with the row key of the unverified index row), this unverified row is skipped and and it is deleted if it is old enough. A new scanner is opened starting from the index row after this unverified index row.  
9) If the data row points back to the unverified index row then, a new scanner is opened starting from the index row. The next row is scanned to check if it is verified. if it is verified, it is returned to the client. If not, then it means the data table row timestamp is lower than than the timestamp of the unverified index row, and the index row that has been rebuilt from the data table row is masked by this unverified row. This happens if the first phase updates (i.e., unverified index row updates) complete but the second phase updates (i.e., data table row updates) fail. There could be back to back such events so we need to scan older versions to retrieve the verified version that is masked by the unverified version(s).

## Immutable Global Index Design Details

In this section, we will have a closer look at the end-end operations for the immutable global indexes in the proposed design.

### Mutations (Upserts and Deletes)

0) An application issues a batch of SQL upsert and/or delete operations on a JDBC connection and commits these operations. The Phoenix client translates these operations into corresponding HBase operations, i.e., puts and deletes. The HBase client turns these operations into batches of mutations based on table region boundaries, and sends each batch to the region server serving the table region corresponding to the batch. The proposed design continues using MutationState and prepares immutable global index mutations on the client side.   
1) The Phoenix client gets the current system millisecond and uses it as the timestamp for data and index table rows. More specifically, the timestamp of the HBase cells in these row mutations will be set to this current system millisecond value.   
2) The Phoenix client prepares index mutations.  
3) The first write phase : The client updates index tables in parallel, where rows are updated with unverified status. If any of the index updates fails, then it fails the batch and returns a failure status back to the application This means the rest of the steps are not executed if this step fails.  
4) The second write phase : The client updates the data table. If the update fails then a failure status is returned to the application.  
5) The third write phase : The client updates index tables in parallel to change verify status to verified and/or delete index rows. It returns the success status to the application even if one or more of the index updates fails.

### Scans

A scan operation for immutable global indexes is handled in the same way it is handled for mutable global indexes (see the previous section on the mutable global indexes) in the proposed design. 

# Correctness

We can prove that a global index table will always return the correct data. We will prove this by exploring all possible cases. Since index row updates are always full row updates, we do not need to consider partial row updates.

We will first prove that the index table returns correct data under the assumption that there is only one pending update for a given data table row. This will be sufficient for proving the correctness of immutable indexes however we also need to consider multiple pending updates for a given data table for mutable indexes. There are three possible cases for an index table to return wrong data for a given row. These cases are as follows:

1. Missing row: An index table does not have a row even though its data table has the corresponding row. Assuming that index table is initialized correctly, there can be four ways to have a missing index row: (a) the index table update for the row is not attempted, (b) the row update is failed and (c) the row is deleted deleted because the corresponding data table row is deleted, and (d) the row is deleted because it is overwritten. We will show these cases are impossible to happen.  
   1. Given that the index table is updated first before that of the data table in strict order, having the row in the data table implies that the index table update has been attempted. Since the index table update is always attempted, either the update is successful, failed, or later deleted. If the update is successful, we cannot have the missing row.   
   2. If the index update is failed then the data table update will not be attempted and therefore, it is not possible to have a data table row but not the corresponding index row because of index update failures.   
   3. Since an index row is deleted only after the corresponding data table row is deleted, there cannot be a missing row because the data row deletes.   
   4. In order to overwrite an existing index row, the existing row is first unverified (by setting the verify status to false) in the first write phase and deleted in the third phase. This means that existing rows will be deleted only after the first two write phases are successful, i.e., only after the corresponding data table row is overridden too. So, an index cannot have a missing row because of overwrites.  
2. The verify status is false: If the verified status is false then the data is retrieved from the data table. If the data table has the corresponding row, then the index will return the correct data. If the data row does not exist, then the index returns no data, which is also the correct behavior.  
3. The verify status is true: Given that the verify status is true for a given row only after both the index and the data table updates are successful, the data returned from the index table will be correct when the verify status is true. 

Given the design is correct under the assumption that there is at most one pending update per row, we will prove that the index table still returns the correct value when this assumption is removed. Assume that there are N pending updates for a given row, where N \> 1\. The proposed design prepares index mutations and updates data table rows under row locks. The results of this locking is that regardless of the number of pending updates on a given row, preparing index updates and updating the data table rows will be serialized by the region server serving the corresponding table region.

Let B (Before) represent the data row state just before applying any of these updates. Assume that these updates arrive to a region server and acquire the locks in an arbitrary order. Let the state of the data row be A (After) after these updates are completed. Because the region server serializes these updates, the result of their execution will be equivalent to the result of executing these updates one at a time in the same order. 

The order in which data table rows are updated in the second write phase can be different from the order in which the index table rows are updated in the first phase. This is because the row locks are released before the index table updates. However, the timestamp for index rows will be the same as the timestamp for the corresponding data table updates. This will ensure that when all the index rows are updated the end result will be the expected result, i.e., the state of an index row will agree with the state of the corresponding data table row. 

Since the third write phase is executed after releasing the row locks, the concurrent third phase updates on the same index row can race. A third phase update is either for setting the verify status to true or for deleting the index row. The same timestamp is used for all three write phases for a given batch of writes. As in the second phase,  this will ensure that when all the index rows are updated the end result will be the expected result, i.e., the state of an index row will agree with the state of the corresponding data table row. 

# Deadlock Prevention

The current design releases Phoenix row locks and lets the index updates happen without holding the row locks. This is done in order to prevent cluster-wide deadlocks due to running out of RPC threads as all the RPC threads can be blocked due to row locks. The proposed design follows the same approach.

In the current implementation, the row locks are taken even for preparing index updates during index rebuilds. Actually, there is no need to acquire row locks for index rebuilds. As mentioned before, the UngrouppedAggregateRegionObserver coprocessor scans data table rows and replays these rows on the data table with “index only” mutation attributes. This means that these mutations are only for preparing index updates and they will not be replayed on the data table. The timestamps for these mutations are inherited from the data table. In other words, the timestamps of these mutations would be in the past. Since all the column values for index rows are included in the data table mutations, the only reason for data table reads is to retrieve the previous state of these rows. Since these rows are in the past, there is no need to acquire row locks for these rows. Therefore, the proposed design does not acquire locks during index rebuilds. This means that it will be deadlock free to initiate RPC calls from an index table region server to a region server of its data table to do read repairs, i.e., index rebuilds.

# Performance Impact

One of the objectives of this proposed design is not to impact the performance significantly. Under the assumption that the occurrence of failures will be not frequent enough to impact overall system performance, we will argue that this design does not change the read performance characteristics of the current global indexes. The basis for this argument is that the design does not add an additional message exchange or IO, or significant computation to impact the latency of read operations. 

When a scan is opened on a table region on a region server, a read point is set. You can think of this read point as a snapshot point (Please see one of Lars's blogs: [http://hadoop-hbase.blogspot.com/2012/03/acid-in-hbase.html](http://hadoop-hbase.blogspot.com/2012/03/acid-in-hbase.html)). The updates that happen after this point on that region server will not be visible to the scan. The unverified rows will be visible to the scan if these rows are committed to an index table region before the scan is opened on the region server for this region.

This means that the number of unverified rows to be scanned will be equal to the size of the intersection of the set of rows to be scanned and the set of rows that are unverified at the time the scan is opened. The rows that are being unverified while the scan is in progress would not contribute to the number. 

For mutable global indexes, the index updates are prepared by the region server serving the data table region that undergoes changes. Although the proposed design and its implementation is flexible to make the third write phase to happen asynchronously, the default mode will be synchronous and very likely that we will rarely need to make it asynchronous. We assume that the third phase is synchronous in this section. Regardless whether writes happen synchronously or asynchronously, they are done using blocking RPCs. 

In a regional server, there are a very limited number of RPC threads that are reserved for index updates. The default value for this number is 10\. A region server serves a number of table regions. The number of unverified rows will be limited by the total number of rows updated by these threads at a given time. Each thread processes one mini batch. Since all these table regions share the same thread pool, the expected number of active index updates for a given table will be even lower on average and bounded by this number. This means that if there are no index failures, the number of unverified rows per a given table region cannot be more than the number of index write threads times the maximum number of rows in a mini batch. Now, index failures will happen but the number of them will be insignificant compared to the number of successful writes, and will be repaired when it is detected during reads. Because of this, we can argue that read repair operation will not significantly impact the read performance of the proposed design.

During read operation, it will be checked if the verify column is true or false. However, this check will not incur a separate read operation as the scan will be updated on the client side to include the verify column. So, the index row verification should not impact the read performance significantly either. Since neither read-repair nor row verification would impact the read performance significantly, we conclude the overall read performance should not be impacted.

This design may slightly lower the maximum overall write performance of an HBase cluster for mutable global indexes due the additional updates at the third phase There are four phases where IO operations happen. Before the first write phase, index updates are prepared. During this preparation, IndexRegionObserver reads the current state on disk for the rows to be updated. This is done to retrieve the missing columns in the row mutations to be done and determine if the update requires deleting an existing row. If a secondary key column is updated by the current mutation then the row corresponding to the previous key needs to be deleted from the index table. Also, row locks need to be acquired before reading data table rows. In HBase, writes are faster than read operation in general (unless reads are served from the cache). Therefore, index update preparation should take longer than updating data or index tables.  Data table updates are done locally and index updates are done typically remotely. Even though data table updates are done locally and therefore it is expected to take shorter than index updates, the row locks need to be acquired before updating the data table. Therefore, roughly, we can assume that all four phases take the same amount of time to estimate the impact of the last phase. Going from 3 IO phases in the current design to four IO phases in the proposed design should increase write latency approximately 33%. This is very much inline with our initial performance tests on mutable indexes, which showed 20-25% in write latency.

In the current implementation of immutable global indexes, for each row mutation, two or more parallel RPC calls from an HBase client to the region servers are done, one for updating the data table and the others for the index tables. This proposal converts these parallel RPC calls to three serial RPC calls. This will increase the write latency and thus reduce write performance for immutable global indexes. There is no read or row lock operation for immutable indexes. This means that the write performance for immutable global indexes is much faster than that for mutable global indexes in the current implementation. Although the proposed design would increase the write latency about 200%, the write performance for immutable global indexes will be still higher than that for mutable global indexes in the proposed design. The read performance and latency should not be significantly impacted for immutable global indexes due to this proposal as explained before.

# Upgrade

This proposal does not require Phoenix DDL changes as the verify column (i.e., the empty column) is a shadow column and is not visible to Phoenix applications/users. Thus, the proposal does not require any upgrade related code changes. 

The design leverages the existing empty column. The empty column value has been set “x”. GlobalIndexChecker will check if the verify column (i.e., the empty column in this case) value is unverified. The  unverified value is represented by a one-byte value equal to 1\. So, The value “x” will be treated as the true value and the old rows will be treated as verified.  

The following steps are followed to upgrade a cluster to the release supporting this feature:

1. Upgrade the servers.  
2. Upgrade the clients.  
   

The new non-transactional data tables and their global indexes that are created after these steps will be configured with the new coprocessors and will be strongly consistent.  However, the existing tables continue to use the old design.

In order to upgrade existing data tables and their indexes to the new design, the GlobalIndexChecker coprocessor should be added to the existing global index tables, and the Indexer coprocessor should be replaced with the IndexRegionObserver on the existing data tables. This can be done table by table. For correctness, GlobalIndexChecker should be added first to index tables and then Indexer should be swapped with IndexRegionObserver on the data tables.  
Upgrade path is the same with global mutable and immutable indexes.

The list of coprocessors for given table can be updated using the HBase shell:

disable '\<Table Name\>'  
alter '\<Table Name\>', 'coprocessor' \=\> ‘\<list of coprocessors\>’  
enable '\<Table Name\>'  
   
After upgrading existing tables, it is advised to rebuild indexes to make sure that indexes are consistent for both old and new rows.

# Implementation Details

This section will identify changes and additions on the existing code base.

## Client Side

### Set the scan attributes for global indexes

A new method will be added to IndexUtil, called setScanAttributes for an index table. This method will take a Scan object and PTable objects for an index and data table as input and set the scan attributes for GlobalIndexChecker for global indexes on non-transactional tables. These attributes will include a boolean flag to instruct GlobalIndexChecker to check the verify column of the index table and the name of the HBase table that is the data table for this index table. The setScanAttributes method is called before a Scan is used. This is done in the constructor method of the TableResultIterator class.

### Client side changes for immutable global indexes

These changes include preparing pre and post index updates and implementing orderly execution of the three phases of the write operations within the MutationState class. These changes will be used for the immutable index tables that are configured with the GlobalIndexChecker coprocessor. The immutable index tables that have not been upgraded to the new design will continue to use the existing client side code path. 

## Server Side

### A New Coprocessor for Write Path: IndexRegionObserver

As mentioned above, IndexRegionObserver mostly changes the order of operations for updating a data table and its index tables. It also adds the mutations for the verify status. It does not need to write index updates to WAL anymore.

### A New IndexCommiter for Implementing Asynchronous Third Write Phase: LazyParallelWritterIndexCommitter

IndexRegionObserver will use an IndexWriter object configured with an instance of a new IndexCommitter class called LazyParallelWritterIndexCommitter to asynchronously set the verify status from false to true or delete index rows.  By default, the third write phase will be synchronous, i.e., TrackingParallelWriterIndexCommitter will be used. The synchronous write phase has about 20-25% impact on the write latency. For mixed (read and write) access patterns, it is recommended the third write phase should be synchronous in order to keep the number of inflight index row updates (i.e., the number of unverified rows) low.

### A New Coprocessor for Read Path: GlobalIndexChecker

The verify status is checked here to determine if read repair is necessary for a scanned index table row. This coprocessor is also responsible for repairing unverified rows, i.e., either making them verified, or skip them and/or delete them if they are old enough.  
