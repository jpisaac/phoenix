# Phoenix Table TTL and Max Lookback Redesign ([PHOENIX 6888](https://issues.apache.org/jira/browse/PHOENIX-6888))

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)  
Updated: Mar 26, 2023

In HBase, the unit of data is a cell and data retention rules are executed at the cell level. These rules are defined at the column family level. Phoenix leverages the data retention features of HBase and exposes them to its users to provide its TTL feature at the table level. However, these rules (since they are defined at the cell level instead of the row level) results in partial row retention that in turn creates data integrity issues at the Phoenix level. 

Similarly, Phoenix’s max lookback feature leverages HBase deleted data retention capabilities to preserve deleted cells within a configurable max lookback window. This requires two data retention windows, max lookback and TTL. One end of these windows is the current time and the end is a moment in the past (i.e., current time minus the window size). Typically, the max lookback window is shorter than the TTL window. In the max lookback window, we would like to preserve the complete history of mutations regardless of how many cell versions these mutations generated. In the remaining TTL window outside the max lookback, we would like to apply the data retention rules defined above. However, HBase provides only one data retention window. Thus, the max lookback window had to be extended to become TTL window and the max lookback feature results in unwantedly retaining deleted data for the maximum of max lookback and TTL periods. 

This document provides a solution to fix both of these issues.

# Background

There are two types of cells in HBase, put and delete. The delete cells are called delete markers. There are four types of delete markers, delete family (Type.DeleteFamily), delete family version (Type.DeleteFamilyVersion), delete column (Type.DeleteColumn) and delete (Type.Delete). A delete family marker for a column family deletes all versions of all cells within the column family while a delete family version marker deletes only the latest cell versions. Similarly a delete column marker for a column deletes all cell versions within the column while a delete marker deletes only the latest version.

There are three column family properties that define the rules to retain cells in HBase. These are VERSIONS, MIN\_VERSIONS, and TTL. TTL defines when a cell is to expire in seconds. If TTL is set to FOREVER then the cells do not expire.  VERSIONS defines the maximum number of cell versions to retain. MIN\_VERSIONS defines the minimum number of (expired or deleted) cell versions to retain.  

KEEP\_DELETED\_CELLS is an HBase column family attribute to keep deleted cells around. If it is set to FALSE, the deleted cells are not retained, that is, are removed by the major compaction. To retain the deleted cells within the TTL period and up to the maximum number of versions, KEEP\_DELETED\_CELLS is set to TRUE. To retain the deleted cells until their delete markers expire, KEEP\_DELETED\_CELLS is set to TTL.

The HBase code and documents do not spell out the data retention policy for all combinations of data retention parameters. The following table attempts to do that.

| TTL | KEEP\_ DELETED\_CELLS | Retain |
| :---- | :---- | :---- |
| Specified | FALSE | At most VERSIONS live put cells within the TTL window At least MIN\_VERSIONS live put cells  |
| Specified | TRUE | At most VERSIONS live/deleted put cells At most VERSIONS delete markers within the TTL window At least  MIN\_VERSIONS put cells  |
| Specified | TTL | At most VERSIONS live put cells At most VERSIONS live delete markers All deleted cells until their delete markers expire At least MIN\_VERSIONS expired but not deleted put cells  |
| Unspecified (FOREVER) | FALSE | At most VERSIONS live put cells |
| Unspecified (FOREVER) | TRUE | At most VERSIONS live/deleted put cells |
| Unspecified (FOREVER) | TTL | Not a valid data retention policy |

HBase allows to override these column family data retention properties dynamically within coprocessor hooks for flushes and compactions. It also allows wrapping a store scanner using the pre compaction coprocessor hook (i.e., preCompact) to implement filtering to decide which cells to be written to the store files during compaction. 

Phoenix is a relational database with a SQL interface, and uses HBase as its backing store. Phoenix tables are backed by HBase tables, and the primary key of a Phoenix table maps to the row key of the underlying HBase table. This means that the PK columns of a Phoenix table are  packed in the row key of the underlying HBase table. A Phoenix table may not have any non-PK column when it is created or may start with some non-PK column and these columns can be dropped over time. This means that Phoenix should be able to support tables with no non-PK columns. However, to form a cell, Phoenix needs to have a non-PK column that is stored on every row. This is the motivation for having an internal column included in every table that is not visible to users. This column is called the empty column. Phoenix includes the empty column cell in every mutation on a given table. When there are multiple column families in a table, Phoenix stores the empty column in only one column family and always in the same column family.

In HBase every cell has its own timestamp. If the timestamp is not specified by applications, HBase uses the current server timestamp for mutations. Thus, in HBase and Phoenix,  the current System Change Number (SCN) is the current timestamp (or wall clock time) of the system. Phoenix allows its applications to change the SCN for a given Phoenix connection to go back in time and retrieve the latest version of cells at that time in the past.  The queries done within such connections will be referred to as SCN queries. The cluster level max lookback age parameter in Phoenix defines the lookback time window where SCN queries are allowed. This means that Phoenix requires retaining row versions that are visible through the max lookback window. This also means that the row versions that are not visible through the max lookback window are accessible via Phoenix and therefore are not really required to be retained.

# Requirements

1. Phoenix should be able to continue leveraging HBase data retention capabilities but should not allow table row versions to partially expire.  
2. The max lookback feature should preserve live or deleted row versions that are visible through the max look window.  
3. The max lookback feature should not lead to retention of unwanted live, expired or deleted row versions that are not visible through the max lookback window.   
4. The expired mutations should be masked.  
5. The solution that meets the above requirements should not degrade the performance, scalability or availability of Phoenix.

# Solution

The solution provided here leverages the HBase capability for changing data retention properties dynamically (as the current implementation of the max lookback feature does) and additionally wraps store scanners using the preCompact hook to implement filtering to decide which cells to be written to the store files. Essentially, the data retention properties are overridden such that all cells including delete markers are preserved and then the decision of what to be removed is determined in the new wrapper store scanner that will be called the Phoenix compaction scanner or CompactionScanner in the Phoenix code base. Similarly, the decision of which cells to be masked is done in a new wrapper region scanner that will be called TTLRegionScanner.  These implement the correct data retention policy for both Phoenix TTL and max lookback. 

## Compaction

The max lookback window and TTL window are defined such that TTL window is greater than or equal to the lookback window. This means that if the TTL period is less than the max lookback period, then the max lookback period is set to the TTL period in CompactionScanner.

An HBase table can be viewed as a set of cells where each cell is identified by a key, which is composed of a row key, family name, column qualifier, and timestamp. A cell holds a value expressed using a byte array. A row is identified with a row key and is the set of cells with this row key. The cells of a given row can be created or updated at different times and thus each cell of a row can have a different timestamp. The latest row version (or the row image) at a given moment is contributed from the latest versions of the cells from each column family at that moment such that these cells are not masked by a delete marker.  This begs the question of what would be the timestamp of a given row as these cells can have different timestamps.  Phoenix defines the timestamp of a row as the maximum of its cell timestamps. In other words, the timestamp of a row is defined by the last mutation time on that row.

As mentioned before, the data retention rules in HBase are defined at the cell level (instead of the row level) and this results in partial row retention that in turn creates data integrity issues at the Phoenix level. To see this, assume that we have a table for products. The cell for the description of a product may be created when the row for this product is created but the description column may never be updated again. However, other columns such as the price for the product, the current inventory level, etc. can be updated multiple times and thus they can have multiple versions of their cells. This means that for this row, there will be one cell version for the description column and the timestamp of this cell can be outside the TTL window but this cell version will and should still be included in all versions of this row. However, the HBase data retention policies would delete this cell as it is outside the TTL window. This creates partial row expiration and thus data integrity issues in Phoenix.

### Phoenix Level Compaction

Although Phoenix DDL statements allow specifying the data retention policy using the HBase data retention model, the HBase model is not really suitable for Phoenix. There are two reasons for it. The first one is that Phoenix needs to retain all cell versions including delete markers that are visible through the max lookback window. This overrides the HBase retention rules. For example, Phoenix may require to retain more versions than VERSIONS specify and retain delete markers even when KEEP\_DELETED\_CELLS is set to FALSE. The second reason is that the cell versions that are not visible to the max lookback are not accessible via Phoenix.

The data retention model for Phoenix should have been based on just two time-based parameters, max lookback age and TTL. The first one defines the set of row versions to retain. The second one defines the maximum time gap between two HBase mutations. When the time gap is larger than TTL, the mutations that are beyond this time gap are expired.

This retention model is implement at the store level for tables with one column family as follows:

1. Sort the cells of a row based on their timestamps in descending order.  
2. Starting from the first cell, check the time gap between subsequent cells. Whenever the time gap is larger than TTL, trim the rest of cells.  
3. Retain all the cells whose age is less than max lookback age.  
4. Retain the cells of the last row version visible via the max lookback window.  
5. After ordering the cells of the last row version based on their timestamps, check the time gap between cells. For the gaps that are larger than TTL, retain a minimum number of empty column cells to make the gaps less than or equal to TTL. Note this is required for TTLRegionScanner not to mask any of the cells of the last row version. 

When there are more than one column families, we need to read all the cells from all column families if there is a gap more than TTL and the store is not the empty column family store. In this case, we read the row using a raw scan at the region level and compact it in memory. Then we take the intersection of  the set of retained cells after the region level compaction, and the set of cells read from the store. This intersection is the set of cells we want to retain for the store.

The set of cells retained by this model is the superset of the set of cells retained by the HBase model with VERSIONS \= 1, MIN\_VERSIONS \= 0, and KEEP\_DELETED\_CELLS \= FALSE (which are the defaults used by Phoenix). If the table does not use these defaults then we need to do compaction using the HBase data retention rules to identify if we need to retain more cells just to be compliant with them and back compatible in a sense. In future, we may completely abandon the HBase level compaction as these additional cells are not visible to Phoenix. The HBase level compaction is described in the next section.

### HBase Level Compaction

To eliminate partial row expiration, we need to apply HBase data retention properties at the row level to preserve the integrity of a given row version. This means that in addition to defining a row timestamp, we also need to determine the number of row versions at a given time.

It is very straightforward to count the number of cell versions for a given column at a given time. It is also straightforward to count the row versions at a given time. To do that, we group the cells with the same timestamp. Each such group corresponds to a row mutation and thus a row version. However, we cannot apply the HBase data retention rules to such row versions. To see this consider that a row has 10 columns. One can create a row mutation that updates all the columns. In this case, we would have one row version. However, one can also create the same row using 10 mutations such that each mutation updates only one column with different timestamps. In this case, we came up with 10 row versions. Phoenix by default set VERSIONS to 1 for its tables. Thus, such a row version definition would not work for Phoenix and also would not be easy to reason. 

HBase scan and get returns the latest version of a row. The latest version of a row at a given time is formed from the latest cell versions such that these cell versions are not masked by delete markers as mentioned before. Please note that this implies that the latest row version can include cells with different timestamps when the row version is formed by multiple row mutations.

After making these observations, we can formulate a row version that is aligned with HBase data retention properties and also preserves the integrity of a row at a given, current time or a time in the past, as follows. 

We introduce the concept of *compaction row version*. A compaction row version includes the latest (put) cell versions from each column such that the cell versions do not cross delete markers. In other words, the compaction row versions are built from cell versions that are all either before or after the next delete family or delete family version maker if family delete markers exist and also individual cell versions are either before or after the next delete or delete column markers if column delete markers exist.  A compaction row version does not share a cell version with the next compaction row version. 

After creating the first compaction row version, we form the next compaction row version from the remaining cell versions. Please note that the first compaction row version corresponds to the latest row version HBase returns when the row is scanned. However, the subsequent compaction row versions do not have an HBase counterpart.  Compaction row versions are used for compaction purposes to determine which row versions to retain. With the compaction row version concept, we can apply HBase data retention parameters to the compaction process at the Phoenix level. 

The version of the first compaction row version is 0, the next one is 1, and so on.  As defined for row versions, we define the timestamp of a compaction row version as the maximum of its cell timestamps. 

In HBase, compaction is done at the store level and thus we attempt to compact a row at the store level. A row compaction version is defined as inside/included in a given time window only if its timestamp falls into the time window. Now we are ready to describe the rules for retaining compaction row versions at the store/column family level as follows. 

**Inside TTL Window**  
If one of the following conditions holds then the cells of the compaction row version are retained.

* The compaction row version is alive and its version is less than VERSIONS  
* The compaction row version is deleted and KEEP\_DELETED\_CELLS is TTL  
* The compaction row version is deleted, its version is less than MIN\_VERSIONS and KeepDeletedCells is TRUE  
    
  **Outside TTL Window**  
  If one of the following conditions holds then the cells of the compaction row version are retained.  
* The compaction row version is alive and its version is less than MIN\_VERSIONS  
* The compaction row version is deleted, its delete marker is inside the TTL window, and KEEP\_DELETED\_CELLS is TTL 

The delete markers are compacted at the cell level using the HBase data retention rules with the caveat that all delete markers inside the max lookback window and all delete markers inserted after the last retained row version are retained. The retained delete markers and the put cells are sorted lexicographically (using the HBase cell library)  and returned to the HBase compaction process to copy in a new store file.

## Masking

For user scans, we make sure that the row versions outside the TTL window are masked. To do that we implement masking for the rows returned by user scans in a region scanner called TTLRegionScanner that intercepts the scanned rows and checks their timestamps. 

Since a scanned row may not include all the cells, we cannot determine the row timestamp unless the empty column cell is included in the row. Please note that Phoenix always includes the empty column cell in every mutation and thus the empty column cell is in every row version.  To make sure that every scanned row returns its empty column cell, we modify the HBase Scan objects created at the Phoenix client (within the constructor of TableResultIterator), and scan filters to include the empty column cell too. 

One of the filters, called FirstKeyOnlyFilter, returns the very first cell of a given row, and this cell may not be the empty column cell if the column encoding is not used for the scanned table. For column encoded tables, the empty column would be the first cell of its column family as its assigned column qualifier is zero. However non-encoded tables, the qualifier names are the ones provided in the table schema and the qualifier for the empty column is “\_0”. This naming makes the empty column the last one. To cover this case where the empty column would not be the first cell of the row, we introduce a new Phoenix filter called EmptyColumnOnlyFilter which replaces FirstKeyOnlyFilter and returns the empty column cell instead of the first cell.

TTLRegionScanner checks the time gap between maximum and minimum timestamps in a scanned row. If the gap is larger than TTL, it reads the row using a raw scan and sorts the cells of a row based on their timestamps in descending order. Starting from the first cell, it checks the time gap between cells. Whenever the time gap is larger than TTL, it trims the rest of cells.

# Testing

In order to ensure, the compaction solution provided here is correct and correctly implemented, we introduce the following exhaustive integration test in addition to existing tests that use compaction. There are two functional requirements that this solution should meet:

* The max lookback feature should preserve live or deleted row versions intact within the max look window.  
* The max lookback feature should not preserve unwanted live, expired or deleted row versions outside the max lookback window. 

This test creates two tables with the same schema. The same row is updated in a loop on both tables with the same content. Each update changes one or more columns chosen randomly with randomly generated values. The null value is also one of the generated values.

After every upsert within the loop, all versions of the row inside the maxlook window are retrieved from each table and compared. The test also occasionally deletes the row from both tables, but compacts only the first table within the loop. This test expects that both tables return the same row content for the same row version.

TTL and max lookback periods are set to 30 and 10 seconds, respectively. The MIN\_VERSIONS parameter is not set so it is zero by default. The loop iterates 500 times. The row is deleted from both tables about 50 times at randomly chosen iterations. The first table is compacted about 10 times at randomly chosen iterations. The time is advanced beyond the TTL window about 10 times at randomly chosen iterations to let the row to expire. The test is executed 6 times, each execution uses one of the combinations of the following parameter values.

1. The table has a single column family or multiple column families   
2. The table is column encoded or not  
3. KEEP\_DELETED\_CELLS is set to FALSE, TRUE, or TTL   
4. The VERSIONS parameter is set to 1 or 5

