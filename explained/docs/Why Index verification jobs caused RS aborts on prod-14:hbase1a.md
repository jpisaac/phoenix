# Why Index verification jobs caused RS aborts on prod-14/hbase1a

## Problem Analysis

Error message:

[`2025-02-19 03`](tel:2025021903)`:20:47,558 ERROR [S_CLOSE_REGION-regionserver/regionserver-0:60020-1] regionserver.HRegion - Failed to acquire close lock on PLATFORM_ENTITY.PLATFORM_IMMUTABLE_ENTITY_DATA,00D2v000001dBzv07t\x7F\xFF\xFElQ.AX32568fda-6678-4a6c-a693-57dc84515189,1736159437705.24f75377351f3bab4789659c61df8d6f. after waiting 60001 ms`
`[2025-02-19 03](tel:2025021903):20:47,572 ERROR [S_CLOSE_REGION-regionserver/regionserver-0:60020-1] regionserver.HRegionServer - ***** ABORTING region server regionserver-0.regionserver.hbase.hbase1a.hbase.core1.aws-prod14-apnortheast2.aws.sfdc.is,60020,1739808277724: Failed to acquire close lock on PLATFORM_ENTITY.PLATFORM_IMMUTABLE_ENTITY_DATA,00D2v000001dBzv07t\x7F\xFF\xFElQ.AX32568fda-6678-4a6c-a693-57dc84515189,1736159437705.24f75377351f3bab4789659c61df8d6f. after waiting 60001 ms *****`

IndexRebuildRegionScanner implements the server side handler of index verification and rebuild scan RPCs. The main loop of IndexRebuildRegionScanner actually checks for region closing. This scanner runs on the data table on which the index is defined and compares the data table rows to the corresponding index table rows for every data table region.

https://git.soma.salesforce.com/bigdata-packaging/phoenix/blob/hadoop-3.3-release-13.13/phoenix-core-server/src/main/java/org/apache/phoenix/coprocessor/IndexRebuildRegionScanner.java#L329-L368

```
do {
       /*
        If region is closing and there are large number of rows being verified/rebuilt with IndexTool,
        not having this check will impact/delay the region closing -- affecting the availability
        as this method holds the read lock on the region.
       **/
       ungroupedAggregateRegionObserver.**checkForRegionClosingOrSplitting**();
       List<Cell> row = new ArrayList<>();
       hasMore = localScanner.nextRaw(row);
       if (!row.isEmpty()) {
          lastCell = row.get(0); // lastCell is any cell from the last visited row
          if (isDummy(row)) {
              break;
          }      
  } while (hasMore && indexMutationCount < pageSizeInRows
            && dataRowCount < pageSizeInRows);
```

So the next question which arises was why the loop was stuck in this `hasMore = localScanner.nextRaw(row);`  

The stack of scanners that we have is IndexRebuildRegionScanner → TTLRegionScanner → PagingRegionScanner → RegionScannerImpl (HBase). One reason why the RegionScannerImpl would not return any row back to the top level scanner is if there is a filter defined on the scan and the rows don’t match the filter. In that case, the RegionScannerImpl will continuously skip the rows till it finds a matching row.
Typically, index scans need to look at every row in the data table. The only time they have to filter rows is when the scan is on a view. This is because all the views share the same underlying HBase table so when verifying a view index we have to set a filter so that we are only looking at the rows belonging to a particular view on which the view index is defined. But we still have to scan the entire base table because we don’t know which regions could potentially have the rows belonging to a particular view. This is because tenantID is the leading prefix of the row key and when verifying indexes we are doing it for all tenants.

From the error message above we know that we failed to acquire close lock on a region of  **`PLATFORM_ENTITY.PLATFORM_IMMUTABLE_ENTITY_DATA`** table. Also, right around the RS abort there was an index verification job running on the view `PLATFORM_ENTITY.EXPLAINABILITY_ACTION_LOG`. Now this view has a key prefix of [**9ay**](https://git.soma.salesforce.com/bigdata-packaging/hbase-schema/blob/hadoop-3/generated/golden_file.sql#L3605C27-L3622) . If we look at the region which failed to close it had a row key prefix `00D2v000001dBzv``**07t**` .  Key prefix ‘**[07t](https://git.soma.salesforce.com/bigdata-packaging/hbase-schema/blob/hadoop-3/generated/golden_file.sql#L2141-L2173)**’ is actually of view PLATFORM_ENTITY.API_EVENT.

Now, this made sense. We are scanning a view with a row key filter set to ‘9ay’ but for regions which belong to other views the filter will not match. We could potentially scan the entire region and not find any matching rows. If during this scan the region needs to move it would first need to close the region but the close will fail because the index scanner has a lock on the region. And, this scan can potentially take more than 60 seconds which ultimately leads to aborting the region server.

To further validate the above theory,  I looked at the thread dumps which are taken when the RS aborts. One of the stack trace in the thread dump was:

```
org.apache.phoenix.expression.BaseCompoundExpression.reset(BaseCompoundExpression.java:138)
    org.apache.phoenix.filter.BooleanExpressionFilter.reset(BooleanExpressionFilter.java:133)
    org.apache.phoenix.filter.**RowKeyComparisonFilter**.reset(RowKeyComparisonFilter.java:61)
    org.apache.phoenix.filter.DelegateFilter.reset(DelegateFilter.java:38)
    org.apache.phoenix.filter.PagingFilter.reset(PagingFilter.java:100)
    org.apache.hadoop.hbase.filter.FilterWrapper.reset(FilterWrapper.java:78)
    org.apache.hadoop.hbase.regionserver.RegionScannerImpl.resetFilters(RegionScannerImpl.java:236)
    org.apache.hadoop.hbase.regionserver.RegionScannerImpl.nextRow(RegionScannerImpl.java:715)
    org.apache.hadoop.hbase.regionserver.RegionScannerImpl.nextInternal(RegionScannerImpl.java:556)
    org.apache.hadoop.hbase.regionserver.RegionScannerImpl.nextRaw(RegionScannerImpl.java:278)
    org.apache.hadoop.hbase.regionserver.RegionScannerImpl.nextRaw(RegionScannerImpl.java:265)
    org.apache.phoenix.coprocessor.PagingRegionScanner.next(PagingRegionScanner.java:91)
    org.apache.phoenix.coprocessor.PagingRegionScanner.nextRaw(PagingRegionScanner.java:128)
    org.apache.phoenix.coprocessor.TTLRegionScanner.next(TTLRegionScanner.java:188)
    org.apache.phoenix.coprocessor.TTLRegionScanner.nextRaw(TTLRegionScanner.java:216)
    org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.next(IndexRebuildRegionScanner.java:337)
    org.apache.phoenix.coprocessor.BaseRegionScanner.nextRaw(BaseRegionScanner.java:56)
    org.apache.phoenix.coprocessor.DelegateRegionScanner.nextRaw(DelegateRegionScanner.java:79)
    org.apache.phoenix.coprocessor.DelegateRegionScanner.nextRaw(DelegateRegionScanner.java:79)
    org.apache.phoenix.coprocessor.BaseScannerRegionObserver$RegionScannerHolder.nextRaw(BaseScannerRegionObserver.java:254)
```

This validated the theory. We are in `RowKeyComparisonFilter` which filtered the row because it had mismatch on the key prefix and the filter was reset.

Unfortunately, paging also didn’t help us here since the RPC timeouts used for index verification scans are currently set too high. The PagingFilter is wrapping the underlying filter. Reducing the RPC timeout or explicitly setting the paging timeout on the scans would fix the problem.

There is more to this though.  I tried to repro the above scenario in an IT environment. I couldn’t repro it initially.  On further debugging, I found that even though Paging was set to a high value, after a certain number of rows were scanned the RegionScannerImpl started returning empty results back to the IndexRebuildRegionScanner on every row which didn’t match the filter. This is exactly what we wanted. 
https://git.soma.salesforce.com/bigdata-packaging/hbase/blob/hadoop-3.3-release-13.13/hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/RegionScannerImpl.java#L506-L507

But the question was why weren’t we seeing this behavior in production. What was happening was that in StoreScanner we were switching from PREAD → STREAM mode. 

```
if (scanUsePread && readType == Scan.ReadType.DEFAULT && bytesRead > preadMaxBytes) {
// return immediately if we want to switch from pread to stream. We need this because we
// can
// only switch in the shipped method, if user use a filter to filter out everything and
// rpc
// timeout is very large then the shipped method will never be called until the whole scan
// is finished, but at that time we have already scan all the data...
// See HBASE-20457 for more details.
// And there is still a scenario that can not be handled. If we have a very large row,
// which
// have millions of qualifiers, and filter.filterRow is used, then even if we set the flag
// here, we still need to scan all the qualifiers before returning...
scannerContext.returnImmediately();
}
```

Once you set `scannerContext.returnImmediately()` , this gives you the same behavior as reaching the time limit. This results in yielding the control back to the Phoenix scanner. (https://git.soma.salesforce.com/bigdata-packaging/hbase/blob/hadoop-3.3-release-13.13/hbase-server/src/main/java/org/apache/hadoop/hbase/regionserver/ScannerContext.java#L108)

However, **in our production we have made the default scan read type to PREAD**. This was done as part of this Data Mention As a side affect of this change, we are no longer switching to stream mode. Once I changed the default scan type to PREAD I was able to reproduce the problem. [Analysis: PREAD vs STREAM](https://salesforce.quip.com/l6wfAZ9xCxvZ) mentions the reasoning behind this config change.


## Proposed Solution

Data Mention Tune RPC timeouts/Paging timeouts for Index verification and rebuild scans






