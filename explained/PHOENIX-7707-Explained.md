# PHOENIX-7707: Phoenix Server Paging on Valid Rows - Detailed Explanation

**Date:** October 13, 2025
**Commit:** 96b8ffea3258359884ef6b507dc9d6ba93d8c773
**Pull Request:** #2294
**Author:** tkhurana

## Executive Summary

PHOENIX-7707 is a significant architectural improvement that changes how Phoenix's server-side paging mechanism works. The core change is moving from **time-based paging on raw HBase rows scanned** to **time-based paging on valid Phoenix rows returned**. This ensures more efficient and predictable pagination behavior, especially when dealing with tables that have many deleted rows, filtered results, or complex server-side operations.

## Context: Server Paging in Phoenix

### The Problem Before PHOENIX-7707

As documented in the [Server Paging in Phoenix design doc](docs/ServerPagingInPhoenix%20(Copy).md), Phoenix introduced server-side paging to prevent HBase RPC timeouts during long-running operations. The original implementation had a fundamental issue:

**Time tracking was based on HBase rows scanned, not valid Phoenix results returned.**

This meant:
- The server would count time while scanning deleted rows (tombstones)
- Time would elapse while filtering out invalid/expired rows
- A page boundary could be reached after scanning many rows but returning zero actual data
- The client would receive a "dummy row" even though no meaningful work was done

### The Original Server Paging Design (Background)

The original paging implementation (PHOENIX-5998, PHOENIX-6207, PHOENIX-6211) introduced:

1. **PageFilter**: A custom HBase filter that tracked elapsed time and stopped scanning when `phoenix.server.page.size.ms` (default: 60% of RPC timeout) was exceeded
2. **PagedRegionScanner**: A wrapper scanner that detected when PageFilter stopped the scan and returned a "dummy result" to signal the client
3. **Dummy Results**: Special results with null family/qualifier/value that told the client to issue another RPC
4. **Time Tracking**: Each scanner tracked time using `EnvironmentEdgeManager.currentTimeMillis()` at the start of its processing loop

#### The Fundamental Flaw

The problem was that **each scanner** tracked time independently:
```java
// OLD APPROACH - Each scanner tracked its own time
long startTime = EnvironmentEdgeManager.currentTimeMillis();
while (hasMore && !isTimedOut()) {
    hasMore = delegate.next(result);
    // Process result...
    if (EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
        // Return dummy result
        return getDummyResult();
    }
}
```

This meant:
- Time tracking started fresh in each nested scanner
- The overall RPC time wasn't properly accounted for across the scanner stack
- The page size was based on rows **scanned** (including invalid ones), not rows **returned**

## The PHOENIX-7707 Solution

### Core Architectural Change

PHOENIX-7707 introduces a new `PhoenixScannerContext` class that:
1. **Tracks time for the entire scan RPC request** (not per-scanner)
2. **Times only valid rows returned**, not raw HBase rows scanned
3. **Persists across the entire scanner stack** in a single RPC
4. **Properly syncs state back to HBase's ScannerContext**

### Key Components

#### 1. PhoenixScannerContext Class

**Location:** [phoenix-core-server/src/main/java/org/apache/hadoop/hbase/regionserver/PhoenixScannerContext.java](phoenix-core-server/src/main/java/org/apache/hadoop/hbase/regionserver/PhoenixScannerContext.java)

**Purpose:** Extends HBase's `ScannerContext` to add Phoenix-specific functionality:

```java
public class PhoenixScannerContext extends ScannerContext {
    // Tracks start time of the RPC on the server
    private final long startTime;

    public PhoenixScannerContext(ScannerContext hbaseContext) {
        super(hbaseContext.keepProgress, null, hbaseContext.isTrackingMetrics());
        startTime = EnvironmentEdgeManager.currentTimeMillis();
    }

    // Check if we've exceeded the page size time limit
    public static boolean isTimedOut(ScannerContext context, long pageSizeMs) {
        if (!(context instanceof PhoenixScannerContext)) return false;
        PhoenixScannerContext phoenixContext = (PhoenixScannerContext) context;
        return EnvironmentEdgeManager.currentTimeMillis()
            - phoenixContext.startTime > pageSizeMs;
    }
}
```

**Key Features:**
- **startTime field**: Captured once at the beginning of the scan RPC
- **isNewScanRpcRequest()**: Detects when a new RPC starts (lastPeekedCell == null)
- **isTimedOut()**: Checks if elapsed time exceeds pageSizeMs
- **updateHBaseScannerContext()**: Syncs metrics and progress back to HBase
- **setReturnImmediately()**: Signals HBase to return results immediately

#### 2. Scanner Hierarchy Changes

The scanner stack in Phoenix looks like this (from top to bottom):
```
BaseScannerRegionObserver (creates PhoenixScannerContext)
  └─> GroupedAggregateRegionScanner
       └─> PagingRegionScanner
            └─> TTLRegionScanner
                 └─> UncoveredIndexRegionScanner
                      └─> HashJoinRegionScanner
                           └─> HBase RegionScanner
```

**Before PHOENIX-7707:**
- Each scanner tracked its own time
- Each scanner created its own "no limit" context
- Time was measured from when each scanner started processing

**After PHOENIX-7707:**
- `BaseScannerRegionObserver` creates ONE `PhoenixScannerContext` per RPC
- This context is passed down through ALL scanners
- ALL scanners check the SAME elapsed time
- Time is measured from the START of the RPC, not individual scanner operations

### Modified Scanner Examples

#### Example 1: PagingRegionScanner

**Before (timing after scanning rows):**
```java
public boolean next(List<Cell> results) {
    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    while (true) {
        hasMore = delegate.next(results);
        if (!results.isEmpty()) {
            return hasMore;
        }
        if (!hasMore) return false;

        // Deleted row - check if we should page
        if (EnvironmentEdgeManager.currentTimeMillis() - startTime > pageSizeMs) {
            // Spent too much time scanning deleted rows
            getDummyResult(rowKey, results);
            return true;
        }
    }
}
```

**After (timing on valid results):**
```java
public boolean next(List<Cell> results, ScannerContext scannerContext) {
    while (true) {
        hasMore = delegate.next(results, scannerContext);
        if (!results.isEmpty()) {
            // Got a valid result - check if we're out of time
            if (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
                PhoenixScannerContext.setReturnImmediately(scannerContext);
            }
            return hasMore;
        }
        // Empty result (deleted row) - check if timed out
        if (!hasMore) return false;
        if (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
            getDummyResult(rowKey, results);
            return true;
        }
    }
}
```

**Key Differences:**
1. Time is checked from a SHARED context, not local startTime
2. When a valid result is found AND timeout occurred → set returnImmediately
3. When scanning deleted rows AND timeout occurred → return dummy row
4. The timing accounts for the ENTIRE RPC, not just this scanner's loop

#### Example 2: TTLRegionScanner

**Before (timing while skipping expired rows):**
```java
private boolean skipExpired(List<Cell> result, boolean raw, boolean hasMore) {
    if (!isExpired(result)) return hasMore;

    long startTime = EnvironmentEdgeManager.currentTimeMillis();
    do {
        hasMore = raw ? delegate.nextRaw(result) : delegate.next(result);
        if (!isExpired(result)) return hasMore;

        // Expired row - check timeout
        if (EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
            Cell cell = result.get(0);
            result.clear();
            getDummyResult(CellUtil.cloneRow(cell), result);
            return true;
        }
    } while (hasMore);
    return false;
}
```

**After (timing with context):**
```java
private boolean skipExpired(List<Cell> result, boolean raw, boolean hasMore,
    ScannerContext scannerContext) {
    if (!isExpired(result)) return hasMore;

    do {
        hasMore = raw
            ? delegate.nextRaw(result, scannerContext)
            : delegate.next(result, scannerContext);

        if (result.isEmpty() || ScanUtil.isDummy(result)) break;
        if (!isExpired(result)) break;

        // Expired row - check timeout
        if (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
            Cell cell = result.get(0);
            result.clear();
            getDummyResult(CellUtil.cloneRow(cell), result);
            return true;
        }
    } while (hasMore);
    return false;
}
```

**Key Difference:** Now uses the shared `PhoenixScannerContext` to check timeout across the entire RPC, not just time spent in TTLRegionScanner.

### How It Works: End-to-End Flow

#### Scan RPC Request Flow

1. **New RPC Arrives at Region Server**
   - `RSRpcServices.scan()` creates an HBase `ScannerContext`
   - Scanner is either newly created or reused from previous RPC

2. **BaseScannerRegionObserver.next() Called**
   ```java
   public boolean next(List<Cell> result, ScannerContext hbaseScannerContext) {
       // Detect if this is a new RPC (lastPeekedCell == null)
       if (PhoenixScannerContext.isNewScanRpcRequest(hbaseScannerContext)) {
           // Create new PhoenixScannerContext with current timestamp
           phoenixScannerContext = new PhoenixScannerContext(hbaseScannerContext);
       }

       // Pass PhoenixScannerContext to all nested scanners
       boolean hasMore = delegate.next(result, phoenixScannerContext);

       // Sync state back to HBase's context
       phoenixScannerContext.updateHBaseScannerContext(hbaseScannerContext, result);
       return hasMore;
   }
   ```

3. **Scanner Stack Processes Request**
   - Each scanner receives the SAME `PhoenixScannerContext`
   - Each scanner can check `isTimedOut()` using the SAME start time
   - Scanners process rows, skip invalid ones, apply filters, etc.

4. **Valid Result Found**
   ```java
   if (!result.isEmpty() && !isDummy(result)) {
       // Check if we're out of time
       if (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
           // Set flag to tell HBase to return immediately
           PhoenixScannerContext.setReturnImmediately(scannerContext);
       }
       return hasMore;
   }
   ```

5. **Timeout While Scanning Invalid Rows**
   ```java
   if (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
       // Generate dummy row to bookmark position
       getDummyResult(currentRowKey, result);
       PhoenixScannerContext.setReturnImmediately(scannerContext);
       return true;
   }
   ```

6. **Return to HBase**
   - `updateHBaseScannerContext()` copies metrics, progress, lastPeekedCell
   - If returnImmediately is set, HBase immediately sends response to client
   - Scanner remains open for next RPC

### Example Scenario

**Table with Heavy Deletes:**
- Table has 10,000 rows
- 9,000 rows are deleted (tombstones)
- 1,000 rows are valid
- Page size: 30 seconds

**Old Behavior (time on rows scanned):**
```
RPC 1: Scan 3,000 rows in 30s → 300 valid results returned
RPC 2: Scan 3,000 rows in 30s → 300 valid results returned
RPC 3: Scan 3,000 rows in 30s → 300 valid results returned
RPC 4: Scan 1,000 rows in 10s → 100 valid results returned
Total: 4 RPCs, 10,000 rows scanned, 1,000 valid results
```

**New Behavior (time on valid rows returned):**
```
RPC 1: Scan until 30s elapsed on valid rows → ~300 valid results
        (May scan 1,000-3,000 raw rows depending on delete distribution)
RPC 2: Scan until 30s elapsed on valid rows → ~300 valid results
RPC 3: Scan until 30s elapsed on valid rows → ~300 valid results
RPC 4: Scan remaining rows → ~100 valid results
Total: 4 RPCs, consistent valid result sizes
```

**Key Improvement:** Each RPC returns approximately the same number of VALID rows, regardless of how many deleted rows were scanned.

## Impact on Different Scanner Types

### 1. GroupedAggregateRegionObserver
**Change:** Checks timeout after each valid row processed for aggregation
```java
while (hasMore && groupByCache.size() < limit) {
    hasMore = innerScanner.next(result, scannerContext);
    if (hasMore && result is valid) {
        aggregators.aggregate(rowAggregators, result);
    }
    if (PhoenixScannerContext.isReturnImmediately(scannerContext)
        || PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
        return getDummyResult(resultsToReturn);
    }
}
```

### 2. HashJoinRegionScanner
**Change:** Times based on joined results produced, not rows scanned
```java
while (shouldAdvance()) {
    hasMore = innerScanner.next(result, scannerContext);
    processResults(result, false);

    if (PhoenixScannerContext.isReturnImmediately(scannerContext)
        || PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
        getDummyResult(currentRowKey, result);
        return true;
    }
}
```

### 3. UncoveredIndexRegionScanner
**Change:** Times based on verified index rows, not raw index scans
```java
while (true) {
    hasMore = indexScanner.next(indexRow, scannerContext);
    dataRow = verifyIndex(indexRow);

    if (dataRow != null) {
        // Got verified row - check timeout
        if (PhoenixScannerContext.isTimedOut(scannerContext, pageSizeMs)) {
            PhoenixScannerContext.setReturnImmediately(scannerContext);
        }
        return hasMore;
    }
}
```

## Code Changes Summary

### Files Modified (19 files)

**New Files:**
- `PhoenixScannerContext.java` (139 lines) - Core new class

**Deleted Files:**
- `ScannerContextUtil.java` - Replaced by PhoenixScannerContext

**Modified Scanner Files:**
- `BaseScannerRegionObserver.java` - Creates and manages PhoenixScannerContext lifecycle
- `DelegateRegionScanner.java` - Simplified to pass context through
- `GroupedAggregateRegionObserver.java` - Updated timeout checks
- `HashJoinRegionScanner.java` - Updated timeout checks
- `PagingRegionScanner.java` - Updated to time valid results
- `TTLRegionScanner.java` - Updated to time valid (non-expired) rows
- `UncoveredIndexRegionScanner.java` - Updated for index verification timing
- `UncoveredLocalIndexRegionScanner.java` - Updated for local index timing
- `UngroupedAggregateRegionScanner.java` - Updated aggregation timing
- `IndexerRegionScanner.java`, `IndexRebuildRegionScanner.java`, etc.

**Test Files:**
- `ServerPagingIT.java` - Added 239 lines of tests
- `GlobalIndexCheckerIT.java` - Updated
- `CountRowsScannedIT.java` - Added 32 lines

### Key Metrics
- **Lines Changed:** +587 added, -204 deleted (net +383)
- **Files Modified:** 19 files
- **Test Coverage:** Extensive new tests in ServerPagingIT.java

## Benefits

### 1. More Predictable Performance
Each page returns approximately the same number of valid results, regardless of:
- Deleted rows in the table
- Expired TTL rows
- Filtered rows
- Index verification failures

### 2. Better Resource Utilization
- Avoids wasting RPC time scanning deleted/invalid rows just to hit page boundaries
- More efficient use of server resources
- Fewer "wasted" RPCs that return dummy rows with no data

### 3. Improved Client Experience
- More consistent result set sizes per RPC
- Fewer empty responses
- More predictable query performance

### 4. Cleaner Architecture
- Centralized timing logic in PhoenixScannerContext
- Eliminated scattered `EnvironmentEdgeManager.currentTimeMillis()` calls
- Better separation of concerns

### 5. Accurate Metrics
- `PAGED_ROWS_COUNTER` metric now tracks actual valid rows paged
- Better visibility into server-side paging behavior

## Testing Strategy

From [ServerPagingIT.java](phoenix-core/src/it/java/org/apache/phoenix/end2end/ServerPagingIT.java):

1. **Zero Page Size Testing:** Sets `phoenix.server.page.size.ms=0` to force paging after every row
2. **Delete Scenarios:** Tests with tables that have many deleted rows
3. **Metric Validation:** Verifies `PAGED_ROWS_COUNTER` is accurately incremented
4. **Various Query Patterns:** Tests scans, aggregations, joins, index lookups
5. **Limit Queries:** Ensures LIMIT clauses work correctly with paging

Example test:
```java
// Upsert 50 rows, delete first 40
for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 10; j++) {
        stmt.setInt(1, i);
        stmt.setInt(2, j);
        stmt.executeUpdate();
    }
}
// Delete first 40 rows
for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
        deleteStmt.setInt(1, i);
        deleteStmt.setInt(2, j);
        deleteStmt.executeUpdate();
    }
}
// Query with LIMIT 10 - should scan past deleted rows
ResultSet rs = stmt.executeQuery("SELECT * FROM table WHERE id1 >= 3 LIMIT 10");
// Verify we get exactly 10 valid rows
// Verify paging metrics show paging occurred
```

## Comparison: Before vs. After

| Aspect | Before PHOENIX-7707 | After PHOENIX-7707 |
|--------|---------------------|-------------------|
| **Time Tracking** | Each scanner tracks own time | Single PhoenixScannerContext per RPC |
| **Page Boundary** | After scanning N raw HBase rows | After returning N valid Phoenix rows |
| **Context Creation** | New context created by each scanner | One context created at RPC start |
| **Timing Start** | When each scanner starts processing | When RPC arrives at server |
| **Deleted Rows** | Count toward page size | Don't count toward page size |
| **Filtered Rows** | Count toward page size | Don't count toward page size |
| **Result Consistency** | Variable (depends on delete ratio) | Consistent valid row counts |
| **Dummy Rows** | Frequent with high delete rates | Less frequent, more meaningful |
| **Code Structure** | Scattered timing logic | Centralized in PhoenixScannerContext |

## Performance Impact

As noted in the original Server Paging design doc:

> "During ITs, paging happens after every scanned row in Phoenix region scanners and after every row that is filtered. Even with this extreme paging, no performance impact on ITs has been observed."

PHOENIX-7707 maintains this property while providing:
- **More predictable per-RPC latency** (based on valid rows, not scanned rows)
- **Better throughput** in tables with high delete rates
- **Reduced wasted RPCs** (fewer empty dummy responses)

## Conclusion

PHOENIX-7707 represents a fundamental improvement to Phoenix's server-side paging architecture. By moving from **time-based paging on rows scanned** to **time-based paging on valid rows returned**, it provides:

1. More predictable and consistent query performance
2. Better handling of tables with high delete rates
3. More efficient use of server resources
4. Cleaner, more maintainable code architecture
5. Accurate metrics for monitoring

The change demonstrates deep understanding of the Phoenix scanner architecture and the subtleties of how HBase row scanning interacts with Phoenix's higher-level operations like filtering, aggregation, and index verification.

## References

- **JIRA:** [PHOENIX-7707](https://issues.apache.org/jira/browse/PHOENIX-7707)
- **Commit:** 96b8ffea3258359884ef6b507dc9d6ba93d8c773
- **Design Doc:** [Server Paging in Phoenix](docs/ServerPagingInPhoenix%20(Copy).md)
- **Related Issues:** PHOENIX-5998, PHOENIX-6207, PHOENIX-6211
- **Code:** [phoenix-core-server/src/main/java/org/apache/hadoop/hbase/regionserver/PhoenixScannerContext.java](phoenix-core-server/src/main/java/org/apache/hadoop/hbase/regionserver/PhoenixScannerContext.java)
