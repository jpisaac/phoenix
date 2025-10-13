# [Phoenix-4925](https://issues.apache.org/jira/browse/PHOENIX-4925) Use a Variant Segment Tree to Organize Guide Post Info

## What're the Problems to Solve?

Regarding the data structure of guide post info and its usage, currently we have the following three main problems:

1. The first problem — The guide posts of a table are encoded into a byte array by using prefix encoding. To generate a parallel scan plan, we need to decode and traverse guide posts sequentially; for each given range specified by either two consecutive guide posts or one guide post and one region boundary, search for the intersections with the scan ranges specified by  WHERE clause predicate filters along primary key axis or index axis. The data structure and the algorithm result in one performance issue — after using Stats (guide post info) to generate plan, the time complexity in BaseResultIterators.getParallelScan(...) is increased from O(m*k) to be O(n*k) , **where m is the total count of regions, n is the total count of guide posts, k is the total count of filters**, and n >> m. Because the check of intersection with a scan range specified by a WHERE clause predicate filter is an expensive operation, it is the root cause of the bug [W-4908447](https://gus.lightning.force.com/lightning/r/a07B0000005061iIAA/view) “[Phoenix Stats] Query plan compilation grows with the number of filters”.
2. The second problem — Currently, we always cache the guide posts of the whole table. Eventually, we need to be able to just cache a part of guide posts of a table (e.g. for tenant  specific view or a special query) or cache the the guide posts with lower resolution. This problem is tracked by [PHOENIX-4927](https://issues.apache.org/jira/browse/PHOENIX-4927) “Disentangle the granularity of guidepost data from that of client cached guide post data”. 
3. The third problem -- Decouple the granularity stored in SYSTEM.STATS from both the granularity stored on the client and the granularity of the scanning. 

Although we only partially solve the last two problems in Phoenix Stats V1, this design document will also cover the high level design for solving the last two problems to make sure the solution for first problem is extensible.

## The Data Structure and Algorithms Proposed

## High Level Design

To solve the first problem, the main idea is that, for each scan range specified by  WHERE clause predicate filters, we need find a data structure in which we can perform range scan to find the guide posts and the estimation info (the estimated # rows, # bytes and **the least update time stamp T**) within the range. In this way, to generate a parallel scan plan, the time complexity will be O(k) (k is defined above) multiplied by the time complexity of performing range scan in the data structure. To minimize the time complexity of the latter, a Variant Segment Tree is proposed. Please refer to [here](https://en.wikipedia.org/wiki/Segment_tree) for the standard Segment Tree.

The guide posts are partitioned to w guide post chunks so that each chunk contains the configured number of guide posts. **Each chunk is encoded by using the configured encoding scheme (for now, prefix encoding by default), and it is the unit of encoding/decoding (or compression/decompression)**.

A leaf node contains the following data (In the future, we should allow to compress and decompress the leaf node as a whole.):

* The guide post chunk (encoded, decoded or both) which is always encoded/decoded or compressed/decompressed as a whole.
* The array of # rows denoted as Rows. **Rows[i] is the sum of the estimated rows of  guide post [0, ..., i]**.
* The array of # bytes denoted as Bytes. **Bytes[i] is the sum of the estimated bytes of  guide post [0, ..., i]**.
* The array of the time stamp denoted as T; **T[i] is the last update time stamp of guide post i**.

An Inner node contains the following data:

* The guide posts (key) range of the sub tree with this node as root node.
* The total count of guide posts in the sub tree.
* The sum of # rows, the sum of # bytes and the least update time stamp in the sub tree.

Compared to the standard segment tree, one of big differences in this proposed variant segment tree is that all the leaf nodes are linked together to facilitate range scan.

According to the above, below is the overview graph of the proposed Variant Segment Tree.
[Image: The Variant Segment Tree Overview.jpg]
## Interface:

```
class GuidePostInfo {
    /**
     * Given the query key Ranges, calculate and return the accumulative estimation from the minimal guide post
     * set which covers the query key ranges.
     *
     * @param queryKeyRanges
     * @return
     */
    public GuidePostEstimation getEstimationOnly(List<KeyRange> queryKeyRanges);

    /**
     * Given the query key Ranges, group the query key ranges by guide post, and for each group return the scan range
     * which is the smallest key range covering the group. If a query key range crosses guide post boundary, it will
     * be split into different groups.
     *
     * For example, assume we have guide posts with keys {10, 20, 30, 50}, given the query key ranges [0, 1), [3, 5),
     * [7, 30), [45, 100), the scan ranges will be [0, 10), [10, 20), [20, 30), [45, 50), [50, UNBOUND)
     *
     * @param queryKeyRanges
     * @return
     */
    public Pair<List<KeyRange>, GuidePostEstimation> generateParallelScanRanges(List<KeyRange> queryKeyRanges);
 }    
```

## Build The Variant Segment Tree

The time to build this segment tree is every time after guide posts loader retrieves guide posts for a given table from the system stats table and before the loader inserts it into the stats client cache or refreshes the entry in the stats client cache. Please refer to “[Use Asynchronous Refresh to Provide Non-blocking Phoenix Stats Cache”](https://salesforce.quip.com/rxokAkVatiQO) for details. 

As showed by the graph 1 below, at the moment of building stats, the boundaries of guide posts and regions are aligned. As the region boundaries changing over time due to Region Merge/Split, some of guide posts and regions aren't aligned — this happens when the guide posts loader build the segment tree (graph 2). In Graph 1 and 2, g_i0 ... g_i15 are typical guide posts which are close to or exactly at the guide post chunk boundaries. g_i9 and g_i10 are two consecutive  guide posts. R0 ... R6 are the end keys of the corresponding regions in graph 1, and R0' ... R6' have the same meaning in graph 2.  
[Image: Use Segment Tree to Organize Guide Post Info (GuidePosts and Regions).jpg]To build this segment tree, besides the guide post info and the configured number of guide posts in a chunk, we also need the region boundaries at the moment of building the segment tree. The key point is that the boundaries of guide post chunks are chosen at the region boundaries  as much as we can. In this way, within a stats refresh cycle at the server side (usually 1 day), the region boundaries at the moment of querying stats are still almost aligned with the guide posts boundaries, because the region merge/split is rare operation within this short period. This characteristic of the segment tree can bring many benefits, e.g. to calculate how many parallel scans that the plan has, we can skip the guide post chunks which don't have region boundaries within their ranges, because we can get the info just from summary info of the corresponding leaf node.

In graph 2, due to region boundary change, R4' is in the key range (g_i9, g_i10) and isn't aligned with any guide post. Graph 3 shows the segment tree built in which the region boundary R4' is within the guide post chunk of the leaf node in the highlighted color. 

[Image: Use Segment Tree to Organize Guide Post Info (Build Segment Tree) (2).jpg]
## Perform Range Scan In The Segment Tree

### Alorithm

Given the key range [Key1, Key2] where Key1 > i4 and Key2 < i14, we can follow the steps listed below to get the estimated info and the total count of parallel scans:

* Step 1: Performing the search in the tree to get the following results (graph 4):

    * The sum of #rows, #bytes and min(T), where #rows, #bytes and T are from the inner nodes (i5, i7], (i7, i12] and the leaf nodes (i4, i5], (i12, i13], (i13, i14] in the yellow color.
    * The list with the leaf node (i4, i5] as the first node and (i13, i14] as the last node.

[Image: Perform Range Scan In The Segment Tree (1).jpg]
* Step 2: To get the correct estimation info for the given search key range [Key1, Key2], we only need to decode guide post chunks in the first and last list node, then search Key1 in the first node and Key2 in the last node, and adjust the estimated info get at the step 1 accordingly.
* Step 3: To get the total count of parallel scans, there are two cases:
    * If USE_STATS_FOR_READ_PARALLELIZATION is false (this is the setting for now and comparable to the base line), the unit of scan is region, so we just need to do binary search in region boundaries to see how many regions that the returned list crosses. The time complexity is log(m) where m is the total count of regions.
    * If USE_STATS_FOR_READ_PARALLELIZATION is true (could be enabled in the future), the unit of scan is guide post or guide posts in configured granularity, so we need to iterate the list nodes in the returned list one by one. For each node, if there is no region boundary between the key range specified by the current guide post chunk, adds the # of guide posts to the total count; otherwise decodes the guide post chunks then search region boundaries in guide posts to get the value to be added to the total count. The time complexity is O(w).

### Time Complexity Analysis

When USE_STATS_FOR_READ_PARALLELIZATION is false, in the algorithm described above, we have time complexity O(log(w)) in step 1, O(n/w) in step 2 and **O(log(m))** in step 3, where w is total count of guide post chunks, m is the total count of regions and n the total count of guide posts. If we have k search ranges specified by WHERE clause predicate filters, **the total time complexity is O(k*max(log(w), n/w, log(m))) which is multiple orders of magnitudes lower than O(k*n) with the original data structure and algorithms**. More importantly, now we avoid the expensive operation of the check of intersections with scan ranges specified by a WHERE clause predicate filters for every given range specified by either two consecutive guide posts or one guide post and one region boundary.

When USE_STATS_FOR_READ_PARALLELIZATION is true,  in the algorithm described above, we have time complexity O(log(w)) in step 1, O(n/w) in step 2 and **O(w)** in step 3. If we have k search ranges specified by WHERE clause predicate filters, **the total time complexity is O(k*max(log(w), n/w, w)), i.e. O(k*max(n/w, w)) which is still **multiple orders of magnitudes lower than** O(k*n) with the original data structure and algorithms**. 

## The solution for supporting tenant specific view and special queries

As mentioned before, we won't actually solve this problem in Phoenix Stats V1, so here only high level design will be provided for now to make sure the solution for first problem is extensible for solving the second problem.

Currently, the key of the stats client cache is <table name, column family name>. Because the tables which use stats only has 1 column family, so now every entry in the stats client cache is the guide post info for a whole table. 

To support caching stats per tenant specific view, the key of status client cache will be changed to the tuple <table name, column family name, key range>.

To support caching stats per query, we can build an index on top of stats client cache. The key of status client cache will be changed to the tuple <table name, column family name, key range>, and this is also the key for index implement by treeMap (using B* Tree provided in standard libraries). If a new entry isn't overlapped with the existing entries, it will be simply as checking and inserting the entry into both index and client cache; otherwise we can use cellingEntry/lowerEntry/higherEntry/lowerEntry APIs provided by treeMap to find the entries which have overlap with the new entry, then merge the stats of these entries into the new entry, finally update both index and the stats client cache.


## The solution for decouple the granularity stored in SYSTEM.STATS from both the granularity stored on the client and the granularity of the scanning 

A leaf node of a variant segment tree is a guide post chunk which contains a group of continuous guide posts. A guide post chunk is not only the minimal unit for compression/encoding but also the minimal unit for scanning. The guide posts of a chunk aren't necessary to be loaded into the tree and into the stats cache – in this case, the chunk / the leaf node will just contain the accumulative estimation info and the key range of that chunk.
