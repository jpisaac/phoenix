# Evaluation Of Prefix Encoding Used For Phoenix Stats

# **Overview**

[Phoenix Stats](https://phoenix.apache.org/update_statistics.html) uses Prefix Encoding to compress memory used by row key byte\[\] of guideposts. Below is a short description from the [PHOENIX-2417](https://issues.apache.org/jira/browse/PHOENIX-2417) which explained why the Prefix Encoding is used:

*We've found that smaller guideposts are better in terms of minimizing any increase in latency for point scans. However, this increases the amount of memory significantly when caching the guideposts on the client. **Guidepost are equidistant row keys in the form of raw byte\[\] which are likely to have a large percentage of their leading bytes in common** ...* 

**The algorithm used by Prefix Encoding to encode data can be found at Appendix A.**

## **Problems Related to Guideposts in Prefix Encoding**

Now we found there are mainly two problems related to guideposts in Prefix Encoding:

1. The guideposts info is maintained in the client stats cache per column family. We can’t cache/retrieve guideposts info per tenant or for a particular key range.  
   Note: the comment on GuidePostInfo Class says it can be organized per region per column family, but it hasn’t been implemented yet.  
2. We have to decode and traverse guideposts sequentially, which causes time complexity of some expensive operations to increase from O(m) to be O(n) , where m is the total count of regions and n is the total count of guide posts. The result is that Query compilation, especially query complexity estimation and parallel scan generation from guideposts, becomes slower (for details, please see [Two Structural Problems In Stats](https://docs.google.com/document/d/1X1H4-4Ug_QxYE77HjlIrSs2TCKNhBxvXscvZmFH6VyY/edit#)).

## **Motivation of The Evaluation**

Before we have further optimization to solve the above problems, we need to understand how much benefit we can get by using Prefix Encoding (to compress guideposts) with real data and discover its positive/negative cases.

# **Summary of Evaluation**

## **Summary**

I mainly evaluated the benefit by using the following typical types of data:

1. Case 1: Primary Key is Sequence in INT (4 Bytes)  
   1. When GUIDEPOSTS\_WIDTH is 100MB, even in the ideal case, the data size actually increased 7.14% after compression.  
   2. When GUIDEPOSTS\_WIDTH is 10MB, even in the ideal case, the data size actually increased 3.6% after compression.  
2. Case 2: Primary Key is Sequence in BIGINT (8 Bytes)  
   1. When GUIDEPOSTS\_WIDTH is 100MB, in the ideal case, the data size reduced 6.25% after compression.  
   2. When GUIDEPOSTS\_WIDTH is 10MB, in the ideal case, the data size reduced increased 9.4% after compression.  
3. Case 3: Real Data From Platform Team  
   With the data known so far, after compression with prefix encoding, the lower bound of size reduced is roughly in the range 10% \~ 45%. I’ll continuously refine the calculation in this part after I know more about the real data.  
4. Case 4: Primary Key is Reverse URL  
   This is a typical use case of BigTable/HBase, whereas Salesforce mightn’t have it. I don’t have real data for this case, but intuitively, this might be one of the typical cases that Prefix Encoding can achieve the most benefit.

## **Takeaway**

1. Case 3 shows that our real data is a good candidate for using Prefix Encoding and guideposts data can be shrunk a lot to benefit client cache.  
2. We should allow customer to choose different compression algorithms or encoding schemes, and make it configurable.  
   Obviously, case 1 is a negative case. As Jacob pointed out, double-delta encoding should be used for case 1 and case 2\. Even for Case 3 and Case 4, prefix encoding mightn’t the best one to make tradeoff between performance and compression ratio.  
3. We should split guideposts in chunks and always encode/decode a chunk as a whole while allowing indexed access across chunks. In this way, we can only cache/fetch part of guideposts of the table and facilitate tenant/view specific query. 

# **Details of Evaluation**

## **The Basic Formula**

Basically the stats client cache ([Google Guava Cache](https://github.com/google/guava/wiki/CachesExplained)) is a map\<K, V\>, where K is “Table Name \+ Column Family Name”, V is GuidePostsInfo Class. After skipping the fields whose contribution to the object size can be ignored, the GuidePostsInfo Class can be simplified as:

| public class GuidePostsInfo {     // All guideposts per column family in Prefix Encoding.    private final ImmutableBytesWritable guidePosts;         // The rowCounts of each guidePost traversed    private final long\[\] rowCounts;         // The bytecounts of each guidePost traversed    private final long\[\] byteCounts;    // The timestamps at which guideposts were created/updated    private final long\[\] gpTimestamps; } |
| :---- |

After ignoring those one-time object sizes, the size of a GuidePostInfo object is:  
Gc \+ (3 \* (size of long \* N)) → Gc \+ 24N  
Where **N** is the total count of guideposts and **Gc** is the size of guideposts compressed with Prefix Encoding.

Without compression, the guideposts is an array of row key byte\[\], and the average size of row key is denoted as **K**, so the size of a GuidePostInfo object is:  
KN \+ 24N  
**The benefit of using prefix encoding can be quantified as (1 \- compressed size / uncompressed size), that is:**  
**1 \- (Gc \+ 24N) / (KN \+ 24N)**

# **Case 1: Primary Key is Sequence in INT (4 Bytes)**

## **GUIDEPOST\_WIDTH is 100MB**

A typical guidepost lis is

| 0 25,000,000 50,000,000 75,000,000 ... |
| :---- |

Every two consecutive guideposts don’t have common prefix in bytes. Starting from the second guidepost, given a guidepost, 6 bytes will be appended to compressed data buffer (for details of calculation, please see Appendix A), so the data size increased 50% after applying prefix encoding to original data. 

The benefit is a negative number:   
1 \- (Gc \+ 24N) / (KN \+ 24N) \= 1 \- (6N \+ 24N) / (4N \+ 24N) \= \-0.0714

## **GUIDEPOST\_WIDTH is 10MB**

A typical guidepost lis is

| 0 2,500,000 5,000,000 7,500,000 ... |
| :---- |

Every two consecutive guideposts have 1-byte common prefix. Starting from the second guidepost, given a guidepost, 5 bytes will be appended to compressed data buffer (for details of calculation, please see Appendix A), so the data size increased 25% after applying prefix encoding to original data. 

The benefit is a negative number:   
1 \- (Gc \+ 24N) / (KN \+ 24N) \= 1 \- (5N \+ 24N) / (4N \+ 24N) \= \-0.036

# **Case 2: Primary Key is Sequence in BIGINT (8 Bytes)**

## **GUIDEPOST\_WIDTH is 100MB**

A typical guidepost lis is

| 0 25,000,000 50,000,000 75,000,000 ... |
| :---- |

Every two consecutive guideposts have 4-bytes common prefix. Starting from the second guidepost, given a guidepost, 6 bytes will be appended to compressed data buffer (for details of calculation, please see Appendix A), so the data size reduced 25% after applying prefix encoding to original data. 

The benefit is:   
1 \- (Gc \+ 24N) / (KN \+ 24N) \= 1 \- (6N \+ 24N) / (8N \+ 24N) \= 0.0625

## **GUIDEPOST\_WIDTH is 10MB**

A typical guidepost lis is

| 0 2,500,000 5,000,000 7,500,000 ... |
| :---- |

Every two consecutive guideposts have 5-bytes common prefix. Starting from the second guidepost, given a guidepost, 5 bytes will be appended to compressed data buffer (for details of calculation, please see Appendix A), so the data size increased 12.5% after applying prefix encoding to original data. 

The benefit is:   
1 \- (Gc \+ 24N) / (KN \+ 24N) \= 1 \- (5N \+ 24N) / (8N \+ 24N) \= 0.094

# **Case 3: Real Data From Platform Team**

## **About Data(Schema, Data Shape and Data Size)**

The data has a base table named “CUSTOM\_ENTITY.CUSTOM\_ENTITY\_DATA\_NO\_ID” with a bunch of views defined and created by customer with Phoenix Dynamic Columns. Both Base Big Obj Table Size MB and Big Obj Table size TB are about 1.35TB. The total count of row is 684M with average row size about 2KB.

The schema of the base table is:

| CREATE TABLE IF NOT EXISTS CUSTOM\_ENTITY.CUSTOM\_ENTITY\_DATA\_NO\_ID (    ORGANIZATION\_ID CHAR(15) NOT NULL,    KEY\_PREFIX CHAR(3) NOT NULL,    CREATED\_DATE DATE,    CREATED\_BY CHAR(15),    SYSTEM\_MODSTAMP DATE    CONSTRAINT PK PRIMARY KEY (        ORGANIZATION\_ID,        KEY\_PREFIX    )) VERSIONS=1, MULTI\_TENANT=true, IMMUTABLE\_ROWS=TRUE, REPLICATION\_SCOPE=1; |
| :---- |

The primary key of the views contains three leading columns ORGANIZATION\_ID, KEY\_PREFIX and ENTITY\_KEY\_PREFIX plus user defined dynamic columns which are unknown.

Data Shape Group BY BIG OBJECT

| TABLES AND VIEWS | ROWS | ORGANIZATION\_ID |
| ----- | ----- | :---- |
| CUSTOM\_ENTITY.CUSTOM\_ENTITY\_DATA\_NO\_ID | 684 M | ALL |
| CUSTOM\_ENTITY."z03" | 110 M | 00Dx0000000GyYS |
| CUSTOM\_ENTITY."z0J" | 500 M | 00Dx0000000GyY9 |
| CUSTOM\_ENTITY."z0N" | 31 M | 00Dx0000000GyYS |

\+-----------------+------------+------------------------------------------+--------------------+--------------------+  
| ORGANIZATION\_ID | KEY\_PREFIX | ENTITY\_KEY\_PREFIX | ROW\_COUNT | ROW\_SIZE |  
\+-----------------+------------+------------------------------------------+--------------------+--------------------+  
| 00Dx00000000MMp| 0RM | z00 | 2 | null |  
| 00Dx0000000GyY9 | 0RM | z0I | 0 | 0 |  
| 00Dx0000000GyY9 | 0RM | z0J | 499999064 | null |  
| 00Dx0000000GyY9 | 0RM | z0K | 0 | 0 |  
| 00Dx0000000GyY9 | 0RM | z0L | 4E+2 | null |  
| 00Dx0000000GyY9 | 0RM | z0S | 0 | 0 |  
| 00Dx0000000GyY9 | 0RM | z0T | 0 | 0 |  
| 00Dx0000000GyY9 | 0RM | z0U | 0 | 0 |  
| 00Dx0000000GyYS | 0RM | z03 | 110779964 | null |  
| 00Dx0000000GyYS | 0RM | z04 | 3758354 | null |  
| 00Dx0000000GyYS | 0RM | z05 | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z06 | 4377562 | null |  
| 00Dx0000000GyYS | 0RM | z07 | 4697342 | null |  
| 00Dx0000000GyYS | 0RM | z08 | 1999513 | null |  
| 00Dx0000000GyYS | 0RM | z09 | 49998 | null |  
| 00Dx0000000GyYS | 0RM | z0A | 199973 | null |  
| 00Dx0000000GyYS | 0RM | z0B | 999885 | null |  
| 00Dx0000000GyYS | 0RM | z0C | 499953 | null |  
| 00Dx0000000GyYS | 0RM | z0D | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z0E | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z0F | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z0G | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z0H | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z0I | 3698316 | null |  
| 00Dx0000000GyYS | 0RM | z0N | 31676889 | 0 |  
| 00Dx0000000GyYS | 0RM | z0O | 0 | 0 |  
| 00Dx0000000GyYS | 0RM | z0P | 0 | 0 |  
\+-----------------+------------+------------------------------------------+--------------------+--------------------+

## **Calculation**

We know the primary key of a Big Object Table is in the format of \[ORGANIZATION\_ID, KEY\_PREFIX, ENTITY\_KEY\_PREFIX, plus dynamic columns\]. With this data shape as described above, we safely assume every two consecutive guideposts have the same \[ORGANIZATION\_ID, KEY\_PREFIX and ENTITY\_KEY\_PREFIX \] with GUIDEPOST\_WIDTH 10MB, 100MB.  Because so far we don’t know about the dynamic columns, so we can roughly measure the benefit in two ways:

1. Assume there is only one dynamic column in BIGINT type which increases sequentially. In this way, we can calculate the benefit which give us some rough idea. The lower bound of how much benefit we can get is decided by ration of the length of the leading compressible part of the dynamic columns and its incompressible part.  
1) GUIDEPOST\_WIDTH is 100MB

   Every two consecutive guideposts have 25-bytes common prefix. Starting from the second guidepost, given a guidepost, 6 bytes will be appended to compressed data buffer. 

   The benefit is: 

   1 \- (Gc \+ 24N) / (KN \+ 24N) \= 1 \- (6N \+ 24N) / (29N \+ 24N) \= 0.434

   

2) GUIDEPOST\_WIDTH is 10MB

   Every two consecutive guideposts have 26-bytes common prefix. Starting from the second guidepost, given a guidepost, 5 bytes will be appended to compressed data buffer. 

   The benefit is: 

   1 \- (Gc \+ 24N) / (KN \+ 24N) \= 1 \- (5N \+ 24N) / (29N \+ 24N) \= 0.453

   

2. Assume we don’t compress \[ORGANIZATION\_ID, KEY\_PREFIX and ENTITY\_KEY\_PREFIX \] then calculate the how much the cache size will increase in absolute value. In this way, we can calculate the lower bound of the benifit.  
1) GUIDEPOST\_WIDTH is 100MB

   We have 13.5K guideposts, so the original size of guideposts without compression is:

   13.5K \* (15 \+ 3 \+ 3\) \= 283.5 KB

   It means at least 283.5KB will be added to 2.9MB, the current compressed guideposts info, i.e., the cache size will increase about 10%.

2) GUIDEPOST\_WIDTH is 10MB

   Same result as the previous case.

# **Case 4: Primary Key is Reverse URL**

This is typical use case of BigTable/HBase, whereas Salesforce mightn’t have it. I don’t have real data for this case, but intuitively, this might be one of typical cases that Prefix Encoding can achieve the most benefit.

# **Appendix A \-- Prefix Encoding**

The algorithm used by Prefix Encoding to encode data is \-- given a list of byte\[\] to compress, for each byte\[\], compare it with the previous byte\[\] to detect the common prefix (the length of the common prefix is denoted as n, the remaining bytes after the common prefix in current byte\[\] is denoted as B), then append n (use one byte in most of cases) to the buffer holding compressed data, then append B as byte array (one byte for array size in most of cases \+ B).

For example:  
Give the list of data:  
12345678  
12341234  
12312345

The compressed data is:  
 123456784412343512345  
