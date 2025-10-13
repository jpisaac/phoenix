# Phoenix Stats Estimation and Scan Selection with Skip Scan; Current and Future.

This document covers the details of Phoenix guide post usage for estimation and possible scan generation in the Phoenix client.  As skip scans are one of the driving features of the adoption of Phoenix the use of stats should reflect as much as possible the optimizations that are run server side in the client side statistics.  

# Guide Post Driven Examples

Given our current guide post based statistics collection the following three examples cover what types of estimation is available during the processing of a Phoenix Skip Scan.  In all cases we have a composite primary key on the base table and occur in a single region for simplicity.  Of note in the examples is the terminology Scan Ranges which is an internal Phoenix notion that covers which logical RowKey ranges are in initial consideration.  The current approach uses essentially the first possible key and the last possible key to inform the Scan Ranges.  The new approach filters out certain intermediate unneeded ranges from the start.  A second area that is covered is the actual sever side scans that are generated.  Due to how the old code handled the boundary conditions of guideposts as being lower inclusive to upper exclusive when the data was actually lower exclusive to upper inclusive.  This in turn may affect which guide posts were considered for estimation purposes leading to incorrect estimations.

## Example 1 - Leading edge of the query is a range

There are 4 Guide Posts with end keys 1'3, 6'A, 9'A, 16'A.
Query: WHERE  ((B >= 0 AND B <= 5) OR (B >= 10 AND B <= 20) ) AND C == 'D'


|Scan Ranges (current)	|Scan Ranges (new)	|Guide Post (#, Key)	|Row #	|PK, B	|PK, C	|Retuned Data	|Scans (current)	|Scans (new)	|
|---	|---	|---	|---	|---	|---	|---	|---	|---	|
|	|	|1,1'C	|1	|0	|A	|	|	|	|
|	|	|2	|1	|B	|	|	|	|
|	|	|3	|1	|C	|	|	|	|
|	|	|2,6'A	|4	|1	|D	|	|	|	|
|	|	|5	|5	|B	|	|	|	|
|	|	|6	|6	|A	|	|	|	|
|	|	|3,9'A	|7	|7	|A	|	|	|	|
|	|	|8	|8	|B	|	|	|	|
|	|	|9	|9	|A	|	|	|	|
|	|	|4,16'A	|10	|10	|A	|	|	|	|
|	|	|11	|11	|B	|	|	|	|
|	|	|12	|12	|A	|	|	|	|
|	|	|13	|14	|B	|	|	|	|
|	|	|14	|16	|A	|	|	|	|


Scan Ranges (current): [0'D, 20'D]
Generated parallel scans (current): [0'D, 1'c), [1'c, 6'A), [9'A, 16'A), [16'A, NextKey(20'D))

Scan Ranges (new): [0'D, 5'D] [10'D, 20'D]
Generated parallel scans (new): [0'D, 1'c), [1'c, NextKey(5'D)), [10'D, 16'A), [16'A, NextKey(20'D))

**Summary**: 

1. Both current code and new code won't generate parallel scan for guide post 3. 
2. New code is more efficient as it scans less number of guide post and generates scans with refined scan ranges. 
3. With current Guide Posts design, there is not enough clue to filter guide post 1 and 4.

## Example 2 - Leading Edge of the Query is single keys

There are 3 Guide Posts with end keys B'3, B'6, C'3. 
Query: WHERE  PK1 IN ('B','C')) AND (PK1 >= 1 AND PK2 <= 2)


|Scan Ranges (current)	|Scan Ranges (new)	|Guide Post (#, Key)	|Row #	|PK1	|PK2	|Retuned Data	|Scans (current)	|Scans (new)	|
|---	|---	|---	|---	|---	|---	|---	|---	|---	|
|	|	|1, B'3	|1	|A	|1	|	|	|	|
|	|	|2	|A	|2	|	|	|	|
|	|	|3	|B	|1	|	|	|	|
|	|	|4	|B	|3	|	|	|	|
|	|	|2, B'6	|5	|B	|4	|	|	|	|
|	|	|6	|B	|5	|	|	|	|
|	|	|7	|B	|6	|	|	|	|
|	|	|3, C'3	|8	|B	|7	|	|	|	|
|	|	|9	|C	|1	|	|	|	|
|	|	|10	|C	|2	|	|	|	|
|	|	|11	|C	|3	|	|	|	|


Scan Ranges (current): [B'1, C'2]
Generated parallel scans (current): [B'1, B'3), [B'6, NextKey(C'2))

Scan Ranges (new): [B'1, B'2] [C'1, C'2]
Generated parallel scans (new): [B'1, NextKey(B'2)), [C'1, NextKey(C'2))

**Summary**:

1. Both current code and new code won't generate parallel scan for guide post 2. 
2. New code is more efficient as it scans less number of guide post and generates scans with refined scan ranges. 

## Example 3 - Guide Post Values are detailed and informative with respect to the WHERE clause

There are 3 Guide Posts with end keys B'3, B'6, C'3.
Query: WHERE (PK1 >= 'B' AND PK1 <= 'C') AND (PK1 >= 1 AND PK2 <= 2)


|Scan Ranges (current)	|Scan Ranges (new)	|Guide Post (#, Key)	|Row #	|PK1	|PK2	|Retuned Data	|Scans (current)	|Scans (new)	|
|---	|---	|---	|---	|---	|---	|---	|---	|---	|
|	|	|1, B'3	|1	|A	|1	|	|	|	|
|	|	|2	|A	|2	|	|	|	|
|	|	|3	|B	|1	|	|	|	|
|	|	|4	|B	|3	|	|	|	|
|	|	|2, B'6	|5	|B	|4	|	|	|	|
|	|	|6	|B	|5	|	|	|	|
|	|	|7	|B	|6	|	|	|	|
|	|	|3, C'3	|8	|B	|7	|	|	|	|
|	|	|9	|C	|1	|	|	|	|
|	|	|10	|C	|2	|	|	|	|
|	|	|11	|C	|3	|	|	|	|

Scan Ranges (current): [B'1, C'2]
Generated parallel scans (current): [B'1, B'3), [B'3, B'6), [B'6, NextKey(C'2))

Scan Ranges (new): [B'1, C'2]
Generated parallel scans (new): [B'1, NextKey(B'2)), [B'3, B'6), [C'1, NextKey(C'2))

**Summary**:

1. **Both current code and new code still generate parallel scan for guide post 2, even where there are enough clues, but the conditions are very restrict — to skip such guide post i for generating scan, it require the prefix of guide post (PK1 in this case) i is equal to that of guide post i-1, i.e., PK1 generally has discrete values (and if that's the case, using IN clause can skip guide post 2 as showed by example 2).** **Open question: Is this a common case for platform?**
2. New code is more efficient as it scans less number of guide post and generates scans with refined scan ranges. 


