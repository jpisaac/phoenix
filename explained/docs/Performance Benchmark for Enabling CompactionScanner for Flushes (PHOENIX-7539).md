# **Performance Benchmark for Enabling CompactionScanner for Flushes (PHOENIX-7539)**

## **Test Setup**

* HBase Cluster setup in distributed mode on my M4 Max Macbook Pro 64GB RAM 1TB SSD 16 cores ( 12 performance \+ 4 efficiency)  
* HBase version: 2.6.2  
* Phoenix: Latest Open source master (5.3)  
* 3 processes (1 Zookeeper \+ 1 HMaster \+ 1 Regionserver)

## **Cluster Configurations**

hbase-site.xml

| \<property\>   \<name\>hbase.cluster.distributed\</name\>   \<value\>true\</value\> \</property\> \<property\>   \<name\>hbase.tmp.dir\</name\>   \<value\>/tmp\</value\> \</property\> \<property\>   \<name\>hbase.unsafe.stream.capability.enforce\</name\>   \<value\>false\</value\> \</property\> \<property\> \<name\>hbase.rootdir\</name\> \<value\>/Users/sanjeetmalhotra/Documents/local\_hbase/hbase\</value\>\</property\>\<property\>   \<name\>hbase.regionserver.wal.codec\</name\>   \<value\>org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec\</value\> \</property\> \<property\>   \<name\>hbase.rpc.controllerfactory.class\</name\>   \<value\>org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory\</value\> \</property\> \<property\>   \<name\>hbase.hstore.compactionThreshold\</name\>   \<value\>12\</value\> \</property\> \<property\>   \<name\>hbase.hstore.blockingStoreFiles\</name\>   \<value\>40\</value\> \</property\> \<property\>   \<name\>hbase.hstore.compaction.max\</name\>   \<value\>20\</value\> \</property\> |
| :---- |

   
hbase-env.sh

| export HBASE\_REGIONSERVER\_OPTS\="$HBASE\_REGIONSERVER\_OPTS \-Xmx10g \-Xms10g \-XX:+AlwaysPreTouch" |
| :---- |

Max lookback age is 0\.

## **Test plan**

Write same batch of 15M rows 10 times in a loop and note down flush time percentiles

## **On Single Column Family Table**

### **Table Schema**

| CREATE TABLE TEST.T\_WITH\_COMPACTION\_ON\_FLUSH(   ID1 INTEGER NOT NULL,   ID2 INTEGER NOT NULL,   VAL1 VARCHAR   CONSTRAINT PK PRIMARY KEY (ID1, ID2)) "phoenix.compaction.scanner.for.flushes.enabled" \= true |
| :---- |

| CREATE TABLE TEST.T\_NO\_COMPACTION\_ON\_FLUSH(   ID1 INTEGER NOT NULL,   ID2 INTEGER NOT NULL,   VAL1 VARCHAR   CONSTRAINT PK PRIMARY KEY (ID1, ID2)) "phoenix.compaction.scanner.for.flushes.enabled" \= false |
| :---- |

### **Data Generation**

Script used to create the data set:

| \#\!/bin/bash\# usage script \<FILENAME\> \<ROW\_COUNT\>filename=$1cat /dev/null \> $filename\#\# table schema\# (id1 integer not null, id2 integer not null, val1 varchar, pk(id1, id2))declare \-a rownumrows=$2uuid\_stream() { python3 \-c 'import uuidtry: while True:   print (str(uuid.uuid4()).upper())except IOError:   pass \# probably an EPIPE because we were closed.'}\# generate a file descriptor that emits an endless stream of integersexec 3\< \<(while true; do gshuf \-r \-i1-99999999; done);echo "Created file descriptor: 3"\# generate a file descriptor that emits an endless stream of UUIDsexec 4\< \<(uuid\_stream)echo "Created file descriptor: 4"for ((i=0;i\<$numrows;i++)); do IFS= read \-r val \<&3 row\[0\]="$val" IFS= read \-r val \<&3 row\[1\]="$val" IFS= read \-r val \<&4 row\[2\]="$val" echo ${row\[@\]} \>\> $filenamedoneexec 3\>&-; echo "Closed file descriptor 3"exec 4\>&-; echo "Closed file descriptor 4"echo "Data generation done" |
| :---- |

Invocation:

| ./test-data.sh test-data.csv 15000000  |
| :---- |

Generated 15M rows which were repeatedly written & flushed:

| \#\!/bin/zshif \[\[ \! \-v HBASE\_HOME \]\]; then echo "Error: HBASE\_HOME environment variable is not set"; exit 1; fitable\_name=$(echo "$1" | tr '\[:lower:\]' '\[:upper:\]')show\_metrics() { curl http://localhost:16030/jmx | grep \-e "Namespace\_default\_table\_${table\_name}\_metric\_flushTime" \-e "Namespace\_default\_table\_${table\_name}\_metric\_flushMemstoreSize" \-e "Namespace\_default\_table\_${table\_name}\_metric\_flushOutputSize"}echo "$(date) Initial value of metrics"show\_metricsfor i in {1..10}; do echo "$(date) Upserting data to the table. Batch: $i" bin/psql.py \-t ${table\_name} \-d ' ' test\-data.csvdoneecho "$(date) Final value of metrics"show\_metricsecho "Done"echo "Running Flush of memstore."echo "flush '${table\_name}'" | $HBASE\_HOME/bin/hbase shell |
| :---- |

#### 

Log line for Compaction on flushes in RS logs:

| CompactionScanner params:- (physical-data\-tablename \= , |
| :---- |

### **Summary**

10 batches of 15M rows were written flushed

| Run \# (memstore flush size) | Compaction on flushes- Flush Time (in ms) |  |  |  |  | No compaction on flushes- Flush Time (in ms) |  |  |  |  |
| :---- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
|  | count | p50 | p90 | p95 | p99 | count | p50 | p90 | p95 | p99 |
| 1 (128MB) | 286 | 549 | 566 | 572 | 572 | 286 | 394 | 416 | 416 | 420 |
| 2 (128MB) | 287 | 564 | 623 | 624 | 629 | 277 | 405 | 430 | 436 | 438 |
| 3 (128MB) | 287 | 515 | 542 | 542 | 542 | 286 | 419 | 432 | 434 | 447 |
| 4 (256 MB) | 144 | 1113 | 1136 | 1138 | 1229 | 134 | 875 | 902 | 932 | 932 |
|  |  |  |  |  |  |  |  |  |  |  |

### **Observation**

* After preFlush hook using CompactionScanner, p95 time per row to flush and pass through CompactionScanner is 1000-1200 ns.  
* Before preFlush hook using CompactionScanner, p95 time per row to flush is 790-830 ns.

## **On Multi Column Family Table**

### **Table Schema**

| CREATE TABLE TEST.T\_WITH\_COMPACTION\_ON\_FLUSH(   ID1 INTEGER NOT NULL,   ID2 INTEGER NOT NULL,   VAL1 VARCHAR,    A.VAL2 VARCHAR   CONSTRAINT PK PRIMARY KEY (ID1, ID2)) "phoenix.compaction.scanner.for.flushes.enabled" \= true, SALT\_BUCKETS \= 3 |
| :---- |

| CREATE TABLE TEST.T\_NO\_COMPACTION\_ON\_FLUSH(   ID1 INTEGER NOT NULL,   ID2 INTEGER NOT NULL,   VAL1 VARCHAR,    A.VAL2 VARCHAR   CONSTRAINT PK PRIMARY KEY (ID1, ID2)) "phoenix.compaction.scanner.for.flushes.enabled" \= false, SALT\_BUCKETS \= 3 |
| :---- |

### **Data Generation**

Script used to create the data set:

| \#\!/bin/bash\# usage script \<FILENAME\> \<ROW\_COUNT\>filename=$1cat /dev/null \> $filename\#\# table schema\# (id1 integer not null, id2 integer not null, val1 varchar, pk(id1, id2))declare \-a rownumrows=$2uuid\_stream() { python3 \-c 'import uuidtry: while True:   print (str(uuid.uuid4()).upper())except IOError:   pass \# probably an EPIPE because we were closed.'}\# generate a file descriptor that emits an endless stream of integersexec 3\< \<(while true; do gshuf \-r \-i1-99999999; done);echo "Created file descriptor: 3"\# generate a file descriptor that emits an endless stream of UUIDsexec 4\< \<(uuid\_stream)echo "Created file descriptor: 4"for ((i=0;i\<$numrows;i++)); do IFS= read \-r val \<&3 row\[0\]="$val" IFS= read \-r val \<&3 row\[1\]="$val" IFS= read \-r val \<&4 row\[2\]="$val"  IFS= read \-r val \<&4 row\[3\]="$val" echo ${row\[@\]} \>\> $filenamedoneexec 3\>&-; echo "Closed file descriptor 3"exec 4\>&-; echo "Closed file descriptor 4"echo "Data generation done" |
| :---- |

Invocation:

| ./test-data.sh test-data-multi-cf.csv 15000000  |
| :---- |

Generated 15M rows which were repeatedly written & flushed:

| \#\!/bin/zshif \[\[ \! \-v HBASE\_HOME \]\]; then echo "Error: HBASE\_HOME environment variable is not set"; exit 1; fitable\_name=$(echo "$1" | tr '\[:lower:\]' '\[:upper:\]')show\_metrics() { metrics\_file=flush\_time.txt rm \-f $metrics\_file grep "Flushed memstore" $HBASE\_HOME/logs/\*regionserver\*.log | grep "table=${table\_name}" | grep \-oE 'in \[0-9\]+ ms' | grep \-oE '\[0-9\]+' \> $metrics\_file sorted\_metrics\_file=sorted\_flush\_time.txt rm \-f $sorted\_metrics\_file sort \-n $metrics\_file \> $sorted\_metrics\_file count=$(wc \-l \< $sorted\_metrics\_file) echo "Flush count: $count" percentiles=(50 90 95 99\) for i in ${percentiles\[@\]}; do   index=$(( (count \* i \+ 99\) / 100 ))   percentile\_value=$(sed \-n "${index}p" $sorted\_metrics\_file)   echo "Flush time p${i}: ${percentile\_value}" done}echo "$(date) Initial value of metrics"show\_metricsfor i in {1..10}; doecho "$(date) Upserting data to the table. Batch: $i"bin/psql.py \-t ${table\_name} \-d ' ' test\-data-multi-cf.csvdoneecho "$(date) Final value of metrics"show\_metricsecho "Done"echo "flush '${table\_name}'" | $HBASE\_HOME/bin/hbase shell\#echo "major\_compact '${table\_name}'" | $HBASE\_HOME/bin/hbase shell |
| :---- |

### **Summary**

10 batches of 15M rows were written flushed

| Run \# (memstore flush size) | Compaction on flushes- Flush Time (in ms) |  |  |  |  | No compaction on flushes- Flush Time (in ms) |  |  |  |  |
| :---- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
|  | count | p50 | p90 | p95 | p99 | count | p50 | p90 | p95 | p99 |
| 1 (128MB) | 637 | 400 | 549 | 576 | 1622 | 651 | 334 | 395 | 415 | 1095 |
| 2 (128MB) | 660 | 402 | 548 | 568 | 997 | 650 | 333 | 389 | 406 | 1049 |
| 3 (256 MB) | 339 | 819 | 1103 | 1135 | 1196 | 339 | 661 | 786 | 798 | 827 |
|  |  |  |  |  |  |  |  |  |  |  |

### **Observation**

* After preFlush hook using CompactionScanner, p95 time per row to flush and pass through CompactionScanner is 1223-1283 ns.  
* Before preFlush hook using CompactionScanner, p95 time per row to flush is 879-902 ns.

## **Conclusion**

After preFlush hook using CompactionScanner, per row 170 \- 404 ns. Some overhead is expected and it appears small enough to conclude that enabling CompactionScanner for preFlush hook will not cause visible performance degradation.