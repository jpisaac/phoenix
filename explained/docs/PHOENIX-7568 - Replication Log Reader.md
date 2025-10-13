# \[In-Progress\] [PHOENIX-7568](https://issues.apache.org/jira/browse/PHOENIX-7568) \- Replication Log Reader

This document describes the detailed design for Replication Log Reader, which is a component of [Phoenix HA Rearchitecture for Consistent Failover](https://docs.google.com/document/d/1U2ULZ1xENDxGhAxdy5U0JYGVO5790VR5AOAJMiXHIPc/edit?tab=t.0#heading=h.pfjicxriktfm), responsible for processing mutation logs received from the source cluster and applying those mutations to the target cluster.

There are 2 approaches described with respect to co-ordination \- [Decentralized Coordination](#bookmark=id.6vu1kyjs5f1v) (Suggested) & [Centralized Coordination](#bookmark=id.20iw8i1bd2yc). This doc also describes the structure for HDFS IN directory (where replication log files will be stored by source cluster) and ZK node structure for each approach. 

Towards the end, there is also comparison of different state tracking mechanism apart from ZK (i.e. Phoenix Tables and Atomic File rename) and due to the disadvantages listed, ZK is chosen for state tracking in both the designs.

## Requirements / High Level Steps

1. Process mutation logs received from the active cluster efficiently.  
2. Apply mutations to the target cluster in the correct order, maintaining data integrity.  
3. Track commit status of each mutation and delete processed log files.  
4. Support recovery and minimize redundant processing after restarts.  
5. Finding the last consistency point must be efficient (to be used by compaction, SCN queries on standby cluster).  
6. Coordinate with Phoenix Compaction to prevent premature data removal.  
7. Handling HA state updates for below scenarios  
   1. Keep a watcher on HA group states, if it’s STANDBY\_TO\_ACTIVE state and all replication files are replayed for that particular HA group, mark the state as Active on current cluster and STANDBY on previously Active cluster.  
   2. In case when standby cluster is not able to process the log files, update the standby cluster state from STANDBY to DEGRADED\_STADNBY\_FOR\_READER (and vice versa once the cluster again starts processing the log files successfully)

## Approach 1 \- Decentralized Coordination

Each RegionServer (RS) independently lists the shards from HDFS IN directory periodically and process the pending replication log files, where co-ordination is done via ZK. There is no Master Process involved for state management or any other use-case. 

### HDFS IN Directory Structure

Each replication log file will be written to configurable (via *phoenix.ha.replication.log.standby.hdfs.path*) HDFS IN directory by source cluster. For rest of this section, base HDFS IN directory is assumed to be */phoenix/replication/logs/*

The HDFS IN directory would be divided into further sub-directories equal to number of shards (configurable via [*phoenix.ha*](http://phoenix.ha)*.replication.log.directory.shards.count*, default value to upper bound on number of RS in the cluster, i.e. 500\) and one RS will be processing all the files of single shard. One RS can process multiple shards

Source cluster would write the log file in one of the shard based on generating a random number (with region-server-id of source and timestamp when file is written), module by number of shards.

Each new log file will be written in below structure

```
/phoenix/replication/logs/shard-xyz/<timestamp>_<source-rs-id>.plog
```

where,

* *shard-xyz*: Sub-directory representing a shard. \`xyz\` is the shard number (e.g., shard-000, shard-001, … shard-xyz based on configuration *phoenix.ha.replication.log.directory.shards.count*).  
* *source-rs-id*: The ID of the Region Server on the source cluster that created the log file.  
* *timestamp*: The timestamp indicating when the log file was created.  
* *.plog*: The file extension indicating it is a Phoenix replication log.

And HDFS IN directory structure would look like

```
/phoenix/replication/logs/
├── shard-000/
│   ├── <timestamp>_<source-rs-id>.plog 
│   ├── <timestamp>_<source-rs-id>.plog
│   └── ...
├── shard-001/
│   ├── <timestamp>_<source-rs-id>.log
│   └── ...
...
├── shard-xyz/
│   ├── <timestamp>_<source-rs-id>.log
│   └── ...

```

A consistent point would be defined as   
**Requirements from Replication Log Writer (Source Cluster)**

1. Writer needs to ensure that log files are written in above shard structure.  
   1. The root directory (e.g. “/phoenix/replication/logs/”) will be configurable using the site configuration parameter phoenix.replication.log.standby.hdfs.url . This is the IN directory.  
2. The distribution of files among shards must be uniform (as much as possible). We can use region-server-id and timestamp as seed \- to ensure even when single RS is writing multiple files (during Migrations) modulo max number of shards, it would still be uniformly distributed. Another way is to just use the sequence number of log file modulo max number of shards.  
   1. The maximum number of shards (modulo) will be configurable using the site configuration parameter phoenix.replication.log.shards .  
   2. Shard directories have the format shard-NNNNN, e.g. shard-00001.  
   3. The maximum value of phoenix.replication.log.shards is 100000\. If values larger than this are found the code should raise an exception.

### 

### Shard Discovery and Assignment

#### Steps

1. Each RegionServer (RS) independently generates the shard list based on configured shard count (phoenix.replication.log.shards) at regular configurable interval (default could be 1 min).  
   1. This is assuming that we source would not be generating any shards that are not in expected format (shard-NNNNN). Shards are pre-generated by RS (instead of listing from HDFS) to reduce the number of HDFS list operation call for list of shards (every configurable interval time and by every RS).  
2. Start iterating over each shard one by one and try to acquire lock.  
   1. Once RS list down all the shards (in sorted order), each RS would start from it’s respective shard number, i.e. rs-1 would start from shard-001, rs-2 would start from shard-002, rs-3 would start from shard-003 and so on (This is to minimize the concurrent ZK calls from different RS)  
   2. Each RS would move to next shard (in circular manner, i.e. if it reach the last shard, jump back to first shard) in the list after going through either 3rd or 4th step  
   3. This would continue till RS reach the same shard from which it started.  
3. If RS is not able to acquire lock (in ZK)  
   1. skip and move to next shard (step 2).  
4. Else RS acquired the lock in ZK  
   1. by adding an ephemeral node as /phoenix/replication/locks/shard-001 (ephemeral node) and value as \<region-server-id\>  
   2. List all the replication log files within this shard  
   3. Process the log files one by one. This would have configurable number of executor threads (default as 4), so it can process that many replication log files in parallel (Details of replication log file processing are added below)  
   4. Once done, delete the previously created ephemeral node and move to next shard.  
   5. If there are more files added to the same shard after list operations, those will be processed next time when either same or some other RS take up lock on this shard.

#### Implementation Details

1. These steps would be done by an implementation of Region Server level co-proc (let’s call it **ReplicationLogDiscovery**).

### 

### Replication Log File Processing

#### Steps

1. ~~Try to acquire a ephemeral lock (in ZK) on path */phoenix/replication/files/\<file\_name\>.plog* (before start processing). This is defensive mechanism to ensure each replication log file is processed only by one RS.~~ This can be (and will be) skipped during implementation to reduce ZK calls.  
2. If lock is not acquired, return. Else jump to step 3\.  
3. Set a configurable batch size on number of entries from single replication log file to be read/processed at once in memory. This is to avoid OOM issues by reading a large file at once  
   1. In HBase replication, replication RPC has default max size of 64 MB and max number of entries as 25000 ([code reference](https://github.com/apache/hbase/blob/fbf310ea6310f9ba1ce9e24cfeb8275aa09ef921/hbase-server/src/main/java/org/apache/hadoop/hbase/replication/regionserver/ReplicationSourceWALReader.java#L100-L101))  
   2. Max size of each replication log file is around 256 MB (compressed) and 512 (un-compressed) (as per the Phoenix HA design doc)  
   3. Let’s say each entry in file is around 10 KB (un-compressed)  
   4. The default value can be 64000, so we read in batch of 64 MB per RS (same as current HBase replication) and single replication log file is read in at max 8 batches (512 / 64\)  
4. Read the entries (using [LogFileReader](https://github.com/apache/phoenix/blob/5113df11e96faa9932ad17cb86ed84bac49b0aa8/phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileReader.java)) from log file in above configured batch size, and for each batch  
   1. Group the entries based on table  
   2. For each table, submit the future and wait for all the futures to complete, each would be applying batch mutate for single table via async hbase client  
   3. After successful / failed processing of log file, this will also call ReplicationLogTracker.markeFinished() / ReplicationLogTracker.markFailed() accordingly.

   

#### Implementation Details

1. We can name it as ReplicationLogProcessor  
2. This is similar to [Replication.replicateEntries()](https://github.com/apache/hbase/blob/5dafa9e3224a1a337643642d296640b72a43f74a/hbase-server/src/main/java/org/apache/hadoop/hbase/replication/regionserver/ReplicationSink.java#L196) logic in HBase async-replication, so it can be reference during implementation.

### Replication Log Tracking

1. This would be a helper class to maintain state of files via managing the interaction with ZK and HDFS.  
2. It will be used by ReplicationLogProcessor and ReplicationLogDiscovery.  
3. Maintains a ZK connection and keep track for shards and replication log file in ZK (i.e. if it’s being processed by particular shard).  
4. When a process will acquire lock on shard, it will add an entry in ZK for the file path and value as rs-hostname:port  
5. Provide methods like markProcessing(Path), markCompleted(Path), markFailed(Path), getStatus(Path)

### 

### Optimization for getting Consistent Point

A consistent point in time for a set of tables (for example a data table and its indexes) in a standby cluster is defined such that all mutations whose timestamp less than this consistency point timestamp have been replayed on these tables. In this approach the consistent point in time is defined by the minimum timestamp value across all files among the shards. This would have complexity of O(S) \+ O(N), where S is the number of shards and N is the number of files (across all shards).

However, we can maintain one directory in HDFS, i.e. /phoenix/replication/consistentpoints/  
That will have files like   
\-/phoenix/replication/consistentpoints/shard-001\_\<timestamp\>  
\-/phoenix/replication/consistentpoints/shard-002\_\<timestamp\>  
... etc

Where timestamp is last consistent checkpoint for a particular shard. After processing a shard, RS would update the respective consistent point file by renaming it to a new consistent point.

If the shard was empty, value will be set to current time. If all the files listed initially by RS are processed successfully, value will be set to timestamp of last successful file processed. If RS failed to process any file, value will be set to timestamp of last file which was successfully processed before first file failure is observed. If RS was not able to process any file successfully, shard’s consistent point will not be updated. 

To get the consistent point, we need to take a minimum timestamp across all the shard files in `/phoenix/replication/consistentpoints` directory which can be done with 1 HDFS list operation \+ O(S) for iteration over S file names.

### Why Sharding is Required?

If shards are not there, in worst case scenario, every RS could make a ZK call to check if any process has acquired lock on file or not.

Let’s say there are N number of region servers and at any given time of time, NUM\_FILES to be replicated, so total number of ZK call would be O(N \* NUM\_FILES) at any given point of time. This could be in millions if number of files are very large.

If we keep S shards (in range of number of RS on the cluster, let’s say 300\) and each RS would acquire lock on single shard and process all of it’s replication files, the number of ZK calls in worst case would O(S \* N ) (in rage of thousands)

**Note**: Another major advantage of sharding is it will reduce the number of HDFS list operations, i.e. in above approach, each replication log files would be listed once (or 2-3 times in worst case when RS processing the first file dies), however if we don’t have shards, each file will be listed by almost all the region servers (300-400 times).

### 

### ZK Node Structure

```
/phoenix/replication
├── shards/           
│   ├── shard-001     # Ephemeral (created by RS which takes lock on shard-001)
│   ├── shard-002     # Ephemeral (created by RS which takes lock on shard-002)
│   └── ...            
│
├── consistentPoint/           
│   ├── shard-001     # Persistent node (value is updated by RS who takes lock on shard-001)
│   ├── shard-002     # Persistent node (value is updated by RS who takes lock on shard-002)
│   └── ..
```

Value will be rs-hostname for both the paths, i.e. shards ownership and files ownership

### Evaluation

**Pros**

1. **No single point of co-ordination \-** Avoids master service dependency and easier to implement and no additional load on HMaster  
2. **Scalable \-** System is horizontally scalable with RS auto-scaling

**Cons**

1. **Possibility of ZK Contention \-** Even after dividing in shards, in worst case scenario, number of request to ZK could continually be O (NUM\_OF\_RS \* NUM\_OF\_SHARDS) \- which could be in order of 50 K or so \- but this will be minimized by random shuffling of shards by *ReplicationLogDiscovery*.

## Approach 2 \- Centralized Coordination

This approach involves 2 major components Master Co-Proc (for state management) and RegionServer Co-proc (for processing of replication log files)

For rest of the section ReplicationLogReaderMaster represents the master co-proc and ReplicationLogReader represents the RS co-proc.

### HDFS IN Directory Structure

This would be same as Approach 1 [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.lcurj0lbgqmo)

### ZK Node Management

```
/phoenix/replication
├── readers/              # Persistent node created by master
│   ├── rs-1              # Ephemeral (created by RS-1 on startup)
│   ├── rs-2              # Ephemeral (created by RS-2 on startup)
│   └── ...               # Used by Master to detect active RS list
│
├── assignments/          # Persistent node, created by master
│   ├── rs-1/             # Persistent (created/managed by Master or RS)
│   │   ├── shard-001     # Persistent (created/managed by Master)
│   │   └── shard-002
│   ├── rs-2/		      # Each RS keeps a watcher on it's own Z node
│   │   └── shard-003
│   └── ...
│
├── files/                     # Persistent (created by Master)
│   ├── log-123456.processing  # Ephemeral (created by RS during processing)
│   ├── log-123457.processing
│   └── ...                    # Ensure only single RS is processing a file


```

#### Tracking Available RS and Shard Assignment

Each RS creates an ephemeral node to let master know that it’s available for shard processing in below ZK structure. **This list is already maintained at */hbase/rs/* in ZK, so same can be leveraged instead of each RS keeping another node**.

```
/phoenix/replication/readers/rs-X
```

ReplicationLogReaderMaster periodically (at configurable interval, default value as 1 min) list out all the child under /phonix/replication/readers \- to figure out available RS for shard assignment (let’s say *availableRSList*).

ReplicationLogReaderMaster deletes the shards ZK nodes in /phonix/replication/assignments directory for RS which are not active (not in *availableRSList*).

ReplicationLogReaderMaster list down all the shard ids (since it’s pre-configured) \- lets say *allShards*.

ReplicationLogReaderMaster list down all the assigned shards, i.e. within /phonix/replication/assignments directory \- let’s say *assignedShards*.

ReplicationLogReaderMaster calculate the difference between assignedShards and allShards \- let’s say *remaingShardsToBeAssigned*.

For each of the remaining shard, master assign the shard to RS that has minimum number of shards assigned.

ReplicationLogReaderMaster would also ensure that no RS has more than MAX\_NUM\_SHARDS\_PER\_RS (configurable)

Each RS would keep a ZK watcher on persistent node /phonix/replication/assignments/rs-xyz to watch if any new shards are assigned to the RS

**Processing of each shard would be same as described in Approach 1 above**:[PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.6vu1kyjs5f1v)

### 

### Evaluation

**Pros**

1. **Less number of ZK interaction per shard** (ideally once per shard, instead of multiple RS trying to acquire same shard in previous design)

**Cons**

1. **More complex design (and state management in ZK)** and **Master Component could be single point of failure.**

## \[Suggested & Finalized\] Approach 3 \- Decentralized Without Coordination

This is a decentralized approach without using an explicit coordinator such as Zookeeper or HMaster, similar to the approach implemented [here](https://git.soma.salesforce.com/bigdata-packaging/bigdata-perf/blob/cca1569270b147228eba87164c27134c8696170e/PhoenixScenarioTest/src/main/java/com/salesforce/phoenixscenariotest/profilestore/ProfileStoreUseCase.java#L219).

In this section, we assume that there is one failover HA group for the sake of simplicity. Each HA group will have its own replication context, directories and cluster states, and the replication for one HA group will progress independently from the rest.

In this approach, we use the HDFS rename operation and java.util UUID\#randomUUID(). The primary cluster writes the replication log files for a given round to the same IN directory. There could be more than one IN directory in a day, in order to limit the number of files within a directory. For example, there can be 128 IN directories and the rounds are assigned to a directory using a modulo arithmetic such that the first round in a day is assigned to the first directory and the second round to the second and so on. This means typically there will be one round in each directory unless the log replay is delayed for 128 rounds which will be rare.  We also need one more IN directory called IN-PROGRESS. This directory is used during replay of log files.

### Replaying a Log File

Replaying a log file from one of these two directories (the current IN directory for a region server, the IN-PROGRESS directory) starts with moving the file to the IN-PROGRESS directory under a new name using the file system rename operation. The file name for a file form IN directory changes from \<timestamp\>-\<region server name\>.plog to  \<timestamp\>-\<region server name\>-\<UUID\>.plog. The file name for a file from the IN\_PROGESS directory changes from \<timestamp\>-\<region server name\>-\<UUID\>.plog to  \<timestamp\>-\<region server name\>-\<another UUID\>.plog. 

If the rename is successful, then the file is opened and its content is replayed. After that the file is deleted. The delete operation may fail because there is a possibility that the file can also be picked up and renamed by another region server as described later. The original file name is included in the prefix of the new name of the file.  The region server will attempt to delete the file. If the file delete fails, the region server checks the IN-PROGRESS directory to find the file using the prefix of the file name. If it does, then it attempts to delete the file again. It can repeat this delete and check and can give up after some predefined number of attempts.

The following figure depicts how replication log files are written directly in the synchronous mode, and first written locally and then copied in the store-and-forward by the active cluster, and how they are replayed by the standby cluster. In the rest of this section, we complete the description of the replay procedure in the standby cluster and then describe the store-and-forward procedure on the active cluster. As the figure implies, these procedures are similar.

### Round-by-Round Replaying Log Files from IN Directories

A region server processes an IN directory one round at a time. Processing a round by a region server means attempting to replay all files for that round from the corresponding IN directory.  Every region server maintains an in-memory variable for currentRoundTimestamp, which is the starting timestamp of the current round. After processing a round from an IN directory and updating its currentRoundTimestamp, a region server may process the IN-PROGRESS directory. 

The algorithm described so far will replay all files from an IN directory if no failure happens. These files will be moved to the IN-PROGRESS directory and then replayed and removed.  If a failure happens during replaying a file, the file will not be deleted.  Since the majority of the log files will be replayed successfully from the IN directory, not all region servers need to process the files in the IN-PROGRESS directory at each round. Only a small percentage of the region servers can process these directories. After completing processing IN directory, a region server checks if it is time to process the IN-PROGRESS directory. This check will be implemented with a random number generator which returns true with a predetermined probability. For example, the probability of returning true for this check can be 0.05 meaning only 5% of the region servers will process the IN-PROGRESS directory for a given round. 

Processing an IN directory by a region server starts with listing the files in the IN directory. Although region servers replay log files round by round, different region servers can start processing the same round at different times. This is because replaying time for a log file can be different from the replaying  time for another. After identifying the list of files for the current round, the region server randomly picks one of these files and attempts to replay using the process starting with renaming the file as described before. If the file replay is successful or not, it repeats the processing for the current round until there is no file left for the current round in the IN directory. This means that each time before attempting to pick another file, a region server lists the files in the IN directory for the current round. It is expected that each time it gets a shorter list.  
     
It is clear that if region servers start processing the same round at different times, the probability of picking the same file will be less. The worst case happens when all of them process the same round almost at the same time and get the same file lists. Let us assume that the number of files and the number of regions servers are the same. This will be a typical scenario especially in future when each cluster will be ACTIVE for one HA group and STANBY for another. 

Now let us calculate the probability of a specific file not picked by any of the region servers. Since there are N files, the probability of not picking that file by a given region server is 1 \- 1/N. Since there are N region servers, the probability of a specific file not picked by any of the region servers will be (1 \- 1/N)N.  The probability for N \= 400 and N \= 100 are 0.3672 and 0.3660, respectively. This means that 63% of the files will be successfully renamed in the first attempt. 37% of the region servers will attempt to pick their files a second time and this time there will be 37% of the files to be renamed. The maximum number of retries will be log2.7(N) and the total number of rename attempts will be less than 1.6  N from the IN directory for the current round as explained below.

When all region servers attempt to rename files at the same time for the same round, there will be N rename operations and 37%  of the rename operations will fail in the first attempt. This means 37% of region servers will reattempt to rename the remaining files (0.37 N) in the IN directory as the number of region servers and the number of files are assumed to be the same and N. The maximum number of files to be renamed will be 0.37 N. This pattern will continue until all files are renamed. The sum of the files to be renamed is the geometric series a \+ ar \+ ar2 \+ ar3 \+ … \=  a/(1 \- r)  where a \= N and r \= 0.37. The sum is \= 1.59 N and thus the total number of renaming operations for the files in an IN directory will be less than 1.6 N. 

### Replaying Log Files from IN-PROGRESS

The region server processes older files (files from the earlier rounds) in the IN-PROGRESS directory. A region server picks the next file to process randomly using a random number generator. Please note processing the IN-PROGRESS directory does mean emptying them up as replaying a file may fail in the middle and also only the older files are processed and the files for the current round are processed in the next round. This is the reason a fraction of region servers attempt to replay all older files in the IN-PROGRESS directory after processing the current round.

It will be rare that the above process will result in replaying a log file concurrently by two region servers. This can happen only when one region server is replaying a file, the other region server renames it. Only renamed files can be replayed and these files can be only in the IN-PROGRESS directory. If a region server sees files in the IN-PROGRESS directory, it processes the files from older rounds to allow the files from the current round to complete. Although there is no guarantee that such a wait operation (by skipping the current round files) will be sufficient to let in-progress replay operations complete, the longer the region server waits the less likely redundant replay happens. 

Let assume that round time is 1 minute meaning the log files include at most one minute write load for a given region server. We can safely assume the log files will be replayed faster than the write time usually meaning that a log file will be replayed within less than a minute usually since replay can use large batches. This is achieved by loading a large number of log records in memory and then creating batches of mutations from these records, a separate batch for each table. Thus, the total wait time can be set to one or more round time intervals. The region server that finishes processing the IN directory may process the files in the IN-PROGRESS directory that are from rounds that are older than its current round. 

### When and Which Round to Replay

When a region server restarts, it needs to find which round to replay. If the standby cluster is in the state STANBY or DEGRADED\_STANBY\_FOR\_READER,  it first checks if the IN-PROGRESS directory includes any files. If it does, the starting round to replay will be the minimum timestamp of these files. If the IN-PROGRESS directory is empty, the region server identifies the earliest round to replay by checking the timestamp of the files in IN directories.

If the standby cluster is in the state DEGRADED\_STANBY\_FOR\_WRITER when a region server restarts, then the region server identifies the earliest round to replay by checking the timestamp of the files in IN directories and the IN-PROGRESS directory. 

Processing of a round starts only if the current wall clock time is ahead of currentRoundTimestamp more than a round time interval. The minimum delta between current time and the round time can be one round time interval plus some configurable time buffer (a fraction of the round time interval). 

currentRoundTimestamp determines which IN directory and round to process. Upon moving a round's files to the IN-PROGRESS directory, a region server updates this timestamp to track the next round. If a standby cluster becomes DEGRADED\_STANBY\_FOR\_WRITER, log files might be delayed, necessitating a recalculation of currentRoundTimestamp. This recalculation does not require scanning all IN directories. A region server can use lastRoundInSync to remember the last fully processed round during sync mode, and scanning restarts from there. After a region server restart, all IN directories are scanned.

### Consistency Point

A consistency point in a standby cluster is defined as the timestamp such that all mutations whose timestamp less than this consistency point timestamp have been replayed. In this approach the consistent point in time is defined by the minimum timestamp of all files in the IN-PROGRESS directory if they are not empty when the standby cluster is in the STANBY or DEGRADED\_STANBY\_FOR\_READER state. If these directories are empty, and the replication is in the sync mode, then the currentRoundTimestamp is the consistency point. 

When the standby state becomes DEGRADED\_STANBY\_FOR\_WRITER meaning the replication switches to the store-and-forward mode, region servers store the current consistent point in their lastRoundInSync local variable and use lastRoundInSync as the consistent point until the state of the standby cluster becomes STANDBY. If the lastRoundInSync is undefined, meaning that when the region server starts, the standby cluster is in the DEGRADED\_STANBY\_FOR\_WRITER state, the consistency point is the minimum of the timestamp of the first round to replay and the last store-and-forward starting timestamp. The last store-and-forward starting timestamp is obtained from the HA store record cached by HA Store Manager. While writing this, the HA store record does not have this field, either we can append this timestamp to the record or use the existing version number in the record as a timestamp field.

### Log Replay When Active in Store-and-Forward Mode

The log replay when the active cluster in the store-and-forward mode, that is, when the standby cluster in the DEGRADED\_STANBY\_FOR\_WRITER state is not very different from that when the active cluster in the synchronous mode except when currentRoundTimestamp catches up with the current wall clock time. In that case, currentRoundTimestamp is set to lastRoundInSync and the region server pauses for some time before starting to reprocess the corresponding IN directory. Please note that reprocessing IN directories is needed in order not to skip the log files that are transferred with a significant lag. 

In this mode, region servers may also pause after each round if the corresponding IN directory is empty. The pause time can be chosen randomly from a range, say between zero and the round duration.

### Store-and-Forward Log Replication

The approach described here can be adapted for the store-and-forward log replication in the active cluster. In this case, the logs are written to the OUT directories local to the active cluster in the store part of the store-and-forward replication instead of the IN directories of the standby cluster. The OUT-PROGRESS directory in the active cluster corresponds to the IN-PROGRESS directory in the standby cluster. The forward part is handled in the active cluster in a similar way to how log replay is handled in the standby cluster except that instead of writing log content to the HBase tables, the file is transferred to the directories of the standby cluster as fallows:

1. The files from an OUT directory are renamed, moved to the OUT-PROGRESS and  written to the corresponding IN directory in the active cluster with their original name that is \<timestamp\>-\<region server name\>.plog.   
2. The files from the OUT-PROGRESS directory are renamed and written to the IN-PROGRESS directory in the active cluster under their new name, that is, \<timestamp\>-\<region server name\>-\<UUID\>.plog.

When a region server restarts, it needs to find which round to forward if the standby cluster is in the DEGRADED\_SRANDBY\_FOR\_WRITER.  For that, it first checks if the OUT-PROGRESS directory includes any files. If it does, the starting round to replay will be the minimum timestamp of these files. If the OUT-PROGRESS directory is empty, the region server identifies the earliest round to forward by checking the timestamp of the files in OUT directories.

Processing of a round starts only if the current wall clock time is ahead of currentRoundTimestamp more than a round time interval. The minimum delta between current time and the round time can be one round time interval plus some configurable time buffer (a fraction of the round time interval). When a region server switches from the synchronous replication mode to the store-and-forward mode, the currentRoundTimestamp is set based on the current wall clock time.

The consistency point handling is not applicable to the store-and-forward log replication.

## Approach 4 \- Decentralized and HDFS based co-ordination

This approach is similar to Approach 1, just that co-ordination is done in HDFS instead of ZK (to avoid dependency on ZK)

**HDFS IN Directory**: Same as Approach 1 [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.lcurj0lbgqmo)

1. Each RS would list out the shards, shuffle the list and start processing the shards one by one, starting from first to last element in the shuffled list.  
2. For each shard, it will iterate through the files for that shard one by one.  
3. For each file, it will try to acquire the lock, i.e. rename the file from */phoenix/replication/locks/shard-xyz* to */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>.plog to /phoenix/replication/locks/shard-xyz* to */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>\_\<current-timestamp\>.plog*  
   1. Here current-timestamp is added as suffix to identify when was the last time this file name updated (used during RS crash scenario described below in this approach)  
4. If RS is not able to rename, it implies some other RS got the lock and current RS would skip this file and move to the next one.  
5. If RS is able to rename, it will start processing the files in the same way as described in approach 1\. However, after finishing each replication log file in the shard, RS will also update the suffix counter in empty file name of the shard.

**How to Handle RS crash / restarts**

If any RS crashes in the middle of processing a shard, its respective lock file will be /`phoenix/replication/locks/shard-xyz/`*\<timestamp\>\_\<rs-id\>\_\<timestamp\>.plog*

To recover from such scenarios, we need to ensure other RS are able to pick up this shard. Hence in step 3, when RS try to acquire a lock, if the file name is of format */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>.plog*, it will be updated to */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>\_\<current-timestamp\>.plog* directly (same as described in step 3 above), however if the shard file name is of format */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>\_\<timestamp\>.plog* RS would check the last updated timestamp for this file from file name (suffix \<timestamp\> previously added), if it’s older than max time required to process a file (configurable with default as 2 mins) \- we can assume that RS which was previously processing this shard has crashed and this shard is available for lock. In that case, current RS would again rename it from */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>\_\<timestamp\>.plog* to */phoenix/replication/locks/shard-xyz/\<timestamp\>\_\<rs-id\>\_\<current-timestamp\>.plog*.

A consistent point in time for a set of tables (for example a data table and its indexes) in a standby cluster is defined such that all mutations whose timestamp less than this consistency point timestamp have been replayed on these tables. In this approach the consistent point in time is defined by the minimum timestamp of all files in across the shard directories.

Other workflows (Shard discovery, processing of log file) would remain same as Approach 1\.

## 

## Other Design Considerations for State Tracking (instead of ZK)

Major advantage of using ZK for state tracking:

1. ZK is very light weight, and proven methodology for tracking states in distributed systems (even HBase replication state and other states within HBase like live region-servers, WAL splitting, etc are maintained in ZK).  
2. Detection and tracking the partially processed file (when RS crash / fails) is very simple in ZK with ephemeral nodes (which get auto-deleted if client session expires, usually few seconds)

Below is comparison with some other potential state tracking systems

### Phoenix Table

**Advantage**: One advantage of Phoenix Table is ease of debugging via SQL queries.  
**Disadvantages**:

1. By default, phoenix operations would not be thread safe, so we need to use atomic phoenix upserts. I believe ZK call would be light weight operation compared to phoenix upserts (TODO: confirm with phoenix team).  
2. Clean up of partially processed files, i.e. if a RS started processing file but crashed in middle, an external process (or DB trigger) needs to update the file state back to NEW  
3. More involved \- because Schema and read / write pattern needs to be carefully designed, to ensure faster read / writes, avoid hotspotting, custom hartbeat \+ timeout (to handle RS crash)

### Atomic File Rename / Update File Path Operation

**Advantages:** Simple approach and no external system dependency for tracking (ZK)  
**Disadvantages**:

1. Clean up of partially processed files, i.e. if a RS started processing file but crashed in middle, an external process need to move this file back to original name / folder (or RS listing down even in-progress files and checking the last modified time to decide if this file needs to be processed \- which is not very clean solution).  
2. Debugging would be difficult \- as there is no global view on which RS is processing a particular file (and we cannot rename based on RS else the rename would always be unique and hence won’t ensure atomicity)  
3. More number of HDFS calls as in worst case all the RS can try to re-name same file (in case shards are not used)

## Phoenix Compaction Config Value

As described in original design doc [Phoenix HA Rearchitecture for Consistent Failover](https://docs.google.com/document/d/1U2ULZ1xENDxGhAxdy5U0JYGVO5790VR5AOAJMiXHIPc/edit?tab=t.0#bookmark=id.b5end0ul4ya9), *“a new configuration setting representing the maximum expected delay for a replicated mutation to be applied on the target cluster. During compaction, Phoenix will calculate the max lookback window as the greater of the currently configured value and this new replication delay setting. This ensures that no relevant data is compacted prematurely, even when replication lags due to network issues, system load, or failover events”*.

We can keep value as couple of days higher than max expected replication delay (let’s say 5 days \- to ensure sufficient time to do HBase release in worst case to fix the issue / increase this config value itself to buy more time)

## 

## Metrics and Monitoring

| Metric Name | Description | Implementation Details  | Corresponding HBase Replication Metric |
| :---- | :---- | :---- | :---- |
| Age of last Applied | The oldest timestamp of data that is yet to be replayed | We can get it from the oldest file (minimum of first file in each shard) still pending to be applied from the HDFS IN directory. | AgeOfLastApplied |
| Replication Log Replay Throughput | The rate (MB/s) at which each RS is able to replay the log file | Capture the runtime metric for each RS and log it periodically (every 5 mins) | Replication Throughput? |

## 

## Conclusion

**Approach 2 \- Centralized Coordination is NOT suitable** as it has a single point of failure (HMaster Co-proc). If there is any issue with HMaster Co-proc, replication replay would stop and none of the log files would be processed. Hence it’s better to have a decentralized approach where each RS can independently process the log files. Even a crash of a single (or a small subset of RS), does not have any major impact system performance.

**Comparison of remaining decentralized approaches, i.e. Approach 1** (Shard \+ ZK Co-ordination)**, Approach 3** (Round Based Decentralized Without Coordination) **and Approach 4** (Shard \+ HDFS Co-ordination).

**Variable definition and their approximate values**

1. R \- Number of RS on the StandBy cluster (upper bound is 400\)  
2. N \- Number of log files to be processed in one round (Every min, each RS on source would roll 1 file, assuming 400 RS on source)  
3. S \- Number of shards in Approach 1 / Approach 4 (upper bound is 500, usually greater than or equal to number of RS)  
   1. Note: N \<= S  
4. K \- Number of round based directories in Approach 3 (128 as mentioned in details of approach 3\)

### Sync Mode

| Decision Parameter | Parameter Weight | Approach 1 (Shard \+ ZK Co-ordination) | Approach 3 (Round Based Decentralized Without Coordination) | Approach 4 (Shard \+ HDFS Co-ordination) | Preferred Approach (Based on parameter) |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **Time to replay a log file** | P0 | 0.5 S ZK read \+ ZK lock acquire (average case) \+ 1 HDFS list | 0.5 log2.7 N HDFS list operations (worst case) | 0.5 S HDFS list (average case) | Approach 3 (S \>= N) |
| **Cost of replaying log files for a given round**  | P0 | S \* R ZK read \+  S ZK lock acquire \+ S ZK lock release \+ S HDFS list \+ S HDFS file rename Each RS would make a read request to ZK (check if node exists), if it doesn’t then issue create ZK node request) \+ S (for release of lock) \+ S (for updating the shard checkpoint file name)  | 2.6 \* N \+ 0.05 R HDFS list \+ 1.6 \* N HDFS rename  Before each rename operation, an HDFS list operation will be executed. After renaming files, one more listing is required and 0.05 \* R region servers will do another HDFS list on IN-PROGRESS. The total number of rename is 1.6 \*N | S\*R HDFS list \+ N\*R \+ S HDFS renames (worst case) | Approach 3 (S \>= N and S \>= R and N \>= R) |
| **Cost of determining consistency point** | P0 | 1 HDFS list After this optimization for last consistent state tracking in Approach 1 (see [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.1ccm18jgyulc)) | 1 HDFS list  | 1 HDFS list Same optimization as Approach 1 is applicable here (see [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.1ccm18jgyulc)) | Any of them |
| **Additional Operations when RS restart / upscale (per RS)** | P1 | None | Calculate currentRoundTimestamp **Average Case** 1 HDFS list operation (In Progress directory) **Worst Case** (K \+ 1\) HDFS list operations  1 for IN-PROGRESS directories \+ K (one per IN directories) | None | Approach 1 / Approach 4 |
| **Possibility of Duplicate Processing without RS crashes or aborts** | P1 (Since duplicate processing is unlikely without RS failures) | No | Yes \-  unlikely but possible | Yes \- less likely but possible | Approach 1 |
| **Dependency on external system (apart from HDFS)** | P2 | Yes \- ZooKeeper | None | None | Approach 3 / Approach 4 |
| **Additional Points** | P3 |  We need to ensure source is uniformly distributing the log files across shards to avoid hotspotting for single shard and more load on single RS ZK is known and proven mechanism for state management in distributed systems (with current HBase replication also using it in similar fashion) |  Single namenode could be bottleneck in case of high rename requests (instead ZK read can be served from 1 of the 5 replicas) | Single namenode could be bottleneck in case of high rename requests |  |

### Store and Forward Mode (Assuming lag of X mins / rounds)

| Decision Parameter | Parameter Weight | Approach 1 (Shard \+ ZK Co-ordination) | Approach 3 (Round Based Decentralized Without Coordination) | Approach 4 (Shard \+ HDFS Co-ordination) | Preferred Approach |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **Time to replay a log file** | P0 | 0.5 S ZK read \+ ZK lock acquire (average case) \+ 1 HDFS list \+ replication lag (worst case) | 0.5 log2.7 N HDFS list operations (worst case) \+ the replication lag  | 0.5 S HDFS list (average case) \+ the replication lag | Approach 3 (S \> N) |
| **Cost of replaying log files for a given round**  | P0 | (S \* R ZK read \+  S ZK lock acquire \+ S ZK lock release \+ S HDFS list \+ S HDFS file rename) \+ (S \* R ZK read \+  S ZK lock acquire \+ S ZK lock release for each delayed round per round time)  | (2.6 \* N \+ 0.05 R HDFS list \+ 1.6 \* N HDFS rename) \+ (R HDFS listing for each delayed round per round time)  | S\*R HDFS list \+ N \* R \+ S (HDFS file rename) \+  ((N \* R HDFS rename) for delayed round per round time)  | Approach 3 (S \>= N and S \>= R and N \>= R) |
| **Cost of determining consistency point** | P0 | 1 HDFS list After this optimization for last consistent state tracking in Approach 1 (see [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.1ccm18jgyulc)) | None | 1 HDFS list Same optimization as Approach 1 is applicable here (see [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.1ccm18jgyulc)) | Approach 3 |
| **Additional Operations when RS restart / upscale (per RS)**  | P1 | None | Calculate currentRoundTimestamp \-  (K \+ 1\) HDFS list operations  **Reasoning**: 1 for IN-PROGRESS directories \+ K (one per IN directories) | None | Approach 1 |

Based on the above, **Approach 3** (Round Based Decentralized Without Coordination) comes out to be most efficient one, hence that is the suggested and finalized approach.

## Requirements from Other Components of Phoenix HA

### Replication Log Writer (Source)

Writer needs to ensure that log files are written in the IN directory structure (Shard in Approach 1 and 128 time based directories in Approach 3). The root directory (e.g. “/phoenix/replication/logs/”) will be configurable using the site configuration parameter phoenix.replication.log.standby.hdfs.url . This is the IN directory.

**For Approach 1 (Shard \+ ZK Based Co-ordination)**  
The distribution of files among shards must be uniform (as much as possible). We can use region-server-id and timestamp as seed \- to ensure even when single RS is writing multiple files (during Migrations) modulo max number of shards, it would still be uniformly distributed. Another way is to just use the sequence number of log file modulo max number of shards.  
The maximum number of shards (modulo) will be configurable using the site configuration parameter phoenix.replication.log.shards. Shard directories have the format shard-NNNNN, e.g. shard-00001. The maximum value of phoenix.replication.log.shards is 100000\. If values larger than this are found the code should raise an exception.

**For Approach 3 (**  
As described in Approach 3 [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.kahc07qfainp), there are 128 sub directories of root IN directory (eg: /phoenix/replication/logs/0, /phoenix/replication/logs/1 …. ).  
Now the day would be divided into rounds of 1 min (total of 24 \* 60 \= 1440 rounds). Each file of a particular round would be written to respective round directory in arithmetic modulo fashion, i.e. file from 0-1 mins would go to /phoenix/replication/logs/0, file from 1-2 mins would got to /phoenix/replication/logs/1 and files from 126-127 mins would go to /phoenix/replication/logs/127 and files from 127-128 min would go to /phoenix/replication/logs/0 (128 modulo 128 \= 0), and so on.

### HA Store

1. A watcher on state of HA Group, that notify the target cluster on any state changes. This is required in Approach 3 ([PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.kahc07qfainp)) to monitor current cluster state and appropriately update currentRoundTimestamp and also decide the consistent point.  
2. Get the most recent timestamp when cluster was in STANDBY state (required in Approach 3 to calculate consistency point)   
3. API to update the cluster state  
   1. Current Active Cluster of HA Group  
      1. ACTIVE\_TO\_STANDBY to STANDY (for previous active cluster, after all IN directory files are successfully applied)  
   2. Current StandBy Cluster of HA Group  
      1. STANDBY\_TO\_ACTIVE to ACTIVE (for previous standby cluster, after all IN directory files are successfully applied)  
      2. STANDBY to DEGRADED\_STANDBY\_FOR\_READER (when standby cluster is not able to apply the log files)  
      3. DEGRADED\_STANDBY\_FOR\_READER to STANDBY (when pending replication log files are successfully applied)

## Future Optimizations

1. Checkpoint each replication log file (in HDFS, similar to WALs) so another RS can resume from it after a failure, avoiding duplicate mutation replay.  
2. Dynamic calculation of phoenix compaction value instead of keeping higher value for [PHOENIX-7568 - Replication Log Reader](https://docs.google.com/document/d/1usap8PCYFU0Z4orznUPvk0tnSv0X-vnbgD_QZejrnv0/edit?tab=t.0#bookmark=id.g92j2lvlnxu)