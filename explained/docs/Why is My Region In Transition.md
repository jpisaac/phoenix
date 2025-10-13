# Why is My Region In Transition?

*A guide to reasons for RIT and debugging them for HBase On Call Support, started by Andrew Purtell.*

### Overview

HBase stores rows of data in *tables*. Tables can be grouped into *namespaces*. A table can belong to only one namespace at a time. Tables are split into groups of lexicographically adjacent rows. These groups are called *regions*. By lexicographically adjacent we mean all rows in the table that sort between the region’s start row key and end row key are stored in the same region. Regions are distributed across the cluster, hosted and made available to clients by region server (or regionserver, or RS) processes. Regions are the physical mechanism used to distribute the write and query load across region servers. Regions are non-overlapping. A single row key belongs to exactly one region at any point in time. Together with the special META table, a table’s regions effectively form a b-tree for the purposes of locating a row within a table. HBase is properly described as a *distributed ordered tree*. HBase implements the BigTable data model. In this model a table can have one or more column families. In HBase all row data for a given column family is placed into a *store*. A store contains one memstore and zero or more store files, or *storefiles*. 

Writes to a region are served by only a single region server at any point in time.  This is how HBase guarantees consistency and how we can cheaply implement atomic operations. In trade, when a region is offline, writes to the portion of the keyspace covered by that region must be held and retried until the region is online again. Reads may be served by either one or up to three regionservers, depending on if the optional *read replica* feature is enabled for the given table(s). 

If we step back, what this architecture looks like from orbit is a collection of many small databases (the regions) which are dynamically assigned to a fleet of servers (the regionservers). The lifecycle of a region is very similar to the lifecycle of a standalone database: At rest the database is a collection of files. These files are “mounted” into a serving process. The serving process announces their availability. The serving process then handles reads and writes. When shutting down, the serving process flushes all pending writes into files, and then at rest the database is a collection of files again. This cycle repeats as the responsibility for serving the data is exchanged from server to server. The responsibility for serving a region shifts when there are server failures or when triggered by an administrator or a built in administrative process, such as the balancer. The process of opening a region or closing a region is transitional by nature. Therefore we term regions currently in the process of opening or closing as *regions in transition*, or RIT. 

If a server or process fails, we have to move region ownership elsewhere so it can be brought online again. When auto-sharding a table, as it grows, we divide regions when they get too big, then move regions around when the distribution of regions or load becomes uneven. These management tasks trigger region movement. When a region is moving it is by definition in transition. 

Without these optimization processes overall performance will degrade. In comparison to that degradation the transient costs of RIT are small. We will cover these causes of RIT in more detail below as well as discuss the general process of region assignment. With the exception of server failures and process crash bugs the sources of RIT can be controlled via custom policy plug points or configuration settings. The general notion is to take the available policy knobs and use them to control the sources of RIT in the system. In trade automatic optimization behaviors like balancing must be replaced with a manual (but automatable) alternative, which can take the form of custom policies, or external control by a custom supervisory process implementing business focused optimization criteria.

### Availability

An attempt at defining region availability as simply as possible:

* A region may be OFFLINE, ONLINE, or in a transitional state. If a region is ONLINE it is available and can receive and process requests from clients; otherwise it cannot.
* A region is available for writing on at most one regionserver.
* A region is available for reading on at most either one or three regionservers, depending on if the read replica feature is enabled for the respective table(s). 
* When a region is in transition, it is not available.
    * If read replicas are enabled, other replicas of the transitioning region can continue to serve read requests, but the replicas may not have received the latest write from the primary in failure cases.
    * Read replicas transition independently from the primary region.
* A region may enter transition for a variety of reasons as discussed in this document.
* Region transitions are meant to be fast. A variety of factors and configuration settings influence time in transition. This document discusses them in detail. 

Unfortunately there is some complexity inherent in distributed data stores.  

### Assignment

What server among the fleet is given responsibility for serving a given region is determined by the currently active HBase master process. The master's AssignmentManager, or AM, component orchestrates the transfer of region serving responsibility from one server to another. Assignment is a state machine implemented in three places: 1. master side logic for making decisions and monitoring progress, 2. regionserver logic for taking action and responsibility, and 3. ZooKeeper as the rendezvous point for #1 and #2. ZooKeeper is used for state synchronization between these distributed processes. We also support an assignment mode, “[zk-less assignment](https://blogs.apache.org/hbase/entry/hbase_zk_less_region_assignment)”, that does not use ZooKeeper, but this assignment mode is not utilized at Salesforce, so is not covered in detail. The golden path of each option is illustrated below.

In standard assignment, the master initiates an assignment, the regionserver processes the request, a zookeeper znode is used by both the master and regionserver as a rendevouz to communicate state changes to the assignment in progress, and finally the regionserver updates the relevant entries in the META table directly:
In HBase 1, assignment state may be concurrently mutated by master and regionserver processes, in either the ZooKeeper hosted rendezvous or in the META table. Disciplined timekeeping is important. As with other aspects of HBase operation clock skew can lead to temporal discontinuities. If these occur during management of assignment state then state changes can be skipped or overwritten. Compared with HBase 2 and up, assignment can be slow in operation because each assign involves transitioning region states through ZooKeeper and META table updates. The effective scale of a cluster by number of regions tends to top out on the order of a few hundred thousand regions. Beyond this point the overheads involved with using zookeeper as rendezvous are prohibitive for the cold restart case. Cold restarts can take hours, and are prone to assignment state corruption if failures occur before the cluster is fully online. The AM in HBase 1 offers a bulk assignment mode which rapidly bootstraps assignments from a cold start to compensate for these shortcomings. HBase 2 provides a complete solution to this problem by rebuilding assignment on a framework for logged, restartable processes.

In zk-less assignment the master initiates the assignment and the regionserver processes the request as before, but the regionserver communicates directly with the master, zookeeper is not involved, and the master reads and updates the META table exclusively. This is broadly speaking the same approach taken in HBase 2.
ZK-less assignment is offered in HBase 1 as a more scalable alternative to standard assignment, but it can be less reliable, because only the master tracks the current state of the assignment in memory. In normal assignment if the master crashes we can recover state of assignments in progress from zookeeper. In zk-less assignment that won't be possible. We rely on the regionservers to successfully transition incomplete assignments to the offline state without intervention from the master or a recovery tool. Newly active masters will notice offline regions and reassign them. My impression is the ZK-less assignment option in HBase 1 is rarely used, so it's a risk, although a very large installation with more than a million regions in production uses it. 

HBase 2 addresses the scalability and reliability challenges of the assignment process better by reimplementing assignment on a framework for composing logged, restartable processes. Each region assignment is a logged, restartable task. This improves the scalability and reliability of the region assignment process in trade for increased assignment latency, due to the necessary overheads of logging and task management.

For more information on HBase 2 improvements in region assignment please refer to this series of Salesforce Engineering Blog posts:

* Evolution of Region Assignment in the Apache HBase Architecture — Part 1: [[link](https://engineering.salesforce.com/evolution-of-region-assignment-in-the-apache-hbase-architecture-part-1-c43b1becc522/)]
    * In this first part of the blog post series, we cover the important design details of HBase and why the AssignmentManager was redesigned in HBase 2, to overcome the shortcomings of its design in earlier HBase versions.
* Evolution of Region Assignment in the Apache HBase Architecture — Part 2: [[link](https://engineering.salesforce.com/evolution-of-region-assignment-in-the-apache-hbase-architecture-part-2-9568fb3790b/)]
    * In this second part of the series, we cover the AssignmentManagerV2, or AMv2, available in HBase 2 releases and describe how it is more robust, resilient, fault-tolerant, and scalable than the AM provided by previous HBase releases. Many of the shortcomings of the earlier design that shipped in HBase 1 have been addressed. With the improved retry mechanisms, the single-writer system, better state management, improved shared/exclusive locking, and durable and reliable logging of state transitions, HBase 2 achieves significantly improved availability and reliability in operation.
* Evolution of Region Assignment in the Apache HBase Architecture — Part 3: [[link](https://engineering.salesforce.com/evolution-of-region-assignment-in-the-apache-hbase-architecture-part-3-e03b814ae92/)]
    * In the third part of the series, we cover the internal mechanism of two of the most complex and critical Procedures: ServerCrashProcedure (SCP) and TransitRegionStateProcedure (TRSP), and also cover how they coordinate to achieve reliable and robust region transition workflows during server shutdown events.

### Splitting and Merging

A region may be split into two smaller regions should it grow too large, or if triggered by an admin API request. The split process is driven by the regionserver. The regionserver periodically considers the split policy configured for the region and, if conditions are met for splitting, initiates the split process. The keyspace of the region is split at the midpoint of the region's key range. Each new daughter region receives half of the parent region's key range. The parent is offlined and the new daughters are brought online. At first the parent and new daughters will all be hosted by the same regionserver and the daughters will share the parent's store files. Splits are therefore completed very quickly. Background housekeeping will eventually copy data from the parent into the daughters and remove references to the parent. Once all references to the parent are dropped the parent region will be garbage collected and deleted. The CatalogJanitor chore in the master periodically performs this garbage collection activity.

Two regions may be merged into a single region if an optional background housekeeping process (the RegionNormalizer chore, running in the master) decides it appropriate, or if triggered by an admin API request. The merge process is the inverse of the split process. When complete the old daughter regions are garbage collected.

### Balancing

The master runs a background chore named LoadBalancer. The responsibility of this component is to analyze the distribution of regions over servers (in all versions), and also the distribution of load over the cluster (in newer versions). Should it determine the regions or load to be unevenly distributed it will iterate in bounded time over random reassignment plans and select the lowest cost plan achieving the best result. The balancer will then hand the plan for reassigning regions to the AM, which will close regions and reopen them elsewhere as directed. The balancer runs periodically in the background. For this reason with the default balancer in place and active you can expect some region movement, some regions in transition, at any time. 

### Managing region transitions

There are various reasons why a region may enter a transitional state:

* Server hardware failure
* Regionserver process crashes / bugs
* Administrative actions like table creation, deletion, schema modification, and explicit onlining (enable) or offlining (disable)
* Automatic management processes:
    * Region splits (auto-sharding)
    * Region merging (keyspace to shard distribution optimization)
    * Balancing (shard to server distribution optimization)

Of these causes of region transitions the automatic management processes can be controlled, by policy plug points or configuration options. Default behaviors can be replaced with business case specific logic. Server hardware issues cannot be controlled by software, so is out of scope for discussion. The risk of regionserver crash bugs is expected to be managed with best practices for software development and deployment.

**Failure Handling and MTTR**

Time to region redeployment after failures depends on how long it took for us to become aware of the failure, which is influenced by zookeeper configuration options, then by how much time is required to split the write-ahead log of the failed server and distribute the recovered edits to the reopening region(s). HBase does this using a master-mediated distributed process. This Hortonworks presentation is a short introduction: [Introduction to HBase MTTR](https://www.slideshare.net/hortonworks/h-base-mttr-final) . The ZooKeeper heartbeat can be tuned to trade off between responsiveness and false positives. Configuration options are available to increase the default parallelization of WAL splitting and region open work. Several HDFS tuning options were also introduced to make stale datanode detection possible (HDFS waits a long time to declare a datanode dead, and in the meantime each attempt to access the dead datanode induces a performance degradation, but can be much more agile with the “stale” designation) and to speed the process of lease recovery, a prerequisite for WAL splitting. Collectively these changes reduce the mean time to recovery (MTTR) of the loss of region availability due to a failed server or process to under one minute. This MTTR is for a full recovery where the region is available to clients for both reads and writes. If read replicas are enabled, during this time there is a partial availability: reads can be served from replicas, reducing the timespan of availability loss for reads from ~minute to ~milliseconds. 

**Splitting and Merging**

Region splitting is the process by which a table is auto-sharded as it grows. Splits typically complete in under a second. Region merging is an optionally enabled process by which uneven sharding, due to changes in the distribution of data over the keyspace, is normalized. Merges also typically complete in under one second. Split times in the range of one or a few minutes have been observed under rare conditions due to resource contention in the network, I/O stack, or assignment manager. This risk is mitigated by best practices for operations, such as sizing the fleet of regionservers appropriately, and tuning split criteria via configuration for the workload. If the default tuning knobs are insufficient, split and merge policies are pluggable. The split or merge policy is given an opportunity to explore the set of store files and decide if any action is warranted. Business case specific logic can be included here. Policies can be set per table. If necessary automatic split and merge behaviors can be completely disabled and accomplished through external and manual (but automatable) means, presumably during scheduled maintenance windows. Splits and merges also require a round of compaction on the affected regions for critical housekeeping. The split and merge actions are typically fast because rather than rewrite data inline with the split or merge process we place reference files in the filesystem that are not real store files, and these must be post-processed, eventually, into separate (post split) or combined (post merge) real store files, by way of compaction. Compaction activity increases IO and GC load so can impact request serving.

**Balancing**

Regions may be moved by the balancer to optimize the distribution of load over the available cluster resources. During balancing, one or more regions are closed on some servers, and then reopened on others. This can lead to short availability gaps. The Balancer implementation is pluggable. The default implementation does a periodic stochastic walk over the space of all possible region assignments using a set of cost functions to evaluate the fitness of any particular plan. Whether or not to balance, and then what sequence of region moves is desirable, are calculated using that set of cost functions. The weights for each cost function can be adjusted by configuration. Custom cost functions implementing business case specific logic can be deployed and included in planning. (For example, imagine a cost function that increases the region move cost by 10x during peak or business hours.) The balancer implementation itself is a plug point and can be replaced with an alternative. Finally, on clusters where any disruption is undesirable, the balancer can be disabled, and enabled/invoked manually only during scheduled maintenance.

### Region State Transitions

The state transitions for successful opening (assignment) and closing (unassignment/reassignment) look like the following. When a region is in one of the italicized states it is considered in-transition and will appear in the regions-in-transition, or RIT, list on the master:

        open: OFFLINE → *PENDING_OPEN* → *OPENING* → OPEN

        close: OPEN → *PENDING_CLOSE* → *CLOSING* → *CLOSED* → OFFLINE

The state transitions for unsuccessful opening (assignment) and closing (unassignment/reassignment) look like:

        open: OFFLINE → *PENDING_OPEN* → *FAILED_OPEN*
                                                                      | → *OPENING* → *FAILED_OPEN*

        close: OPEN → *PENDING_CLOSE* → *FAILED_CLOSE*
                                                                  | → *CLOSING* → *FAILED_CLOSE* 

FAILED_OPEN and FAILED_CLOSE are italicized even though they are terminal states for a given assignment attempt because regions in those states are still reported as in-transition.

If a region transitions to FAILED_OPEN state the master will wait five minutes by default and then try again, and again.

If a region transitions to FAILED_CLOSE state then manual operator intervention will be required. The assignment manager detected cause to doubt that the region close process completed successfully and cannot safely proceed with reassignment. The regionserver involved may still be actively holding resources for the region. Operators can monitor the *ritCountOverThreshold* metric for alerting on when intervention may be required, and poll the regions-in-transition list maintained by the master. Manual resolution of FAILED_CLOSE typically involves terminating the implicated regionserver to ensure the region resources are released, and possibly then manual reassignment using the admin API if server crash handling for the terminated regionserver did not automatically resolve the condition. 

As a safety valve, all FAILED_* states are transitioned to OFFLINE after a failover of the active master. So, in an emergency, triggering a shift in the active role from one master to another will trigger reassignment of all RIT regions, including those in FAILED_* state. This action should not be taken lightly, because, as discussed above, a region in FAILED_CLOSE state may still be partially active. Reassignment in that case would cause (partial) double assignment. Under double assignment conditions the risk of data loss is elevated.

If a server is declared dead any regions hosted on it are immediately transitioned into OFFLINE state as soon as the server failure is processed by the master.

During a split, the old parent and new daughter regions go through the following transitions:

        parent (old): OPEN → *SPLITTING* → SPLIT 

        daughter (new): *SPLITTING_NEW* → OPEN

Or upon split transaction or server failure:

        parent (old): SPLITTING → OPEN (split transaction failure)
                                                  | → OFFLINE → *PENDING_OPEN* → *OPENING* → OPEN (server failure)

        daughter (new): SPLITTING_NEW → OFFLINE

Splitting is an emergent process performed by the regionserver fleet. The regionserver decides a region split is warranted, then engages the master in a multi-step process to execute it. The master is informed of the initiation of the split and the status of the new daughters by the regionserver as it executes the split transaction procedure. After a split has completed the new regions are located on the same regionserver as the parent. They may be moved later by cluster balancing action, initiated by the master, or if the host regionserver fails. If the split transaction aborts the SPLITTING region is transitioned back to OPEN, any SPLITTING_NEW daughters are transitioned to OFFLINE and garbage collected later. If the regionserver crashes during the split procedure prior to the point of no return (PONR), the server crash handling process will roll it back. The parent region is transitioned from SPLITTING to OFFLINE, then reopened normally, and the daughter regions of the failed split are transitioned to OFFLINE and then garbage collected later. If the regionserver crashed after the split procedure passed PONR then the master will roll forward the split as part of server crash handling. SPLITTING_NEW daughters will be brought ONLINE and the SPLITING parent will be offlined.

In contrast to splitting, merging is a process initiated by the master. If region merging is enabled, a master chore named RegionNormalizer will periodically evaluate if adjacent regions meet the criteria for merging, and then drive the multi-step process that merges them.

During a merge, the old daughter and new parent regions go through the following transitions:

        daughter (old): OPEN → *MERGING* → MERGED

        parent (new): *MERGING_NEW* → OPEN

Or upon merge transaction or server failure:

        daughter (old): MERGING → OPEN (merge transaction failure)
                                                     | → OFFLINE → *PENDING_OPEN* → *OPENING* → OPEN (server failure) 

        parent (new): MERGING_NEW → OFFLINE

Merging is the inverse of splitting as you would expect and crash recovery takes the same approach. Before PONR the merge procedure is rolled back by the master during server crash handling. After PONR the merge procedure is rolled forward.

### Region State Detail

Below is the complete list of region states. The common reasons they may be stuck in the transitional states are also described.

* **OFFLINE
    
    **The region is offline and not opening.

* **PENDING_OPEN
    
    **The master has sent the open request but the regionserver has not begun the open process.
    * The master transitions a region to PENDING_OPEN state and tries to assign the region to a regionserver. The regionserver may or may not have received the open region request. The master retries sending the open region request to the regionserver until the RPC goes through or the master runs out of retries. 
    * The assignment manager will use a constant time retry strategy if the regionserver fails to process the open request in a timely manner, retrying up to `hbase.assignment.maximum.attempts` times. If attempts on one server continue to fail the open will be retried on another server. Change `hbase.assignment.retry.sleep.max` to something like 60000 to get exponential backoff up to max sleep time of 60 seconds per attempt or 300000 to get exponential backoff up to max sleep time of 5 minutes per attempt. Setting `hbase.assignment.maximum.attempts` to an extremely large value, up to INT_MAX (2147483647) is another option, but not recommended, because we really should give up on an unopenable region, perhaps due to data corruption. There is a risk that the regionserver that has failed to open the region in a timely manner will also fail to respond to the close request that must proceed a reassignment attempt in a timely manner, especially after a large number of servers have simultaneously failed and all regionservers are backlogged with region open work. (Enable exponential backoff and increase open/close executor pool sizes to mitigate.) If the close request times out, the assignment manager must transition the region into the terminal FAILED_CLOSE state for operator intervention.    
    * The assignment manager is allocated a fixed number of threads for performing assignment work. If there are more regions to open than available threads some must wait until AM handlers become available. If there are a large number of regions to open some may linger in this state for a while. Consider increasing the size of the AM executor pool to handle spikes in region open demand if frequent RIT in this state are observed. This is controlled by the master site file configuration option `hbase.assignment.threads.max` (default 30). If considering increasing this you should also consider a proportional increase of the handler threads for processing ZK watch events triggered by regionserver side updates to the assignment state rendezvous. That is controlled by the master site file configuration option `hbase.assignment.zkevent.workers` (default 20).  If you increase the number of AM worker threads you should consider a proportional increase of open executor threads on the regionserver side.
    * The regionservers have a fixed sized executor pool for processing region open requests. An incoming request must wait if all the open request handlers are busy. Consider increasing the size of the open executor pool to handle spikes in region open demand if frequent RIT in this state are observed. This is controlled by the regionserver site file configuration options `hbase.regionserver.executor.openregion.threads` (default 3) for normal regions, `hbase.regionserver.executor.openpriorityregion.threads` (default 3) for priority regions, and `hbase.regionserver.executor.openmeta.threads` (default 1) for meta table regions.
    * When a cluster is cold started all regions must be assigned, leading to an initial spike in assignment work and an expected backlog of regions in PENDING_OPEN state. An assignment is considered a bulk assignment once there are more than `hbase.bulk.assignment.threshold.regions` (default 7) regions or `hbase.bulk.assignment.threshold.servers` (default 3) servers to consider at once. Bulk assignment is processed by a dedicated thread pool sized according to the master site configuration option `hbase.bulk.assignment.threadpool.size` (default 20). Adjusting these parameters is recommended if cold cluster restarts take a long time. If adjusting this threadpool size it is important to also increase the regionserver side resources proportionally as explained above.
    * A region hosted by a crashed regionserver cannot be reassigned until the crashed regionserver's write-ahead logs have been processed and split into per-region recovered edits files. Reassignment of a region from a crashed server will be held up by the distributed split work backlog. Every regionserver runs a background daemon thread that manages the acquisition and execution of distributed log split tasks. This thread registers a watcher on a znode managed by the master. When the master is processing a server shutdown or crash or cluster restart when it detects the presence of unprocessed WAL files it will register the WAL files for processing under the znode. One or more live regionservers will attempt to get an exclusive lock on an entry. One of them wins, splits the WAL file, deletes the entry, then will acquire more work or go back to sleep if the worklist is empty. A regionserver can acquire at most a fixed number of log split tasks determined by configuration, `hbase.regionserver.wal.max.splitters` (default 2). If the number of entries/logs to process exceeds the number of available split workers in the cluster, perhaps due to the correlated failure of a significant subset of the fleet, then splitting work will fall behind. Regions may remain in RIT until the backlog is cleared.

* **OPENING
    
    **The region is in the process of being opened.
    * An assignment may linger in OPENING state if the regionserver is having trouble opening the region. For example, an unhealthy or overburdened HDFS namenode may be slow to respond to directory listing and file open requests. 
    * If the regionserver previously hosting the region crashed then the write ahead log of that server will have been split before reassignment and the regionserver may have a *recovered edits* file to process before the region can be brought into OPEN state. This is usually performed quickly but HDFS level issues could stall the reads.
    * Stalls in the region open process will stall the region open handler executing the open procedure. Once all handlers are consumed no further open requests can be processed. This backpressure is intentional. 
    * When the master runs out of open request retries it prevents the regionserver from opening the region by transitioning the region to CLOSING state, even if the regionserver is starting to open the region.
    * If the regionserver cannot open the region, it notifies the master. The master transitions the region to CLOSED state and tries to open the region on a different regionserver. The master will retry the open only so many times. This controlled by the master site configuration parameter `hbase.assignment.maximum.attempts` (open source default 10, SFDC fork default 100). After that the master transitions the region to FAILED_OPEN state.
    * The assignment manager will use a constant time retry strategy if the regionserver fails to process the open request in a timely manner, retrying up to `hbase.assignment.maximum.attempts` times. If attempts on one server continue to fail the open will be retried on another server. Change `hbase.assignment.retry.sleep.max` to something like 60000 to get exponential backoff up to max sleep time of 60 seconds per attempt or 300000 to get exponential backoff up to max sleep time of 5 minutes per attempt. Setting `hbase.assignment.maximum.attempts` to an extremely large value, up to INT_MAX (2147483647) is another option, but not recommended, because we really should give up on an unopenable region, perhaps due to data corruption. There is a risk that the regionserver that has failed to open the region in a timely manner will also fail to respond to the close request that must proceed a reassignment attempt in a timely manner, especially after a large number of servers have simultaneously failed and all regionservers are backlogged with region open work. (Enable exponential backoff and increase open/close executor pool sizes to mitigate.) If the close request times out, the assignment manager must transition the region into the terminal FAILED_CLOSE state for operator intervention. 

* **OPEN
    
    **The region is open and the regionserver has notified the master.

* **PENDING_CLOSE
    
    **The master has sent the close request but the regionserver has not begun the close process.
    * The assignment manager is allocated a fixed number of threads for performing assignment work. If there are more regions to close than available threads some must wait until AM handlers become available. If there are a large number of regions to close some may linger in this state for a while. This should rarely occur. Table drops or sudden intense balancer activity (a sudden cap add) could cause it. Consider increasing the size of the AM executor pool to handle spikes in region close demand if frequent RIT in this state are observed. This is controlled by the master site file configuration option `hbase.assignment.threads.max` (default 30).  If considering increasing this you should also consider a proportional increase of the handler threads for processing ZK watch events triggered by regionserver side updates to the assignment state rendezvous. That is controlled by the master site file configuration option `hbase.assignment.zkevent.workers` (default 20). If you increase the number of AM worker threads you should consider a proportional increase of close executor threads on the regionserver side.
    * The regionservers have a fixed size executor pool for processing region close requests. An incoming request must wait if all the close request handlers are busy. Consider increasing the size of the close executor pool to handle spikes in region close demand if frequent RIT in this state are observed. This is controlled by the regionserver site file configuration options `hbase.regionserver.executor.closeregion.threads` (default 3) for all regions except those of the meta table, and `hbase.regionserver.executor.closemeta.threads` (default 1) for meta table regions. 

* **CLOSING
    
    **The region is in the process of being closed.
    * The master transitions a region to CLOSING state. The regionserver holding the region may or may not have received the close region request. The master retries sending the close request to the server until the RPC goes through or the master runs out of retries.
    * The regionserver must acquire a lock on the region before it can close it. If there are gets, mutations, or scans in progress, lock acquisition will fail, stalling the close process until acquisition can succeed. Long running or wedged Phoenix scanners have been a cause of slow closes in the past. In general locking discipline around region close is tricky and stalls in CLOSING state or regions which end up in FAILED_CLOSE states should be taken at face value as a possible bug.
    * After acquiring the close lock the regionserver must flush any unflushed data in memstore to disk as new storefiles. Slowness or other trouble at the HDFS layer can stall the close process here. If we fail to flush five times the regionserver will abort. The regionserver abort triggers the server crash recovery process, which splits the write ahead log, generates recovered edits files, and prepares the affected regions for reassignment to another regionserver. No data is lost even though the memstore flushing upon close failed.
    * Store files are closed in parallel. Slowness or other trouble at the HDFS layer can stall the close process here. 
    * Stalls in the region close process will stall the region close handler executing the close procedure. Once all handlers are consumed no further close requests can be processed. This backpressure is intentional. 
    * If the master runs out of close request retries it will transition the region state to FAILED_CLOSE.

* **CLOSED
    
    **The regionserver has closed the region and notified the master.

* **SPLITTING
    
    **The regionserver started splitting the region and notified the master.
    * When a regionserver is about to split a region, it notifies the master. The master transitions the region to be split from OPEN to SPLITTING state and adds the two new regions to be created by the regionserver. These two regions are in SPLITTING_NEW state initially.
    * Incomplete rollback of a failed split may leave both parent and daughter regions in OPEN state, leading to an overlap in the region chain; or may leave the parent and/or one daughter OFFLINE, leading to a hole in the region chain. We are obviously not supposed to reach these states without completed rollback so an occurrence of this problem represents a bug. The hbck tool repairs these cases.

* **SPLITTING_NEW
    
    **This region is being created by a split which is in progress.
    * Upon receiving a notification that a split transaction is about to begin, the master creates two placeholders in META for the new daughters and sets them to this state. 
    * The master does not update the region states until it is notified by the server that the split is done. If the split is successful, the splitting region is transitioned from SPLITTING to SPLIT state and the two new regions are transitioned from SPLITTING_NEW to OPEN state.
    * Server failure during split causes immediate transition of SPLITTING_NEW → OFFLINE and eventual garbage collection of the failed split.

* **SPLIT
    
    **The regionserver has completed the split of the region and notified the master.
    * SPLIT is equivalent to OFFLINE in terms of assignment status but signifies the region will be garbage collected.

* **MERGING
    
    **The regionserver started merging a region.
    * When a regionserver is about to merge two regions, it notifies the master first. The master transitions the two regions to be merged from OPEN to MERGING state, and adds the new region which will hold the contents of the merged regions region to the regionserver. The new region is in MERGING_NEW state initially.
    * Incomplete rollback of a failed merge can cause an overlap or hole in the region chain by the same mechanism as incomplete rollback of a failed split. We are obviously not supposed to incorrectly transition without a completed rollback so an occurrence represents a bug. The hbck tool repairs these cases.

* **MERGING_NEW
    
    **This region is being created by a merge of two regions.
    * Upon receiving a notification that a merge transaction is about to begin, the master creates a placeholder in META for the new combined region and sets it to this state. 
    * The master does not update the region states until it is notified by the regionserver that the merge has completed. If the merge is successful, the two merging regions are transitioned from MERGING to MERGED state and the new region is transitioned from MERGING_NEW to OPEN state.
    * Server failure during merge causes immediate transition of MERGING_NEW → OFFLINE and eventual garbage collection of the failed merge.

* **MERGED
    
    **The regionserver has completed the merge of two old daughter regions into a single new region and notified the master. The old daughter regions are placed into this state. 
    * MERGED is equivalent to OFFLINE in terms of assignment status but signifies the region will be garbage collected.

* **FAILED_OPEN
    
    **The region failed to open and the AM will not retry any more.
    * A region can fail to open if any one of its storefiles cannot be opened. The file may be missing because it was prematurely archived by compaction (a bug we have fixed) or because an external process deleted it. The file may be corrupt and unreadable because all replicas for one of its constituent blocks are unavailable or corrupt at the HDFS level. More rarely, but seen here at SFDC, a required compression algorithm may not be available on the regionserver because native runtime dependencies were not properly installed into the OS image. 
    * If the HDFS level issue is transient the automatic retry behavior for regions in FAILED_OPEN state will recover availability without intervention. An example of when this could happen is a scenario where multiple datanodes are offline, leading to blocks with all replicas offline, leading to corrupt HFiles. As soon as the offline datanodes are restarted their blocks are available again and the HFiles are no longer corrupt due to missing blocks.
    * For regions in FAILED_OPEN state a background task in the master kicks off a new assignment attempt every five minutes (by default).
    * If the active master role fails over or the master is restarted a FAILED_OPEN region transitions immediately to OFFLINE. As with a similar transition of FAILED_CLOSE to OFFLINE in this case, this is a safety valve that ensures operators are not stymied by dogged insistence of maintaining the FAILED_* states. (Consider if the AM insisted on tracking FAILED_* states set by previous master processes, a bug in the AM preventing a transition would render the region permanently unavailable.)

* **FAILED_CLOSE
    
    **The region failed to close and the AM will not retry any more.
    * A region fails to close if the regionserver hosting the region gets stuck somewhere in the close process.
    * There is no automated recovery for regions in FAILED_CLOSE state because the regionserver hosting the region may still have it in a partially opened state. The failure to close stymies automatic mechanisms. Opening it elsewhere might lead to a double assignment, which could lead to irreversible corruption and data loss. We avoid corruption by placing the region in the terminal FAILED_CLOSE state until operator intervention. Terminate the regionserver and the region will be reassigned and reopened. The operator can also manually trigger reassignment but termination of the regionserver currently potentially holding region resources is strongly advised first for assured fencing of all region data access.
    * If the active master role fails over or the master is restarted a FAILED_CLOSE region transitions immediately to OFFLINE. ~~This seems like a bug if true. We shouldn't lose track of FAILED_CLOSE states.~~  As with a similar transition of FAILED_OPEN to OFFLINE in this case, this is a safety valve that ensures operators are not stymied by dogged insistence of maintaining the FAILED_* states. (Consider if the AM insisted on tracking FAILED_* states set by previous master processes, a bug in the AM preventing a transition would render the region permanently unavailable.)

This is the complete state transition diagram. OFFLINE and OPEN are goal states, so are shaded. Black transitions are normal. Red transitions are error handling.

```
digraph RegionStates {

  OFFLINE [ style = filled ]
  OPEN [ style = filled ]

  { rank=same OFFLINE OPEN }

  OFFLINE -> PENDING_OPEN

  PENDING_OPEN -> OPENING
  PENDING_OPEN -> FAILED_OPEN [ color = red ]

  OPENING -> { CLOSING, CLOSED, OPEN }
  OPENING -> FAILED_OPEN [ color = red ]

  FAILED_OPEN -> CLOSING

  PENDING_CLOSE -> CLOSING
  PENDING_CLOSE -> FAILED_CLOSE [ color = red ]

  OPEN -> { CLOSING, PENDING_CLOSE, SPLITTING, MERGING }
  OPEN -> OFFLINE  [ color = red ]

  CLOSING -> CLOSED
  CLOSING -> FAILED_CLOSE [ color = red ]

  FAILED_CLOSE -> CLOSING

  CLOSED -> OFFLINE

  SPLITTING -> OPEN [ color = red ]
  SPLITTING -> SPLIT
  SPLIT -> OFFLINE

  SPLITTING_NEW -> OPEN
  SPLITTING_NEW -> OFFLINE [ color = red ]

  MERGING -> OPEN [ color = red ]
  MERGING -> MERGED
  MERGED -> OFFLINE

  MERGING_NEW -> OPEN
  MERGING_NEW -> OFFLINE [ color = red ]

}
```

