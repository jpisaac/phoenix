# Next Gen SOR with HBase on S3

Contributors: [Kadir Ozdemir](mailto:kozdemir@salesforce.com)  
Updated: Oct 31, 2024

This document describes a proposal to leverage HBase on S3 to recover an HBase cluster within a region and a new way of taking backups and restoring from them.

# Problem Statement

It is desirable to use backups on S3 as a last resource to recover an HBase cluster that becomes unavailable. However, restoring data from backup images on S3 can take days. The current architecture for backups makes this recovery option infeasible. Even if we make backup light and fast using the CDC indexes as described [here](https://docs.google.com/document/u/0/d/17xE802rlqubWoX49F3nt95SrIyVwICUnbExdlU1HBSk/edit), restoring all tables would take days as we need to physically copy data from S3.

# Solution

A solution for cluster recovery from backup images needs to be radically different from how we take backups and restore them today. It requires looking at the problem in a completely different perspective. This proposal attempts to do just that. 

As the problem statement points out, the main obstacle for recovering a cluster from backups is the physical data copy operation from S3. This means that we need to find a way to eliminate this copy operation, and to directly mount/point to the data on S3. Today the backup images are stored in the HDFS sequence file format. To achieve this, we need to store the backup images using the HFile format. Another point is that not all tables are SOR tables and we do not take backups of these tables. If backup images were used to recover a cluster, we need to generate backup images for all tables. This motivates us to leverage HBase on S3 for cluster recovery from S3.

In the current architecture as shown below, each Phoenix cluster has its own S3 bucket where backup images are stored. These backup images are generated using periodic backup MapReduce jobs. Similarly, restoring these backup images are done using a MapReduce job when the restore operation is required for a tenant or table. We do not have a well-defined process to do cluster level recovery.

The following is the proposed architecture where we employ two clusters, primary and backup. The primary cluster is used for serving applications, and the backup cluster is used for backup and restore. The backup cluster is an HBase on S3 cluster. HBase replication is set up between the primary and backup cluster. 

This new architecture essentially replaces the MapReduce based backup jobs on the primary cluster with an HBase on S3 cluster. With this architecture, there is no need to run periodic MapReduce jobs to take backups anymore and the backup images will be available to customers almost instantly and continuously. Taking tenant, table and cluster level backups will be unified with a simple and efficient process. Please note that we still need to run MapReduce jobs to do the initial copy from the primary to the backup cluster when the backup cluster is created.

The backup cluster cost is expected to be less than 25% of the primary cluster. This is because half of the compute cost is due to DN pods in core clusters and the backup cluster will have a fraction of these DN pods just to serve replication load. Currently the core clusters consume less than 10% of the EBS baseline throughput (please see [this](https://docs.google.com/document/d/162RAaOWge7UnKp-ojIzOVqn__19yryoJqRap3Je6ydM/edit?tab=t.0#heading=h.sd3uvju0le3u)). This includes reads and the background jobs. It is safe to assume that the write load will be around 5%. This means that the backup cluster needs only 5% of the DN pods and EBS volumes of the primary cluster. The cost of storing data in S3 is about 10% of the cost of EBS (please see [this](https://salesforce.quip.com/E0exAlj8Dal2)). Since the backups will be eliminated, the additional storage cost of S3 for the actual data should not be more than 5% of the EBS cost of the primary cluster. So we can assume that the additional storage cost of the backup cluster will be 10% of the EBS cost of the primary cluster.

Since the backup cluster will not serve read traffic and includes only 5% of the DNs, we can assume that the compute cost of the backup cluster will be about 40% of that of its primary cluster. The storage cost should not be more than 10% of the storage cost of the primary cluster as explained above. We know that EBS costs more than compute does in core clusters, and thus the backup cluster cost is expected to be less than 25% of the primary cluster.  
Given that this cluster replaces the MapReduce based jobs for backups, the cost of YARN cluster for these jobs will also be an additional saving.

It is worth noting that this architecture does not take away the ability to generate sealed backup images that are separate from the live data if required. In this case, the backup jobs will run on the backup cluster to generate these images. This will require extra compute and S3 storage space. However, taking backups will not impact the activities in the primary cluster. We can also make these backup operations light and fast using the CDC indexes as described [here](https://docs.google.com/document/u/0/d/17xE802rlqubWoX49F3nt95SrIyVwICUnbExdlU1HBSk/edit).

## Cluster Failover 

The purpose of cluster failover is to let the application continue to access their data while the primary cluster goes through a troubleshooting process. As long as the WAL files are readable from the HDFS layer of the primary cluster, we will be able to provide a failover capability without data loss. 

We first start with initiating the process to scale the backup cluster compute capacity to serve customer read and write traffic. 

When the primary cluster becomes unhealthy, all WAL changes may not have been replicated yet to the backup cluster. The issues causing the unavailability of the primary cluster may also prevent replicating all WAL changes to the backup cluster. This might be because some region servers cannot be restarted. If failover proceeds without successfully replicating all WAL records, then applications will experience data loss at least temporarily. 

In order to prevent this data loss due to asynchronous replication between the primary and backup clusters, we need replicate the remaining changes (the changes that have not been replicated yet) from the WAL files of the primary cluster to the backup cluster after stopping the writes to the primary cluster by using [the dual cluster client](https://salesforce.quip.com/PS3UArrI3K2c). The write traffic is stopped by setting HA State to STANDBY for the primary cluster.

We can have a tool that reads the WAL records from the HDFS layer of the primary cluster and applies them to the backup cluster as HBase replication does. When this is done, we can let applications switch to the backup cluster. For this we again use the dual cluster client which does not require restarting application servers. In this case, we set HA State to ACTIVE for the backup cluster.

With this, there will be no data loss but just service interruption during failover. This means there will be no specific action that applications need to take for the HBase failover. 

When the primary cluster recovers and catches up with the backup cluster meaning that the replication lag from the backup cluster to the primary cluster becomes insignificant, we pause the traffic to the backup cluster. This is achieved using the dual cluster client. This will let the backup cluster drain the replication queue. After that the applications can switch to the primary cluster using the dual cluster client.

In the case of a complete disaster such that we cannot even read the WAL files from HDFS, we skip the step for replicating remaining WAL records. This may result in data loss. This will be a very rare event. To prevent this, we may want to mirror WAL synchronously from the primary cluster to the backup cluster and then replay the records from this mirror copy to create the replication stream for the backup cluster. Mirroring a file will have lower latency when compared to HBase replication. This is because mirroring will be at the HDFS layer while HBase replication requires processing of HBase mutations which involves Phoenix server side operations including indexing.

## Regular Restore

The regular restore operation for a tenant or table does not require the above process, instead the MapReduce based process is used to restore data from HFiles on S3. 

In order to use HFiles as backup images, we need to preserve delete markers, deleted cells, and cell versions around for the backup period. For this, we will use the same max lookback feature used for this purpose currently. The difference is that we need to set the max lookback age to a larger value. Preserving delete markers, deleted cells and all cell versions would impact the performance of queries after the applications fail over to the backup cluster.  The [dual file compaction](https://docs.google.com/document/d/1Ea42tEBh2X2fCq0_tXSe1BgEqBz58oswJULEbA8-MfI/edit?tab=t.0#heading=h.6aqy2kv2yrun) feature will be a solution to address this performance issue. This feature separates live data from historical data during compaction, and lets queries scan only the live data. 

The data migration from the primary cluster can also leverage the backup cluster as the source of data migration. 

# Reducing Blast Radius and UltraHA

In order to reduce the fault domain and blast radius, we need to have multiple primary clusters in a single FD. With this proposal, each primary cluster will have its own backup cluster. We will continue having UltraHA over primary clusters. This is depicted below.

It is worth noting that if a region is not large enough to justify having multiple primary clusters, UltraHA applications can use the backup cluster as the second cluster for UltraHA. This is shown below.

# Tiering

One can treat the primary cluster as the first tier and the backup cluster as the second/backup tier. In other words, one calls the first tier as the hot tier, the second tier as the warm tier. So, one can set short data retention for the first tier and longer or unlimited data retention for the second tier. This will be a way to reduce the storage cost for applications. 

Argus can be such an application where the primary cluster is used for serving queries for most recent data. For example, the data retention for the primary cluster can be three days and the data retention for the backup cluster can be much longer than the current 45 day TTL as the cost of data storage will be an order of magnitude cheaper than that for EBS. The Argus queries for the last three days are directed to the primary cluster. The queries covering longer periods can be federated over two clusters. In most cases, this federation may not be even needed as the lag between primary and backup clusters will be expected in minutes. 

# Alternative Approach

In 1P data centers, we maintain clusters in pairs of two clusters where each cluster in the pair is a primary cluster for the application servers in its data center as well as a secondary cluster for the application servers in another data center. 

We can use a similar model of deployment in Falcon such that we maintain multiple clusters within an FD,  and each cluster is the primary cluster for a set of application servers (most likely a set of cells in Falcon) in the FD as well as the secondary cluster for other application servers in the same FD. This deployment model is depicted below. Please note for the sake of simplicity, two clusters are shown in an FD. A given FD may have more than two clusters when needed.

The model will allow us to switch the applications from one cluster to another in the event of long unavailability. The following table compares the proposed architecture with this architecture.

|  | Proposed Architecture | Alternative Architecture | Notes |
| :---- | :---- | :---- | :---- |
| CTS | Lower | Higher | The cost of proposed architecture will be roughly 66% of the alternative architecture. The cost benefit of the proposed architecture will be even more when the backup cluster is used as a second storage tier. Please see the cost analysis below. |
| Flexibility | Higher | Lower | The proposed architecture is flexible to support storage tiering, continuous backups, and instant access to the backup data. |

Given that the clusters in the alternative approach have to store twice as much data as being both the primary and the secondary cluster, the cluster storage cost will be 100% higher than that of the primary clusters in the proposed architecture. 

The compute cost of the clusters in the alternative architecture will also be almost 90% higher. This is because the number of DN pods will be 2x and the number of table regions will also be 2x when compared to the proposed architecture. There will be some savings due to not serving queries for the secondary cluster data. Given that EBS dominates the cost of our cluster, it will be fair to say that the overall cost of clusters in the alternative architecture will be at least 90% higher.

The backup cluster cost is expected to be less than 25% of the primary cluster as analyzed earlier in the document. Let the primary cluster cost of the proposed architecture be 100\. Then the backup cluster cost will be 25 and the cluster cost of the alternative architecture will be 190\. This means that the overall cost for the proposed architecture will be (125/190)\*100 \= 66% of the total cost of the alternative architecture roughly.

Since the proposed architecture virtually eliminates the cost of backups, the compute cost saving from that will be additional saving. The cost benefit of the proposed architecture will be even more when the backup cluster is used as a second storage tier.

From the above table, it is clear that the proposed architecture has lower CTS and is simply a better SOR architecture. 

# Conclusions

For all use cases except for Vagabond, secondary clusters do not exist in Falcon today. And so, the long standing question is how we can take advantage of secondary clusters beyond the Vagabond use case and if we should use them for primary cluster recovery. The proposal provides an answer to this question. 

With this proposal, we will be able to repurpose secondary clusters as backup clusters. Here the term “backup” has two meanings. The first one is to indicate that a backup cluster is used for taking backups. The second meaning is that a backup cluster is used for recovering its primary cluster in the event of long unavailability.

This proposal improves the availability of the primary cluster not just by enabling cluster recovery from backups but also by removing the impact of taking backups from it. 

The backup cluster cost is expected to be less than 25% of the primary cluster. When compared to the alternative active-active architecture, the proposed active-backup architecture has roughly 33% better CTS and opens up the possibility of saving more using storage tiering.