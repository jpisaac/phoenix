# rHBas e Synchronous Replication

# Background

The current replication in HBase is asynchronousin asynchronous. So if the master cluster crashes, the slave cluster may not have the newest data. If users want strong consistency then they can not switch to the slave cluster.  
Alibaba has an internal version of HBase which adds the support of synchronous replication. The slides can be found [here](https://www.slideshare.net/HBaseCon/synchronous-replication-for-hbase). At xiaomi we also have some customers who need this feature, so we plan to implement this feature in the official HBase branch.

# Basic Idea

The basic idea is easy to understand.

* Setup two clusters, active(A) and standby(S), and connect them with asynchronous replication.  
* All read/write are performed at A, S only receives replication data.  
* Be  
* sides the normal WAL logging, A will also write a copy of the WAL(remote WAL) to the HDFS cluster of S.  
* When the asynchronous replication goes on, also delete remote WALs on S which have already been replicated.  
* If A crashes and we want S to be the next active cluster, replay the remote WALs on S before offering service.

# Integration into HBase

## Replication Peer

Add a flag to indicate whether replication is synchronous or not. And also another flag to indicate the state of the current cluster:

* Active. In this state, the cluster will write remote log to standby cluster, and will reject any replication request from this peer.  
* Downgrade Active. In this state, the cluster will not write remote log to standby cluster, but will still reject any replication request from this peer..  
* Standby. In this state, the cluster will forbid any read/write request for the tables in this peer, and will only accept replication request. And also give up replicating any data to active cluster. 

So here we need to add a peer field in the replication request as we need to reject the replication request under *A* or *DA* state. In the next section we will show you why we need to reject the replication request.

## State Transition

To simplify the logic, we do not introduce inter cluster procedure (Of course this may increase the complexity for OP).

* For newly configured synchronous replication peer, the default state is DA.  
* *DA* \-\> *S*.   
* *DA* \-\> *A*.  
* *S* \-\> *DA*.

The path to setup a synchronous replication from the beginning(Cluster CA and CB):

1. Config synchronous replication on both CA and CB, the two clusters will both transit to *DA*.  
2. Transit CB from *DA* to *S*  
3. Transit CA from *DA* to *A*  
4. Now the synchronous replication is up with CA as the active cluster and CB as the standby cluster.

If error occurs, the approach is the same:

* Transit the cluster which you want it to be the next active cluster to *DA*, no matter it is active or standby.

The way to re-setup the synchronous is also straight-forward:

* If the broken cluster is in state *S*, then clean up the remote WAL directory, and then transit the current active cluster from DA to A(step 3 above).  
* if the broken cluster is in state *A*, then first you need to transit it from *A* to *S*, then start from step 3 above. Notice that the broken cluster will be *S* after your operations, do not try to transit it to *A*. In the first version of synchronous replication we will not add safety checks so this operation is valid but obviously, you will loss data if you do this...

Let me explain why we need to reject replication request under *A* or *DA*(especially *DA* as if you have a cluster in state *A* then you can make sure the other cluster is in state *S* as the *DA*\-\>*S* transition always comes first so no replication request will be made).  
Think of a DC lost. The DC for the active cluster(CA) is not reachable, so you transit the slave cluster(CB) from *S* to *DA* to offer service. But the DC for CA is not reachable so you can not transit it to a state other than *A*, once the network is back it will start replicating data to CB as it still think CB is the slave cluster. But new data are already written to CB so this will cause data inconsistency. For example, we have edit 1\~6, then these replay sequences are valid:  
1, 2, 3  
2, 3  
3, 4, 5, 6  
5, 6  
As when you restart from a previous edit, finally you will always reach the max known edit.  
But in the above scenario, the new data are only written to CB, so the replication request from CA will not have the newest data, i.e, the sequences maybe:  
1, 2, 3, 4, 5, 6  
2, 3, 4  
This is not valid. So we need to reject replication request from CA and let it give up replication when it finally transit to state *S*.

## WAL Logging

Introduce a new type of AsyncWriter which combines two AsyncProtobufLogWriters together. Only the write of the two AsyncProtobufLogWriters both succeed we consider the write succeed. In the first version, we need to write remote WAL first, and if it is successful then write the local WAL. Will explain the reason in the last two section.  
The remote WAL will be placed in a directory other than the normal WAL directory. And the name of the WAL file will be the RS name of active cluster. The directory will be created by standby cluster when it transited into state *S*.  
Further optimization maybe only encoding once and also only use one memory buffer. Now the logics are both in AsyncProtobufLogWriter so the simple implementation will double the calculate and memory.

## Remote WAL ‘Replay’

When transit from *S* to *DA*, we need to ‘replay’ the remote WAL. In general, this is not a normal WAL replay. The mvcc number of the two clusters are not the same so we can not use the typical WAL replay. And the regions which need to receive the edits are already opened and onlined. I think it is more like a replication—convert the edit to a put and then execute it. And notice that, here we have the same problem which serial replication wants to solve([HBASE-9465](https://issues.apache.org/jira/browse/HBASE-9465)), so we need to disable major compaction when replaying remote WAL, and re-enable it in the end.  
So I think we could do it like this.

1. Master collect all the remote WAL files and generate a task for each of them.  
2. The RS which assigned the task will read the WAL file, convert the edits to puts, and then execute them, just like what they do when replication.  
3. If all tasks have been finished,  then we are done.

For safety, we need to rename the remote WAL directory, and make sure all the files under the directory are closed(by calling recoverLease). This is used to prevent further remote WALs as we always said, we can not make sure that the old active cluster is in the right state.

## How To Handle WAL Logging Error

We will retry forever if there are errors. This is the default behavior of AsyncFSWAL. But RS may be crashed so there could be differences between the normal WAL and remote WAL. As in the first version, we will write remote WAL and then local WAL, so the only possible inconsistency is remote WAL success but local WAL fail. The solution is straight-forward:

* When doing remote WAL replay, we will also write WAL, and then replicate them back. 

# Why remote WAL first

**A1. *Only active cluster(which is read/written by clients) can have more data.***  
This is obvious. If a newly promoted active cluster has less data than the old one then we loss data. So this implies:  
**A2. *Only active cluster(including the current promoting one) can replay a WAL file with more data, either local or remote.***  
This is also easy to understand. Replaying a WAL file with more data will lead to more data, so only active cluster can do this, otherwise we loss data.  
We can use this two assertions to verify our solution for ‘remote WAL success but local WAL fail’. Notice that this means the remote WAL will have more data. Consider two clusters, CA and CB, CA is A and CB is S. If we transit CA to *DA*, then the remote WAL will be discarded and CB will not replay it any more. If we transit CB to *DA*, then it is the active cluster being promoted currently so we are still safe.  
Now let’s see why writing local/remote WAL concurrently is not safe. Still CA and CB but now CA has more data. If we transit CB to *DA* then CA will replay the WAL file with more data when doing WAL splitting which breaks ***A2***. In the slides of Alibaba, they say that we need to copy the WAL file to CB when doing WAL splitting in CA, and retry forever. But actually this is not operable. This means whether you can promote CB to *DA* depends on the status of CA. What if the DC for CA is completely offline?

# Possible way to write WAL concurrently

Obviously, we need to stop replaying WAL if we do not know the whether it may contains more data. So the first thing is like the Alibaba’s solution, copy WAL files to CB when doing WAL splitting, and retry forever. Then there are two options to keep consistency:

1. If we find that this can not be done, for example, CB has already been promoted to *DA* and the remote WAL directory has been gone, then we give up all the data in CA and copy all the data back from CB when we want to setup synchronous replication again.  
2. If we failed to copy, we need to find out the WAL files which have not been replicated by asynchronous replication yet, and give up splitting and replaying them. The data in these WAL files will finally be replicated back

Solution 1 is simple but costly. Solution 2 is more friendly for operators but makes things more complicated as the failover logic is already very very complicated.  
But anyway, writing WAL concurrently could greatly increase the write throughput, so I think we must do this optimization in the next version of synchronous replication.